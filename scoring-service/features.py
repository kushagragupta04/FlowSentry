"""
features.py — Redis feature lookup for the scoring service.

Reads pre-computed sliding-window features for an account_id from Redis.
These features are written by the Flink processor and represent the account's
behavioral profile computed from the stream.

If features are missing (account not seen before, or Redis cache expired),
returns conservative zero-value defaults. The scoring model is trained to
treat zero values as "no history" — the risk score will be dominated by
raw transaction fields (amount, country mismatch) in that case.

Performance target: p99 Redis read < 2ms (verified under load test).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Optional

import redis.asyncio as aioredis
import structlog

log = structlog.get_logger()

# These defaults are returned when an account has no feature history.
# IMPORTANT: Values must match the training distribution floor values (train.py):
#   - distinct_countries_10min: trained with values 1 or 2 (addr1!=addr2 + 1), never 0.
#     Using 0 is OOD and inflates risk scores to ~86% for clean cold-start accounts.
#   - distinct_merchants_24hr: trained with C2.clip(1, 50), minimum was 1, never 0.
# Feeding 0 for these features produces out-of-distribution model inputs.
_DEFAULT_FEATURES = {
    "txn_count_5min": 0,
    "avg_amount_1hr": 0.0,
    "distinct_merchants_24hr": 1,  # training floor = 1 (C2 clipped to min 1)
    "distinct_countries_10min": 1, # training floor = 1 (addr1!=addr2 + 1, always ≥ 1)
}

_KEY_PREFIX = "features"


@dataclass
class AccountFeatures:
    """Sliding-window behavioral features for one account."""
    txn_count_5min: int = 0
    avg_amount_1hr: float = 0.0
    distinct_merchants_24hr: int = 1  # floor=1 to match training distribution
    distinct_countries_10min: int = 1  # floor=1 to match training distribution
    # Meta
    from_cache: bool = False
    account_id: str = ""

    def to_feature_vector_partial(self) -> dict:
        """Return only the numeric fields used in model inference."""
        return {
            "txn_count_5min": self.txn_count_5min,
            "avg_amount_1hr": self.avg_amount_1hr,
            "distinct_merchants_24hr": self.distinct_merchants_24hr,
            "distinct_countries_10min": self.distinct_countries_10min,
        }


class FeatureStore:
    """
    Async Redis client wrapper for feature lookups.
    Single instance shared across all FastAPI requests.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Initialize the async Redis connection pool."""
        self._client = await aioredis.from_url(
            self._redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=1,     # 1 second read timeout — fail fast for latency SLO
            max_connections=50,
        )
        await self._client.ping()
        log.info("feature_store_connected", url=self._redis_url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    async def get_features(self, account_id: str) -> AccountFeatures:
        """
        Retrieve behavioral features for account_id.
        Returns defaults on miss, expiry, or Redis error.

        Redis errors do NOT raise — they return defaults with from_cache=False.
        This ensures a Redis outage degrades gracefully (higher false-negative
        rate) rather than hard-failing the synchronous scoring path.
        """
        if self._client is None:
            return AccountFeatures(account_id=account_id)

        key = f"{_KEY_PREFIX}:{account_id}"
        try:
            raw = await self._client.get(key)
        except Exception as e:
            log.warning("redis_get_failed", account_id=account_id, error=str(e))
            return AccountFeatures(account_id=account_id)

        if raw is None:
            log.debug("redis_cache_miss", account_id=account_id)
            return AccountFeatures(account_id=account_id)

        try:
            data = json.loads(raw)
            return AccountFeatures(
                txn_count_5min=int(data.get("txn_count_5min", 0)),
                avg_amount_1hr=float(data.get("avg_amount_1hr", 0.0)),
                distinct_merchants_24hr=int(data.get("distinct_merchants_24hr", 0)),
                distinct_countries_10min=int(data.get("distinct_countries_10min", 0)),
                from_cache=True,
                account_id=account_id,
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            log.warning("redis_parse_failed", account_id=account_id, error=str(e))
            return AccountFeatures(account_id=account_id)
