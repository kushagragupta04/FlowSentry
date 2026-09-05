"""
redis_sink.py — Redis feature writer for the Flink stream processor.

Writes computed sliding-window features to Redis after every account event.
Key format: features:{account_id}
TTL: 90000 seconds (25 hours) — covers the 24-hour window plus buffer.

This module is imported by the Flink KeyedProcessFunction and called
synchronously within the processElement callback. Redis writes are
synchronous here because Flink's exactly-once guarantees apply at the
checkpoint level, not at the Redis write level. If a checkpoint fails
and the job restarts, the feature recomputation from the checkpoint
replays the same events and produces the same Redis writes.
"""

from __future__ import annotations

import json
import os
from typing import Optional

import redis
import structlog

log = structlog.get_logger()

# Feature TTL: 25 hours covers the widest window (24hr) with buffer
_FEATURES_TTL_SECONDS = int(os.getenv("REDIS_FEATURES_TTL_SECONDS", "90000"))
_REDIS_KEY_PREFIX = "features"


class RedisFeatureSink:
    """
    Writes computed features to Redis.
    Designed to be instantiated once per Flink operator instance (per TaskSlot).
    Uses connection pooling for efficiency under high TPS.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: Optional[redis.Redis] = None

    def open(self) -> None:
        """Called once when the Flink operator opens. Establishes Redis connection."""
        self._client = redis.from_url(
            self._redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=2,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        # Verify connectivity immediately — fail fast
        self._client.ping()
        log.info("redis_sink_opened", url=self._redis_url)

    def close(self) -> None:
        """Called when the operator closes. Clean up connection."""
        if self._client:
            self._client.close()

    def write_features(self, account_id: str, features: dict) -> None:
        """
        Write features for account_id to Redis with TTL.
        Uses SET with EX to atomically write + set expiry.
        """
        if self._client is None:
            raise RuntimeError("RedisFeatureSink not opened — call open() first")

        key = f"{_REDIS_KEY_PREFIX}:{account_id}"
        value = json.dumps(features)

        try:
            self._client.set(key, value, ex=_FEATURES_TTL_SECONDS)
        except redis.RedisError as e:
            # Log and continue — Redis write failure should not crash the Flink job.
            # Features will be missing from Redis until the next event for this account
            # recomputes and writes them. The scoring service handles missing features
            # by returning zero-value defaults.
            log.error(
                "redis_write_failed",
                account_id=account_id,
                error=str(e),
            )

    def read_features(self, account_id: str) -> Optional[dict]:
        """
        Read features for account_id (used in integration tests / debugging).
        Returns None if the key does not exist or has expired.
        """
        if self._client is None:
            raise RuntimeError("RedisFeatureSink not opened — call open() first")

        key = f"{_REDIS_KEY_PREFIX}:{account_id}"
        value = self._client.get(key)
        return json.loads(value) if value else None
