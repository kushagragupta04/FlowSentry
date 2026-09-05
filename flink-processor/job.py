"""
job.py — PyFlink stream feature processor for RealTimeFraudGuard.

Architecture:
  Kafka (transactions) → Flink KeyedProcessFunction (keyed by account_id)
                       → Sliding window state (RocksDB) → Redis (feature store)

Computed features per account:
  - txn_count_5min:          count of transactions in the last 5 minutes
  - avg_amount_1hr:          rolling average amount in the last 1 hour
  - distinct_merchants_24hr: approximate distinct merchants in last 24 hours
  - distinct_countries_10min: distinct geo countries in last 10 minutes

Design decisions:
  - KeyedProcessFunction instead of Flink's built-in SlidingWindow:
    Built-in SlidingWindows store one state entry per (window, key) pair,
    which for 4 windows × 10k accounts = 40k state entries per slide interval.
    KeyedProcessFunction with a single ListState stores raw events per account
    and recomputes windows on each new event — fewer state entries, cheaper
    checkpoint overhead, and exact control over what gets stored.

  - RocksDB state backend (configured in docker-compose FLINK_PROPERTIES):
    Chosen over FsStateBackend because at 5k TPS × 10k accounts with 24-hour
    windows, heap state would hold millions of event timestamps. RocksDB
    spills to disk and uses memory-mapped I/O for hot keys.

  - Checkpointing every 30 seconds to MinIO (S3-compatible):
    Ensures window state survives Flink job restarts. The checkpoint interval
    is a tradeoff: shorter = less recovery work, longer = lower overhead.
    30s is standard for stream jobs at this TPS.

  - At-least-once processing (not exactly-once at the Flink level):
    Exactly-once would require a transactional sink (Redis does not support
    two-phase commit). With at-least-once, a duplicate event increments the
    window count once — acceptable for fraud features where a small double-count
    is far less harmful than a missed event.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Iterator, List, Optional

import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger()

# ── Window durations (seconds) ────────────────────────────────
WINDOW_5MIN   = 5  * 60
WINDOW_1HR    = 60 * 60
WINDOW_24HR   = 24 * 60 * 60
WINDOW_10MIN  = 10 * 60
MAX_WINDOW    = WINDOW_24HR  # longest window; events older than this are pruned


def _compute_features(events: List[dict], now_ms: int) -> dict:
    """
    Compute all sliding-window features from a list of historical events for
    one account. Called inside the Flink KeyedProcessFunction on every new event.

    Args:
        events: List of event dicts, each with keys:
                  timestamp_ms, amount, merchant_id, geo_country
        now_ms: Current event time in milliseconds since epoch.

    Returns:
        Feature dict ready to be written to Redis and used in the scoring feature vector.
    """
    now_s = now_ms / 1000.0

    txn_count_5min = 0
    total_amount_1hr = 0.0
    count_1hr = 0
    merchants_24hr: set = set()
    countries_10min: set = set()

    for evt in events:
        evt_s = evt["timestamp_ms"] / 1000.0
        age_s = now_s - evt_s

        if age_s <= WINDOW_5MIN:
            txn_count_5min += 1

        if age_s <= WINDOW_1HR:
            total_amount_1hr += evt["amount"]
            count_1hr += 1

        if age_s <= WINDOW_24HR:
            merchants_24hr.add(evt["merchant_id"])

        if age_s <= WINDOW_10MIN:
            countries_10min.add(evt.get("geo_country", ""))

    avg_amount_1hr = (total_amount_1hr / count_1hr) if count_1hr > 0 else 0.0

    return {
        "txn_count_5min": txn_count_5min,
        "avg_amount_1hr": round(avg_amount_1hr, 4),
        "distinct_merchants_24hr": len(merchants_24hr),
        "distinct_countries_10min": len(countries_10min),
        "computed_at_ms": now_ms,
    }


def _prune_events(events: List[dict], now_ms: int) -> List[dict]:
    """
    Remove events older than the maximum window (24 hours) to bound state size.
    Without pruning, the RocksDB state grows unboundedly for active accounts.
    """
    cutoff_ms = now_ms - (MAX_WINDOW * 1000)
    return [e for e in events if e["timestamp_ms"] >= cutoff_ms]


# ─── PyFlink job (DataStream API) ─────────────────────────────
try:
    from pyflink.common import WatermarkStrategy, Duration, Types
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
    from pyflink.datastream.connectors.kafka import (
        KafkaSource,
        KafkaOffsetsInitializer,
    )
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
    from pyflink.datastream.state import ListStateDescriptor
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    log.warning("pyflink_not_available", message="Running in standalone/test mode without PyFlink")


if PYFLINK_AVAILABLE:
    from redis_sink import RedisFeatureSink

    class FeatureComputeFunction(KeyedProcessFunction):
        """
        Flink KeyedProcessFunction that:
          1. Receives transaction events keyed by account_id
          2. Appends the event to per-account ListState
          3. Prunes events older than 24 hours from state
          4. Recomputes all sliding-window features
          5. Writes features to Redis

        State:
          _event_state: ListState[str] — JSON-serialized event records.
                        One list per account_id (enforced by Flink keying).
        """

        def __init__(self, redis_url: str) -> None:
            self._redis_url = redis_url
            self._redis_sink: Optional[RedisFeatureSink] = None
            self._event_state = None

        def open(self, runtime_context: RuntimeContext) -> None:
            """
            Called once per task slot when the operator starts (or restores from checkpoint).
            State descriptor registration is idempotent — Flink restores state from checkpoint.
            """
            descriptor = ListStateDescriptor("account_events", Types.STRING())
            self._event_state = runtime_context.get_list_state(descriptor)

            self._redis_sink = RedisFeatureSink(self._redis_url)
            self._redis_sink.open()
            log.info("feature_function_opened")

        def close(self) -> None:
            if self._redis_sink:
                self._redis_sink.close()

        def process_element(self, value: str, ctx: KeyedProcessFunction.Context) -> Iterator:
            """
            Called for every event in the keyed stream.
            value: JSON string of the transaction event (deserialized from Kafka).
            """
            try:
                event = json.loads(value)
            except json.JSONDecodeError as e:
                log.error("invalid_event_json", error=str(e))
                return

            now_ms = event.get("timestamp_ms", int(time.time() * 1000))
            account_id = ctx.get_current_key()

            # Add current event to state
            event_record = {
                "timestamp_ms": now_ms,
                "amount": event.get("amount", 0.0),
                "merchant_id": event.get("merchant_id", ""),
                "geo_country": event.get("geo_location", {}).get("country", ""),
            }
            self._event_state.add(json.dumps(event_record))

            # Read all historical events for this account
            all_events = [json.loads(e) for e in self._event_state.get() or []]

            # Prune expired events (older than 24 hours)
            pruned_events = _prune_events(all_events, now_ms)

            # Update state with pruned events if any were removed
            if len(pruned_events) < len(all_events):
                self._event_state.clear()
                for evt in pruned_events:
                    self._event_state.add(json.dumps(evt))

            # Compute features from all events in window
            features = _compute_features(pruned_events, now_ms)
            features["account_id"] = account_id

            # Write to Redis
            self._redis_sink.write_features(account_id, features)

            log.debug(
                "features_updated",
                account_id=account_id,
                txn_count_5min=features["txn_count_5min"],
                avg_amount_1hr=features["avg_amount_1hr"],
                distinct_merchants_24hr=features["distinct_merchants_24hr"],
                distinct_countries_10min=features["distinct_countries_10min"],
            )

            # No output — this is a side-effect-only function
            return iter([])

        def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext) -> Iterator:
            # No timers registered — returning empty
            return iter([])


    def build_kafka_source(
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        schema_registry_url: str,
    ) -> KafkaSource:
        """
        Build a KafkaSource that reads raw bytes from the transactions topic.
        We deserialize manually in the process function because PyFlink's Avro
        + Schema Registry integration is unstable; using SimpleStringSchema with
        manual Avro deserialization is more reliable at this version.
        """
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(bootstrap_servers)
            .set_topics(topic)
            .set_group_id(group_id)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )


    def run_job() -> None:
        """Entry point for the Flink job submission."""
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        schema_registry_url = os.environ["SCHEMA_REGISTRY_URL"]
        topic = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
        group_id = os.environ.get("FLINK_CONSUMER_GROUP", "flink-feature-processor")
        redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "4")))

        # Checkpointing (RocksDB backend configured in flink-conf.yaml/FLINK_PROPERTIES)
        env.enable_checkpointing(30_000)  # 30 seconds in ms

        kafka_source = build_kafka_source(bootstrap_servers, topic, group_id, schema_registry_url)

        # Bounded-out-of-orderness watermark: tolerate up to 5 seconds of late arrival
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))

        stream = env.from_source(
            source=kafka_source,
            watermark_strategy=watermark_strategy,
            source_name="transactions-kafka-source",
        )

        # Key by account_id extracted from the JSON string
        keyed_stream = stream.key_by(
            lambda raw: json.loads(raw).get("account_id", "unknown")
        )

        # Apply the feature computation function
        keyed_stream.process(FeatureComputeFunction(redis_url))

        log.info("submitting_flink_job", topic=topic, parallelism=env.get_parallelism())
        env.execute("fraudguard-feature-processor")


    if __name__ == "__main__":
        run_job()

else:
    # Standalone mode for unit testing without a Flink cluster
    def test_compute_features():
        """Smoke test for feature computation logic (no Flink/Redis required)."""
        import time
        now_ms = int(time.time() * 1000)
        events = [
            {"timestamp_ms": now_ms - 60_000,   "amount": 100.0, "merchant_id": "M001", "geo_country": "US"},
            {"timestamp_ms": now_ms - 200_000,  "amount": 250.0, "merchant_id": "M002", "geo_country": "GB"},
            {"timestamp_ms": now_ms - 500_000,  "amount": 50.0,  "merchant_id": "M001", "geo_country": "US"},
            {"timestamp_ms": now_ms - 3700_000, "amount": 75.0,  "merchant_id": "M003", "geo_country": "DE"},
        ]
        features = _compute_features(events, now_ms)
        assert features["txn_count_5min"] == 1, f"Expected 1, got {features['txn_count_5min']}"
        assert features["distinct_merchants_24hr"] == 3
        assert features["distinct_countries_10min"] == 2
        print("✓ Feature computation test passed:", features)

    test_compute_features()
    print("Run with PyFlink for full job execution.")
