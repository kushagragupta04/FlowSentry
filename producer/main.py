"""
main.py — Synthetic transaction producer for RealTimeFraudGuard.

Generates realistic transaction events with configurable TPS and fraud rate,
serializes them with Avro (schema registry enforced), and publishes to Kafka
using account_id as the partition key to guarantee per-account ordering.

Key production properties:
  - Schema validated at startup against registry (hard fail if schema incompatible)
  - Delivery callbacks for every message (no fire-and-forget data loss)
  - Prometheus metrics exported on port 8001
  - Structured JSON logging via structlog
  - Graceful shutdown on SIGTERM/SIGINT
"""

from __future__ import annotations

import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from threading import Event
from typing import Any

import structlog
from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, Gauge, start_http_server

from schema import (
    get_schema_registry_client,
    get_avro_serializer,
    register_schema,
    transaction_to_dict,
)

load_dotenv()

# ── Logging ─────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ── Prometheus metrics ────────────────────────────────────────
MESSAGES_PRODUCED = Counter(
    "producer_messages_total",
    "Total Avro messages published to Kafka",
    ["topic", "status"],  # status: success | error
)
PRODUCE_LATENCY = Histogram(
    "producer_produce_latency_seconds",
    "Time from produce() call to delivery callback (end-to-end broker ack)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
MESSAGES_IN_FLIGHT = Gauge(
    "producer_messages_in_flight",
    "Number of messages produced but not yet acknowledged",
)
PRODUCER_TPS = Gauge(
    "producer_actual_tps",
    "Actual messages produced per second (rolling 1s window)",
)

# ── Synthetic data pools ──────────────────────────────────────
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "CN", "BR", "CA", "AU", "JP", "MX", "SG", "NG", "ZA", "KR"]
MERCHANTS = [f"MERCHANT_{i:04d}" for i in range(2000)]
# Device fingerprint pool: smaller than accounts to simulate device reuse
DEVICES = [f"fp_{uuid.uuid4().hex[:16]}" for _ in range(5000)]
# Account pool: fixed for the lifetime of the process so window features accumulate
ACCOUNTS = [f"ACC_{i:06d}" for i in range(10000)]


def _generate_transaction(fraud_rate: float) -> dict[str, Any]:
    """
    Generate a single synthetic transaction event matching the Avro schema.

    Fraud signals embedded in synthetic data (to make model training meaningful):
      - High amount (>$500)
      - Shipping country ≠ billing country
      - Geo country ≠ billing country
      - Rapid successive transactions (indirectly: high fraud_rate accounts)
    """
    is_fraud = random.random() < fraud_rate
    account_id = random.choice(ACCOUNTS)
    billing_country = random.choice(COUNTRIES)

    if is_fraud:
        # Fraud pattern: amount spike + country mismatch
        amount = round(random.uniform(300, 8000), 2)
        shipping_country = random.choice([c for c in COUNTRIES if c != billing_country])
        geo_country = random.choice([c for c in COUNTRIES if c != billing_country])
    else:
        amount = round(random.uniform(2, 450), 2)
        # 92% of legitimate transactions: shipping = billing
        shipping_country = billing_country if random.random() < 0.92 else random.choice(COUNTRIES)
        geo_country = billing_country if random.random() < 0.85 else random.choice(COUNTRIES)

    geo_lat = round(random.uniform(-60, 75), 6)
    geo_lon = round(random.uniform(-180, 180), 6)

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "amount": amount,
        "merchant_id": random.choice(MERCHANTS),
        "device_fingerprint": random.choice(DEVICES),
        "geo_location": {
            "lat": geo_lat,
            "lon": geo_lon,
            "country": geo_country,
            "city": "Unknown",
        },
        "timestamp_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "billing_country": billing_country,
        "shipping_country": shipping_country,
    }


# Track produce timestamps for delivery callback latency calculation
_produce_times: dict[str, float] = {}
_shutdown_event = Event()


def _delivery_callback(err: Any, msg: Any) -> None:
    """
    Called by librdkafka thread for every produced message.
    Records success/failure metrics and logs any delivery errors.
    """
    MESSAGES_IN_FLIGHT.dec()

    # Recover the transaction_id from message key for latency tracking
    key = msg.key().decode("utf-8") if msg.key() else None
    produce_time = _produce_times.pop(key or "", None)
    if produce_time is not None:
        PRODUCE_LATENCY.observe(time.monotonic() - produce_time)

    if err is not None:
        MESSAGES_PRODUCED.labels(topic=msg.topic(), status="error").inc()
        log.error("delivery_failed", topic=msg.topic(), error=str(err), partition=msg.partition())
    else:
        MESSAGES_PRODUCED.labels(topic=msg.topic(), status="success").inc()


def _build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        # Compression reduces broker I/O under high TPS
        "compression.type": "lz4",
        # Wait up to 5ms to batch messages — reduces per-message overhead
        "linger.ms": 5,
        # 1MB batch size
        "batch.size": 1_048_576,
        # Require acknowledgement from the leader (durability without full ISR wait)
        "acks": "1",
        # Allow up to 5 in-flight requests for throughput
        "max.in.flight.requests.per.connection": 5,
        # Enable idempotence for exactly-once produce semantics
        "enable.idempotence": False,  # Disabled: idempotence requires acks=all
        # Socket buffers
        "socket.send.buffer.bytes": 131072,
    })


def run(
    bootstrap_servers: str,
    schema_registry_url: str,
    topic: str,
    tps: int,
    fraud_rate: float,
) -> None:
    """Main producer loop. Exits on SIGTERM/SIGINT."""

    # Register schema at startup — hard fail if incompatible
    log.info("registering_schema", schema_registry=schema_registry_url, topic=topic)
    sr_client = get_schema_registry_client(schema_registry_url)
    schema_id = register_schema(sr_client, subject=f"{topic}-value")
    log.info("schema_registered", schema_id=schema_id)

    serializer = get_avro_serializer(sr_client)
    ctx = SerializationContext(topic, MessageField.VALUE)
    producer = _build_producer(bootstrap_servers)

    log.info("producer_starting", tps=tps, fraud_rate=fraud_rate, topic=topic)

    interval = 1.0 / tps  # seconds between messages
    window_start = time.monotonic()
    window_count = 0

    try:
        while not _shutdown_event.is_set():
            loop_start = time.monotonic()

            txn = _generate_transaction(fraud_rate)
            key = txn["account_id"]          # partition key = account_id
            value = serializer(txn, ctx)

            _produce_times[key] = time.monotonic()
            MESSAGES_IN_FLIGHT.inc()

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=_delivery_callback,
            )

            # Poll callbacks without blocking (allows delivery_callback to fire)
            producer.poll(0)

            window_count += 1
            elapsed = time.monotonic() - window_start
            if elapsed >= 1.0:
                PRODUCER_TPS.set(window_count / elapsed)
                window_count = 0
                window_start = time.monotonic()

            # Rate limiting: sleep remainder of interval
            sleep_time = interval - (time.monotonic() - loop_start)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("producer_stopping", reason="keyboard_interrupt")
    finally:
        log.info("flushing_producer", timeout_seconds=30)
        remaining = producer.flush(30)
        if remaining > 0:
            log.warning("unflushed_messages", count=remaining)
        log.info("producer_stopped")


def main() -> None:
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, lambda *_: _shutdown_event.set())
    signal.signal(signal.SIGINT, lambda *_: _shutdown_event.set())

    # Start Prometheus metrics server
    metrics_port = int(os.getenv("PRODUCER_METRICS_PORT", "8001"))
    start_http_server(metrics_port)
    log.info("metrics_server_started", port=metrics_port)

    run(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        topic=os.environ.get("TRANSACTIONS_TOPIC", "transactions"),
        tps=int(os.environ.get("PRODUCER_TPS", "100")),
        fraud_rate=float(os.environ.get("FRAUD_RATE", "0.02")),
    )


if __name__ == "__main__":
    main()
