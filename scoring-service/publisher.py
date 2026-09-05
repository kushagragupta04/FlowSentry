"""
publisher.py — Fire-and-forget flagged event publisher for the scoring service.

When a transaction is flagged or blocked, this module publishes the full
event context to the 'flagged-events' Kafka topic so the LLM worker can
generate an investigation note asynchronously.

Critical design constraint:
  This MUST NOT slow or fail the synchronous decision path.
  The Kafka produce() call is non-blocking (returns immediately).
  If the broker is unavailable, the exception is caught and logged,
  but the scoring response is already returned to the client.
  The flagged event will be missed in that case — acceptable for the
  async investigation path (the decision is already persisted to Postgres).
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import structlog
from confluent_kafka import Producer, KafkaError

log = structlog.get_logger()

# Shared producer instance (initialized once at startup)
_producer: Producer | None = None


def init_publisher(bootstrap_servers: str) -> None:
    """Initialize the Kafka producer. Call once at application startup."""
    global _producer
    _producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": "0",                    # Fire-and-forget: no ack wait (latency priority)
        "linger.ms": 0,                 # No batching delay on this path
        "compression.type": "lz4",
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 32_768,
        "message.timeout.ms": 5000,     # Give up after 5 seconds
    })
    log.info("flagged_event_publisher_initialized")


def _delivery_callback(err: Any, msg: Any) -> None:
    """Async delivery callback — only logs errors (fire-and-forget)."""
    if err is not None:
        log.error(
            "flagged_event_delivery_failed",
            topic=msg.topic() if msg else "unknown",
            error=str(err),
        )


def publish_flagged_event(
    transaction_id: str,
    account_id: str,
    transaction: dict,
    decision: str,
    risk_score: float,
    triggered_rules: list,
    feature_snapshot: dict,
    topic: str,
) -> None:
    """
    Publish a flagged/blocked transaction to the async investigation queue.

    This is called AFTER the synchronous decision is already returned and persisted.
    Failures are logged but never re-raise — they must not affect the scoring path.

    The payload includes:
      - Full transaction fields (for LLM context)
      - Decision and risk score
      - List of triggered rules (which signals fired)
      - Feature snapshot (window feature values at decision time)
    """
    global _producer

    if _producer is None:
        log.warning("publisher_not_initialized", transaction_id=transaction_id)
        return

    payload = {
        "transaction_id": transaction_id,
        "account_id": account_id,
        "transaction": transaction,
        "decision": decision,
        "risk_score": risk_score,
        "triggered_rules": triggered_rules,
        "feature_snapshot": feature_snapshot,
        "published_at_ms": int(time.time() * 1000),
    }

    try:
        _producer.produce(
            topic=topic,
            key=account_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=_delivery_callback,
        )
        # poll(0) is non-blocking — triggers any pending delivery callbacks
        _producer.poll(0)
    except Exception as e:
        # Catch-all: Kafka unavailable, serialization error, etc.
        # MUST NOT propagate — the scoring response is already sent.
        log.error(
            "flagged_event_publish_error",
            transaction_id=transaction_id,
            error=str(e),
        )


def flush_publisher(timeout_seconds: float = 5.0) -> None:
    """Flush any buffered messages. Called during graceful shutdown."""
    if _producer is not None:
        remaining = _producer.flush(timeout=timeout_seconds)
        if remaining > 0:
            log.warning("publisher_flush_incomplete", remaining=remaining)
