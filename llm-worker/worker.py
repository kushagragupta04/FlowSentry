"""
worker.py — Async LLM investigation worker for RealTimeFraudGuard.

Consumes flagged transaction events from Kafka, generates investigation
notes using Groq (llama-3.3-70b-versatile), and persists notes to PostgreSQL.

Design principles:
  - NEVER affects the synchronous scoring path — this is a fully separate process
  - At-least-once consumption with manual offset commits (after successful write)
  - Dead-letter queue (DLQ) for messages that exhaust retries
  - Redis cache for common rule combinations (cost control)
  - Prometheus metrics for LLM latency and error rate (separate from scoring SLOs)
  - PII stripped BEFORE any data leaves this service to Groq

Kafka consumer configuration:
  - enable.auto.commit=false (manual offset commit after successful processing)
  - This ensures at-least-once: if the worker crashes before committing,
    the message will be redelivered on restart. Combined with ON CONFLICT
    in the Postgres insert, duplicate deliveries are idempotent.
"""

from __future__ import annotations

import json
import os
import signal
import time
from threading import Event
from typing import Optional

import structlog
import asyncpg
import redis
from groq import Groq, RateLimitError, APIError
from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from dotenv import load_dotenv

from pii_stripper import strip_pii
from prompt_templates import (
    SYSTEM_PROMPT,
    build_user_prompt,
    make_cache_key,
)

load_dotenv()

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ── Prometheus metrics ─────────────────────────────────────────
LLM_CALLS = Counter("llm_worker_calls_total", "LLM API calls", ["status"])  # success|error|cached
LLM_LATENCY = Histogram(
    "llm_worker_call_duration_seconds",
    "Groq API call latency",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
LLM_TOKENS = Counter("llm_worker_tokens_total", "LLM tokens used", ["type"])  # prompt|completion
LLM_COST_USD = Counter("llm_worker_estimated_cost_usd_total", "Estimated LLM cost (USD)")
MESSAGES_PROCESSED = Counter("llm_worker_messages_processed_total", "Messages consumed from Kafka", ["status"])
DLQ_MESSAGES = Counter("llm_worker_dlq_total", "Messages sent to DLQ")
CACHE_HIT_RATE = Counter("llm_worker_cache_total", "LLM cache lookups", ["result"])  # hit|miss
CONSUMER_LAG_GAUGE = Gauge("llm_worker_consumer_lag", "Estimated Kafka consumer lag")

# ── Groq pricing (as of 2024) — for cost tracking only ────────
# llama3-70b-8192: $0.59/M input tokens, $0.79/M output tokens
GROQ_COST_INPUT_PER_TOKEN  = 0.59 / 1_000_000
GROQ_COST_OUTPUT_PER_TOKEN = 0.79 / 1_000_000

_shutdown_event = Event()
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds


class LLMWorker:
    def __init__(self) -> None:
        self._groq = Groq(api_key=os.environ["GROQ_API_KEY"])
        self._model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        self._max_tokens = int(os.getenv("GROQ_MAX_TOKENS", "512"))
        self._temperature = float(os.getenv("GROQ_TEMPERATURE", "0.3"))
        self._cache_enabled = os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true"
        self._cache_ttl = int(os.getenv("LLM_CACHE_TTL_SECONDS", "3600"))

        self._db_pool: Optional[asyncpg.Pool] = None
        self._redis: Optional[redis.Redis] = None
        self._consumer: Optional[Consumer] = None

    def _connect_redis(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._redis.ping()
        log.info("redis_connected")

    def _connect_consumer(self) -> None:
        bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        group_id = os.getenv("KAFKA_CONSUMER_GROUP_LLM", "llm-worker-cg")
        topic = os.getenv("FLAGGED_EVENTS_TOPIC", "flagged-events")

        self._consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit: at-least-once guarantee
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,  # 5 minutes: LLM calls can be slow
        })
        self._consumer.subscribe([topic])
        log.info("kafka_consumer_subscribed", topic=topic, group=group_id)

    def _connect_db(self) -> None:
        """Synchronous PostgreSQL connection for the worker (not async)."""
        import psycopg2
        db_url = os.environ["DATABASE_URL"]
        self._db_conn = psycopg2.connect(db_url)
        self._db_conn.autocommit = False
        log.info("db_connected")

    def _get_cached_note(self, cache_key: str) -> Optional[str]:
        """Check Redis for a cached LLM response."""
        if not self._cache_enabled or not self._redis:
            return None
        try:
            return self._redis.get(cache_key)
        except Exception:
            return None

    def _cache_note(self, cache_key: str, note: str) -> None:
        """Cache an LLM response in Redis."""
        if not self._cache_enabled or not self._redis:
            return
        try:
            self._redis.set(cache_key, note, ex=self._cache_ttl)
        except Exception as e:
            log.warning("cache_write_failed", error=str(e))

    def _call_groq(self, stripped_event: dict) -> tuple[str, int, int]:
        """
        Call Groq API with PII-stripped event data.
        Returns: (note_text, prompt_tokens, completion_tokens)
        """
        system_msg = SYSTEM_PROMPT
        user_msg = build_user_prompt(stripped_event)

        t0 = time.monotonic()
        response = self._groq.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=self._max_tokens,
            temperature=self._temperature,
        )
        elapsed = time.monotonic() - t0
        LLM_LATENCY.observe(elapsed)

        note = response.choices[0].message.content.strip()
        prompt_tokens = response.usage.prompt_tokens
        completion_tokens = response.usage.completion_tokens

        # Track token usage and estimated cost
        LLM_TOKENS.labels(type="prompt").inc(prompt_tokens)
        LLM_TOKENS.labels(type="completion").inc(completion_tokens)
        cost = (prompt_tokens * GROQ_COST_INPUT_PER_TOKEN +
                completion_tokens * GROQ_COST_OUTPUT_PER_TOKEN)
        LLM_COST_USD.inc(cost)

        log.info(
            "groq_call_complete",
            model=self._model,
            latency_ms=round(elapsed * 1000, 1),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            estimated_cost_usd=round(cost, 6),
        )
        return note, prompt_tokens, completion_tokens

    def _write_investigation_note(
        self,
        transaction_id: str,
        note: str,
        model_used: str,
        prompt_tokens: int,
        completion_tokens: int,
        llm_latency_ms: int,
        cache_hit: bool,
        triggered_rules: list,
    ) -> None:
        """Write LLM note to PostgreSQL investigation_notes table."""
        import psycopg2.extras
        cursor = self._db_conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO investigation_notes (
                    transaction_id, note_text, model_used,
                    prompt_tokens, completion_tokens,
                    llm_latency_ms, cache_hit, triggered_rules
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                transaction_id, note, model_used,
                prompt_tokens, completion_tokens,
                llm_latency_ms, cache_hit,
                json.dumps(triggered_rules),
            ))
            # Also refresh the materialized view so the dashboard reflects the new note
            cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY analyst_queue")
            self._db_conn.commit()
            log.info("investigation_note_saved", transaction_id=transaction_id)
        except Exception as e:
            self._db_conn.rollback()
            raise e
        finally:
            cursor.close()

    def _write_to_dlq(self, raw_payload: str, error_message: str, retry_count: int) -> None:
        """Write failed message to DLQ table in Postgres."""
        cursor = self._db_conn.cursor()
        try:
            data = json.loads(raw_payload) if raw_payload else {}
            cursor.execute("""
                INSERT INTO llm_worker_dlq (transaction_id, raw_event, error_message, retry_count)
                VALUES (%s, %s, %s, %s)
            """, (
                data.get("transaction_id"),
                json.dumps(data),
                error_message,
                retry_count,
            ))
            self._db_conn.commit()
            DLQ_MESSAGES.inc()
        except Exception as e:
            self._db_conn.rollback()
            log.error("dlq_write_failed", error=str(e))
        finally:
            cursor.close()

    def process_message(self, raw_value: bytes) -> None:
        """
        Process a single flagged event:
          1. Parse event
          2. Strip PII
          3. Check cache
          4. Call Groq (if not cached)
          5. Write note to Postgres
        """
        event = json.loads(raw_value.decode("utf-8"))
        transaction_id = event.get("transaction_id", "unknown")
        triggered_rules = event.get("triggered_rules", [])
        decision = event.get("decision", "flag")

        log.info("processing_flagged_event", transaction_id=transaction_id, decision=decision)

        # Strip PII before any external call
        stripped = strip_pii(event)

        # Check cache
        cache_key = make_cache_key(triggered_rules, decision)
        cached_note = self._get_cached_note(cache_key)

        if cached_note:
            CACHE_HIT_RATE.labels(result="hit").inc()
            LLM_CALLS.labels(status="cached").inc()
            log.info("llm_cache_hit", transaction_id=transaction_id)
            self._write_investigation_note(
                transaction_id=transaction_id,
                note=cached_note,
                model_used=f"{self._model}(cached)",
                prompt_tokens=0,
                completion_tokens=0,
                llm_latency_ms=0,
                cache_hit=True,
                triggered_rules=triggered_rules,
            )
            return

        CACHE_HIT_RATE.labels(result="miss").inc()

        # Call Groq
        t0 = time.monotonic()
        note, prompt_tokens, completion_tokens = self._call_groq(stripped)
        llm_latency_ms = int((time.monotonic() - t0) * 1000)
        LLM_CALLS.labels(status="success").inc()

        # Cache for future similar rule combinations
        self._cache_note(cache_key, note)

        # Persist to Postgres
        self._write_investigation_note(
            transaction_id=transaction_id,
            note=note,
            model_used=self._model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            llm_latency_ms=llm_latency_ms,
            cache_hit=False,
            triggered_rules=triggered_rules,
        )

    def run(self) -> None:
        """Main consumer loop."""
        log.info("llm_worker_starting")
        self._connect_redis()
        self._connect_consumer()
        self._connect_db()

        # Start Prometheus metrics server
        metrics_port = int(os.getenv("LLM_WORKER_METRICS_PORT", "8003"))
        start_http_server(metrics_port)
        log.info("metrics_server_started", port=metrics_port)

        try:
            while not _shutdown_event.is_set():
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("kafka_error", error=str(msg.error()))
                    continue

                raw_value = msg.value()
                retry_count = 0
                success = False

                while retry_count < MAX_RETRIES and not success:
                    try:
                        self.process_message(raw_value)
                        # Commit offset AFTER successful processing
                        self._consumer.commit(message=msg, asynchronous=False)
                        MESSAGES_PROCESSED.labels(status="success").inc()
                        success = True
                    except (RateLimitError, APIError) as e:
                        retry_count += 1
                        backoff = RETRY_BACKOFF_BASE ** retry_count
                        log.warning(
                            "llm_api_error_retry",
                            error=str(e),
                            retry=retry_count,
                            backoff_seconds=backoff,
                        )
                        LLM_CALLS.labels(status="error").inc()
                        time.sleep(backoff)
                    except Exception as e:
                        log.error("message_processing_failed", error=str(e))
                        retry_count = MAX_RETRIES  # Force DLQ

                if not success:
                    self._write_to_dlq(raw_value.decode("utf-8", errors="replace"),
                                       "exhausted retries", retry_count)
                    self._consumer.commit(message=msg, asynchronous=False)
                    MESSAGES_PROCESSED.labels(status="dlq").inc()

        except KeyboardInterrupt:
            log.info("llm_worker_stopping", reason="keyboard_interrupt")
        finally:
            if self._consumer:
                self._consumer.close()
            if hasattr(self, "_db_conn") and self._db_conn:
                self._db_conn.close()
            log.info("llm_worker_stopped")


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: _shutdown_event.set())
    signal.signal(signal.SIGINT, lambda *_: _shutdown_event.set())
    worker = LLMWorker()
    worker.run()


if __name__ == "__main__":
    main()
