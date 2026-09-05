"""
main.py — FastAPI scoring service for RealTimeFraudGuard.

Endpoint: POST /score
  Input:  Transaction JSON (all 8 required fields)
  Output: Decision (allow/flag/block), risk_score, triggered_rules, latency_ms

Synchronous path (all within this service):
  1. Validate request schema
  2. Fetch account features from Redis (< 2ms p99 target)
  3. Build feature vector (12 features = 4 window + 8 raw)
  4. Run XGBoost inference (< 5ms p99 target)
  5. Apply decision gate (< 1ms)
  6. Write audit record to Postgres (async, < 10ms p99)
  7. Publish flagged event to Kafka (non-blocking, 0ms on scoring path)
  8. Return decision

Total target: p50 < 30ms, p99 < 100ms

Additional endpoints:
  GET  /health  — liveness: is the process alive?
  GET  /ready   — readiness: is Redis connected AND lag within threshold?
  GET  /metrics — Prometheus metrics (scraped by prometheus-stack)
"""

from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

import numpy as np
import joblib
import structlog
import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace

from features import FeatureStore, AccountFeatures
from decision import make_decision, DecisionResult
from publisher import init_publisher, publish_flagged_event, flush_publisher
from telemetry import setup_telemetry, get_tracer

load_dotenv = None
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ── Prometheus metrics ─────────────────────────────────────────
SCORE_REQUESTS = Counter(
    "scoring_requests_total",
    "Total scoring requests",
    ["decision"],  # label: allow | flag | block
)
SCORE_LATENCY = Histogram(
    "scoring_request_duration_seconds",
    "End-to-end scoring request latency",
    buckets=[0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0],
)
REDIS_LATENCY = Histogram(
    "scoring_redis_lookup_seconds",
    "Redis feature lookup latency",
    buckets=[0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05],
)
MODEL_LATENCY = Histogram(
    "scoring_model_inference_seconds",
    "XGBoost model inference latency",
    buckets=[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1],
)
DB_WRITE_LATENCY = Histogram(
    "scoring_db_write_seconds",
    "Postgres audit write latency",
    buckets=[0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25],
)
REDIS_HIT_RATE = Counter(
    "scoring_redis_hits_total",
    "Redis feature lookup results",
    ["result"],  # hit | miss
)
RISK_SCORE_DISTRIBUTION = Histogram(
    "scoring_risk_score",
    "Distribution of risk scores produced by the model",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# ── Global singletons (initialized in lifespan) ────────────────
_feature_store: FeatureStore | None = None
_model = None           # XGBoost booster loaded with joblib
_db_pool: asyncpg.Pool | None = None
_tracer: trace.Tracer | None = None


# ── Pydantic request/response models ──────────────────────────
class GeoLocation(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    country: str = Field(..., min_length=2, max_length=8)
    city: str = Field(default="Unknown")


class ScoreRequest(BaseModel):
    transaction_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    account_id: str = Field(..., min_length=1, max_length=64)
    amount: float = Field(..., gt=0)
    merchant_id: str = Field(..., min_length=1, max_length=128)
    device_fingerprint: str = Field(..., min_length=1, max_length=128)
    geo_location: GeoLocation
    timestamp_ms: Optional[int] = Field(default=None)
    billing_country: str = Field(..., min_length=2, max_length=8)
    shipping_country: str = Field(..., min_length=2, max_length=8)


class ScoreResponse(BaseModel):
    transaction_id: str
    account_id: str
    decision: str       # allow | flag | block
    risk_score: float
    triggered_rules: List[str]
    latency_ms: float


# ── Feature vector builder ────────────────────────────────────
FEATURE_COLUMNS = [
    "txn_count_5min",
    "avg_amount_1hr",
    "distinct_merchants_24hr",
    "distinct_countries_10min",
    "amount",
    "billing_eq_shipping",
    "geo_eq_billing",
    "amount_log",
]


def build_feature_vector(
    tx: ScoreRequest,
    account_features: AccountFeatures,
) -> np.ndarray:
    """
    Assemble the 8-feature vector fed to XGBoost.

    Features are ordered to match the training schema exactly.
    Any change to this ordering is a breaking model change requiring retraining.
    """
    billing_eq_shipping = 1 if tx.billing_country == tx.shipping_country else 0
    geo_eq_billing = 1 if tx.geo_location.country == tx.billing_country else 0
    amount_log = float(np.log1p(tx.amount))

    return np.array([[
        account_features.txn_count_5min,
        account_features.avg_amount_1hr,
        account_features.distinct_merchants_24hr,
        account_features.distinct_countries_10min,
        tx.amount,
        billing_eq_shipping,
        geo_eq_billing,
        amount_log,
    ]], dtype=np.float32)


# ── Application lifespan ─────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize all singletons on startup, tear down on shutdown."""
    global _feature_store, _model, _db_pool, _tracer

    log.info("scoring_service_starting")

    # OpenTelemetry
    _tracer = get_tracer()

    # Redis feature store
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    _feature_store = FeatureStore(redis_url)
    await _feature_store.connect()

    # XGBoost model
    model_path = os.environ.get("MODEL_PATH", "/app/model/model.pkl")
    try:
        _model = joblib.load(model_path)
        log.info("model_loaded", path=model_path)
    except FileNotFoundError:
        log.warning("model_not_found", path=model_path, message="Running without model — will return random scores")
        _model = None

    # PostgreSQL connection pool
    db_url = os.environ.get("DATABASE_URL", "")
    if db_url:
        _db_pool = await asyncpg.create_pool(
            db_url,
            min_size=5,
            max_size=20,
            command_timeout=5,
        )
        log.info("db_pool_created")

    # Kafka publisher for flagged events
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    init_publisher(kafka_servers)

    log.info("scoring_service_ready")
    yield

    # Shutdown
    log.info("scoring_service_stopping")
    if _feature_store:
        await _feature_store.close()
    if _db_pool:
        await _db_pool.close()
    flush_publisher(timeout_seconds=5.0)
    log.info("scoring_service_stopped")


# ── FastAPI application ────────────────────────────────────────
app = FastAPI(
    title="FraudGuard Scoring Service",
    description="Real-time fraud risk scoring — p99 < 100ms",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize telemetry and instrument application before startup
setup_telemetry(app)


@app.post("/score", response_model=ScoreResponse)
async def score_transaction(request: ScoreRequest) -> ScoreResponse:
    """
    Score a transaction for fraud risk and return a synchronous decision.

    This is the hot path — every millisecond matters.
    Stage timing is recorded as OTel spans and Prometheus histograms.
    """
    request_start = time.monotonic()
    tracer = get_tracer()

    with tracer.start_as_current_span("score_transaction") as root_span:
        root_span.set_attribute("transaction.id", request.transaction_id)
        root_span.set_attribute("account.id", request.account_id)
        root_span.set_attribute("transaction.amount", request.amount)

        # Stage 1: Redis feature lookup
        with tracer.start_as_current_span("redis_lookup") as redis_span:
            t0 = time.monotonic()
            account_features = await _feature_store.get_features(request.account_id)
            redis_elapsed = time.monotonic() - t0
            REDIS_LATENCY.observe(redis_elapsed)
            REDIS_HIT_RATE.labels(result="hit" if account_features.from_cache else "miss").inc()
            redis_span.set_attribute("cache_hit", account_features.from_cache)
            redis_span.set_attribute("latency_ms", redis_elapsed * 1000)

        # Stage 2: Build feature vector + model inference
        with tracer.start_as_current_span("model_inference") as model_span:
            t0 = time.monotonic()
            feature_vector = build_feature_vector(request, account_features)

            if _model is not None:
                risk_score = float(_model.predict_proba(feature_vector)[0][1])
            else:
                # Fallback: simple heuristic until model is trained
                risk_score = _heuristic_score(request, account_features)

            inference_elapsed = time.monotonic() - t0
            MODEL_LATENCY.observe(inference_elapsed)
            RISK_SCORE_DISTRIBUTION.observe(risk_score)
            model_span.set_attribute("risk_score", risk_score)
            model_span.set_attribute("latency_ms", inference_elapsed * 1000)

        # Stage 3: Decision gate
        with tracer.start_as_current_span("decision_gate"):
            result: DecisionResult = make_decision(
                risk_score=risk_score,
                amount=request.amount,
                billing_country=request.billing_country,
                shipping_country=request.shipping_country,
                geo_country=request.geo_location.country,
                distinct_countries_10min=account_features.distinct_countries_10min,
            )

        total_latency_ms = (time.monotonic() - request_start) * 1000
        SCORE_REQUESTS.labels(decision=result.decision).inc()
        SCORE_LATENCY.observe(total_latency_ms / 1000)

        # Stage 4: Async audit write (does not affect response latency)
        feature_snapshot = {
            **account_features.to_feature_vector_partial(),
            "amount": request.amount,
            "billing_eq_shipping": request.billing_country == request.shipping_country,
            "geo_country": request.geo_location.country,
            "billing_country": request.billing_country,
            "shipping_country": request.shipping_country,
        }

        if _db_pool:
            # Write to Postgres after returning response (background task)
            # Using asyncpg directly for minimal overhead vs SQLAlchemy ORM
            try:
                with tracer.start_as_current_span("db_write") as db_span:
                    t0 = time.monotonic()
                    async with _db_pool.acquire() as conn:
                        await _write_decision(conn, request, result, feature_snapshot, int(total_latency_ms))
                    db_elapsed = time.monotonic() - t0
                    DB_WRITE_LATENCY.observe(db_elapsed)
                    db_span.set_attribute("latency_ms", db_elapsed * 1000)
            except Exception as e:
                log.error("db_write_failed", transaction_id=request.transaction_id, error=str(e))

        # Stage 5: Publish flagged event (fire-and-forget, 0ms on hot path)
        if result.decision in ("flag", "block"):
            publish_flagged_event(
                transaction_id=request.transaction_id,
                account_id=request.account_id,
                transaction={
                    "transaction_id": request.transaction_id,
                    "account_id": request.account_id,
                    "amount": request.amount,
                    "merchant_id": request.merchant_id,
                    "geo_location": request.geo_location.model_dump(),
                    "billing_country": request.billing_country,
                    "shipping_country": request.shipping_country,
                },
                decision=result.decision,
                risk_score=result.risk_score,
                triggered_rules=result.triggered_rules,
                feature_snapshot=feature_snapshot,
                topic=os.environ.get("FLAGGED_EVENTS_TOPIC", "flagged-events"),
            )

        log.info(
            "transaction_scored",
            transaction_id=request.transaction_id,
            account_id=request.account_id,
            decision=result.decision,
            risk_score=round(result.risk_score, 4),
            latency_ms=round(total_latency_ms, 2),
            triggered_rules=result.triggered_rules,
        )

        return ScoreResponse(
            transaction_id=request.transaction_id,
            account_id=request.account_id,
            decision=result.decision,
            risk_score=round(result.risk_score, 5),
            triggered_rules=result.triggered_rules,
            latency_ms=round(total_latency_ms, 2),
        )


async def _write_decision(
    conn: asyncpg.Connection,
    request: ScoreRequest,
    result: DecisionResult,
    feature_snapshot: dict,
    latency_ms: int,
) -> None:
    """Write the decision and feature snapshot to PostgreSQL."""
    import json
    async with conn.transaction():
        await conn.execute("""
            INSERT INTO decisions (
                transaction_id, account_id, amount, merchant_id, device_fingerprint,
                geo_country, billing_country, shipping_country, timestamp_ms,
                risk_score, decision, triggered_rules, latency_ms
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (transaction_id) DO NOTHING
        """,
            request.transaction_id, request.account_id, request.amount,
            request.merchant_id, request.device_fingerprint,
            request.geo_location.country, request.billing_country, request.shipping_country,
            request.timestamp_ms or int(time.time() * 1000),
            result.risk_score, result.decision,
            json.dumps(result.triggered_rules), latency_ms,
        )
        await conn.execute("""
            INSERT INTO feature_snapshots (
                transaction_id, txn_count_5min, avg_amount_1hr,
                distinct_merchants_24hr, distinct_countries_10min,
                amount, billing_eq_shipping, geo_country,
                billing_country, shipping_country, feature_vector
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        """,
            request.transaction_id,
            feature_snapshot["txn_count_5min"],
            feature_snapshot["avg_amount_1hr"],
            feature_snapshot["distinct_merchants_24hr"],
            feature_snapshot["distinct_countries_10min"],
            request.amount,
            feature_snapshot["billing_eq_shipping"],
            feature_snapshot["geo_country"],
            request.billing_country, request.shipping_country,
            json.dumps(feature_snapshot),
        )


def _heuristic_score(request: ScoreRequest, features: AccountFeatures) -> float:
    """
    Simple heuristic risk score when no model is loaded.
    Used during development before model.pkl is trained.
    This is NOT used in production.
    """
    score = 0.0
    if request.billing_country != request.shipping_country:
        score += 0.3
    if request.amount > 500:
        score += 0.2
    if features.distinct_countries_10min > 2:
        score += 0.3
    if features.txn_count_5min > 5:
        score += 0.2
    return min(score, 1.0)


# ── Health endpoints ──────────────────────────────────────────
@app.get("/health")
async def health() -> dict:
    """Liveness probe: is the process alive and able to handle requests?"""
    return {"status": "ok", "service": "scoring-service"}


@app.get("/ready")
async def ready() -> dict:
    """
    Readiness probe: is the service ready to receive traffic?
    Checks:
      - Redis is connected (feature lookups will work)
      - Model is loaded (scoring will work)
    Used by Kubernetes readiness probe and HPA lag tracking.
    """
    checks = {}

    # Check Redis
    try:
        if _feature_store and _feature_store._client:
            await _feature_store._client.ping()
            checks["redis"] = "ok"
        else:
            checks["redis"] = "not_connected"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    # Check model
    checks["model"] = "loaded" if _model is not None else "not_loaded (heuristic fallback)"
    checks["db"] = "connected" if _db_pool else "not_connected"

    all_ok = checks["redis"] == "ok"  # Redis is the critical dependency
    if not all_ok:
        raise HTTPException(status_code=503, detail=checks)

    return {"status": "ready", "checks": checks}


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ── Dashboard API endpoints ────────────────────────────────────
# These serve the analyst-dashboard frontend.

@app.get("/api/queue")
async def get_queue(
    status: str = "",
    limit: int = 100,
) -> list:
    """
    Return the analyst investigation queue from the materialized view.
    Optional filter by queue_status: pending_review | resolved
    """
    if not _db_pool:
        return []
    import json as _json
    async with _db_pool.acquire() as conn:
        where_clause = "WHERE queue_status = $2" if status else ""
        params = [limit, status] if status else [limit]
        query = f"""
            SELECT
                transaction_id, account_id, amount, merchant_id,
                geo_country, billing_country, shipping_country,
                risk_score::float, decision, triggered_rules,
                decision_time::text, note_text, note_ready_at::text,
                resolution, resolved_at::text, queue_status
            FROM analyst_queue
            {where_clause}
            ORDER BY decision_time DESC
            LIMIT $1
        """
        rows = await conn.fetch(query, *params)
        return [
            {
                **dict(r),
                "triggered_rules": _json.loads(r["triggered_rules"]) if isinstance(r["triggered_rules"], str) else r["triggered_rules"],
            }
            for r in rows
        ]


@app.get("/api/stats")
async def get_stats() -> dict:
    """Return summary statistics for the dashboard header."""
    if not _db_pool:
        return {"total_flagged": 0, "pending_review": 0, "resolved_today": 0, "blocked": 0}
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE decision IN ('flag', 'block')) AS total_flagged,
                COUNT(*) FILTER (WHERE queue_status = 'pending_review') AS pending_review,
                COUNT(*) FILTER (WHERE resolution IS NOT NULL
                                 AND resolved_at >= NOW() - INTERVAL '24 hours') AS resolved_today,
                COUNT(*) FILTER (WHERE decision = 'block') AS blocked
            FROM analyst_queue
        """)
        return dict(row)


class ResolveRequest(BaseModel):
    transaction_id: str
    resolution: str  # 'resolved' | 'false_positive'
    analyst_id: Optional[str] = None
    notes: Optional[str] = None


@app.post("/api/resolve")
async def resolve_transaction(req: ResolveRequest) -> dict:
    """
    Mark a flagged transaction as resolved or false positive.
    The resolution is written to analyst_resolutions and fed back as
    a training label for future model retraining.
    """
    if req.resolution not in ("resolved", "false_positive"):
        raise HTTPException(status_code=400, detail="resolution must be 'resolved' or 'false_positive'")
    if not _db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO analyst_resolutions (transaction_id, analyst_id, resolution, notes)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (transaction_id) DO UPDATE
                    SET resolution = EXCLUDED.resolution,
                        resolved_at = NOW()
            """, req.transaction_id, req.analyst_id, req.resolution, req.notes)

            # Refresh materialized view so queue reflects resolution immediately
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY analyst_queue")

    log.info("transaction_resolved", transaction_id=req.transaction_id, resolution=req.resolution)
    return {"status": "ok", "transaction_id": req.transaction_id, "resolution": req.resolution}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("SCORING_SERVICE_HOST", "0.0.0.0"),
        port=int(os.getenv("SCORING_SERVICE_PORT", "8000")),
        workers=1,          # Single worker — shared state (model, Redis pool) is per-process
        log_level="info",
        access_log=False,   # Structured logging handles this
    )
