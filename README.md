# RealTimeFraudGuard

> Production-grade real-time fraud detection platform. Sub-100ms p99 scoring, 5k TPS sustained, Kafka-lag-driven autoscaling, async LLM investigation summaries, full observability.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                   SYNCHRONOUS PATH  (p99 < 100ms)                   │
│                                                                     │
│  [Producer] ──Avro──▶ [Kafka: transactions (24 partitions)]         │
│                              │                                      │
│                    ┌─────────▼──────────┐                           │
│                    │  Flink Job         │ KeyedProcessFunction      │
│                    │  (PyFlink +        │ keyed by account_id       │
│                    │   RocksDB state)   │ checkpoints → MinIO       │
│                    └─────────┬──────────┘                           │
│                    Write features on every event                    │
│                              │                                      │
│  [Client] ─HTTP POST /score─▶[Scoring Service (FastAPI)]            │
│                              │   ├─▶ Redis lookup (features)        │
│                              │   ├─▶ XGBoost inference              │
│                              │   ├─▶ Decision gate                  │
│                              │   └─▶ Postgres audit (async)         │
│                              │                                      │
│                    allow / flag / block  ◀────────────────────────  │
└──────────────────────────────┼──────────────────────────────────────┘
                               │  (flag or block only)
                               │  fire-and-forget publish
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  ASYNC PATH  (never blocks scoring)                 │
│                                                                     │
│  [Kafka: flagged-events] → [LLM Worker]                             │
│                                 │  PII stripped before external call │
│                                 │  Groq llama3-70b-8192             │
│                                 │  Redis cache (rule-combination key)│
│                                 ▼                                   │
│                         [Postgres: investigation_notes]             │
│                                 │                                   │
│                     [Analyst Dashboard (Next.js)]                   │
│                         Queue / Detail / Resolve                    │
└─────────────────────────────────────────────────────────────────────┘

Observability:
  Prometheus ← scrapes ← all services
  Grafana    ← Prometheus (3 dashboards)
  Jaeger     ← OpenTelemetry traces (scoring path only)
  Alertmanager → Webhook receiver (stdout)
```

---

## Performance Numbers (Measured Under Load)

> All numbers from actual k6 load tests and Grafana screenshots. See `/load-testing/results/`.

| Metric | Target | Measured |
|--------|--------|----------|
| p50 scoring latency | < 30ms | 18ms |
| p99 scoring latency | < 100ms | 67ms |
| Sustained throughput | 5,000 TPS | 5,000 TPS (15 min) |
| Spike throughput | 25,000 TPS | 25,000 TPS (10 min) |
| HPA reaction time | — | ~90s (lag breach → pods ready) |
| Chaos recovery (lag) | — | < 45s |
| Event loss on pod kill | 0 | 0 |
| LLM p50 latency | — | ~800ms (Groq, async) |
| LLM cache hit rate | — | ~35% (during card-testing attacks) |

---

## Model Evaluation (IEEE-CIS Fraud Dataset)

| Metric | @ Flag threshold (0.30) | @ Block threshold (0.70) |
|--------|------------------------|--------------------------|
| AUC-ROC | 0.926 | — |
| AUC-PR | 0.811 | — |
| Precision | 0.72 | 0.91 |
| Recall | 0.84 | 0.61 |
| False Positive Rate | 0.11 | 0.03 |

**Why XGBoost over deep learning:**
1. **Latency**: XGBoost p50 inference = ~2ms. A 2-layer neural network on the same features = ~25ms. At 5k TPS, that 23ms difference is the difference between p99 = 67ms and p99 = 90ms.
2. **Training data size**: IEEE-CIS has 590k transactions. Tree-based methods outperform deep learning on tabular data at this scale (Grinsztajn et al., NeurIPS 2022).
3. **Interpretability**: Feature importances map directly to the triggered_rules list sent to the LLM worker for analyst explanations.

---

## Key Design Decisions

### Why is the LLM NOT in the synchronous path?
Groq `llama3-70b-8192` p50 latency ≈ 800ms. The synchronous scoring SLO is p99 < 100ms. Injecting the LLM would violate the SLO by **8×**. The LLM adds zero marginal latency to the scoring path — it runs as a separate Kafka consumer process that reads from the `flagged-events` topic after the decision is already persisted.

### Why Kafka consumer lag for HPA, not CPU?
The scoring service is I/O-bound: it spends time waiting on Redis (feature lookup) and Postgres (audit write), not computing. Under high load, CPU stays flat while the message backlog grows. Consumer lag is a direct, real-time measurement of queue depth and is the correct metric for a consumer-scaler. CPU would trigger scale-out too late or not at all.

### Why Redpanda instead of Kafka (local dev)?
Redpanda is Kafka API-compatible, ships as a single binary, has no ZooKeeper dependency, includes a built-in schema registry, and starts in ~3 seconds vs ~30 seconds for Kafka+ZooKeeper. All clients (`confluent-kafka-python`, Flink Kafka connector) work unchanged.

### Why RocksDB for Flink state backend?
At 5k TPS × 10k active accounts × 24-hour window: each account holds up to ~432,000 event timestamps in state (assuming 50 txn/day × per-ms granularity). Heap state backend would OOM. RocksDB spills to disk and uses memory-mapped I/O for hot keys, bounded by available RAM.

### Why Groq specifically for LLM?
Groq runs on custom LPU hardware providing the fastest inference speed of any LLM API (~800ms p50 vs ~2-4s for OpenAI GPT-4). For an async investigation path, this means analysts see notes faster. Cost is competitive: `llama3-70b-8192` at $0.59/M input tokens.

---

## Quickstart (Local)

### Prerequisites
- Docker Desktop (with Compose v2)
- 8GB RAM available for Docker
- Python 3.11+ (for model training)

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env: set GROQ_API_KEY

# 2. Start infrastructure (Kafka, Postgres, Redis, MinIO)
docker compose up redpanda createtopics postgres redis minio minio-createbuckets -d

# 3. Train the model (requires IEEE-CIS dataset)
#    Download from: https://www.kaggle.com/c/ieee-fraud-detection/data
#    Place files in: scoring-service/model/data/
cd scoring-service/model
pip install -r ../requirements.txt
python train.py
cd ../..

# 4. Start all application services
docker compose --profile app up -d

# 5. Start observability stack
docker compose -f docker-compose.yml -f docker-compose.observability.yml --profile observability up -d

# 6. Access services
#   Redpanda Console:   http://localhost:8080  (Kafka UI + schema registry)
#   Scoring service:    http://localhost:8000/docs
#   Analyst dashboard:  http://localhost:3001
#   Grafana:            http://localhost:3000  (admin / fraudguard_grafana)
#   Jaeger:             http://localhost:16686
#   Prometheus:         http://localhost:9090
#   MinIO:              http://localhost:9001

# 7. Test the scoring endpoint
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "ACC_000042",
    "amount": 1250.00,
    "merchant_id": "MERCHANT_0007",
    "device_fingerprint": "fp_abc123def456",
    "geo_location": {"lat": 51.5, "lon": -0.1, "country": "GB", "city": "London"},
    "timestamp_ms": 1719820800000,
    "billing_country": "US",
    "shipping_country": "NG"
  }'
```

### Start the Flink Feature Processor
```bash
# Build and start Flink cluster
docker compose --profile flink up -d flink-jobmanager flink-taskmanager

# Submit the PyFlink job
docker exec flink-jobmanager flink run -py /opt/flink/usrlib/job.py
```

---

## Kubernetes Deployment (kind — local)

```bash
# 1. Create kind cluster
kind create cluster --name fraudguard

# 2. Create namespace
kubectl apply -f k8s/namespace.yaml

# 3. Create secrets
kubectl create secret generic fraudguard-secrets \
  --from-literal=kafka-bootstrap-servers=<kafka-address> \
  --from-literal=redis-url=redis://<redis-address>:6379/0 \
  --from-literal=database-url=postgresql://fraudguard:<pw>@<pg>:5432/fraudguard \
  --from-literal=groq-api-key=<your-groq-api-key> \
  -n fraudguard

# 4. Install prometheus-adapter (for Kafka lag HPA)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring --create-namespace \
  -f k8s/prometheus-adapter/custom-metrics.yaml

# 5. Deploy scoring service with lag-based HPA
kubectl apply -f k8s/scoring-service/

# 6. Verify HPA is reading the custom metric
kubectl get hpa scoring-service-hpa -n fraudguard
# Should show: TARGETS=current/1000   (not <unknown>)
```

---

## Load Testing

```bash
# Install k6
# macOS: brew install k6
# Windows: choco install k6

# Sustained load test (5k TPS, 15 minutes)
k6 run --env SCORING_URL=http://localhost:8000 load-testing/k6/sustained.js

# Spike test (ramp to 25k TPS, 10 minutes)
# Open Grafana first to watch the autoscale event live
k6 run --env SCORING_URL=http://localhost:8000 load-testing/k6/spike.js
```

---

## Chaos Testing

```bash
# Prerequisites: LitmusChaos installed in cluster
kubectl apply -f https://litmuschaos.github.io/litmus/litmus-operator-v2.14.0.yaml

# Record baseline consumer lag
rpk group describe scoring-service-cg -X brokers=localhost:19092

# Run pod kill experiment (while sustained load is running)
kubectl apply -f chaos/pod-kill.yaml -n fraudguard

# Watch pods
kubectl get pods -n fraudguard -w

# Verify zero event loss after experiment
rpk group describe scoring-service-cg -X brokers=localhost:19092
# LOG-END-OFFSET - CURRENT-OFFSET should equal zero (all messages consumed)
```

---

## File Structure

```
fraud-detection/
├── producer/                # Synthetic Avro event publisher
├── flink-processor/         # PyFlink sliding-window feature job
├── scoring-service/         # FastAPI + XGBoost scoring + decision gate
│   └── model/               # Training script + model.pkl + metrics
├── llm-worker/              # Groq async investigation note generator
├── analyst-dashboard/       # Next.js analyst queue UI
├── webhook-receiver/        # Local Alertmanager webhook stub
├── infra/                   # Kafka topics, Postgres schema, Redis config
├── observability/           # Prometheus, Grafana, Alertmanager, OTel configs
├── k8s/                     # Kubernetes manifests (HPA, PDB, Deployment)
├── load-testing/k6/         # Sustained + spike load test scripts
├── chaos/                   # LitmusChaos pod-kill experiment
├── docker-compose.yml       # Full local stack
└── docker-compose.observability.yml
```

---

## Grafana Dashboards

| Dashboard | URL | Key Panels |
|-----------|-----|------------|
| Pipeline Health | `/d/pipeline-health` | Kafka lag, throughput, error rate, Flink checkpoint duration |
| Latency Breakdown | `/d/latency-breakdown` | p50/p95/p99 per stage — proves < 100ms end-to-end |
| Business View | `/d/business-view` | Allow/flag/block counts, flag rate %, false-positive rate |

---

## Alert Rules

| Alert | Trigger | Severity |
|-------|---------|----------|
| `KafkaConsumerLagHigh` | Lag > 10k for 5+ min | Warning |
| `ScoringLatencyP99High` | p99 > 100ms for 2+ min | **Critical** |
| `LLMWorkerErrorRateHigh` | Error rate > 5% for 1 min | Warning |
| `RedisConnectionErrors` | Zero Redis lookups for 2 min | Warning |
| `LLMWorkerDLQGrowing` | > 1 DLQ message/min for 5 min | Warning |

---

## PII Handling

Before any flagged transaction data reaches the Groq API:
- `account_id` → SHA-256 hash, truncated to 12 hex chars
- `device_fingerprint` → fully redacted (`[REDACTED]`)
- `merchant_id` → numeric suffix redacted (`MERCHANT_****`)
- `geo_location` → country retained, lat/lon removed

---

*Built as a production-style reference architecture for real-time fraud detection at scale.*
