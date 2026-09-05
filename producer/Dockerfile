# ── Build stage ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

# confluent-kafka requires librdkafka headers at build time
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY producer/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# librdkafka runtime dependency
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --no-create-home --shell /bin/false producer

WORKDIR /app
COPY --from=builder /install /usr/local
COPY --chown=producer:producer producer /app/
COPY --chown=producer:producer infra/schema-registry/transaction.avsc /infra/schema-registry/transaction.avsc

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

USER producer

EXPOSE 8001

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/metrics')"

CMD ["python", "main.py"]
