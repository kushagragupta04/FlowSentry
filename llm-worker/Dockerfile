FROM python:3.11-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential librdkafka-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 libpq5 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --no-create-home --shell /bin/false llmworker
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --chown=llmworker:llmworker . /app/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
USER llmworker
EXPOSE 8003
CMD ["python", "worker.py"]
