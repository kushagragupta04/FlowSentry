"""
telemetry.py — OpenTelemetry setup for the scoring service.

Configures distributed tracing with OTLP export to Jaeger/Grafana Tempo.
Creates named spans for each stage of the synchronous scoring path:
  - redis_lookup    (feature retrieval)
  - model_inference (XGBoost predict)
  - decision_gate   (threshold + rules)
  - db_write        (Postgres audit record)

This attribution is what proves the p99 < 100ms claim in Jaeger:
each stage's contribution to the total latency is visible in the trace.
"""

from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


def setup_telemetry(app=None) -> trace.Tracer:
    """
    Initialize OpenTelemetry. Call once at application startup.

    If OTEL_EXPORTER_OTLP_ENDPOINT is set, exports to that endpoint (Jaeger/Tempo).
    Falls back to console exporter if endpoint is not configured.
    """
    service_name = os.getenv("OTEL_SERVICE_NAME", "scoring-service")
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

    resource = Resource.create({
        "service.name": service_name,
        "service.version": os.getenv("SERVICE_VERSION", "1.0.0"),
        "deployment.environment": os.getenv("ENVIRONMENT", "local"),
    })

    provider = TracerProvider(resource=resource)

    if otlp_endpoint:
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    else:
        exporter = ConsoleSpanExporter()

    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Auto-instrument FastAPI routes
    if app is not None:
        FastAPIInstrumentor.instrument_app(app)

    return trace.get_tracer(service_name)


def get_tracer() -> trace.Tracer:
    """Get the configured tracer. Call after setup_telemetry()."""
    return trace.get_tracer(os.getenv("OTEL_SERVICE_NAME", "scoring-service"))
