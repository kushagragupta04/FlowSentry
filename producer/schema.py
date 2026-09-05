"""
schema.py — Avro schema loader and serializer factory for the transaction producer.

Uses Confluent Schema Registry for schema versioning. The schema is registered
on startup if not already present, ensuring that all consumers share the same
schema ID without silent drift.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Path to the canonical Avro schema definition
_SCHEMA_PATH = Path(__file__).parent.parent / "infra" / "schema-registry" / "transaction.avsc"

# Cached schema string (loaded once at module import)
_SCHEMA_STR: Optional[str] = None


def load_schema_str() -> str:
    """Load and cache the Avro schema string from disk."""
    global _SCHEMA_STR
    if _SCHEMA_STR is None:
        _SCHEMA_STR = _SCHEMA_PATH.read_text(encoding="utf-8")
    return _SCHEMA_STR


def get_schema_registry_client(url: str) -> SchemaRegistryClient:
    """Create a SchemaRegistryClient pointed at the given URL."""
    return SchemaRegistryClient({"url": url})


def register_schema(client: SchemaRegistryClient, subject: str) -> int:
    """
    Register the transaction schema under `subject` if not already registered.
    Returns the schema ID assigned by the registry.

    The subject naming convention is <topic>-value (Confluent standard).
    A schema that fails compatibility checks will raise an exception here,
    BEFORE any messages are produced — this is the schema guard we rely on.
    """
    schema_str = load_schema_str()
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = client.register_schema(subject, schema)
    return schema_id


def get_avro_serializer(client: SchemaRegistryClient) -> AvroSerializer:
    """
    Return an AvroSerializer that embeds the schema ID in the first 5 bytes
    of every message (Confluent wire format), enabling consumers to look up
    the schema from the registry without out-of-band coordination.
    """
    schema_str = load_schema_str()
    return AvroSerializer(
        client,
        schema_str,
        conf={
            "auto.register.schemas": True,   # register on first produce
            "use.latest.version": False,       # always validate against defined schema
        },
    )


def get_avro_deserializer(client: SchemaRegistryClient) -> AvroDeserializer:
    """Return an AvroDeserializer for consumers (e.g., integration tests)."""
    schema_str = load_schema_str()
    return AvroDeserializer(client, schema_str)


def transaction_to_dict(transaction: dict, ctx: SerializationContext) -> dict:  # noqa: ARG001
    """
    Identity transformation — the transaction dict is already in the correct
    shape for the Avro serializer. Passed as `to_dict` to AvroSerializer.
    """
    return transaction
