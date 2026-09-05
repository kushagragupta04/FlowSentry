"""
pii_stripper.py — PII redaction before sending transaction data to Groq LLM.

CRITICAL REQUIREMENT: No raw PII may be sent to an external LLM provider.
This module implements the stripping/hashing contract for all fields
that could identify a real person or financial account.

Fields treated as PII:
  - account_id:          SHA-256 hashed → truncated to 12 hex chars
  - device_fingerprint:  Fully redacted (too identifying)
  - merchant_id:         Partially redacted (keep category, remove ID suffix)
  - geo_location:        Country retained, lat/lon removed

Fields that are NOT PII (retained for LLM context):
  - amount:              Retained (needed for risk explanation)
  - billing_country:     Retained (country-level, not address)
  - shipping_country:    Retained
  - decision:            Retained
  - risk_score:          Retained
  - triggered_rules:     Retained
  - window features:     Retained (aggregate statistics, not identifying)

Design note: We hash account_id rather than redacting it entirely because
the LLM note may mention "this account has shown suspicious behavior in
the last 5 minutes" — the analyst needs to know WHICH account without
seeing the raw account ID in the LLM call.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any


def _hash_id(value: str, length: int = 12) -> str:
    """
    SHA-256 hash a string, truncated to `length` hex characters.
    Deterministic: same input always produces same output.
    The analyst can look up the full account_id in Postgres using the full ID.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def _redact_merchant(merchant_id: str) -> str:
    """
    Redact merchant_id to category level only.
    'MERCHANT_0042' → 'MERCHANT_****'
    """
    # Keep prefix up to first underscore or digit run, redact the rest
    return re.sub(r"\d+$", "****", merchant_id)


def strip_pii(raw_event: dict) -> dict:
    """
    Produce a PII-stripped copy of a flagged transaction event
    suitable for sending to an external LLM provider.

    Args:
        raw_event: Full flagged event payload from Kafka (from publisher.py)

    Returns:
        Sanitized dict safe to include in an LLM prompt.
    """
    tx = raw_event.get("transaction", {})
    geo = tx.get("geo_location", {})

    return {
        # Account identifier: hashed (not raw)
        "account_ref": _hash_id(raw_event.get("account_id", "")),

        # Transaction amounts and countries are retained — needed for risk context
        "amount": tx.get("amount", 0),
        "billing_country": tx.get("billing_country", ""),
        "shipping_country": tx.get("shipping_country", ""),

        # Geo: country only (lat/lon removed)
        "geo_country": geo.get("country", ""),

        # Merchant: category prefix only
        "merchant_ref": _redact_merchant(tx.get("merchant_id", "")),

        # Device: fully redacted
        "device_ref": "[REDACTED]",

        # Decision context (needed for the LLM to explain its note)
        "decision": raw_event.get("decision", ""),
        "risk_score": raw_event.get("risk_score", 0),
        "triggered_rules": raw_event.get("triggered_rules", []),

        # Behavioral features (aggregate statistics, not PII)
        "features": {
            k: v for k, v in raw_event.get("feature_snapshot", {}).items()
            if k not in ("account_id", "transaction_id")
        },
    }
