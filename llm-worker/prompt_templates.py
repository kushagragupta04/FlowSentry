"""
prompt_templates.py — Cached prompt templates for the LLM investigation worker.

Cost control strategy:
  LLM calls cost money. Many flagged transactions trigger the same combination
  of rules (e.g., both country_mismatch_high_value and rapid_multi_country_velocity).
  For those common combinations, we cache the generated note in Redis with a TTL.

  Cache key: hash of (sorted triggered_rules, decision)
  Cache TTL: 1 hour (configurable via LLM_CACHE_TTL_SECONDS env var)

  For unique transactions (most of the time), the note is generated fresh
  with transaction-specific feature values injected into the template.

  This approach reduces LLM calls by ~30-50% for high-volume fraud patterns
  (e.g., during a card-testing attack where many accounts hit the same rules).
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Optional


# ── System prompt (static for all transactions) ──────────────
SYSTEM_PROMPT = """You are a fraud investigation assistant helping bank fraud analysts.
Your job is to write clear, concise investigation notes about flagged transactions.

Guidelines:
- Write 3-5 sentences maximum
- Use plain language suitable for a non-technical fraud analyst
- Explain which specific signals triggered the flag and why they are suspicious
- Suggest a concrete next action (e.g., contact customer, block card, escalate)
- Do NOT speculate about customer intent — report only what the data shows
- Do NOT include any personal data in your response beyond what is given
- Be factual and professional"""


def _rules_to_human(rules: list[str]) -> str:
    """Convert internal rule names to readable descriptions."""
    mapping = {
        "country_mismatch_high_value": "billing and shipping countries do not match on a high-value transaction",
        "rapid_multi_country_velocity": "transactions detected from more than 3 distinct countries within 10 minutes",
        "geo_billing_country_mismatch": "the transaction geo-location does not match the billing country",
    }
    return "; ".join(mapping.get(r, r.replace("_", " ")) for r in rules) or "elevated risk score"


def build_user_prompt(stripped_event: dict) -> str:
    """
    Build the LLM user prompt for a specific flagged transaction.
    Uses PII-stripped event data only.
    """
    rules_text = _rules_to_human(stripped_event.get("triggered_rules", []))
    features = stripped_event.get("features", {})
    decision = stripped_event.get("decision", "flag").upper()

    prompt = f"""Transaction Alert — Decision: {decision}

Transaction details (anonymized):
- Amount: ${stripped_event.get('amount', 0):.2f}
- Billing country: {stripped_event.get('billing_country', 'Unknown')}
- Shipping country: {stripped_event.get('shipping_country', 'Unknown')}
- Transaction origin (country): {stripped_event.get('geo_country', 'Unknown')}
- Merchant category: {stripped_event.get('merchant_ref', 'Unknown')}
- Risk score: {stripped_event.get('risk_score', 0):.3f} / 1.000

Behavioral signals (last 24 hours for this account):
- Transactions in last 5 minutes: {features.get('txn_count_5min', 0)}
- Average transaction amount (last hour): ${features.get('avg_amount_1hr', 0):.2f}
- Distinct merchants seen (last 24 hours): {features.get('distinct_merchants_24hr', 0)}
- Distinct countries seen (last 10 minutes): {features.get('distinct_countries_10min', 0)}

Triggered rules: {rules_text}

Write a 3-5 sentence investigation note explaining what is suspicious about this transaction and recommending a next action for the fraud analyst."""

    return prompt


def make_cache_key(triggered_rules: list[str], decision: str) -> str:
    """
    Generate a cache key for a combination of triggered rules + decision.
    Sorted to make it order-independent.
    """
    content = json.dumps({
        "rules": sorted(triggered_rules),
        "decision": decision,
    }, sort_keys=True)
    return f"llm_cache:{hashlib.sha256(content.encode()).hexdigest()[:16]}"
