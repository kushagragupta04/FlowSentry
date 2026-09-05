"""
decision.py — Decision gate for the fraud scoring service.

Takes an XGBoost risk score (0–1) and applies:
  1. Deterministic business rules (pre-model, fast, interpretable)
  2. Risk score thresholds

Returns: decision ('allow' | 'flag' | 'block'), risk_score, triggered_rules

Justification for deterministic rules BEFORE model scoring:
  - Some fraud signals are categorical hard-blocks that don't need ML
    (e.g., OFAC-listed countries in production). These save an inference call.
  - Keeping rules separate from the model makes them auditable and
    independently testable — critical for compliance and model governance.

Threshold values (configurable via environment variables):
  - allow:  risk_score < 0.30
  - flag:   0.30 ≤ risk_score < 0.70
  - block:  risk_score ≥ 0.70

These thresholds are starting points. In production, set them from
the precision/recall tradeoff on the held-out validation set for the
target false-positive rate your business can tolerate.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Tuple

# Decision threshold config — read once at module load
THRESHOLD_FLAG = float(os.getenv("THRESHOLD_FLAG", "0.30"))
THRESHOLD_BLOCK = float(os.getenv("THRESHOLD_BLOCK", "0.70"))

# Business rule thresholds
RULE_MAX_COUNTRIES_10MIN = int(os.getenv("RULE_MAX_COUNTRIES_10MIN", "3"))
RULE_COUNTRY_MISMATCH_MIN_AMOUNT = float(os.getenv("RULE_COUNTRY_MISMATCH_MIN_AMOUNT", "500.0"))

# Valid decisions (string literals used in Postgres and Kafka)
DECISION_ALLOW = "allow"
DECISION_FLAG  = "flag"
DECISION_BLOCK = "block"


@dataclass
class DecisionResult:
    """Complete decision output for a single transaction."""
    decision: str                          # 'allow' | 'flag' | 'block'
    risk_score: float                      # raw XGBoost output (0–1)
    triggered_rules: List[str] = field(default_factory=list)
    # Hard-block overrides model score (True if a business rule forced block)
    rule_override: bool = False


def apply_business_rules(
    amount: float,
    billing_country: str,
    shipping_country: str,
    geo_country: str,
    distinct_countries_10min: int,
    risk_score: float,
) -> Tuple[str | None, List[str]]:
    """
    Apply deterministic business rules that can override or augment the model score.

    Returns:
        (forced_decision or None, list of triggered rule names)
        If forced_decision is not None, it overrides the model threshold.
    """
    triggered = []
    forced_decision = None

    # Rule 1: Country mismatch with high-value transaction
    # Rationale: Shipping to a different country from billing on a $500+ transaction
    # is a strong fraud signal that should always at minimum flag the transaction.
    if billing_country != shipping_country and amount >= RULE_COUNTRY_MISMATCH_MIN_AMOUNT:
        triggered.append("country_mismatch_high_value")
        if forced_decision is None:
            forced_decision = DECISION_FLAG

    # Rule 2: Rapid multi-country activity (velocity check)
    # Rationale: Seeing transactions from >3 distinct countries in 10 minutes
    # is physically impossible for a legitimate user — hard block.
    if distinct_countries_10min > RULE_MAX_COUNTRIES_10MIN:
        triggered.append("rapid_multi_country_velocity")
        forced_decision = DECISION_BLOCK  # Hard block, overrides any prior forced decision

    # Rule 3: Geo-country ≠ billing country (softer signal)
    if geo_country and geo_country != billing_country:
        triggered.append("geo_billing_country_mismatch")
        # Does not force a decision — raises the effective score in model

    return forced_decision, triggered


def make_decision(
    risk_score: float,
    amount: float,
    billing_country: str,
    shipping_country: str,
    geo_country: str,
    distinct_countries_10min: int,
) -> DecisionResult:
    """
    Apply business rules + risk score thresholds to produce a final decision.

    Order of precedence:
      1. Hard-block business rules (rule_override=True)
      2. Risk score thresholds (with rule boosting)
    """
    forced, triggered_rules = apply_business_rules(
        amount=amount,
        billing_country=billing_country,
        shipping_country=shipping_country,
        geo_country=geo_country,
        distinct_countries_10min=distinct_countries_10min,
        risk_score=risk_score,
    )

    # If a business rule forced a hard-block, apply it regardless of score
    if forced == DECISION_BLOCK:
        return DecisionResult(
            decision=DECISION_BLOCK,
            risk_score=risk_score,
            triggered_rules=triggered_rules,
            rule_override=True,
        )

    # Apply model-score thresholds
    if risk_score >= THRESHOLD_BLOCK:
        decision = DECISION_BLOCK
    elif risk_score >= THRESHOLD_FLAG:
        decision = DECISION_FLAG
    else:
        decision = DECISION_ALLOW

    # A business rule can promote 'allow' to 'flag' but not demote 'block' to 'flag'
    if forced == DECISION_FLAG and decision == DECISION_ALLOW:
        decision = DECISION_FLAG
        rule_override = True
    else:
        rule_override = False

    return DecisionResult(
        decision=decision,
        risk_score=risk_score,
        triggered_rules=triggered_rules,
        rule_override=rule_override,
    )
