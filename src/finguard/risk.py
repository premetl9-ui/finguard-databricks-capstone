from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskInputs:
    amount_deviation: float = 0.0
    recent_txn_count: int = 0
    is_new_destination: bool = False
    is_unusual_type: bool = False
    is_international: bool = False
    is_high_value: bool = False


@dataclass(frozen=True)
class RiskResult:
    score: int
    level: str
    reasons: tuple[str, ...]


def risk_level(score: int) -> str:
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "HIGH"
    if score >= 30:
        return "MEDIUM"
    return "LOW"


def score_transaction(inputs: RiskInputs) -> RiskResult:
    """Transparent rule-based model from the FinGuard proposal.

    Rules:
    - amount much larger than customer average: +30
    - multiple transactions in a short time window: +20
    - new destination/account: +15
    - unusual transaction type: +15
    - international transaction: +10
    - high-value transaction: +10
    """
    score = 0
    reasons: list[str] = []

    if inputs.amount_deviation >= 3.0:
        score += 30
        reasons.append("amount >= 3x customer average")
    if inputs.recent_txn_count >= 3:
        score += 20
        reasons.append("multiple transactions in short window")
    if inputs.is_new_destination:
        score += 15
        reasons.append("new destination/account")
    if inputs.is_unusual_type:
        score += 15
        reasons.append("unusual transaction type")
    if inputs.is_international:
        score += 10
        reasons.append("international transaction")
    if inputs.is_high_value:
        score += 10
        reasons.append("high-value transaction")

    score = min(score, 100)
    return RiskResult(score=score, level=risk_level(score), reasons=tuple(reasons))
