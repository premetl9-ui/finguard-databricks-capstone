import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from finguard.risk import RiskInputs, risk_level, score_transaction


def test_risk_bands():
    assert risk_level(0) == "LOW"
    assert risk_level(29) == "LOW"
    assert risk_level(30) == "MEDIUM"
    assert risk_level(59) == "MEDIUM"
    assert risk_level(60) == "HIGH"
    assert risk_level(79) == "HIGH"
    assert risk_level(80) == "CRITICAL"
    assert risk_level(100) == "CRITICAL"


def test_all_rules_cap_at_100():
    result = score_transaction(
        RiskInputs(
            amount_deviation=5.0,
            recent_txn_count=5,
            is_new_destination=True,
            is_unusual_type=True,
            is_international=True,
            is_high_value=True,
        )
    )
    assert result.score == 100
    assert result.level == "CRITICAL"
    assert len(result.reasons) == 6


def test_high_risk_example():
    result = score_transaction(
        RiskInputs(
            amount_deviation=4.2,
            recent_txn_count=4,
            is_new_destination=True,
        )
    )
    assert result.score == 65
    assert result.level == "HIGH"


def test_normal_transaction_low_risk():
    result = score_transaction(RiskInputs(amount_deviation=1.1, recent_txn_count=1))
    assert result.score == 0
    assert result.level == "LOW"
    assert result.reasons == ()
