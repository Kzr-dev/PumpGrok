"""Required failures are KILL. The score does not override them."""

from __future__ import annotations

from decimal import Decimal

from pumpgrok.config.settings import SafetyThresholds
from pumpgrok.tools.safety.checks import CheckResult
from pumpgrok.tools.safety.checks.readers import check_deployer_history, check_liquidity_depth
from pumpgrok.tools.safety.score_report import recommend_verdict


def _warn(check_id: str = "lp_status") -> CheckResult:
    return CheckResult(
        id=check_id,  # type: ignore[arg-type]
        status="warn",
        value=1,
        threshold=1,
        weight=Decimal("0.1"),
        reason="residual",
    )


def test_depth_below_the_floor_is_kill() -> None:
    thresholds = SafetyThresholds()
    row = check_liquidity_depth({"liquidity_lamports": 11}, 10, thresholds)
    assert row.status == "fail"
    assert row.value == 1.1
    assert thresholds.liquidity_multiple_min == 10
    assert recommend_verdict([row], thresholds) == "KILL"


def test_serial_deployer_is_kill() -> None:
    row = check_deployer_history({"is_known_bad": False, "rugs": 0, "tokens_seen": 100})
    assert row.status == "fail"
    assert recommend_verdict([row], SafetyThresholds()) == "KILL"


def test_two_signatures_are_not_a_serial_fail() -> None:
    row = check_deployer_history({"is_known_bad": False, "rugs": 0, "tokens_seen": 2})
    assert row.status == "pass"


def test_warns_still_use_the_score_ladder() -> None:
    thresholds = SafetyThresholds()
    assert recommend_verdict([_warn()], thresholds) == "CLEAR"
    conditional = [
        _warn("lp_status"),
        _warn("token2022_extensions"),
        _warn("lp_status"),
        _warn("lp_status"),
    ]
    assert recommend_verdict(conditional, thresholds) == "CONDITIONAL"
    killed = [_warn() for _ in range(7)]
    assert recommend_verdict(killed, thresholds) == "KILL"
