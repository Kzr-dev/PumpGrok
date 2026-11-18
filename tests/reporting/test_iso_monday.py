"""Calendar edges of the weekly window."""

from pumpgrok.config.settings import SafetyThresholds
from pumpgrok.reporting.weekly import _iso_monday
from pumpgrok.tools.safety.checks.readers import check_liquidity_depth


def test_iso_monday_crosses_a_month_and_a_year() -> None:
    new_year = _iso_monday("2026-01-01")
    assert new_year < "2026-01-01"
    assert _iso_monday(new_year) == new_year
    march = _iso_monday("2026-03-01")
    assert _iso_monday(march) == march


def test_liquidity_warns_between_the_floor_and_the_recommended_multiple() -> None:
    thresholds = SafetyThresholds()
    warn = check_liquidity_depth({"liquidity_lamports": 150}, 10, thresholds)
    clear = check_liquidity_depth({"liquidity_lamports": 300}, 10, thresholds)
    vacuous = check_liquidity_depth({"liquidity_lamports": 10}, 0, thresholds)
    assert warn.status == "warn"
    assert clear.status == "pass"
    assert vacuous.status == "pass"
