"""Exploratory paper keeps a short hard list and ranks the rest."""

from __future__ import annotations

from decimal import Decimal

from pumpgrok.config.settings import SafetyThresholds
from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda, curve_token_accounts
from pumpgrok.tools.safety.checks import CheckResult
from pumpgrok.tools.safety.checks.readers import (
    check_deployer_history,
    check_token2022_extensions,
    check_top10_concentration,
)
from pumpgrok.tools.safety.score_report import (
    ScoreReportIn,
    _apply_exploratory,
    _Readings,
    exploratory_features,
    recommend_verdict,
)

MINT = "So11111111111111111111111111111111111111112"


def _row(check_id: str, status: str, reason: str, value: object = 1) -> CheckResult:
    return CheckResult(
        id=check_id,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        value=value,  # type: ignore[arg-type]
        threshold=1,
        weight=Decimal("0.1"),
        reason=reason,
    )


def test_top10_fail_stays_a_kill_in_the_standard_profile() -> None:
    row = check_top10_concentration({"top10_share": "1"}, SafetyThresholds())
    assert row.status == "fail"
    assert recommend_verdict([row], SafetyThresholds()) == "KILL"


def test_exploratory_records_top10_and_deployer_without_killing() -> None:
    top = check_top10_concentration({"top10_share": "0.9"}, SafetyThresholds())
    serial = check_deployer_history({"is_known_bad": True, "rugs": 0, "tokens_seen": 12})
    rows = _apply_exploratory([top, serial], None)
    assert all(row.status == "warn" and str(row.reason).startswith("advisory:") for row in rows)
    assert recommend_verdict(rows, SafetyThresholds(), exploratory=True) == "CLEAR"


def test_exploratory_still_kills_a_live_mint_and_an_unpriceable_curve() -> None:
    live = _row("mint_authority", "fail", "mint authority is live and can dilute holders")
    assert recommend_verdict([live], SafetyThresholds(), exploratory=True) == "KILL"
    graduated = _row("lp_status", "warn", "graduated", value="graduated")
    priced = _apply_exploratory([graduated], {"graduated": True})
    assert priced[0].status == "fail"
    assert recommend_verdict(priced, SafetyThresholds(), exploratory=True) == "KILL"
    empty = _row(
        "liquidity_depth", "fail", "no measurable liquidity for the intended size", value=0
    )
    kept = _apply_exploratory([empty], None)
    assert kept[0].status == "fail"
    # The warning sentence is not evidence. A metadata-only mint stays a warn.
    cloud4 = [
        {
            "extension": "metadataPointer",
            "state": {
                "authority": None,
                "metadataAddress": "7Rc161As4QWvRicpK4wJBH9SngXPQPeP9DHf3yhXpump",
            },
        },
        {
            "extension": "tokenMetadata",
            "state": {
                "additionalMetadata": [],
                "mint": "7Rc161As4QWvRicpK4wJBH9SngXPQPeP9DHf3yhXpump",
                "name": "Dragon cat",
                "symbol": "Dragon cat",
                "updateAuthority": None,
                "uri": "https://ipfs.io/ipfs/bafkreibgttowguavxu",
            },
        },
    ]
    ordinary = check_token2022_extensions(
        {"extensions": cloud4},
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert ordinary.status == "warn"
    assert "review transfer hooks" not in ordinary.reason
    assert recommend_verdict([ordinary], SafetyThresholds(), exploratory=True) == "CLEAR"


def test_a_thin_but_priceable_curve_is_advisory() -> None:
    thin = _row("liquidity_depth", "fail", "liquidity is 2.0x the intended size", value=2.0)
    rows = _apply_exploratory([thin], None)
    assert rows[0].status == "warn"
    assert recommend_verdict(rows, SafetyThresholds(), exploratory=True) == "CLEAR"


def test_many_advisories_do_not_score_kill_in_exploratory() -> None:
    warns = [_row("lp_status", "warn", "residual") for _ in range(8)]
    assert recommend_verdict(warns, SafetyThresholds()) == "KILL"
    assert recommend_verdict(warns, SafetyThresholds(), exploratory=True) == "CLEAR"


def test_rank_prefers_a_less_concentrated_curve() -> None:
    concentrated = ScoreReportIn(mint=MINT, curve_account=MINT, intended_size_lamports=200_000_000)
    readings = _Readings(
        auth=None,
        lp=None,
        meta=None,
        top={"top10_share": "0.8"},
        dep={"tokens_seen": 1, "is_known_bad": False},
        liq={"liquidity_lamports": 30_000_000_000, "curve_progress": "0.05"},
    )
    crowded = exploratory_features(concentrated, readings)
    spread = exploratory_features(
        concentrated,
        _Readings(
            auth=None,
            lp=None,
            meta=None,
            top={"top10_share": "0.1"},
            dep={"tokens_seen": 1, "is_known_bad": False},
            liq={"liquidity_lamports": 30_000_000_000, "curve_progress": "0.05"},
        ),
    )
    assert Decimal(str(spread["score"])) > Decimal(str(crowded["score"]))
    assert spread["buy_velocity"] is None


def _ext(meta: dict[str, object]) -> str:
    row = check_token2022_extensions(meta, exploratory=True, transfer_tax_hard_fail_pct=10.0)
    assert row.status == "fail"
    return row.reason


def test_cloud4_metadata_extensions_stay_advisory() -> None:
    """The cloud4 recording's only extension pair. It is not a sell block."""
    recorded = [
        {
            "extension": "metadataPointer",
            "state": {
                "authority": None,
                "metadataAddress": "7Rc161As4QWvRicpK4wJBH9SngXPQPeP9DHf3yhXpump",
            },
        },
        {
            "extension": "tokenMetadata",
            "state": {
                "additionalMetadata": [],
                "mint": "7Rc161As4QWvRicpK4wJBH9SngXPQPeP9DHf3yhXpump",
                "name": "Dragon cat",
                "symbol": "Dragon cat",
                "updateAuthority": None,
                "uri": "https://ipfs.io/ipfs/bafkreibgttowguavxu",
            },
        },
    ]
    row = check_token2022_extensions(
        {"extension_details": recorded},
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    # extension_details are raw dicts with an extension key, same as the RPC payload.
    assert row.status == "warn"
    prose = check_token2022_extensions(
        {"extensions": ["review transfer hooks"]},
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert prose.status == "warn"


def test_sell_unsafe_extensions_kill_and_a_small_fee_does_not() -> None:
    assert "transfer hook" in _ext(
        {"extensions": [{"extension": "transferHook", "state": {"programId": "Hook111"}}]}
    )
    assert "non-transferable" in _ext(
        {"extensions": [{"extension": "nonTransferable", "state": {}}]}
    )
    assert "permanent delegate" in _ext(
        {"extensions": [{"extension": "permanentDelegate", "state": {"delegate": "Seize111"}}]}
    )
    idle_hook = check_token2022_extensions(
        {"extensions": [{"extension": "transferHook", "state": {"programId": None}}]},
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert idle_hook.status == "warn"
    small_fee = check_token2022_extensions(
        {
            "extensions": [
                {
                    "extension": "transferFeeConfig",
                    "state": {"newerTransferFee": {"transferFeeBasisPoints": 50}},
                }
            ]
        },
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert small_fee.status == "warn"
    assert "exceeds" in _ext(
        {
            "extensions": [
                {
                    "extension": "transferFeeConfig",
                    "state": {"newerTransferFee": {"transferFeeBasisPoints": 1500}},
                }
            ]
        }
    )


def test_a_normalized_fee_record_uses_the_settings_bound() -> None:
    row = check_token2022_extensions(
        {"extension_details": [{"name": "transferFeeConfig", "fee_bps": 2000, "program": None}]},
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert row.status == "fail"
    quiet = check_token2022_extensions(
        {
            "extensions": [
                {
                    "extension": "transferFeeConfig",
                    "state": {"transferFeeBasisPoints": "100"},
                }
            ]
        },
        exploratory=True,
        transfer_tax_hard_fail_pct=10.0,
    )
    assert quiet.status == "warn"


def test_curve_inventory_is_a_different_account_from_the_curve_pda() -> None:
    curve = bonding_curve_pda(MINT)
    accounts = curve_token_accounts(curve, MINT)
    assert len(accounts) == 2
    assert curve not in accounts
    assert len(set(accounts)) == 2
