"""A $100 book caps a 20% trade near $20 and trips the 5% halt near $5."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from pumpgrok.config.settings import SafetyThresholds, load_settings
from pumpgrok.core.ids import IdFactory
from pumpgrok.core.risk_budget import RiskBudget
from pumpgrok.ledger.store import LedgerStore
from pumpgrok.replay.clock import FixtureClock, format_ts_utc
from pumpgrok.replay.rng import make_rng
from pumpgrok.risk.engine import RiskEngine
from pumpgrok.risk.fast_audit import finalize_report
from pumpgrok.risk.limits import EngineLimits
from pumpgrok.risk.session_book import (
    BookUnresolved,
    apply_book,
    book_from_usd,
    budget_for_book,
    config_hash,
    locked_book,
    record_blind_acceptance,
    record_session_start,
)
from pumpgrok.supervisor.live_paper import trade_size_lamports
from pumpgrok.tools import ensure_paper_genesis
from pumpgrok.tools.safety.checks import CHECK_IDS

SOL_USD = Decimal("200")
BOOK_USD = Decimal("100")
# $100 / $200 = 0.5 SOL.
EQUITY = 500_000_000
# 5% of $100 is $5, which is 0.025 SOL at $200.
LOSS = 25_000_000
MINT = "So11111111111111111111111111111111111111112"


def _quote() -> dict[str, object]:
    return {
        "ok": True,
        "price_out_ui_per_in_ui": "200",
        "in_amount_raw": 1_000_000_000,
        "out_amount_raw": 200_000_000,
        "input_decimals": 9,
        "output_decimals": 6,
    }


def _budget() -> RiskBudget:
    return RiskBudget(
        paper_equity_usd=BOOK_USD,
        max_trade_fraction=Decimal("0.20"),
        max_token_fraction=Decimal("0.20"),
    )


def test_hundred_dollar_book_caps_the_trade_at_twenty_dollars() -> None:
    book = book_from_usd(_budget(), _quote())
    assert book.equity_lamports == EQUITY
    assert book.sol_usd == SOL_USD
    size = trade_size_lamports(book.equity_lamports, _budget(), EngineLimits())
    size_usd = Decimal(size) / Decimal(1_000_000_000) * SOL_USD
    assert size_usd == Decimal("20")
    assert size == 100_000_000


def test_missing_quote_refuses_the_book() -> None:
    with pytest.raises(BookUnresolved, match="not resolved"):
        book_from_usd(_budget(), {"ok": False, "code": "data_stale"})


def test_five_percent_halt_fires_at_five_dollars(tmp_path: Path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    book = book_from_usd(_budget(), _quote())
    budget = apply_book(store, _budget(), book)
    clock = FixtureClock(1_700_000_000)
    ids = IdFactory(clock, make_rng(1))
    store.append(
        ts_utc=format_ts_utc(clock.now()),
        source="paper",
        correlation_id="test",
        idempotency_key="fill-under",
        type="FillRecorded",
        payload={
            "source": "paper",
            "order_id": "ord-under",
            "decision_id": ids.ulid(),
            "side": "sell",
            "mint": MINT,
            "filled_lamports": 0,
            "filled_units": 0,
            "filled_fraction": "1",
            "price_avg": "0.00001",
            "cash_delta_lamports": -(LOSS - 1),
            "latency_s": "1.0",
        },
    )
    engine = RiskEngine(
        store=store,
        budget=budget,
        ids=ids,
        clock=clock,
        limits=EngineLimits(),
        autonomy="autonomous-paper",
        get_reserves=lambda _mint: (42_000_000_000, 764_000_000_000_000),
        env={},
    )
    assert engine.evaluate_halts(source="paper", correlation_id="test") is None
    store.append(
        ts_utc=format_ts_utc(clock.now()),
        source="paper",
        correlation_id="test",
        idempotency_key="fill-at",
        type="FillRecorded",
        payload={
            "source": "paper",
            "order_id": "ord-at",
            "decision_id": ids.ulid(),
            "side": "sell",
            "mint": MINT,
            "filled_lamports": 0,
            "filled_units": 0,
            "filled_fraction": "1",
            "price_avg": "0.00001",
            "cash_delta_lamports": -1,
            "latency_s": "1.0",
        },
    )
    assert engine.evaluate_halts(source="paper", correlation_id="test") == "daily_loss"
    halt = store.read(types=["HaltSet"], limit=None)[-1]["payload"]
    assert halt["reason"] == "daily_loss"
    assert halt["day_start_equity_lamports"] == EQUITY


def test_a_locked_ten_sol_ledger_refuses_a_usd_book(tmp_path: Path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    store.set_meta("initial_equity_lamports", "10000000000")
    with pytest.raises(BookUnresolved, match="new paper ledger"):
        locked_book(store, _budget())


def test_session_start_records_the_book_and_a_blind_acceptance(tmp_path: Path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    clock = FixtureClock(1_700_000_000)
    book = book_from_usd(_budget(), _quote())
    budget = apply_book(store, _budget(), book)
    record_blind_acceptance(
        store, clock, by="human:op", reason="SAFE-4 advisory", approval_id="apr-1"
    )
    digest = record_session_start(
        store,
        clock,
        book=book,
        budget=budget,
        limits=EngineLimits(),
        thresholds=SafetyThresholds(),
        autonomy="autonomous-paper",
    )
    note = store.read(types=["JournalNote"], limit=None)[-1]["payload"]
    assert note["seat"] == "bot-02-risk"
    assert f"config_hash={digest}" in note["text"]
    assert "book_lamports=500000000" in note["text"]
    assert "autonomy=autonomous-paper" in note["text"]
    assert "blind_acceptance=YES" in note["text"]
    assert "blind_acceptance_by=human:op" in note["text"]
    changed = store.read(types=["ConfigChanged"], limit=None)[-1]["payload"]
    assert changed["new_hash"] == digest
    assert "session_book" in changed["keys_changed"]
    assert config_hash(budget, EngineLimits(), SafetyThresholds(), book, "manual-ticket") != digest
    consumed = [
        row
        for row in store.read(types=["ApprovalRecorded"], limit=None)
        if row["payload"].get("consumed") is True
    ]
    assert consumed and consumed[-1]["payload"]["kind"] == "blind_accept"


def test_manual_halt_is_a_ledger_row(tmp_path: Path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    book = book_from_usd(_budget(), _quote())
    budget = budget_for_book(_budget(), book)
    apply_book(store, _budget(), book)
    clock = FixtureClock(1_700_000_000)
    engine = RiskEngine(
        store=store,
        budget=budget,
        ids=IdFactory(clock, make_rng(2)),
        clock=clock,
        autonomy="manual-ticket",
        env={},
    )
    engine.request_manual_halt("human:op", "desk stop", source="paper", correlation_id="desk")
    halt = store.read(types=["HaltSet"], limit=None)[-1]["payload"]
    assert halt["reason"] == "manual"
    assert halt["set_by"] == "human:op"
    assert halt["detail"] == "desk stop"
    with pytest.raises(ValueError, match="already active"):
        engine.request_manual_halt("human:op", "again", source="paper")


def test_usd_book_is_not_stamped_as_ten_sol(tmp_path: Path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    ensure_paper_genesis(store, _budget())
    assert store.get_meta("initial_equity_lamports") is None


def test_overlay_accepts_a_usd_book(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    overlay = tmp_path / "operator.toml"
    overlay.write_text(
        '[risk]\npaper_equity_usd = "100"\nmax_trade_fraction = "0.20"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("PUMPGROK_CONFIG", str(overlay))
    settings = load_settings()
    assert settings.risk.paper_equity_usd == Decimal("100")
    assert settings.risk.max_trade_fraction == Decimal("0.20")


def test_self_audit_is_yes_only_when_no_check_is_blind() -> None:
    checks = [
        {"id": check_id, "status": "pass", "reason": "ok", "weight": Decimal("0.1")}
        for check_id in CHECK_IDS
    ]
    clear = finalize_report(
        {"verdict_recommendation": "CLEAR", "checks": checks, "score": 100},
        mint=MINT,
        ticket_id="SOL-20261112-008",
    )
    assert clear["self_audit"] == "YES"
    assert "Blind checks: none" in clear["text"]
    checks[3] = {**checks[3], "status": "blind", "reason": "transfer tax unreadable"}
    blind = finalize_report(
        {"verdict_recommendation": "CONDITIONAL", "checks": checks, "score": 90},
        mint=MINT,
        ticket_id="SOL-20261112-008",
    )
    parsed = json.loads(json.dumps(blind))
    assert parsed["self_audit"] == "NO"
    assert parsed["seat"] == "bot-02-risk"
    assert "transfer_tax" in parsed["blind_checks"]
    assert "Blind checks: transfer_tax" in parsed["text"]
