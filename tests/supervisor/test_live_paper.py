"""Paper session: audit, one buy, halt blocks the next entry, 429 does not retry."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from pumpgrok.config.settings import ExitsSettings
from pumpgrok.core.ids import IdFactory
from pumpgrok.core.risk_budget import RiskBudget
from pumpgrok.execution.paper_adapter import PaperAdapter
from pumpgrok.execution.paper_adapter.models import CurveSnapshot
from pumpgrok.ledger.store import LedgerStore
from pumpgrok.replay.clock import FixtureClock
from pumpgrok.replay.fixtures import load_dataset
from pumpgrok.replay.rng import make_rng
from pumpgrok.risk.engine import RiskEngine
from pumpgrok.risk.limits import EngineLimits
from pumpgrok.supervisor.live_paper import (
    LivePaperDeps,
    SessionRecorder,
    run_live_paper,
    trade_size_lamports,
)
from pumpgrok.tools.market.launch_feed import Launch

MINT = "So11111111111111111111111111111111111111112"
CURVE = "8" * 44
START = 1_769_900_000.0
EQUITY = 10_000_000_000
S0 = 42_000_000_000
T0 = 766_428_571_428_571


class _Feed:
    def __init__(self, snap: CurveSnapshot) -> None:
        self._snap = snap

    async def curve(self, mint: str) -> CurveSnapshot | None:
        return self._snap

    def reserves(self, mint: str) -> tuple[int, int] | None:
        return self._snap.s_lamports, self._snap.t_units


def _deps(
    tmp_path: Path,
    *,
    autonomy: str,
    launches: list[Launch],
    quote: dict[str, Any],
    halt: bool = False,
) -> LivePaperDeps:
    clock = FixtureClock(START)
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    store.set_meta("initial_equity_lamports", str(EQUITY))
    store.set_meta("source", "paper")
    if halt:
        store.append(
            ts_utc="2026-11-12T00:00:00.000Z",
            source="paper",
            correlation_id="test",
            idempotency_key="halt|test",
            type="HaltSet",
            payload={
                "source": "paper",
                "reason": "daily_loss",
                "detail": "FLOOR HALTED",
                "equity_lamports": EQUITY // 2,
                "day_start_equity_lamports": EQUITY,
                "set_by": "risk_engine",
            },
        )
    snap = CurveSnapshot(
        mint=MINT, s_lamports=S0, t_units=T0, source="rpc", layout="pump", ts=START
    )
    feed = _Feed(snap)
    ids = IdFactory(clock, make_rng(7))
    budget = RiskBudget()
    limits = EngineLimits()
    risk = RiskEngine(
        store=store,
        budget=budget,
        ids=IdFactory(clock, make_rng(7)),
        clock=clock,
        limits=limits,
        autonomy=autonomy,  # type: ignore[arg-type]
        get_reserves=feed.reserves,
        env={},
    )
    adapter = PaperAdapter(
        store=store,
        risk=risk,
        ids=ids,
        clock=clock,
        rng=make_rng(7),
        budget=budget,
        limits=limits,
        price_source=feed,
        p_drop=0.0,
        source="paper",
    )
    pending = list(launches)

    async def discover() -> list[Launch]:
        if not pending:
            return []
        return [pending.pop(0)]

    async def audit(launch: Launch, ticket: str, size: int) -> dict[str, Any]:
        del launch, size
        return {
            "verdict": "CLEAR",
            "final": False,
            "text": f"Verdict: CLEAR\n{ticket}",
            "checks": [],
        }

    async def quote_fn(*_args: object) -> dict[str, Any]:
        return quote

    async def price_for(_mint: str) -> int:
        return 0

    return LivePaperDeps(
        clock=clock,
        store=store,
        risk=risk,
        adapter=adapter,
        exits=ExitsSettings(),
        budget=budget,
        limits=limits,
        ids=ids,
        autonomy=autonomy,
        discover=discover,
        audit=audit,
        quote=quote_fn,
        price_for=price_for,
        slippage_bps=100,
    )


async def _drive(deps: LivePaperDeps, duration: float) -> Any:
    task = asyncio.create_task(run_live_paper(deps, duration_s=duration))
    for _ in range(400):
        if task.done():
            break
        deps.clock.advance_to(deps.clock.now() + 1.0)
        await asyncio.sleep(0)
    return await task


def _launch() -> Launch:
    return Launch(mint=MINT, curve_account=CURVE, creator=MINT, name="Paper")


def _quote() -> dict[str, Any]:
    return {
        "ok": True,
        "retry": False,
        "amount_unit": "raw",
        "in_amount_raw": 200_000_000,
        "out_amount_raw": 1_000_000,
        "input_decimals": 9,
        "output_decimals": 6,
        "code": None,
    }


async def test_autonomous_paper_buys_once_through_the_adapter(tmp_path: Path) -> None:
    deps = _deps(tmp_path, autonomy="autonomous-paper", launches=[_launch()], quote=_quote())
    size = trade_size_lamports(EQUITY, deps.budget, deps.limits)
    assert size == 200_000_000
    report = await _drive(deps, 30)
    assert report.buys == 1
    assert report.audits == 1
    assert report.kills == 0
    placed = [
        row
        for row in deps.store.read(limit=None)
        if row["type"] == "OrderPlaced" and row["payload"]["side"] == "buy"
    ]
    assert len(placed) == 1
    assert deps.store.read(types=["JournalNote"], limit=None)


async def test_manual_ticket_does_not_buy(tmp_path: Path) -> None:
    deps = _deps(tmp_path, autonomy="manual-ticket", launches=[_launch()], quote=_quote())
    report = await _drive(deps, 5)
    assert report.buys == 0
    assert report.audits == 1
    assert not [row for row in deps.store.read(limit=None) if row["type"] == "OrderPlaced"]


async def test_daily_loss_halt_blocks_a_new_entry(tmp_path: Path) -> None:
    deps = _deps(
        tmp_path, autonomy="autonomous-paper", launches=[_launch()], quote=_quote(), halt=True
    )
    report = await _drive(deps, 5)
    assert report.halted == "daily_loss"
    assert report.buys == 0


async def test_stale_quote_does_not_place_or_retry(tmp_path: Path) -> None:
    quote = {
        "ok": False,
        "code": "data_stale",
        "blind": True,
        "retry": False,
        "status": 429,
        "backoff_s": 1,
    }
    deps = _deps(
        tmp_path, autonomy="autonomous-paper", launches=[_launch(), _launch()], quote=quote
    )
    report = await _drive(deps, 10)
    assert report.buys == 0
    assert report.quotes_stale == 1
    assert not [row for row in deps.store.read(limit=None) if row["type"] == "OrderPlaced"]


def test_recorder_writes_a_loadable_dataset(tmp_path: Path) -> None:
    recorder = SessionRecorder(tmp_path / "fast", dataset="fast", seed_hint=1)
    recorder.add(
        {
            "ts_utc": "2026-11-12T00:00:00.000Z",
            "kind": "tick",
            "mint": MINT,
            "S": S0,
            "T": T0,
            "complete": False,
            "source": "rpc",
        }
    )
    recorder.add(
        {
            "ts_utc": "2026-11-12T00:00:01.000Z",
            "kind": "tick",
            "mint": MINT,
            "S": S0 + 1,
            "T": T0,
            "complete": False,
            "source": "rpc",
        }
    )
    assert recorder.close() == tmp_path / "fast"
    loaded = load_dataset(tmp_path / "fast")
    assert loaded.manifest.dataset == "fast"
    assert len(loaded.events) == 2
