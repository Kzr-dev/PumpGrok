"""A fast crash must trip the trailing stop on the crossing tick, not the next poll."""

from __future__ import annotations

import json
from pathlib import Path

from pumpgrok.config.settings import ExitsSettings
from pumpgrok.engine.models import ExitPosition
from pumpgrok.engine.watcher import PositionWatcher
from pumpgrok.replay.clock import FixtureClock
from pumpgrok.supervisor.exit_watch import PriceTick, TickExitWatch

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "replay" / "fast-crash" / "events.jsonl"
)
MINT = "CrashMint11111111111111111111111111111111"


class _Book:
    def __init__(self, position: ExitPosition, exits: ExitsSettings) -> None:
        self.positions = {position.mint: position}
        self.exit_config = exits
        self.poll_seconds = exits.stop_loss_poll_seconds
        self.persisted = 0

    def persist(self) -> None:
        self.persisted += 1


def _prices() -> list[tuple[str, int]]:
    rows = [json.loads(line) for line in FIXTURE.read_text(encoding="utf-8").splitlines()]
    return [(str(row["ts_utc"]), int(row["S"])) for row in rows]


def test_fixture_crash_is_53_percent_under_the_peak_and_has_a_35_percent_tick() -> None:
    prices = [price for _ts, price in _prices()]
    peak = max(prices)
    crash = prices[-1]
    drawdown = (peak - crash) / peak * 100
    assert abs(drawdown - 53) < 0.05
    crossed = [price for price in prices if price != peak and (peak - price) / peak * 100 >= 35]
    assert crossed
    assert crossed[0] != crash


def _position(entry: int) -> ExitPosition:
    return ExitPosition(
        mint=MINT, entry_price=entry, peak_price=entry, token_amount=1_000, opened_at=0
    )


async def test_tick_watch_sells_at_the_35_percent_cross_not_the_crash() -> None:
    prices = [price for _ts, price in _prices()]
    entry = prices[0]
    crash = prices[-1]
    book = _Book(_position(entry), ExitsSettings())
    sold: list[int] = []

    async def sell(position: ExitPosition, price: int, reason: str) -> None:
        assert reason == "trailing_stop"
        sold.append(price)
        book.positions.pop(position.mint, None)

    clock = FixtureClock(start=0)
    watch = TickExitWatch(book, sell, clock)
    for index, price in enumerate(prices):
        # One second per fixture row, except the crash which is the last stamp.
        clock.advance_to(float(index if index < len(prices) - 1 else 17))
        await watch.apply_tick(PriceTick(MINT, price, clock.now(), "ws"))
    assert sold == [20398]
    assert sold[0] != crash


async def test_poll_only_watcher_fills_the_crash_when_the_middle_ticks_are_unseen() -> None:
    prices = [price for _ts, price in _prices()]
    entry = prices[0]
    peak = max(prices)
    crash = prices[-1]
    book = _Book(_position(entry), ExitsSettings())
    samples = iter((peak, crash))
    sold: list[int] = []

    async def price_fn(_mint: str) -> int:
        return next(samples)

    async def sell(position: ExitPosition, price: int, reason: str) -> None:
        assert reason == "trailing_stop"
        sold.append(price)
        book.positions.pop(position.mint, None)

    clock = FixtureClock(start=0)
    watcher = PositionWatcher(book, price_fn, sell, clock)
    await watcher.check_once(clock.now())
    clock.advance_to(16)
    await watcher.check_once(clock.now())
    assert sold == [crash]


async def test_stale_tick_does_not_sell_and_a_poll_can() -> None:
    book = _Book(_position(1000), ExitsSettings())
    sold: list[str] = []

    async def sell(position: ExitPosition, price: int, reason: str) -> None:
        sold.append(reason)
        book.positions.pop(position.mint, None)

    clock = FixtureClock(start=100)
    watch = TickExitWatch(book, sell, clock)
    await watch.apply_tick(PriceTick(MINT, 100, 0, "ws"))
    assert sold == []
    assert MINT in watch.price_failures

    async def price_fn(_mint: str) -> int:
        return 0

    await watch.poll_stale(price_fn)
    await watch.poll_stale(price_fn)
    assert MINT in watch.blind
    assert sold == []
