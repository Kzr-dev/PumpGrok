"""Tick-driven exit watch for open paper positions.

Stop, trailing and take-profit rules run on every fresh price tick. When a
tick is older than the configured poll interval, or none arrives, the watch
falls back to one polled read. Repeated misses become blind and are not
treated as a price. Exit thresholds stay on ``ExitsSettings``; this module
does not choose them.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pumpgrok.engine.exits import apply_price
from pumpgrok.engine.models import ExitPosition
from pumpgrok.engine.watcher import PositionBook, PositionWatcher
from pumpgrok.replay.clock import Clock

log = logging.getLogger(__name__)

# Same miss count as the polled watcher, so blind means the same thing on both paths.
BLIND_AFTER = PositionWatcher.BLIND_AFTER

__all__ = ["BLIND_AFTER", "PriceTick", "TickExitWatch"]


@dataclass(frozen=True)
class PriceTick:
    """One observed price. ``price`` is a ``PRICE_SCALE`` integer. ``ts`` is epoch seconds."""

    mint: str
    price: int
    ts: float
    source: str = "ws"


class TickExitWatch:
    """Evaluate exit rules on each fresh tick, then poll what the socket missed."""

    def __init__(
        self,
        book: PositionBook,
        sell_fn: Callable[[ExitPosition, int, str], Awaitable[None]],
        clock: Clock,
    ) -> None:
        self.book = book
        self.sell_fn = sell_fn
        self.clock = clock
        self.price_failures: dict[str, int] = {}
        self._last_ts: dict[str, float] = {}

    @property
    def blind(self) -> list[str]:
        """Positions with no fresh price. Exit rules do not fire on them."""
        return [
            mint
            for mint, misses in self.price_failures.items()
            if misses >= BLIND_AFTER and mint in self.book.positions
        ]

    def _stale_after(self) -> float:
        """A tick older than one poll interval is not a price. The interval is the exit setting."""
        return self.book.poll_seconds

    def _miss(self, mint: str) -> None:
        misses = self.price_failures.get(mint, 0) + 1
        self.price_failures[mint] = misses
        if misses == BLIND_AFTER:
            log.error(
                "position %s without a fresh price %d times: exit rules are blind",
                mint[:8],
                misses,
            )

    async def apply_tick(self, tick: PriceTick) -> str | None:
        """Evaluate one tick. Returns the mint when a rule sells it.

        A stale tick is a miss, not a fill. The sell callback runs at most
        once for the tick that crossed a rule.
        """
        position = self.book.positions.get(tick.mint)
        if position is None or tick.price <= 0:
            return None
        if self.clock.now() - tick.ts > self._stale_after():
            self._miss(tick.mint)
            return None
        self.price_failures.pop(tick.mint, None)
        self._last_ts[tick.mint] = tick.ts
        signal = apply_price(position, tick.price, self.book.exit_config, tick.ts)
        if signal is None:
            return None
        log.info(
            "exiting %s on %s (%s)",
            position.mint[:8],
            signal.reason,
            signal.detail,
        )
        await self.sell_fn(position, tick.price, signal.reason)
        return tick.mint

    async def drain(self, queue: asyncio.Queue[PriceTick | None]) -> list[str]:
        """Apply every tick already queued. Does not wait for a future tick."""
        sold: list[str] = []
        while True:
            try:
                tick = queue.get_nowait()
            except asyncio.QueueEmpty:
                return sold
            if tick is None:
                continue
            mint = await self.apply_tick(tick)
            if mint is not None:
                sold.append(mint)

    async def poll_stale(self, price_fn: Callable[[str], Awaitable[int]]) -> list[str]:
        """Poll positions whose last tick is older than one poll interval."""
        sold: list[str] = []
        now = self.clock.now()
        limit = self._stale_after()
        for position in list(self.book.positions.values()):
            last = self._last_ts.get(position.mint)
            if last is not None and now - last <= limit:
                continue
            try:
                price = await price_fn(position.mint)
            except Exception as exc:
                log.warning("poll price failed for %s: %s", position.mint[:8], type(exc).__name__)
                price = 0
            if price <= 0:
                self._miss(position.mint)
                continue
            mint = await self.apply_tick(PriceTick(position.mint, price, now, "poll"))
            if mint is not None:
                sold.append(mint)
        return sold
