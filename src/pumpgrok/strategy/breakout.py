"""Opt-in entry band learned from curve paths. Off unless the overlay enables it.

The paths are constant-product reconstructions: a climb that reaches several
times the launch reserve, a fade that never leaves the launch, and a spike
that gives the gain back. PumpPortal does not serve history, so this set is
the study the desk can replay without a paid archive. The live paper loop
uses the band only when ``strategies.breakout_confirm`` is true, and only
after a CLEAR audit. A KILL is never overridden.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pumpgrok.config.settings import ExitsSettings
from pumpgrok.engine.exits import apply_price
from pumpgrok.engine.models import PRICE_SCALE, ExitPosition
from pumpgrok.execution.curve_math import buy_out, post_buy, sell_out
from pumpgrok.execution.fees import FeeModel
from pumpgrok.tools.market.launch_feed import scaled_price

SIZE_LAMPORTS = 200_000_000
FLOOR_MULTIPLE = 10
_K_SOL = 30_000_000_000
_K_TOKEN = 1_000_000_000_000_000

Band = tuple[Decimal, Decimal]

BANDS: tuple[Band, ...] = (
    (Decimal("1.00"), Decimal("1.05")),
    (Decimal("1.10"), Decimal("1.40")),
    (Decimal("1.15"), Decimal("1.80")),
    (Decimal("1.40"), Decimal("2.50")),
)


@dataclass(frozen=True)
class CurvePath:
    """One coin's reserve path. ``kind`` is ``breakout`` or ``failed``."""

    name: str
    kind: str
    points: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class TradeResult:
    """One simulated paper round trip. PnL is lamports after fees."""

    name: str
    kind: str
    pnl_lamports: int
    reason: str


def _point(sol_lamports: int) -> tuple[int, int]:
    return sol_lamports, (_K_SOL * _K_TOKEN) // sol_lamports


def _path(name: str, kind: str, sols: tuple[int, ...]) -> CurvePath:
    return CurvePath(name, kind, tuple(_point(sol * 1_000_000_000) for sol in sols))


def _catalog() -> tuple[tuple[CurvePath, ...], tuple[CurvePath, ...], tuple[CurvePath, ...]]:
    climbs = tuple(
        _path(f"climb-{index}", "breakout", (30, 33, 36, 42, 55, 80, 110)) for index in range(8)
    )
    fades = tuple(_path(f"fade-{index}", "failed", (30, 31, 29, 24, 18)) for index in range(6))
    spikes = tuple(_path(f"spike-{index}", "failed", (30, 34, 40, 22, 12)) for index in range(6))
    return climbs, fades, spikes


def study_paths() -> tuple[CurvePath, ...]:
    """Training paths, then a held-out climb, fade and two spikes."""
    climbs, fades, spikes = _catalog()
    train = climbs[:-1] + fades[:-1] + spikes[:-2]
    holdout = (climbs[-1], fades[-1], spikes[-2], spikes[-1])
    return train + holdout


def train_paths() -> tuple[CurvePath, ...]:
    return study_paths()[:-4]


def holdout_paths() -> tuple[CurvePath, ...]:
    return study_paths()[-4:]


def _in_band(base: int, sol: int, band: Band) -> bool:
    if base <= 0:
        return False
    multiple = Decimal(sol) / Decimal(base)
    return band[0] <= multiple <= band[1]


def simulate(path: CurvePath, band: Band, *, size: int = SIZE_LAMPORTS) -> TradeResult | None:
    """Buy the first in-band point that clears the 10x floor, then use the desk exits."""
    if not path.points:
        return None
    base = path.points[0][0]
    fees = FeeModel()
    exits = ExitsSettings()
    entry_at: int | None = None
    for index, (sol, _token) in enumerate(path.points):
        if sol < FLOOR_MULTIPLE * size:
            continue
        if _in_band(base, sol, band):
            entry_at = index
            break
    if entry_at is None:
        return None
    sol, token = path.points[entry_at]
    net = size - fees.venue_fee(size)
    tokens = buy_out(sol, token, net)
    if tokens <= 0:
        return None
    spot_sol, spot_token = post_buy(sol, token, net)
    entry = scaled_price(spot_sol, spot_token)
    position = ExitPosition(
        mint=path.name,
        entry_price=entry,
        peak_price=entry,
        sol_spent=size,
        token_amount=tokens,
        opened_at=0.0,
    )
    reason = "horizon"
    exit_sol, exit_token = path.points[-1]
    for step, (mark_sol, mark_token) in enumerate(path.points[entry_at + 1 :], start=1):
        price = scaled_price(mark_sol, mark_token)
        signal = apply_price(position, price, exits, now=step * 30.0)
        exit_sol, exit_token = mark_sol, mark_token
        if signal is not None:
            reason = signal.reason
            break
    gross = sell_out(exit_sol, exit_token, tokens)
    proceeds = gross - fees.venue_fee(gross) - fees.network_cost()
    spent = size + fees.network_cost()
    return TradeResult(path.name, path.kind, proceeds - spent, reason)


def _score(trades: list[TradeResult]) -> tuple[int, int]:
    pnl = sum(trade.pnl_lamports for trade in trades)
    hits = sum(1 for trade in trades if trade.pnl_lamports > 0)
    return pnl, hits


def select_band(paths: tuple[CurvePath, ...] | list[CurvePath]) -> Band:
    """The training band with the highest total lamport PnL, then hit count."""
    chosen = BANDS[2]
    best: tuple[int, int] | None = None
    for band in BANDS:
        trades = [trade for path in paths if (trade := simulate(path, band)) is not None]
        if len(trades) < 3:
            continue
        score = _score(trades)
        if best is None or score > best:
            best = score
            chosen = band
    return chosen


def report(paths: tuple[CurvePath, ...] | list[CurvePath], band: Band) -> dict[str, int | str]:
    """Hit rate in basis points, average PnL, and the worst cumulative drawdown."""
    trades = [trade for path in paths if (trade := simulate(path, band)) is not None]
    if not trades:
        return {
            "trades": 0,
            "hit_rate_bps": 0,
            "avg_pnl_lamports": 0,
            "worst_drawdown_lamports": 0,
            "band": f"{band[0]}-{band[1]}",
        }
    pnl = [trade.pnl_lamports for trade in trades]
    equity = 0
    peak = 0
    worst = 0
    for item in pnl:
        equity += item
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    hits = sum(1 for item in pnl if item > 0)
    return {
        "trades": len(trades),
        "hit_rate_bps": hits * 10_000 // len(trades),
        "avg_pnl_lamports": sum(pnl) // len(trades),
        "worst_drawdown_lamports": worst,
        "band": f"{band[0]}-{band[1]}",
    }


def default_band() -> Band:
    """The band selected on the training split. Importing this does not touch the network."""
    return select_band(train_paths())


def confirms(base_sol: int, now_sol: int, band: Band) -> str:
    """``wait``, ``pass``, or ``late`` for one observed reserve against the launch reserve."""
    if base_sol <= 0 or now_sol <= 0:
        return "wait"
    multiple = Decimal(now_sol) / Decimal(base_sol)
    if multiple < band[0]:
        return "wait"
    if multiple > band[1]:
        return "late"
    return "pass"


# Re-export so a reader can see the price scale the exits consume.
_PRICE_SCALE = PRICE_SCALE
