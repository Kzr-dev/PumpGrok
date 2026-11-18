"""The breakout band is chosen on the training paths and reported on the holdout."""

from __future__ import annotations

from decimal import Decimal

from pumpgrok.strategy.breakout import (
    default_band,
    holdout_paths,
    report,
    select_band,
    simulate,
    train_paths,
)

LAUNCH = (Decimal("1.00"), Decimal("1.05"))
CONFIRM = (Decimal("1.15"), Decimal("1.80"))


def test_training_band_beats_buying_the_launch_on_the_holdout() -> None:
    band = select_band(train_paths())
    assert default_band() == band
    chosen = report(holdout_paths(), band)
    launch = report(holdout_paths(), LAUNCH)
    assert int(chosen["trades"]) > 0
    assert int(chosen["avg_pnl_lamports"]) >= int(launch["avg_pnl_lamports"])
    assert int(chosen["worst_drawdown_lamports"]) <= 0


def test_a_fade_does_not_enter_the_confirmation_band() -> None:
    fade = next(path for path in train_paths() if path.name.startswith("fade"))
    assert simulate(fade, CONFIRM) is None
