"""The risk audit runs all ten checks, in parallel, and fail-closes."""

from __future__ import annotations

import asyncio

from pumpgrok.replay.clock import FixtureClock
from pumpgrok.risk.fast_audit import audit_mint
from pumpgrok.tools.onchain.rpc import RpcError
from pumpgrok.tools.safety.checks import CHECK_IDS

MINT = "So11111111111111111111111111111111111111112"
CURVE = "8" * 44


class _Rpc:
    def __init__(self) -> None:
        self.clock = FixtureClock(start=10)
        self.in_flight = 0
        self.max_in_flight = 0
        self.calls = 0

    async def call(self, method: str, params: list[object]) -> object:
        del method, params
        self.calls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.in_flight -= 1
        raise RpcError("missing")


async def test_missing_reads_are_kill_and_the_readers_overlap() -> None:
    rpc = _Rpc()
    report = await audit_mint(
        rpc,  # type: ignore[arg-type]
        mint=MINT,
        curve_account=CURVE,
        creator=MINT,
        intended_size_lamports=200_000_000,
    )
    assert report["verdict"] == "KILL"
    assert report["final"] is True
    assert report["self_audit"] == "YES"
    assert len(report["checks"]) == len(CHECK_IDS) == 10
    assert "All 10 checks completed: YES" in report["text"]
    assert "Final: KILL cannot be overridden" in report["text"]
    assert rpc.max_in_flight > 1
