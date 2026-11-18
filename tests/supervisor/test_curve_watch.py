"""Curve reads fill the reserve cache the liquidity floor consults."""

from __future__ import annotations

import asyncio
import base64
import json

from pumpgrok.config.settings import SafetyThresholds
from pumpgrok.core.errors import OrderRejectedError
from pumpgrok.core.ids import IdFactory
from pumpgrok.engine.models import ExitPosition
from pumpgrok.ledger.store import LedgerStore
from pumpgrok.replay.clock import FixtureClock, WallClock
from pumpgrok.replay.rng import make_rng
from pumpgrok.strategy.breakout import confirms, default_band, report, simulate, train_paths
from pumpgrok.supervisor.live_paper import (
    RpcCurveFeed,
    _account_loop,
    _sell,
    collect_launches,
)
from pumpgrok.tools.onchain.curve_layout import encode_curve_fixture, encode_pump_curve_account
from pumpgrok.tools.onchain.priority_fee import estimate_priority_fee
from pumpgrok.tools.onchain.rpc import RpcError
from pumpgrok.tools.safety.checks.readers import (
    check_deployer_history,
    check_liquidity_depth,
    check_lp_status,
)

MINT = "So11111111111111111111111111111111111111112"
CURVE = "8" * 44


def _account() -> str:
    raw = encode_curve_fixture(
        virtual_sol_reserves_lamports=30_000_000_000,
        virtual_token_reserves_units=1_000_000_000_000_000,
        real_sol_reserves_lamports=1_000_000_000,
        real_token_reserves_units=900_000_000_000_000,
        total_supply_units=1_000_000_000_000_000,
        complete=False,
    )
    return base64.b64encode(raw).decode("ascii")


def test_curve_read_warms_reserves_for_the_floor() -> None:
    encoded = _account()

    class _Rpc:
        clock = FixtureClock(10)

        async def call(self, method: str, params: list[object]) -> dict[str, object]:
            assert method == "getAccountInfo"
            return {"value": {"data": [encoded, "base64"]}, "context": {"slot": 3}}

    feed = RpcCurveFeed(_Rpc(), {})  # type: ignore[arg-type]
    assert feed.reserves(MINT) is None

    async def run() -> None:
        assert await feed.curve(MINT) is None
        feed.remember(MINT, CURVE)
        snap = await feed.curve(MINT)
        assert snap is not None
        assert feed.reserves(MINT) == (snap.s_lamports, snap.t_units)
        assert snap.s_lamports >= 10 * 200_000_000

    asyncio.run(run())


def test_collect_launches_returns_a_create_and_ignores_junk(monkeypatch) -> None:
    raw = json.dumps({"txType": "create", "mint": MINT, "bondingCurveKey": CURVE, "name": "A"})

    class _Sock:
        async def __aenter__(self) -> _Sock:
            return self

        async def __aexit__(self, *_a: object) -> bool:
            return False

        async def send(self, _data: str) -> None:
            return None

        def __aiter__(self) -> _Sock:
            return self

        async def __anext__(self) -> str:
            if self.done:
                raise StopAsyncIteration
            self.done = True
            return raw

        done = False

    monkeypatch.setattr(
        "pumpgrok.supervisor.live_paper.open_public_socket", lambda *_a, **_k: _Sock()
    )

    async def run() -> list[dict[str, str]]:
        clock = WallClock()
        assert await collect_launches("wss://pumpportal.fun/api/data", 0, 1, clock) == []
        return await collect_launches("wss://pumpportal.fun/api/data", 2, 1, clock)

    found = asyncio.run(run())
    assert found[0]["mint"] == MINT


def test_account_loop_maps_a_subscription_to_a_tick() -> None:
    raw = encode_pump_curve_account(
        virtual_sol_reserves_lamports=30_000_000_000,
        virtual_token_reserves_units=1_000_000_000_000_000,
        real_sol_reserves_lamports=1_000_000_000,
        real_token_reserves_units=900_000_000_000_000,
        total_supply_units=1_000_000_000_000_000,
        complete=False,
    )
    data = base64.b64encode(raw).decode("ascii")
    notice = json.dumps(
        {
            "params": {
                "subscription": 4,
                "result": {"value": {"data": [data, "base64"]}},
            }
        }
    )

    class _Sock:
        def __init__(self) -> None:
            self.sent: list[str] = []
            self.inbox = [
                json.dumps({"id": 1, "result": 4}),
                notice,
            ]

        async def send(self, body: str) -> None:
            self.sent.append(body)

        async def recv(self) -> str:
            if not self.inbox:
                await asyncio.sleep(3600)
            return self.inbox.pop(0)

    async def run() -> None:
        ticks: asyncio.Queue = asyncio.Queue()
        curves = {MINT: CURVE}
        task = asyncio.create_task(_account_loop(_Sock(), curves, ticks, WallClock()))
        tick = await asyncio.wait_for(ticks.get(), timeout=2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert tick.mint == MINT
        assert tick.price > 0

    asyncio.run(run())


def test_curve_read_failures_leave_the_cache_empty() -> None:
    class _Down:
        clock = FixtureClock(1)

        async def call(self, method: str, params: list[object]) -> dict[str, object]:
            del method, params
            raise RuntimeError("down")

    class _Empty:
        clock = FixtureClock(1)

        async def call(self, method: str, params: list[object]) -> dict[str, object]:
            del method, params
            return {"value": None}

    async def run() -> None:
        down = RpcCurveFeed(_Down(), {MINT: CURVE})  # type: ignore[arg-type]
        empty = RpcCurveFeed(_Empty(), {MINT: CURVE})  # type: ignore[arg-type]
        assert await down.curve(MINT) is None
        assert await empty.curve(MINT) is None
        assert down.reserves(MINT) is None

    asyncio.run(run())


def test_refused_exit_is_not_retried(tmp_path) -> None:
    store = LedgerStore(str(tmp_path / "ledger.sqlite"))
    clock = FixtureClock(20)
    ids = IdFactory(clock, make_rng(3))
    position = ExitPosition(mint=MINT, token_amount=10, entry_price=1, peak_price=1)

    class _Adapter:
        calls = 0

        async def place(self, *_args: object, **_kwargs: object) -> None:
            self.calls += 1
            raise OrderRejectedError("halted", stage="ACCEPTED")

    adapter = _Adapter()

    async def run() -> None:
        await _sell(
            adapter,  # type: ignore[arg-type]
            ids,
            store,
            clock,
            position,
            reason="stop_loss",
            slippage_bps=50,
        )

    asyncio.run(run())
    assert adapter.calls == 1


def test_confirmation_band_and_reader_edges() -> None:
    band = default_band()
    assert confirms(30, 20, band) == "wait"
    assert confirms(30, 50, band) == "pass" or confirms(30, 50, band) == "late"
    assert confirms(0, 10, band) == "wait"
    climb = next(path for path in train_paths() if path.kind == "breakout")
    trade = simulate(climb, band)
    assert trade is not None and trade.pnl_lamports > 0
    assert report([], band)["trades"] == 0
    thresholds = SafetyThresholds()
    assert check_lp_status({"stage": "unknown"}).status == "blind"
    assert check_lp_status({"stage": "graduated"}).status == "warn"
    assert check_lp_status({"stage": "bonding_curve"}).status == "pass"
    bad = check_deployer_history({"is_known_bad": True, "rugs": 0, "tokens_seen": 0})
    assert bad.status == "fail"
    assert check_liquidity_depth({"liquidity_lamports": 0}, 10, thresholds).status == "fail"


def test_priority_fee_failures_stay_typed() -> None:
    class _Empty:
        async def call(self, method: str, params: list[object]) -> list[object]:
            del method, params
            return []

    class _Boom:
        async def call(self, method: str, params: list[object]) -> list[object]:
            del method, params
            raise RpcError("down")

    class _Odd:
        async def call(self, method: str, params: list[object]) -> int:
            del method, params
            return 1

    async def run() -> None:
        empty = await estimate_priority_fee(_Empty())  # type: ignore[arg-type]
        boom = await estimate_priority_fee(_Boom())  # type: ignore[arg-type]
        odd = await estimate_priority_fee(_Odd())  # type: ignore[arg-type]
        assert empty["ok"] is False
        assert boom["ok"] is False
        assert odd["ok"] is False

    asyncio.run(run())
