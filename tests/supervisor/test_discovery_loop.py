"""Portal and program-log discovery, plus a zero-length public session."""

from __future__ import annotations

import asyncio
import json

from pumpgrok.config.settings import load_settings
from pumpgrok.replay.clock import WallClock
from pumpgrok.supervisor.live_paper import (
    _ack_subscription,
    _listen_creates,
    _listen_program_logs,
    _notification_tick,
    serve_public_paper,
)
from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda

MINT = "So11111111111111111111111111111111111111112"
URL = "wss://pumpportal.fun/api/data"


class _Socket:
    def __init__(self, messages: list[str], *, fail_on_reentry: bool = False) -> None:
        self.messages = messages
        self.sent: list[str] = []
        self.entries = 0
        self.fail_on_reentry = fail_on_reentry

    async def __aenter__(self) -> _Socket:
        self.entries += 1
        if self.fail_on_reentry and self.entries > 1:
            raise RuntimeError("closed")
        return self

    async def __aexit__(self, *_args: object) -> bool:
        return False

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> _Socket:
        return self

    async def __anext__(self) -> str:
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


def test_portal_delivers_one_create_and_records_the_subscription(monkeypatch) -> None:
    curve = bonding_curve_pda(MINT)
    raw = json.dumps(
        {"txType": "create", "mint": MINT, "bondingCurveKey": curve, "name": "Paper"}
    )
    socket = _Socket([raw])

    def connect(*_args: object, **_kwargs: object) -> _Socket:
        return socket

    monkeypatch.setattr("pumpgrok.supervisor.live_paper.open_public_socket", connect)
    monkeypatch.setattr(
        "pumpgrok.supervisor.live_paper.discovery_delay", lambda *_a, **_k: 0.0
    )

    async def run() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        seen: set[str] = set()
        portal: dict[str, float] = {}
        task = asyncio.create_task(
            _listen_creates(URL, queue, WallClock(), seen, portal)
        )
        launch = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert launch.mint == MINT
        assert "subscribeNewToken" in socket.sent[0]
        assert portal["at"] > 0

    asyncio.run(run())


def test_program_logs_enqueue_a_create_when_the_portal_is_quiet(monkeypatch) -> None:
    curve = bonding_curve_pda(MINT)
    notice = json.dumps(
        {
            "params": {
                "result": {
                    "value": {
                        "signature": "sig",
                        "err": None,
                        "logs": ["Program log: Instruction: Create"],
                    }
                }
            }
        }
    )
    socket = _Socket([notice], fail_on_reentry=True)

    def connect(*_args: object, **_kwargs: object) -> _Socket:
        return socket

    monkeypatch.setattr("pumpgrok.supervisor.live_paper.open_public_socket", connect)
    monkeypatch.setattr(
        "pumpgrok.supervisor.live_paper.discovery_delay", lambda *_a, **_k: 0.0
    )

    class _Rpc:
        async def call(self, method: str, params: list[object]) -> dict[str, object]:
            assert method == "getTransaction"
            return {
                "meta": {"logMessages": ["Program log: Instruction: Create"]},
                "transaction": {
                    "message": {
                        "instructions": [
                            {
                                "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                                "accounts": [MINT, "a", curve],
                            }
                        ]
                    }
                },
            }

    async def run() -> None:
        queue: asyncio.Queue = asyncio.Queue()
        seen: set[str] = set()
        task = asyncio.create_task(
            _listen_program_logs(
                "wss://rpc.example/ws",
                queue,
                _Rpc(),  # type: ignore[arg-type]
                WallClock(),
                portal_seen={"at": 0.0},
                seen=seen,
            )
        )
        launch = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert launch.curve_account == curve
        assert MINT in seen

    asyncio.run(run())


def test_account_ack_ignores_a_notice_without_a_price() -> None:
    pending = {1: MINT}
    by_sub: dict[int, str] = {}
    assert _ack_subscription({"id": 1, "result": 4}, pending, by_sub) is True
    assert by_sub[4] == MINT
    assert _notification_tick({"params": {"subscription": 4}}, by_sub, 1.0) is None
    assert _ack_subscription({"id": "nope"}, pending, by_sub) is False


def test_zero_duration_session_starts_and_stops(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for name in list(__import__("os").environ):
        if name.startswith("PUMPGROK_"):
            monkeypatch.delenv(name, raising=False)

    class _Idle:
        async def __aenter__(self) -> _Idle:
            return self

        async def __aexit__(self, *_args: object) -> bool:
            return False

        async def send(self, _data: str) -> None:
            return None

        def __aiter__(self) -> _Idle:
            return self

        async def __anext__(self) -> str:
            await asyncio.sleep(3600)
            return ""

    monkeypatch.setattr(
        "pumpgrok.supervisor.live_paper.open_public_socket", lambda *_a, **_k: _Idle()
    )
    settings = load_settings()

    async def run() -> None:
        report = await serve_public_paper(
            settings, duration_s=0, record_name=None, ledger_path=tmp_path / "ledger.sqlite"
        )
        assert report.audits == 0
        assert report.buys == 0

    asyncio.run(run())
