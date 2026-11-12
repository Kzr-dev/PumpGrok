"""The risk audit is fail-closed, JSON-safe, and paced on the public RPC."""

from __future__ import annotations

import asyncio
import json
import random
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from pumpgrok.cli.parsing import build_parser
from pumpgrok.core.tool import RetryPolicy
from pumpgrok.replay.clock import FixtureClock
from pumpgrok.risk.fast_audit import (
    PUBLIC_RPC_MIN_INTERVAL_S,
    audit_mint,
    dumps_audit,
    finalize_report,
)
from pumpgrok.supervisor.live_paper import audit_with
from pumpgrok.tools.market.launch_feed import Launch
from pumpgrok.tools.onchain.rpc import RpcClient, RpcError
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
    assert report["self_audit"] == "NO"
    assert report["seat"] == "bot-02-risk"
    assert len(report["checks"]) == len(CHECK_IDS) == 10
    assert "All 10 checks completed: NO" in report["text"]
    assert "Blind checks:" in report["text"]
    assert "none" not in report["text"].split("Blind checks:", 1)[1]
    assert "Final: KILL cannot be overridden" in report["text"]
    assert rpc.max_in_flight == 1
    parsed = json.loads(dumps_audit(report))
    assert parsed["verdict"] == "KILL"
    assert json.loads(json.dumps(report))["final"] is True


def test_live_shaped_decimal_payload_serializes() -> None:
    """The crash was Decimal weights inside the audit JSON."""
    report = finalize_report(
        {
            "verdict_recommendation": "KILL",
            "score": 40,
            "blind_checks": ["mint_authority"],
            "advisory_blind_checks": ["top10_concentration"],
            "checks": [
                {
                    "id": "mint_authority",
                    "status": "blind",
                    "value": None,
                    "threshold": None,
                    "weight": Decimal("0.15"),
                    "reason": "mint authority unreadable",
                }
            ],
            "evidence": [
                {
                    "check": "mint_authority",
                    "source": "solana_rpc",
                    "slot": None,
                    "fetched_at": datetime(2026, 11, 12, tzinfo=UTC),
                }
            ],
        },
        mint="6MZZGJutbeEny8KB7Lexu3qxSTe7dHe1kQL3RVwCpump",
        ticket_id="SOL-20261112-001",
    )
    parsed = json.loads(dumps_audit(report))
    assert parsed["checks"][0]["weight"] == "0.15"
    assert parsed["verdict"] == "KILL"
    assert parsed["final"] is True


async def test_run_loop_audit_path_dumps_the_same_report() -> None:
    rpc = _Rpc()
    audit = audit_with(rpc)  # type: ignore[arg-type]
    report = await audit(
        Launch(mint=MINT, curve_account=CURVE, creator=MINT, name="live"),
        "SOL-20261112-001",
        200_000_000,
    )
    parsed = json.loads(dumps_audit(report))
    assert parsed["verdict"] == "KILL"
    assert parsed["final"] is True


async def test_http_429_is_blind_not_a_pass() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "slow down"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rpc = RpcClient(
        ["https://rpc.invalid"],
        client,
        FixtureClock(start=1),
        random.Random(1),
        retry=RetryPolicy(attempts=1, base_s=0.0, max_s=0.0),
        min_interval_s=0.0,
    )
    report = await audit_mint(
        rpc,
        mint=MINT,
        curve_account=CURVE,
        creator=MINT,
        intended_size_lamports=200_000_000,
    )
    assert report["verdict"] == "KILL"
    assert report["final"] is True
    statuses = {row["id"]: row["status"] for row in report["checks"]}
    assert statuses["mint_authority"] == "blind"
    assert statuses["liquidity_depth"] == "blind"
    assert "pass" not in statuses.values()
    json.loads(dumps_audit(report))


async def test_public_spacing_keeps_requests_a_second_apart() -> None:
    clock = FixtureClock(start=0)
    stamps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        stamps.append(clock.now())
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": None})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rpc = RpcClient(
        ["https://rpc.invalid"],
        client,
        clock,
        random.Random(1),
        min_interval_s=PUBLIC_RPC_MIN_INTERVAL_S,
        retry=RetryPolicy(attempts=1, base_s=0.0, max_s=0.0),
    )

    async def both() -> None:
        await asyncio.gather(rpc.call("getAccountInfo", ["a"]), rpc.call("getAccountInfo", ["b"]))

    task = asyncio.create_task(both())
    for _ in range(40):
        if task.done():
            break
        clock.advance_to(clock.now() + 0.25)
        await asyncio.sleep(0)
    await task
    assert stamps[1] - stamps[0] >= PUBLIC_RPC_MIN_INTERVAL_S


def test_run_help_describes_duration_and_record(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["run", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "not yet implemented" not in out
    assert "public-data paper session" in out
    assert "var/recordings" in out
