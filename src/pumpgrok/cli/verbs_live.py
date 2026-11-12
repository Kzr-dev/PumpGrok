"""Live-data paper verbs: risk audit, scout candidates, and a timed paper run.

These verbs stay on public reads and the paper adapter. They do not sign,
broadcast, or open a live trading path.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from pumpgrok.cli.codes import EXIT_OK
from pumpgrok.cli.errors import _say
from pumpgrok.config.settings import Settings
from pumpgrok.replay.clock import WallClock
from pumpgrok.risk.fast_audit import (
    audit_mint,
    build_audit_rpc,
    dumps_audit,
    resolve_curve_account,
)
from pumpgrok.supervisor.live_paper import (
    collect_launches,
    serve_public_paper,
    trade_size_lamports,
)


def _json(payload: dict[str, object]) -> int:
    _say(sys.stdout, json.dumps(payload))
    return EXIT_OK


def risk_audit(args: object, settings: Settings) -> int:
    """Run the ten-check audit. Missing ``--mint`` is a usage error, not exit 5."""
    mint = getattr(args, "mint", None)
    if not isinstance(mint, str) or not mint:
        _say(
            sys.stdout,
            json.dumps({"ok": False, "code": "usage", "error": "risk audit requires --mint"}),
        )
        return 1
    curve = getattr(args, "curve_account", None)
    creator = getattr(args, "creator", None)
    try:
        curve_account = resolve_curve_account(mint, curve if isinstance(curve, str) else None)
    except ValueError:
        _say(
            sys.stdout,
            json.dumps({"ok": False, "code": "usage", "error": "mint is not a public key"}),
        )
        return 1

    async def _run() -> dict[str, object]:
        rpc, client = build_audit_rpc(settings, WallClock())
        try:
            return await audit_mint(
                rpc,
                mint=mint,
                curve_account=curve_account,
                creator=creator if isinstance(creator, str) and creator else None,
                intended_size_lamports=trade_size_lamports(
                    settings.risk.paper_equity_lamports, settings.risk, settings.limits
                ),
                ticket_id="SOL-19700101-000",
            )
        finally:
            await client.aclose()

    payload = asyncio.run(_run())
    _say(sys.stdout, dumps_audit(payload))
    return EXIT_OK


def scout_pipeline(args: object, settings: Settings) -> int:
    """List candidate mints. ``--timeout 0`` (the default) does not open a socket."""
    if not getattr(args, "candidates", False):
        _say(
            sys.stdout,
            json.dumps(
                {"ok": False, "code": "usage", "error": "scout pipeline requires --candidates"}
            ),
        )
        return 1
    limit = int(getattr(args, "limit", 5) or 5)
    fixture = getattr(args, "fixture", None)
    timeout = float(getattr(args, "timeout", 0) or 0)
    if isinstance(fixture, str) and fixture:
        candidates = _fixture_candidates(Path(fixture), limit)
        return _json({"ok": True, "candidates": candidates, "source": "fixture"})
    if timeout <= 0:
        return _json({"ok": True, "candidates": [], "source": "idle", "timeout_s": 0})
    found = asyncio.run(collect_launches(settings.data.ws_url, timeout, limit, WallClock()))
    return _json({"ok": True, "candidates": found, "source": "pumpportal", "timeout_s": timeout})


def _fixture_candidates(path: Path, limit: int) -> list[dict[str, str]]:
    events = path / "events.jsonl" if path.is_dir() else path
    if not events.is_file():
        return []
    seen: list[dict[str, str]] = []
    taken: set[str] = set()
    for line in events.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        mint = row.get("mint")
        if not isinstance(mint, str) or mint in taken:
            continue
        taken.add(mint)
        seen.append({"mint": mint, "name": str(row.get("name") or "")})
        if len(seen) >= limit:
            break
    return seen


def paper_duration(args: object, settings: Settings) -> int:
    """Timed paper session on public data. ``--record`` writes a replay dataset."""
    duration = int(getattr(args, "duration", 0) or 0)
    record = getattr(args, "record", None)
    record_name = record if isinstance(record, str) and record else None
    report = asyncio.run(
        serve_public_paper(settings, duration_s=float(duration), record_name=record_name)
    )
    _say(
        sys.stdout,
        (
            f"paper session autonomy={report.autonomy} audits={report.audits} "
            f"kills={report.kills} buys={report.buys} sells={report.sells} "
            f"halt={report.halted or 'none'} quotes_stale={report.quotes_stale}"
        ),
    )
    if record_name:
        _say(sys.stdout, f"recorded={record_name}")
    return EXIT_OK
