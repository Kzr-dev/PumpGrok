"""Fast fail-closed risk audit over the ten safety checks.

The checklist is ``safety.score_report``. Reads run concurrently through that
report, and the RPC client may cache a successful body for a few seconds of
injected clock time. A missing required read is blind, and blind on a required
check is KILL. SAFE-4 and SAFE-8 stay advisory while no paid RPC provider is
recorded. A KILL is final for the mint in the calling session.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from pumpgrok.config.settings import Settings
from pumpgrok.core.ids import Clock
from pumpgrok.core.tool import RetryPolicy
from pumpgrok.replay.clock import WallClock
from pumpgrok.replay.rng import make_rng
from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda
from pumpgrok.tools.onchain.rpc import RpcClient
from pumpgrok.tools.safety.checks import CHECK_IDS
from pumpgrok.tools.safety.provider import paid_rpc_provider_recorded
from pumpgrok.tools.safety.score_report import ScoreReportIn, run_score_report

_AUDIT_CACHE_S = 2.0
#: Public mainnet budget: one request per second, no burst. Paid RPC is unpaced.
PUBLIC_RPC_MIN_INTERVAL_S = 1.0


def rpc_min_interval_s() -> float:
    """Spacing between RPC calls. Zero when a paid provider is recorded."""
    if paid_rpc_provider_recorded():
        return 0.0
    return PUBLIC_RPC_MIN_INTERVAL_S


def _json_ready(value: Any) -> Any:
    """Replace ``Decimal`` and ``datetime`` so ``json.dumps`` cannot raise."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def audit_text(report: dict[str, Any], *, mint: str, ticket_id: str) -> str:
    """ASCII checklist the risk seat journals. A KILL line is not optional."""
    verdict = str(report.get("verdict_recommendation", "KILL"))
    checks = report.get("checks") or []
    lines = [
        f"LEAD / TICKET-ID: {ticket_id}",
        f"Mint: {mint}",
        f"Verdict: {verdict}",
        "Residual Risks:",
    ]
    risks = [
        f"{index}. {row.get('id')}: {row.get('reason')}"
        for index, row in enumerate(checks, 1)
        if isinstance(row, dict) and row.get("status") in ("fail", "blind", "warn")
    ]
    if not risks:
        risks = ["1. none recorded"]
    lines.extend(risks)
    evidence = report.get("evidence") or []
    lines.append(f"Evidence: solana_rpc checks={len(evidence)}")
    if verdict == "CONDITIONAL":
        lines.append("Conditions: required checks must be fresh before an entry")
    if verdict == "KILL":
        lines.append("Final: KILL cannot be overridden")
    complete = isinstance(checks, list) and len(checks) == len(CHECK_IDS)
    lines.append(f"Self-Audit: All 10 checks completed: {'YES' if complete else 'NO'}")
    return "\n".join(lines)


def finalize_report(report: dict[str, Any], *, mint: str, ticket_id: str) -> dict[str, Any]:
    """Add the CLI fields. ``final`` is true only for KILL."""
    verdict = str(report.get("verdict_recommendation", "KILL"))
    checks = report.get("checks") or []
    body = {
        "ok": True,
        "mint": mint,
        "ticket_id": ticket_id,
        "verdict": verdict,
        "final": verdict == "KILL",
        "self_audit": "YES" if isinstance(checks, list) and len(checks) == len(CHECK_IDS) else "NO",
        "blind_checks": list(report.get("blind_checks") or []),
        "advisory_blind_checks": list(report.get("advisory_blind_checks") or []),
        "checks": checks,
        "score": report.get("score"),
        "text": audit_text(report, mint=mint, ticket_id=ticket_id),
    }
    ready = _json_ready(body)
    if not isinstance(ready, dict):
        return body
    return ready


async def audit_mint(
    rpc: RpcClient,
    *,
    mint: str,
    curve_account: str,
    creator: str | None,
    intended_size_lamports: int,
    ticket_id: str = "SOL-19700101-000",
) -> dict[str, Any]:
    """Run the ten checks once. The returned KILL is final for the caller to record."""
    report = await run_score_report(
        ScoreReportIn(
            mint=mint,
            curve_account=curve_account,
            creator=creator,
            intended_size_lamports=intended_size_lamports,
        ),
        rpc,
        thresholds=None,
    )
    return finalize_report(report, mint=mint, ticket_id=ticket_id)


def build_audit_rpc(
    settings: Settings, clock: Clock | None = None
) -> tuple[RpcClient, httpx.AsyncClient]:
    """RPC client with a short success cache. The caller closes the HTTP client."""
    active = clock if clock is not None else WallClock()
    client = httpx.AsyncClient()
    rpc = RpcClient(
        list(settings.rpc.endpoints),
        client,
        active,
        make_rng(settings.replay.seed),
        api_key=settings.rpc.api_key,
        cache_ttl_s=_AUDIT_CACHE_S,
        min_interval_s=rpc_min_interval_s(),
        retry=RetryPolicy(attempts=1, base_s=0.5, max_s=2.0),
    )
    return rpc, client


def resolve_curve_account(mint: str, curve_account: str | None) -> str:
    """Use the supplied curve account, or derive the bonding-curve PDA."""
    if curve_account:
        return curve_account
    return bonding_curve_pda(mint)


def dumps_audit(payload: dict[str, Any]) -> str:
    """One JSON line. ``Decimal`` weights and datetimes are encoded, never raised."""
    return json.dumps(_json_ready(payload))
