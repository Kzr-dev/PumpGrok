"""Paper session on live public data.

Discovery, the ten-check risk audit, one paper buy through PaperAdapter, a
tick-driven exit, and a ledger journal. The human autonomy level and the
risk engine gate every entry. A daily-loss or drawdown halt blocks new
buys. Quotes are informational. Nothing in this module signs or broadcasts.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import connect as open_public_socket

from pumpgrok.config.settings import ExitsSettings, Settings
from pumpgrok.core.adapter import EXIT_RULE_REASONS, ExitRuleReason, OrderRequest, Side
from pumpgrok.core.errors import OrderRejectedError, OrderUnknownError
from pumpgrok.core.ids import Clock, IdFactory
from pumpgrok.core.risk_budget import RiskBudget
from pumpgrok.core.tool import RetryPolicy
from pumpgrok.engine.models import ExitPosition
from pumpgrok.execution.paper_adapter import PaperAdapter
from pumpgrok.execution.paper_adapter.models import CurveSnapshot
from pumpgrok.ledger.autonomy import read_autonomy
from pumpgrok.ledger.paths import PAPER_LEDGER_PATH
from pumpgrok.ledger.store import LedgerStore
from pumpgrok.ledger.tickets import allocate_ticket
from pumpgrok.replay.clock import WallClock, format_ts_utc
from pumpgrok.replay.rng import make_rng
from pumpgrok.risk.engine import RiskEngine
from pumpgrok.risk.fast_audit import audit_mint, dumps_audit, rpc_min_interval_s
from pumpgrok.risk.limits import EngineLimits
from pumpgrok.risk.session_book import (
    apply_book,
    budget_for_book,
    locked_book,
    record_session_start,
    resolve_session_book,
)
from pumpgrok.supervisor.exit_rules import LedgerExitBook
from pumpgrok.supervisor.exit_watch import PriceTick, TickExitWatch
from pumpgrok.tools.market.jupiter import QUOTE_URL, get_quote
from pumpgrok.tools.market.launch_feed import (
    Launch,
    account_notification_price,
    launch_from_message,
    quote_is_actionable,
    scaled_price,
    ui_price_note,
)
from pumpgrok.tools.onchain.curve_layout import (
    CurveDecodeError,
    decode_curve_account_base64,
)
from pumpgrok.tools.onchain.rpc import RpcClient

log = logging.getLogger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"
_TOKEN_DECIMALS = 6
_SOL_DECIMALS = 9
QuoteCall = Callable[[tuple[str, str, int, int, int, int]], Awaitable[dict[str, Any]]]


class CurveFeed(Protocol):
    """Curve reads for the adapter and the synchronous liquidity floor."""

    async def curve(self, mint: str) -> CurveSnapshot | None: ...

    def reserves(self, mint: str) -> tuple[int, int] | None: ...


@dataclass
class PaperSessionReport:
    """What one timed paper session did. Counts are integers, never money."""

    audits: int = 0
    kills: int = 0
    buys: int = 0
    sells: int = 0
    halted: str | None = None
    autonomy: str = "manual-ticket"
    journaled: int = 0
    quotes_stale: int = 0
    attempted_lamports: list[int] = field(default_factory=list)


@dataclass
class LivePaperDeps:
    """Injected edges. Tests pass fakes. The CLI passes the public clients."""

    clock: Clock
    store: LedgerStore
    risk: RiskEngine
    adapter: PaperAdapter
    exits: ExitsSettings
    budget: RiskBudget
    limits: EngineLimits
    ids: IdFactory
    autonomy: str
    discover: Callable[[], Awaitable[list[Launch]]]
    audit: Callable[[Launch, str, int], Awaitable[dict[str, Any]]]
    quote: QuoteCall
    price_for: Callable[[str], Awaitable[int]]
    ticks: asyncio.Queue[PriceTick | None] = field(default_factory=asyncio.Queue)
    record: Callable[[dict[str, Any]], None] | None = None
    slippage_bps: int = 100


def trade_size_lamports(equity_lamports: int, budget: RiskBudget, limits: EngineLimits) -> int:
    """Buy size from the risk budget fraction of equity, not below the engine minimum.

    The fraction and the minimum both come from settings. This function does
    not invent a third number: a fraction that lands under the minimum is
    returned as that fraction and the risk engine refuses it.
    """
    del limits
    if equity_lamports <= 0:
        return 0
    return int(Decimal(equity_lamports) * budget.max_trade_fraction)


def _journal(
    store: LedgerStore,
    clock: Clock,
    *,
    key: str,
    text: str,
    refs: list[str],
    note_kind: str = "verdict",
    seat: str = "bot-09-journal",
) -> None:
    store.append(
        ts_utc=format_ts_utc(clock.now()),
        source="paper",
        correlation_id="live-paper",
        idempotency_key=key,
        type="JournalNote",
        payload={
            "source": "paper",
            "seat": seat,
            "note_kind": note_kind,
            "text": text,
            "refs": refs,
        },
    )


def halt_reason(risk: RiskEngine) -> str | None:
    """Fire a fresh daily-loss or drawdown halt, and report one that is already latched."""
    fresh = risk.evaluate_halts(source="paper", correlation_id="live-paper")
    state = risk._fold()
    if state.halt.active:
        return str(state.halt.reason or fresh or "halt")
    return fresh


class RpcCurveFeed:
    """``getAccountInfo`` curve reads. A pump decode is source rpc, layout pump."""

    def __init__(self, rpc: RpcClient, curves: dict[str, str]) -> None:
        self._rpc = rpc
        self._curves = curves
        self._last: dict[str, tuple[int, int]] = {}

    def remember(self, mint: str, curve_account: str) -> None:
        self._curves[mint] = curve_account

    async def curve(self, mint: str) -> CurveSnapshot | None:
        account = self._curves.get(mint)
        if not account:
            return None
        try:
            result = await self._rpc.call("getAccountInfo", [account, {"encoding": "base64"}])
        except Exception as exc:
            log.warning("curve read failed error=%s", type(exc).__name__)
            return None
        if not isinstance(result, dict) or result.get("value") is None:
            return None
        value = result["value"]
        if not isinstance(value, dict):
            return None
        try:
            state = decode_curve_account_base64(value.get("data"))
        except CurveDecodeError:
            return None
        self._last[mint] = (
            state.virtual_sol_reserves_lamports,
            state.virtual_token_reserves_units,
        )
        context = result.get("context")
        slot = context.get("slot") if isinstance(context, dict) else None
        return CurveSnapshot(
            mint=mint,
            s_lamports=state.virtual_sol_reserves_lamports,
            t_units=state.virtual_token_reserves_units,
            complete=state.complete,
            source="rpc",
            layout=state.layout,
            slot=slot if isinstance(slot, int) else None,
            ts=self._rpc.clock.now(),
        )

    def reserves(self, mint: str) -> tuple[int, int] | None:
        return self._last.get(mint)


async def _sell(
    adapter: PaperAdapter,
    ids: IdFactory,
    store: LedgerStore,
    clock: Clock,
    position: ExitPosition,
    *,
    reason: str,
    slippage_bps: int,
) -> None:
    """One exit-seat sell. ``unknown`` is not retried."""
    ticket = "SOL-19700101-001"
    for event in store.read(types=["PositionOpened"], limit=None):
        payload = event.get("payload")
        if isinstance(payload, dict) and payload.get("mint") == position.mint:
            ticket = str(payload.get("ticket_id") or ticket)
    rule: ExitRuleReason | None = None
    if reason in EXIT_RULE_REASONS:
        rule = reason  # type: ignore[assignment]
    request = OrderRequest(
        ticket_id=ticket,
        mint=position.mint,
        side=Side.SELL,
        decision_id=ids.ulid(),
        token_units=int(position.token_amount),
        max_slippage_bps=max(1, slippage_bps),
        approval_id=f"apr-{ids.ulid()}",
        exit_reason=rule,
        correlation_id="live-paper",
    )
    try:
        await adapter.place(
            request,
            seat="bot-06-exit",
            exit_reason=request.exit_reason,
            correlation_id="live-paper",
        )
    except OrderUnknownError:
        log.warning("exit unknown mint=%s; not retried", position.mint[:8])
    except OrderRejectedError as exc:
        log.info("exit refused mint=%s reason=%s", position.mint[:8], exc.reason)


def _entry_allowed(
    deps: LivePaperDeps, launch: Launch, verdict: str, halted: str | None, open_count: int
) -> bool:
    """One autonomous-paper CLEAR entry while the book is under its position cap."""
    if halted is not None or deps.autonomy != "autonomous-paper" or verdict != "CLEAR":
        return False
    if deps.adapter._open_units(launch.mint) > 0:
        return False
    return open_count < deps.budget.max_open_positions


async def _backoff(deps: LivePaperDeps, quote: dict[str, Any], deadline: float) -> None:
    backoff = quote.get("backoff_s")
    if isinstance(backoff, bool) or not isinstance(backoff, (int, float)) or backoff <= 0:
        return
    remaining = deadline - deps.clock.now()
    await deps.clock.sleep(min(float(backoff), max(0.0, remaining)))


async def _place_buy(deps: LivePaperDeps, launch: Launch, ticket: str, size: int, note: str) -> str:
    """Place one buy. ``unknown`` and a refusal are journaled and not retried."""
    request = OrderRequest(
        ticket_id=ticket,
        mint=launch.mint,
        side=Side.BUY,
        decision_id=deps.ids.ulid(),
        size_lamports=size,
        max_slippage_bps=deps.slippage_bps,
        approval_id=f"apr-{deps.ids.ulid()}",
        correlation_id="live-paper",
    )
    try:
        fill = await deps.adapter.place(
            request,
            seat="bot-04-sniper",
            creator=launch.creator,
            correlation_id="live-paper",
        )
    except OrderUnknownError:
        _journal(
            deps.store,
            deps.clock,
            key=f"buy-unknown|{launch.mint}|{ticket}",
            text=f"buy unknown; not retried; {note}",
            refs=[ticket, launch.mint],
        )
        return "unknown"
    except OrderRejectedError as exc:
        _journal(
            deps.store,
            deps.clock,
            key=f"buy-refused|{launch.mint}|{ticket}",
            text=f"buy refused {exc.reason}; not retried; {note}",
            refs=[ticket, launch.mint],
        )
        return "refused"
    _journal(
        deps.store,
        deps.clock,
        key=f"buy|{launch.mint}|{ticket}",
        text=f"paper buy filled={fill.filled_units}; {note}",
        refs=[ticket, launch.mint],
    )
    if deps.record is not None:
        snap = await deps.adapter._read_curve(launch.mint)
        if snap is not None:
            deps.record(
                {
                    "ts_utc": format_ts_utc(deps.clock.now()),
                    "kind": "tick",
                    "mint": launch.mint,
                    "S": snap.s_lamports,
                    "T": snap.t_units,
                    "complete": snap.complete,
                    "source": "rpc",
                }
            )
    return "filled"


async def _handle_launch(
    deps: LivePaperDeps,
    launch: Launch,
    *,
    halted: str | None,
    deadline: float,
    killed: set[str],
    attempted: set[str],
    report: PaperSessionReport,
) -> None:
    """Audit one launch and, when the gates allow, place a single buy."""
    if launch.mint in killed or launch.mint in attempted:
        return
    size = trade_size_lamports(deps.adapter.equity_lamports(), deps.budget, deps.limits)
    report.attempted_lamports.append(size)
    ticket = allocate_ticket(
        deps.store,
        ts_utc=format_ts_utc(deps.clock.now()),
        source="paper",
        correlation_id="live-paper",
        seat="bot-01-scout",
        title=launch.name,
    )
    audited = await deps.audit(launch, ticket, size)
    report.audits += 1
    verdict = str(audited.get("verdict", "KILL"))
    _journal(
        deps.store,
        deps.clock,
        key=f"audit|{launch.mint}|{ticket}",
        text=str(audited.get("text") or verdict),
        refs=[ticket, launch.mint],
        note_kind="safety_report",
        seat="bot-02-risk",
    )
    report.journaled += 1
    if deps.record is not None:
        deps.record(
            {
                "ts_utc": format_ts_utc(deps.clock.now()),
                "kind": "launch",
                "mint": launch.mint,
                "name": launch.name,
                "verdict": verdict,
                "source": "pumpportal",
            }
        )
    if verdict == "KILL" or audited.get("final") is True:
        killed.add(launch.mint)
        report.kills += 1
        return
    open_count = len(await deps.adapter.positions())
    if not _entry_allowed(deps, launch, verdict, halted, open_count):
        return
    quote = await deps.quote(
        (SOL_MINT, launch.mint, size, deps.slippage_bps, _SOL_DECIMALS, _TOKEN_DECIMALS)
    )
    note = ui_price_note(quote)
    if not quote_is_actionable(quote):
        report.quotes_stale += 1
        attempted.add(launch.mint)
        _journal(
            deps.store,
            deps.clock,
            key=f"quote-stale|{launch.mint}|{ticket}",
            text=note,
            refs=[ticket, launch.mint],
        )
        report.journaled += 1
        await _backoff(deps, quote, deadline)
        return
    attempted.add(launch.mint)
    outcome = await _place_buy(deps, launch, ticket, size, note)
    report.journaled += 1
    if outcome == "filled":
        report.buys += 1


async def _exit_open(
    deps: LivePaperDeps, book: LedgerExitBook, watch: TickExitWatch, report: PaperSessionReport
) -> None:
    book.sync(deps.store)
    sold = await watch.drain(deps.ticks)
    sold.extend(await watch.poll_stale(deps.price_for))
    report.sells += len(sold)
    for mint in sold:
        _journal(
            deps.store,
            deps.clock,
            key=f"exit|{mint}|{format_ts_utc(deps.clock.now())}",
            text=f"paper exit mint={mint}",
            refs=[mint],
        )
        report.journaled += 1


def _close_session(deps: LivePaperDeps, report: PaperSessionReport) -> None:
    _journal(
        deps.store,
        deps.clock,
        key=f"session|{format_ts_utc(deps.clock.now())}|{report.buys}|{report.kills}",
        text=(
            f"paper session audits={report.audits} kills={report.kills} "
            f"buys={report.buys} sells={report.sells} halt={report.halted or 'none'} "
            f"autonomy={report.autonomy}"
        ),
        refs=[],
        note_kind="review",
    )
    report.journaled += 1


async def run_live_paper(deps: LivePaperDeps, *, duration_s: float) -> PaperSessionReport:
    """Run until ``duration_s`` of injected clock time has passed."""
    report = PaperSessionReport(autonomy=deps.autonomy)
    killed: set[str] = set()
    attempted: set[str] = set()
    book = LedgerExitBook(deps.exits)
    watch = TickExitWatch(book, _bound_sell(deps), deps.clock)
    deadline = deps.clock.now() + duration_s
    while deps.clock.now() < deadline:
        reason = halt_reason(deps.risk)
        report.halted = reason
        for launch in await deps.discover():
            await _handle_launch(
                deps,
                launch,
                halted=reason,
                deadline=deadline,
                killed=killed,
                attempted=attempted,
                report=report,
            )
        await _exit_open(deps, book, watch, report)
        remaining = deadline - deps.clock.now()
        if remaining <= 0:
            break
        await deps.clock.sleep(min(1.0, remaining))
    _close_session(deps, report)
    return report


def _bound_sell(
    deps: LivePaperDeps,
) -> Callable[[ExitPosition, int, str], Awaitable[None]]:
    async def sell(position: ExitPosition, price: int, reason: str) -> None:
        del price
        await _sell(
            deps.adapter,
            deps.ids,
            deps.store,
            deps.clock,
            position,
            reason=reason,
            slippage_bps=deps.slippage_bps,
        )

    return sell


class SessionRecorder:
    """Append ticks and write a replay manifest when the session closes."""

    def __init__(self, directory: Path, *, dataset: str, seed_hint: int) -> None:
        self.directory = directory
        self.dataset = dataset
        self.seed_hint = seed_hint
        self._rows: list[dict[str, Any]] = []

    def add(self, row: dict[str, Any]) -> None:
        self._rows.append(row)

    def close(self) -> Path | None:
        """Write ``events.jsonl`` and ``manifest.json``. ``None`` when nothing was recorded."""
        if not self._rows:
            return None
        self.directory.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(row, separators=(",", ":")) for row in self._rows]
        body = ("\n".join(lines) + "\n").encode("utf-8")
        events = self.directory / "events.jsonl"
        events.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        first = str(self._rows[0].get("ts_utc", ""))
        last = str(self._rows[-1].get("ts_utc", ""))
        manifest = {
            "dataset": self.dataset,
            "recorded_from_utc": first,
            "recorded_to_utc": last,
            "seed_hint": self.seed_hint,
            "sources": {"rpc": "live-paper", "pumpportal": "live-paper"},
            "sha256": {"events.jsonl": digest},
            "notes": "Recorded by pumpgrok run --record. Curve ticks only; no keys.",
        }
        (self.directory / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        return self.directory


def default_quote(client: Any, *, quote_url: str = QUOTE_URL) -> QuoteCall:
    """One Swap V2 quote for both buy and sell sides. The URL is the settings value."""

    async def quote(parts: tuple[str, str, int, int, int, int]) -> dict[str, Any]:
        input_mint, output_mint, amount_s, slip_s, dec_in_s, dec_out_s = parts
        amount = int(amount_s)
        if amount <= 0:
            return {"ok": False, "error": "data_stale", "code": "data_stale", "retry": False}
        return await get_quote(
            input_mint,
            output_mint,
            amount,
            int(slip_s),
            client,
            quote_url=quote_url,
            input_decimals=int(dec_in_s),
            output_decimals=int(dec_out_s),
        )

    return quote


def audit_with(rpc: RpcClient) -> Callable[[Launch, str, int], Awaitable[dict[str, Any]]]:
    async def audit(launch: Launch, ticket: str, size: int) -> dict[str, Any]:
        try:
            report = await audit_mint(
                rpc,
                mint=launch.mint,
                curve_account=launch.curve_account,
                creator=launch.creator,
                intended_size_lamports=size,
                ticket_id=ticket,
            )
            dumps_audit(report)
        except (TypeError, ValueError):
            return {
                "ok": False,
                "mint": launch.mint,
                "ticket_id": ticket,
                "verdict": "KILL",
                "final": True,
                "text": f"LEAD / TICKET-ID: {ticket}\nVerdict: KILL\nreport was not serializable",
                "checks": [],
                "blind_checks": [],
                "advisory_blind_checks": [],
            }
        return report

    return audit


async def price_from_feed(feed: CurveFeed, mint: str) -> int:
    snap = await feed.curve(mint)
    if snap is None or snap.t_units <= 0:
        return 0
    return scaled_price(snap.s_lamports, snap.t_units)


def _launch_row(payload: dict[str, Any]) -> dict[str, str] | None:
    launch = launch_from_message(payload)
    if launch is None:
        return None
    return {"mint": launch.mint, "curve_account": launch.curve_account, "name": launch.name}


async def collect_launches(
    url: str, timeout_s: float, limit: int, clock: Clock
) -> list[dict[str, str]]:
    """Collect create messages until ``limit`` or the timeout."""
    found: list[dict[str, str]] = []
    if timeout_s <= 0 or limit <= 0 or not _public_url(url, ("wss", "https")):
        return found
    try:
        async with (
            clock.timeout(timeout_s),
            open_public_socket(url, open_timeout=timeout_s) as socket,
        ):
            await socket.send(json.dumps({"method": "subscribeNewToken"}))
            async for raw in socket:
                row = _row_from_raw(raw)
                if row is None:
                    continue
                found.append(row)
                if len(found) >= limit:
                    break
    except TimeoutError:
        return found
    except Exception as exc:
        log.warning("discovery unavailable error=%s", type(exc).__name__)
    return found


def _row_from_raw(raw: object) -> dict[str, str] | None:
    if not isinstance(raw, str):
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return _launch_row(payload)


def recordings_dir(name: str) -> Path:
    """Dataset directory for ``run --record``. The name is a single path segment."""
    return Path("var") / "recordings" / name


def paper_ledger() -> Path:
    return PAPER_LEDGER_PATH


def _public_url(url: str, schemes: tuple[str, ...]) -> bool:
    """True for a TLS URL with no userinfo. The URL is not logged."""
    parts = urlsplit(url)
    hosted = parts.scheme in schemes and bool(parts.hostname)
    secret = bool(parts.username or parts.password or "@" in parts.netloc)
    return hosted and not secret


def _rpc_wss(https_url: str) -> str | None:
    """Websocket form of a keyless https RPC endpoint. Query strings are dropped."""
    if not _public_url(https_url, ("https",)):
        return None
    parts = urlsplit(https_url)
    host = parts.hostname or ""
    netloc = f"{host}:{parts.port}" if parts.port else host
    return urlunsplit(("wss", netloc, parts.path, "", ""))


async def _listen_creates(url: str, queue: asyncio.Queue[Launch], clock: Clock) -> None:
    """Subscribe to new pump.fun mints. Failures wait on the clock and try again."""
    if not _public_url(url, ("wss", "https")):
        log.warning("discovery endpoint refused")
        return
    while True:
        try:
            async with open_public_socket(url, open_timeout=10) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                async for raw in ws:
                    if not isinstance(raw, str):
                        continue
                    try:
                        payload = json.loads(raw)
                    except ValueError:
                        continue
                    if isinstance(payload, dict):
                        launch = launch_from_message(payload)
                        if launch is not None:
                            queue.put_nowait(launch)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("discovery unavailable error=%s", type(exc).__name__)
            await clock.sleep(1.0)


async def _listen_accounts(
    wss_url: str,
    curves: dict[str, str],
    ticks: asyncio.Queue[PriceTick | None],
    clock: Clock,
) -> None:
    """Subscribe to curve accounts. A decode failure is not a price."""
    if not _public_url(wss_url, ("wss",)):
        log.warning("account watch endpoint refused")
        return
    while True:
        try:
            async with open_public_socket(wss_url, open_timeout=10) as socket:
                await _account_loop(socket, curves, ticks, clock)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("account watch unavailable error=%s", type(exc).__name__)
            await clock.sleep(1.0)


def _ack_subscription(
    payload: dict[str, Any], pending: dict[int, str], by_sub: dict[int, str]
) -> bool:
    """Record a subscribe acknowledgement. True when this message was that ack."""
    request_id = payload.get("id")
    result = payload.get("result")
    if not isinstance(request_id, int) or not isinstance(result, int):
        return False
    mint = pending.get(request_id)
    if mint is not None:
        by_sub[result] = mint
    return True


def _notification_tick(
    payload: dict[str, Any], by_sub: dict[int, str], now: float
) -> PriceTick | None:
    params = payload.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("subscription"), int):
        return None
    mint = by_sub.get(int(params["subscription"]), "")
    price = account_notification_price(payload)
    if price is None or not mint:
        return None
    return PriceTick(mint=mint, price=price, ts=now, source="ws")


async def _account_loop(
    socket: Any,
    curves: dict[str, str],
    ticks: asyncio.Queue[PriceTick | None],
    clock: Clock,
) -> None:
    """Map each subscription id back to the mint that owns the curve account."""
    next_id = 1
    pending: dict[int, str] = {}
    by_sub: dict[int, str] = {}
    sent: set[str] = set()
    while True:
        next_id = await _subscribe_missing(socket, curves, pending, sent, next_id)
        raw = await socket.recv()
        payload = _json_object(raw)
        if payload is None or _ack_subscription(payload, pending, by_sub):
            continue
        tick = _notification_tick(payload, by_sub, clock.now())
        if tick is not None:
            ticks.put_nowait(tick)


def _json_object(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, str):
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


async def _subscribe_missing(
    socket: Any,
    curves: dict[str, str],
    pending: dict[int, str],
    sent: set[str],
    next_id: int,
) -> int:
    for mint, account in list(curves.items()):
        if account in sent:
            continue
        body = {
            "jsonrpc": "2.0",
            "id": next_id,
            "method": "accountSubscribe",
            "params": [account, {"encoding": "base64", "commitment": "confirmed"}],
        }
        await socket.send(json.dumps(body))
        pending[next_id] = mint
        sent.add(account)
        next_id += 1
    return next_id


async def _session_budget(
    settings: Settings, store: LedgerStore, client: httpx.AsyncClient, clock: Clock
) -> tuple[RiskBudget, str]:
    """Resolve or reuse the book, then record the session-start row."""
    opened = locked_book(store, settings.risk)
    if opened is None:
        opened = await resolve_session_book(settings.risk, client, settings.jupiter.quote_url)
        budget = apply_book(store, settings.risk, opened)
    else:
        budget = budget_for_book(settings.risk, opened)
    autonomy = read_autonomy(store)
    record_session_start(
        store,
        clock,
        book=opened,
        budget=budget,
        limits=settings.limits,
        thresholds=settings.safety.thresholds,
        autonomy=autonomy,
    )
    return budget, autonomy


async def serve_public_paper(
    settings: Settings,
    *,
    duration_s: float,
    record_name: str | None,
    ledger_path: Path | None = None,
) -> PaperSessionReport:
    """One paper session against public market data. Live mode never reaches here."""
    clock = WallClock()
    path = ledger_path if ledger_path is not None else PAPER_LEDGER_PATH
    store = LedgerStore(str(path))
    client = httpx.AsyncClient()
    try:
        budget, autonomy = await _session_budget(settings, store, client, clock)
        rpc = RpcClient(
            list(settings.rpc.endpoints),
            client,
            clock,
            make_rng(settings.replay.seed),
            api_key=settings.rpc.api_key,
            cache_ttl_s=2.0,
            min_interval_s=rpc_min_interval_s(),
            retry=RetryPolicy(attempts=1, base_s=0.5, max_s=2.0),
        )
        curves: dict[str, str] = {}
        feed = RpcCurveFeed(rpc, curves)
        ids = IdFactory(clock, make_rng(settings.replay.seed))
        risk = RiskEngine(
            store=store,
            budget=budget,
            ids=IdFactory(clock, make_rng(settings.replay.seed)),
            clock=clock,
            limits=settings.limits,
            autonomy=autonomy,  # type: ignore[arg-type]
            get_reserves=feed.reserves,
            env=os.environ,
        )
        adapter = PaperAdapter(
            store=store,
            risk=risk,
            ids=ids,
            clock=clock,
            rng=make_rng(settings.replay.seed),
            budget=budget,
            limits=settings.limits,
            price_source=feed,
            p_drop=0.0,
            source="paper",
        )
        launches: asyncio.Queue[Launch] = asyncio.Queue()
        ticks: asyncio.Queue[PriceTick | None] = asyncio.Queue()
        recorder = (
            SessionRecorder(
                recordings_dir(record_name), dataset=record_name, seed_hint=settings.replay.seed
            )
            if record_name
            else None
        )

        async def discover() -> list[Launch]:
            found: list[Launch] = []
            while True:
                try:
                    launch = launches.get_nowait()
                except asyncio.QueueEmpty:
                    break
                feed.remember(launch.mint, launch.curve_account)
                found.append(launch)
            return found

        async def price_for(mint: str) -> int:
            return await price_from_feed(feed, mint)

        deps = LivePaperDeps(
            clock=clock,
            store=store,
            risk=risk,
            adapter=adapter,
            exits=settings.exits,
            budget=budget,
            limits=settings.limits,
            ids=ids,
            autonomy=autonomy,
            discover=discover,
            audit=audit_with(rpc),
            quote=default_quote(client, quote_url=settings.jupiter.quote_url),
            price_for=price_for,
            ticks=ticks,
            record=None if recorder is None else recorder.add,
            slippage_bps=max(1, int(settings.limits.max_slippage_bps)),
        )
        create_task = asyncio.create_task(
            _listen_creates(settings.data.ws_url, launches, clock), name="pump-creates"
        )
        wss = _rpc_wss(settings.rpc.endpoints[0]) if settings.rpc.endpoints else None
        account_task: asyncio.Task[None] | None = None
        if wss is not None:
            account_task = asyncio.create_task(
                _listen_accounts(wss, curves, ticks, clock), name="curve-accounts"
            )
        try:
            report = await run_live_paper(deps, duration_s=duration_s)
        finally:
            create_task.cancel()
            if account_task is not None:
                account_task.cancel()
            await asyncio.gather(
                create_task, *([account_task] if account_task else []), return_exceptions=True
            )
        if recorder is not None:
            recorder.close()
        return report
    finally:
        store.close()
        await client.aclose()
