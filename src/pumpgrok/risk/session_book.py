"""Resolve one paper book and keep sizing, halts and genesis on that lamport amount.

``paper_equity_usd`` is converted once, at session start, from a SOL/USD
quote. ``paper_equity_sol`` is the explicit SOL book. A configured USD book
that cannot be quoted refuses the session. An existing ledger whose genesis
lamports differ from the resolved book also refuses: the locked book is not
rewritten.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

from pumpgrok.config.settings import SafetyThresholds
from pumpgrok.core.ids import Clock
from pumpgrok.core.risk_budget import RiskBudget
from pumpgrok.core.units import LAMPORTS_PER_SOL, sol_to_lamports
from pumpgrok.ledger.store import LedgerStore
from pumpgrok.replay.clock import format_ts_utc
from pumpgrok.risk.limits import EngineLimits
from pumpgrok.tools.market.jupiter import get_quote

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
_RISK_SEAT = "bot-02-risk"


class BookUnresolved(Exception):
    """The configured book cannot be applied. The session must not start."""


@dataclass(frozen=True)
class SessionBook:
    """One resolved paper book. ``equity_lamports`` is the only amount consumers use."""

    equity_lamports: int
    denomination: str
    paper_equity_usd: Decimal | None
    sol_usd: Decimal | None
    evidence: str


def budget_for_book(budget: RiskBudget, book: SessionBook) -> RiskBudget:
    """Copy whose SOL equity is the resolved lamport book."""
    sol = Decimal(book.equity_lamports) / Decimal(LAMPORTS_PER_SOL)
    return budget.model_copy(update={"paper_equity_sol": sol})


def _meta_decimal(store: LedgerStore, key: str) -> Decimal | None:
    raw = store.get_meta(key)
    if raw is None or raw == "":
        return None
    return Decimal(raw)


def locked_book(store: LedgerStore, budget: RiskBudget) -> SessionBook | None:
    """The genesis book already on the ledger, or ``None`` when it has none.

    A configured USD book that this ledger was not opened with is unresolved.
    The locked lamports stay; a new price does not rewrite them.
    """
    existing = store.get_meta("initial_equity_lamports")
    if existing is None:
        return None
    lamports = int(existing)
    stored_usd = _meta_decimal(store, "session_paper_equity_usd")
    if budget.paper_equity_usd is not None and stored_usd != budget.paper_equity_usd:
        raise BookUnresolved(
            "ledger book was not opened from "
            f"paper_equity_usd={format(budget.paper_equity_usd, 'f')}; "
            "start a new paper ledger"
        )
    sol_usd = _meta_decimal(store, "session_sol_usd")
    evidence = store.get_meta("session_book_evidence")
    if not evidence:
        evidence = f"ledger initial_equity_lamports={lamports}"
    denomination = store.get_meta("session_book_denomination") or "ledger"
    return SessionBook(
        equity_lamports=lamports,
        denomination=denomination,
        paper_equity_usd=stored_usd if stored_usd is not None else budget.paper_equity_usd,
        sol_usd=sol_usd,
        evidence=evidence,
    )


def _sol_usd(quote: dict[str, Any]) -> Decimal | None:
    price = quote.get("price_out_ui_per_in_ui")
    if price not in (None, ""):
        try:
            value = Decimal(str(price))
        except (ArithmeticError, ValueError):
            value = Decimal(0)
        if value > 0:
            return value
    in_raw = quote.get("in_amount_raw")
    out_raw = quote.get("out_amount_raw")
    in_dec = quote.get("input_decimals")
    out_dec = quote.get("output_decimals")
    if (
        isinstance(in_raw, bool)
        or isinstance(out_raw, bool)
        or not isinstance(in_raw, int)
        or not isinstance(out_raw, int)
        or not isinstance(in_dec, int)
        or not isinstance(out_dec, int)
    ):
        return None
    if in_raw <= 0 or out_raw <= 0:
        return None
    in_ui = Decimal(in_raw) / (Decimal(10) ** in_dec)
    out_ui = Decimal(out_raw) / (Decimal(10) ** out_dec)
    if in_ui <= 0:
        return None
    return out_ui / in_ui


def book_from_sol(budget: RiskBudget) -> SessionBook:
    """The explicit SOL book. Zero lamports is unresolved."""
    lamports = budget.paper_equity_lamports
    if lamports <= 0:
        raise BookUnresolved("paper_equity_sol resolved to zero lamports")
    sol = format(budget.paper_equity_sol, "f")
    return SessionBook(
        equity_lamports=lamports,
        denomination="sol",
        paper_equity_usd=None,
        sol_usd=None,
        evidence=f"paper_equity_sol={sol} lamports={lamports}",
    )


def book_from_usd(budget: RiskBudget, quote: dict[str, Any]) -> SessionBook:
    """Convert ``paper_equity_usd`` at the quote. A missing price refuses."""
    usd = budget.paper_equity_usd
    if usd is None:
        raise BookUnresolved("paper_equity_usd is unset")
    if not quote.get("ok"):
        code = quote.get("code") or quote.get("error") or "unavailable"
        raise BookUnresolved(f"SOL/USD quote unavailable ({code}); session book not resolved")
    sol_usd = _sol_usd(quote)
    if sol_usd is None or sol_usd <= 0:
        raise BookUnresolved("SOL/USD quote has no positive price; session book not resolved")
    lamports = sol_to_lamports(usd / sol_usd)
    if lamports <= 0:
        raise BookUnresolved("USD book resolved to zero lamports")
    evidence = (
        f"paper_equity_usd={format(usd, 'f')} sol_usd={format(sol_usd, 'f')} "
        f"in_amount_raw={quote.get('in_amount_raw')} "
        f"out_amount_raw={quote.get('out_amount_raw')} "
        f"input_decimals={quote.get('input_decimals')} "
        f"output_decimals={quote.get('output_decimals')} "
        f"lamports={lamports}"
    )
    return SessionBook(
        equity_lamports=lamports,
        denomination="usd",
        paper_equity_usd=usd,
        sol_usd=sol_usd,
        evidence=evidence,
    )


async def resolve_session_book(
    budget: RiskBudget, client: httpx.AsyncClient, quote_url: str
) -> SessionBook:
    """USD when configured, otherwise the explicit SOL value. One quote, no retry."""
    if budget.paper_equity_usd is not None:
        quote = await get_quote(
            SOL_MINT,
            USDC_MINT,
            LAMPORTS_PER_SOL,
            50,
            client,
            quote_url=quote_url,
            input_decimals=9,
            output_decimals=6,
        )
        return book_from_usd(budget, quote)
    return book_from_sol(budget)


def _money_events(store: LedgerStore) -> bool:
    """True when the ledger already moved paper money without a recorded book."""
    money = {"FillRecorded", "OrderPlaced", "PositionOpened", "MarkToMarket", "HaltSet"}
    return any(event["type"] in money for event in store.read(limit=None))


def apply_book(store: LedgerStore, budget: RiskBudget, book: SessionBook) -> RiskBudget:
    """Write genesis on a new ledger, or refuse when the locked book differs."""
    resolved = budget_for_book(budget, book)
    existing = store.get_meta("initial_equity_lamports")
    if existing is None:
        if _money_events(store):
            raise BookUnresolved(
                "paper ledger has money events but no initial_equity_lamports genesis"
            )
        store.set_meta("initial_equity_lamports", str(book.equity_lamports))
        store.set_meta("session_book_evidence", book.evidence)
        store.set_meta("session_book_denomination", book.denomination)
        if book.sol_usd is not None:
            store.set_meta("session_sol_usd", format(book.sol_usd, "f"))
        if book.paper_equity_usd is not None:
            store.set_meta("session_paper_equity_usd", format(book.paper_equity_usd, "f"))
        return resolved
    if int(existing) != book.equity_lamports:
        raise BookUnresolved(
            f"ledger initial_equity_lamports is {existing}; "
            f"configured book is {book.equity_lamports}. "
            "A new book needs a new paper ledger."
        )
    return resolved


def _stable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return format(Decimal(str(value)), "f")
    if isinstance(value, dict):
        return {str(key): _stable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_stable(item) for item in value]
    return value


def config_hash(
    budget: RiskBudget,
    limits: EngineLimits,
    thresholds: SafetyThresholds,
    book: SessionBook,
    autonomy: str,
) -> str:
    """sha256 of the book, the autonomy level, and the risk and safety settings."""
    payload = {
        "autonomy": autonomy,
        "book_lamports": book.equity_lamports,
        "denomination": book.denomination,
        "sol_usd": None if book.sol_usd is None else format(book.sol_usd, "f"),
        "paper_equity_usd": (
            None if book.paper_equity_usd is None else format(book.paper_equity_usd, "f")
        ),
        "risk": _stable(budget.model_dump(mode="python")),
        "limits": _stable(limits.model_dump(mode="python")),
        "safety": _stable(thresholds.model_dump(mode="python")),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def open_blind_acceptance(store: LedgerStore) -> dict[str, Any] | None:
    """Latest unconsumed ``blind_accept`` approval, if a human recorded one."""
    latest: dict[str, Any] | None = None
    consumed: set[str] = set()
    for event in store.read(types=["ApprovalRecorded"], limit=None):
        payload = event.get("payload")
        if not isinstance(payload, dict) or payload.get("kind") != "blind_accept":
            continue
        approval_id = str(payload.get("id") or "")
        if payload.get("consumed") is True:
            consumed.add(approval_id)
            if latest is not None and str(latest.get("id")) == approval_id:
                latest = None
            continue
        if approval_id not in consumed:
            latest = payload
    return latest


def record_blind_acceptance(
    store: LedgerStore,
    clock: Clock,
    *,
    by: str,
    reason: str,
    approval_id: str,
) -> None:
    """Record a human accepting blind checks for one later session. It does not clear a KILL."""
    ts_utc = format_ts_utc(clock.now())
    store.append(
        ts_utc=ts_utc,
        source="paper",
        correlation_id="desk",
        idempotency_key=f"blind-accept|{approval_id}",
        type="ApprovalRecorded",
        payload={
            "source": "paper",
            "id": approval_id,
            "kind": "blind_accept",
            "approved_by": by,
            "at": ts_utc,
            "ticket_id": "",
            "mint": "",
            "size_lamports": 0,
            "token_units": 0,
            "max_slippage_bps": 0,
            "exit_rule_id": "",
            "raw_message": reason,
            "consumed": False,
        },
    )
    store.append(
        ts_utc=ts_utc,
        source="paper",
        correlation_id="desk",
        idempotency_key=f"blind-accept-note|{approval_id}",
        type="JournalNote",
        payload={
            "source": "paper",
            "seat": _RISK_SEAT,
            "note_kind": "note",
            "text": f"blind acceptance recorded by {by} id={approval_id} reason={reason}",
            "refs": [approval_id],
        },
    )


def record_session_start(
    store: LedgerStore,
    clock: Clock,
    *,
    book: SessionBook,
    budget: RiskBudget,
    limits: EngineLimits,
    thresholds: SafetyThresholds,
    autonomy: str,
) -> str:
    """Append the session book, autonomy, config hash and blind-acceptance decision."""
    ts_utc = format_ts_utc(clock.now())
    digest = config_hash(budget, limits, thresholds, book, autonomy)
    acceptance = open_blind_acceptance(store)
    accepted = acceptance is not None
    by = str(acceptance.get("approved_by")) if acceptance else "none"
    approval_id = str(acceptance.get("id")) if acceptance else "none"
    usd = format(book.paper_equity_usd, "f") if book.paper_equity_usd else "none"
    sol_usd = format(book.sol_usd, "f") if book.sol_usd is not None else "none"
    text = "\n".join(
        [
            "session start",
            f"autonomy={autonomy}",
            f"config_hash={digest}",
            f"book_lamports={book.equity_lamports}",
            f"denomination={book.denomination}",
            f"paper_equity_usd={usd}",
            f"sol_usd={sol_usd}",
            f"evidence={book.evidence}",
            f"blind_acceptance={'YES' if accepted else 'NO'}",
            f"blind_acceptance_by={by}",
            f"blind_acceptance_id={approval_id}",
        ]
    )
    rows: list[dict[str, Any]] = [
        {
            "ts_utc": ts_utc,
            "source": "paper",
            "correlation_id": "live-paper",
            "idempotency_key": f"session-start|{digest}|{ts_utc}",
            "type": "ConfigChanged",
            "payload": {
                "source": "paper",
                "by": "desk",
                "keys_changed": ["session_book", "config_hash"],
                "old_hash": "",
                "new_hash": digest,
            },
        },
        {
            "ts_utc": ts_utc,
            "source": "paper",
            "correlation_id": "live-paper",
            "idempotency_key": f"session-start-note|{digest}|{ts_utc}",
            "type": "JournalNote",
            "payload": {
                "source": "paper",
                "seat": _RISK_SEAT,
                "note_kind": "note",
                "text": text,
                "refs": [],
            },
        },
    ]
    if acceptance is not None:
        rows.append(
            {
                "ts_utc": ts_utc,
                "source": "paper",
                "correlation_id": "live-paper",
                "idempotency_key": f"consumed|{approval_id}",
                "type": "ApprovalRecorded",
                "payload": {
                    "source": "paper",
                    "id": approval_id,
                    "kind": "blind_accept",
                    "approved_by": by,
                    "at": str(acceptance.get("at") or ts_utc),
                    "ticket_id": "",
                    "mint": "",
                    "size_lamports": 0,
                    "token_units": 0,
                    "max_slippage_bps": 0,
                    "exit_rule_id": "",
                    "raw_message": str(acceptance.get("raw_message") or ""),
                    "consumed": True,
                },
            }
        )
    store.append_batch(rows)
    return digest
