"""PumpPortal discovery, and a Solana logs fallback when that socket is down.

PumpPortal's free method is ``subscribeNewToken`` on
``wss://pumpportal.fun/api/data``. ``subscribeTokenTrade`` is metered and
needs an API key, so this desk does not send it. One connection carries the
subscription. A refused handshake waits on an exponential delay with jitter
instead of reconnecting every second. While the portal is quiet, create
events are read from ``logsSubscribe`` on the pump.fun program.
"""

from __future__ import annotations

import json
import random
from typing import Any

from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PORTAL_NEW_TOKEN = {"method": "subscribeNewToken"}
_CREATE_MARK = "Instruction: Create"


def portal_headers() -> dict[str, str]:
    """Headers for the data socket. No key, no userinfo."""
    return {"User-Agent": "pumpgrok-paper/0.1", "Origin": "https://pumpportal.fun"}


def handshake_status(exc: BaseException) -> int | None:
    """HTTP status from a websocket handshake error, when the library exposes one."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def discovery_delay(attempt: int, rng: random.Random, *, denied: bool) -> float:
    """Seconds before the next connect. A 403 starts at 30s and doubles, capped at 120s."""
    floor = 30.0 if denied else 1.0
    step = min(max(attempt, 0), 6)
    growth = float(min(floor * (2**step), 120.0))
    jitter = float(rng.random()) * growth * 0.25
    return growth + jitter


def logs_create_signature(message: dict[str, Any]) -> str | None:
    """Signature of a pump create log, or ``None`` when this notice is not one."""
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    result = params.get("result")
    if not isinstance(result, dict):
        return None
    value = result.get("value")
    if not isinstance(value, dict) or value.get("err") is not None:
        return None
    logs = value.get("logs")
    if not isinstance(logs, list) or not any(_CREATE_MARK in str(line) for line in logs):
        return None
    signature = value.get("signature")
    if not isinstance(signature, str) or not signature:
        return None
    return signature


def _account_key(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        pubkey = value.get("pubkey")
        if isinstance(pubkey, str):
            return pubkey
    return ""


def _create_pair(item: dict[str, Any]) -> tuple[str, str] | None:
    if _account_key(item.get("programId")) != PUMP_PROGRAM:
        return None
    accounts = item.get("accounts")
    if not isinstance(accounts, list) or len(accounts) < 3:
        return None
    mint = _account_key(accounts[0])
    curve = _account_key(accounts[2])
    if len(mint) < 32 or len(curve) < 32:
        return None
    try:
        derived = bonding_curve_pda(mint)
    except ValueError:
        return None
    if derived != curve:
        return None
    return mint, curve


def mint_from_create_transaction(tx: dict[str, Any]) -> tuple[str, str] | None:
    """``(mint, curve)`` when the transaction is a pump create and the curve PDA matches."""
    meta = tx.get("meta")
    logs = meta.get("logMessages") if isinstance(meta, dict) else None
    if not isinstance(logs, list) or not any(_CREATE_MARK in str(line) for line in logs):
        return None
    message = tx.get("transaction")
    body = message.get("message") if isinstance(message, dict) else None
    if not isinstance(body, dict) or not isinstance(body.get("instructions"), list):
        return None
    for item in body["instructions"]:
        if isinstance(item, dict):
            pair = _create_pair(item)
            if pair is not None:
                return pair
    return None


def logs_subscribe_body(request_id: int) -> str:
    """One ``logsSubscribe`` for mentions of the pump.fun program."""
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "logsSubscribe",
        "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "confirmed"}],
    }
    return json.dumps(body)


def portal_subscribe_body() -> str:
    """The free new-token subscription. Trade streams are not requested."""
    return json.dumps(PORTAL_NEW_TOKEN)
