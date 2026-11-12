"""Parse public pump.fun launch and trade messages into desk records.

The parsers accept the fields PumpPortal and a Solana account notification
actually carry. Reserve fields are taken only as integer lamports and token
base units. A SOL float or a USD price is ignored so a buy and a sell cannot
disagree about the unit. This module does not open a socket and does not sign.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from pumpgrok.engine.models import PRICE_SCALE
from pumpgrok.execution.curve_math import spot
from pumpgrok.tools.onchain.curve_layout import CurveDecodeError, decode_curve_account_base64
from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda


@dataclass(frozen=True)
class Launch:
    """One newly observed pump.fun mint."""

    mint: str
    curve_account: str
    creator: str | None
    name: str


def _text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _int_field(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def launch_from_message(payload: dict[str, Any]) -> Launch | None:
    """A create message, or ``None`` when the mint or its curve cannot be named."""
    kind = _text(payload, "txType", "type", "method").lower()
    if kind and kind not in ("create", "subscribenewtoken"):
        return None
    mint = _text(payload, "mint")
    if len(mint) < 32:
        return None
    curve = _text(payload, "bondingCurveKey", "bonding_curve", "curve_account")
    if len(curve) < 32:
        try:
            curve = bonding_curve_pda(mint)
        except ValueError:
            return None
    creator = _text(payload, "traderPublicKey", "creator", "user") or None
    name = _text(payload, "name", "symbol") or mint[:8]
    return Launch(mint=mint, curve_account=curve, creator=creator, name=name)


def reserves_from_message(payload: dict[str, Any]) -> tuple[int, int] | None:
    """Virtual reserves as integers, or ``None`` when the message is not in base units."""
    sol = _int_field(
        payload,
        "virtualSolReserves",
        "virtual_sol_reserves",
        "vSolInBondingCurveLamports",
    )
    token = _int_field(
        payload,
        "virtualTokenReserves",
        "virtual_token_reserves",
        "vTokensInBondingCurve",
    )
    if sol is None or token is None or sol <= 0 or token <= 0:
        return None
    return sol, token


def scaled_price(sol_lamports: int, token_units: int) -> int:
    """PRICE_SCALE integer from integer reserves. The same conversion the exit seat uses."""
    lamports_per_token = spot(sol_lamports, token_units)
    return int((lamports_per_token * PRICE_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def tick_price_from_account(data: object) -> tuple[int, int, bool] | None:
    """``(scaled_price, slot_unused_zero, complete)`` from a base64 account, or ``None``."""
    try:
        state = decode_curve_account_base64(data)
    except CurveDecodeError:
        return None
    if state.layout != "pump":
        return None
    price = scaled_price(state.virtual_sol_reserves_lamports, state.virtual_token_reserves_units)
    return price, 0, state.complete


def price_from_trade(payload: dict[str, Any]) -> int | None:
    """Scaled price from integer reserves on a trade message. Float USD fields are ignored."""
    reserves = reserves_from_message(payload)
    if reserves is None:
        return None
    return scaled_price(reserves[0], reserves[1])


def account_notification_price(message: dict[str, Any]) -> int | None:
    """Scaled price from a Solana ``accountNotification``, or ``None`` when it will not decode."""
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    result = params.get("result")
    if not isinstance(result, dict):
        return None
    value = result.get("value")
    if not isinstance(value, dict):
        return None
    decoded = tick_price_from_account(value.get("data"))
    if decoded is None:
        return None
    return decoded[0]


def quote_is_actionable(quote: dict[str, Any]) -> bool:
    """True for one successful quote. A 429 is not actionable and is not retried."""
    return bool(quote.get("ok")) and quote.get("retry") is not True and quote.get("status") != 429


def ui_price_note(quote: dict[str, Any]) -> str:
    """One ASCII line naming raw amounts and decimals. Empty when the quote has no amounts."""
    if not quote.get("ok"):
        code = str(quote.get("code") or quote.get("error") or "unavailable")
        return f"jupiter quote {code} retry=false"
    raw_in = quote.get("in_amount_raw")
    raw_out = quote.get("out_amount_raw")
    dec_in = quote.get("input_decimals")
    dec_out = quote.get("output_decimals")
    if raw_in is None or raw_out is None:
        return "jupiter quote missing raw amounts"
    return (
        f"jupiter raw in={raw_in} decimals={dec_in} out={raw_out} decimals={dec_out} "
        f"unit={quote.get('amount_unit', 'raw')}"
    )


def sol_amount_ui(lamports: int) -> str:
    """SOL as a decimal string from integer lamports."""
    return format(Decimal(lamports) / Decimal(1_000_000_000), "f")
