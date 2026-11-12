"""Jupiter Swap V2 quote, informational only (STRUCT-002, R44, R38).

``GET`` ``https://api.jup.ag/swap/v2/order`` with no ``taker`` so all routers
compete. Never ``/execute``, ``/build`` or ``/submit``: this module cannot
place an order. The optional ``JUPITER_API_KEY`` is read once in the
composition root (``resolve_api_key``), carried as ``SecretStr`` and sent as
``x-api-key``; the keyless state is logged, never the key (Pitfall 3).

v1 unit trap (Pitfall 2): v1 ``priceImpactPct`` was a decimal ratio while v2
``priceImpact`` is percentage points. This module maps
``price_impact_ratio = priceImpact / 100`` and never reads ``priceImpactPct``.
``inAmount``/``outAmount`` handling follows the hand-authored fixture
(ASSUMED A2): both are mapped defensively with ``.get``.
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any

import httpx
from pydantic import SecretStr

logger = logging.getLogger(__name__)

QUOTE_URL = "https://api.jup.ag/swap/v2/order"

_BANNED_URL_MARKERS = ("lite-api", "/swap/v1", "/execute", "/build", "/submit")


def resolve_api_key() -> SecretStr:
    """Read ``JUPITER_API_KEY`` once in the composition root (Open Q6).

    This is deliberately not a ``PUMPGROK_`` Settings field: TOML must never
    hold secrets. Empty means keyless.
    """
    return SecretStr(os.environ.get("JUPITER_API_KEY", ""))


def _raw_amount(value: object) -> int | None:
    """Parse a Jupiter integer-string amount. ``None`` when it is not a raw integer."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _ui_amount(raw: int | None, decimals: int | None) -> str | None:
    """UI amount as a decimal string. ``None`` until both the raw amount and decimals exist."""
    if raw is None or decimals is None:
        return None
    scale = Decimal(10) ** int(decimals)
    return format(Decimal(raw) / scale, "f")


def _backoff_seconds(response: httpx.Response) -> float:
    """Seconds to wait before a later quote. Never a reason to resend an order."""
    retry_after = response.headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            return 1.0
    if response.headers.get("x-ratelimit-reset"):
        return 1.0
    return 1.0


def _rate_limited(response: httpx.Response) -> dict[str, Any]:
    """HTTP 429 is stale data, not an order to retry."""
    logger.warning("jupiter quote stale status=429")
    return {
        "ok": False,
        "error": "data_stale",
        "code": "data_stale",
        "blind": True,
        "status": 429,
        "retry": False,
        "backoff_s": _backoff_seconds(response),
        "amount_unit": "raw",
    }


def _unsupported(reason: str) -> dict[str, Any]:
    logger.warning("jupiter quote refused reason=%s", reason)
    return {
        "ok": False,
        "error": reason,
        "code": "data_stale",
        "blind": True,
        "retry": False,
        "amount_unit": "raw",
    }


def _unit_fields(
    data: dict[str, Any],
    *,
    input_decimals: int | None,
    output_decimals: int | None,
) -> dict[str, Any]:
    """Raw amounts plus explicit decimals. UI prices are omitted until both decimals are set."""
    in_raw = _raw_amount(data.get("inAmount"))
    out_raw = _raw_amount(data.get("outAmount"))
    price_raw: str | None = None
    price_ui: str | None = None
    if in_raw is not None and out_raw is not None and in_raw > 0:
        price_raw = format(Decimal(out_raw) / Decimal(in_raw), "f")
        in_ui = _ui_amount(in_raw, input_decimals)
        out_ui = _ui_amount(out_raw, output_decimals)
        if in_ui is not None and out_ui is not None and Decimal(in_ui) > 0:
            price_ui = format(Decimal(out_ui) / Decimal(in_ui), "f")
    return {
        "amount_unit": "raw",
        "in_amount_raw": in_raw,
        "out_amount_raw": out_raw,
        "input_decimals": input_decimals,
        "output_decimals": output_decimals,
        "in_amount_ui": _ui_amount(in_raw, input_decimals),
        "out_amount_ui": _ui_amount(out_raw, output_decimals),
        "price_out_raw_per_in_raw": price_raw,
        "price_out_ui_per_in_ui": price_ui,
        "price_impact_unit": "ratio",
        "retry": False,
    }


async def get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    client: httpx.AsyncClient,
    *,
    quote_url: str = QUOTE_URL,
    api_key: SecretStr | None = None,
    timeout_seconds: float = 12.0,
    input_decimals: int | None = None,
    output_decimals: int | None = None,
) -> dict[str, Any]:
    """Fetch one quote-only Swap V2 order for either a buy or a sell.

    Both sides use this function and the same URL. Amounts are raw integer
    base units; decimals are caller-supplied and never inferred from a USD
    field. HTTP 429 returns ``data_stale`` with ``retry`` false and a
    ``backoff_s`` for a later quote. This function does not sleep and does
    not send a second request. Transport and decode failures return
    ``{"ok": False}`` and log the error class. Programming errors propagate.
    """
    lowered = quote_url.lower()
    if any(marker in lowered for marker in _BANNED_URL_MARKERS):
        return _unsupported("unsupported_quote_surface")
    key = api_key if api_key is not None else SecretStr("")
    keyless = not key.get_secret_value()
    logger.info("jupiter quote keyless=%s", keyless)
    headers: dict[str, str] = {}
    if not keyless:
        headers["x-api-key"] = key.get_secret_value()
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": str(slippage_bps),
    }
    try:
        resp = await client.get(quote_url, params=params, headers=headers, timeout=timeout_seconds)
        if resp.status_code == 429:
            return _rate_limited(resp)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            return {"ok": False, "error": "jupiter quote failed"}
        route = data.get("routePlan") or []
        if not isinstance(route, list):
            return {"ok": False, "error": "jupiter quote failed"}
        # v2 priceImpact is percentage points. The deprecated v1 priceImpactPct
        # ratio is never read, so a mixed payload cannot change the unit.
        price_impact = data.get("priceImpact")
        ratio: float | None = None
        if price_impact is not None:
            try:
                ratio = float(price_impact) / 100
            except (ArithmeticError, TypeError, ValueError):
                ratio = None
        quoted = {
            "ok": True,
            "inputMint": data.get("inputMint"),
            "outputMint": data.get("outputMint"),
            "inAmount": data.get("inAmount"),
            "outAmount": data.get("outAmount"),
            "otherAmountThreshold": data.get("otherAmountThreshold"),
            "slippageBps": data.get("slippageBps"),
            "priceImpactRatio": ratio,
            "routePlan": [
                {
                    "swapInfo": r.get("swapInfo", {}),
                    "percent": r.get("percent"),
                }
                for r in route
                if isinstance(r, dict)
            ],
            "router": data.get("router"),
            "requestId": data.get("requestId"),
            "raw": data,
        }
        quoted.update(
            _unit_fields(data, input_decimals=input_decimals, output_decimals=output_decimals)
        )
        return quoted
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("jupiter quote failed error=%s", type(exc).__name__)
        return {"ok": False, "error": "jupiter quote failed"}
