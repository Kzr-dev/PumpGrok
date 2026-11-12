"""Swap V2 quotes share one surface, raw units, and a non-retrying 429."""

from __future__ import annotations

import httpx
import pytest

from pumpgrok.tools.market.jupiter import QUOTE_URL, get_quote
from pumpgrok.tools.market.launch_feed import quote_is_actionable


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_429_is_data_stale_and_not_retried() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429, headers={"retry-after": "2"}, json={"error": "slow down"})

    result = await get_quote("A", "B", 1_000_000_000, 50, _client(handler))
    assert calls["n"] == 1
    assert result["ok"] is False
    assert result["code"] == "data_stale"
    assert result["blind"] is True
    assert result["retry"] is False
    assert result["backoff_s"] == 2.0
    assert quote_is_actionable(result) is False


async def test_buy_and_sell_share_the_v2_surface_and_name_decimals() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url).split("?", 1)[0])
        assert "taker" not in request.url.params
        return httpx.Response(
            200,
            json={
                "inputMint": request.url.params["inputMint"],
                "outputMint": request.url.params["outputMint"],
                "inAmount": request.url.params["amount"],
                "outAmount": "2000000",
                "priceImpact": "1.5",
                "priceImpactPct": "999",
                "routePlan": [],
            },
        )

    client = _client(handler)
    buy = await get_quote(
        "SOL",
        "TOK",
        1_000_000_000,
        50,
        client,
        input_decimals=9,
        output_decimals=6,
    )
    sell = await get_quote(
        "TOK",
        "SOL",
        2_000_000,
        50,
        client,
        input_decimals=6,
        output_decimals=9,
    )
    assert seen == [QUOTE_URL, QUOTE_URL]
    assert buy["amount_unit"] == sell["amount_unit"] == "raw"
    assert buy["in_amount_raw"] == 1_000_000_000
    assert buy["input_decimals"] == 9
    assert buy["output_decimals"] == 6
    assert buy["in_amount_ui"] == "1"
    assert sell["input_decimals"] == 6
    assert sell["output_decimals"] == 9
    assert buy["priceImpactRatio"] == pytest.approx(0.015)
    assert "999" not in str(buy["priceImpactRatio"])
    assert quote_is_actionable(buy) is True


async def test_lite_api_url_is_refused_before_a_request() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("deprecated surface must not be called")

    result = await get_quote(
        "A",
        "B",
        1,
        50,
        _client(handler),
        quote_url="https://lite-api.jup.ag/swap/v1/quote",
    )
    assert result["ok"] is False
    assert result["error"] == "unsupported_quote_surface"
    assert result["retry"] is False
