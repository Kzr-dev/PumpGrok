"""PumpPortal backoff and the program-log create parser."""

from __future__ import annotations

import random

from pumpgrok.tools.market.discovery import (
    PUMP_PROGRAM,
    discovery_delay,
    handshake_status,
    logs_create_signature,
    mint_from_create_transaction,
    portal_subscribe_body,
)
from pumpgrok.tools.onchain.pump_pda import bonding_curve_pda

MINT = "So11111111111111111111111111111111111111112"


def test_denied_handshake_backs_off_and_grows() -> None:
    rng = random.Random(1)
    first = discovery_delay(0, rng, denied=True)
    second = discovery_delay(1, random.Random(1), denied=True)
    assert first >= 30.0
    assert second > first
    assert discovery_delay(8, random.Random(1), denied=True) <= 120.0 * 1.25


def test_portal_subscribe_is_the_free_method() -> None:
    assert "subscribeNewToken" in portal_subscribe_body()
    assert "subscribeTokenTrade" not in portal_subscribe_body()


def test_handshake_status_reads_the_response() -> None:
    class _Response:
        status_code = 403

    class _Error(Exception):
        response = _Response()

    assert handshake_status(_Error()) == 403
    assert handshake_status(RuntimeError("down")) is None


def test_create_log_and_transaction_yield_the_mint() -> None:
    curve = bonding_curve_pda(MINT)
    notice = {
        "params": {
            "result": {
                "value": {
                    "signature": "sig",
                    "err": None,
                    "logs": [f"Program {PUMP_PROGRAM} invoke", "Program log: Instruction: Create"],
                }
            }
        }
    }
    assert logs_create_signature(notice) == "sig"
    tx = {
        "meta": {"logMessages": ["Program log: Instruction: Create"]},
        "transaction": {
            "message": {
                "instructions": [
                    {"programId": PUMP_PROGRAM, "accounts": [MINT, "auth", curve]},
                ]
            }
        },
    }
    assert mint_from_create_transaction(tx) == (MINT, curve)
    assert logs_create_signature({"params": {"result": {"value": {"err": "x"}}}}) is None
    assert logs_create_signature({}) is None
    assert mint_from_create_transaction({}) is None
    assert mint_from_create_transaction({"meta": {"logMessages": ["Instruction: Create"]}}) is None
