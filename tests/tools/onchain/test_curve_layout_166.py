"""166-byte pump.fun bonding-curve accounts decode, and 49-byte fixtures still do."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from pumpgrok.tools.onchain.curve_layout import (
    CURVE_ACCOUNT_SIZE,
    PUMP_CURVE_ACCOUNT_SIZE,
    CurveDecodeError,
    decode_curve_account,
    encode_curve_fixture,
    encode_pump_curve_account,
    rpc_curve_price_allowed,
)

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "rpc" / "bonding_curve" / "curve_166.json"
)

VSOL = 30_000_000_000
VTOK = 1_073_000_000_000_000
RSOL = 1_000_000_000
RTOK = 800_000_000_000_000
SUPPLY = 1_000_000_000_000_000


def test_assumed_49_byte_fixture_still_decodes_in_repo_order() -> None:
    raw = encode_curve_fixture(
        virtual_sol_reserves_lamports=VSOL,
        virtual_token_reserves_units=VTOK,
        real_sol_reserves_lamports=RSOL,
        real_token_reserves_units=RTOK,
        total_supply_units=SUPPLY,
        complete=False,
    )
    assert len(raw) == CURVE_ACCOUNT_SIZE == 49
    state = decode_curve_account(raw)
    assert state.layout == "assumed"
    assert state.virtual_sol_reserves_lamports == VSOL
    assert state.virtual_token_reserves_units == VTOK
    assert rpc_curve_price_allowed(curve_source="rpc", layout=state.layout) is False


def test_166_byte_account_uses_documented_order_and_prices_paper_entries() -> None:
    raw = encode_pump_curve_account(
        virtual_sol_reserves_lamports=VSOL,
        virtual_token_reserves_units=VTOK,
        real_sol_reserves_lamports=RSOL,
        real_token_reserves_units=RTOK,
        total_supply_units=SUPPLY,
        complete=False,
        is_mayhem_mode=False,
        is_cashback_coin=True,
    )
    assert len(raw) == PUMP_CURVE_ACCOUNT_SIZE == 166
    state = decode_curve_account(raw)
    assert state.layout == "pump"
    assert state.account_len == 166
    assert state.virtual_sol_reserves_lamports == VSOL
    assert state.virtual_token_reserves_units == VTOK
    assert state.real_sol_reserves_lamports == RSOL
    assert state.real_token_reserves_units == RTOK
    assert state.is_cashback_coin is True
    assert state.creator is not None
    assert rpc_curve_price_allowed(curve_source="rpc", layout="pump") is True
    assert rpc_curve_price_allowed(curve_source="fixture", layout="assumed") is True


def test_committed_166_byte_fixture_round_trips() -> None:
    record = json.loads(FIXTURE.read_text(encoding="utf-8"))
    raw = base64.b64decode(record["account_b64"])
    assert len(raw) == 166
    state = decode_curve_account(raw)
    assert state.virtual_sol_reserves_lamports == record["virtual_sol_reserves_lamports"]
    assert state.virtual_token_reserves_units == record["virtual_token_reserves_units"]
    assert state.layout == "pump"


def test_short_pump_prefix_still_decodes() -> None:
    raw = encode_pump_curve_account(
        virtual_sol_reserves_lamports=VSOL,
        virtual_token_reserves_units=VTOK,
        real_sol_reserves_lamports=RSOL,
        real_token_reserves_units=RTOK,
        total_supply_units=SUPPLY,
        complete=True,
        account_len=49,
    )
    state = decode_curve_account(raw)
    assert state.complete is True
    assert state.creator is None
    assert state.virtual_sol_reserves_lamports == VSOL


def test_unknown_discriminator_is_rejected() -> None:
    try:
        decode_curve_account(b"\x00" * 49)
    except CurveDecodeError as exc:
        assert "discriminator" in str(exc)
    else:
        raise AssertionError("unknown discriminator must fail closed")
