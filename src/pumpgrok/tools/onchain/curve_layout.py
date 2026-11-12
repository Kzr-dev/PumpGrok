"""Bonding-curve account decode over Solana RPC (STRUCT-002, B9, T-03-02-01).

Two layouts share this module:

* ``assumed``: the 49-byte fixture layout produced by
  :func:`encode_curve_fixture` (discriminator ``ASSUMED``, repo field order
  virtual-SOL then virtual-token). Existing replay fixtures keep decoding
  under it. ``LAYOUT_STATUS`` stays ``ASSUMED`` for that path, and an RPC
  snapshot tagged with it still fails closed.
* ``pump``: the public pump.fun bonding-curve account. An 8-byte Anchor
  discriminator (``17b7f83760d8ac60``), then little-endian
  virtual-token, virtual-SOL, real-token, real-SOL, total supply, a
  completion flag, then optional creator and flag bytes. Live accounts are
  166 bytes (October 2026 upgrade). Any length from the original 49 bytes
  through 166 decodes; bytes past the known prefix are reserved and ignored.
  A documented pump decode may price a paper entry. It never signs or sends.

Shared by ``market.price`` and ``onchain.lp_status``. RPC only: this module
performs no HTTP reads and references no quote endpoint (R44, R46, R47).
"""

from __future__ import annotations

import base64
import struct
from decimal import Decimal
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict

from pumpgrok.core.units import lamports_to_sol, sol_to_lamports
from pumpgrok.tools._common import UNITS_PER_TOKEN

_CURVE_STRUCT = struct.Struct("<8sQQQQQB")
CURVE_ACCOUNT_SIZE = _CURVE_STRUCT.size

#: Live pump.fun bonding-curve account length after the October 2026 upgrade.
PUMP_CURVE_ACCOUNT_SIZE: Final[int] = 166
#: Original initialize size. Lengths between this and the current size are
#: the same prefix plus optional creator, flag, and reserved bytes.
PUMP_CURVE_MIN_SIZE: Final[int] = 49

_ASSUMED_DISCRIMINATOR: Final[bytes] = b"ASSUMED\x00"
_PUMP_DISCRIMINATOR: Final[bytes] = bytes.fromhex("17b7f83760d8ac60")
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

#: Bonding-curve layout verification state (R65). ASSUMED until a human
#: reviews a live probe. The global flag still fail-closes RPC snapshots
#: that were not decoded under the documented pump discriminator. A snapshot
#: whose layout is ``pump`` may price a paper entry.
LAYOUT_STATUS: Final[str] = "ASSUMED"

#: Lamports of real SOL at which the curve is considered complete (catalog:
#: 85 SOL, the retired oracle ``src/monitor.py:30``). [ASSUMED] threshold.
COMPLETION_SOL_LAMPORTS = sol_to_lamports(Decimal(85))


class CurveDecodeError(ValueError):
    """The account bytes do not match the ASSUMED curve layout."""


class CurveState(BaseModel):
    """Decoded bonding-curve counters. Money is int; ratios stay Decimal."""

    model_config = ConfigDict(frozen=True)

    virtual_sol_reserves_lamports: int
    virtual_token_reserves_units: int
    real_sol_reserves_lamports: int
    real_token_reserves_units: int
    total_supply_units: int
    complete: bool
    layout: Literal["assumed", "pump"] = "assumed"
    account_len: int = CURVE_ACCOUNT_SIZE
    creator: str | None = None
    is_mayhem_mode: bool | None = None
    is_cashback_coin: bool | None = None


def _b58(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    digits: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(_BASE58_ALPHABET[remainder])
    zeros = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * zeros + "".join(reversed(digits))


def _require_reserves(vsol: int, vtok: int, supply: int) -> None:
    for name, value in (
        ("virtual_sol", vsol),
        ("virtual_token", vtok),
        ("supply", supply),
    ):
        if value == 0:
            raise CurveDecodeError(f"curve reserve {name} is zero; price undefined")


def _decode_prefix(data: bytes) -> tuple[int, int, int, int, int, int]:
    """Unpack the 49-byte prefix. Returns the six integer fields in wire order."""
    try:
        unpacked = _CURVE_STRUCT.unpack(data[:CURVE_ACCOUNT_SIZE])
    except struct.error as exc:
        raise CurveDecodeError(f"curve account unpack failed: {type(exc).__name__}") from exc
    _disc, first, second, third, fourth, supply, complete = unpacked
    if complete not in (0, 1):
        raise CurveDecodeError(f"curve completion flag not boolean: {complete!r}")
    return first, second, third, fourth, supply, complete


def decode_curve_account(data: bytes) -> CurveState:
    """Decode raw account bytes.

    The ``ASSUMED`` discriminator keeps the historical fixture order and the
    exact 49-byte length. The pump discriminator uses the documented order
    (virtual token, then virtual SOL) and accepts the original 49-byte
    account through the current 166-byte account.
    """
    if len(data) < 8:
        raise CurveDecodeError(f"curve account size {len(data)} is shorter than a discriminator")
    disc = data[:8]
    if disc == _ASSUMED_DISCRIMINATOR:
        if len(data) != CURVE_ACCOUNT_SIZE:
            raise CurveDecodeError(
                f"curve account size {len(data)} != ASSUMED {CURVE_ACCOUNT_SIZE}"
            )
        vsol, vtok, rsol, rtok, supply, complete = _decode_prefix(data)
        _require_reserves(vsol, vtok, supply)
        return CurveState(
            virtual_sol_reserves_lamports=vsol,
            virtual_token_reserves_units=vtok,
            real_sol_reserves_lamports=rsol,
            real_token_reserves_units=rtok,
            total_supply_units=supply,
            complete=bool(complete),
            layout="assumed",
            account_len=len(data),
        )
    if disc != _PUMP_DISCRIMINATOR:
        raise CurveDecodeError("curve account discriminator is not the pump or fixture layout")
    if not PUMP_CURVE_MIN_SIZE <= len(data) <= PUMP_CURVE_ACCOUNT_SIZE:
        raise CurveDecodeError(
            f"pump curve account size {len(data)} is outside "
            f"{PUMP_CURVE_MIN_SIZE}..{PUMP_CURVE_ACCOUNT_SIZE}"
        )
    # Documented order: virtual token, virtual SOL, real token, real SOL.
    vtok, vsol, rtok, rsol, supply, complete = _decode_prefix(data)
    _require_reserves(vsol, vtok, supply)
    creator: str | None = None
    mayhem: bool | None = None
    cashback: bool | None = None
    if len(data) >= 81:
        creator = _b58(data[49:81])
    if len(data) >= 82 and data[81] in (0, 1):
        mayhem = bool(data[81])
    if len(data) >= 83 and data[82] in (0, 1):
        cashback = bool(data[82])
    return CurveState(
        virtual_sol_reserves_lamports=vsol,
        virtual_token_reserves_units=vtok,
        real_sol_reserves_lamports=rsol,
        real_token_reserves_units=rtok,
        total_supply_units=supply,
        complete=bool(complete),
        layout="pump",
        account_len=len(data),
        creator=creator,
        is_mayhem_mode=mayhem,
        is_cashback_coin=cashback,
    )


def decode_curve_account_base64(data: object) -> CurveState:
    """Decode the ``data`` field of a base64 ``getAccountInfo`` value.

    Accepts only the ``["<base64>", "base64"]`` pair form. The ``jsonParsed``
    object form carries no curve bytes and is rejected as malformed.
    """
    if not isinstance(data, list) or len(data) != 2 or data[1] != "base64":
        raise CurveDecodeError("curve account is not base64-encoded bytes")
    try:
        raw = base64.b64decode(data[0], validate=True)
    except (ValueError, TypeError) as exc:
        raise CurveDecodeError("curve account base64 is malformed") from exc
    return decode_curve_account(raw)


def encode_curve_fixture(
    *,
    virtual_sol_reserves_lamports: int,
    virtual_token_reserves_units: int,
    real_sol_reserves_lamports: int,
    real_token_reserves_units: int,
    total_supply_units: int,
    complete: bool,
) -> bytes:
    """Build [ASSUMED]-layout account bytes for tests. Never used in production."""
    return _CURVE_STRUCT.pack(
        _ASSUMED_DISCRIMINATOR,
        virtual_sol_reserves_lamports,
        virtual_token_reserves_units,
        real_sol_reserves_lamports,
        real_token_reserves_units,
        total_supply_units,
        1 if complete else 0,
    )


def encode_pump_curve_account(
    *,
    virtual_sol_reserves_lamports: int,
    virtual_token_reserves_units: int,
    real_sol_reserves_lamports: int,
    real_token_reserves_units: int,
    total_supply_units: int,
    complete: bool,
    account_len: int = PUMP_CURVE_ACCOUNT_SIZE,
    creator: bytes = b"\x00" * 32,
    is_mayhem_mode: bool = False,
    is_cashback_coin: bool = False,
) -> bytes:
    """Build documented pump-layout account bytes for tests. Never used in production.

    Wire order is virtual token, virtual SOL, real token, real SOL. ``account_len``
    must sit between the original 49 bytes and the current 166.
    """
    if not PUMP_CURVE_MIN_SIZE <= account_len <= PUMP_CURVE_ACCOUNT_SIZE:
        raise ValueError(
            f"pump curve account_len {account_len} is outside "
            f"{PUMP_CURVE_MIN_SIZE}..{PUMP_CURVE_ACCOUNT_SIZE}"
        )
    if len(creator) != 32:
        raise ValueError("pump curve creator must be 32 bytes")
    prefix = _CURVE_STRUCT.pack(
        _PUMP_DISCRIMINATOR,
        virtual_token_reserves_units,
        virtual_sol_reserves_lamports,
        real_token_reserves_units,
        real_sol_reserves_lamports,
        total_supply_units,
        1 if complete else 0,
    )
    body = bytearray(account_len)
    body[:CURVE_ACCOUNT_SIZE] = prefix
    if account_len >= 81:
        body[49:81] = creator
    if account_len >= 82:
        body[81] = 1 if is_mayhem_mode else 0
    if account_len >= 83:
        body[82] = 1 if is_cashback_coin else 0
    return bytes(body)


def curve_price_sol_per_token(state: CurveState) -> Decimal:
    """SOL per token from virtual reserves (vendor ``price_from_reserves`` math).

    The ported float expression ``(vsol / 1e9) / (vtok / 1e6)`` is evaluated
    here in :class:`Decimal` at the wrapper boundary; the vendor module keeps
    its float form untouched (characterization pins).
    """
    return lamports_to_sol(
        Decimal(state.virtual_sol_reserves_lamports)
        / Decimal(state.virtual_token_reserves_units)
        * Decimal(UNITS_PER_TOKEN)
    )


def curve_progress(state: CurveState) -> Decimal:
    """Completion ratio 0..1 from real SOL against the 85 SOL bar."""
    ratio = Decimal(state.real_sol_reserves_lamports) / Decimal(COMPLETION_SOL_LAMPORTS)
    return min(Decimal(1), max(Decimal(0), ratio))


def curve_market_cap_lamports(state: CurveState) -> int:
    """Market cap in lamports: virtual SOL scaled by supply over virtual tokens."""
    return (
        state.virtual_sol_reserves_lamports * state.total_supply_units
    ) // state.virtual_token_reserves_units


def rpc_curve_price_allowed(*, curve_source: str, layout: str | None = None) -> bool:
    """True when a price from ``curve_source`` may price a paper entry.

    An RPC snapshot may price a paper entry only when it decoded under the
    documented pump discriminator (``layout="pump"``) or the global status
    is ``VERIFIED``. The historical fixture layout stays fail-closed on RPC
    while ``LAYOUT_STATUS`` is ``ASSUMED``. Fixture, rest and pumpportal
    integer ticks never pass through this gate, so replay is unaffected.
    """
    if curve_source != "rpc":
        return True
    if layout == "pump":
        return True
    return LAYOUT_STATUS == "VERIFIED"
