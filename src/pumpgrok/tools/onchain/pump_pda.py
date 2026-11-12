"""Pump.fun bonding-curve PDA from a mint (read-only, no keys).

The address is the program-derived address of seed ``bonding-curve`` plus
the mint under the public pump program. It is used only to name the account
``getAccountInfo`` should read. This module does not sign or send.
"""

from __future__ import annotations

import hashlib

_PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
_SEED = b"bonding-curve"
_PDA_MARKER = b"ProgramDerivedAddress"
_P = 2**255 - 19
_D = 37095705934669439343138083508754565189542113879843219016388785533085940283555
_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {char: index for index, char in enumerate(_BASE58)}


def _b58_decode(text: str) -> bytes:
    number = 0
    for char in text:
        digit = _BASE58_INDEX.get(char)
        if digit is None:
            raise ValueError("character outside the base58 alphabet")
        number = number * 58 + digit
    zeros = len(text) - len(text.lstrip("1"))
    body = number.to_bytes((number.bit_length() + 7) // 8, "big")
    return b"\x00" * zeros + body


def _b58_encode(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    digits: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(_BASE58[remainder])
    zeros = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * zeros + "".join(reversed(digits))


def _on_curve(point: bytes) -> bool:
    """True when ``point`` decompresses to an ed25519 point."""
    if len(point) != 32:
        return False
    y = int.from_bytes(point, "little")
    parity = y >> 255
    y &= (1 << 255) - 1
    if y >= _P:
        return False
    y2 = (y * y) % _P
    u = (y2 - 1) % _P
    v = (_D * y2 + 1) % _P
    x2 = (u * pow(v, _P - 2, _P)) % _P
    if x2 == 0:
        return parity == 0
    x = pow(x2, (_P + 3) // 8, _P)
    if (x * x - x2) % _P != 0:
        x = (x * pow(2, (_P - 1) // 4, _P)) % _P
    if (x * x - x2) % _P != 0:
        return False
    if (x & 1) != parity:
        x = _P - x
    return True


def find_program_address(seeds: list[bytes], program_id: bytes) -> tuple[bytes, int]:
    """Highest bump whose sha256 digest is off the ed25519 curve."""
    for bump in range(255, -1, -1):
        payload = b"".join(seeds) + bytes([bump]) + program_id + _PDA_MARKER
        digest = hashlib.sha256(payload).digest()
        if not _on_curve(digest):
            return digest, bump
    raise RuntimeError("no program address bump")


def bonding_curve_pda(mint: str, program_id: str = _PUMP_PROGRAM) -> str:
    """Base58 bonding-curve account for ``mint``. Raises ``ValueError`` on a bad key."""
    mint_bytes = _b58_decode(mint)
    program_bytes = _b58_decode(program_id)
    if len(mint_bytes) != 32 or len(program_bytes) != 32:
        raise ValueError("mint and program must be 32-byte public keys")
    address, _bump = find_program_address([_SEED, mint_bytes], program_bytes)
    return _b58_encode(address)
