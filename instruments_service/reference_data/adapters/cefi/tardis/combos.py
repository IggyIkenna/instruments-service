"""Deribit combo/spread symbol parser.

Cohesion module of the ``adapters.cefi.tardis`` package (split from the former
monolithic ``adapters/cefi/tardis.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Deribit encodes legs deterministically in the symbol name.
Format: BASE-CODE-EXPIRY-STRIKES  (single-expiry)
        BASE-CODE-EXP1_EXP2-STRIKES  (calendar/diagonal)
Where CODE is one of 33 structure codes.  Strikes separated by _.
PERP in expiry position → BASE-PERPETUAL instrument name.
"""

# Package-internal access: the tardis package is ONE logical namespace split
# across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from unified_api_contracts.internal import InstrumentLeg, InstrumentType

__all__ = [
    "_DERIBIT_COMBO_STRUCTURES",
    "_DERIBIT_DUAL_EXPIRY_CODES",
    "_parse_deribit_combo_legs",
]

# Maps structure code → list of (option_type, side, ratio) per leg position.
# option_type: "C", "P", or None (future/perp leg).
# side: "BUY" or "SELL".  ratio: int multiplier.
_DERIBIT_COMBO_STRUCTURES: dict[str, list[tuple[str | None, str, int]]] = {
    # --- Future spreads (2 legs) ---
    "FS": [(None, "BUY", 1), (None, "SELL", 1)],
    # --- Vanilla option spreads (2 legs, same expiry) ---
    "CS": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PS": [("P", "BUY", 1), ("P", "SELL", 1)],
    "STRD": [("C", "BUY", 1), ("P", "BUY", 1)],
    "STRG": [("P", "BUY", 1), ("C", "BUY", 1)],
    "RR": [("P", "BUY", 1), ("C", "SELL", 1)],
    "RRITM": [("P", "BUY", 1), ("C", "SELL", 1)],
    "GUTS": [("C", "BUY", 1), ("P", "BUY", 1)],
    "REV": [("C", "BUY", 1), ("P", "SELL", 1)],
    # --- 3-leg (butterflies, ladders) ---
    "CBUT": [("C", "BUY", 1), ("C", "SELL", 2), ("C", "BUY", 1)],
    "PBUT": [("P", "BUY", 1), ("P", "SELL", 2), ("P", "BUY", 1)],
    "CBUT111": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "PBUT111": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "BUY", 1)],
    "CLAD": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "SELL", 1)],
    "PLAD": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "SELL", 1)],
    # --- 4-leg (condors, iron butterflies, boxes) ---
    "IBUT": [("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1), ("C", "BUY", 1)],
    "ICOND": [("P", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "CCOND": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "PCOND": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "SELL", 1), ("P", "BUY", 1)],
    "BOX": [("C", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("P", "BUY", 1)],
    # --- Calendar / diagonal (2 expiries) ---
    "CCAL": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PCAL": [("P", "BUY", 1), ("P", "SELL", 1)],
    "CDIAG": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PDIAG": [("P", "BUY", 1), ("P", "SELL", 1)],
    "STDC": [("C", "BUY", 1), ("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1)],
    "DSTDC": [("C", "BUY", 1), ("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1)],
    # --- Ratio spreads (2 legs, unequal ratios) ---
    "CSR12": [("C", "BUY", 1), ("C", "SELL", 2)],
    "CSR13": [("C", "BUY", 1), ("C", "SELL", 3)],
    "CSR23": [("C", "BUY", 2), ("C", "SELL", 3)],
    "PSR12": [("P", "BUY", 1), ("P", "SELL", 2)],
    "PSR13": [("P", "BUY", 1), ("P", "SELL", 3)],
    "PSR23": [("P", "BUY", 2), ("P", "SELL", 3)],
    # --- Jelly roll (4 legs, 2 expiries) ---
    "JR": [("C", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("P", "BUY", 1)],
}

# Codes that use two expiries (calendar/diagonal families)
_DERIBIT_DUAL_EXPIRY_CODES = frozenset(
    {
        "CCAL",
        "PCAL",
        "CDIAG",
        "PDIAG",
        "STDC",
        "DSTDC",
        "JR",
    }
)


def _parse_deribit_combo_legs(raw_id: str, venue: str) -> list[InstrumentLeg]:
    """Parse Deribit combo symbol into InstrumentLeg list.

    Symbol format examples:
        BTC-FS-25APR26_PERP          → future spread
        BTC-CS-25APR26-90000_100000  → call spread
        BTC-CBUT-25APR26-80000_90000_100000  → call butterfly
        ETH_USDC-FS-25APR26_PERP    → linear future spread
        BTC-CCAL-25APR26_3APR26-90000 → call calendar

    Returns empty list if symbol can't be parsed (caller skips the combo).
    """
    # Split: BASE-CODE-REST
    # Base may contain underscore (BTC_USDC), so find the structure code.
    parts = raw_id.split("-")
    if len(parts) < 3:
        return []

    # Find the structure code — it's the first part that matches a known code.
    code_idx = -1
    code = ""
    for i, p in enumerate(parts):
        if p in _DERIBIT_COMBO_STRUCTURES:
            code_idx = i
            code = p
            break

    if code_idx < 0:
        return []

    base = "-".join(parts[:code_idx])  # e.g. "BTC" or "BTC_USDC" or "ETH"
    rest = parts[code_idx + 1 :]  # everything after the code
    structure = _DERIBIT_COMBO_STRUCTURES[code]
    is_dual_expiry = code in _DERIBIT_DUAL_EXPIRY_CODES

    # --- Future spread (FS): BASE-FS-EXP1_EXP2 ---
    if code == "FS":
        if not rest:
            return []
        expiry_part = rest[0]  # e.g. "25APR26_PERP" or "25APR26_3APR26"
        expiries = expiry_part.split("_")
        legs: list[InstrumentLeg] = []
        for i, (_, side, ratio) in enumerate(structure):
            if i >= len(expiries):
                break
            exp = expiries[i]
            # PERP → BASE-PERPETUAL, else BASE-EXPIRY (Deribit future format)
            if exp == "PERP":
                leg_name = f"{base}-PERPETUAL"
                leg_type = InstrumentType.PERPETUAL
            else:
                leg_name = f"{base}-{exp}"
                leg_type = InstrumentType.FUTURE
            legs.append(
                InstrumentLeg(
                    instrument_key=f"{venue}:{leg_type}:{leg_name}",
                    side=side,
                    ratio=ratio,
                )
            )
        return legs

    # --- Calendar/diagonal (dual expiry): BASE-CODE-EXP1_EXP2-STRIKES ---
    if is_dual_expiry:
        if not rest:
            return []
        expiry_part = rest[0]
        expiries = expiry_part.split("_")
        if len(expiries) < 2:
            return []
        exp_far, exp_near = expiries[0], expiries[1]
        strikes_part = rest[1] if len(rest) > 1 else ""
        strikes = strikes_part.split("_") if strikes_part else []

        legs = []
        half = len(structure) // 2
        for leg_idx, (opt_type, side, ratio) in enumerate(structure):
            # Alternate expiries: first half = far, second half = near
            exp = exp_far if leg_idx < half else exp_near
            strike = strikes[leg_idx % len(strikes)] if strikes else ""
            suffix = f"-{strike}-{opt_type}" if opt_type and strike else ""
            leg_name = f"{base}-{exp}{suffix}"
            legs.append(
                InstrumentLeg(
                    instrument_key=f"{venue}:OPTION:{leg_name}" if opt_type else f"{venue}:FUTURE:{leg_name}",
                    side=side,
                    ratio=ratio,
                )
            )
        return legs

    # --- Single-expiry option combos: BASE-CODE-EXPIRY-K1_K2[_K3[_K4]] ---
    if not rest:
        return []

    expiry = rest[0]
    strikes_part = rest[1] if len(rest) > 1 else ""
    strikes = strikes_part.split("_") if strikes_part else []

    # For STRD (straddle): single strike, two legs (C + P at same strike)
    legs = []
    for i, (opt_type, side, ratio) in enumerate(structure):
        # Map strike index: for structures with fewer strikes than legs,
        # reuse strikes (e.g. STRD has 1 strike, 2 legs).
        strike = strikes[min(i, len(strikes) - 1)] if strikes else ""
        suffix = f"-{strike}-{opt_type}" if opt_type and strike else ""
        leg_name = f"{base}-{expiry}{suffix}"
        legs.append(
            InstrumentLeg(
                instrument_key=f"{venue}:OPTION:{leg_name}" if opt_type else f"{venue}:FUTURE:{leg_name}",
                side=side,
                ratio=ratio,
            )
        )
    return legs
