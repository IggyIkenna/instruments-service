"""Unit tests — Morpho Blue reference-data adapter's ``_market_to_records`` id construction.

Pure static-method tests (no network): each Morpho market must emit an
A_TOKEN + DEBT_TOKEN pair whose canonical instrument_key is well-formed —
including when an upstream on-chain token symbol itself carries a reserved
``:`` delimiter (confirmed live against blue-api.morpho.org 2026-08-01: GMX's
Arbitrum GM token symbol is ``"GM:ETH/USD[WETH-USDC]"``).
"""

from __future__ import annotations

from datetime import UTC, datetime

from unified_api_contracts.internal import InstrumentType

from instruments_service.reference_data.adapters.defi.morpho import (
    MorphoReferenceDataAdapter,
    _sanitize_symbol_segment,
)

_AVAILABLE_SINCE = datetime(2022, 1, 1, tzinfo=UTC)


def _market(collateral_symbol: str, loan_symbol: str, market_id: str) -> dict[str, object]:
    return {
        "marketId": market_id,
        "loanAsset": {"address": "0xloan", "symbol": loan_symbol, "name": loan_symbol, "decimals": 6},
        "collateralAsset": {
            "address": "0xcoll",
            "symbol": collateral_symbol,
            "name": collateral_symbol,
            "decimals": 18,
        },
        "lltv": "860000000000000000",
        "state": {"supplyAssets": "0", "borrowAssets": "0", "supplyApy": "0", "borrowApy": "0"},
    }


def test_market_to_records_dash_separates_pair_key() -> None:
    market = _market("USDC", "EURC", "0x305dd1abcdef1234")
    records = MorphoReferenceDataAdapter._market_to_records(market, "MORPHO-BASE", "BASE", _AVAILABLE_SINCE)

    assert len(records) == 2
    a_token, debt_token = records
    assert a_token.instrument_type == InstrumentType.A_TOKEN
    assert debt_token.instrument_type == InstrumentType.DEBT_TOKEN
    # Exactly 2 colons (the reserved VENUE:TYPE:SYMBOL delimiters) — no 3rd
    # colon leaking into the SYMBOL segment.
    assert a_token.instrument_key.count(":") == 2
    assert debt_token.instrument_key.count(":") == 2
    assert a_token.instrument_key == "MORPHO-BASE:A_TOKEN:AUSDC-EURC-0x305dd1"
    assert debt_token.instrument_key == "MORPHO-BASE:DEBT_TOKEN:DEBTUSDC-EURC-0x305dd1"


def test_market_to_records_sanitizes_embedded_colon_in_upstream_symbol() -> None:
    """Real-world case: a collateral symbol (GMX GM token) carries its own ':'.

    Without sanitization this collides with build_canonical_instrument_id's
    reserved-':' guard and raises, taking down the whole per-chain discovery
    loop (no per-market isolation in get_instruments()).
    """
    gm_symbol = "GM:ETH/USD[WETH-USDC]"
    market = _market(gm_symbol, "USDC", "0x1a926ab8")

    records = MorphoReferenceDataAdapter._market_to_records(market, "MORPHO-ARBITRUM", "ARBITRUM", _AVAILABLE_SINCE)

    assert len(records) == 2
    for rec in records:
        assert rec.instrument_key.count(":") == 2, rec.instrument_key
        # The sanitized symbol never contains the raw ':' from the upstream token.
        symbol_segment = rec.instrument_key.split(":", 2)[2]
        assert ":" not in symbol_segment
    a_token = records[0]
    assert a_token.instrument_key == "MORPHO-ARBITRUM:A_TOKEN:AGM_ETH/USD[WETH-USDC]-USDC-0x1a926a"
    # Metadata fields preserve the raw, unsanitized on-chain symbol.
    assert a_token.base_asset_symbol_onchain == gm_symbol
    assert a_token.base_asset == gm_symbol


def test_sanitize_symbol_segment() -> None:
    assert _sanitize_symbol_segment("USDC") == "USDC"
    assert _sanitize_symbol_segment("GM:ETH/USD[WETH-USDC]") == "GM_ETH/USD[WETH-USDC]"
    assert _sanitize_symbol_segment("A:B:C") == "A_B_C"
