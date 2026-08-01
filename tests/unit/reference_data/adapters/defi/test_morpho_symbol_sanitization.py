"""Regression guard: Morpho's `pair_key` disambiguator must survive a raw ERC-20 symbol that
embeds a canonical-id-reserved character.

Real trigger, confirmed live 2026-08-01 against blue-api.morpho.org (chainId 42161 /
ARBITRUM, marketId 0x1a926ab8add08dca634f8d6cecd8c866e166a4affc65801beda9f239d21b622a): a GMX
GM-vault collateral asset reports ``symbol="GM:ETH/USD[WETH-USDC]"``. Before the
`_sanitize_symbol` fix, this embedded ``:`` reached
`build_canonical_instrument_id(..., passthrough=True)`, which raises `ValueError` on any
non-sports/prediction symbol containing ``:`` (canonical_id_builder.py's fail-loud
double-wrapped-id guard) — and since `get_instruments()`'s per-market loop has no
try/except, one such market would abort discovery for the ENTIRE chain
(instrument_id_format_canonicalization_2026_07_08.md finding 6).
"""

from __future__ import annotations

from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

_REAL_GM_VAULT_MARKET: dict[str, object] = {
    "marketId": "0x1a926ab8add08dca634f8d6cecd8c866e166a4affc65801beda9f239d21b622a",
    "loanAsset": {"address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "symbol": "USDC", "decimals": 6},
    "collateralAsset": {
        "address": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "symbol": "GM:ETH/USD[WETH-USDC]",
        "decimals": 18,
    },
}

_NORMAL_MARKET: dict[str, object] = {
    "marketId": "0x125081abcdef0000000000000000000000000000000000000000000000000",
    "loanAsset": {"address": "0xUSDC", "symbol": "USDC", "decimals": 6},
    "collateralAsset": {"address": "0xCBBTC", "symbol": "cbBTC", "decimals": 8},
}


class TestMorphoSymbolSanitization:
    def test_unsafe_collateral_symbol_no_longer_raises(self) -> None:
        records = MorphoReferenceDataAdapter._market_to_records(_REAL_GM_VAULT_MARKET, "MORPHO-ARBITRUM", "ARBITRUM")
        assert len(records) == 2
        for rec in records:
            # Exactly 2 colons — the reserved VENUE:TYPE:SYMBOL delimiter, nothing embedded.
            assert rec.instrument_key.count(":") == 2
            assert "/" not in rec.instrument_key
            assert "[" not in rec.instrument_key
            assert "]" not in rec.instrument_key

    def test_normal_market_pair_key_unchanged(self) -> None:
        """Byte-identical for the common case — sanitization is a no-op on clean symbols."""
        records = MorphoReferenceDataAdapter._market_to_records(_NORMAL_MARKET, "MORPHO-BASE", "BASE")
        assert len(records) == 2
        a_token, debt_token = records
        assert a_token.instrument_key == "MORPHO-BASE:A_TOKEN:AcbBTC-USDC-0x125081"
        assert debt_token.instrument_key == "MORPHO-BASE:DEBT_TOKEN:DEBTcbBTC-USDC-0x125081"
