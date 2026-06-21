"""Regression tests for Kraken spot + Kraken Futures symbol parsing.

Guards the mvp_instrument_universe_gap_audit (2026-06-17) fix: the Tardis
``kraken`` (spot, ``BASE/QUOTE``) and ``cryptofacilities`` (Kraken Futures,
``PF_<BASE><QUOTE>`` / ``FI_<BASE><QUOTE>_<YYMMDD>``) instrument ids were
mis-parsed by the generic splitter — ``AAVE/USD`` kept the slash (``AAVE/``)
and ``PF_AAVEUSD`` kept the prefix / greedily matched the ``TUSD`` quote
(``PF_AAVE``) — so EVERY Kraken instrument was filtered out
(``parsed 0 instruments``). Kraken denominates BTC as ``XBT``.
"""

from __future__ import annotations

import pytest
from unified_api_contracts import TardisInstrumentDetail

from instruments_service.reference_data.adapters.cefi.tardis import (
    _resolve_base_quote,
    _split_kraken_symbol,
)


def _item(raw_id: str) -> TardisInstrumentDetail:
    """A Tardis item with no explicit base/quote → forces symbol-based parsing."""
    return TardisInstrumentDetail(id=raw_id, type="spot")


@pytest.mark.parametrize(
    ("raw_id", "exchange", "expected_base", "expected_quote"),
    [
        # Kraken spot — BASE/QUOTE; XBT→BTC alias.
        ("AAVE/USD", "kraken", "AAVE", "USD"),
        ("AAVE/ETH", "kraken", "AAVE", "ETH"),
        ("1INCH/USD", "kraken", "1INCH", "USD"),
        ("XBT/USD", "kraken", "BTC", "USD"),
        ("XBT/EUR", "kraken", "BTC", "EUR"),
        # Kraken Futures — typed prefix + concatenated BASE+QUOTE; XBT→BTC.
        ("PF_AAVEUSD", "cryptofacilities", "AAVE", "USD"),
        ("PF_ADAUSD", "cryptofacilities", "ADA", "USD"),
        ("PF_SOLUSD", "cryptofacilities", "SOL", "USD"),
        ("PI_XBTUSD", "cryptofacilities", "BTC", "USD"),
        ("PF_XBTUSD", "cryptofacilities", "BTC", "USD"),
        # Fixed-expiry inverse future — trailing _YYMMDD must be dropped.
        ("FI_XBTUSD_240329", "cryptofacilities", "BTC", "USD"),
    ],
)
def test_resolve_base_quote_kraken(raw_id: str, exchange: str, expected_base: str, expected_quote: str) -> None:
    base, quote = _resolve_base_quote(_item(raw_id), raw_id, exchange)
    assert base == expected_base, f"{raw_id}: base {base!r} != {expected_base!r}"
    assert quote == expected_quote, f"{raw_id}: quote {quote!r} != {expected_quote!r}"


def test_split_kraken_symbol_non_kraken_shape_returns_empty() -> None:
    """A non-Kraken-shaped id (no slash, no known futures prefix) returns ("", "")."""
    assert _split_kraken_symbol("BTCUSDT") == ("", "")


def test_kraken_base_is_in_universe_for_aave() -> None:
    """AAVE (an mvp_instrument_universe_gap base) must survive parsing → asset filter."""
    from unified_api_contracts import CEFI_BASE_ASSET_UNIVERSE

    base, _quote = _resolve_base_quote(_item("AAVE/USD"), "AAVE/USD", "kraken")
    assert base in CEFI_BASE_ASSET_UNIVERSE
