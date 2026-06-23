"""Regression tests for Bitfinex spot symbol parsing + venue-aware quote gate.

Guards the cefi_universe_capture_rule (2026-06-23) fix: Bitfinex spot ids come in
two shapes the generic splitter mis-parsed —
  * concatenated 3-char short tickers (``ALGUSD`` = ALGO/USD, ``ATOUST`` =
    ATOM/USDT, ``DSHBTC`` = DASH/BTC, ``IOTUSD`` = IOTA/USD), and
  * ``BASE:QUOTE`` colon-delimited ids (``AAVE:USD``, ``LINK:UST`` → LINK/USDT)
    where the generic suffix-matcher kept the colon (``AAVE:`` base).
Bitfinex's short base codes (ALG/ATO/DSH/IOT/UDC) + quote pseudo-codes
(UST=USDT, UDC=USDC) are mapped back to canonical tickers so the base-membership
leg of ``is_in_mvp_capture_universe`` passes (the 25-base spot↔perp overlap).

Also guards the per-venue accepted-quote extension: KRW is accepted ONLY for
UPBIT (kimchi premium), USDT/USDC/USD everywhere.
"""

from __future__ import annotations

import pytest
from unified_api_contracts import TardisInstrumentDetail

from instruments_service.reference_data.adapters.cefi.tardis import (
    _passes_asset_filter,
    _resolve_base_quote,
    _resolve_bitfinex_spot,
)


def _item(raw_id: str) -> TardisInstrumentDetail:
    """A Tardis item with no explicit base/quote → forces symbol-based parsing."""
    return TardisInstrumentDetail(id=raw_id, type="spot")


@pytest.mark.parametrize(
    ("raw_id", "expected_base", "expected_quote"),
    [
        # Concatenated 3-char short tickers (legacy) — Bitfinex base aliases applied.
        ("ALGUSD", "ALGO", "USD"),
        ("ATOUST", "ATOM", "USDT"),
        ("DSHBTC", "DASH", "BTC"),
        ("IOTUSD", "IOTA", "USD"),
        # Colon-delimited (modern) — colon is the base/quote separator.
        ("AAVE:USD", "AAVE", "USD"),
        ("LINK:UST", "LINK", "USDT"),
        ("1INCH:UST", "1INCH", "USDT"),
        ("BTC:CNHT", "BTC", "CNHT"),
        # UDC (Bitfinex code for USDC) as a base.
        ("UDCUSD", "USDC", "USD"),
    ],
)
def test_resolve_bitfinex_spot(raw_id: str, expected_base: str, expected_quote: str) -> None:
    """Bitfinex spot ids resolve to canonical (base, quote) with aliases applied."""
    base, quote = _resolve_bitfinex_spot(raw_id)
    assert (base, quote) == (expected_base, expected_quote)


def test_resolve_bitfinex_spot_via_resolve_base_quote() -> None:
    """The bitfinex branch in _resolve_base_quote routes through the resolver."""
    # ALGUST previously fell through to an empty quote → was dropped entirely.
    assert _resolve_base_quote(_item("ALGUST"), "ALGUST", "bitfinex") == ("ALGO", "USDT")
    assert _resolve_base_quote(_item("AAVE:UST"), "AAVE:UST", "bitfinex") == ("AAVE", "USDT")


def test_bitfinex_derivatives_base_alias() -> None:
    """Bitfinex perp ids (BASE F0 : QUOTE F0) also apply the base alias (ALG→ALGO)."""
    assert _resolve_base_quote(_item("ALGF0:USTF0"), "ALGF0:USTF0", "bitfinex-derivatives") == ("ALGO", "USDT")


def test_passes_asset_filter_krw_only_for_upbit() -> None:
    """KRW quote is accepted ONLY for UPBIT (kimchi premium); rejected elsewhere."""
    assert _passes_asset_filter("BTC", "KRW", "SPOT_PAIR", "UPBIT") is True
    assert _passes_asset_filter("ADA", "KRW", "SPOT_PAIR", "UPBIT") is True
    assert _passes_asset_filter("BTC", "KRW", "SPOT_PAIR", "BINANCE-SPOT") is False
    # The default quotes still pass on UPBIT and elsewhere.
    assert _passes_asset_filter("ADA", "USDT", "SPOT_PAIR", "UPBIT") is True
    assert _passes_asset_filter("ALGO", "USDT", "SPOT_PAIR", "BITFINEX-SPOT") is True
    # No-venue (default) keeps the fleet quotes (no KRW).
    assert _passes_asset_filter("BTC", "KRW", "SPOT_PAIR") is False
