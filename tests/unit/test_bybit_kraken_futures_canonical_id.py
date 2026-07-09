"""Regression tests: Bybit + Kraken-Futures margin-type bugs and the @LIN/@INV
canonical-symbol builder (instrument_id_format_canonicalization_2026_07_08.md
finding 1, PERPETUAL scope expanded 2026-07-09).

Two real, pre-existing margin-type-inference bugs fixed here:
  * Bybit had NO bybit-specific branch in ``_infer_margin_type`` — every
    inverse (coin-margined) instrument defaulted to the LINEAR fallback.
    Real example: ``BYBIT:PERPETUAL:BTC-USD`` is genuinely inverse but was
    mislabeled linear.
  * Kraken-Futures (Tardis ``cryptofacilities``) had NO cryptofacilities
    branch either — every real inverse product (``PI_``/``FI_``-prefixed)
    was also mislabeled linear. Real proof case (the exact evidence the
    operator used to justify extending the ``@LIN``/``@INV`` marker to
    PERPETUAL, not just dated derivatives): ``PF_AAVEUSD`` (genuinely
    linear) and ``PI_XBTUSD`` (genuinely inverse) both quote-resolve to
    ``USD`` — margin type is NOT quote-inferable for this venue, it lives in
    the raw instrument-type prefix.

All raw ids below are REAL, live-verified against
``api.tardis.dev/v1/exchanges/{bybit,cryptofacilities}`` on 2026-07-09 — not
fabricated (the workspace has an established history of catching fabricated
Bybit examples; every shape here was independently confirmed present in the
real, current Tardis symbol listing before being used as a test fixture).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from unified_api_contracts import TardisInstrumentDetail
from unified_api_contracts.internal import InstrumentType, MarginType

from instruments_service.reference_data.adapters.cefi.tardis import (
    _build_canonical_future_key,
    _build_canonical_perpetual_key,
    _build_dated_derivative_canonical_symbol,
    _build_perpetual_canonical_symbol,
    _infer_margin_type,
    _resolve_base_quote,
    _split_bybit_symbol,
)


def _item(raw_id: str) -> TardisInstrumentDetail:
    """A Tardis item with no explicit base/quote → forces symbol-based parsing.

    Matches production reality: ``_fetch_exchange_instruments`` calls the
    free/no-auth ``/v1/exchanges/{exchange}`` endpoint, whose
    ``TardisExchangeDetail.instruments`` mapping never populates
    ``baseCurrency``/``quoteCurrency`` (only the authenticated, not-called
    ``/v1/instruments/{exchange}`` endpoint would). Every real Bybit/
    Kraken-Futures instrument goes through the symbol-splitting fallback.
    """
    return TardisInstrumentDetail(id=raw_id, type="future")


# ---------------------------------------------------------------------------
# _infer_margin_type — the real bug fixes
# ---------------------------------------------------------------------------


class TestInferMarginTypeBybit:
    """Real bug: no bybit branch existed → every inverse instrument → LINEAR."""

    def test_bare_usd_perpetual_is_inverse(self) -> None:
        """Real: BTCUSD is a real Bybit coin-margined perpetual (28 real rows)."""
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTCUSD", "bybit") == MarginType.INVERSE

    def test_usdt_perpetual_is_linear(self) -> None:
        """Real: BTCUSDT is a real Bybit USDT-margined perpetual (907 real rows)."""
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USDT", "BTCUSDT", "bybit") == MarginType.LINEAR

    def test_bare_dash_future_is_inverse(self) -> None:
        """Real: BTC-01DEC23 (classic coin-margined quarterly, 255 real rows)."""
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTC-01DEC23", "bybit") == MarginType.INVERSE

    def test_concatenated_quote_future_is_linear(self) -> None:
        """Real: BTCUSDT-25DEC26 (USDT-margined dated future, 360 real rows)."""
        assert _infer_margin_type(InstrumentType.FUTURE, "USDT", "BTCUSDT-25DEC26", "bybit") == MarginType.LINEAR

    def test_month_code_future_is_inverse(self) -> None:
        """Real: BTCUSDH22 (legacy CME-style coin-margined quarterly, 46 real rows)."""
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTCUSDH22", "bybit") == MarginType.INVERSE

    def test_spot_returns_none_unaffected(self) -> None:
        assert _infer_margin_type(InstrumentType.SPOT_PAIR, "USDT", "BTCUSDT", "bybit") is None


class TestInferMarginTypeKrakenFutures:
    """Real bug: no cryptofacilities branch existed → every inverse instrument → LINEAR.

    Real proof case: PI_XBTUSD (inverse) and PF_XBTUSD (linear) BOTH quote to
    "USD" via _split_kraken_symbol — margin type is only recoverable from the
    raw PI_/PF_/FI_/FF_ prefix, never the quote.
    """

    def test_pi_prefix_perpetual_is_inverse(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "PI_XBTUSD", "cryptofacilities") == (
            MarginType.INVERSE
        )

    def test_pf_prefix_perpetual_is_linear(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "PF_XBTUSD", "cryptofacilities") == (
            MarginType.LINEAR
        )

    def test_operator_proof_case_aave_linear_vs_btc_inverse(self) -> None:
        """The exact real-world pair the operator cited to justify @LIN/@INV on
        PERPETUAL: KRAKEN-FUTURES:PERPETUAL:AAVE-USD (linear) vs
        KRAKEN-FUTURES:PERPETUAL:BTC-USD (inverse) — same quote, different
        real margin type, previously indistinguishable and both mislabeled
        linear.
        """
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "PF_AAVEUSD", "cryptofacilities") == (
            MarginType.LINEAR
        )
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "PI_XBTUSD", "cryptofacilities") == (
            MarginType.INVERSE
        )

    def test_fi_prefix_future_is_inverse(self) -> None:
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "FI_XBTUSD_260731", "cryptofacilities") == (
            MarginType.INVERSE
        )

    def test_ff_prefix_future_is_linear(self) -> None:
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "FF_XBTUSD_260731", "cryptofacilities") == (
            MarginType.LINEAR
        )


# ---------------------------------------------------------------------------
# _split_bybit_symbol — the real base/quote resolution gap
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw_id", "expected_base", "expected_quote"),
    [
        # Bare dash shape (255 real rows) — quote implied, not in the symbol.
        ("BTC-01DEC23", "BTC", "USD"),
        ("ETH-08DEC23", "ETH", "USD"),
        # Concatenated-quote dash shape (360 real rows).
        ("BTCUSDT-25DEC26", "BTC", "USDT"),
        ("DOGEUSDT-10JUL26", "DOGE", "USDT"),
        # No-dash CME-style month-code shape (46 real rows) — previously
        # silently DROPPED (quote resolution failed entirely).
        ("BTCUSDH22", "BTC", "USD"),
        ("ETHUSDZ21", "ETH", "USD"),
        # PERPETUAL shapes are NOT handled by this function (no dash, no
        # month-code match) — caller falls through to the generic splitter.
        ("BTCUSDT", "", ""),
        ("BTCUSD", "", ""),
    ],
)
def test_split_bybit_symbol(raw_id: str, expected_base: str, expected_quote: str) -> None:
    assert _split_bybit_symbol(raw_id) == (expected_base, expected_quote)


@pytest.mark.parametrize(
    ("raw_id", "expected_base", "expected_quote"),
    [
        ("BTC-01DEC23", "BTC", "USD"),
        ("BTCUSDT-25DEC26", "BTC", "USDT"),
        ("BTCUSDH22", "BTC", "USD"),
        # PERPETUALs fall through to the pre-existing generic _split_symbol path.
        ("BTCUSDT", "BTC", "USDT"),
        ("BTCUSD", "BTC", "USD"),
    ],
)
def test_resolve_base_quote_bybit_end_to_end(raw_id: str, expected_base: str, expected_quote: str) -> None:
    base, quote = _resolve_base_quote(_item(raw_id), raw_id, "bybit")
    assert (base, quote) == (expected_base, expected_quote)


# ---------------------------------------------------------------------------
# Canonical @LIN/@INV symbol + full VENUE:TYPE:SYMBOL key builders
# ---------------------------------------------------------------------------


class TestCanonicalSymbolBuilders:
    def test_perpetual_inverse_marker(self) -> None:
        assert _build_perpetual_canonical_symbol("BTC", "USD", MarginType.INVERSE) == "BTC-USD@INV"

    def test_perpetual_linear_marker(self) -> None:
        assert _build_perpetual_canonical_symbol("BTC", "USDT", MarginType.LINEAR) == "BTC-USDT@LIN"

    def test_perpetual_unknown_margin_omits_marker(self) -> None:
        assert _build_perpetual_canonical_symbol("BTC", "USD", None) == "BTC-USD"

    def test_dated_derivative_inverse_yyyymmdd(self) -> None:
        expiry = datetime(2026, 7, 31, tzinfo=UTC)
        assert _build_dated_derivative_canonical_symbol("XBT", "USD", MarginType.INVERSE, expiry) == (
            "XBT-USD@INV-20260731"
        )

    def test_dated_derivative_linear_yyyymmdd(self) -> None:
        expiry = datetime(2023, 12, 1, tzinfo=UTC)
        assert _build_dated_derivative_canonical_symbol("BTC", "USDT", MarginType.LINEAR, expiry) == (
            "BTC-USDT@LIN-20231201"
        )

    def test_dated_derivative_with_strike_and_right(self) -> None:
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        result = _build_dated_derivative_canonical_symbol(
            "BTC", "", MarginType.INVERSE, expiry, strike=Decimal("48000"), option_right="C"
        )
        assert result == "BTC@INV-20260710-48000-C"


class TestCanonicalKeyBuildersRoutedThroughSharedUacBuilder:
    """Full VENUE:TYPE:SYMBOL construction via build_instrument_id(passthrough=True)."""

    def test_kraken_futures_perpetual_operator_proof_pair(self) -> None:
        """Matches the exact operator proof examples from
        instrument_id_format_canonicalization_2026_07_08.md finding 1.
        """
        linear = _build_canonical_perpetual_key("KRAKEN-FUTURES", "AAVE", "USD", MarginType.LINEAR)
        inverse = _build_canonical_perpetual_key("KRAKEN-FUTURES", "BTC", "USD", MarginType.INVERSE)
        assert linear == "KRAKEN-FUTURES:PERPETUAL:AAVE-USD@LIN"
        assert inverse == "KRAKEN-FUTURES:PERPETUAL:BTC-USD@INV"
        # Same quote, different margin type — must be DISTINCT canonical keys
        # (this is the exact collision the marker exists to prevent).
        assert linear != inverse

    def test_kraken_futures_dated_future_matches_doc_target(self) -> None:
        """Matches CEFI_INSTRUMENTS.md's decided target example verbatim."""
        expiry = datetime(2026, 7, 31, tzinfo=UTC)
        key = _build_canonical_future_key("KRAKEN-FUTURES", "XBT", "USD", MarginType.INVERSE, expiry)
        assert key == "KRAKEN-FUTURES:FUTURE:XBT-USD@INV-20260731"

    def test_bybit_perpetual_inverse(self) -> None:
        """Real proof: BYBIT:PERPETUAL:BTC-USD is genuinely inverse."""
        key = _build_canonical_perpetual_key("BYBIT", "BTC", "USD", MarginType.INVERSE)
        assert key == "BYBIT:PERPETUAL:BTC-USD@INV"

    def test_bybit_perpetual_linear(self) -> None:
        key = _build_canonical_perpetual_key("BYBIT", "BTC", "USDT", MarginType.LINEAR)
        assert key == "BYBIT:PERPETUAL:BTC-USDT@LIN"

    def test_bybit_dated_future_bare_shape_is_inverse_usd(self) -> None:
        """Real target for BTC-01DEC23: base=BTC, quote=USD (implied), margin=INVERSE
        — the classic coin-margined quarterly, verified against the real
        Tardis symbol listing (255 real BTC-DDMMMYY rows exist, all inverse;
        no USDT-quoted linear product uses this bare-dash shape).
        """
        expiry = datetime(2023, 12, 1, tzinfo=UTC)
        key = _build_canonical_future_key("BYBIT", "BTC", "USD", MarginType.INVERSE, expiry)
        assert key == "BYBIT:FUTURE:BTC-USD@INV-20231201"

    def test_bybit_dated_future_concatenated_shape_is_linear_usdt(self) -> None:
        """Real target for BTCUSDT-25DEC26: base=BTC, quote=USDT, margin=LINEAR."""
        expiry = datetime(2026, 12, 25, tzinfo=UTC)
        key = _build_canonical_future_key("BYBIT", "BTC", "USDT", MarginType.LINEAR, expiry)
        assert key == "BYBIT:FUTURE:BTC-USDT@LIN-20261225"


# ---------------------------------------------------------------------------
# End-to-end: real raw_id → fixed margin inference → canonical key
# ---------------------------------------------------------------------------


def test_end_to_end_bybit_future_bare_shape() -> None:
    """BTC-01DEC23 through the full corrected pipeline: split → infer margin
    → build canonical key. Confirms the fix chain from raw Tardis symbol to
    the final @LIN/@INV-embedded canonical id.
    """
    raw_id = "BTC-01DEC23"
    base, quote = _resolve_base_quote(_item(raw_id), raw_id, "bybit")
    margin = _infer_margin_type(InstrumentType.FUTURE, quote, raw_id, "bybit")
    expiry = datetime(2023, 12, 1, tzinfo=UTC)
    key = _build_canonical_future_key("BYBIT", base, quote, margin, expiry)
    assert key == "BYBIT:FUTURE:BTC-USD@INV-20231201"


def test_end_to_end_kraken_futures_pi_vs_pf_no_longer_collide() -> None:
    """Before the fix: both PI_XBTUSD and PF_XBTUSD resolved base=BTC,
    quote=USD, and margin_type fell through to the LINEAR default for both —
    an undetectable collision if margin type were ever embedded in the key.
    After the fix, they resolve to genuinely distinct margin types and
    therefore distinct canonical keys.
    """
    pi_base, pi_quote = _resolve_base_quote(_item("PI_XBTUSD"), "PI_XBTUSD", "cryptofacilities")
    pf_base, pf_quote = _resolve_base_quote(_item("PF_XBTUSD"), "PF_XBTUSD", "cryptofacilities")
    assert (pi_base, pi_quote) == (pf_base, pf_quote) == ("BTC", "USD")

    pi_margin = _infer_margin_type(InstrumentType.PERPETUAL, pi_quote, "PI_XBTUSD", "cryptofacilities")
    pf_margin = _infer_margin_type(InstrumentType.PERPETUAL, pf_quote, "PF_XBTUSD", "cryptofacilities")
    assert pi_margin == MarginType.INVERSE
    assert pf_margin == MarginType.LINEAR

    pi_key = _build_canonical_perpetual_key("KRAKEN-FUTURES", pi_base, pi_quote, pi_margin)
    pf_key = _build_canonical_perpetual_key("KRAKEN-FUTURES", pf_base, pf_quote, pf_margin)
    assert pi_key == "KRAKEN-FUTURES:PERPETUAL:BTC-USD@INV"
    assert pf_key == "KRAKEN-FUTURES:PERPETUAL:BTC-USD@LIN"
    assert pi_key != pf_key
