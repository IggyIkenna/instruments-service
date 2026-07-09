"""Regression tests: Deribit's shared @LIN/@INV canonical-symbol builder
extension (instrument_id_format_canonicalization_2026_07_08.md finding 1,
PERPETUAL scope expanded 2026-07-09).

Mirrors ``test_bybit_kraken_futures_canonical_id.py`` / ``test_okx_margin_
type_and_canonical_id.py`` — the same shared Bybit/Kraken-Futures/OKX dated-
derivative + perpetual builder set (``_build_canonical_perpetual_key``,
``_build_canonical_future_key``, ``_build_dated_derivative_canonical_symbol``)
is reused here; Deribit's own real contribution is ``_build_canonical_option_
key`` (the OPTION analog of ``_build_canonical_future_key`` — no prior venue
in this migration lists options) plus real evidence that Deribit's
PERPETUAL/FUTURE/OPTION targets all drop the quote segment (``quote=""``),
unlike Kraken-Futures/Bybit which keep it.

Real current → target pairs (confirmed against the real prod cefi catalog
(``instruments-store-cefi-prd-<project>/prod/catalog.parquet``), 2026-07-09 —
263,979 real DERIBIT PERPETUAL/FUTURE/OPTION rows, 0 parse failures, 0
collisions against this exact transform):
  DERIBIT:PERPETUAL:BTC-USD        (inverse) -> DERIBIT:PERPETUAL:BTC-USD@INV
  DERIBIT:PERPETUAL:BTC-USDC       (linear)  -> DERIBIT:PERPETUAL:BTC-USDC@LIN
  DERIBIT:FUTURE:BTC-10JUL26       (inverse) -> DERIBIT:FUTURE:BTC@INV-20260710
  DERIBIT:FUTURE:AVAX_USDC-10JUL26 (linear)  -> DERIBIT:FUTURE:AVAX@LIN-20260710
  DERIBIT:OPTION:BTC-10JUL26-48000-C  (inverse) -> DERIBIT:OPTION:BTC@INV-20260710-48000-C
  DERIBIT:OPTION:BTC_USDC-...-48000-C (linear)  -> DERIBIT:OPTION:BTC@LIN-...-48000-C
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import MarginType

from instruments_service.reference_data.adapters.cefi.tardis.parsing import (
    _build_canonical_future_key,
    _build_canonical_option_key,
    _build_canonical_perpetual_key,
    _infer_margin_type,
)


class TestDeribitPerpetualKeepsQuote:
    def test_inverse_perpetual(self) -> None:
        """Real: DERIBIT:PERPETUAL:BTC-USD is genuinely inverse (coin-settled)."""
        key = _build_canonical_perpetual_key("DERIBIT", "BTC", "USD", MarginType.INVERSE)
        assert key == "DERIBIT:PERPETUAL:BTC-USD@INV"

    def test_linear_perpetual(self) -> None:
        """Real: DERIBIT:PERPETUAL:BTC-USDC is genuinely linear (USDC-settled)."""
        key = _build_canonical_perpetual_key("DERIBIT", "BTC", "USDC", MarginType.LINEAR)
        assert key == "DERIBIT:PERPETUAL:BTC-USDC@LIN"

    def test_same_base_distinct_margin_types_produce_distinct_keys(self) -> None:
        inverse = _build_canonical_perpetual_key("DERIBIT", "BTC", "USD", MarginType.INVERSE)
        linear = _build_canonical_perpetual_key("DERIBIT", "BTC", "USDC", MarginType.LINEAR)
        assert inverse != linear


class TestDeribitDatedDerivativesDropQuote:
    """Deribit's real target drops the quote segment for FUTURE/OPTION —
    quote="" — unlike Kraken-Futures/Bybit, which keep it. Matches the doc's
    literal example: DERIBIT:OPTION:BTC@INV-20260710-48000-C, not
    DERIBIT:OPTION:BTC-USD@INV-....
    """

    def test_inverse_future_matches_doc_target(self) -> None:
        """Real: DERIBIT:FUTURE:BTC-10JUL26 (raw DDMMMYY) -> the doc's own
        worked example, DERIBIT:FUTURE:BTC@INV-20260710.
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_future_key("DERIBIT", "BTC", "", MarginType.INVERSE, expiry)
        assert key == "DERIBIT:FUTURE:BTC@INV-20260710"

    def test_linear_future(self) -> None:
        """Real: DERIBIT:FUTURE:AVAX_USDC-10JUL26 (linear, real prod row)."""
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_future_key("DERIBIT", "AVAX", "", MarginType.LINEAR, expiry)
        assert key == "DERIBIT:FUTURE:AVAX@LIN-20260710"

    def test_inverse_option_matches_doc_target_verbatim(self) -> None:
        """Matches CEFI_INSTRUMENTS.md's decided target example verbatim:
        DERIBIT:OPTION:BTC@INV-20260710-48000-C.
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "", MarginType.INVERSE, expiry, Decimal("48000"), "C")
        assert key == "DERIBIT:OPTION:BTC@INV-20260710-48000-C"

    def test_linear_option(self) -> None:
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "", MarginType.LINEAR, expiry, Decimal("48000"), "C")
        assert key == "DERIBIT:OPTION:BTC@LIN-20260710-48000-C"

    def test_inverse_vs_linear_option_same_base_distinct_keys(self) -> None:
        """Same base/expiry/strike/right, different margin type — must be
        DISTINCT canonical keys (the collision the marker exists to prevent).
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        inverse = _build_canonical_option_key("DERIBIT", "BTC", "", MarginType.INVERSE, expiry, Decimal("48000"), "C")
        linear = _build_canonical_option_key("DERIBIT", "BTC", "", MarginType.LINEAR, expiry, Decimal("48000"), "C")
        assert inverse != linear

    def test_put_right(self) -> None:
        expiry = datetime(2026, 4, 25, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "ETH", "", MarginType.INVERSE, expiry, Decimal("3500"), "P")
        assert key == "DERIBIT:OPTION:ETH@INV-20260425-3500-P"

    def test_non_integer_strike_formatted_without_trailing_zeros(self) -> None:
        expiry = datetime(2026, 4, 25, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "", MarginType.INVERSE, expiry, Decimal("48000.5"), "C")
        assert key == "DERIBIT:OPTION:BTC@INV-20260425-48000.5-C"


class TestInferMarginTypeDeribitUnaffected:
    """Deribit's quote-based margin inference (USD=inverse, else linear
    fallthrough) was already correct — confirmed here so a future edit to
    ``_infer_margin_type`` doesn't silently regress this venue.
    """

    def test_usd_quote_is_inverse(self) -> None:
        from unified_api_contracts.internal import InstrumentType

        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTC-PERPETUAL", "deribit") == MarginType.INVERSE

    def test_usdc_quote_is_linear(self) -> None:
        from unified_api_contracts.internal import InstrumentType

        assert (
            _infer_margin_type(InstrumentType.PERPETUAL, "USDC", "BTC_USDC-PERPETUAL", "deribit") == MarginType.LINEAR
        )
