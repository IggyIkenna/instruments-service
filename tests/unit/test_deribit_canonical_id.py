"""Regression tests: Deribit's shared @LIN/@INV canonical-symbol builder
extension (instrument_id_format_canonicalization_2026_07_08.md finding 1,
PERPETUAL scope expanded 2026-07-09; QUOTE-ALWAYS-PRESENT ruling 2026-07-18).

Mirrors ``test_bybit_kraken_futures_canonical_id.py`` / ``test_okx_margin_
type_and_canonical_id.py`` — the same shared Bybit/Kraken-Futures/OKX dated-
derivative + perpetual builder set (``_build_canonical_perpetual_key``,
``_build_canonical_future_key``, ``_build_dated_derivative_canonical_symbol``)
is reused here; Deribit's own real contribution is ``_build_canonical_option_
key`` (the OPTION analog of ``_build_canonical_future_key`` — no prior venue
in this migration lists options).

OPERATOR RULING 2026-07-18 (supersedes the earlier "Deribit drops the quote for
dated derivatives" decision): the canonical id is ALWAYS
``VENUE:TYPE:BASE-QUOTE@MARGIN[-YYYYMMDD][-STRIKE-C|P]`` — the quote is present
regardless of venue/asset class. For Deribit the quote is ``USDC`` for linear
(USDC-settled, the ``AVAX_USDC-…`` family) and ``USD`` for inverse (coin-settled,
the classic BTC/ETH family).

The exact LIVE defect rows this fix targets (verified on prod/catalog.parquet,
2026-07-18 — 265,538 DERIBIT rows missing the quote):
  raw ``AVAX_USDC-1APR26`` (linear)   -> DERIBIT:FUTURE:AVAX-USDC@LIN-20260401
  raw ``BTC-5APR19-3250-C`` (inverse) -> DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C

Other current -> target pairs (from the real prod cefi catalog):
  DERIBIT:PERPETUAL:BTC-USD        (inverse) -> DERIBIT:PERPETUAL:BTC-USD@INV
  DERIBIT:PERPETUAL:BTC-USDC       (linear)  -> DERIBIT:PERPETUAL:BTC-USDC@LIN
  DERIBIT:FUTURE:BTC-10JUL26       (inverse) -> DERIBIT:FUTURE:BTC-USD@INV-20260710
  DERIBIT:FUTURE:AVAX_USDC-10JUL26 (linear)  -> DERIBIT:FUTURE:AVAX-USDC@LIN-20260710
  DERIBIT:OPTION:BTC-10JUL26-48000-C  (inverse) -> DERIBIT:OPTION:BTC-USD@INV-20260710-48000-C
  DERIBIT:OPTION:BTC_USDC-...-48000-C (linear)  -> DERIBIT:OPTION:BTC-USDC@LIN-...-48000-C
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts import TardisInstrumentDetail
from unified_api_contracts.internal import InstrumentType, MarginType

from instruments_service.reference_data.adapters.cefi.tardis.adapter import TardisReferenceDataAdapter
from instruments_service.reference_data.adapters.cefi.tardis.parsing import (
    _build_canonical_future_key,
    _build_canonical_option_key,
    _build_canonical_perpetual_key,
    _infer_margin_type,
    _resolve_base_quote,
)


def _item(raw_id: str, itype: str) -> TardisInstrumentDetail:
    """A Tardis item with no explicit base/quote → forces symbol-based parsing.

    Matches production reality: the free/no-auth ``/v1/exchanges/{exchange}``
    endpoint never populates ``baseCurrency``/``quoteCurrency`` — every real
    Deribit instrument goes through the symbol-splitting fallback.
    """
    return TardisInstrumentDetail(id=raw_id, type=itype)


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


class TestDeribitDatedDerivativesKeepQuote:
    """Operator ruling 2026-07-18: Deribit's dated FUTURE/OPTION now KEEP the
    quote in the BASE-QUOTE segment (``USD`` inverse / ``USDC`` linear), exactly
    like Kraken-Futures/Bybit/OKX — no longer dropped. The earlier
    quote-dropping target (``DERIBIT:OPTION:BTC@INV-…``) is superseded.
    """

    def test_inverse_future_carries_usd_quote(self) -> None:
        """Real: DERIBIT:FUTURE:BTC-10JUL26 (inverse) -> BTC-USD@INV-20260710."""
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_future_key("DERIBIT", "BTC", "USD", MarginType.INVERSE, expiry)
        assert key == "DERIBIT:FUTURE:BTC-USD@INV-20260710"

    def test_linear_future_carries_usdc_quote(self) -> None:
        """Real: DERIBIT:FUTURE:AVAX_USDC-10JUL26 (linear) -> AVAX-USDC@LIN-20260710."""
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_future_key("DERIBIT", "AVAX", "USDC", MarginType.LINEAR, expiry)
        assert key == "DERIBIT:FUTURE:AVAX-USDC@LIN-20260710"

    def test_inverse_option_carries_usd_quote(self) -> None:
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "USD", MarginType.INVERSE, expiry, Decimal("48000"), "C")
        assert key == "DERIBIT:OPTION:BTC-USD@INV-20260710-48000-C"

    def test_linear_option_carries_usdc_quote(self) -> None:
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "USDC", MarginType.LINEAR, expiry, Decimal("48000"), "C")
        assert key == "DERIBIT:OPTION:BTC-USDC@LIN-20260710-48000-C"

    def test_inverse_vs_linear_option_same_base_distinct_keys(self) -> None:
        """Same base/expiry/strike/right, different margin type + quote — must be
        DISTINCT canonical keys (the collision the marker+quote exist to prevent).
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        inverse = _build_canonical_option_key(
            "DERIBIT", "BTC", "USD", MarginType.INVERSE, expiry, Decimal("48000"), "C"
        )
        linear = _build_canonical_option_key("DERIBIT", "BTC", "USDC", MarginType.LINEAR, expiry, Decimal("48000"), "C")
        assert inverse != linear

    def test_put_right(self) -> None:
        expiry = datetime(2026, 4, 25, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "ETH", "USD", MarginType.INVERSE, expiry, Decimal("3500"), "P")
        assert key == "DERIBIT:OPTION:ETH-USD@INV-20260425-3500-P"

    def test_non_integer_strike_formatted_without_trailing_zeros(self) -> None:
        expiry = datetime(2026, 4, 25, tzinfo=UTC)
        key = _build_canonical_option_key("DERIBIT", "BTC", "USD", MarginType.INVERSE, expiry, Decimal("48000.5"), "C")
        assert key == "DERIBIT:OPTION:BTC-USD@INV-20260425-48000.5-C"


class TestInferMarginTypeDeribitUnaffected:
    """Deribit's quote-based margin inference (USD=inverse, else linear
    fallthrough) was already correct — confirmed here so a future edit to
    ``_infer_margin_type`` doesn't silently regress this venue.
    """

    def test_usd_quote_is_inverse(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTC-PERPETUAL", "deribit") == MarginType.INVERSE

    def test_usdc_quote_is_linear(self) -> None:
        assert (
            _infer_margin_type(InstrumentType.PERPETUAL, "USDC", "BTC_USDC-PERPETUAL", "deribit") == MarginType.LINEAR
        )


class TestLiveDefectRowsResolveEndToEnd:
    """The two EXACT live defect rows from the operator's 2026-07-18 ruling,
    driven through the real resolution chain (``_resolve_base_quote`` ->
    ``_infer_margin_type`` -> canonical-key builder) so the raw Deribit symbol
    provably produces the quote-carrying canonical id — not just the builder in
    isolation.
    """

    def test_linear_future_avax_usdc(self) -> None:
        """raw ``AVAX_USDC-1APR26`` (linear) -> DERIBIT:FUTURE:AVAX-USDC@LIN-20260401."""
        raw_id = "AVAX_USDC-1APR26"
        base, quote = _resolve_base_quote(_item(raw_id, "future"), raw_id, "deribit")
        assert (base, quote) == ("AVAX", "USDC")
        margin = _infer_margin_type(InstrumentType.FUTURE, quote, raw_id, "deribit")
        assert margin == MarginType.LINEAR
        key = _build_canonical_future_key("DERIBIT", base, quote, margin, datetime(2026, 4, 1, tzinfo=UTC))
        assert key == "DERIBIT:FUTURE:AVAX-USDC@LIN-20260401"

    def test_inverse_option_btc_usd(self) -> None:
        """raw ``BTC-5APR19-3250-C`` (inverse) -> DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C."""
        raw_id = "BTC-5APR19-3250-C"
        base, quote = _resolve_base_quote(_item(raw_id, "option"), raw_id, "deribit")
        assert (base, quote) == ("BTC", "USD")
        margin = _infer_margin_type(InstrumentType.OPTION, quote, raw_id, "deribit")
        assert margin == MarginType.INVERSE
        key = _build_canonical_option_key(
            "DERIBIT", base, quote, margin, datetime(2019, 4, 5, tzinfo=UTC), Decimal("3250"), "C"
        )
        assert key == "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C"


class TestLiveDefectRowsWiredIntoAdapter:
    """The two EXACT live defect rows through the REAL
    ``adapter._parse_tardis_instrument`` — proves the quote-drop removal is
    actually wired into the live capture path (adapter.py), not just the
    standalone builders. This is the direct regression guard for the
    ``dated_quote = "" if exchange == "deribit"`` defect that was removed.
    """

    def test_linear_future_avax_usdc_instrument_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        record = adapter._parse_tardis_instrument(_item("AVAX_USDC-1APR26", "future"), "deribit")
        assert record is not None
        assert record.instrument_key == "DERIBIT:FUTURE:AVAX-USDC@LIN-20260401"
        assert record.canonical_instrument_id == "DERIBIT:FUTURE:AVAX-USDC@LIN-20260401"
        assert record.base_asset == "AVAX"
        assert record.quote_asset == "USDC"
        assert record.margin_type == MarginType.LINEAR

    def test_inverse_option_btc_usd_instrument_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        record = adapter._parse_tardis_instrument(_item("BTC-5APR19-3250-C", "option"), "deribit")
        assert record is not None
        assert record.instrument_key == "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C"
        assert record.canonical_instrument_id == "DERIBIT:OPTION:BTC-USD@INV-20190405-3250-C"
        assert record.base_asset == "BTC"
        assert record.quote_asset == "USD"
        assert record.margin_type == MarginType.INVERSE
