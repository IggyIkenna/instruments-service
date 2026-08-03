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
from unified_api_contracts.internal import InstrumentRecord, InstrumentType, MarginType

from instruments_service.reference_data.adapters.cefi.tardis import (
    _build_canonical_future_key,
    _build_canonical_perpetual_key,
    _build_dated_derivative_canonical_symbol,
    _build_perpetual_canonical_symbol,
    _disambiguate_colliding_dated_derivatives,
    _infer_margin_type,
    _parse_bybit_month_code_expiry,
    _resolve_base_quote,
    _split_bybit_symbol,
)
from instruments_service.reference_data.adapters.cefi.tardis.adapter import TardisReferenceDataAdapter


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


class TestInferMarginTypeBitgetFutures:
    """Real bug (2026-07-14): no bitget-futures branch existed → every inverse
    instrument → LINEAR. Live-verified via Bitget's own public REST
    (api.bitget.com/api/v2/mix/market/contracts?productType=coin-futures):
    USD-quoted BTCUSD/ETHUSD perpetuals + their dated-quarterly siblings
    (BTCUSDH25 etc) list coin collateral (supportMarginCoins=["BTC","ETH",...]),
    i.e. genuinely coin-margined, same shape as Bybit/Deribit/Binance-futures."""

    def test_bare_usd_month_code_future_is_inverse(self) -> None:
        """Real: BTCUSDH25 (dated quarterly, coin-margined)."""
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTCUSDH25", "bitget-futures") == MarginType.INVERSE

    def test_usdt_perpetual_is_linear(self) -> None:
        """Real: 0GUSDT is a real Bitget USDT-margined perpetual (captured today)."""
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USDT", "0GUSDT", "bitget-futures") == MarginType.LINEAR

    def test_spot_returns_none_unaffected(self) -> None:
        assert _infer_margin_type(InstrumentType.SPOT_PAIR, "USDT", "BTCUSDT", "bitget-futures") is None


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
# Bitget Futures dated quarterlies — real bug (2026-07-14): no "bitget-futures"
# branch in _resolve_base_quote, so all 16 real FUTURE symbols on this venue
# (identical no-dash CME-month-code shape as Bybit's legacy quarterlies —
# live-verified api.tardis.dev/v1/exchanges/bitget-futures) resolved quote=""
# and were silently dropped by adapter.py's empty-quote guard. Result: the
# instruments-service catalogue carried ZERO FUTURE rows for BITGET-FUTURES
# (only its 714 PERPETUAL rows), a genuine Layer-1 completeness hole for
# mvp_backfill_cefi_tick_v10_2026_06_27.md's G4 gate.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw_id", "expected_base", "expected_quote"),
    [
        ("BTCUSDH25", "BTC", "USD"),
        ("BTCUSDU26", "BTC", "USD"),
        ("ETHUSDH25", "ETH", "USD"),
        # PERPETUALs (already-working path) stay unaffected by the new branch.
        ("0GUSDT", "0G", "USDT"),
    ],
)
def test_resolve_base_quote_bitget_futures_end_to_end(raw_id: str, expected_base: str, expected_quote: str) -> None:
    base, quote = _resolve_base_quote(_item(raw_id), raw_id, "bitget-futures")
    assert (base, quote) == (expected_base, expected_quote)


# ---------------------------------------------------------------------------
# Real regression fix (2026-07-14, cefi_layer1_denominator_gaps_2026_07_03.md
# "NEW FINDING ... shared _build_canonical_future_key can collide"): BITGET-
# FUTURES's BTCUSDM26 (real Jun-2026 quarterly) and BTCUSDU26 (real Sep-2026
# quarterly) both report availableTo=2026-04-28 in Tardis's own metadata
# (live-verified 2026-07-14 via api.tardis.dev/v1/exchanges/bitget-futures;
# same for ETHUSDM26/ETHUSDU26) — since the month-code expiry fallback
# (_parse_bybit_month_code_expiry) was only wired for exchange=="bybit",
# BITGET-FUTURES fell through to that coincidentally-shared availableTo, so
# two genuinely distinct real contracts collapsed onto ONE canonical
# instrument_key. _disambiguate_colliding_dated_derivatives repairs this as a
# post-pass over one exchange's parsed batch (no extra API calls).
# ---------------------------------------------------------------------------


def _future_item(raw_id: str, available_to: str | None) -> TardisInstrumentDetail:
    """A FUTURE item with no ``expiry`` field, forcing the ``availableTo``
    fallback branch — matches the real Tardis ``/v1/exchanges`` response
    shape (no auth → no explicit ``expiry`` field), and reproduces the exact
    coincidental-collision precondition from the finding.
    """
    return TardisInstrumentDetail(id=raw_id, type="future", availableTo=available_to)


class TestDisambiguateCollidingDatedDerivatives:
    def test_bitget_futures_btc_quarterlies_collide_before_fix(self) -> None:
        """Sanity precondition: without the repair pass, two distinct real
        BITGET-FUTURES contracts really do collide on instrument_key when
        Tardis's availableTo coincides — proves the bug this test guards
        against is real, not hypothetical.
        """
        adapter = TardisReferenceDataAdapter()
        btc_m26 = adapter._parse_tardis_instrument(
            _future_item("BTCUSDM26", "2026-04-28T00:00:00.000Z"), "bitget-futures"
        )
        btc_u26 = adapter._parse_tardis_instrument(
            _future_item("BTCUSDU26", "2026-04-28T00:00:00.000Z"), "bitget-futures"
        )
        assert btc_m26 is not None
        assert btc_u26 is not None
        assert btc_m26.raw_symbol != btc_u26.raw_symbol
        assert btc_m26.instrument_key == btc_u26.instrument_key  # the real collision

    def test_bitget_futures_btc_eth_quarterlies_resolve_distinct_ids(self) -> None:
        """After the repair pass: the exact 4-row scenario from the finding
        (2 BTC + 2 ETH siblings, same coincidental availableTo) each resolve
        to a distinct canonical id, keyed off the real CME month-code letter
        the raw_id itself encodes (M=Jun, U=Sep) rather than the
        coincidentally-shared availableTo.
        """
        adapter = TardisReferenceDataAdapter()
        raw_ids = ["BTCUSDM26", "BTCUSDU26", "ETHUSDM26", "ETHUSDU26"]
        records = [
            adapter._parse_tardis_instrument(_future_item(raw_id, "2026-04-28T00:00:00.000Z"), "bitget-futures")
            for raw_id in raw_ids
        ]
        assert all(record is not None for record in records)

        _disambiguate_colliding_dated_derivatives(records)  # type: ignore[arg-type]

        keys = [record.instrument_key for record in records if record is not None]
        assert len(keys) == len(set(keys)), f"expected 4 distinct ids, got {keys}"
        for record in records:
            assert record is not None
            assert record.instrument_key == record.canonical_instrument_id
        # Real expiries: month-code M (Jun) and U (Sep) genuinely differ —
        # the symbol-encoded date wins over the coincidentally-equal
        # availableTo fallback.
        btc_m26, btc_u26, eth_m26, eth_u26 = records
        assert btc_m26 is not None and btc_u26 is not None
        assert btc_m26.expiry is not None
        assert btc_u26.expiry is not None
        assert btc_m26.expiry.month == 6
        assert btc_u26.expiry.month == 9
        assert btc_m26.expiry != btc_u26.expiry
        assert eth_m26 is not None and eth_u26 is not None
        assert eth_m26.instrument_key == "BITGET-FUTURES:FUTURE:ETH-USD@INV-" + btc_m26.expiry.strftime("%Y%m%d")
        assert eth_u26.instrument_key == "BITGET-FUTURES:FUTURE:ETH-USD@INV-" + btc_u26.expiry.strftime("%Y%m%d")

    def test_non_colliding_records_are_left_byte_identical(self) -> None:
        """The repair pass must be a no-op for the 12/16 already-correct
        BITGET-FUTURES rows this same recapture shipped — only groups that
        actually collide may be touched (stop-on-surprise: no blast radius
        beyond the real defect).
        """
        adapter = TardisReferenceDataAdapter()
        record = adapter._parse_tardis_instrument(
            _future_item("BTCUSDH25", "2025-03-28T00:00:00.000Z"), "bitget-futures"
        )
        assert record is not None
        original_key = record.instrument_key
        original_expiry = record.expiry

        _disambiguate_colliding_dated_derivatives([record])

        assert record.instrument_key == original_key
        assert record.expiry == original_expiry

    def test_fallback_embeds_raw_symbol_when_no_month_code_disambiguator_exists(self) -> None:
        """When a collision can't be resolved via the CME month-code (e.g. two
        dash-shaped raw_ids sharing an expiry, a different real shape), the
        repair falls back to the legacy VENUE:TYPE:RAW_ID embedding instead of
        leaving the collision in place or fabricating a wrong date.
        """
        expiry = datetime(2026, 4, 28, tzinfo=UTC)
        first = InstrumentRecord(
            instrument_key="BITGET-FUTURES:FUTURE:BTC-USD@INV-20260428",
            canonical_instrument_id="BITGET-FUTURES:FUTURE:BTC-USD@INV-20260428",
            venue="BITGET-FUTURES",
            instrument_type=InstrumentType.FUTURE,
            raw_symbol="BTC-28APR26-A",
            base_asset="BTC",
            quote_asset="USD",
            margin_type=MarginType.INVERSE,
            expiry=expiry,
        )
        second = InstrumentRecord(
            instrument_key="BITGET-FUTURES:FUTURE:BTC-USD@INV-20260428",
            canonical_instrument_id="BITGET-FUTURES:FUTURE:BTC-USD@INV-20260428",
            venue="BITGET-FUTURES",
            instrument_type=InstrumentType.FUTURE,
            raw_symbol="BTC-28APR26-B",
            base_asset="BTC",
            quote_asset="USD",
            margin_type=MarginType.INVERSE,
            expiry=expiry,
        )
        records = [first, second]

        _disambiguate_colliding_dated_derivatives(records)

        assert first.instrument_key == "BITGET-FUTURES:FUTURE:BTC-28APR26-A"
        assert second.instrument_key == "BITGET-FUTURES:FUTURE:BTC-28APR26-B"
        assert first.instrument_key != second.instrument_key
        assert first.canonical_instrument_id == first.instrument_key
        assert second.canonical_instrument_id == second.instrument_key


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
            "BTC", "USD", MarginType.INVERSE, expiry, strike=Decimal("48000"), option_right="C"
        )
        assert result == "BTC-USD@INV-20260710-48000-C"

    def test_marker_without_quote_raises_fail_loud(self) -> None:
        """Operator ruling 2026-07-18: a resolved @LIN/@INV marker with NO quote
        is a contradiction — the builder RAISES rather than silently dropping to
        a base-only symbol (the exact ``AVAX@LIN`` defect that lost 265k quotes).
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        with pytest.raises(ValueError, match=r"marker.*quote"):
            _build_dated_derivative_canonical_symbol("BTC", "", MarginType.INVERSE, expiry)
        with pytest.raises(ValueError, match=r"marker.*quote"):
            _build_dated_derivative_canonical_symbol(
                "BTC", "", MarginType.LINEAR, expiry, strike=Decimal("48000"), option_right="C"
            )
        with pytest.raises(ValueError, match=r"marker.*quote"):
            _build_perpetual_canonical_symbol("BTC", "", MarginType.INVERSE)

    def test_no_marker_no_quote_degrades_to_bare_base_no_raise(self) -> None:
        """When the margin type is UNRESOLVED (no marker), a bare-BASE degrade is
        still allowed — only the marker-present / quote-absent contradiction raises.
        """
        expiry = datetime(2026, 7, 10, tzinfo=UTC)
        assert _build_perpetual_canonical_symbol("BTC", "", None) == "BTC"
        assert _build_dated_derivative_canonical_symbol("BTC", "", None, expiry) == "BTC-20260710"


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


# ---------------------------------------------------------------------------
# Real regression fix (2026-07-09): the no-dash CME-month-code shape
# (BTCUSDH22) had a resolvable base/quote (fixed above) but NO expiry-
# resolution fallback branch — expiry stayed None, InstrumentRecord(FUTURE)
# raised pydantic.ValidationError unguarded, and that ONE symbol's exception
# killed the ENTIRE Bybit venue fetch (0 rows, live-reproduced 2026-07-09).
# Two independent fixes: (1) _parse_bybit_month_code_expiry — a real
# expiry-resolution branch; (2) a per-item try/except around InstrumentRecord
# construction so no future gap of this shape can zero a whole venue again.
# ---------------------------------------------------------------------------


class TestParseBybitMonthCodeExpiry:
    """_parse_bybit_month_code_expiry: last-Friday-of-contract-month convention.

    Convention cross-checked 2026-07-09 against all 42 already-resolvable
    siblings of this shape (api.tardis.dev/v1/exchanges/bybit): every one's
    real ``availableTo`` timestamp lands exactly 1 calendar day after this
    function's computed Friday, zero exceptions.
    """

    @pytest.mark.parametrize(
        ("raw_id", "expected"),
        [
            # The 4 real, currently-ACTIVE contracts of this shape (no
            # availableTo yet — these are the ones that genuinely had NO
            # expiry-resolution path before this fix).
            ("BTCUSDU26", datetime(2026, 9, 25, tzinfo=UTC)),
            ("BTCUSDZ26", datetime(2026, 12, 25, tzinfo=UTC)),
            ("ETHUSDU26", datetime(2026, 9, 25, tzinfo=UTC)),
            ("ETHUSDZ26", datetime(2026, 12, 25, tzinfo=UTC)),
            # Real, already-EXPIRED siblings — cross-check: computed Friday +
            # 1 calendar day must equal the real Tardis availableTo above.
            ("BTCUSDH22", datetime(2022, 3, 25, tzinfo=UTC)),  # availableTo 2022-03-26
            ("BTCUSDU21", datetime(2021, 9, 24, tzinfo=UTC)),  # availableTo 2021-09-25
            ("ETHUSDZ21", datetime(2021, 12, 31, tzinfo=UTC)),  # availableTo 2022-01-01
            ("BTCUSDM23", datetime(2023, 6, 30, tzinfo=UTC)),  # availableTo 2023-07-01 (month-end rollover)
            # Lowercase input still matches (raw_id.upper() inside the function).
            ("btcusdu26", datetime(2026, 9, 25, tzinfo=UTC)),
        ],
    )
    def test_resolves_last_friday_of_contract_month(self, raw_id: str, expected: object) -> None:
        assert _parse_bybit_month_code_expiry(raw_id) == expected

    @pytest.mark.parametrize(
        "raw_id",
        [
            "BTCUSDT",  # perpetual — no month code
            "BTC-01DEC23",  # dash shape — handled by a different fallback
            "BTCUSDT-25DEC26",  # concatenated-quote dash shape
            "NOTAMATCH",
            "",
        ],
    )
    def test_non_matching_shapes_return_none(self, raw_id: str) -> None:
        assert _parse_bybit_month_code_expiry(raw_id) is None


class TestBybitMonthCodeExpiryWiredIntoAdapter:
    """End-to-end: the real adapter._parse_tardis_instrument, not just the
    standalone parsing helper — proves the fix is actually wired into the
    live fallback chain (adapter.py), not just unit-tested in isolation.
    """

    def _item(self, raw_id: str) -> TardisInstrumentDetail:
        return TardisInstrumentDetail(id=raw_id, type="future")

    @pytest.mark.parametrize(
        ("raw_id", "expected_expiry"),
        [
            ("BTCUSDU26", datetime(2026, 9, 25, tzinfo=UTC)),
            ("BTCUSDZ26", datetime(2026, 12, 25, tzinfo=UTC)),
            ("ETHUSDU26", datetime(2026, 9, 25, tzinfo=UTC)),
            ("ETHUSDZ26", datetime(2026, 12, 25, tzinfo=UTC)),
        ],
    )
    def test_previously_broken_active_contracts_now_resolve(self, raw_id: str, expected_expiry: object) -> None:
        """Before the fix: each of these 4 real, currently-active Bybit
        symbols raised pydantic.ValidationError inside ``_parse_tardis_
        instrument`` (expiry=None on a FUTURE) — and because that exception
        was unguarded, it took the whole Bybit venue down with it. After the
        fix: a valid InstrumentRecord with the correct expiry.
        """
        adapter = TardisReferenceDataAdapter()
        record = adapter._parse_tardis_instrument(self._item(raw_id), "bybit")
        assert record is not None
        assert record.expiry == expected_expiry
        assert record.instrument_type == InstrumentType.FUTURE
        assert record.base_asset in ("BTC", "ETH")
        assert record.quote_asset == "USD"

    def test_bad_symbol_is_skipped_not_raised_and_does_not_poison_the_batch(self) -> None:
        """Regression for the resiliency gap itself (independent of the
        specific month-code bug): a synthetic FUTURE symbol that resolves a
        non-empty quote (passes the pre-existing empty-quote guard) but has
        NO matching expiry-resolution fallback of any kind must be SKIPPED
        (return None), not raise — and must not stop the adapter from
        parsing the good items around it in the same per-venue loop (the
        exact failure mode that zeroed the whole Bybit venue pre-fix).
        """
        adapter = TardisReferenceDataAdapter()
        good = self._item("BTCUSDH22")
        unresolvable_expiry = TardisInstrumentDetail(id="AAAUSDT", type="future")

        results = [
            record
            for record in (
                adapter._parse_tardis_instrument(good, "bybit"),
                adapter._parse_tardis_instrument(unresolvable_expiry, "bybit"),
                adapter._parse_tardis_instrument(good, "bybit"),
            )
            if record is not None
        ]

        assert len(results) == 2
        assert all(r.raw_symbol == "BTCUSDH22" for r in results)
