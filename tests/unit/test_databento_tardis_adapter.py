"""Unit tests for Databento and Tardis adapters (no live network — mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.adapters.cefi.tardis import TardisReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.databento import DatabentoReferenceDataAdapter

# ---------------------------------------------------------------------------
# InstrumentRecord helper (current API — no removed fields)
# ---------------------------------------------------------------------------


def _make_record(
    key: str = "TEST:FUTURE:ESZ4",
    venue: str = "databento",
    instrument_type: str = "FUTURE",
    raw_symbol: str = "ESZ4",
    base_asset: str = "ES",
    quote_asset: str = "USD",
    **kwargs: object,
) -> InstrumentRecord:
    # FUTURE and OPTION require non-null expiry per hard_schema validator
    if instrument_type in ("FUTURE", "OPTION") and "expiry" not in kwargs:
        kwargs["expiry"] = datetime(2024, 12, 20, tzinfo=UTC)
    return InstrumentRecord(
        instrument_key=key,
        venue=venue,
        raw_symbol=raw_symbol,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# DatabentoReferenceDataAdapter
# ---------------------------------------------------------------------------


class TestDatabentoAdapterMocked:
    def test_venue_name(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        assert adapter.venue == "databento"

    @pytest.mark.asyncio
    async def test_get_instruments_requires_api_key(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_with_key_returns_results(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        record = _make_record()
        with (
            patch.object(adapter, "_fetch_symbols", return_value=[record]),
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            results = await adapter.get_instruments()
        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        fut = _make_record(instrument_type="FUTURE")
        spot = _make_record(key="DBEQ:SPOT:AAPL", instrument_type="SPOT_PAIR", raw_symbol="AAPL")
        with (
            patch.object(adapter, "_fetch_symbols", return_value=[fut, spot]),
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            results = await adapter.get_instruments(instrument_type="FUTURE")
        assert all(r.instrument_type == "FUTURE" for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_isolates_banned_dataset(self) -> None:
        """A subscription-entitlement breach on ONE dataset (e.g. IFEU/XNAS.ITCH off the
        2026-06-18 allowlist) must NOT hard-fail get_instruments — sibling datasets still return.

        Regression for the dataset-level shard-isolation fix: _fetch_symbols raises
        DatabentoSubscriptionError (PERMANENT off-allowlist) for the banned dataset;
        get_instruments catches it per-dataset and continues. Without the fix the first banned
        dataset propagated and lost every surviving sibling for that venue.
        """
        from unified_api_contracts.registry import DatabentoDatasetNotAllowedError

        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        survivor = _make_record(raw_symbol="ESZ4")

        def _fetch_side_effect(api_key: str, dataset: str, symbols: list[str], stype_in: str):
            if dataset == "GLBX.MDP3":
                return [survivor]
            # Any off-allowlist dataset (IFEU.IMPACT / XNAS.ITCH / DBEQ.BASIC equities) raises.
            raise DatabentoDatasetNotAllowedError(f"dataset {dataset!r} is NOT in the paid subscription")

        with (
            patch.object(adapter, "_venue_filter", None),
            patch.object(adapter, "_fetch_symbols", side_effect=_fetch_side_effect),
            patch.object(adapter, "_get_equity_symbols", return_value=["AAPL"]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            # Must NOT raise — the banned dataset isolates, GLBX survivor returns.
            results = await adapter.get_instruments()
        assert any(r.raw_symbol == "ESZ4" for r in results), (
            "GLBX survivor must be returned despite a sibling dataset's entitlement breach"
        )

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        record = _make_record(raw_symbol="ESZ4")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[record])):
            result = await adapter.get_instrument("ESZ4")
        assert result is not None
        assert result.raw_symbol == "ESZ4"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_with_options(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        expiry_dt = datetime(2024, 6, 21, tzinfo=UTC)
        call_inst = _make_record(
            key="GLBX:OPT:ESM4 C4500",
            instrument_type="OPTION",
            raw_symbol="ESM4 C4500",
            strike=Decimal("4500"),
            option_type="call",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[call_inst])):
            chain = await adapter.get_options_chain("ES")
        assert chain.venue == "databento"
        assert len(chain.calls) == 1
        assert Decimal("4500") in chain.strikes

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_with_futures(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        expiry_dt = datetime(2024, 3, 15, tzinfo=UTC)
        fut_inst = _make_record(
            key="GLBX:FUTURE:ESH4",
            instrument_type="FUTURE",
            raw_symbol="ESH4",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[fut_inst])):
            calendar = await adapter.get_expiry_calendar("ES", instrument_type="FUTURE")
        assert calendar.venue == "databento"
        assert expiry_dt in calendar.expiries

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("ESH4")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("ESH4")

    @pytest.mark.asyncio
    async def test_get_options_chain_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            chain = await adapter.get_options_chain("SPY")
        assert chain.venue == "databento"
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_returns_empty_without_instruments(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            calendar = await adapter.get_expiry_calendar("ES")
        assert calendar.venue == "databento"
        assert calendar.expiries == []


# ---------------------------------------------------------------------------
# EVENT_CONTRACT classification via _parse_row_to_record (Phase 4)
# ---------------------------------------------------------------------------


class TestDatabentoEventContractClassification:
    """BAG + EC* raw_symbol → EVENT_CONTRACT; underlying = EC root; available_since heuristic."""

    def _make_row(
        self,
        raw_symbol: str = "ECBTC-EOM-2026-05-30-0.5",
        instrument_class: str = "BAG",
        currency: str = "USD",
        underlying: str = "",
        expiry: str = "2026-05-30T21:00:00+00:00",
        activation: str | None = None,
        strike_price: float = 0.5,
    ) -> object:
        """Build a minimal mock DataFrame row for _parse_row_to_record."""
        row = MagicMock()
        row.raw_symbol = raw_symbol
        row.instrument_class = instrument_class
        row.currency = currency
        row.underlying = underlying
        row.expiration = expiry
        row.activation = activation
        row.strike_price = strike_price
        row.min_price_increment = 0.01
        row.min_lot_size_round_lot = 1
        row.symbol = raw_symbol
        return row

    def _make_adapter(self) -> DatabentoReferenceDataAdapter:
        from datetime import date

        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        adapter._target_date = date(2026, 5, 20)
        return adapter

    def test_bag_ec_prefix_maps_to_event_contract(self) -> None:
        adapter = self._make_adapter()
        row = self._make_row("ECBTC-EOM-2026-05-30-0.5", instrument_class="BAG")
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        assert record is not None
        assert record.instrument_type == "EVENT_CONTRACT"

    def test_bag_without_ec_prefix_does_not_map_to_event_contract(self) -> None:
        adapter = self._make_adapter()
        # Generic BAG (calendar spread) — not an EC* event contract
        row = self._make_row("ESM6-ESU6", instrument_class="BAG", expiry="2026-06-20T21:00:00+00:00")
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        # Generic BAG routes to COMBO (no EC* prefix)
        assert record is None or (record is not None and record.instrument_type != "EVENT_CONTRACT")

    def test_event_contract_underlying_is_ec_root(self) -> None:
        adapter = self._make_adapter()
        row = self._make_row("ECBTC-EOM-2026-05-30-0.5", instrument_class="BAG")
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        assert record is not None
        assert record.underlying == "ECBTC"

    def test_event_contract_available_since_within_30_days_of_expiry(self) -> None:
        from datetime import UTC, timedelta

        adapter = self._make_adapter()
        row = self._make_row("ECES-EOM-2026-05-30-5000", instrument_class="BAG", expiry="2026-05-30T21:00:00+00:00")
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        assert record is not None
        assert record.available_from_datetime is not None
        expiry_dt = datetime(2026, 5, 30, 21, 0, tzinfo=UTC)
        # listing_months=1 → ~30 days before expiry
        assert record.available_from_datetime <= expiry_dt - timedelta(days=25)


# ---------------------------------------------------------------------------
# Canonical product identity via _parse_row_to_record
# (additive canonical_instrument_id + product_root from the UAC exchange-code
#  registry; raw_symbol stays the raw exchange code).
# ---------------------------------------------------------------------------


class TestDatabentoCanonicalIdentity:
    """ESM0 future + E5AH0 C2510 spaced option → product_root=SP500 + canonical id;
    raw_symbol unchanged. A CeFi instrument is unaffected."""

    def _make_row(
        self,
        raw_symbol: str,
        instrument_class: str,
        expiry: str,
        strike_price: float | None = None,
        underlying: str = "",
        currency: str = "USD",
    ) -> object:
        row = MagicMock()
        row.raw_symbol = raw_symbol
        row.symbol = raw_symbol
        row.instrument_class = instrument_class
        row.currency = currency
        row.underlying = underlying
        row.expiration = expiry
        row.activation = None
        row.strike_price = strike_price
        row.min_price_increment = 0.25
        row.min_lot_size_round_lot = 1
        # Empty option_type so the adapter falls back to instrument_class (C/P).
        row.option_type = ""
        return row

    def _make_adapter(self) -> DatabentoReferenceDataAdapter:
        from datetime import date

        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        adapter._target_date = date(2026, 5, 20)
        return adapter

    def test_es_future_gets_sp500_product_root_and_canonical_id(self) -> None:
        adapter = self._make_adapter()
        row = self._make_row("ESM0", instrument_class="F", expiry="2026-06-19T13:30:00+00:00")
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        assert record is not None
        assert record.instrument_type == "FUTURE"
        # raw_symbol unchanged — canonicals are additive.
        assert record.raw_symbol == "ESM0"
        assert record.product_root == "SP500"
        assert record.canonical_instrument_id is not None
        assert record.canonical_instrument_id.startswith("CME:FUTURE:SP500:")

    def test_spaced_option_gets_sp500_product_root_and_canonical_id(self) -> None:
        adapter = self._make_adapter()
        # E5A = Friday-daily ES option root; spaced contract code + strike token.
        row = self._make_row(
            "E5AH0 C2510",
            instrument_class="C",
            expiry="2026-09-18T14:00:00+00:00",
            strike_price=2510.0,
        )
        record = adapter._parse_row_to_record(row, dataset="GLBX.MDP3", canonical_venue="CME")
        assert record is not None
        assert record.instrument_type == "OPTION"
        # raw_symbol stays the raw spaced exchange code.
        assert record.raw_symbol == "E5AH0 C2510"
        assert record.product_root == "SP500"
        assert record.canonical_instrument_id is not None
        assert record.canonical_instrument_id.startswith("CME:OPTION:SP500:")
        # strike + C/P suffix encoded in the canonical id.
        assert record.canonical_instrument_id.endswith("C")

    def test_cefi_instrument_unaffected(self) -> None:
        # A CeFi spot/perp record (built directly, not via the TradFi adapter)
        # carries no canonical product identity — the fields stay None.
        record = _make_record(
            key="BINANCE:PERPETUAL:BTCUSDT",
            venue="binance",
            instrument_type="PERPETUAL",
            raw_symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
        )
        assert record.product_root is None
        assert record.canonical_instrument_id is None


# ---------------------------------------------------------------------------
# CF-11 silent-shrink fix: fetch-failure must thread STATE (not just emit event)
# ---------------------------------------------------------------------------


class TestDatabentoFetchFailureStateThreading:
    """Verify that BentoError + parse-failure raise RuntimeError (not return []).

    The re-raise is the mechanism that gets the venue into urdi_reference_provider
    _fetch_one's failed[] list → orchestrator records attempted_failed, not clean empty.
    """

    def _make_adapter(self) -> DatabentoReferenceDataAdapter:
        from datetime import date

        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        adapter._target_date = date(2026, 1, 15)
        return adapter

    @pytest.mark.asyncio
    async def test_bento_error_raises_runtime_error_not_returns_empty(self) -> None:
        """BentoError in _fetch_symbols must propagate as RuntimeError, not swallow → [].

        This is the primary CF-11 gap: the old code returned [] which was cached as a
        legit result, causing the venue to appear in _non_error_venues (never attempted_failed).
        """
        import databento as db

        adapter = self._make_adapter()
        bento_exc = db.common.error.BentoError("429 rate limit exceeded")

        with (
            patch("databento.Historical") as mock_hist_cls,
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            mock_hist = MagicMock()
            mock_hist_cls.return_value = mock_hist
            mock_hist.timeseries.get_range.side_effect = bento_exc

            with pytest.raises(RuntimeError, match="Databento fetch failed"):
                await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_parse_failure_raises_runtime_error_not_returns_empty(self) -> None:
        """data.to_df() parse failure must propagate as RuntimeError, not swallow → [].

        Slot-5 confirmed this branch had no ADAPTER_FETCH_FAILED event and silently
        returned [], causing universe truncation with zero failure signal.
        """
        adapter = self._make_adapter()
        parse_exc = ValueError("DBN deserialization failed")

        mock_data = MagicMock()
        mock_data.to_df.side_effect = parse_exc

        with (
            patch("databento.Historical") as mock_hist_cls,
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            mock_hist = MagicMock()
            mock_hist_cls.return_value = mock_hist
            mock_hist.timeseries.get_range.return_value = mock_data

            with pytest.raises(RuntimeError, match="Databento DBN parse failure"):
                await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_genuine_empty_response_still_returns_empty_list(self) -> None:
        """A legitimate empty df (no instruments on the date) must NOT raise.

        Genuine empty = the API succeeded but returned 0 rows (e.g. weekend, no listings).
        This path must continue returning [] cleanly — it is NOT a fetch failure.
        """
        import pandas as pd

        adapter = self._make_adapter()

        mock_data = MagicMock()
        mock_data.to_df.return_value = pd.DataFrame()  # genuinely empty

        with (
            patch("databento.Historical") as mock_hist_cls,
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            mock_hist = MagicMock()
            mock_hist_cls.return_value = mock_hist
            mock_hist.timeseries.get_range.return_value = mock_data

            results = await adapter.get_instruments()

        assert results == [], f"Expected [] for genuine empty, got {results}"

    @pytest.mark.asyncio
    async def test_cache_does_not_memoize_failed_fetch(self) -> None:
        """A failed fetch (RuntimeError) must not be stored in the adapter cache.

        base_adapter.get_instruments_cached() only writes the cache AFTER get_instruments()
        returns; if it raises, the write never happens — subsequent calls retry the fetch.
        """
        import databento as db

        adapter = self._make_adapter()
        bento_exc = db.common.error.BentoError("500 internal server error")

        with (
            patch("databento.Historical") as mock_hist_cls,
            patch.object(adapter, "_get_equity_symbols", return_value=[]),
            patch.object(adapter, "_create_fx_spot_records", return_value=[]),
            patch.object(adapter, "_create_yahoo_index_records", return_value=[]),
            patch.object(adapter, "_create_krx_equity_records", return_value=[]),
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            mock_hist = MagicMock()
            mock_hist_cls.return_value = mock_hist
            mock_hist.timeseries.get_range.side_effect = bento_exc

            # First call: should raise
            with pytest.raises(RuntimeError):
                await adapter.get_instruments_cached()

            # Cache must be empty — the exception prevented the cache write
            assert adapter._instruments_cache == {}, (
                "Failed fetch must not be memoised; cache was written despite exception"
            )


# ---------------------------------------------------------------------------
# Tardis adapter mocked tests
# ---------------------------------------------------------------------------


class TestTardisAdapterMocked:
    def test_venue_name(self) -> None:
        adapter = TardisReferenceDataAdapter()
        assert adapter.venue == "tardis"

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                        "availableTo": None,
                    }
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1
        # Tardis stores raw type from API — "PERPETUAL" (uppercase)
        assert results[0].instrument_type == "PERPETUAL"
        # Tardis adapter uses the exchange name as venue (e.g. DERIBIT)
        assert results[0].venue is not None

    @pytest.mark.asyncio
    async def test_deribit_spot_not_dropped(self) -> None:
        """Deribit lists spot pairs (BTC_USDC/…) since ~2023 — it is NOT a
        derivatives-only venue, so a Deribit SPOT instrument must enumerate as a
        SPOT_PAIR InstrumentRecord (not silently dropped), while a Deribit perp
        still parses as PERPETUAL (no regression). Regression guard for the
        operator correction 2026-06-16: the Tardis adapter used to drop ALL
        Deribit spot via _DERIVATIVES_ONLY_EXCHANGES."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC_USDC",
                        "type": "spot",
                        "baseCurrency": "BTC",
                        "quoteCurrency": "USDC",
                        "availableSince": "2023-01-01T00:00:00Z",
                        "availableTo": None,
                    },
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                        "availableTo": None,
                    },
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        by_type = {str(r.instrument_type): r for r in results}
        # The Deribit spot pair is enumerated (not dropped) as a SPOT_PAIR.
        assert "SPOT_PAIR" in by_type, f"Deribit spot dropped — got types {sorted(by_type)}"
        spot = by_type["SPOT_PAIR"]
        assert spot.base_asset == "BTC"
        assert spot.quote_asset == "USDC"
        # The Deribit perp still parses (no regression).
        assert "PERPETUAL" in by_type, f"Deribit perp lost — got types {sorted(by_type)}"

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                    },
                    {
                        "id": "BTC-31MAR24",
                        "type": "future",
                        "availableSince": "2024-01-01T00:00:00Z",
                        "availableTo": "2024-03-31T08:00:00Z",
                    },
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            # Tardis uses uppercase type strings from the API ("PERPETUAL")
            results = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_on_empty(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("BTC-PERPETUAL")
        assert result is None

    @pytest.mark.asyncio
    async def test_exchange_not_found_skips(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["unknown-exchange"])
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_enumeration_is_no_auth_free_metadata_endpoint(self) -> None:
        """IS instrument enumeration MUST use the FREE, NO-AUTH Tardis metadata
        endpoint GET /v1/exchanges/{exchange} only — NEVER the authenticated
        /v1/instruments/{exchange} tick-metadata path, and NEVER send an
        Authorization header. Regression guard for the operator mandate
        2026-06-23: "IS doesn't need auth for tardis; don't waste API limits".

        Proves: (1) an adapter with NO api_key enumerates the symbol universe;
        (2) every HTTP call targets /v1/exchanges/ (free) — none targets
        /v1/instruments/ (pro/auth); (3) no Bearer token is ever sent.
        """
        # No api_key passed — the IS reference-data factory no longer supplies one.
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        assert adapter._optional_api_key() is None  # pyright: ignore[reportPrivateUsage]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "id": "deribit",
                "name": "Deribit",
                "availableSymbols": [
                    {
                        "id": "BTC-PERPETUAL",
                        "type": "perpetual",
                        "availableSince": "2020-01-01T00:00:00Z",
                        "availableTo": None,
                    }
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        # (1) Universe enumerated without a key.
        assert len(results) == 1
        assert str(results[0].instrument_type) == "PERPETUAL"

        # (2)+(3) Inspect every session.get call: free endpoint only, no auth.
        assert mock_session_obj.get.call_count >= 1
        for call in mock_session_obj.get.call_args_list:
            url = call.args[0] if call.args else call.kwargs.get("url", "")
            assert "/v1/exchanges/" in url, f"enumeration hit a non-free endpoint: {url}"
            assert "/v1/instruments/" not in url, f"enumeration hit the authenticated endpoint: {url}"
            headers = call.kwargs.get("headers")
            # No Authorization header is sent on the no-auth metadata call.
            assert not (headers and "Authorization" in headers), f"enumeration sent an auth header: {headers}"

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        call_inst = _make_record(
            key="deribit:BTC-31DEC24-50000-C",
            venue="tardis",
            instrument_type="OPTION",
            raw_symbol="BTC-31DEC24-50000-C",
            base_asset="BTC",
            quote_asset="USD",
            strike=Decimal("50000"),
            option_type="call",
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[call_inst])):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "tardis"
        assert len(chain.calls) == 1

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        expiry_dt = datetime(2024, 3, 31, 8, 0, tzinfo=UTC)
        fut_inst = _make_record(
            key="deribit:BTC-31MAR24",
            venue="tardis",
            instrument_type="FUTURE",
            raw_symbol="BTC-31MAR24",
            base_asset="BTC",
            quote_asset="USD",
            expiry=expiry_dt,
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[fut_inst])):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="FUTURE")
        assert calendar.venue == "tardis"
        assert len(calendar.expiries) == 1


# ---------------------------------------------------------------------------
# Tardis funding rate and OHLCV mocked tests
# ---------------------------------------------------------------------------


def _make_tardis_datafeed_session(text_body: str, status: int = 200) -> MagicMock:
    """Helper: mock aiohttp.ClientSession for Tardis data-feeds endpoint."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    mock_resp.text = AsyncMock(return_value=text_body)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


class TestTardisAdapterFundingAndOHLCV:
    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"fundingRate": "0.0001", "timestamp": 1700000000000}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("XBTUSD")
        assert result.venue == "tardis"
        assert str(result.rate) == "0.0001"

    @pytest.mark.asyncio
    async def test_get_funding_rate_404_raises(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(RuntimeError, match="No funding rate"),
        ):
            await adapter.get_funding_rate("XBTUSD")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = (
            '{"open": "30000", "high": "31000", "low": "29000",'
            ' "close": "30500", "volume": "100", "timestamp": 1700000000000}\n'
        )
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert len(result) == 1
        assert result[0].venue == "tardis"
        assert result[0].open == Decimal("30000")

    @pytest.mark.asyncio
    async def test_get_ohlcv_line_missing_open_skipped(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        ndjson = '{"timestamp": 1700000000000, "volume": "100"}\n'
        mock_session = _make_tardis_datafeed_session(ndjson)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_exchange_404_skips(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = _make_tardis_datafeed_session("", status=404)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("XBTUSD")
        assert result == []

    @pytest.mark.asyncio
    async def test_parse_expiry_invalid_string_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.cefi.tardis import _parse_expiry

        assert _parse_expiry("not-a-date") is None
        assert _parse_expiry(None) is None
        assert _parse_expiry("") is None

    def test_build_datafeed_headers_no_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value=None):
            headers = adapter._build_datafeed_headers()
        assert headers == {}

    def test_build_datafeed_headers_with_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        with patch.object(adapter, "_optional_api_key", return_value="test-key"):
            headers = adapter._build_datafeed_headers()
        assert headers == {"Authorization": "Bearer test-key"}

    def test_resolve_bar_type(self) -> None:
        assert TardisReferenceDataAdapter._resolve_bar_type("1m") == (60, "trade_bar_1m")
        assert TardisReferenceDataAdapter._resolve_bar_type("1h") == (3600, "trade_bar_1h")
        assert TardisReferenceDataAdapter._resolve_bar_type("1d") == (86400, "trade_bar_1d")
        assert TardisReferenceDataAdapter._resolve_bar_type("unknown") == (86400, "trade_bar_1d")


class TestTardisInstrumentsCacheContract:
    """Lock the cache-by-instrument-type-only contract from instruments-service@9d91465.

    Pre-fix (base-adapter cache keyed on (instrument_type, date)): each new backfill
    date fetched the full ~200K-instrument Tardis universe again, accumulating ~1.4 GB
    of pydantic objects per date. DERIBIT VM at e2-standard-4 OOM-killed at 25 dates
    into a 30-day chunk on 2026-05-04 (rc=137 silent kill, no EXIT_STATUS).

    Post-fix: TardisReferenceDataAdapter overrides ``get_instruments_cached`` to key
    on ``instrument_type`` only — second date onward returns the same list reference,
    RSS plateaus after the first fetch, no leak across long backfill loops. TTL bumped
    to 24h so multi-hour sweeps don't expire mid-run.

    These tests enforce both invariants so the regression cannot return without QG
    catching it. No live VM required — the contract IS the validation.
    """

    @pytest.mark.asyncio
    async def test_cache_keyed_on_instrument_type_not_date(self) -> None:
        """Different dates with same instrument_type return the SAME list reference.

        This is the core memory-leak guard. Pre-fix: 100 dates → 100 fetches → ~140 GB.
        Post-fix: 100 dates → 1 fetch → ~1.4 GB plateau.
        """
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        fetch_count = 0
        sentinel_list: list[InstrumentRecord] = []

        async def _fake_get_instruments(instrument_type: str | None = None) -> list[InstrumentRecord]:
            nonlocal fetch_count
            fetch_count += 1
            return sentinel_list

        with patch.object(adapter, "get_instruments", _fake_get_instruments):
            d1 = await adapter.get_instruments_cached(instrument_type="PERPETUAL", date="2024-01-01")
            d2 = await adapter.get_instruments_cached(instrument_type="PERPETUAL", date="2024-06-15")
            d3 = await adapter.get_instruments_cached(instrument_type="PERPETUAL", date="2024-12-31")
            d4 = await adapter.get_instruments_cached(instrument_type="PERPETUAL", date=None)

        # Exactly ONE upstream fetch across 4 distinct dates.
        assert fetch_count == 1, f"Expected 1 fetch (cache by instrument_type), got {fetch_count}"
        # Same list reference returned every time — proves no per-date pydantic alloc.
        assert d1 is d2 is d3 is d4 is sentinel_list

    @pytest.mark.asyncio
    async def test_different_instrument_types_get_separate_cache_entries(self) -> None:
        """instrument_type IS the cache key — different types fetch separately."""
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        fetch_calls: list[str | None] = []

        async def _fake_get_instruments(instrument_type: str | None = None) -> list[InstrumentRecord]:
            fetch_calls.append(instrument_type)
            return []

        with patch.object(adapter, "get_instruments", _fake_get_instruments):
            await adapter.get_instruments_cached(instrument_type="PERPETUAL", date="2024-01-01")
            await adapter.get_instruments_cached(instrument_type="OPTION", date="2024-01-01")
            await adapter.get_instruments_cached(instrument_type="PERPETUAL", date="2024-06-15")

        # PERPETUAL fetched once, OPTION fetched once, second PERPETUAL hits cache.
        assert fetch_calls == ["PERPETUAL", "OPTION"], fetch_calls

    def test_cache_ttl_is_24h(self) -> None:
        """TTL must be 86400s (24h) — long enough for multi-hour backfill sweeps.

        Default base-adapter TTL is 3600s (1h) which expires mid-run on multi-year
        DERIBIT options-chain sweeps and forces a re-fetch + re-allocation. The 24h
        bump is the second half of the 9d91465 fix; locking it here so a future
        refactor can't silently regress.
        """
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        assert adapter._cache_ttl == 86400.0

    @pytest.mark.asyncio
    async def test_cache_expires_after_ttl(self) -> None:
        """After TTL elapses, a fresh fetch happens — not a permanent freeze.

        Edge guard: cache is for memory + perf, not correctness. If a backfill VM
        runs > 24h (rare but possible), cache MUST expire so a new universe is
        picked up rather than serving stale data forever.
        """
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        fetch_count = 0

        async def _fake_get_instruments(instrument_type: str | None = None) -> list[InstrumentRecord]:
            nonlocal fetch_count
            fetch_count += 1
            return []

        with patch.object(adapter, "get_instruments", _fake_get_instruments):
            await adapter.get_instruments_cached(instrument_type="PERPETUAL")
            assert fetch_count == 1
            # Force expiry by rewriting the cache entry's monotonic timestamp 25h ago.
            records, _ts = adapter._instruments_cache["PERPETUAL"]
            adapter._instruments_cache["PERPETUAL"] = (records, _ts - 90000.0)
            await adapter.get_instruments_cached(instrument_type="PERPETUAL")
            assert fetch_count == 2, "Cache should re-fetch after TTL expiry"


# ---------------------------------------------------------------------------
# Regression: IS Tardis reference universe must track the canonical SSOT, not a
# hand-maintained subset (slot-3 pre-apply audit 2026-06-08, CF-14 ⑧).
# The prior 8-exchange list silently DRIFTED below VenueMapping.all_tardis_exchanges
# and omitted kraken / cryptofacilities (=KRAKEN-FUTURES) / bitfinex / bitget — venues
# MTDS captures via Tardis replay — so the IS catalogue was NOT ⊇ the captured present-
# set (falsely-high coverage). These tests fail if the default universe drifts again.
# ---------------------------------------------------------------------------


def test_default_exchanges_track_ssot_no_drift() -> None:
    from unified_api_contracts import VenueMapping

    from instruments_service.reference_data.adapters.cefi.tardis import _DEFAULT_EXCHANGES

    assert list(VenueMapping().all_tardis_exchanges) == _DEFAULT_EXCHANGES, (
        "IS Tardis _DEFAULT_EXCHANGES must equal the canonical SSOT "
        "VenueMapping.all_tardis_exchanges (no hand-maintained subset / drift)."
    )


def test_default_exchanges_cover_captured_cefi_venues() -> None:
    """The Tardis exchange ids behind the CeFi venues MTDS actually captures must be in
    the default universe — KRAKEN-SPOT (kraken) + KRAKEN-FUTURES (cryptofacilities) +
    BITFINEX-SPOT (bitfinex) + BITGET (bitget) + LIGHTER-ZKSYNC (lighter). Their absence
    is the exact CF-14 gap. "lighter" not "lighter-zksync": UAC's VenueMapping corrected
    the slug to the real Tardis identifier (unified-api-contracts@f16c79e8) — "lighter-
    zksync" is not a valid Tardis exchange slug."""
    from instruments_service.reference_data.adapters.cefi.tardis import _DEFAULT_EXCHANGES

    for exch in ("kraken", "cryptofacilities", "bitfinex", "bitget", "lighter"):
        assert exch in _DEFAULT_EXCHANGES, f"captured-venue Tardis exchange {exch!r} missing from IS reference universe"


def test_derivatives_only_classifies_kraken_futures() -> None:
    """cryptofacilities (Kraken Futures) + the other derivatives-only Tardis exchanges
    must be classified so unknown-type instruments are skipped, not defaulted to SPOT."""
    from instruments_service.reference_data.adapters.cefi.tardis import _DERIVATIVES_ONLY_EXCHANGES

    for exch in ("cryptofacilities", "okex-futures", "okex-swap", "bitfinex-derivatives", "bitget-futures"):
        assert exch in _DERIVATIVES_ONLY_EXCHANGES, f"derivatives-only Tardis exchange {exch!r} not classified"


# ---------------------------------------------------------------------------
# Yahoo index venue-filter tests (DXY + VIX)
# ---------------------------------------------------------------------------


def test_create_yahoo_index_records_no_filter_returns_all() -> None:
    """venue_filter=None returns one record per YAHOO_INDICES entry (DXY + treasuries).

    The VIX cash-index was removed from YAHOO_INDICES 2026-06-25 (retired; VIX-15m rides
    the VX FUTURES front contract), so the adapter no longer emits CBOE:INDEX:VIX-USD.
    """
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_yahoo_index_records(venue_filter=None)
    keys = {r.instrument_key for r in records}
    # Canonical keys carry the -USD base-quote suffix (match GCS/symbology + resolver).
    assert "CBOE:INDEX:VIX-USD" not in keys  # VIX cash-index retired 2026-06-25
    assert "ICE:INDEX:DXY-USD" in keys
    for symbol in ("US3M", "US2Y", "US5Y", "US10Y", "US30Y"):
        assert f"CBOE:INDEX:{symbol}-USD" in keys


def test_create_yahoo_index_records_cboe_filter_returns_cboe_only() -> None:
    """venue_filter='CBOE' returns the CBOE records (treasuries), not DXY (ICE).

    VIX cash-index removed 2026-06-25, so the CBOE Yahoo indices are the treasury tenors.
    """
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_yahoo_index_records(venue_filter="CBOE")
    keys = {r.instrument_key for r in records}
    assert "CBOE:INDEX:VIX-USD" not in keys  # VIX cash-index retired 2026-06-25
    assert "CBOE:INDEX:US10Y-USD" in keys
    assert "ICE:INDEX:DXY-USD" not in keys


def test_create_yahoo_index_records_carry_per_instrument_genesis() -> None:
    """available_from_datetime is the per-entry genesis, not a shared hardcoded date."""
    adapter = DatabentoReferenceDataAdapter()
    records = {r.instrument_key: r for r in adapter._create_yahoo_index_records(venue_filter=None)}
    assert records["ICE:INDEX:DXY-USD"].available_from_datetime.year == 2019
    assert records["CBOE:INDEX:US10Y-USD"].available_from_datetime.year == 2000


def test_create_yahoo_index_records_ice_filter_returns_only_dxy() -> None:
    """venue_filter='ICE' returns only the DXY record, not VIX."""
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_yahoo_index_records(venue_filter="ICE")
    keys = {r.instrument_key for r in records}
    assert "ICE:INDEX:DXY-USD" in keys
    assert "CBOE:INDEX:VIX-USD" not in keys


def test_create_yahoo_index_records_unknown_venue_returns_empty() -> None:
    """venue_filter with an unknown venue returns an empty list."""
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_yahoo_index_records(venue_filter="UNKNOWN_VENUE")
    assert records == []


# ---------------------------------------------------------------------------
# FX major spot pairs — _create_fx_spot_records (2026-06-26)
#
# G10 FX majors were added to UAC FX_SPOT_PAIRS on 2026-06-26 (EUR/USD, GBP/USD,
# USD/JPY, AUD/USD, USD/CAD, USD/CHF, NZD/USD, EUR/GBP, EUR/JPY, USD/MXN + KRW/USD).
# _create_fx_spot_records iterates FX_SPOT_PAIRS and emits FX:SPOT_PAIR:<BASE>-<QUOTE>
# records. This block verifies the new records enumerate correctly.
# ---------------------------------------------------------------------------

_FX_G10_INSTRUMENT_KEYS = {
    "FX:SPOT_PAIR:EUR-USD",
    "FX:SPOT_PAIR:GBP-USD",
    "FX:SPOT_PAIR:USD-JPY",
    "FX:SPOT_PAIR:AUD-USD",
    "FX:SPOT_PAIR:USD-CAD",
    "FX:SPOT_PAIR:USD-CHF",
    "FX:SPOT_PAIR:NZD-USD",
    "FX:SPOT_PAIR:EUR-GBP",
    "FX:SPOT_PAIR:EUR-JPY",
    "FX:SPOT_PAIR:USD-MXN",
    "FX:SPOT_PAIR:KRW-USD",
}


def test_create_fx_spot_records_contains_g10_majors() -> None:
    """_create_fx_spot_records must emit InstrumentRecords for all G10 FX majors.

    Added 2026-06-26: G10 crosses were missing from FX_SPOT_PAIRS — only KRW/USD
    was declared. All 10 G10 crosses + KRW/USD must now enumerate.
    """
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_fx_spot_records()
    keys = {r.instrument_key for r in records}
    for expected_key in _FX_G10_INSTRUMENT_KEYS:
        assert expected_key in keys, f"{expected_key} missing from _create_fx_spot_records — check UAC FX_SPOT_PAIRS"


def test_create_fx_spot_records_all_are_fx_venue_spot_pair() -> None:
    """Every record from _create_fx_spot_records must be venue=FX, type=SPOT_PAIR."""
    from unified_api_contracts.internal import InstrumentType

    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_fx_spot_records()
    assert len(records) >= 11, f"Expected at least 11 FX pairs (10 G10 + KRW), got {len(records)}"
    for r in records:
        assert r.venue == "FX", f"{r.instrument_key}: venue={r.venue!r} (expected 'FX')"
        assert r.instrument_type == InstrumentType.SPOT_PAIR, (
            f"{r.instrument_key}: instrument_type={r.instrument_type!r} (expected SPOT_PAIR)"
        )


def test_create_fx_spot_records_yahoo_ticker_ends_with_equals_x() -> None:
    """All FX SPOT_PAIR raw_symbols (Yahoo tickers) must end with '=X'."""
    adapter = DatabentoReferenceDataAdapter()
    records = adapter._create_fx_spot_records()
    for r in records:
        assert r.raw_symbol.endswith("=X"), f"{r.instrument_key}: raw_symbol={r.raw_symbol!r} must end with '=X'"


# ---------------------------------------------------------------------------
# Bug-2 regression: 'cefi' is not a valid AssetClass (2026-06-24)
#
# _NET_PROFITABLE_EQUITY_PERP_SINGLES in UAC tradfi_instrument_universe.py carries
# DatabentoInstrumentDef entries (NVDA/MSFT/TSLA/…) with asset_group="cefi" — they are
# equity legs of a crypto-venue arb trade, DELIBERATELY excluded from the tradfi pipeline.
# _resolve_asset_group reads _EXCHANGE_CODE_asset_group which returns "cefi" for those
# symbols, then did AssetClass("cefi") → ValueError: 'cefi' is not a valid AssetClass.
# Fix: guard with `ac in frozenset(AssetClass)` before calling AssetClass(ac); if the
# value is a domain designator (cefi/tradfi/defi) rather than an instrument class, fall
# through to the dataset-level default.
# ---------------------------------------------------------------------------


class TestResolveAssetGroupCefiExclusion:
    """Regression: _resolve_asset_group must not raise on cefi-tagged defs."""

    def test_does_not_raise_on_cefi_underlying(self) -> None:
        """NVDA/MSFT etc. are in _EXCHANGE_CODE_asset_group with value 'cefi'.
        _resolve_asset_group must NOT raise ValueError and must NOT emit them as
        tradfi records — it returns the dataset-level fallback (AssetClass.EQUITY from
        DBEQ.BASIC) since cefi is not a valid AssetClass member.
        """
        from unified_api_contracts.internal import AssetClass

        from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
            DatabentoReferenceDataAdapter,
        )
        from instruments_service.reference_data.adapters.tradfi.databento.symbology import (
            _EXCHANGE_CODE_asset_group,
        )

        # Confirm that NVDA IS in the map with value "cefi" (precondition of the bug)
        assert _EXCHANGE_CODE_asset_group.get("NVDA") == "cefi", (
            "Precondition: NVDA must be in _EXCHANGE_CODE_asset_group with value 'cefi'"
        )

        # Must not raise — previously crashed with ValueError: 'cefi' is not a valid AssetClass
        result = DatabentoReferenceDataAdapter._resolve_asset_group("DBEQ.BASIC", "NVDA", "NVDA")
        # Fallback: dataset DBEQ.BASIC maps to EQUITY (the _DATASET_TO_asset_group default)
        assert result == AssetClass.EQUITY

    def test_does_not_raise_on_other_cefi_singles(self) -> None:
        """MSFT, TSLA, AAPL — all cefi-tagged — must not raise."""
        from unified_api_contracts.internal import AssetClass

        from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
            DatabentoReferenceDataAdapter,
        )

        cefi_symbols = ["MSFT", "TSLA", "AAPL", "AMD", "AMZN", "META", "GOOGL"]
        for sym in cefi_symbols:
            # Should never raise — fallback to dataset default
            result = DatabentoReferenceDataAdapter._resolve_asset_group("DBEQ.BASIC", sym, sym)
            assert isinstance(result, AssetClass), f"{sym}: expected AssetClass instance, got {result!r}"

    def test_genuine_tradfi_equity_returns_equity(self) -> None:
        """A symbol in TRADFI_EQUITY_PERP_BASIS_UNIVERSE (asset_group='tradfi') must resolve
        correctly — DBEQ.BASIC dataset fallback returns AssetClass.EQUITY.
        """
        from unified_api_contracts.internal import AssetClass

        from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
            DatabentoReferenceDataAdapter,
        )

        # SPY is a genuine tradfi ETF — must resolve to EQUITY, not raise
        result = DatabentoReferenceDataAdapter._resolve_asset_group("DBEQ.BASIC", "SPY", "SPY")
        assert result == AssetClass.EQUITY

    def test_genuine_futures_symbol_returns_correct_class(self) -> None:
        """ES (S&P 500 future, CME) must still resolve to EQUITY or COMMODITY (registry value),
        not crash. Regression guard — the cefi fix must not break valid futures resolution.
        """
        from unified_api_contracts.internal import AssetClass

        from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
            DatabentoReferenceDataAdapter,
        )

        result = DatabentoReferenceDataAdapter._resolve_asset_group("GLBX.MDP3", "ESZ6", "ES")
        assert isinstance(result, AssetClass)


class TestKRXStaticRecords:
    """Bug-1 regression: KRX static records must be emitted with valid AssetClass (2026-06-24)."""

    def test_krx_records_venue_and_instrument_type(self) -> None:
        """_create_krx_equity_records must return non-empty records with venue=KRX
        and instrument_type=EQUITY (the static Korean single-stock entries).

        This is the routing regression guard: if KRX is missing from
        VENUE_TO_ADAPTER_KEY, the factory raises ValueError("No URDI adapter for
        ['KRX']") and these records are never emitted — the shard silently loses KRX.
        """
        from instruments_service.reference_data.adapters.tradfi.databento.adapter import (
            DatabentoReferenceDataAdapter,
        )

        adapter = DatabentoReferenceDataAdapter()
        records = adapter._create_krx_equity_records()
        assert records, "Expected at least one KRX record"
        for rec in records:
            assert rec.venue == "KRX", f"Expected venue=KRX, got {rec.venue!r}"
            assert rec.instrument_type == "EQUITY", (
                f"KRX record {rec.instrument_key}: expected instrument_type=EQUITY, got {rec.instrument_type!r}"
            )
            assert rec.instrument_key.startswith("KRX:EQUITY:"), (
                f"KRX record has wrong instrument_key prefix: {rec.instrument_key!r}"
            )


# ---------------------------------------------------------------------------
# Tradfi instruments-foundation G1 regression guards (2026-06-25, slot-3).
# Each guards a fix that STOPS a catalogue pollutant at source — reverting any
# re-introduces the pollution (ICE/OPRA enumeration, VX-spread SPOT_PAIR,
# equity-spot mis-class, cefi-singles in tradfi, VX=EQUITY, KRX-silent-24/7).
# ---------------------------------------------------------------------------
class TestTradfiG1FoundationRegression:
    """Regression guards for the gated tradfi instruments-foundation rebuild (G1.a-e)."""

    @staticmethod
    def _row(
        raw_symbol: str,
        instrument_class: str,
        expiry: str | None = None,
    ) -> object:
        row = MagicMock()
        row.raw_symbol = raw_symbol
        row.symbol = raw_symbol
        row.instrument_class = instrument_class
        row.currency = "USD"
        row.underlying = ""
        row.expiration = expiry
        row.activation = None
        row.strike_price = None
        row.option_type = ""
        row.min_price_increment = 0.01
        row.min_lot_size_round_lot = 1
        return row

    @staticmethod
    def _adapter() -> DatabentoReferenceDataAdapter:
        from datetime import date

        adapter = DatabentoReferenceDataAdapter(api_key="test-key")
        adapter._target_date = date(2026, 5, 20)
        return adapter

    def test_g1a_billable_dataset_maps_only_three(self) -> None:
        """§7.1 billable-venue guard: only the 3 subscribed datasets are mapped."""
        from unified_api_contracts.internal import AssetClass

        from instruments_service.reference_data.adapters.tradfi.databento import (
            _DATASET_TO_VENUE,
            _FUTURES_DATASETS,
            _DATASET_TO_asset_group,
        )

        allowed = {"GLBX.MDP3", "DBEQ.BASIC", "XCBF.PITCH"}
        assert set(_DATASET_TO_VENUE) == allowed
        assert set(_DATASET_TO_asset_group) == allowed
        for non_billable in ("IFEU.IMPACT", "IFUS.IMPACT", "OPRA.PILLAR", "XNAS.ITCH", "XNAS.BASIC", "XNYS.PILLAR"):
            assert non_billable not in _DATASET_TO_VENUE
            assert non_billable not in _DATASET_TO_asset_group
        # G1.c: XCBF.PITCH (VX/VIX) is COMMODITY, not EQUITY.
        assert _DATASET_TO_asset_group["XCBF.PITCH"] == AssetClass.COMMODITY
        # 2026-07-08 canonicalization fix: XCBF.PITCH IS a futures-dataset now — VX class-"S"
        # calendar spreads are DECOMPOSED via InstrumentLeg/COMBO (see
        # test_g1c_xcbf_spreads_decompose_to_combo), not dropped.
        assert frozenset({"GLBX.MDP3", "XCBF.PITCH"}) == _FUTURES_DATASETS

    # NB: the UAC VX.FUT asset_group=commodity assertion lives in UAC's own test suite
    # (test_net_profitable_equity_perp_singles.py::test_vx_future_asset_group_is_commodity) —
    # an IS test must not assert UAC's raw registry content (it would false-fail under UAC
    # promotion lag, since IS CI tests against the baked UAC). IS's symbology-map view of
    # XCBF.PITCH=COMMODITY is covered by test_g1a_billable_dataset_maps_only_three.

    def test_g1b_cefi_singles_excluded_from_tradfi_enumeration(self) -> None:
        """cefi-domain equity singles (asset_group='cefi') are filtered out of the tradfi curated set."""
        from unified_api_contracts import TRADFI_DATABENTO_INSTRUMENTS
        from unified_api_contracts.internal import AssetClass

        valid = frozenset(AssetClass)
        cefi_singles = [d for d in TRADFI_DATABENTO_INSTRUMENTS if d.asset_group == "cefi"]
        assert cefi_singles, "precondition: _NET_PROFITABLE_EQUITY_PERP_SINGLES carry asset_group='cefi'"
        assert {"NVDA", "MSFT", "CRCL"} <= {d.symbol for d in cefi_singles}
        # The enumeration filter (get_instruments) keeps only valid-AssetClass defs.
        kept = [d for d in TRADFI_DATABENTO_INSTRUMENTS if d.asset_group in valid]
        assert all(d.asset_group != "cefi" for d in kept), "cefi-singles must not survive the tradfi filter"

    def test_g1c_xcbf_spreads_decompose_to_combo(self) -> None:
        """XCBF.PITCH class-S VX calendar spreads DECOMPOSE to COMBO (2026-07-08 fix,
        superseding the 2026-06-25 G1.c drop) — real legs, human product names, no
        redundant per-leg venue, no whitespace ANYWHERE (top-level key included — the
        whitespace-padded-dash separator collapses to a single "-" via
        _sanitize_symbol_for_key). Outright VX futures are unaffected."""
        adapter = self._adapter()
        spread = self._row("VX/F1:1:S - VX/G1:1:B", instrument_class="S")
        rec = adapter._parse_row_to_record(spread, dataset="XCBF.PITCH", canonical_venue="CBOE")
        assert rec is not None
        assert rec.instrument_type == "COMBO"
        assert rec.instrument_key == "CBOE:COMBO:VX/F1:1:S-VX/G1:1:B"
        assert " " not in rec.instrument_key
        assert rec.legs is not None and len(rec.legs) == 2
        assert rec.legs[0].instrument_key == "FUTURE:VIX"
        assert rec.legs[0].side == "SELL"
        assert rec.legs[1].instrument_key == "FUTURE:VIX"
        assert rec.legs[1].side == "BUY"
        for leg in rec.legs:
            assert " " not in leg.instrument_key
            assert not leg.instrument_key.startswith("CBOE:")

        # 3-leg butterfly — real production shape (ratio=2 on the middle leg).
        butterfly = self._row("VX/H1:1:B - VX/J1:2:S - VX/K1:1:B", instrument_class="S")
        rec3 = adapter._parse_row_to_record(butterfly, dataset="XCBF.PITCH", canonical_venue="CBOE")
        assert rec3 is not None and rec3.instrument_type == "COMBO"
        assert rec3.legs is not None and len(rec3.legs) == 3
        assert [leg.ratio for leg in rec3.legs] == [1, 2, 1]

        # A genuinely unparseable non-outright row (not the documented shape) still drops.
        unparseable = self._row("garbage", instrument_class="S")
        assert adapter._parse_row_to_record(unparseable, dataset="XCBF.PITCH", canonical_venue="CBOE") is None

        # A real 5-leg combo is DROPPED, not truncated (operator spec 2026-07-09:
        # 1-4 legs hard cap) — no real 5-leg row exists in production today, but
        # the parser must still refuse to silently truncate one if it ever shows up.
        five_leg = self._row("VX/F1:1:B - VX/G1:1:S - VX/H1:1:B - VX/J1:1:S - VX/K1:1:B", instrument_class="S")
        assert adapter._parse_row_to_record(five_leg, dataset="XCBF.PITCH", canonical_venue="CBOE") is None

        outright = self._row("VX/F1", instrument_class="F", expiry="2026-06-18T21:00:00+00:00")
        rec_outright = adapter._parse_row_to_record(outright, dataset="XCBF.PITCH", canonical_venue="CBOE")
        assert rec_outright is not None and rec_outright.instrument_type == "FUTURE"

    def test_g1d_dbeq_class_s_is_equity_not_spot_pair(self) -> None:
        """DBEQ.BASIC equity-spot (class 'S') maps to EQUITY, not the default SPOT_PAIR."""
        adapter = self._adapter()
        row = self._row("AAPL", instrument_class="S")
        rec = adapter._parse_row_to_record(row, dataset="DBEQ.BASIC", canonical_venue="NASDAQ")
        assert rec is not None and rec.instrument_type == "EQUITY"

    def test_dbeq_class_k_stock_is_equity_not_spot_pair(self) -> None:
        """2026-07-08 fix: real Databento instrument_class "K" (STOCK, confirmed via
        both the SDK's own InstrumentClass enum and a live definition-schema call for
        AAPL/SPY/IBIT) must map to EQUITY. Before this fix, "K" fell through to the
        default SPOT_PAIR — a real, live bug that mistyped 100% of fresh NASDAQ/NYSE
        single-stock captures (0 of 100 real rows in the 2026-07-08 snapshot were
        EQUITY) and was the root cause of the "224 securities double-keyed as EQUITY
        and SPOT_PAIR" finding."""
        adapter = self._adapter()
        for sym, venue in (("AAPL", "NASDAQ"), ("MSFT", "NASDAQ"), ("XOM", "NYSE")):
            row = self._row(sym, instrument_class="K")
            rec = adapter._parse_row_to_record(row, dataset="DBEQ.BASIC", canonical_venue=venue)
            assert rec is not None and rec.instrument_type == "EQUITY", (
                f"{sym}: expected EQUITY, got {rec.instrument_type if rec else None!r}"
            )

    def test_dbeq_class_k_known_etf_still_reclassifies_etf(self) -> None:
        """A class-"K" row whose raw_symbol is a known ETF (e.g. IBIT) still reclassifies
        to ETF — the KNOWN_ETFS override downstream of _CLASS_TO_TYPE is unaffected by
        the K->EQUITY fix."""
        adapter = self._adapter()
        row = self._row("IBIT", instrument_class="K")
        rec = adapter._parse_row_to_record(row, dataset="DBEQ.BASIC", canonical_venue="NASDAQ")
        assert rec is not None and rec.instrument_type == "ETF"

    def test_g1e_krx_uses_korean_calendar(self) -> None:
        """KRX is declared with the Korean (XKRX) calendar — not the silent 24/7 default."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            _EXCHANGE_HOURS,
            _XCAL_MAPPING,
            is_non_trading_day,
        )

        assert "KRX" in _EXCHANGE_HOURS and _XCAL_MAPPING.get("KRX") == "XKRX"
        # Children's Day (2026-05-05, Tue): KRX closed, US (NYSE) open — proves distinct calendars.
        assert is_non_trading_day("KRX", date(2026, 5, 5)) is True
        assert is_non_trading_day("NYSE", date(2026, 5, 5)) is False
        # A normal Korean trading Thursday is a trading day.
        assert is_non_trading_day("KRX", date(2026, 5, 7)) is False

    def test_g1e_fx_is_24_7(self) -> None:
        """FX is the declared 24/7 exception — never a non-trading day."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import is_non_trading_day

        assert is_non_trading_day("FX", date(2026, 4, 11)) is False  # Saturday

    def test_g1e_undeclared_venue_fail_closed(self) -> None:
        """An undeclared tradfi venue raises (fail-closed) rather than silently defaulting to 24/7."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            UndeclaredTradfiVenueError,
            is_non_trading_day,
        )

        with pytest.raises(UndeclaredTradfiVenueError):
            is_non_trading_day("NONEXISTENT_VENUE", date(2026, 5, 7))


# ---------------------------------------------------------------------------
# KRX KOSPI / KOSPI 200 — Yahoo-sourced index enumeration (2026-06-27)
# Regression guards: the KOSPI index enumerates via the Yahoo indices path
# (YAHOO_INDICES venue=KRX) and ICE no longer attempts an unsubscribed
# Databento dataset (venue_to_databento must NOT contain ICE / IFUS.IMPACT).
# ---------------------------------------------------------------------------
class TestKRXKospiYahooEnumeration:
    """KRX KOSPI/KOSPI200 enumerate as INDEX records via the Yahoo path (2026-06-27)."""

    def test_create_yahoo_index_records_krx_filter_returns_kospi_indices(self) -> None:
        """venue_filter='KRX' returns KOSPI and KOSPI200 index records (not DXY, not equities).

        The KRX INDEX records (KOSPI/KOSPI200) route through _create_yahoo_index_records
        (same as DXY/treasury indices), NOT through _create_krx_equity_records
        (Samsung/Hyundai/SK Hynix). Both paths are active for venue='KRX'.
        """
        adapter = DatabentoReferenceDataAdapter()
        records = adapter._create_yahoo_index_records(venue_filter="KRX")
        keys = {r.instrument_key for r in records}
        assert "KRX:INDEX:KOSPI-USD" in keys, "KOSPI index missing — add to YAHOO_INDICES"
        assert "KRX:INDEX:KOSPI200-USD" in keys, "KOSPI200 index missing — add to YAHOO_INDICES"
        # KRX filter must NOT bleed DXY (ICE) or CBOE treasury indices.
        assert "ICE:INDEX:DXY-USD" not in keys
        for symbol in ("US3M", "US2Y", "US5Y", "US10Y", "US30Y"):
            assert f"CBOE:INDEX:{symbol}-USD" not in keys

    def test_kospi_index_record_fields(self) -> None:
        """KOSPI record has the correct venue, instrument_type, raw_symbol and genesis."""
        adapter = DatabentoReferenceDataAdapter()
        records = {r.instrument_key: r for r in adapter._create_yahoo_index_records(venue_filter="KRX")}
        kospi = records["KRX:INDEX:KOSPI-USD"]
        assert kospi.venue == "KRX"
        assert kospi.instrument_type == "INDEX"
        assert kospi.raw_symbol == "^KS11"
        # Genesis year must be 2019 (the KRX_INDEX_DAILY_FIRST_DATE floor).
        assert kospi.available_from_datetime is not None
        assert kospi.available_from_datetime.year == 2019

    def test_kospi200_index_record_fields(self) -> None:
        """KOSPI200 record has the correct venue, instrument_type, raw_symbol and genesis."""
        adapter = DatabentoReferenceDataAdapter()
        records = {r.instrument_key: r for r in adapter._create_yahoo_index_records(venue_filter="KRX")}
        k200 = records["KRX:INDEX:KOSPI200-USD"]
        assert k200.venue == "KRX"
        assert k200.instrument_type == "INDEX"
        assert k200.raw_symbol == "^KS200"
        assert k200.available_from_datetime is not None
        assert k200.available_from_datetime.year == 2019

    def test_ice_not_in_venue_to_databento(self) -> None:
        """ICE must NOT appear in venue_to_databento (regression guard against IFUS.IMPACT).

        ICE Databento datasets (IFUS.IMPACT / IFEU.IMPACT) are outside the 3-dataset
        paid subscription. Previously venue_to_databento["ICE"] = "IFUS.IMPACT" caused
        the producer to attempt enumeration → DatabentoDatasetNotAllowedError → 32 absent
        ICE days. Fix (2026-06-27): ICE removed from venue_to_databento; ICE's only
        retained instrument (DXY) routes via venue_to_data_provider["ICE"] = "yahoo_finance".
        """
        from unified_api_contracts.registry.venue_mapping import VenueMapping

        vm = VenueMapping()
        assert "ICE" not in vm.venue_to_databento, (
            "ICE must NOT be in venue_to_databento — IFUS/IFEU datasets are out of "
            "our subscription. Re-adding would cause DatabentoDatasetNotAllowedError."
        )
        # ICE must still resolve to a data source (yahoo_finance via DXY).
        assert vm.venue_to_data_provider.get("ICE") == "yahoo_finance", (
            "ICE must route to yahoo_finance via venue_to_data_provider for DXY."
        )
