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
            patch.object(adapter, "_enrich_session_metadata"),
        ):
            results = await adapter.get_instruments(instrument_type="FUTURE")
        assert all(r.instrument_type == "FUTURE" for r in results)

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
    BITFINEX-SPOT (bitfinex) + BITGET (bitget). Their absence is the exact CF-14 gap."""
    from instruments_service.reference_data.adapters.cefi.tardis import _DEFAULT_EXCHANGES

    for exch in ("kraken", "cryptofacilities", "bitfinex", "bitget", "lighter-zksync"):
        assert exch in _DEFAULT_EXCHANGES, f"captured-venue Tardis exchange {exch!r} missing from IS reference universe"


def test_derivatives_only_classifies_kraken_futures() -> None:
    """cryptofacilities (Kraken Futures) + the other derivatives-only Tardis exchanges
    must be classified so unknown-type instruments are skipped, not defaulted to SPOT."""
    from instruments_service.reference_data.adapters.cefi.tardis import _DERIVATIVES_ONLY_EXCHANGES

    for exch in ("cryptofacilities", "okex-futures", "okex-swap", "huobi-dm", "bitfinex-derivatives", "bitget-futures"):
        assert exch in _DERIVATIVES_ONLY_EXCHANGES, f"derivatives-only Tardis exchange {exch!r} not classified"
