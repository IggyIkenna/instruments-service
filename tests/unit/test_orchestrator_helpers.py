"""Tests for orchestrator helper functions — no cloud/network dependencies."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import (
    _build_defi_venues,
    _get_instruments_bucket,
    _write_catalogue_record,
    filter_defi_instruments_by_relevance,
    filter_instruments_by_date,
    get_venues_for_categories,
    is_venue_available,
)


def _make_record(
    instrument_key: str = "TEST",
    venue: str = "TEST-VENUE",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "ETH",
    quote_asset: str = "USDT",
    available_since: datetime | None = None,
    available_to: datetime | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue=venue,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        available_from_datetime=available_since,
        available_to_datetime=available_to,
    )


# ---------------------------------------------------------------------------
# get_venues_for_categories
# ---------------------------------------------------------------------------


class TestGetVenuesForCategories:
    def test_cefi_returns_cefi_venues(self) -> None:
        venues = get_venues_for_categories(["CEFI"])
        assert "BINANCE-SPOT" in venues
        assert "DERIBIT" in venues
        assert "HYPERLIQUID" in venues

    def test_defi_returns_defi_venues(self) -> None:
        venues = get_venues_for_categories(["DEFI"])
        assert any("AAVEV3" in v for v in venues)
        assert any("UNISWAPV3" in v for v in venues)

    def test_tradfi_returns_tradfi_venues(self) -> None:
        venues = get_venues_for_categories(["TRADFI"])
        assert "CME" in venues
        assert "NASDAQ" in venues

    def test_sports_returns_api_football(self) -> None:
        venues = get_venues_for_categories(["SPORTS"])
        assert "API_FOOTBALL" in venues

    def test_prediction_returns_polymarket_kalshi(self) -> None:
        venues = get_venues_for_categories(["PREDICTION"])
        assert "POLYMARKET" in venues
        assert "KALSHI" in venues

    def test_all_includes_all_categories(self) -> None:
        venues = get_venues_for_categories(["ALL"])
        assert "BINANCE-SPOT" in venues
        assert "CME" in venues
        assert "API_FOOTBALL" in venues
        assert "POLYMARKET" in venues
        assert any("AAVEV3" in v for v in venues)

    def test_empty_categories_returns_empty(self) -> None:
        venues = get_venues_for_categories([])
        assert venues == []

    def test_deduplication(self) -> None:
        venues = get_venues_for_categories(["CEFI", "CEFI"])
        # Should not have duplicates
        assert len(venues) == len(set(venues))

    def test_case_insensitive(self) -> None:
        venues_upper = get_venues_for_categories(["CEFI"])
        venues_lower = get_venues_for_categories(["cefi"])
        assert venues_upper == venues_lower


# ---------------------------------------------------------------------------
# is_venue_available
# ---------------------------------------------------------------------------


class TestIsVenueAvailable:
    def test_unknown_venue_returns_true(self) -> None:
        assert is_venue_available("TOTALLY_UNKNOWN_VENUE", "2020-01-01") is True

    def test_known_venue_before_launch_returns_false(self) -> None:
        # If a venue has a launch date in 2020, checking 2010 should be False
        from instruments_service.engine.orchestrator import _VENUE_LAUNCH_DATES

        for venue, _launch_date in _VENUE_LAUNCH_DATES.items():
            if _launch_date > "2010-01-01":
                assert is_venue_available(venue, "2010-01-01") is False
                break

    def test_known_venue_after_launch_returns_true(self) -> None:
        from instruments_service.engine.orchestrator import _VENUE_LAUNCH_DATES

        for venue, _launch_date in _VENUE_LAUNCH_DATES.items():
            assert is_venue_available(venue, "2030-01-01") is True
            break


# ---------------------------------------------------------------------------
# filter_instruments_by_date
# ---------------------------------------------------------------------------


class TestFilterInstrumentsByDate:
    def test_no_date_constraints_passes(self) -> None:
        record = _make_record()
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_since_before_date_passes(self) -> None:
        record = _make_record(available_since=datetime(2024, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_since_after_date_filtered(self) -> None:
        record = _make_record(available_since=datetime(2025, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 0

    def test_available_to_after_date_passes(self) -> None:
        record = _make_record(available_to=datetime(2025, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 1

    def test_available_to_before_date_filtered(self) -> None:
        record = _make_record(available_to=datetime(2024, 1, 1, tzinfo=UTC))
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([record], date_dt)
        assert len(result) == 0

    def test_defi_venue_missing_available_since_warns(self, caplog) -> None:
        record = _make_record(venue="AAVEV3-ETHEREUM")
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        defi_venues = frozenset(["AAVEV3-ETHEREUM"])
        with caplog.at_level("WARNING"):
            result = filter_instruments_by_date([record], date_dt, defi_venues=defi_venues)
        assert len(result) == 1  # still included
        assert any("available_from_datetime=None" in r.message for r in caplog.records)

    def test_multiple_records_filters_correctly(self) -> None:
        r1 = _make_record(instrument_key="A", available_since=datetime(2024, 1, 1, tzinfo=UTC))
        r2 = _make_record(instrument_key="B", available_since=datetime(2025, 1, 1, tzinfo=UTC))
        r3 = _make_record(instrument_key="C")
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        result = filter_instruments_by_date([r1, r2, r3], date_dt)
        assert len(result) == 2  # r1 and r3 pass, r2 filtered


# ---------------------------------------------------------------------------
# filter_defi_instruments_by_relevance
# ---------------------------------------------------------------------------


class TestFilterDefiInstrumentsByRelevance:
    def test_dex_both_major_passes(self) -> None:
        record = _make_record(
            venue="UNISWAPV3-ETHEREUM",
            base_asset="WETH",
            quote_asset="USDC",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_dex_one_long_tail_filtered(self) -> None:
        record = _make_record(
            venue="UNISWAPV3-ETHEREUM",
            base_asset="PEPE",
            quote_asset="WETH",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 0

    def test_lending_base_major_passes(self) -> None:
        record = _make_record(
            venue="AAVEV3-ETHEREUM",
            base_asset="WETH",
            quote_asset="USD",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_lending_base_long_tail_filtered(self) -> None:
        record = _make_record(
            venue="AAVEV3-ETHEREUM",
            base_asset="SHIB",
            quote_asset="USD",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 0


# ---------------------------------------------------------------------------
# _build_defi_venues
# ---------------------------------------------------------------------------


class TestBuildDefiVenues:
    def test_includes_static_venues(self) -> None:
        venues = _build_defi_venues()
        assert "LIDO-ETHEREUM" in venues
        assert "ETHERFI-ETHEREUM" in venues
        assert "ETHENA-ETHEREUM" in venues

    def test_includes_solana_venues(self) -> None:
        venues = _build_defi_venues()
        assert "DRIFT-SOLANA" in venues
        assert "KAMINO-SOLANA" in venues
        assert "RAYDIUM-SOLANA" in venues
        assert "ORCA-SOLANA" in venues
        assert "MARINADE-SOLANA" in venues

    def test_includes_subgraph_venues(self) -> None:
        venues = _build_defi_venues()
        assert any("AAVEV3" in v for v in venues)
        assert any("UNISWAPV3" in v for v in venues)

    def test_returns_non_empty(self) -> None:
        venues = _build_defi_venues()
        assert len(venues) > 10


# ---------------------------------------------------------------------------
# _get_instruments_bucket
# ---------------------------------------------------------------------------


class TestGetInstrumentsBucket:
    def test_bucket_with_category(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.get_bucket_name", return_value="instruments-store-defi-test"
        ):
            bucket = _get_instruments_bucket("DEFI")
        assert "instruments" in bucket.lower()

    def test_bucket_fallback(self) -> None:
        with patch("instruments_service.engine.orchestrator.get_bucket_name", side_effect=AttributeError):
            bucket = _get_instruments_bucket("CEFI")
        assert "instruments" in bucket.lower()

    def test_bucket_test_mode(self) -> None:
        import instruments_service.config.service_config as sc

        old = sc._config
        try:
            sc._config = None
            with patch.dict("os.environ", {"IS_TEST_RUN": "true"}):
                sc._config = None
                cfg = sc.InstrumentsServiceConfig()
                cfg.is_test_run = True
                sc._config = cfg
                with patch(
                    "instruments_service.engine.orchestrator.get_bucket_name",
                    return_value="instruments-store-defi-test",
                ):
                    bucket = _get_instruments_bucket("DEFI")
                assert bucket.endswith("-test")
        finally:
            sc._config = old


# ---------------------------------------------------------------------------
# _write_catalogue_record
# ---------------------------------------------------------------------------


class TestWriteCatalogueRecord:
    def test_non_blocking_on_error(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.ManifestWriter",
            side_effect=ConnectionError("no GCS"),
        ):
            # Should not raise
            _write_catalogue_record(
                "bucket",
                "instrument_availability/by_date/day=2026-03-22/venue=BINANCE-SPOT/instruments.parquet",
                "2026-03-22",
                10,
            )

    def test_successful_write(self) -> None:
        mock_writer = MagicMock()
        with patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_writer):
            _write_catalogue_record(
                "bucket",
                "instrument_availability/by_date/day=2026-03-22/venue=BINANCE-SPOT/instruments.parquet",
                "2026-03-22",
                10,
            )
        mock_writer.add.assert_called_once()
        mock_writer.write.assert_called_once()
