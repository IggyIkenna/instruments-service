"""Tests for orchestrator helper functions — no cloud/network dependencies."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import (
    _build_defi_venues,
    _get_instruments_bucket,
    _write_catalogue_record,
    _write_fixture_mapping,
    filter_defi_instruments_by_relevance,
    filter_instruments_by_date,
    get_venues_for_asset_groups,
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
# get_venues_for_asset_groups
# ---------------------------------------------------------------------------


class TestGetVenuesForCategories:
    def test_cefi_returns_cefi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["CEFI"])
        assert "BINANCE-SPOT" in venues
        assert "DERIBIT" in venues
        assert "HYPERLIQUID" in venues

    def test_defi_returns_defi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["DEFI"])
        assert any("AAVEV3" in v for v in venues)
        assert any("UNISWAPV3" in v for v in venues)

    def test_tradfi_returns_tradfi_venues(self) -> None:
        venues = get_venues_for_asset_groups(["TRADFI"])
        assert "CME" in venues
        assert "NASDAQ" in venues

    def test_sports_returns_api_football(self) -> None:
        venues = get_venues_for_asset_groups(["SPORTS"])
        assert "API_FOOTBALL" in venues

    def test_prediction_returns_polymarket_kalshi(self) -> None:
        venues = get_venues_for_asset_groups(["PREDICTION"])
        assert "POLYMARKET" in venues
        assert "KALSHI" in venues

    def test_all_includes_all_categories(self) -> None:
        venues = get_venues_for_asset_groups(["ALL"])
        assert "BINANCE-SPOT" in venues
        assert "CME" in venues
        assert "API_FOOTBALL" in venues
        assert "POLYMARKET" in venues
        assert any("AAVEV3" in v for v in venues)

    def test_empty_categories_returns_empty(self) -> None:
        venues = get_venues_for_asset_groups([])
        assert venues == []

    def test_deduplication(self) -> None:
        venues = get_venues_for_asset_groups(["CEFI", "CEFI"])
        # Should not have duplicates
        assert len(venues) == len(set(venues))

    def test_case_insensitive(self) -> None:
        venues_upper = get_venues_for_asset_groups(["CEFI"])
        venues_lower = get_venues_for_asset_groups(["cefi"])
        assert venues_upper == venues_lower


# ---------------------------------------------------------------------------
# is_venue_available
# ---------------------------------------------------------------------------


class TestIsVenueAvailable:
    def test_unknown_venue_returns_true(self) -> None:
        assert is_venue_available("TOTALLY_UNKNOWN_VENUE", "2020-01-01") is True

    def test_known_venue_before_launch_returns_false(self) -> None:
        # If a venue has a launch date in 2020, checking 2010 should be False.
        # SSOT: UAC VenueMapping.get_instrument_discovery_start (replaced the
        # local _VENUE_LAUNCH_DATES dict in commit a3355f2).
        from instruments_service.engine.orchestrator import _VENUE_MAPPING

        for venue in ("BINANCE-SPOT", "DERIBIT", "HYPERLIQUID", "BYBIT", "OKX"):
            launch_date = _VENUE_MAPPING.get_instrument_discovery_start(venue)
            if launch_date is not None and launch_date > "2010-01-01":
                assert is_venue_available(venue, "2010-01-01") is False
                return
        # If no known venue resolved (unexpected), fail loud — UAC drift signal.
        raise AssertionError(
            "No probed venue resolved a discovery-start date via VenueMapping — "
            "either UAC venue_start_dates regressed or every probed venue launched pre-2010."
        )

    def test_known_venue_after_launch_returns_true(self) -> None:
        # SSOT: UAC VenueMapping.get_instrument_discovery_start.
        from instruments_service.engine.orchestrator import _VENUE_MAPPING

        for venue in ("BINANCE-SPOT", "DERIBIT", "HYPERLIQUID", "BYBIT", "OKX"):
            if _VENUE_MAPPING.get_instrument_discovery_start(venue) is not None:
                assert is_venue_available(venue, "2030-01-01") is True
                return
        raise AssertionError("No probed venue resolved a discovery-start date via VenueMapping — UAC drift.")


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
            "instruments_service.engine.orchestrator.get_write_bucket_name", return_value="instruments-store-defi-test"
        ):
            bucket = _get_instruments_bucket("DEFI")
        assert "instruments" in bucket.lower()

    def test_bucket_fallback(self) -> None:
        with patch("instruments_service.engine.orchestrator.get_write_bucket_name", side_effect=AttributeError):
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
                    "instruments_service.engine.orchestrator.get_write_bucket_name",
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


# ---------------------------------------------------------------------------
# _write_fixture_mapping
# ---------------------------------------------------------------------------


class TestWriteFixtureMapping:
    def test_404_forward_poll_window_no_classify(self) -> None:
        """Missing instruments parquet on a future date in the rolling window is expected (zero fixtures)."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = Exception(
            "404 GET https://storage.googleapis.com/... No such object: .../instruments.parquet"
        )
        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.datetime") as mock_datetime,
            patch("instruments_service.engine.orchestrator.classify_and_emit_error") as mock_classify,
        ):
            mock_datetime.now.return_value = datetime(2026, 4, 21, 12, 0, 0, tzinfo=UTC)
            _write_fixture_mapping("test-bucket", "2026-04-28")
        mock_classify.assert_not_called()


# ---------------------------------------------------------------------------
# Bug 2 regression — UnboundLocalError on get_leagues_needing_refresh
# ---------------------------------------------------------------------------


class TestGetLeaguesNeedingRefreshImportScope:
    """Regression for Bug 2 — forward-poll VM ``af-backfill-20260421-142640``.

    A conditional ``from unified_api_contracts.sports import
    get_leagues_needing_refresh`` inside the TRANSFERMARKT branch of
    ``process_instruments`` made Python treat the name as a function-local,
    so the second call site (zero-fixture fast path) raised
    ``UnboundLocalError: cannot access local variable
    'get_leagues_needing_refresh' where it is not associated with a value``
    whenever the code path skipped the TRANSFERMARKT branch.

    Fix: hoist the import to module scope (single source of truth at the
    module top-level) and drop the local ``from`` statement. These tests
    lock the fix in so no one re-adds a conditional import.
    """

    def test_imported_at_module_level(self) -> None:
        """The symbol must be resolvable on the orchestrator module namespace."""
        from instruments_service.engine import orchestrator

        assert hasattr(orchestrator, "get_leagues_needing_refresh"), (
            "get_leagues_needing_refresh must be imported at module scope — "
            "a local import would re-introduce Bug 2 (UnboundLocalError)."
        )
        assert callable(orchestrator.get_leagues_needing_refresh)

    def test_no_local_import_in_source(self) -> None:
        """Source scan: no conditional ``import get_leagues_needing_refresh`` remains inside any function body.

        Prevents regression: the pattern ``from ... import
        get_leagues_needing_refresh`` inside ``process_instruments`` or
        any helper would silently shadow the module-level name and trigger
        UnboundLocalError on alternative code paths.
        """
        import inspect

        from instruments_service.engine import orchestrator

        src = inspect.getsource(orchestrator)
        # Strip the single authoritative module-level import block before scanning.
        # The module-level import lives in a ``from unified_api_contracts.sports import (`` block.
        lines = src.splitlines()
        in_module_import_block = False
        filtered_lines: list[str] = []
        for line in lines:
            if line.startswith("from unified_api_contracts.sports import ("):
                in_module_import_block = True
                continue
            if in_module_import_block:
                if line.strip() == ")":
                    in_module_import_block = False
                continue
            filtered_lines.append(line)
        filtered_src = "\n".join(filtered_lines)
        assert "import get_leagues_needing_refresh" not in filtered_src, (
            "No function-local import of get_leagues_needing_refresh is "
            "permitted — it caused Bug 2 UnboundLocalError. Keep the "
            "symbol imported once at module scope."
        )
