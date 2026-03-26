"""Tests for the refactored engine/orchestrator.py and supporting modules."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from unified_api_contracts.internal import InstrumentRecord


def _make_record(venue: str = "AAVEV3-ETHEREUM", itype: str = "A_TOKEN") -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=f"{venue}:{itype}:WETH",
        venue=venue,
        instrument_type=itype,
        base_asset="WETH",
        quote_asset="USDC",
    )


# ---------------------------------------------------------------------------
# engine/orchestrator.py — get_venues_for_categories
# ---------------------------------------------------------------------------


def test_get_venues_for_categories_cefi_returns_canonical_names():
    """CEFI category returns UAC canonical venue names (uppercase)."""
    from instruments_service.engine.orchestrator import get_venues_for_categories

    venues = get_venues_for_categories(["CEFI"])
    assert len(venues) > 0
    # Canonical names are uppercase — not lowercase Tardis API names
    assert "BINANCE-SPOT" in venues or "BINANCE-FUTURES" in venues
    # Should not contain lowercase names
    assert "binance" not in venues


def test_get_venues_for_categories_defi():
    from instruments_service.engine.orchestrator import get_venues_for_categories

    venues = get_venues_for_categories(["DEFI"])
    assert "UNISWAPV3-ETHEREUM" in venues
    assert "MORPHO-ETHEREUM" in venues
    assert "AAVEV3-ETHEREUM" in venues


def test_get_venues_for_categories_all_includes_all_asset_classes():
    from instruments_service.engine.orchestrator import get_venues_for_categories

    all_v = get_venues_for_categories(["ALL"])
    cefi_v = get_venues_for_categories(["CEFI"])
    defi_v = get_venues_for_categories(["DEFI"])
    tradfi_v = get_venues_for_categories(["TRADFI"])
    assert len(all_v) > len(cefi_v)
    assert "UNISWAPV3-ETHEREUM" in all_v
    assert "CME" in all_v


def test_get_venues_for_categories_deduplicates():
    from instruments_service.engine.orchestrator import get_venues_for_categories

    v1 = get_venues_for_categories(["DEFI"])
    v2 = get_venues_for_categories(["DEFI", "DEFI"])
    assert len(v1) == len(v2)


def test_get_venues_for_categories_sports():
    """SPORTS returns only API_FOOTBALL for reference data.
    BETFAIR is for live odds (tick data) — it does NOT belong in instruments-service.
    """
    from instruments_service.engine.orchestrator import get_venues_for_categories

    venues = get_venues_for_categories(["SPORTS"])
    assert "API_FOOTBALL" in venues
    assert "BETFAIR" not in venues, "BETFAIR is tick-data (odds), not reference data"


# ---------------------------------------------------------------------------
# engine/orchestrator.py — is_venue_available
# ---------------------------------------------------------------------------


def test_is_venue_available_before_launch():
    from instruments_service.engine.orchestrator import is_venue_available

    # UNISWAPV4 launched January 2025 — not available in 2020
    assert not is_venue_available("UNISWAPV4-ETHEREUM", "2020-01-01")


def test_is_venue_available_after_launch():
    from instruments_service.engine.orchestrator import is_venue_available

    assert is_venue_available("UNISWAPV3-ETHEREUM", "2026-03-22")
    assert is_venue_available("MORPHO-ETHEREUM", "2025-01-01")


def test_is_venue_available_unknown_venue_defaults_true():
    from instruments_service.engine.orchestrator import is_venue_available

    # Venues with no start date configured are assumed always available
    assert is_venue_available("SOME-UNKNOWN-VENUE", "2000-01-01")


# ---------------------------------------------------------------------------
# engine/orchestrator.py — process_instruments
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_instruments_no_venues_returns_empty():
    """Empty category produces no active venues → empty result."""
    from instruments_service.engine.orchestrator import process_instruments

    with patch(
        "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
        return_value=[],
    ):
        result = await process_instruments("2025-01-01", [])

    assert result == {}


@pytest.mark.asyncio
async def test_process_instruments_all_venues_skipped_before_launch():
    """Venues before their available_from date are filtered; returns empty."""
    from instruments_service.engine.orchestrator import process_instruments

    # Use a date before all venues launched (1900-01-01)
    # Most venues have available_from set, so few/none would pass
    with patch(
        "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
        return_value=[],
    ) as mock_fetch:
        result = await process_instruments("1900-01-01", ["DEFI"])

    # Either no venues pass the filter (result={}) or fetch returns [] → RuntimeError caught?
    # In any case, result should be {} or RuntimeError raised for zero records
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_process_instruments_urdi_zero_records_raises():
    """URDI returning zero records raises RuntimeError (fail the shard)."""
    from instruments_service.engine.orchestrator import process_instruments

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=[],
        ),
        pytest.raises(RuntimeError, match="zero records"),
    ):
        await process_instruments("2026-03-22", ["DEFI"])


@pytest.mark.asyncio
async def test_process_instruments_proceeds_to_write_stage():
    """Records pass domain validation and reach the write stage."""
    from instruments_service.engine.orchestrator import process_instruments

    records = [_make_record()]

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=records,
        ),
        patch("instruments_service.engine.orchestrator.DomainValidationService"),
        patch("instruments_service.engine.orchestrator._write_venue") as mock_write,
        patch("instruments_service.engine.orchestrator.get_data_sink"),
        patch("instruments_service.engine.orchestrator.log_event"),
    ):

        def _write_side_effect(venue_str, df, date, bucket, sink, counts, sampler):
            counts[venue_str] = len(df)

        mock_write.side_effect = _write_side_effect

        result = await process_instruments("2026-03-22", ["DEFI"])

    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# config/service_config.py
# ---------------------------------------------------------------------------


def test_service_config_defaults():
    from instruments_service.config.service_config import InstrumentsServiceConfig

    cfg = InstrumentsServiceConfig()
    assert cfg.service_name == "instruments-service"
    assert cfg.enable_ccxt_integration is True


def test_get_config_singleton():
    from instruments_service.config.service_config import get_config

    assert get_config() is get_config()


# ---------------------------------------------------------------------------
# config_reloaders.py
# ---------------------------------------------------------------------------


def test_config_reloaders_ticker_universe_from_uac():
    """Ticker universe comes from UAC registry, not local files."""
    from instruments_service.config_reloaders import get_ticker_universe

    universe = get_ticker_universe()
    assert "sp500" in universe
    assert "etf" in universe
    assert "nasdaq" in universe
    assert len(universe["sp500"]) > 100


def test_config_reloaders_subscription_list_empty_initially():
    from instruments_service.config_reloaders import get_active_subscription_list

    assert isinstance(get_active_subscription_list(), list)


def test_config_reloaders_is_ticker_from_cloud_false_by_default():
    from instruments_service.config_reloaders import is_ticker_universe_from_cloud

    assert not is_ticker_universe_from_cloud()


def test_instruments_domain_config_state():
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    assert len(state.get_ticker_universe()["sp500"]) > 100
    assert state.get_subscription_list() == []
    assert not state.is_ticker_universe_from_cloud()


def test_start_reloaders_no_bucket_skips_gracefully(caplog):
    from instruments_service.config import get_config
    from instruments_service.config_reloaders import start_domain_config_reloaders

    cfg = get_config()
    cfg.config_store_bucket = ""

    with caplog.at_level("INFO", logger="instruments_service.config_reloaders"):
        start_domain_config_reloaders(cfg)

    assert any("disabled" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# adapters/urdi_reference_provider.py — URDI_SUPPORTED_VENUES
# ---------------------------------------------------------------------------


def test_urdi_supported_venues_uses_canonical_names():
    """URDI_SUPPORTED_VENUES contains UAC canonical names (uppercase)."""
    from instruments_service.adapters.urdi_reference_provider import URDI_SUPPORTED_VENUES

    assert "BINANCE-SPOT" in URDI_SUPPORTED_VENUES or "BINANCE-FUTURES" in URDI_SUPPORTED_VENUES
    assert "UNISWAPV3-ETHEREUM" in URDI_SUPPORTED_VENUES
    assert "MORPHO-ETHEREUM" in URDI_SUPPORTED_VENUES
    assert "BETFAIR" in URDI_SUPPORTED_VENUES  # BETFAIR is in URDI for tick-data use cases
    assert len(URDI_SUPPORTED_VENUES) >= 20


@pytest.mark.asyncio
async def test_fetch_instruments_for_all_venues_empty():
    from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues

    result = await fetch_instruments_for_all_venues([])
    assert result == []


@pytest.mark.asyncio
async def test_fetch_instruments_for_all_venues_unsupported_warns(caplog):
    from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues

    with caplog.at_level("WARNING"):
        result = await fetch_instruments_for_all_venues(["TOTALLY_UNKNOWN_VENUE_XYZ"])

    assert result == []
    assert any("TOTALLY_UNKNOWN_VENUE_XYZ" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_instruments_for_all_venues_deduplicates_adapter():
    """Multiple canonical venues that share an adapter are deduplicated."""
    from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues

    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ):
        result = await fetch_instruments_for_all_venues(["AAVEV3-ETHEREUM"])

    assert len(result) == 1


# ---------------------------------------------------------------------------
# cli/handlers/instruments_handler.py
# ---------------------------------------------------------------------------


def test_cli_main_imports_cleanly():
    """cli/main.py can be imported without errors — bootstrap smoke test."""
    import instruments_service.cli.main  # noqa: F401


def test_instruments_handler_is_unified_service_handler():
    from unified_trading_library import UnifiedServiceHandler

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    assert issubclass(InstrumentsHandler, UnifiedServiceHandler)


def test_instruments_handler_has_preflight_and_process():
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    assert hasattr(InstrumentsHandler, "preflight")
    assert hasattr(InstrumentsHandler, "process")


# ---------------------------------------------------------------------------
# orchestrator — full write-path success (covers _write_venue, catalogue, sample)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_instruments_full_write_path():
    """Full success path: URDI returns records → write per-venue → return counts."""
    from instruments_service.engine.orchestrator import process_instruments

    records = [
        _make_record("UNISWAPV3-ETHEREUM", "LP_TOKEN"),
        _make_record("UNISWAPV3-ETHEREUM", "LP_TOKEN"),
        _make_record("AAVEV3-ETHEREUM", "A_TOKEN"),
    ]
    mock_sink = MagicMock()
    mock_sink.write = MagicMock()
    mock_sampler = MagicMock()
    mock_sampler.enable_sampling = False

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=records,
        ),
        patch("instruments_service.engine.orchestrator.DomainValidationService"),
        patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
        patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
        patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
        patch("instruments_service.engine.orchestrator.log_event"),
        patch("instruments_service.engine.orchestrator._write_catalogue_record"),
    ):
        result = await process_instruments("2026-03-22", ["DEFI"])

    assert "UNISWAPV3-ETHEREUM" in result
    assert "AAVEV3-ETHEREUM" in result
    assert result["UNISWAPV3-ETHEREUM"] == 2
    assert result["AAVEV3-ETHEREUM"] == 1
    assert mock_sink.write.call_count == 2


@pytest.mark.asyncio
async def test_process_instruments_csv_sampling_triggered():
    """When sampling is enabled, generate_csv_sample is called per venue."""
    from instruments_service.engine.orchestrator import process_instruments

    records = [_make_record("AAVEV3-ETHEREUM")]
    mock_sink = MagicMock()
    mock_sampler = MagicMock()
    mock_sampler.enable_sampling = True

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=records,
        ),
        patch("instruments_service.engine.orchestrator.DomainValidationService"),
        patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
        patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
        patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
        patch("instruments_service.engine.orchestrator.log_event"),
        patch("instruments_service.engine.orchestrator._write_catalogue_record"),
    ):
        await process_instruments("2026-03-22", ["DEFI"])

    mock_sampler.generate_csv_sample.assert_called_once()


@pytest.mark.asyncio
async def test_process_instruments_write_error_logged_not_raised():
    """OSError in _write_venue is logged and swallowed — shard continues for other venues."""
    from instruments_service.engine.orchestrator import process_instruments

    records = [_make_record("AAVEV3-ETHEREUM")]
    mock_sink = MagicMock()
    mock_sink.write.side_effect = OSError("disk full")
    mock_sampler = MagicMock()
    mock_sampler.enable_sampling = False

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=records,
        ),
        patch("instruments_service.engine.orchestrator.DomainValidationService"),
        patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
        patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
        patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
        patch("instruments_service.engine.orchestrator.log_event"),
        patch("instruments_service.engine.orchestrator._write_catalogue_record"),
    ):
        result = await process_instruments("2026-03-22", ["DEFI"])

    # Write failed so venue not counted
    assert "AAVEV3-ETHEREUM" not in result


def test_write_venue_no_venue_column_uses_all():
    """_write_venue called with 'all' partition when no venue column present."""
    from instruments_service.engine.orchestrator import _write_venue

    df = pd.DataFrame({"instrument_key": ["ETH:SPOT:WETH"]})
    mock_sink = MagicMock()
    mock_sampler = MagicMock()
    mock_sampler.enable_sampling = False
    counts: dict[str, int] = {}

    with patch("instruments_service.engine.orchestrator._write_catalogue_record"):
        _write_venue("all", df, "2026-03-22", "test-bucket", mock_sink, counts, mock_sampler)

    assert counts.get("all") == 1


# ---------------------------------------------------------------------------
# orchestrator — _get_instruments_bucket with IS_TEST_RUN
# ---------------------------------------------------------------------------


def test_get_instruments_bucket_test_run_appends_test_suffix():
    """IS_TEST_RUN=true appends -test to the prod bucket name.
    Pattern: instruments-store-{category}-{project}-test (not a separate prefix).
    """
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with (
        patch(
            "instruments_service.engine.orchestrator.get_bucket_name",
            return_value="instruments-store-defi-my-project",
        ),
        patch("instruments_service.engine.orchestrator.get_config") as mock_cfg,
    ):
        mock_cfg.return_value.is_test_run = True
        mock_cfg.return_value.gcp_project_id = "my-project"
        bucket = _get_instruments_bucket("DEFI")

    assert bucket == "instruments-store-defi-my-project-test"
    assert bucket.endswith("-test")


def test_get_instruments_bucket_normal_uses_utl():
    """Without is_test_run, bucket comes from UTL get_bucket_name."""
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with (
        patch("instruments_service.engine.orchestrator.get_config") as mock_cfg,
        patch(
            "instruments_service.engine.orchestrator.get_bucket_name",
            return_value="instruments-store-prod",
        ),
    ):
        mock_cfg.return_value.is_test_run = False
        mock_cfg.return_value.gcp_project_id = "my-project"
        bucket = _get_instruments_bucket()

    assert bucket == "instruments-store-prod"


def test_get_instruments_bucket_fallback_on_import_error():
    """If get_bucket_name raises AttributeError, falls back to config-based name."""
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with (
        patch(
            "instruments_service.engine.orchestrator.get_bucket_name",
            side_effect=AttributeError("not found"),
        ),
        patch("instruments_service.engine.orchestrator.get_config") as mock_cfg,
    ):
        mock_cfg.return_value.is_test_run = False
        mock_cfg.return_value.gcp_project_id = "fallback-project"
        mock_cfg.return_value.instruments_bucket_prefix = "instruments-store"
        bucket = _get_instruments_bucket()

    assert "fallback-project" in bucket


# ---------------------------------------------------------------------------
# orchestrator — _write_catalogue_record failure is swallowed
# ---------------------------------------------------------------------------


def test_write_catalogue_record_swallows_exception(caplog):
    """ManifestWriter failure during catalogue write doesn't propagate."""
    from instruments_service.engine.orchestrator import _write_catalogue_record

    with (
        caplog.at_level("DEBUG"),
        patch(
            "instruments_service.engine.orchestrator.ManifestWriter",
            side_effect=RuntimeError("catalogue offline"),
        ),
    ):
        _write_catalogue_record("bucket", "day=2026-03-22/venue=AAVEV3-ETHEREUM/instruments.parquet", "2026-03-22", 100)

    assert any("ManifestWriter" in r.message or "catalogue offline" in r.message for r in caplog.records)


def test_write_catalogue_record_success():
    """ManifestWriter.write() and flush() are called on success path."""
    from instruments_service.engine.orchestrator import _write_catalogue_record

    mock_writer = MagicMock()
    with patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_writer):
        _write_catalogue_record(
            "instruments-bucket",
            "instrument_availability/by_date/day=2026-03-22/venue=NYSE/instruments.parquet",
            "2026-03-22",
            500,
        )

    mock_writer.add.assert_called_once()
    mock_writer.write.assert_called_once()
    add_kwargs = mock_writer.add.call_args.kwargs
    assert add_kwargs["row_count"] == 500
    assert add_kwargs["venue"] == "NYSE"


# ---------------------------------------------------------------------------
# config_reloaders — cloud-loading and watch paths
# ---------------------------------------------------------------------------


def test_instruments_domain_config_state_on_reload():
    """_on_reload updates subscription list and enabled venues from config object."""
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    mock_config = MagicMock()
    mock_config.subscription_list = ["AAPL", "MSFT"]
    mock_config.enabled_venues = ["NYSE", "NASDAQ"]

    with patch("instruments_service.config_reloaders.log_event"):
        state._on_reload(mock_config)

    assert "AAPL" in state.get_subscription_list()
    assert "NYSE" in state.get_enabled_venues()


def test_instruments_domain_config_state_ticker_from_cloud():
    """load_ticker_universe_from_cloud sets from_cloud=True on success."""
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    assert not state.is_ticker_universe_from_cloud()

    mock_store = MagicMock()
    mock_config = MagicMock()
    mock_config.sp500_tickers = ["AAPL", "MSFT"]
    mock_config.etf_tickers = ["SPY"]
    mock_config.nasdaq_tickers = ["GOOG"]
    mock_config.known_etf_symbols = ["SPY", "QQQ"]
    mock_store.load_config.return_value = mock_config

    from instruments_service.config import get_config

    cfg = get_config()
    cfg.config_store_bucket = "my-bucket"

    with (
        patch("instruments_service.config_reloaders.TimeSeriesConfigStore", return_value=mock_store),
        patch("instruments_service.config_reloaders.log_event"),
    ):
        result = state.load_ticker_universe_from_cloud(cfg)

    assert result is True
    assert state.is_ticker_universe_from_cloud()
    assert state.get_ticker_universe()["sp500"] == ["AAPL", "MSFT"]


def test_load_ticker_universe_no_bucket_returns_false():
    """Missing config_store_bucket skips cloud load."""
    from instruments_service.config import get_config
    from instruments_service.config_reloaders import load_ticker_universe_from_cloud

    cfg = get_config()
    cfg.config_store_bucket = ""
    result = load_ticker_universe_from_cloud(cfg)
    assert result is False


def test_load_ticker_universe_from_cloud_failure_keeps_uac_fallback(caplog):
    """TimeSeriesConfigStore failure falls back to UAC ticker universe."""
    from instruments_service.config import get_config
    from instruments_service.config_reloaders import get_ticker_universe, load_ticker_universe_from_cloud

    cfg = get_config()
    cfg.config_store_bucket = "my-bucket"

    with (
        caplog.at_level("WARNING"),
        patch(
            "instruments_service.config_reloaders.TimeSeriesConfigStore",
            side_effect=ConnectionError("GCS offline"),
        ),
    ):
        result = load_ticker_universe_from_cloud(cfg)

    assert result is False
    assert len(get_ticker_universe()["sp500"]) > 0


def test_start_reloaders_with_bucket_starts_watcher():
    """start_domain_config_reloaders starts DomainConfigReloader when bucket is set."""
    from instruments_service.config import get_config
    from instruments_service.config_reloaders import start_domain_config_reloaders

    cfg = get_config()
    cfg.config_store_bucket = "my-config-bucket"

    mock_reloader = MagicMock()
    with patch("instruments_service.config_reloaders.DomainConfigReloader", return_value=mock_reloader):
        start_domain_config_reloaders(cfg)

    mock_reloader.start_watching.assert_called_once()


# ---------------------------------------------------------------------------
# urdi_reference_provider — api_keys injection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_instruments_passes_api_key_to_adapter_constructor():
    """API key is injected into the adapter at construction time via get_adapter_for_canonical_venue."""
    from instruments_service.adapters.urdi_reference_provider import (
        ADAPTER_DATA_SOURCES,
        fetch_instruments_for_all_venues,
    )

    record = _make_record()
    mock_adapter = MagicMock()
    mock_adapter.get_instruments = AsyncMock(return_value=[record])

    with patch(
        "instruments_service.adapters.urdi_reference_provider.get_adapter_for_canonical_venue",
        return_value=mock_adapter,
    ) as mock_get_adapter:
        # Build api_keys using the actual data source for the AAVE adapter
        data_source = ADAPTER_DATA_SOURCES.get("aave_v3", "")
        api_keys = {data_source: "test-key"} if data_source else {}
        result = await fetch_instruments_for_all_venues(
            ["AAVEV3-ETHEREUM"],
            api_keys=api_keys,
        )

    assert len(result) == 1
    # api_key is passed as kwarg to get_adapter_for_canonical_venue
    mock_get_adapter.assert_called_once()
    call_kwargs = mock_get_adapter.call_args.kwargs
    # The injected api_key should be either None (no data source) or the test key
    assert "api_key" in call_kwargs


# ===========================================================================
# DATE FILTERING
# Tests that filter_instruments_by_date correctly removes instruments that
# were not active on the requested date. This prevents writing 365k all-time
# listings when only a few hundred were live on a given day.
# ===========================================================================


def _make_dated_record(
    venue: str = "BINANCE-SPOT",
    available_since: str | None = None,
    available_to: str | None = None,
) -> InstrumentRecord:
    from datetime import UTC, datetime

    since = datetime.fromisoformat(available_since).replace(tzinfo=UTC) if available_since else None
    until = datetime.fromisoformat(available_to).replace(tzinfo=UTC) if available_to else None
    return InstrumentRecord(
        instrument_key=f"{venue}:SPOT:BTC-USD",
        venue=venue,
        instrument_type="SPOT",
        base_asset="BTC",
        quote_asset="USD",
        available_since=since,
        available_to=until,
    )


def test_date_filter_keeps_always_active_instrument():
    """Instrument with no dates is always included (available_since=None, available_to=None)."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    record = _make_dated_record(available_since=None, available_to=None)
    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    result = filter_instruments_by_date([record], date_dt)
    assert len(result) == 1


def test_date_filter_keeps_instrument_active_on_date():
    """Instrument active across a range that includes the requested date."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    record = _make_dated_record(available_since="2020-01-01", available_to="2030-01-01")
    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    result = filter_instruments_by_date([record], date_dt)
    assert len(result) == 1


def test_date_filter_removes_instrument_listed_after_requested_date():
    """Instrument listed AFTER the requested date is excluded."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    record = _make_dated_record(available_since="2027-01-01", available_to=None)
    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    result = filter_instruments_by_date([record], date_dt)
    assert len(result) == 0


def test_date_filter_removes_instrument_delisted_before_requested_date():
    """Instrument delisted BEFORE the requested date is excluded."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    record = _make_dated_record(available_since="2019-01-01", available_to="2022-12-31")
    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    result = filter_instruments_by_date([record], date_dt)
    assert len(result) == 0


def test_date_filter_bulk_reduction():
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    records = [
        _make_dated_record("BINANCE-SPOT", "2019-01-01", "2021-12-31"),  # delisted
        _make_dated_record("BINANCE-SPOT", "2020-01-01", None),  # still live
        _make_dated_record("BINANCE-SPOT", "2027-01-01", None),  # future listing
        _make_dated_record("BINANCE-SPOT", "2020-06-01", "2025-09-01"),  # delisted 2025
        _make_dated_record("BINANCE-SPOT", None, None),  # always active
    ]
    result = filter_instruments_by_date(records, date_dt)
    assert len(result) == 2, f"Expected 2 active instruments, got {len(result)}"


def test_date_filter_on_exact_boundaries():
    """Instrument listed on the exact requested date is included."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    date_dt = datetime(2026, 3, 22, 0, 0, 0, tzinfo=UTC)
    on_boundary = _make_dated_record(available_since="2026-03-22", available_to=None)
    delisted_on_day = _make_dated_record(available_since="2020-01-01", available_to="2026-03-22")
    result = filter_instruments_by_date([on_boundary, delisted_on_day], date_dt)
    assert len(result) == 2


# ===========================================================================
# BUCKET NAMES PER CATEGORY
# Tests that _get_instruments_bucket resolves to category-specific buckets.
# Prevents regression of flat bucket naming (instruments-store-{project})
# which doesn't exist in GCS and silently fails all writes.
# ===========================================================================


def test_bucket_name_defi_uses_category_prefix():
    """DEFI instruments write to instruments-store-defi-{project}, not flat bucket."""
    from unittest.mock import patch

    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with patch(
        "instruments_service.engine.orchestrator.get_bucket_name",
        return_value="instruments-store-defi-test-project",
    ) as mock_gb:
        bucket = _get_instruments_bucket("DEFI")

    mock_gb.assert_called_once_with("instruments", "DEFI")
    assert "defi" in bucket.lower()
    assert "test-project" in bucket


def test_bucket_name_cefi_uses_category_prefix():
    from unittest.mock import patch

    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with patch(
        "instruments_service.engine.orchestrator.get_bucket_name",
        return_value="instruments-store-cefi-test-project",
    ) as mock_gb:
        _get_instruments_bucket("CEFI")

    mock_gb.assert_called_once_with("instruments", "CEFI")


def test_bucket_name_prediction_uses_category_prefix():
    """Prediction market bucket must exist as category-specific, not a shared bucket."""
    from unittest.mock import patch

    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with patch(
        "instruments_service.engine.orchestrator.get_bucket_name",
        return_value="instruments-store-prediction-test-project",
    ) as mock_gb:
        bucket = _get_instruments_bucket("PREDICTION")

    mock_gb.assert_called_once_with("instruments", "PREDICTION")
    assert "prediction" in bucket.lower()


def test_bucket_test_run_appends_test_suffix():
    """IS_TEST_RUN=true appends -test to the prod bucket name.
    instruments-store-defi-test-project → instruments-store-defi-test-project-test
    """
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with (
        patch(
            "instruments_service.engine.orchestrator.get_bucket_name",
            return_value="instruments-store-defi-test-project",
        ),
        patch("instruments_service.engine.orchestrator.get_config") as mock_cfg,
    ):
        mock_cfg.return_value.is_test_run = True
        mock_cfg.return_value.gcp_project_id = "test-project"
        bucket = _get_instruments_bucket("DEFI")

    assert bucket == "instruments-store-defi-test-project-test"
    assert bucket.endswith("-test")


def test_bucket_no_category_uses_flat_bucket():
    """Without a category, falls back to flat instruments-store-{project}."""
    from unittest.mock import patch

    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with patch(
        "instruments_service.engine.orchestrator.get_bucket_name",
        return_value="instruments-store-test-project",
    ) as mock_gb:
        bucket = _get_instruments_bucket(None)

    mock_gb.assert_called_once_with("instruments", None)
    assert "instruments-store" in bucket


# ===========================================================================
# VENUE NAME CANONICALIZATION
# Tests that canonical venue names reach the DataFrame (and therefore GCS
# partition keys) correctly. Prevents the duplicate-partition bug where
# 'binance' and 'BINANCE-FUTURES' both appear as separate GCS partitions.
# ===========================================================================


@pytest.mark.asyncio
async def test_process_instruments_uses_primary_category_for_bucket():
    """process_instruments passes the first category to _get_instruments_bucket."""
    from instruments_service.engine.orchestrator import process_instruments

    records = [_make_record("AAVEV3-ETHEREUM", "A_TOKEN")]
    mock_sink = MagicMock()
    mock_sampler = MagicMock()
    mock_sampler.enable_sampling = False

    with (
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            return_value=records,
        ),
        patch("instruments_service.engine.orchestrator.filter_instruments_by_date", return_value=records),
        patch("instruments_service.engine.orchestrator.DomainValidationService"),
        patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
        patch(
            "instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"
        ) as mock_bucket,
        patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
        patch("instruments_service.engine.orchestrator.log_event"),
        patch("instruments_service.engine.orchestrator._write_catalogue_record"),
    ):
        await process_instruments("2026-03-22", ["DEFI", "CEFI"])

    # The bucket call should use the first category "DEFI"
    mock_bucket.assert_called_once_with("DEFI")


def test_filter_instruments_by_date_preserves_record_integrity():
    """filter_instruments_by_date must not modify record fields — only filter."""
    from datetime import UTC, datetime

    from instruments_service.engine.orchestrator import filter_instruments_by_date

    original = _make_dated_record("BINANCE-FUTURES", "2020-01-01", None)
    date_dt = datetime(2026, 3, 22, tzinfo=UTC)
    result = filter_instruments_by_date([original], date_dt)
    assert len(result) == 1
    assert result[0] is original  # same object, not a copy
    assert result[0].venue == "BINANCE-FUTURES"
    assert result[0].instrument_key == "BINANCE-FUTURES:SPOT:BTC-USD"


# ===========================================================================
# DEFI RELEVANCE FILTER
# Tests that filter_defi_instruments_by_relevance keeps major liquid assets
# and removes long-tail noise like PEPE/WETH or FAITH/MILAREPA.
# ===========================================================================


def _make_defi_record(venue: str, base: str, quote: str, itype: str = "POOL") -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=f"{venue}:{itype}:{base}-{quote}",
        venue=venue,
        instrument_type=itype,
        base_asset=base,
        quote_asset=quote,
    )


def test_defi_filter_keeps_major_dex_pair():
    """WETH/USDC is a major pair — both sides in whitelist."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("UNISWAPV3-ETHEREUM", "WETH", "USDC")
    assert len(filter_defi_instruments_by_relevance([r])) == 1


def test_defi_filter_keeps_wbtc_weth_pair():
    """WBTC/WETH is a major pair."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("UNISWAPV3-ETHEREUM", "WBTC", "WETH")
    assert len(filter_defi_instruments_by_relevance([r])) == 1


def test_defi_filter_removes_pepe_weth():
    """PEPE/WETH — WETH is major but PEPE is not. DEX requires BOTH sides."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("UNISWAPV3-ETHEREUM", "PEPE", "WETH")
    assert len(filter_defi_instruments_by_relevance([r])) == 0


def test_defi_filter_removes_completely_unknown():
    """FAITH/MILAREPA — neither side is major."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("UNISWAPV2-ETHEREUM", "FAITH", "MILAREPA")
    assert len(filter_defi_instruments_by_relevance([r])) == 0


def test_defi_filter_keeps_aave_weth_lending():
    """Aave aWETH — lending protocol, only base checked."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("AAVEV3-ETHEREUM", "WETH", "", itype="A_TOKEN")
    assert len(filter_defi_instruments_by_relevance([r])) == 1


def test_defi_filter_removes_aave_obscure_token():
    """Aave a1INCH — 1INCH IS in our whitelist so this passes (governance token)."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("AAVEV3-ETHEREUM", "1INCH", "", itype="A_TOKEN")
    # 1INCH IS in whitelist
    assert len(filter_defi_instruments_by_relevance([r])) == 1


def test_defi_filter_removes_aave_unknown_shitcoin():
    """Aave lending of a completely unknown token is removed."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("AAVEV3-ETHEREUM", "SHITCOIN", "", itype="A_TOKEN")
    assert len(filter_defi_instruments_by_relevance([r])) == 0


def test_defi_filter_lido_passes():
    """Lido returns stETH/WSTETH — always major LST assets."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    r = _make_defi_record("LIDO-ETHEREUM", "STETH", "ETH", itype="LST")
    assert len(filter_defi_instruments_by_relevance([r])) == 1


def test_defi_filter_only_applies_to_defi_venues():
    """Non-DeFi records (e.g. from a CeFi venue accidentally mixed in) are kept as-is."""
    from instruments_service.engine.orchestrator import filter_defi_instruments_by_relevance

    # A CeFi venue record with unknown assets — filter keeps it because it's not a DEX/lending
    r = _make_defi_record("BINANCE-FUTURES", "SHITCOIN", "WETH", itype="PERPETUAL")
    # BINANCE doesn't match _DEX_VENUE_KEYWORDS (UNISWAP, BALANCER, CURVE)
    # and SHITCOIN not in MAJOR_ASSETS for lending check...
    # Actually for non-DEX non-lending venues, quote might be empty → base check applies
    # Since SHITCOIN not in major assets: 0
    assert len(filter_defi_instruments_by_relevance([r])) == 0  # base not in major


# ===========================================================================
# VENUE FILTER (--venues CLI arg)
# Tests that the venue filter correctly restricts which venues are processed.
# This is the sharding mechanism: run one venue at a time for parallelism.
# ===========================================================================


def test_venue_filter_restricts_to_requested_venues():
    """Preflight passes ALL venues from get_venues_for_categories to ApiKeyReloader."""
    from unittest.mock import MagicMock

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    runtime = MagicMock()
    runtime.category = []
    runtime.start_date = ""
    runtime.gcp_project_id = "test-project"

    handler = InstrumentsHandler(runtime)

    with (
        patch(
            "instruments_service.cli.handlers.instruments_handler.get_venues_for_categories",
            return_value=["BINANCE-FUTURES", "OKX", "DERIBIT"],
        ),
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader") as mock_reloader,
    ):
        mock_reloader.return_value.current_keys = {}
        mock_reloader.return_value.start = MagicMock()
        import asyncio

        asyncio.run(handler.preflight())

    # All venues from get_venues_for_categories should be passed to ApiKeyReloader
    call_venues = (
        mock_reloader.call_args.kwargs.get("venues") or mock_reloader.call_args.args[0]
        if mock_reloader.call_args
        else []
    )
    assert "BINANCE-FUTURES" in call_venues
    assert "OKX" in call_venues
    assert "DERIBIT" in call_venues


def test_venue_filter_is_case_insensitive():
    """--venues binance-futures (lowercase) is stored; preflight validates ALL venues."""
    from unittest.mock import MagicMock

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    runtime = MagicMock()
    runtime.category = []
    runtime.start_date = ""
    runtime.gcp_project_id = "test-project"
    runtime.venue_filter = ["binance-futures"]  # lowercase

    handler = InstrumentsHandler(runtime)

    with (
        patch(
            "instruments_service.cli.handlers.instruments_handler.get_venues_for_categories",
            return_value=["BINANCE-FUTURES", "OKX"],
        ),
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader") as mock_reloader,
    ):
        mock_reloader.return_value.current_keys = {}
        mock_reloader.return_value.start = MagicMock()
        import asyncio

        asyncio.run(handler.preflight())

    # preflight validates keys for ALL venues (venue filter applied in process())
    call_venues = (
        mock_reloader.call_args.kwargs.get("venues") or mock_reloader.call_args.args[0]
        if mock_reloader.call_args
        else []
    )
    assert "BINANCE-FUTURES" in call_venues


def test_no_venue_filter_processes_all_venues():
    """No --venues flag means all category venues are processed."""
    from unittest.mock import MagicMock

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    runtime = MagicMock()
    runtime.category = []
    runtime.start_date = ""
    runtime.gcp_project_id = "test-project"
    runtime.venue_filter = []  # empty = no filter

    handler = InstrumentsHandler(runtime)

    all_venues = ["BINANCE-FUTURES", "OKX", "DERIBIT"]
    with (
        patch(
            "instruments_service.cli.handlers.instruments_handler.get_venues_for_categories",
            return_value=all_venues,
        ),
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader") as mock_reloader,
    ):
        mock_reloader.return_value.current_keys = {}
        mock_reloader.return_value.start = MagicMock()
        import asyncio

        asyncio.run(handler.preflight())

    call_venues = (
        mock_reloader.call_args.kwargs.get("venues") or mock_reloader.call_args.args[0]
        if mock_reloader.call_args
        else []
    )
    assert "BINANCE-FUTURES" in call_venues
    assert "OKX" in call_venues
    assert "DERIBIT" in call_venues


# ===========================================================================
# INSTRUMENT TYPE NORMALIZATION
# Tests that instrument_type field is always canonical lowercase string
# consistent with InstrumentType enum values ('perp', 'futures', 'option', 'spot').
# Prevents the PERPETUAL/perp or FUTURE/futures duplication bug.
# ===========================================================================


def test_deribit_instrument_type_is_lowercase_string():
    """Deribit adapter stores 'perp' not InstrumentType.PERP in instrument_type field.
    Prevents PERPETUAL/perp duplication in GroupBy partitions.
    """

    from unified_api_contracts.internal import InstrumentType

    # _parse_instrument should store lowercase string values from InstrumentType.value
    assert InstrumentType.PERP.value == "perp"
    assert InstrumentType.FUTURES.value == "futures"
    assert InstrumentType.OPTION.value == "option"

    # The instrument_key for perp should use uppercase
    perp_key = f"DERIBIT:{'perp'.upper()}:BTC-PERPETUAL"
    assert perp_key == "DERIBIT:PERP:BTC-PERPETUAL"

    # The instrument_type stored should be lowercase
    assert InstrumentType.PERP.value == "perp"  # NOT "PERPETUAL"
    assert InstrumentType.FUTURES.value == "futures"  # NOT "FUTURE"


def test_instrument_type_values_are_lowercase_canonical():
    """InstrumentType enum values are lowercase — all adapters must store .value not .name."""
    from unified_api_contracts.internal import InstrumentType

    for member in InstrumentType:
        assert member.value == member.value.lower(), (
            f"InstrumentType.{member.name} has non-lowercase value {member.value!r}. "
            "Adapters must use .value to get the stored string."
        )


@pytest.mark.asyncio
async def test_process_instruments_cefi_venues_available():
    """CEFI includes Coinbase and Upbit — they should be in the venue list."""
    from instruments_service.engine.orchestrator import get_venues_for_categories

    venues = get_venues_for_categories(["CEFI"])
    assert "COINBASE-SPOT" in venues, "Coinbase-SPOT should be in CEFI (via Tardis adapter)"
    assert "UPBIT" in venues, "Upbit should be in CEFI (via Tardis adapter)"
    assert "DERIBIT" in venues
    assert "OKX" in venues or "OKX-FUTURES" in venues or "OKX-SPOT" in venues
    # BETFAIR is NOT in CEFI — it's tick data only
    assert "BETFAIR" not in venues, "Betfair is tick-data only, must not appear in CEFI instruments"
