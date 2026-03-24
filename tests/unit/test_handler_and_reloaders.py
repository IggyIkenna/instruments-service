"""Coverage tests for InstrumentsHandler and InstrumentsDomainConfigState."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# InstrumentsHandler — preflight() and process()
# ---------------------------------------------------------------------------


def _make_runtime(start: str = "2026-03-22", end: str = "2026-03-22", categories=None):
    rt = MagicMock()
    rt.start_date = start
    rt.end_date = end
    rt.categories = categories or ["DEFI"]
    rt.gcp_project_id = "test-project"
    return rt


@pytest.mark.asyncio
async def test_handler_uses_api_key_reloader():
    """Handler must use ApiKeyReloader, not raw validate_api_keys_for_venues."""
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())
    assert hasattr(handler, "_key_reloader"), "Handler must use ApiKeyReloader (not _api_keys)"


@pytest.mark.asyncio
async def test_handler_preflight_mock_mode_starts_reloader():
    """In mock mode ApiKeyReloader.start() returns empty keys (no Secret Manager)."""
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())

    mock_reloader = MagicMock()
    mock_reloader.current_keys = {}

    with (
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader", return_value=mock_reloader),
        patch("instruments_service.cli.handlers.instruments_handler.validate_data_availability", return_value=set()),
    ):
        await handler.preflight()

    mock_reloader.start.assert_called_once()
    assert handler._completed_dates == set()


@pytest.mark.asyncio
async def test_handler_preflight_populates_completed_dates():
    """preflight() stores completed dates from validate_data_availability."""
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime("2026-03-22", "2026-03-22"))
    completed = {"2026-03-22"}

    mock_reloader = MagicMock()
    mock_reloader.current_keys = {}

    with (
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader", return_value=mock_reloader),
        patch(
            "instruments_service.cli.handlers.instruments_handler.validate_data_availability", return_value=completed
        ),
    ):
        await handler.preflight()

    assert handler._completed_dates == completed


@pytest.mark.asyncio
async def test_handler_process_skips_completed_date():
    """process() returns None for dates already in _completed_dates."""
    from unified_trading_library import BatchPayload

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())
    handler._completed_dates = {"2026-03-22"}

    payload = BatchPayload(date="2026-03-22", categories=["DEFI"])
    result = await handler.process(payload)

    assert result is None


@pytest.mark.asyncio
async def test_handler_process_redo_all_bypasses_skip():
    """redo_all=True in payload.extra bypasses the completed date check."""
    from unified_trading_library import BatchPayload

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())
    handler._completed_dates = {"2026-03-22"}

    payload = BatchPayload(date="2026-03-22", categories=["DEFI"], extra={"redo_all": True})

    with patch("instruments_service.cli.handlers.instruments_handler.engine_orchestrator") as mock_orch:
        mock_orch.process_instruments = AsyncMock(return_value={"MORPHO-ETHEREUM": 10})
        result = await handler.process(payload)

    assert result == {"MORPHO-ETHEREUM": 10}


@pytest.mark.asyncio
async def test_handler_process_calls_orchestrator():
    """process() calls engine_orchestrator.process_instruments with date and categories."""
    from unified_trading_library import BatchPayload

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())

    payload = BatchPayload(date="2026-03-22", categories=["DEFI"])

    with patch("instruments_service.cli.handlers.instruments_handler.engine_orchestrator") as mock_orch:
        mock_orch.process_instruments = AsyncMock(return_value={"CURVE-ETHEREUM": 49})
        result = await handler.process(payload)

    mock_orch.process_instruments.assert_awaited_once_with(
        date="2026-03-22",
        categories=["DEFI"],
        redo_all=False,
        api_keys={},
        venue_override=None,
    )
    assert result == {"CURVE-ETHEREUM": 49}


@pytest.mark.asyncio
async def test_handler_preflight_logs_api_keys_when_present():
    """preflight() logs which data sources have keys when reloader returns non-empty."""
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())

    mock_reloader = MagicMock()
    mock_reloader.current_keys = {"tardis": "key1", "databento": "key2"}

    with (
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader", return_value=mock_reloader),
        patch("instruments_service.cli.handlers.instruments_handler.validate_data_availability", return_value=set()),
    ):
        await handler.preflight()

    assert handler._key_reloader is not None
    assert handler._key_reloader.current_keys == {"tardis": "key1", "databento": "key2"}


@pytest.mark.asyncio
async def test_handler_preflight_data_availability_error_is_warned_not_raised():
    """Data availability check failure is logged as warning and preflight continues."""
    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())

    mock_reloader = MagicMock()
    mock_reloader.current_keys = {}

    with (
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader", return_value=mock_reloader),
        patch(
            "instruments_service.cli.handlers.instruments_handler.validate_data_availability",
            side_effect=ConnectionError("GCS offline"),
        ),
    ):
        await handler.preflight()

    assert handler._completed_dates == set()


@pytest.mark.asyncio
async def test_handler_preflight_handles_validation_error():
    """If ApiKeyReloader.start() fails, the exception propagates (fail the shard)."""
    from unified_internal_contracts import StartupValidationError

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())

    mock_reloader = MagicMock()
    mock_reloader.start.side_effect = StartupValidationError("Missing key")

    with (
        patch("instruments_service.cli.handlers.instruments_handler.ApiKeyReloader", return_value=mock_reloader),
        pytest.raises(StartupValidationError),
    ):
        await handler.preflight()


@pytest.mark.asyncio
async def test_handler_process_reads_keys_from_reloader():
    """process() reads current_keys from the reloader, not a frozen dict."""
    from unified_trading_library import BatchPayload

    from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())
    mock_reloader = MagicMock()
    mock_reloader.current_keys = {"tardis": "live-key"}
    handler._key_reloader = mock_reloader

    payload = BatchPayload(date="2026-03-22", categories=["DEFI"])

    with patch("instruments_service.cli.handlers.instruments_handler.engine_orchestrator") as mock_orch:
        mock_orch.process_instruments = AsyncMock(return_value={"CURVE-ETHEREUM": 49})
        await handler.process(payload)

    mock_orch.process_instruments.assert_awaited_once_with(
        date="2026-03-22",
        categories=["DEFI"],
        redo_all=False,
        api_keys={"tardis": "live-key"},
        venue_override=None,
    )


# ---------------------------------------------------------------------------
# InstrumentsDomainConfigState — more paths
# ---------------------------------------------------------------------------


def test_domain_config_state_on_reload_updates_lists():
    """_on_reload() updates subscription list and enabled venues."""
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()

    config = MagicMock()
    config.subscription_list = ["BTC-USDT", "ETH-USDT"]
    config.enabled_venues = ["BINANCE-FUTURES", "OKX"]

    state._on_reload(config)

    assert state.get_subscription_list() == ["BTC-USDT", "ETH-USDT"]
    assert state.get_enabled_venues() == ["BINANCE-FUTURES", "OKX"]


def test_domain_config_state_get_ticker_universe_keys():
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    universe = state.get_ticker_universe()
    assert set(universe.keys()) == {"sp500", "etf", "nasdaq", "known_etfs"}
    assert len(universe["sp500"]) > 100


def test_domain_config_state_load_cloud_no_bucket():
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    cfg = MagicMock()
    cfg.config_store_bucket = ""

    result = state.load_ticker_universe_from_cloud(cfg)
    assert result is False
    assert not state.is_ticker_universe_from_cloud()


def test_domain_config_state_load_cloud_store_error():
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    cfg = MagicMock()
    cfg.config_store_bucket = "test-bucket"
    cfg.project_id = "test-project"

    with patch(
        "instruments_service.config_reloaders.TimeSeriesConfigStore",
        side_effect=ConnectionError("no network"),
    ):
        result = state.load_ticker_universe_from_cloud(cfg)

    assert result is False


def test_domain_config_state_stop_watching_no_reloader():
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    # Should not raise
    state.stop_watching()


def test_domain_config_state_stop_watching_with_active_reloader():
    """stop_watching() calls stop_watching() on the reloader and clears it."""
    from instruments_service.config_reloaders import InstrumentsDomainConfigState

    state = InstrumentsDomainConfigState()
    mock_reloader = MagicMock()
    state._reloader = mock_reloader

    state.stop_watching()

    mock_reloader.stop_watching.assert_called_once()
    assert state._reloader is None


def test_module_level_convenience_functions():
    """Module-level convenience functions delegate to the singleton state."""
    from instruments_service.config_reloaders import (
        get_active_enabled_venues,
        get_active_subscription_list,
        get_ticker_universe,
        is_ticker_universe_from_cloud,
        stop_domain_config_reloaders,
    )

    assert isinstance(get_active_subscription_list(), list)
    assert isinstance(get_active_enabled_venues(), list)
    universe = get_ticker_universe()
    assert "sp500" in universe
    assert not is_ticker_universe_from_cloud()
    stop_domain_config_reloaders()  # no-op when not started


# ---------------------------------------------------------------------------
# orchestrator.py — _get_instruments_bucket, _write_catalogue_record
# ---------------------------------------------------------------------------


def test_get_instruments_bucket_fallback():
    """Falls back to gcp_project_id if get_bucket_name unavailable."""
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    with patch("instruments_service.engine.orchestrator.get_bucket_name", side_effect=AttributeError):
        bucket = _get_instruments_bucket()

    assert "test-project" in bucket or "instruments" in bucket.lower()


def test_write_catalogue_record_non_blocking_on_error():
    """ManifestWriter failure does not raise — it's non-blocking."""
    from instruments_service.engine.orchestrator import _write_catalogue_record

    with patch(
        "instruments_service.engine.orchestrator.ManifestWriter",
        side_effect=ConnectionError("no GCS"),
    ):
        # Should not raise
        _write_catalogue_record(
            "bucket", "instrument_availability/by_date/day=2026-03-22/test.parquet", "2026-03-22", 10
        )
