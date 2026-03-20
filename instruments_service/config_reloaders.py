"""Domain config hot-reload wiring for instruments-service.

Two config domains:
1. InstrumentDomainConfig — subscription list, enabled venues, categories (hot-reloaded via PubSub)
2. TickerUniverseConfig — S&P 500, ETF, NASDAQ tickers (loaded from cloud storage at startup;
   batch mode uses TimeSeriesConfigStore.config_for_date() for historical replay)

Services use UCI ConfigStore (cloud-agnostic — routes to GCS/S3 based on CLOUD_PROVIDER mode var).
"""

from __future__ import annotations

import logging
from datetime import date

from unified_api_contracts import KNOWN_ETFS
from unified_config_interface import (
    ConfigStoreError,
    InstrumentDomainConfig,
    TickerUniverseConfig,
    TimeSeriesConfigStore,
)
from unified_events_interface import log_event
from unified_trading_library import DomainConfigReloader

from instruments_service.config.instrument_definitions import (
    ETF_TICKERS,
    NASDAQ_TICKERS,
    SP500_TICKERS,
)

logger = logging.getLogger(__name__)
_instrument_reloader: DomainConfigReloader[InstrumentDomainConfig] | None = None

# Module-level snapshot of the latest reloaded subscription list.
# Consumers (e.g. InstrumentsService engine) call get_active_subscription_list()
# to obtain the current live set without holding a reference to the reloader.
_active_subscription_list: list[str] = []
_active_enabled_venues: list[str] = []

# Module-level snapshot of the ticker universe (loaded from cloud storage or fallback)
_ticker_universe_sp500: list[str] = []
_ticker_universe_etf: list[str] = []
_ticker_universe_nasdaq: list[str] = []
_ticker_universe_known_etfs: list[str] = []
_ticker_universe_loaded: bool = False


def get_active_subscription_list() -> list[str]:
    """Return the latest hot-reloaded instrument subscription list.

    Returns an empty list if the reloader has not yet fired or cloud storage
    reload is disabled (CONFIG_STORE_BUCKET not set).
    """
    return list(_active_subscription_list)


def get_active_enabled_venues() -> list[str]:
    """Return the latest hot-reloaded enabled venue list."""
    return list(_active_enabled_venues)


def get_ticker_universe() -> dict[str, list[str]]:
    """Return the loaded ticker universe (from cloud storage or fallback).

    Returns dict with keys: sp500, etf, nasdaq, known_etfs.
    Falls back to embedded tickers.json if cloud storage loading failed or was skipped.
    """
    if not _ticker_universe_loaded:
        _load_ticker_universe_fallback()
    return {
        "sp500": list(_ticker_universe_sp500),
        "etf": list(_ticker_universe_etf),
        "nasdaq": list(_ticker_universe_nasdaq),
        "known_etfs": list(_ticker_universe_known_etfs),
    }


def is_ticker_universe_from_cloud() -> bool:
    """Return True if the ticker universe was loaded from cloud storage (not fallback)."""
    return _ticker_universe_loaded


def _load_ticker_universe_fallback() -> None:
    """Load ticker universe from embedded tickers.json (fallback when cloud storage unavailable)."""
    global _ticker_universe_sp500, _ticker_universe_etf, _ticker_universe_nasdaq
    global _ticker_universe_known_etfs, _ticker_universe_loaded

    _ticker_universe_sp500 = list(SP500_TICKERS)
    _ticker_universe_etf = list(ETF_TICKERS)
    _ticker_universe_nasdaq = list(NASDAQ_TICKERS)
    _ticker_universe_known_etfs = sorted(KNOWN_ETFS)
    # _ticker_universe_loaded stays False — indicates fallback, not cloud
    logger.info(
        "Ticker universe loaded from embedded defaults: %d SP500, %d ETF, %d NASDAQ",
        len(_ticker_universe_sp500),
        len(_ticker_universe_etf),
        len(_ticker_universe_nasdaq),
    )


def _on_instruments_reload(config: InstrumentDomainConfig) -> None:
    global _active_subscription_list, _active_enabled_venues

    _active_subscription_list = list(config.subscription_list)
    _active_enabled_venues = list(config.enabled_venues)

    logger.info(
        "Instruments domain config reloaded: %d instruments, %d venues",
        len(_active_subscription_list),
        len(_active_enabled_venues),
    )
    logger.debug(
        "Active subscription list: %s",
        _active_subscription_list[:10],  # log first 10 to avoid log spam
    )

    log_event(
        "CONFIG_RELOADED",
        details={
            "domain": "instruments",
            "subscription_count": len(_active_subscription_list),
            "venue_count": len(_active_enabled_venues),
        },
    )


def load_ticker_universe_from_cloud(
    service_config: object,
    target_date: date | None = None,
) -> bool:
    """Load ticker universe from cloud storage via UCI ConfigStore.

    In batch mode: uses TimeSeriesConfigStore.config_for_date(target_date)
    to load the config effective at the target date (historical replay).

    In live mode (target_date=None): loads the active/latest config.

    Falls back to embedded tickers.json if cloud storage is unavailable
    or CONFIG_STORE_BUCKET is not set.

    Args:
        service_config: Service config with config_store_bucket and project_id
        target_date: For batch mode, the date to replay config at. None for live/latest.

    Returns:
        True if loaded from cloud storage, False if fell back to embedded defaults.
    """
    global _ticker_universe_sp500, _ticker_universe_etf, _ticker_universe_nasdaq
    global _ticker_universe_known_etfs, _ticker_universe_loaded

    config_store_bucket: str = getattr(service_config, "config_store_bucket", "")
    project_id: str | None = getattr(service_config, "project_id", None)

    if not config_store_bucket:
        logger.info("CONFIG_STORE_BUCKET not set — using embedded ticker defaults")
        _load_ticker_universe_fallback()
        return False

    try:
        store = TimeSeriesConfigStore(
            bucket_name=config_store_bucket,
            service_name="instruments-reference-data",
            schema_version="1.0",
            project_id=project_id or "",
        )

        if target_date is not None:
            # Batch mode: replay config effective at target_date
            config = store.config_for_date(target_date, TickerUniverseConfig)
            logger.info("Ticker universe loaded from cloud storage (replay date=%s)", target_date)
        else:
            # Live mode: load latest active config
            config = store.load_config(TickerUniverseConfig)
            logger.info("Ticker universe loaded from cloud storage (latest active)")

        _ticker_universe_sp500 = list(config.sp500_tickers)
        _ticker_universe_etf = list(config.etf_tickers)
        _ticker_universe_nasdaq = list(config.nasdaq_tickers)
        _ticker_universe_known_etfs = list(config.known_etf_symbols)
        _ticker_universe_loaded = True

        log_event(
            "CONFIG_LOADED",
            details={
                "domain": "instruments-reference-data",
                "source": "cloud_storage",
                "sp500_count": len(_ticker_universe_sp500),
                "etf_count": len(_ticker_universe_etf),
                "effective_date": config.effective_date,
                "replay_date": str(target_date) if target_date else "latest",
            },
        )
        return True

    except (ConfigStoreError, ImportError, ConnectionError, TimeoutError, OSError, ValueError) as exc:
        logger.warning("Cloud storage ticker universe load failed, using embedded defaults: %s", exc)
        _load_ticker_universe_fallback()
        return False


def start_domain_config_reloaders(service_config: object) -> None:
    global _instrument_reloader

    config_store_bucket: str = getattr(service_config, "config_store_bucket", "")
    project_id: str | None = getattr(service_config, "project_id", None)

    if not config_store_bucket:
        logger.info("CONFIG_STORE_BUCKET not set — domain config hot-reload disabled")
        return

    _instrument_reloader = DomainConfigReloader(
        domain="instruments",
        config_class=InstrumentDomainConfig,
        config_bucket=config_store_bucket,
        project_id=project_id,
    )
    _instrument_reloader.on_reload(_on_instruments_reload)
    _instrument_reloader.start_watching()
    logger.info("Instruments domain config reloader started")


def stop_domain_config_reloaders() -> None:
    global _instrument_reloader
    if _instrument_reloader is not None:
        _instrument_reloader.stop_watching()
        _instrument_reloader = None
    logger.info("Instruments domain config reloader stopped")
