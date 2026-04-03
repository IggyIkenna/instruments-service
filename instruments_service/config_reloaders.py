"""Domain config hot-reload wiring for instruments-service.

Manages three config domains:
1. InstrumentDomainConfig — subscription list, enabled venues (hot-reloaded via PubSub)
2. Ticker universe — SP500/ETF/NASDAQ tickers (from UAC registry as fallback,
   or from cloud ConfigStore for date-replay in batch mode)
3. DeFi major assets filter — which base/quote assets are kept in DEFI category
   (whitelist of ETH, BTC, USDT, USDC and known derivatives). Defaults are
   hardcoded here; can be overridden via cloud ConfigStore.

Uses UTL DomainConfigReloader — no custom retry or PubSub wiring needed.
"""

from __future__ import annotations

import logging
from datetime import date

from unified_api_contracts import KNOWN_ETFS, TRADFI_TICKER_UNIVERSE
from unified_trading_library import (
    ConfigStoreError,
    DomainConfigReloader,
    InstrumentDomainConfig,
    TickerUniverseConfig,
    TimeSeriesConfigStore,
    log_event,
)

from instruments_service.config.service_config import InstrumentsServiceConfig

logger = logging.getLogger(__name__)


class InstrumentsDomainConfigState:
    """Holds the latest hot-reloaded instruments domain config state.

    Replaces module-level mutable globals — state is instance-owned
    so tests can create isolated instances without cross-contamination.
    """

    # DeFi major assets SSOT: InstrumentDomainConfig.defi_major_assets (UCI).
    # No local copy — imported at __init__ time, hot-reloadable via cloud ConfigStore.

    def __init__(self) -> None:
        self._subscription_list: list[str] = []
        self._enabled_venues: list[str] = []
        self._ticker_sp500: list[str] = list(TRADFI_TICKER_UNIVERSE["sp500_tickers"])
        self._ticker_etf: list[str] = list(TRADFI_TICKER_UNIVERSE["etf_tickers"])
        self._ticker_nasdaq: list[str] = list(TRADFI_TICKER_UNIVERSE["nasdaq_tickers"])
        self._ticker_known_etfs: list[str] = sorted(KNOWN_ETFS)
        self._ticker_from_cloud: bool = False
        # SSOT for DeFi major assets is InstrumentDomainConfig.defi_major_assets (UCI).
        # Read the field default without instantiating the full config (avoids env var conflicts).
        _field = InstrumentDomainConfig.model_fields["defi_major_assets"]
        self._defi_major_assets: frozenset[str] = _field.default_factory() if _field.default_factory else frozenset()
        self._reloader: DomainConfigReloader[InstrumentDomainConfig] | None = None

    def get_subscription_list(self) -> list[str]:
        return list(self._subscription_list)

    def get_enabled_venues(self) -> list[str]:
        return list(self._enabled_venues)

    def get_ticker_universe(self) -> dict[str, list[str]]:
        return {
            "sp500": list(self._ticker_sp500),
            "etf": list(self._ticker_etf),
            "nasdaq": list(self._ticker_nasdaq),
            "known_etfs": list(self._ticker_known_etfs),
        }

    def is_ticker_universe_from_cloud(self) -> bool:
        return self._ticker_from_cloud

    def get_defi_major_assets(self) -> frozenset[str]:
        """Return the current DeFi major assets whitelist.

        Used by the orchestrator to filter DEFI instruments to ETH/BTC/USDT/USDC
        and their liquid derivatives. Falls back to the hardcoded default if
        no cloud config has been loaded.
        """
        return self._defi_major_assets

    def update_defi_major_assets(self, assets: frozenset[str]) -> None:
        """Replace the DeFi major assets whitelist (called on cloud config reload)."""
        self._defi_major_assets = assets
        logger.info("DeFi major assets filter updated: %d assets", len(assets))

    def _on_reload(self, config: InstrumentDomainConfig) -> None:
        self._subscription_list = list(config.subscription_list)
        self._enabled_venues = list(config.enabled_venues)
        logger.info(
            "Instruments domain config reloaded: %d instruments, %d venues",
            len(self._subscription_list),
            len(self._enabled_venues),
        )
        log_event(
            "CONFIG_RELOADED",
            details={
                "domain": "instruments",
                "subscription_count": len(self._subscription_list),
                "venue_count": len(self._enabled_venues),
            },
        )

    def load_ticker_universe_from_cloud(
        self,
        service_config: InstrumentsServiceConfig,
        target_date: date | None = None,
    ) -> bool:
        """Load ticker universe from cloud ConfigStore (batch replay or live latest).

        Falls back to UAC registry (TRADFI_TICKER_UNIVERSE) if cloud unavailable.
        Returns True if loaded from cloud, False if using fallback.
        """
        bucket: str = service_config.config_store_bucket
        project_id: str | None = service_config.gcp_project_id
        if not bucket:
            logger.info("CONFIG_STORE_BUCKET not set — using UAC registry ticker defaults")
            return False
        try:
            store = TimeSeriesConfigStore(
                bucket_name=bucket,
                service_name="instruments-reference-data",
                schema_version="1.0",
                project_id=project_id or "",
            )
            config = (
                store.config_for_date(target_date, TickerUniverseConfig)
                if target_date is not None
                else store.load_config(TickerUniverseConfig)
            )
            self._ticker_sp500 = list(config.sp500_tickers)
            self._ticker_etf = list(config.etf_tickers)
            self._ticker_nasdaq = list(config.nasdaq_tickers)
            self._ticker_known_etfs = list(config.known_etf_symbols)
            self._ticker_from_cloud = True
            log_event(
                "CONFIG_LOADED",
                details={"domain": "instruments-reference-data", "source": "cloud_storage"},
            )
            return True
        except (ConfigStoreError, ImportError, ConnectionError, TimeoutError, OSError, ValueError) as exc:
            logger.warning("Cloud ticker load failed, using UAC registry fallback: %s", exc)
            return False

    def start_watching(self, service_config: InstrumentsServiceConfig) -> None:
        """Start hot-reload watcher for InstrumentDomainConfig."""
        bucket: str = service_config.config_store_bucket
        project_id: str | None = service_config.gcp_project_id
        if not bucket:
            logger.info("CONFIG_STORE_BUCKET not set — domain config hot-reload disabled")
            return
        self._reloader = DomainConfigReloader(
            domain="instruments",
            config_class=InstrumentDomainConfig,
            config_bucket=bucket,
            project_id=project_id,
        )
        self._reloader.on_reload(self._on_reload)
        self._reloader.start_watching()
        logger.info("Instruments domain config reloader started")

    def stop_watching(self) -> None:
        if self._reloader is not None:
            self._reloader.stop_watching()
            self._reloader = None


# Module-level singleton — convenience functions below delegate to this instance.
_state = InstrumentsDomainConfigState()


def get_active_subscription_list() -> list[str]:
    return _state.get_subscription_list()


def get_active_enabled_venues() -> list[str]:
    return _state.get_enabled_venues()


def get_ticker_universe() -> dict[str, list[str]]:
    return _state.get_ticker_universe()


def is_ticker_universe_from_cloud() -> bool:
    return _state.is_ticker_universe_from_cloud()


def load_ticker_universe_from_cloud(service_config: InstrumentsServiceConfig, target_date: date | None = None) -> bool:
    return _state.load_ticker_universe_from_cloud(service_config, target_date)


def start_domain_config_reloaders(service_config: InstrumentsServiceConfig) -> None:
    _state.start_watching(service_config)


def stop_domain_config_reloaders() -> None:
    _state.stop_watching()


def get_defi_major_assets() -> frozenset[str]:
    """Return the current DeFi major assets filter whitelist.

    Called by the orchestrator's filter_defi_instruments_by_relevance.
    Defaults to the hardcoded set; can be overridden via cloud ConfigStore.
    """
    return _state.get_defi_major_assets()
