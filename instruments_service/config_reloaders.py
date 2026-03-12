"""Domain config hot-reload wiring for instruments-service."""

from __future__ import annotations

import logging

from unified_config_interface import InstrumentDomainConfig
from unified_events_interface import log_event
from unified_trading_library import DomainConfigReloader

logger = logging.getLogger(__name__)
_instrument_reloader: DomainConfigReloader[InstrumentDomainConfig] | None = None

# Module-level snapshot of the latest reloaded subscription list.
# Consumers (e.g. InstrumentsService engine) call get_active_subscription_list()
# to obtain the current live set without holding a reference to the reloader.
_active_subscription_list: list[str] = []
_active_enabled_venues: list[str] = []


def get_active_subscription_list() -> list[str]:
    """Return the latest hot-reloaded instrument subscription list.

    Returns an empty list if the reloader has not yet fired or GCS reload
    is disabled (CONFIG_STORE_BUCKET not set).
    """
    return list(_active_subscription_list)


def get_active_enabled_venues() -> list[str]:
    """Return the latest hot-reloaded enabled venue list."""
    return list(_active_enabled_venues)


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
