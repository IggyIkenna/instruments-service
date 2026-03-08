"""Domain config hot-reload wiring for instruments-service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unified_config_interface import InstrumentDomainConfig
    from unified_trading_library import DomainConfigReloader

logger = logging.getLogger(__name__)
_instrument_reloader: DomainConfigReloader[InstrumentDomainConfig] | None = None


def _on_instruments_reload(config: InstrumentDomainConfig) -> None:
    logger.info(
        "Instruments domain config reloaded: %d instruments, %d venues",
        len(config.subscription_list),
        len(config.enabled_venues),
    )
    # TODO(GH-BACKLOG): Hook into InstrumentsService subscription list update when live reload is wired


def start_domain_config_reloaders(service_config: object) -> None:
    from unified_config_interface import InstrumentDomainConfig
    from unified_trading_library import DomainConfigReloader

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
