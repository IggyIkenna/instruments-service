"""
CLI Handlers

Registry for CLI mode handlers.

Note: Query functionality has been moved to unified-trading-library.
Use InstrumentsDomainClient from unified-trading-library to query instruments.

All handler imports are lazy (inside get_handler_for_mode) to avoid
pulling in heavy dependencies like unified_market_interface at
import time, which breaks test collection for unrelated handler tests.
"""

import logging

from instruments_service.cli.base_handler import ModeHandler

logger = logging.getLogger(__name__)

_handler_registry: dict[str, type[ModeHandler]] = {}


def register_handler(mode: str, handler_class: type[ModeHandler]) -> None:
    """Register a handler for a specific mode."""
    _handler_registry[mode] = handler_class
    logger.debug("Registered handler for mode: %s", mode)


def _populate_registry() -> None:
    """Lazily import and register all handlers on first use."""
    from .aggregate_handler import AggregateHandler
    from .corporate_actions_backfill_handler import CorporateActionsBackfillHandler
    from .corporate_actions_handler import CorporateActionsHandler
    from .corporate_actions_production_handler import CorporateActionsProductionHandler
    from .corporate_actions_update_handler import CorporateActionsUpdateHandler
    from .generate_date_views_handler import GenerateDateViewsHandler
    from .instrument_handler import InstrumentHandler

    register_handler("aggregate", AggregateHandler)
    register_handler("instruments", InstrumentHandler)
    register_handler("corporate_actions", CorporateActionsHandler)
    register_handler("corporate_actions_backfill", CorporateActionsBackfillHandler)
    register_handler("generate_date_views", GenerateDateViewsHandler)
    register_handler("corporate_actions_update", CorporateActionsUpdateHandler)
    register_handler("corporate_actions_production", CorporateActionsProductionHandler)
    logger.debug("Final registry: %s", _handler_registry)


def get_handler_for_mode(mode: str, config: dict[str, object]) -> ModeHandler:
    """
    Get handler instance for a specific mode.

    Args:
        mode: Operation mode (e.g., 'instruments', 'corporate_actions', 'corporate_actions_production', etc.)
        config: Configuration dictionary

    Returns:
        Handler instance

    Raises:
        ValueError: If mode is not supported
    """
    if not _handler_registry:
        try:
            _populate_registry()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Error registering handlers: %s", e, exc_info=True)
            raise

    if mode not in _handler_registry:
        raise ValueError(f"Unsupported mode: {mode}. Supported modes: {list(_handler_registry.keys())}")

    handler_class = _handler_registry[mode]
    return handler_class(config)


__all__ = ["get_handler_for_mode", "register_handler"]
