"""
CLI Handlers

Registry for CLI mode handlers.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import logging
from typing import Any

from instruments_service.cli.base_handler import ModeHandler

from .corporate_actions_backfill_handler import CorporateActionsBackfillHandler
from .corporate_actions_handler import CorporateActionsHandler
from .corporate_actions_production_handler import CorporateActionsProductionHandler
from .corporate_actions_update_handler import CorporateActionsUpdateHandler
from .generate_date_views_handler import GenerateDateViewsHandler
from .instrument_handler import InstrumentHandler

logger = logging.getLogger(__name__)

# Import handlers (lazy to avoid circular imports)
_handler_registry: dict[str, type[ModeHandler]] = {}


def register_handler(mode: str, handler_class: type[ModeHandler]) -> None:
    """Register a handler for a specific mode."""
    _handler_registry[mode] = handler_class
    logger.debug(f"Registered handler for mode: {mode}")


def get_handler_for_mode(mode: str, config: dict[str, Any]) -> ModeHandler:
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
            register_handler("instruments", InstrumentHandler)
            register_handler("corporate_actions", CorporateActionsHandler)
            register_handler("corporate_actions_backfill", CorporateActionsBackfillHandler)
            register_handler("generate_date_views", GenerateDateViewsHandler)
            register_handler("corporate_actions_update", CorporateActionsUpdateHandler)
            register_handler("corporate_actions_production", CorporateActionsProductionHandler)
            logger.debug(f"Registered 'instruments' handler: {InstrumentHandler}")
            logger.debug(f"Registered 'corporate_actions' handler: {CorporateActionsHandler}")
            logger.debug(f"Registered 'corporate_actions_backfill' handler: {CorporateActionsBackfillHandler}")
            logger.debug(f"Registered 'generate_date_views' handler: {GenerateDateViewsHandler}")
            logger.debug(f"Registered 'corporate_actions_update' handler: {CorporateActionsUpdateHandler}")
            logger.debug(f"Registered 'corporate_actions_production' handler: {CorporateActionsProductionHandler}")
            logger.debug(f"Final registry: {_handler_registry}")
        except Exception as e:
            logger.error(f"Error registering handlers: {e}", exc_info=True)
            raise

    if mode not in _handler_registry:
        raise ValueError(f"Unsupported mode: {mode}. Supported modes: {list(_handler_registry.keys())}")

    handler_class = _handler_registry[mode]
    return handler_class(config)


__all__ = ["get_handler_for_mode", "register_handler"]
