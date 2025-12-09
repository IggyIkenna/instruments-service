"""
CLI Handlers

Registry for CLI mode handlers.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

from typing import Dict, Any
import logging
from .instrument_handler import InstrumentHandler
from instruments_service.cli.base_handler import ModeHandler

logger = logging.getLogger(__name__)

# Import handlers (lazy to avoid circular imports)
_handler_registry = {}


def register_handler(mode: str, handler_class):
    """Register a handler for a specific mode."""
    _handler_registry[mode] = handler_class
    logger.debug(f"Registered handler for mode: {mode}")


def get_handler_for_mode(mode: str, config: Dict[str, Any]) -> ModeHandler:
    """
    Get handler instance for a specific mode.

    Args:
        mode: Operation mode (e.g., 'instruments')
        config: Configuration dictionary

    Returns:
        Handler instance

    Raises:
        ValueError: If mode is not supported
    """
    if not _handler_registry:
        try:
            register_handler("instruments", InstrumentHandler)
            logger.debug(f"Registered 'instruments' handler: {InstrumentHandler}")
            logger.debug(f"Final registry: {_handler_registry}")
        except Exception as e:
            logger.error(f"Error registering handlers: {e}", exc_info=True)
            raise

    if mode not in _handler_registry:
        raise ValueError(
            f"Unsupported mode: {mode}. Supported modes: {list(_handler_registry.keys())}"
        )

    handler_class = _handler_registry[mode]
    return handler_class(config)


__all__ = ["get_handler_for_mode", "register_handler"]
