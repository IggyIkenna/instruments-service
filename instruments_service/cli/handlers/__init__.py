"""
CLI Handlers

Registry for CLI mode handlers.
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Import handlers (lazy to avoid circular imports)
_handler_registry = {}


def register_handler(mode: str, handler_class):
    """Register a handler for a specific mode."""
    _handler_registry[mode] = handler_class
    logger.debug(f"Registered handler for mode: {mode}")


def get_handler_for_mode(mode: str, config: Dict[str, Any]):
    """
    Get handler instance for a specific mode.
    
    Args:
        mode: Operation mode (e.g., 'instruments', 'instruments-query')
        config: Configuration dictionary
        
    Returns:
        Handler instance
        
    Raises:
        ValueError: If mode is not supported
    """
    # Lazy import to avoid circular dependencies
    if not _handler_registry:
        from .instrument_handler import InstrumentHandler
        from .instruments_query_handler import InstrumentsQueryHandler
        
        register_handler('instruments', InstrumentHandler)
        register_handler('instruments-query', InstrumentsQueryHandler)
    
    if mode not in _handler_registry:
        raise ValueError(f"Unsupported mode: {mode}. Supported modes: {list(_handler_registry.keys())}")
    
    handler_class = _handler_registry[mode]
    return handler_class(config)


__all__ = ['get_handler_for_mode', 'register_handler']



