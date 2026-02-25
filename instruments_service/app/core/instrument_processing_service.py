"""
Instrument Processing Service

Centralizes ALL instrument operations (canonical key generation, CCXT integration, metadata)
Split into modular components for better maintainability.

This service combines:
- Base processing functionality
- Specialized mixins for different integrations
- High-level processing handlers
"""

from __future__ import annotations

import logging
from typing import Any

from instruments_service.app.core.instrument_processing_base import (
    InstrumentProcessingBase,
)
from instruments_service.app.core.instrument_processing_handlers import (
    InstrumentProcessingHandlers,
)
from instruments_service.app.core.instrument_processing_mixins import (
    CCXTIntegrationMixin,
    DatabentoIntegrationMixin,
    DefiIntegrationMixin,
    SymbolProcessingMixin,
    TardisIntegrationMixin,
)
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class InstrumentProcessingService(
    InstrumentProcessingBase,
    TardisIntegrationMixin,
    DatabentoIntegrationMixin,
    DefiIntegrationMixin,
    CCXTIntegrationMixin,
    SymbolProcessingMixin,
    InstrumentProcessingHandlers,
):
    """
    Centralized instrument processing service.

    Combines modular components to provide complete instrument processing
    functionality with clean separation of concerns.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize instrument processing service.

        Args:
            config: Configuration with instrument processing settings.
        """
        # Initialize base functionality first
        InstrumentProcessingBase.__init__(self, config)

        # Store project ID for mixins
        self._tardis_project_id = str(config.get("project_id") or instruments_config.gcp_project_id)

        # Initialize all mixin components
        self.__init_defi_integration()
        self.__init_ccxt_integration(config)
        self.__init_symbol_processing()

        # Initialize tardis adapter as None (lazy-loaded)
        self.tardis_adapter = None

        logger.info(
            f"✅ InstrumentProcessingService initialized with modular architecture: "
            f"api_key={'*' * (len(self.api_key) - 4) + self.api_key[-4:] if self.api_key else 'None'}, "
            f"ccxt_integration={self.processing_config.enable_ccxt_integration}, "
            f"caching={self.processing_config.enable_metadata_caching}"
        )
