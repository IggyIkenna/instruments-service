"""
Main Orchestration Service for Instruments Service.

Coordinates instrument processing, storage, and batch operations.
Follows unified repository structure pattern with modular components.
"""

from __future__ import annotations

import logging

from instruments_service.app.core.instruments_service_processors import InstrumentsServiceProcessors

logger = logging.getLogger(__name__)


class InstrumentsService(InstrumentsServiceProcessors):
    """
    Main orchestration service that combines all instrument processing capabilities.

    This is a thin wrapper that combines:
    - Base functionality (InstrumentsServiceBase)
    - Processing methods (InstrumentsServiceProcessors)

    The modular architecture keeps each component focused and maintainable.
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestration service.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - sink_bucket: Sink bucket name (optional, auto-detected)
                - analytics_dataset: Analytics dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        # Initialize base class which handles all setup
        super().__init__(config)

        logger.info("✅ InstrumentsService initialized with modular architecture")

    def __repr__(self) -> str:
        """String representation of the service."""
        return (
            f"InstrumentsService("
            f"project_id={self.config.get('project_id')}, "
            f"ccxt_enabled={self.config.get('enable_ccxt_integration', True)}, "
            f"caching_enabled={self.config.get('enable_metadata_caching', True)})"
        )
