"""
Orchestrator Base Module

Provides core orchestration functionality and initialization.
Split from orchestrator.py for better maintainability.
"""

from __future__ import annotations

import logging
from typing import cast

from unified_market_interface import VenueMapping

from instruments_service.adapters import StorageAdapter
from instruments_service.config import instruments_config
from instruments_service.engine.operations.instruments.batch_orchestrator import InstrumentBatchProcessor

logger = logging.getLogger(__name__)


class OrchestratorBase:
    """
    Base orchestrator with core initialization and configuration.

    Provides foundational services and configuration for orchestration.
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestrator base.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - gcs_bucket: GCS bucket name (optional, auto-detected)
                - bigquery_dataset: BigQuery dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        # Initialize processing service - lazy import to avoid circular dependency
        from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

        processing_config: dict[str, object] = {
            "project_id": str(cast(str | None, config.get("project_id")) or instruments_config.gcp_project_id),
            "enable_ccxt_integration": cast(bool, config.get("enable_ccxt_integration", True)),
            "enable_metadata_caching": cast(bool, config.get("enable_metadata_caching", True)),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = StorageAdapter()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping
        self.venue_mapping: VenueMapping = VenueMapping()

        logger.info("✅ OrchestratorBase initialized")

    def get_processing_stats(self) -> dict[str, object]:
        """Get processing statistics."""
        return {
            "processing_service": self.processing_service.get_processing_stats(),
            "batch_processor": {
                "max_batch_size": self.batch_processor.max_batch_size,
                "lookback_days": self.batch_processor.lookback_days,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "processing_service"):
            self.processing_service.cleanup()
        logger.info("🧹 Orchestrator cleanup completed")
