"""
Base DeFi Adapter for Instruments Service

Provides common functionality for DeFi protocol adapters.
Re-exports the shared BaseDefiAdapter from unified-cloud-services.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Import shared base adapter from unified-cloud-services
from unified_cloud_services.adapters.defi import BaseDefiAdapter as SharedBaseDefiAdapter
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class BaseDefiAdapter(SharedBaseDefiAdapter):
    """
    Base class for DeFi protocol adapters in instruments-service.

    Extends the shared BaseDefiAdapter from unified-cloud-services with
    instruments-service specific configuration.
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize base DeFi adapter.

        Args:
            chain: Chain identifier (e.g., 'ETHEREUM', 'ARBITRUM')
            api_key: Optional API key (uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Use instruments_config for default project_id
        project_id = project_id or instruments_config.gcp_project_id
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)

    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for this protocol.

        Subclasses must implement this method.

        Returns:
            List of instrument definition dictionaries
        """
        raise NotImplementedError("Subclasses must implement get_instrument_metadata()")

    async def download_market_data(
        self,
        instrument: Dict[str, Any],
        date: datetime,
        data_types: List[str],
    ) -> Dict[str, Any]:
        """
        Download market data for an instrument.

        For instruments-service, this returns empty - market data is handled
        by market-tick-data-handler.

        Returns:
            Empty dictionary (not applicable for instruments-service)
        """
        return {}
