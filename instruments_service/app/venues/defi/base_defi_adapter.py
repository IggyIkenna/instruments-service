"""
Base DeFi Adapter

Provides common functionality for DeFi protocol adapters.
"""

import logging
from typing import Dict, Optional, Any

from instruments_service.settings import env_configs

logger = logging.getLogger(__name__)


class BaseDefiAdapter:
    """
    Base class for DeFi protocol adapters.

    Provides common initialization and validation methods.
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
        self.chain = chain.upper()
        self.api_key = api_key
        # Default to GCP_PROJECT_ID env var if not provided
        self.project_id = project_id or env_configs.gcp_project_id

    def _validate_instrument_definition(self, inst_def: Dict[str, Any]) -> bool:
        """
        Validate instrument definition has required fields.

        Args:
            inst_def: Instrument definition dictionary

        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            "instrument_key",
            "venue",
            "instrument_type",
            "base_asset",
            "quote_asset",
        ]

        for field in required_fields:
            if field not in inst_def or not inst_def[field]:
                logger.warning(f"Missing required field '{field}' in instrument definition")
                return False

        return True
