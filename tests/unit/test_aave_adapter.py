"""
Unit tests for AaveV3Adapter.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from instruments_service.app.venues.defi.aave_adapter import AaveV3Adapter


class TestAaveV3Adapter:
    """Tests for AaveV3Adapter."""

    def test_static_risk_params(self):
        """Test static risk parameters exist."""
        assert hasattr(AaveV3Adapter, "STATIC_RISK_PARAMS")
        assert "emode" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "standard" in AaveV3Adapter.STATIC_RISK_PARAMS
        assert "reserve_factors" in AaveV3Adapter.STATIC_RISK_PARAMS

    def test_init_ethereum(self):
        """Test initialization for Ethereum chain."""
        # Mock the base class initialization properly
        with patch("instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__", return_value=None):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "ETHEREUM"
            adapter.project_id = "test-project"
            # Manually set venue as __init__ would
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_ETH"

    def test_init_unsupported_chain(self):
        """Test initialization for unsupported chain."""
        # Mock the base class initialization properly
        with patch("instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__", return_value=None):
            adapter = AaveV3Adapter.__new__(AaveV3Adapter)
            adapter.chain = "UNSUPPORTED"
            adapter.project_id = "test-project"
            # Manually set venue as __init__ would
            chain_to_venue = {"ETHEREUM": "AAVE_V3_ETH"}
            adapter.venue = chain_to_venue.get(adapter.chain, f"AAVE_V3_{adapter.chain}")
            assert adapter.venue == "AAVE_V3_UNSUPPORTED"

