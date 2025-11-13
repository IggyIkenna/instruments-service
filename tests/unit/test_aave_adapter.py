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
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__"
        ) as mock_base_init:
            # BaseDefiAdapter.__init__ sets self.chain, self.project_id
            def init_side_effect(self_obj, chain, api_key=None, project_id=None):
                self_obj.chain = chain
                self_obj.project_id = project_id or "test-project"
            mock_base_init.side_effect = init_side_effect
            adapter = AaveV3Adapter(chain="ETHEREUM")
            assert adapter.venue == "AAVE_V3_ETH"

    def test_init_unsupported_chain(self):
        """Test initialization for unsupported chain."""
        with patch(
            "instruments_service.app.venues.defi.aave_adapter.BaseDefiAdapter.__init__"
        ) as mock_base_init:
            def init_side_effect(self_obj, chain, api_key=None, project_id=None):
                self_obj.chain = chain
                self_obj.project_id = project_id or "test-project"
            mock_base_init.side_effect = init_side_effect
            # AaveV3Adapter doesn't raise ValueError, it creates venue name from chain
            adapter = AaveV3Adapter(chain="UNSUPPORTED")
            assert adapter.venue == "AAVE_V3_UNSUPPORTED"

