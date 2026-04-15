"""Integration sanity test - verifies module structure without network."""

import pytest

from instruments_service.reference_data import create_reference_data_adapter


@pytest.mark.integration
def test_factory_creates_adapter():
    """Verify factory creates adapter for known venue."""
    adapter = create_reference_data_adapter("hyperliquid")
    assert adapter.venue == "HYPERLIQUID"
