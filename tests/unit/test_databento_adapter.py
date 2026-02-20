"""
Unit tests for Databento adapter loading (Task 1.4.4).

Tests that TRADFI venues load DatabentoAdapter via adapter_loader.
"""

import pytest

from instruments_service.app.core.adapter_loader import (
    clear_adapter_cache,
    get_adapter_for_venue,
)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear adapter cache before each test."""
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def test_get_adapter_for_tradfi_venue_loads_databento():
    """Test loading adapter for TRADFI venue returns DatabentoAdapter."""
    # CME, XNAS, etc. map to databento data source (DataSourceMapping)
    adapter = get_adapter_for_venue("CME")
    assert adapter is not None
    assert "DatabentoAdapter" in str(type(adapter))


def test_databento_adapter_caching():
    """Test that Databento adapters are cached (singleton per data source)."""
    adapter1 = get_adapter_for_venue("CME")
    adapter2 = get_adapter_for_venue("XNAS")  # Same data source (databento)

    # Should return same instance (cached)
    assert adapter1 is adapter2


def test_databento_vs_tardis_different_adapters():
    """Test that Databento and Tardis adapters are different."""
    databento_adapter = get_adapter_for_venue("CME")
    tardis_adapter = get_adapter_for_venue("BINANCE-FUTURES")

    assert databento_adapter is not tardis_adapter
    assert "DatabentoAdapter" in str(type(databento_adapter))
    assert "TardisAdapter" in str(type(tardis_adapter))
