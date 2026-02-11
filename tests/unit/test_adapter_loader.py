"""Unit tests for lazy adapter loading."""

import pytest

from instruments_service.app.core.adapter_loader import (
    clear_adapter_cache,
    get_adapter_for_venue,
    get_cached_adapters,
)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear adapter cache before each test."""
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def test_get_adapter_for_cefi_venue():
    """Test loading adapter for CEFI venue."""
    adapter = get_adapter_for_venue("BINANCE-FUTURES")
    assert adapter is not None
    assert "TardisAdapter" in str(type(adapter))


def test_get_adapter_for_onchain_perp_venue():
    """Test loading adapter for on-chain perp venue."""
    adapter = get_adapter_for_venue("ASTER")
    assert adapter is not None
    assert "AsterAdapter" in str(type(adapter))


def test_get_adapter_for_defi_venue():
    """Test loading adapter for DeFi venue."""
    adapter = get_adapter_for_venue("UNISWAP-V3")
    assert adapter is not None
    assert "UniswapV3Adapter" in str(type(adapter))


def test_adapter_caching():
    """Test that adapters are cached (singleton per data source)."""
    adapter1 = get_adapter_for_venue("BINANCE-FUTURES")
    adapter2 = get_adapter_for_venue("COINBASE-SPOT")  # Same data source (tardis)

    # Should return same instance (cached)
    assert adapter1 is adapter2


def test_different_data_sources_different_adapters():
    """Test that different data sources get different adapters."""
    tardis_adapter = get_adapter_for_venue("BINANCE-FUTURES")
    aster_adapter = get_adapter_for_venue("ASTER")

    assert tardis_adapter is not aster_adapter


def test_unknown_venue_raises_error():
    """Test that unknown venue raises ValueError."""
    with pytest.raises(ValueError, match="Unknown venue"):
        get_adapter_for_venue("UNKNOWN_VENUE")


def test_get_cached_adapters():
    """Test getting cached adapters."""
    # Initially empty
    assert get_cached_adapters() == {}

    # Load some adapters
    get_adapter_for_venue("BINANCE-FUTURES")
    get_adapter_for_venue("ASTER")

    cached = get_cached_adapters()
    assert "tardis" in cached
    assert "aster" in cached
    assert len(cached) == 2


def test_clear_adapter_cache():
    """Test clearing adapter cache."""
    # Load adapter
    get_adapter_for_venue("BINANCE-FUTURES")
    assert len(get_cached_adapters()) == 1

    # Clear cache
    clear_adapter_cache()
    assert get_cached_adapters() == {}

    # Reload creates new instance
    adapter_new = get_adapter_for_venue("BINANCE-FUTURES")
    assert adapter_new is not None


def test_defi_venue_specific_adapters():
    """Test that different DeFi venues load their specific adapters."""
    uniswap_v2 = get_adapter_for_venue("UNISWAP-V2")
    uniswap_v3 = get_adapter_for_venue("UNISWAP-V3")
    aave = get_adapter_for_venue("AAVE-V3")

    assert "UniswapV2Adapter" in str(type(uniswap_v2))
    assert "UniswapV3Adapter" in str(type(uniswap_v3))
    assert "AaveV3Adapter" in str(type(aave))


def test_case_insensitive_venue_names():
    """Test that venue names are case-insensitive."""
    adapter1 = get_adapter_for_venue("binance-futures")
    adapter2 = get_adapter_for_venue("BINANCE-FUTURES")

    # Should return same cached instance
    assert adapter1 is adapter2
