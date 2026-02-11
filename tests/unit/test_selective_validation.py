"""Unit tests for selective API key validation."""

from unittest.mock import patch

import pytest

from instruments_service.app.core.selective_validation import validate_required_api_keys


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_validate_single_cefi_venue(mock_get_secret):
    """Test validating API key for single CEFI venue."""
    mock_get_secret.return_value = "test-tardis-key"

    api_keys = validate_required_api_keys(["BINANCE-FUTURES"])

    assert api_keys == {"tardis": "test-tardis-key"}
    assert mock_get_secret.call_count == 1


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_validate_multiple_cefi_venues(mock_get_secret):
    """Test validating API keys for multiple CEFI venues (same data source)."""
    mock_get_secret.return_value = "test-tardis-key"

    api_keys = validate_required_api_keys(["BINANCE-FUTURES", "COINBASE-SPOT"])

    # Only one API key needed (both use tardis)
    assert api_keys == {"tardis": "test-tardis-key"}
    assert mock_get_secret.call_count == 1


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_validate_multiple_data_sources(mock_get_secret):
    """Test validating API keys for multiple data sources."""

    def get_secret_side_effect(project_id, secret_name, fallback_env_var):
        if "tardis" in secret_name.lower():
            return "test-tardis-key"
        elif "databento" in secret_name.lower():
            return "test-databento-key"
        return None

    mock_get_secret.side_effect = get_secret_side_effect

    api_keys = validate_required_api_keys(["BINANCE-FUTURES", "CME"])

    assert api_keys == {
        "tardis": "test-tardis-key",
        "databento": "test-databento-key",
    }
    assert mock_get_secret.call_count == 2


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_no_api_keys_required_for_public_venues(mock_get_secret):
    """Test that public venues (e.g., Aster) require no API keys."""
    api_keys = validate_required_api_keys(["ASTER", "HYPERLIQUID"])

    # No API keys needed
    assert api_keys == {}
    assert mock_get_secret.call_count == 0


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_mixed_public_and_private_venues(mock_get_secret):
    """Test validating mix of public and private venues."""
    mock_get_secret.return_value = "test-tardis-key"

    api_keys = validate_required_api_keys(["BINANCE-FUTURES", "ASTER"])

    # Only tardis key needed (Aster is public)
    assert api_keys == {"tardis": "test-tardis-key"}
    assert mock_get_secret.call_count == 1


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_missing_api_key_raises_error(mock_get_secret):
    """Test that missing API key raises ValueError."""
    mock_get_secret.return_value = None  # Simulate missing key

    with pytest.raises(ValueError, match="API key validation failed"):
        validate_required_api_keys(["BINANCE-FUTURES"])


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_api_key_stripped(mock_get_secret):
    """Test that API keys are stripped of whitespace."""
    mock_get_secret.return_value = "  test-key-with-spaces  \n"

    api_keys = validate_required_api_keys(["BINANCE-FUTURES"])

    assert api_keys == {"tardis": "test-key-with-spaces"}


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_defi_venues_require_thegraph_key(mock_get_secret):
    """Test that DeFi venues require The Graph API key."""
    mock_get_secret.return_value = "test-graph-key"

    api_keys = validate_required_api_keys(["UNISWAP-V3", "AAVE-V3"])

    assert api_keys == {"thegraph": "test-graph-key"}
    assert mock_get_secret.call_count == 1


@patch("instruments_service.app.core.selective_validation.get_secret_with_fallback")
def test_secret_fetch_failure_raises_error(mock_get_secret):
    """Test that secret fetch failure is handled gracefully."""
    mock_get_secret.side_effect = Exception("Secret Manager unavailable")

    with pytest.raises(ValueError, match="API key validation failed"):
        validate_required_api_keys(["BINANCE-FUTURES"])
