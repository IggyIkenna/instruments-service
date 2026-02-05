"""
Tests for CLI handlers __init__.py to increase coverage.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import pytest
from unittest.mock import Mock, patch
from instruments_service.cli.handlers import get_handler_for_mode, register_handler


class TestCLIHandlersInit:
    """Tests for CLI handlers registry."""

    def test_register_handler(self):
        """Test registering a handler."""
        mock_handler_class = Mock()
        register_handler("test-mode", mock_handler_class)

        # Verify handler was registered
        from instruments_service.cli.handlers import _handler_registry

        assert "test-mode" in _handler_registry
        assert _handler_registry["test-mode"] == mock_handler_class

    def test_get_handler_for_mode_instruments(self):
        """Test getting instruments handler."""
        config = {"project_id": "test-project", "tardis_api_key": "test-key"}

        # Clear registry first to ensure fresh state
        from instruments_service.cli.handlers import _handler_registry

        _handler_registry.clear()

        # Mock InstrumentHandler to avoid API key requirements
        with patch("instruments_service.cli.handlers.instrument_handler.InstrumentHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler_class.return_value = mock_handler

            handler = get_handler_for_mode("instruments", config)

            assert handler is not None
            assert "instruments" in _handler_registry

    def test_get_handler_for_mode_unsupported(self):
        """Test getting unsupported mode raises error."""
        config = {"project_id": "test-project", "tardis_api_key": "test-key"}

        # Clear and populate registry first
        from instruments_service.cli.handlers import _handler_registry

        _handler_registry.clear()

        # Mock handler to populate registry
        with patch("instruments_service.cli.handlers.instrument_handler.InstrumentHandler"):
            get_handler_for_mode("instruments", config)  # This populates registry

        with pytest.raises(ValueError, match="Unsupported mode"):
            get_handler_for_mode("unsupported-mode", config)

    def test_get_handler_for_mode_lazy_import(self):
        """Test that handlers are lazily imported."""
        # Clear registry to test lazy import
        from instruments_service.cli.handlers import _handler_registry

        original_registry = _handler_registry.copy()
        _handler_registry.clear()

        config = {"project_id": "test-project"}

        with patch("instruments_service.cli.handlers.instrument_handler.InstrumentHandler") as mock_handler_class:
            mock_handler = Mock()
            mock_handler_class.return_value = mock_handler

            handler = get_handler_for_mode("instruments", config)

            assert handler is not None
            # Verify registry was populated
            assert "instruments" in _handler_registry

        # Restore original registry
        _handler_registry.update(original_registry)
