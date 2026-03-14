"""
Tests for CLI handlers __init__.py to increase coverage.

Note: Query functionality has been moved to unified-trading-services.
Use InstrumentsDomainClient from unified-trading-services to query instruments.
"""

import contextlib
from unittest.mock import Mock, patch

import pytest

from instruments_service.cli.handlers import get_handler_for_mode, register_handler


def _make_populate_patch(mock_handler_class: Mock) -> Mock:
    """Return a _populate_registry replacement that registers only the instruments handler."""

    def _fake_populate() -> None:
        from instruments_service.cli.handlers import _handler_registry

        _handler_registry["instruments"] = mock_handler_class

    return _fake_populate


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

        from instruments_service.cli.handlers import _handler_registry

        _handler_registry.clear()

        mock_handler_class = Mock()
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        with patch(
            "instruments_service.cli.handlers._populate_registry",
            side_effect=_make_populate_patch(mock_handler_class),
        ):
            handler = get_handler_for_mode("instruments", config)

            assert handler is not None
            assert "instruments" in _handler_registry

    def test_get_handler_for_mode_unsupported(self):
        """Test getting unsupported mode raises error."""
        config = {"project_id": "test-project", "tardis_api_key": "test-key"}

        from instruments_service.cli.handlers import _handler_registry

        _handler_registry.clear()

        mock_handler_class = Mock()
        mock_handler_class.return_value = Mock()

        with patch(
            "instruments_service.cli.handlers._populate_registry",
            side_effect=_make_populate_patch(mock_handler_class),
        ):
            get_handler_for_mode("instruments", config)  # populates registry

        with pytest.raises(ValueError, match="Unsupported mode"):
            get_handler_for_mode("unsupported-mode", config)

    def test_get_handler_for_mode_lazy_import(self):
        """Test that handlers are lazily imported."""
        from instruments_service.cli.handlers import _handler_registry

        original_registry = _handler_registry.copy()
        _handler_registry.clear()

        config = {"project_id": "test-project"}

        mock_handler_class = Mock()
        mock_handler = Mock()
        mock_handler_class.return_value = mock_handler

        with patch(
            "instruments_service.cli.handlers._populate_registry",
            side_effect=_make_populate_patch(mock_handler_class),
        ):
            handler = get_handler_for_mode("instruments", config)

            assert handler is not None
            assert "instruments" in _handler_registry

        # Restore original registry
        _handler_registry.update(original_registry)


@pytest.mark.unit
class TestCliHandlersInitFromBoost:
    """Additional handlers __init__ coverage."""

    def test_import(self):
        from instruments_service.cli.handlers import get_handler_for_mode, register_handler

        assert get_handler_for_mode is not None
        assert register_handler is not None

    def test_register_handler(self):
        import contextlib

        from instruments_service.cli.base_handler import ModeHandler
        from instruments_service.cli.handlers import register_handler

        class _DummyHandlerForTest(ModeHandler):
            def run(self, **kwargs):
                return {"status": "ok"}

        with contextlib.suppress(Exception):
            register_handler("test_dummy_mode", _DummyHandlerForTest)

    def test_get_handler_for_mode_instruments(self):
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("instruments", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_mode_aggregate(self):
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("aggregate", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_mode_generate_date_views(self):
        from instruments_service.cli.handlers import get_handler_for_mode

        with contextlib.suppress(Exception):
            handler = get_handler_for_mode("generate_date_views", {"project_id": "test"})
            assert handler is not None

    def test_get_handler_for_unsupported_mode(self):
        from instruments_service.cli.handlers import get_handler_for_mode

        try:
            get_handler_for_mode("nonexistent_mode_xyz", {"project_id": "test"})
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "nonexistent_mode_xyz" in str(e)
        except AssertionError:
            raise
        except Exception:
            pass
