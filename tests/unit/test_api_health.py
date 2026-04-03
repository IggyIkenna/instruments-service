"""Tests for the instruments-service health API endpoint."""

from __future__ import annotations

from datetime import date


def test_health_router_importable() -> None:
    """api/main.py is importable and exports expected symbols."""
    from instruments_service.api.main import app, create_app

    assert app is not None
    assert callable(create_app)


def test_data_freshness_no_processed_date() -> None:
    """data_freshness returns stale when no date has been processed."""
    from instruments_service.api.main import _data_freshness

    result = _data_freshness()
    assert isinstance(result, dict)
    assert "stale" in result


def test_data_freshness_with_processed_date() -> None:
    """data_freshness returns non-stale when a date has been processed."""
    from instruments_service.api import main as api_main

    old = api_main._last_processed_date
    try:
        api_main._last_processed_date = date(2026, 3, 22)
        result = api_main._data_freshness()
        assert result["stale"] is False
        assert result["last_processed_date"] == "2026-03-22"
    finally:
        api_main._last_processed_date = old


def test_set_last_processed_date() -> None:
    """set_last_processed_date updates the module-level state."""
    from instruments_service.api import main as api_main
    from instruments_service.api.main import set_last_processed_date

    old = api_main._last_processed_date
    try:
        set_last_processed_date(date(2026, 3, 15))
        assert api_main._last_processed_date == date(2026, 3, 15)
    finally:
        api_main._last_processed_date = old
