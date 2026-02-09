"""
Integration tests for CLI handlers.

These tests use real services when credentials are available.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from instruments_service.cli.handlers.instrument_handler import InstrumentHandler
from instruments_service.config import instruments_config


@pytest.fixture
def config():
    """Configuration for handlers - uses real project from config."""
    return {
        "project_id": instruments_config.gcp_project_id,
    }


@pytest.fixture
def mock_instrument_handler(config):
    """Create instrument handler - uses real services."""
    handler = InstrumentHandler(config)
    return handler


def test_instrument_handler_initialization(config):
    """Test instrument handler initialization with real services."""
    handler = InstrumentHandler(config)
    assert handler.config == config
    assert handler.instruments_service is not None
    assert handler.cloud_storage is not None


@pytest.mark.live
@pytest.mark.skipif(
    os.environ.get("CLOUD_MOCK_MODE") == "true",
    reason="Skipping live service test in Cloud Build (CLOUD_MOCK_MODE=true)",
)
@pytest.mark.skipif(
    not instruments_config.gcp_project_id
    and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
    reason="Requires GCP credentials for real service testing",
)
def test_instrument_handler_run(mock_instrument_handler):
    """Test instrument handler run method with real services (skipped in Cloud Build)."""
    # Use a past date to avoid future date skipping
    past_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    result = mock_instrument_handler.run(start_date=past_date, end_date=past_date, force=False)

    assert result["status"] in ["success", "partial", "warning", "skipped"]
    assert "instruments_generated" in result or "dates_skipped" in result


@pytest.mark.live
@pytest.mark.skipif(
    os.environ.get("CLOUD_MOCK_MODE") == "true",
    reason="Skipping live service test in Cloud Build (CLOUD_MOCK_MODE=true)",
)
@pytest.mark.skipif(
    not instruments_config.gcp_project_id
    and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
    reason="Requires GCP credentials for real service testing",
)
def test_instrument_handler_run_with_categories(mock_instrument_handler):
    """Test instrument handler run method with market categories (skipped in Cloud Build)."""
    past_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    result = mock_instrument_handler.run(
        start_date=past_date,
        end_date=past_date,
        force=False,
        cefi=True,
        tradfi=False,
        defi=False,
    )

    assert result["status"] in ["success", "partial", "warning", "skipped"]
