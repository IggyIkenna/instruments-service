"""
Integration tests for CLI handlers.

These tests use real services when credentials are available.
"""

import pytest
import os
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

from instruments_service.cli.handlers.instrument_handler import InstrumentHandler
from instruments_service.cli.handlers.instruments_query_handler import (
    InstrumentsQueryHandler,
)


@pytest.fixture
def config():
    """Configuration for handlers - uses real project if available."""
    # Use real project ID from environment or default
    project_id = os.getenv("GCP_PROJECT_ID", "central-element-323112")
    return {
        "project_id": project_id,
    }


@pytest.fixture
def mock_instrument_handler(config):
    """Create instrument handler - uses real services."""
    handler = InstrumentHandler(config)
    return handler


@pytest.fixture
def mock_query_handler(config):
    """Create query handler - uses real services."""
    handler = InstrumentsQueryHandler(config)
    return handler


def test_instrument_handler_initialization(config):
    """Test instrument handler initialization with real services."""
    handler = InstrumentHandler(config)
    assert handler.config == config
    assert handler.instruments_service is not None
    assert handler.cloud_storage is not None


def test_query_handler_initialization(config):
    """Test query handler initialization with real services."""
    handler = InstrumentsQueryHandler(config)
    assert handler.config == config
    assert handler.client is not None


@pytest.mark.skipif(
    not os.getenv("GCP_PROJECT_ID") and not os.path.exists(
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    ),
    reason="Requires GCP credentials for real service testing"
)
def test_instrument_handler_run(mock_instrument_handler):
    """Test instrument handler run method with real services."""
    # Use a past date to avoid future date skipping
    from datetime import datetime, timedelta, timezone
    past_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    
    result = mock_instrument_handler.run(
        start_date=past_date, end_date=past_date, force=False
    )

    assert result["status"] in ["success", "partial", "warning", "skipped"]
    assert "instruments_generated" in result or "dates_skipped" in result


@pytest.mark.skipif(
    not os.getenv("GCP_PROJECT_ID") and not os.path.exists(
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    ),
    reason="Requires GCP credentials for real service testing"
)
def test_query_handler_list_query(config):
    """Test query handler list query with real services."""
    handler = InstrumentsQueryHandler(config)
    
    # Use a past date that likely has data
    from datetime import datetime, timedelta, timezone
    past_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    result = handler.run(
        start_date=past_date, end_date=past_date, query_type="list"
    )

    assert result["status"] in ["success", "warning"]
    assert result["query_type"] == "list"


@pytest.mark.skipif(
    not os.getenv("GCP_PROJECT_ID") and not os.path.exists(
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    ),
    reason="Requires GCP credentials for real service testing"
)
def test_query_handler_summary_query(config):
    """Test query handler summary query with real services."""
    handler = InstrumentsQueryHandler(config)
    
    # Use a past date that likely has data
    from datetime import datetime, timedelta, timezone
    past_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    result = handler.run(start_date=past_date, query_type="summary")

    assert result["status"] in ["success", "warning"]
    assert result["query_type"] == "summary"
    if result["status"] == "success":
        assert "results" in result
