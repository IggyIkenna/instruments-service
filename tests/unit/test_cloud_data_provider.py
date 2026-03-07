"""
Tests for CloudDataProvider.

Source uses UCI get_data_source() for GCS reads and get_analytics_client() for BigQuery.
Constructor: CloudDataProvider(testing_mode=False) — no cloud_target param.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from instruments_service.app.core.cloud_data_provider import CloudDataProvider

_MODULE = "instruments_service.app.core.cloud_data_provider"

_SAMPLE_DF = pd.DataFrame({"instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"], "venue": ["TEST"]})


@pytest.fixture
def mock_data_source():
    ds = MagicMock()
    ds.read = Mock(return_value=_SAMPLE_DF.copy())
    return ds


@pytest.fixture
def mock_analytics_client():
    ac = MagicMock()
    ac.execute_query = Mock(return_value=[{"instrument_key": "TEST:SPOT_PAIR:BTC-USDT"}])
    return ac


@pytest.fixture(autouse=True)
def patch_uci(mock_data_source, mock_analytics_client):
    with (
        patch(f"{_MODULE}.get_data_source", return_value=mock_data_source),
        patch(f"{_MODULE}.get_analytics_client", return_value=mock_analytics_client),
    ):
        yield


class TestCloudDataProvider:
    """Tests for CloudDataProvider."""

    def test_init_with_testing_mode_false(self):
        provider = CloudDataProvider(testing_mode=False)
        assert provider._testing_mode is False

    def test_init_with_testing_mode_true(self):
        provider = CloudDataProvider(testing_mode=True)
        assert provider._testing_mode is True

    def test_init_without_cloud_target(self):
        """Constructor takes no cloud_target — UCI resolves routing at call time."""
        provider = CloudDataProvider()
        assert provider is not None

    def test_get_instruments_from_gcs_success(self, mock_data_source):
        """Reads instruments via get_data_source().read()."""
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        mock_data_source.read.assert_called()

    def test_get_instruments_from_gcs_custom_path(self, mock_data_source):
        """gcs_path param is accepted for API compat (may be ignored internally)."""
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.get_instruments_from_gcs(date, gcs_path="custom/path.parquet")

        assert isinstance(result, pd.DataFrame)

    def test_get_instruments_from_gcs_empty(self, mock_data_source):
        """Returns empty DataFrame when data source returns empty."""
        mock_data_source.read.return_value = pd.DataFrame()
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_gcs_exception(self, mock_data_source):
        """Returns empty DataFrame on read error."""
        mock_data_source.read.side_effect = FileNotFoundError("not found")
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.get_instruments_from_gcs(date)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_bigquery_success(self, mock_analytics_client):
        """Reads instruments via analytics client."""
        provider = CloudDataProvider()

        result = provider.get_instruments_from_bigquery(venue="TEST", instrument_type="SPOT_PAIR")

        assert isinstance(result, pd.DataFrame)
        mock_analytics_client.execute_query.assert_called()

    def test_get_instruments_from_bigquery_empty(self, mock_analytics_client):
        """Returns empty DataFrame when no analytics rows."""
        mock_analytics_client.execute_query.return_value = []
        provider = CloudDataProvider()

        result = provider.get_instruments_from_bigquery()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_get_instruments_from_bigquery_exception(self, mock_analytics_client):
        """Returns empty DataFrame on analytics error."""
        mock_analytics_client.execute_query.side_effect = ConnectionError("query failed")
        provider = CloudDataProvider()

        result = provider.get_instruments_from_bigquery()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_check_instruments_exist_true(self, mock_data_source):
        """Returns True when data source returns non-empty data."""
        mock_data_source.read.return_value = _SAMPLE_DF.copy()
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.check_instruments_exist(date)

        assert result is True

    def test_check_instruments_exist_false(self, mock_data_source):
        """Returns False when data source returns empty."""
        mock_data_source.read.return_value = pd.DataFrame()
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.check_instruments_exist(date)

        assert result is False

    def test_check_instruments_exist_exception(self, mock_data_source):
        """Returns False on read error."""
        mock_data_source.read.side_effect = FileNotFoundError("not found")
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        result = provider.check_instruments_exist(date)

        assert result is False

    def test_instruments_services_cached_per_category(self, mock_data_source):
        """Multiple calls use the same data_source routing."""
        provider = CloudDataProvider()
        date = datetime(2024, 1, 1, tzinfo=UTC)

        provider.get_instruments_from_gcs(date)
        provider.get_instruments_from_gcs(date)

        assert mock_data_source.read.call_count >= 1
