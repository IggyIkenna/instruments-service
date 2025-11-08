"""
Integration tests for InstrumentsClient.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from datetime import datetime

from instruments_service.clients.instruments_client import InstrumentsClient


@pytest.fixture
def mock_client():
    """Create client with mocked cloud service."""
    with patch('instruments_service.clients.instruments_client.StandardizedDomainCloudService') as mock_cloud:
        client = InstrumentsClient(
            project_id='test-project',
            bucket_name='test-bucket'
        )
        client.cloud_service = mock_cloud
        return client


def test_client_initialization():
    """Test client initialization."""
    with patch('instruments_service.clients.instruments_client.StandardizedDomainCloudService'):
        client = InstrumentsClient(
            project_id='test-project',
            bucket_name='test-bucket'
        )
        assert client.project_id == 'test-project'
        assert client.bucket_name == 'test-bucket'


def test_get_instruments_for_date(mock_client):
    """Test getting instruments for a date."""
    # Mock GCS download
    mock_client.cloud_service.download_from_gcs = Mock(return_value=pd.DataFrame({
        'instrument_key': ['TEST:SPOT_PAIR:BTC-USDT'],
        'venue': ['TEST'],
        'instrument_type': ['SPOT_PAIR'],
        'available_from_datetime': ['2023-05-23T00:00:00Z'],
        'available_to_datetime': [None]
    }))
    
    result = mock_client.get_instruments_for_date(
        date='2023-05-23',
        venue='TEST'
    )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_get_instrument_details(mock_client):
    """Test getting instrument details."""
    # Mock GCS download
    mock_client.cloud_service.download_from_gcs = Mock(return_value=pd.DataFrame({
        'instrument_key': ['TEST:SPOT_PAIR:BTC-USDT'],
        'venue': ['TEST'],
        'instrument_type': ['SPOT_PAIR'],
        'available_from_datetime': ['2023-05-23T00:00:00Z'],
        'available_to_datetime': [None]
    }))
    
    details = mock_client.get_instrument_details(
        date='2023-05-23',
        instrument_id='TEST:SPOT_PAIR:BTC-USDT'
    )
    
    assert details is not None
    assert details['instrument_key'] == 'TEST:SPOT_PAIR:BTC-USDT'


def test_get_summary_stats(mock_client):
    """Test getting summary statistics."""
    # Mock GCS download
    mock_client.cloud_service.download_from_gcs = Mock(return_value=pd.DataFrame({
        'instrument_key': ['TEST:SPOT_PAIR:BTC-USDT', 'TEST:SPOT_PAIR:ETH-USDT'],
        'venue': ['TEST', 'TEST'],
        'instrument_type': ['SPOT_PAIR', 'SPOT_PAIR'],
        'base_asset': ['BTC', 'ETH'],
        'quote_asset': ['USDT', 'USDT'],
        'ccxt_symbol': ['BTC/USDT', 'ETH/USDT'],  # Add required ccxt_symbol column
        'data_types': ['trades,book_snapshot_5', 'trades,book_snapshot_5'],
        'available_from_datetime': ['2023-05-23T00:00:00Z', '2023-05-23T00:00:00Z'],
        'available_to_datetime': [None, None]
    }))
    
    stats = mock_client.get_summary_stats('2023-05-23')
    
    assert stats['total_instruments'] == 2
    assert stats['venues'] == 1
    assert stats['instrument_types'] == 1


