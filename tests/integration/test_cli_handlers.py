"""
Integration tests for CLI handlers.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

from instruments_service.cli.handlers.instrument_handler import InstrumentHandler
from instruments_service.cli.handlers.instruments_query_handler import InstrumentsQueryHandler


@pytest.fixture
def mock_config():
    """Mock configuration for handlers."""
    return {
        'project_id': 'test-project',
        'gcs_bucket': 'test-bucket',
        'bigquery_dataset': 'test_dataset'
    }


@pytest.fixture
def mock_instrument_handler(mock_config):
    """Create instrument handler with mocked dependencies."""
    with patch('instruments_service.cli.handlers.instrument_handler.InstrumentProcessingService'), \
         patch('instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage'):
        handler = InstrumentHandler(mock_config)
        return handler


@pytest.fixture
def mock_query_handler(mock_config):
    """Create query handler with mocked dependencies."""
    # Patch InstrumentsClient at the module where it's imported (clients module)
    with patch('instruments_service.clients.instruments_client.InstrumentsClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        handler = InstrumentsQueryHandler(mock_config)
        handler.client = mock_client  # Set the mocked client
        return handler


def test_instrument_handler_initialization(mock_config):
    """Test instrument handler initialization."""
    with patch('instruments_service.cli.handlers.instrument_handler.InstrumentProcessingService'), \
         patch('instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage'):
        handler = InstrumentHandler(mock_config)
        assert handler.config == mock_config


def test_query_handler_initialization(mock_config):
    """Test query handler initialization."""
    # Patch InstrumentsClient at the module where it's imported (clients module)
    with patch('instruments_service.clients.instruments_client.InstrumentsClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        handler = InstrumentsQueryHandler(mock_config)
        assert handler.config == mock_config
        assert handler.client is not None


def test_instrument_handler_run(mock_instrument_handler):
    """Test instrument handler run method."""
    # Mock the generation method
    mock_instrument_handler._generate_instruments_for_date = Mock(return_value={
        'TEST:SPOT_PAIR:BTC-USDT': Mock(model_dump=lambda: {'instrument_key': 'TEST:SPOT_PAIR:BTC-USDT'})
    })
    mock_instrument_handler.cloud_storage.store_instruments = Mock(return_value=True)
    
    result = mock_instrument_handler.run(
        start_date='2023-05-23',
        end_date='2023-05-23',
        force=False
    )
    
    assert result['status'] in ['success', 'partial', 'warning']
    assert 'instruments_generated' in result


def test_query_handler_list_query(mock_config):
    """Test query handler list query."""
    import pandas as pd
    
    # Patch InstrumentsClient at the module where it's imported
    with patch('instruments_service.clients.instruments_client.InstrumentsClient') as mock_client_class:
        mock_client = Mock()
        mock_client.get_instruments_for_date = Mock(return_value=pd.DataFrame({
            'instrument_key': ['TEST:SPOT_PAIR:BTC-USDT'],
            'venue': ['TEST'],
            'instrument_type': ['SPOT_PAIR']
        }))
        mock_client_class.return_value = mock_client
        
        handler = InstrumentsQueryHandler(mock_config)
        handler.client = mock_client
        
        result = handler.run(
            start_date='2023-05-23',
            end_date='2023-05-23',
            query_type='list'
        )
        
        assert result['status'] == 'success'
        assert result['query_type'] == 'list'


def test_query_handler_summary_query(mock_config):
    """Test query handler summary query."""
    # Patch InstrumentsClient at the module where it's imported
    with patch('instruments_service.clients.instruments_client.InstrumentsClient') as mock_client_class:
        mock_client = Mock()
        mock_client.get_summary_stats = Mock(return_value={
            'total_instruments': 100,
            'venues': 5,
            'instrument_types': 4
        })
        mock_client_class.return_value = mock_client
        
        handler = InstrumentsQueryHandler(mock_config)
        handler.client = mock_client
        
        result = handler.run(
            start_date='2023-05-23',
            query_type='summary'
        )
        
        assert result['status'] == 'success'
        assert result['query_type'] == 'summary'
        assert result['results']['total_instruments'] == 100


