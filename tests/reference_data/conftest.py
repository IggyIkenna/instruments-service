"""Shared fixtures for unified-reference-data-interface tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter
from instruments_service.reference_data.adapters.deribit import DeribitReferenceDataAdapter
from instruments_service.reference_data.adapters.ibkr import IBKRReferenceDataAdapter


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: mark test as integration test (requires external services)")


@pytest.fixture(scope="session")
def binance_adapter() -> BinanceReferenceDataAdapter:
    return BinanceReferenceDataAdapter()


@pytest.fixture(scope="session")
def deribit_adapter() -> DeribitReferenceDataAdapter:
    return DeribitReferenceDataAdapter()


@pytest.fixture
def mock_ib() -> MagicMock:
    """MagicMock(spec=IB) fixture for IBKR adapter unit tests.

    Canonical inject-IB pattern: mock at the ib_insync.IB object level.
    HTTP VCR is not applicable to TWS binary socket protocol.

    Usage:
        def test_something(mock_ib):
            mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[...])
            adapter = IBKRReferenceDataAdapter(ib=mock_ib)
    """
    from ib_insync import IB

    ib = MagicMock(spec=IB)
    ib.reqContractDetailsAsync = AsyncMock(return_value=[])
    ib.reqMatchingSymbolsAsync = AsyncMock(return_value=[])
    return ib


@pytest.fixture
def ibkr_adapter(mock_ib: MagicMock) -> IBKRReferenceDataAdapter:
    """IBKRReferenceDataAdapter with injected mock IB connection."""
    return IBKRReferenceDataAdapter(ib=mock_ib)
