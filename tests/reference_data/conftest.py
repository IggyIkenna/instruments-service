"""Shared fixtures for unified-reference-data-interface tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from instruments_service.reference_data.adapters.tradfi.ibkr import IBKRReferenceDataAdapter


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: mark test as integration test (requires external services)")


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
