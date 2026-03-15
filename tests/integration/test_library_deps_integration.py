"""Integration tests that import and exercise each library dependency."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_unified_api_contracts_import() -> None:
    from unified_api_contracts import HttpRateLimitHeaders

    assert HttpRateLimitHeaders is not None


@pytest.mark.integration
def test_unified_trading_library_import() -> None:
    from unified_trading_library import GracefulShutdownHandler

    assert GracefulShutdownHandler is not None


@pytest.mark.integration
def test_unified_config_interface_import() -> None:
    from unified_config_interface import BaseConfig

    assert BaseConfig is not None


@pytest.mark.integration
def test_unified_events_interface_import() -> None:
    from unified_events_interface import log_event, setup_events

    assert callable(log_event)
    assert callable(setup_events)


@pytest.mark.integration
def test_unified_domain_client_import() -> None:
    from unified_trading_library.domain_client import build_path, get_writer

    assert callable(build_path)
    assert callable(get_writer)


@pytest.mark.integration
def test_unified_cloud_interface_import() -> None:
    from unified_cloud_interface import get_storage_client

    assert callable(get_storage_client)


@pytest.mark.integration
def test_unified_reference_data_interface_import() -> None:
    from unified_reference_data_interface import InstrumentRecord, get_reference_adapter

    assert InstrumentRecord is not None
    assert callable(get_reference_adapter)


@pytest.mark.integration
def test_unified_internal_contracts_import() -> None:
    from unified_internal_contracts import LifecycleEventType

    assert LifecycleEventType is not None


@pytest.mark.integration
def test_unified_market_interface_import() -> None:
    from unified_market_interface import VenueMapping

    assert VenueMapping is not None
