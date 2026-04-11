"""Tests for BaseSportsReferenceAdapter and sports adapter factory."""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.sports.factory import (
    create_sports_reference_adapter,
)


class TestSportsFactory:
    def test_create_api_football(self) -> None:
        adapter = create_sports_reference_adapter("api_football", api_key="test-key")
        assert adapter is not None
        assert adapter.venue == "api_football"

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            create_sports_reference_adapter("nonexistent_provider")


class TestSportsAdapterVenueProperty:
    """All sports adapters return a valid venue string."""

    def test_api_football_venue(self) -> None:
        adapter = create_sports_reference_adapter("api_football", api_key="test")
        assert adapter.venue == "api_football"
