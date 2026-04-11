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

    def test_create_odds_api(self) -> None:
        adapter = create_sports_reference_adapter("odds_api", api_key="test-key")
        assert adapter is not None
        assert adapter.venue == "odds_api"

    def test_create_footystats(self) -> None:
        adapter = create_sports_reference_adapter("footystats", api_key="test-key")
        assert adapter is not None

    def test_create_understat(self) -> None:
        adapter = create_sports_reference_adapter("understat")
        assert adapter is not None

    def test_create_open_meteo(self) -> None:
        adapter = create_sports_reference_adapter("open_meteo")
        assert adapter is not None

    def test_create_transfermarkt(self) -> None:
        adapter = create_sports_reference_adapter("transfermarkt")
        assert adapter is not None

    def test_create_soccerfootball_info(self) -> None:
        adapter = create_sports_reference_adapter("soccerfootball_info")
        assert adapter is not None

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            create_sports_reference_adapter("nonexistent_provider")


class TestSportsAdapterVenueProperty:
    """All sports adapters return a valid venue string."""

    def test_api_football_venue(self) -> None:
        adapter = create_sports_reference_adapter("api_football", api_key="test")
        assert adapter.venue == "api_football"

    def test_odds_api_venue(self) -> None:
        adapter = create_sports_reference_adapter("odds_api", api_key="test")
        assert adapter.venue == "odds_api"

    def test_footystats_venue(self) -> None:
        adapter = create_sports_reference_adapter("footystats", api_key="test")
        assert adapter.venue == "footystats"

    def test_understat_venue(self) -> None:
        adapter = create_sports_reference_adapter("understat")
        assert adapter.venue == "understat"

    def test_open_meteo_venue(self) -> None:
        adapter = create_sports_reference_adapter("open_meteo")
        assert adapter.venue == "open_meteo"

    def test_transfermarkt_venue(self) -> None:
        adapter = create_sports_reference_adapter("transfermarkt")
        assert adapter.venue == "transfermarkt"

    def test_soccerfootball_info_venue(self) -> None:
        adapter = create_sports_reference_adapter("soccerfootball_info")
        assert adapter.venue == "soccerfootball_info"
