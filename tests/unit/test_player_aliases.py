"""Unit tests for player_aliases.py — PlayerAliasResolver."""

from __future__ import annotations

from instruments_service.sports.player_aliases import (
    PlayerAliasResolver,
    load_player_mappings_from_dict,
)


def _sample_mappings() -> list[dict[str, object]]:
    return [
        {
            "canonical_player_id": "SALAH_M",
            "display_name": "Mohamed Salah",
            "api_football_player_id": 306,
            "understat_player_id": 1250,
            "footystats_player_id": None,
            "soccer_football_player_id": None,
        },
        {
            "canonical_player_id": "HAALAND_E",
            "display_name": "Erling Haaland",
            "api_football_player_id": 1100,
            "understat_player_id": 8260,
            "footystats_player_id": None,
            "soccer_football_player_id": None,
        },
        {
            "canonical_player_id": "SAKA_B",
            "display_name": "Bukayo Saka",
            "api_football_player_id": 1460,
            "understat_player_id": None,
            "footystats_player_id": None,
            "soccer_football_player_id": None,
        },
    ]


class TestPlayerAliasResolver:
    def test_resolver_indexes_by_api_football_id(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        result = resolver.find_by_api_football_id(306)
        assert result is not None
        assert result.canonical_player_id == "SALAH_M"
        assert result.display_name == "Mohamed Salah"

    def test_resolver_indexes_by_understat_id(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        result = resolver.find_by_understat_id(8260)
        assert result is not None
        assert result.canonical_player_id == "HAALAND_E"

    def test_resolver_mapping_count(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        assert resolver.mapping_count == 3

    def test_load_from_dict(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        assert len(mappings) == 3
        assert mappings[0].canonical_player_id == "SALAH_M"

    def test_find_none_for_missing_id(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        assert resolver.find_by_api_football_id(99999) is None
        assert resolver.find_by_understat_id(99999) is None

    def test_find_none_for_missing_understat(self) -> None:
        """Saka has no understat_player_id — find_by_understat_id should not find him."""
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        # Saka's api_football_id works
        result = resolver.find_by_api_football_id(1460)
        assert result is not None
        assert result.canonical_player_id == "SAKA_B"
