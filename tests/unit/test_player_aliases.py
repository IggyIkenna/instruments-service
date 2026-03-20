"""Unit tests for player_aliases.py — PlayerAliasResolver."""

from __future__ import annotations

import io
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from instruments_service.sports.player_aliases import (
    PlayerAliasResolver,
    load_player_mappings_from_dict,
    load_player_mappings_from_gcs,
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

    def test_get_mapping_by_canonical_id(self) -> None:
        mappings = load_player_mappings_from_dict(_sample_mappings())
        resolver = PlayerAliasResolver(mappings)
        result = resolver.get_mapping("HAALAND_E")
        assert result is not None
        assert result.api_football_player_id == 1100
        assert resolver.get_mapping("UNKNOWN") is None


def _make_parquet_bytes(data: list[dict[str, object]]) -> bytes:
    """Create in-memory Parquet bytes from a list of dicts."""
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class TestLoadPlayerMappingsFromGcs:
    def test_load_from_gcs_returns_mappings(self) -> None:
        raw = _make_parquet_bytes(
            [
                {
                    "canonical_player_id": "SALAH_M",
                    "display_name": "Mohamed Salah",
                    "api_football_player_id": 306,
                    "understat_player_id": 1250,
                    "footystats_player_id": None,
                    "soccer_football_player_id": None,
                }
            ]
        )
        with patch(
            "unified_cloud_interface.download_from_storage",
            return_value=raw,
        ):
            mappings = load_player_mappings_from_gcs("test-bucket", "sports/player_mappings.parquet")

        assert len(mappings) == 1
        assert mappings[0].canonical_player_id == "SALAH_M"
        assert mappings[0].api_football_player_id == 306
