"""Player alias resolver — cross-provider player identity resolution.

Mirrors the pattern of team_aliases.py. Loads PlayerMapping objects (from GCS
or dict) and provides O(1) lookup by canonical player ID, API-Football ID,
or Understat ID.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Sequence

from unified_api_contracts import PlayerMapping  # noqa: deep-import — UAC sports is the correct domain facade

logger = logging.getLogger(__name__)


class PlayerAliasResolver:
    """Resolves player identities across data providers.

    Indexes PlayerMapping objects by canonical_player_id, api_football_player_id,
    and understat_player_id for O(1) lookups.
    """

    def __init__(self, mappings: Sequence[PlayerMapping]) -> None:
        self._by_canonical: dict[str, PlayerMapping] = {}
        self._by_api_football: dict[int, str] = {}
        self._by_understat: dict[int, str] = {}

        for m in mappings:
            self._by_canonical[m.canonical_player_id] = m
            if m.api_football_player_id is not None:
                self._by_api_football[m.api_football_player_id] = m.canonical_player_id
            if m.understat_player_id is not None:
                self._by_understat[m.understat_player_id] = m.canonical_player_id

    def find_by_api_football_id(self, player_id: int) -> PlayerMapping | None:
        """Look up a player by API-Football player ID."""
        canonical = self._by_api_football.get(player_id)
        if canonical is None:
            return None
        return self._by_canonical.get(canonical)

    def find_by_understat_id(self, understat_id: int) -> PlayerMapping | None:
        """Look up a player by Understat player ID."""
        canonical = self._by_understat.get(understat_id)
        if canonical is None:
            return None
        return self._by_canonical.get(canonical)

    def get_mapping(self, canonical_player_id: str) -> PlayerMapping | None:
        """Look up a player by canonical player ID."""
        return self._by_canonical.get(canonical_player_id)

    @property
    def mapping_count(self) -> int:
        """Number of unique canonical players indexed."""
        return len(self._by_canonical)


def load_player_mappings_from_dict(data: list[dict[str, object]]) -> list[PlayerMapping]:
    """Construct PlayerMapping list from plain dicts (for testing)."""
    return [PlayerMapping.model_validate(d) for d in data]


def load_player_mappings_from_gcs(bucket: str, path: str) -> list[PlayerMapping]:
    """Load PlayerMapping objects from a GCS parquet file.

    Uses lazy import of unified_cloud_interface to avoid eager dependency.
    """
    import pandas as pd  # noqa: import-inside — lazy load to avoid eager ~100ms pandas import
    from unified_cloud_interface import download_from_storage  # noqa: import-inside — lazy load GCS client

    raw_bytes = download_from_storage(bucket, path)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    return [PlayerMapping.model_validate(row) for row in df.to_dict(orient="records")]
