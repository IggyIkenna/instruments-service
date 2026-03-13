"""Team Alias Resolution — cross-provider team identity mapping.

Migrated from ``sports-betting-services-previous/footballbets/core/models.py``
(SQLAlchemy ``TeamMapping`` table with af/us/ft/sf/od/tm provider IDs) and
``footballbets/utils/team_name_changes.py`` (API-Football team_id -> corrected name).

Uses the ``TeamMapping`` Pydantic schema from ``unified-api-contracts`` as the
canonical data model, replacing the old SQLAlchemy ORM rows.

Capabilities:
- Resolve any provider-specific team ID to a canonical team ID.
- Look up full cross-provider mapping by canonical ID.
- Enumerate all known aliases/names for a team.
- Fuzzy name search across all aliases (accent-normalised, case-insensitive).
- Track historical team name changes with ``TeamNameHistory``.
- Load mappings from GCS Parquet or from plain dicts (for testing).
"""

from __future__ import annotations

import io
import logging
from collections import defaultdict
from typing import cast

import pandas as pd
from pydantic import BaseModel, ConfigDict
from unified_api_contracts import TeamMapping
from unified_cloud_interface import download_from_storage

from instruments_service.sports.team_normalizer import normalize_for_fuzzy as _normalize_for_fuzzy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider field names — maps provider key to TeamMapping attribute name.
# The values in the TeamMapping schema that carry provider-specific IDs.
# ---------------------------------------------------------------------------

_PROVIDER_ID_FIELDS: dict[str, str] = {
    "api_football": "api_football_id",
    "footystats": "footystats_id",
    "understat": "understat_name",
    "soccer_football": "soccer_football_id",
    "betfair": "betfair_id",
    "pinnacle": "pinnacle_id",
    "odds_api": "odds_api_key",
}


# ---------------------------------------------------------------------------
# TeamNameHistory — frozen model for tracking name changes over time
# ---------------------------------------------------------------------------


class TeamNameHistory(BaseModel):  # CORRECT-LOCAL
    """Records a historical team name change.

    Frozen so instances can be stored in sets and used as dict keys.
    """

    model_config = ConfigDict(frozen=True)

    canonical_team_id: str
    old_name: str
    new_name: str
    effective_date: str
    reason: str | None = None


# ---------------------------------------------------------------------------
# TeamAliasResolver — the main resolver class
# ---------------------------------------------------------------------------


class TeamAliasResolver:
    """Resolves team identities across multiple data providers.

    Loads :class:`TeamMapping` objects and indexes them for fast lookup by:
    - provider + provider_id  (e.g. ``("api_football", "42")``)
    - canonical_team_id       (e.g. ``"ARS"``)
    - fuzzy name search       (e.g. ``"Bayern München"`` -> ``"BAY"``)

    The resolver is **read-only after construction**: call one of the
    ``load_*`` helpers to build the mapping list, then pass it in.
    """

    def __init__(self, mappings: list[TeamMapping]) -> None:
        self._mappings: list[TeamMapping] = list(mappings)

        # canonical_team_id -> TeamMapping
        self._by_canonical: dict[str, TeamMapping] = {}

        # (provider, str(provider_id)) -> canonical_team_id
        self._by_provider: dict[tuple[str, str], str] = {}

        # canonical_team_id -> set of all known name strings
        self._aliases: dict[str, set[str]] = defaultdict(set)

        # normalised_name -> canonical_team_id  (for fuzzy lookup)
        self._name_index: dict[str, str] = {}

        for mapping in self._mappings:
            self._index_mapping(mapping)

    # -- internal indexing ---------------------------------------------------

    def _index_mapping(self, mapping: TeamMapping) -> None:
        cid = mapping.canonical_team_id
        self._by_canonical[cid] = mapping

        # Index provider IDs
        for provider, field in _PROVIDER_ID_FIELDS.items():
            value: str | int | None = cast(str | int | None, getattr(mapping, field, None))
            if value is not None:
                self._by_provider[(provider, str(value))] = cid

        # Collect all name variants
        names: set[str] = {mapping.display_name}
        names.update(mapping.aliases)
        self._aliases[cid] = names

        # Build normalised name index
        for name in names:
            norm = _normalize_for_fuzzy(name)
            if norm and norm not in self._name_index:
                self._name_index[norm] = cid

    # -- public API ----------------------------------------------------------

    def resolve_team(self, provider: str, provider_id: str | int) -> str | None:
        """Return the canonical_team_id for a provider-specific ID.

        Parameters
        ----------
        provider:
            One of ``"api_football"``, ``"footystats"``, ``"understat"``,
            ``"soccer_football"``, ``"betfair"``, ``"pinnacle"``, ``"odds_api"``.
        provider_id:
            The provider-specific identifier (int or str).

        Returns
        -------
        str | None
            The canonical team ID, or ``None`` if not found.
        """
        return self._by_provider.get((provider, str(provider_id)))

    def get_mapping(self, canonical_team_id: str) -> TeamMapping | None:
        """Return the full :class:`TeamMapping` for *canonical_team_id*."""
        return self._by_canonical.get(canonical_team_id)

    def get_aliases(self, canonical_team_id: str) -> frozenset[str]:
        """Return all known name variants for *canonical_team_id*.

        Returns an empty frozenset if the team is unknown.
        """
        return frozenset(self._aliases.get(canonical_team_id, set()))

    def find_by_name(self, name: str) -> TeamMapping | None:
        """Fuzzy name lookup — accent-stripped, case-insensitive.

        Searches across ``display_name`` and all ``aliases`` of every mapping.
        Returns the matching :class:`TeamMapping`, or ``None``.
        """
        norm = _normalize_for_fuzzy(name)
        cid = self._name_index.get(norm)
        if cid is not None:
            return self._by_canonical.get(cid)
        return None

    @property
    def mapping_count(self) -> int:
        """Number of canonical teams indexed."""
        return len(self._by_canonical)


# ---------------------------------------------------------------------------
# Loader helpers
# ---------------------------------------------------------------------------


def load_team_mappings_from_dict(
    data: list[dict[str, str | int | list[str] | frozenset[str] | None]],
) -> list[TeamMapping]:
    """Build :class:`TeamMapping` objects from a list of plain dicts.

    Intended for testing and local development.  Each dict should contain
    keys matching :class:`TeamMapping` field names.  The ``aliases`` field
    may be a ``list[str]`` or ``frozenset[str]``; lists are auto-converted.

    Parameters
    ----------
    data:
        List of dicts with TeamMapping-compatible keys.

    Returns
    -------
    list[TeamMapping]
        Validated TeamMapping instances.
    """
    mappings: list[TeamMapping] = []
    for row in data:
        # Convert aliases from list to frozenset if needed
        raw_aliases = row.get("aliases")
        if isinstance(raw_aliases, list):
            row = {**row, "aliases": frozenset(raw_aliases)}
        mappings.append(TeamMapping.model_validate(row))
    return mappings


def load_team_mappings_from_gcs(
    bucket: str,
    path: str,
) -> list[TeamMapping]:
    """Load :class:`TeamMapping` objects from a Parquet file in GCS.

    Uses ``get_storage_client()`` from ``unified-cloud-interface`` to download
    the Parquet bytes, then deserialises via ``pyarrow`` / ``pandas``.

    Parameters
    ----------
    bucket:
        GCS bucket name.
    path:
        Blob path within the bucket (e.g. ``"sports/team_mappings.parquet"``).

    Returns
    -------
    list[TeamMapping]
        Validated TeamMapping instances loaded from the Parquet file.
    """
    raw_bytes = download_from_storage(bucket, path)
    df = pd.read_parquet(io.BytesIO(raw_bytes))

    mappings: list[TeamMapping] = []
    for _, row in df.iterrows():
        row_dict: dict[str, str | int | frozenset[str] | None] = {}
        for col in df.columns:
            raw_val: object = cast(object, row[col])  # suppress pandas Any → object
            val: object = raw_val
            # pandas may produce numpy types; convert to native Python
            try:
                is_na = val is None or (isinstance(val, float) and pd.isna(val))
            except (TypeError, ValueError):
                is_na = False
            if is_na:
                row_dict[str(col)] = None
            else:
                row_dict[str(col)] = cast(str | int, val)

        # Handle aliases column — may be stored as delimited string
        raw_aliases = row_dict.get("aliases")
        if isinstance(raw_aliases, str):
            alias_set: frozenset[str] = frozenset(a.strip() for a in raw_aliases.split(",") if a.strip())
            row_dict["aliases"] = alias_set
        elif raw_aliases is None:
            row_dict["aliases"] = frozenset()

        mappings.append(TeamMapping.model_validate(row_dict))

    logger.info("Loaded %d team mappings from gs://%s/%s", len(mappings), bucket, path)  # noqa: gs-uri — log message only
    return mappings


def load_default_mappings() -> TeamAliasResolver:
    """Build a :class:`TeamAliasResolver` populated with all known team data.

    Imports the hardcoded mapping data from
    :mod:`instruments_service.sports.team_mapping_data` (EPL + Bundesliga),
    converts each entry to a :class:`TeamMapping`, and returns a fully
    populated resolver ready for use.

    Returns
    -------
    TeamAliasResolver
        A resolver containing all EPL, Bundesliga, and historical team mappings.
    """

    from instruments_service.sports.team_mapping_data import (  # noqa: lazy-load — avoids eager load of ~2MB sports mapping data
        BUNDESLIGA_TEAM_MAPPINGS,
        EPL_TEAM_MAPPINGS,
    )

    all_data: list[dict[str, str | int | list[str] | frozenset[str] | None]] = []
    all_data.extend(cast(list[dict[str, str | int | list[str] | frozenset[str] | None]], EPL_TEAM_MAPPINGS))
    all_data.extend(cast(list[dict[str, str | int | list[str] | frozenset[str] | None]], BUNDESLIGA_TEAM_MAPPINGS))

    mappings = load_team_mappings_from_dict(all_data)
    logger.info("Loaded %d default team mappings (EPL + Bundesliga)", len(mappings))
    return TeamAliasResolver(mappings)
