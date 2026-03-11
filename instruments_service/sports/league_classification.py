"""
League Classification — Pydantic models and registry for league classification config.

Migrated from ``sports-betting-services-previous/extra/league_classification_config.py``.

Provides:
- ``LeagueClassificationType``  — StrEnum for classification labels
- ``LeagueClassification``  — Pydantic model for a classified league entry
- ``LeagueClassificationRegistry``  — registry with query methods
- ``LEAGUE_CLASSIFICATION_DATA``  — 94 leagues (complete classification)
"""

from __future__ import annotations

import logging
from enum import StrEnum

from pydantic import BaseModel, Field

from instruments_service.sports.league_data_classification import LEAGUE_CLASSIFICATION_DATA

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Classification enum
# ---------------------------------------------------------------------------


class LeagueClassificationType(StrEnum):
    """Classification label for a league."""

    PREDICTION = "Prediction"
    REFERENCE = "Reference"
    FEATURES = "Features"


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------


class LeagueClassification(BaseModel):  # CORRECT-LOCAL
    """Canonical classification entry for a football league.

    Migrated from the old ``LEAGUE_CLASSIFICATION`` dict in
    ``sports-betting-services-previous``.

    Attributes:
        league_id: API-Football numeric league ID.
        name: League display name (from ``api_football_league_name``).
        country: Country or region (e.g. ``England``, ``Spain``).
        tier: League tier (1 = top division, 2 = second, 3 = third, etc.).
        classification: ``Prediction``, ``Reference``, or ``Features``.
        odds_api_name: The Odds API league key, or ``None`` if not available.
        data_sources: Map of data provider name to enabled flag.
    """

    league_id: int = Field(description="API-Football numeric league ID")
    name: str = Field(description="League display name")
    country: str = Field(description="Country or region")
    tier: int = Field(ge=1, description="League tier (1=top, 2=second, ...)")
    classification: LeagueClassificationType = Field(description="Prediction / Reference / Features")
    odds_api_name: str | None = Field(default=None, description="The Odds API league key")
    data_sources: dict[str, bool] = Field(
        default_factory=dict,
        description="Data provider name -> enabled flag",
    )

    model_config = {"frozen": True}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class LeagueClassificationRegistry:
    """Registry of league classifications with query helpers.

    Loads from a Python dict mapping ``league_id (int) -> LeagueClassification``.
    Designed for forward-compatible migration to ConfigStore YAML.
    """

    def __init__(self, data: dict[int, LeagueClassification]) -> None:
        self._leagues: dict[int, LeagueClassification] = dict(data)

    @classmethod
    def from_raw_dict(
        cls, raw: dict[int, dict[str, str | int | bool | dict[str, bool] | None]]
    ) -> LeagueClassificationRegistry:
        """Build registry from a raw dict matching the old config format.

        Each value is expected to have keys: ``country_region``,
        ``api_football_id``, ``api_football_league_name``, ``odds_api_league_name``,
        ``classification``, ``tier``, ``data_sources``.
        """
        leagues: dict[int, LeagueClassification] = {}
        for league_id, entry in raw.items():
            country_val: str = str(entry["country_region"]) if "country_region" in entry else ""
            name_val: str = str(entry["api_football_league_name"]) if "api_football_league_name" in entry else ""
            tier_val = entry.get("tier", 1)
            classification_val = entry.get("classification", "Prediction")
            odds_api_val = entry.get("odds_api_league_name")
            raw_data_sources = entry.get("data_sources")
            data_sources_val: dict[str, bool] = (
                {k: bool(v) for k, v in raw_data_sources.items()} if isinstance(raw_data_sources, dict) else {}
            )

            leagues[league_id] = LeagueClassification(
                league_id=league_id,
                name=str(name_val),
                country=str(country_val),
                tier=int(str(tier_val)),
                classification=LeagueClassificationType(str(classification_val)),
                odds_api_name=str(odds_api_val) if odds_api_val is not None else None,
                data_sources=dict(data_sources_val),
            )
        return cls(leagues)

    # -- Query methods -------------------------------------------------------

    def get_league(self, league_id: int) -> LeagueClassification | None:
        """Look up a league by its API-Football numeric ID."""
        return self._leagues.get(league_id)

    def get_prediction_leagues(self) -> list[LeagueClassification]:
        """Return tier 1 and tier 2 Prediction leagues."""
        return [
            league
            for league in self._leagues.values()
            if league.classification == LeagueClassificationType.PREDICTION and league.tier <= 2
        ]

    def get_leagues_by_tier(self, tier: int) -> list[LeagueClassification]:
        """Return all leagues at a given tier."""
        return [league for league in self._leagues.values() if league.tier == tier]

    def get_data_sources(self, league_id: int) -> dict[str, bool]:
        """Return the data-source flags for a league, or empty dict if unknown."""
        league = self._leagues.get(league_id)
        if league is None:
            return {}
        return dict(league.data_sources)

    def get_all_leagues(self) -> list[LeagueClassification]:
        """Return all leagues in the registry."""
        return list(self._leagues.values())

    @property
    def league_count(self) -> int:
        """Number of leagues in the registry."""
        return len(self._leagues)


# ---------------------------------------------------------------------------
# Default registry instance — built from the embedded data
# ---------------------------------------------------------------------------

DEFAULT_CLASSIFICATION_REGISTRY = LeagueClassificationRegistry.from_raw_dict(LEAGUE_CLASSIFICATION_DATA)
