"""
League Definition — dataclass and helper constants for league definitions.

Contains the ``LeagueDefinition`` frozen dataclass, data-source frozenset
presets, country-code mappings, and season-month defaults used by the
league-data modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeagueDefinition:
    """Canonical definition of a sports league.

    Attributes:
        league_id: Unique identifier (e.g. ``EPL``, ``NBA``).
        display_name: Human-readable name.
        sport: Sport type (e.g. ``FOOTBALL``, ``BASKETBALL``).
        country: ISO 3166-1 alpha-2 country code or ``INTL`` for international.
        season_months: ``(start_month, end_month)`` — 1-indexed months of the
            regular season.
        has_playoffs: Whether the league has a playoff/knockout phase after
            regular season.
        data_sources: Which data APIs have coverage for this league.
        api_football_id: API-Football league ID, or ``None`` for non-football.
        tier: League tier — 1 (top), 2, 3, etc.  ``0`` for cup/reference
            competitions and non-football leagues.
        classification: League classification label (``Prediction``,
            ``Features``, ``Reference``, or ``Other``).
    """

    league_id: str
    display_name: str
    sport: str
    country: str
    season_months: tuple[int, int]
    has_playoffs: bool
    data_sources: frozenset[str]
    api_football_id: int | None
    tier: int
    classification: str


# ---------------------------------------------------------------------------
# Helpers — build data_sources frozenset from the raw config booleans
# ---------------------------------------------------------------------------

_DATA_SOURCE_KEYS: tuple[tuple[str, str], ...] = (
    ("api_football", "api_football"),
    ("soccerfootball_info", "soccerfootball_info"),
    ("footystats", "footystats"),
    ("transfermarkt", "transfermarkt"),
    ("understat", "understat"),
    ("odds_api", "odds_api"),
    ("open_meteo", "open_meteo"),
)


def _sources(*names: str) -> frozenset[str]:
    """Convenience to build a frozenset of data-source names."""
    return frozenset(names)


# Full-source prediction leagues (all 7 sources)
PRED_FULL = _sources(
    "api_football",
    "soccerfootball_info",
    "footystats",
    "transfermarkt",
    "understat",
    "odds_api",
    "open_meteo",
)

# Prediction leagues with all sources except Understat
PRED_NO_UNDERSTAT = _sources(
    "api_football",
    "soccerfootball_info",
    "footystats",
    "transfermarkt",
    "odds_api",
    "open_meteo",
)

# Prediction leagues without FootyStats (subscription limit) but with Understat=False
PRED_NO_FOOTYSTATS = _sources(
    "api_football",
    "soccerfootball_info",
    "transfermarkt",
    "odds_api",
    "open_meteo",
)

# Features leagues — API-Football + FootyStats + Transfermarkt
FEAT_STANDARD = _sources(
    "api_football",
    "footystats",
    "transfermarkt",
)

# Features leagues without FootyStats
FEAT_NO_FOOTYSTATS = _sources(
    "api_football",
    "transfermarkt",
)

# Reference/cup leagues — API-Football only
REF_API_ONLY = _sources("api_football")

# Non-football leagues — no football-specific data sources
NO_FOOTBALL_SOURCES: frozenset[str] = frozenset()


# ---------------------------------------------------------------------------
# Country helpers — map source country names to ISO alpha-2
# ---------------------------------------------------------------------------

COUNTRY_MAP: dict[str, str] = {
    "England": "GB",
    "Spain": "ES",
    "Germany": "DE",
    "Italy": "IT",
    "France": "FR",
    "Netherlands": "NL",
    "Portugal": "PT",
    "Belgium": "BE",
    "Turkey": "TR",
    "Greece": "GR",
    "Scotland": "GB",
    "Austria": "AT",
    "Switzerland": "CH",
    "Denmark": "DK",
    "Sweden": "SE",
    "Norway": "NO",
    "Poland": "PL",
    "Argentina": "AR",
    "Brazil": "BR",
    "Chile": "CL",
    "USA": "US",
    "Mexico": "MX",
    "Japan": "JP",
    "South Korea": "KR",
    "Australia": "AU",
    "Multi": "INTL",
}


# ---------------------------------------------------------------------------
# Season month defaults by country (European = Aug-May, Americas vary)
# ---------------------------------------------------------------------------

# European leagues: August-May
EURO_SEASON = (8, 5)
# South American / MLS / Calendar year: February-November (varies)
CALENDAR_SEASON = (2, 11)
# Japanese / Korean / Scandinavian: March-November
SPRING_AUTUMN_SEASON = (3, 11)
# Australian A-League: October-May
AUS_SEASON = (10, 5)

SEASON_BY_COUNTRY: dict[str, tuple[int, int]] = {
    "GB": EURO_SEASON,
    "ES": EURO_SEASON,
    "DE": EURO_SEASON,
    "IT": EURO_SEASON,
    "FR": EURO_SEASON,
    "NL": EURO_SEASON,
    "PT": EURO_SEASON,
    "BE": EURO_SEASON,
    "TR": EURO_SEASON,
    "GR": EURO_SEASON,
    "AT": EURO_SEASON,
    "CH": EURO_SEASON,
    "DK": EURO_SEASON,
    "PL": EURO_SEASON,
    "AR": CALENDAR_SEASON,
    "BR": (4, 12),
    "CL": CALENDAR_SEASON,
    "US": CALENDAR_SEASON,
    "MX": EURO_SEASON,
    "JP": SPRING_AUTUMN_SEASON,
    "KR": SPRING_AUTUMN_SEASON,
    "AU": AUS_SEASON,
    "SE": SPRING_AUTUMN_SEASON,
    "NO": SPRING_AUTUMN_SEASON,
    "INTL": (9, 6),
}
