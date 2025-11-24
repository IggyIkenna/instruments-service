"""
Sports Instrument Builder

Build static sports instrument definitions from
API-Football fixtures + Betfair market catalogues.

Follows same pattern as crypto/TradFi instrument builders.
"""

import logging
import re
from typing import Dict, List, Any, Iterable, Optional
from datetime import datetime, timezone

from instruments_service.models import InstrumentDefinition
from instruments_service.config import SportsLeagueConfig

logger = logging.getLogger(__name__)


def _slug_team(name: str) -> str:
    """Basic slugging for team names."""
    slug = re.sub(r"[^A-Za-z0-9]+", "-", name.upper()).strip("-")
    return slug


def _build_match_slug(
    league: SportsLeagueConfig,
    kickoff_ts_utc: datetime,
    home_name: str,
    away_name: str,
) -> str:
    """Build match slug: {league_code}-{home_slug}-VS-{away_slug}-{YYYYMMDDTHHMM}"""
    d = kickoff_ts_utc.strftime("%Y%m%dT%H%M")
    return f"{league.league_code}-{_slug_team(home_name)}-VS-{_slug_team(away_name)}-{d}"


def _canonical_instrument_key(
    venue: str,
    market_group: str,
    league: SportsLeagueConfig,
    kickoff_ts_utc: datetime,
    home_name: str,
    away_name: str,
    line: Optional[str] = None,
) -> str:
    """
    Generate canonical instrument key with asset class prefix.
    
    Format: FOOTBALL:VENUE:MARKET_GROUP:COMPETITION:YYYYMMDDTHHMM:HOME-AWAY[@LINE]
    Example: FOOTBALL:BETFAIR:MATCH_WINNER:ENG-PREMIER_LEAGUE:20250315T1500:ARSENAL-LIVERPOOL
    """
    dt_str = kickoff_ts_utc.strftime("%Y%m%dT%H%M")
    home_slug = _slug_team(home_name)
    away_slug = _slug_team(away_name)
    payload = f"{league.league_code}:{dt_str}:{home_slug}-{away_slug}"
    if line:
        payload = f"{payload}@{line}"
    # Add FOOTBALL asset class prefix
    return f"FOOTBALL:{venue}:{market_group}:{payload}"


class SportsInstrumentBuilder:
    """
    Build static sports instrument definitions from
    API-Football fixtures + Betfair market catalogues.
    
    Follows same pattern as crypto/TradFi instrument builders.
    """
    
    def __init__(self, venue: str = "BETFAIR"):
        self.venue = venue
    
    def build_instruments_for_match(
        self,
        league: SportsLeagueConfig,
        fixture: Dict[str, Any],
        betfair_markets: Iterable[Dict[str, Any]],
    ) -> List[InstrumentDefinition]:
        """
        Build InstrumentDefinition entries for a single match.
        
        Creates 3 instruments per match:
        - MATCH_WINNER (1X2)
        - TOTAL_GOALS_OU_2_5 (Over/Under 2.5)
        - BTTS (Both Teams To Score)
        
        Args:
            league: League configuration
            fixture: API-Football fixture object
            betfair_markets: Betfair market catalogue results
            
        Returns:
            List of InstrumentDefinition objects
        """
        # Extract fixture info
        match_info = fixture.get("fixture", {})
        teams_info = fixture.get("teams", {})
        home_info = teams_info.get("home", {})
        away_info = teams_info.get("away", {})
        
        home_name = home_info.get("name") or "HOME"
        away_name = away_info.get("name") or "AWAY"
        home_team_id = str(home_info.get("id", ""))
        away_team_id = str(away_info.get("id", ""))
        fixture_id = str(match_info.get("id", ""))
        
        # Parse kickoff timestamp (UTC)
        kickoff_raw = match_info.get("date")
        if not kickoff_raw:
            logger.warning(f"Missing kickoff date for fixture {fixture_id}")
            return []
        
        try:
            kickoff_ts = datetime.fromisoformat(kickoff_raw.replace("Z", "+00:00"))
            if kickoff_ts.tzinfo is None:
                kickoff_ts = kickoff_ts.replace(tzinfo=timezone.utc)
            else:
                kickoff_ts = kickoff_ts.astimezone(timezone.utc)
        except Exception as e:
            logger.warning(f"Failed to parse kickoff date '{kickoff_raw}' for fixture {fixture_id}: {e}")
            return []
        
        # Build match slug
        match_slug = _build_match_slug(league, kickoff_ts, home_name, away_name)
        
        instruments: List[InstrumentDefinition] = []
        
        # Index markets by Betfair marketTypeCode
        markets_by_type: Dict[str, List[Dict[str, Any]]] = {}
        for m in betfair_markets:
            mtype = m.get("marketType")
            if mtype in ["MATCH_ODDS", "OVER_UNDER_25", "BOTH_TEAMS_TO_SCORE"]:
                markets_by_type.setdefault(mtype, []).append(m)
        
        # Get season from fixture
        season = str(fixture.get("league", {}).get("season", ""))
        
        # 1) MATCH_ODDS → MATCH_WINNER instruments
        for m in markets_by_type.get("MATCH_ODDS", []):
            # Use market start time from Betfair if available, otherwise use fixture kickoff
            start_time_str = m.get("marketStartTime")
            if start_time_str:
                try:
                    start_ts = datetime.fromisoformat(start_time_str.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    start_ts = kickoff_ts
            else:
                start_ts = kickoff_ts
            
            inst_key = _canonical_instrument_key(
                venue=self.venue,
                market_group="MATCH_WINNER",
                league=league,
                kickoff_ts_utc=start_ts,
                home_name=home_name,
                away_name=away_name,
            )
            
            instruments.append(
                InstrumentDefinition(
                    instrument_key=inst_key,
                    venue=self.venue,
                    instrument_type="MATCH_WINNER",
                    symbol=match_slug,
                    venue_type="exchange",
                    asset_class="SPORTS",
                    data_provider="BETFAIR",
                    data_types="sports_odds",
                    base_asset="",
                    quote_asset="",
                    settle_currency="GBP",
                    available_from_datetime=start_ts.isoformat(),
                    available_to_datetime=None,  # Markets settle after match ends
                    sport="FOOTBALL",
                    competition_code=league.league_code,
                    season=season,
                    match_id=fixture_id,
                    match_slug=match_slug,
                    market_group="MATCH_WINNER",
                    market_param=None,
                    market_outcomes="HOME,DRAW,AWAY",
                    sport_home_team_id=home_team_id,
                    sport_away_team_id=away_team_id,
                    sport_home_team_name=home_name,
                    sport_away_team_name=away_name,
                    kickoff_ts_utc=start_ts,
                    kickoff_fixture_time=kickoff_ts,  # Official kickoff from API-Football
                    kickoff_inferred_from_odds=None,  # Will be populated later from odds data analysis
                    kickoff_delta_minutes=None,  # Will be calculated when inferred kickoff is available
                    max_hours_after_kickoff=league.max_hours_after_kickoff if hasattr(league, 'max_hours_after_kickoff') else 2.0,
                    pre_match_open_ts_utc=None,
                    settle_ts_utc=None,
                    in_play_supported=True,
                )
            )
        
        # 2) OVER_UNDER_25 → TOTAL_GOALS_OU_2_5
        for m in markets_by_type.get("OVER_UNDER_25", []):
            start_time_str = m.get("marketStartTime")
            if start_time_str:
                try:
                    start_ts = datetime.fromisoformat(start_time_str.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    start_ts = kickoff_ts
            else:
                start_ts = kickoff_ts
            
            inst_key = _canonical_instrument_key(
                venue=self.venue,
                market_group="TOTAL_GOALS_OU_2_5",
                league=league,
                kickoff_ts_utc=start_ts,
                home_name=home_name,
                away_name=away_name,
                line="2.5",
            )
            
            instruments.append(
                InstrumentDefinition(
                    instrument_key=inst_key,
                    venue=self.venue,
                    instrument_type="TOTAL_GOALS_OU_2_5",
                    symbol=match_slug,
                    venue_type="exchange",
                    asset_class="SPORTS",
                    data_provider="BETFAIR",
                    data_types="sports_odds",
                    base_asset="",
                    quote_asset="",
                    settle_currency="GBP",
                    available_from_datetime=start_ts.isoformat(),
                    available_to_datetime=None,
                    sport="FOOTBALL",
                    competition_code=league.league_code,
                    season=season,
                    match_id=fixture_id,
                    match_slug=match_slug,
                    market_group="TOTAL_GOALS_OU_2_5",
                    market_param="2.5",
                    market_outcomes="OVER,UNDER",
                    sport_home_team_id=home_team_id,
                    sport_away_team_id=away_team_id,
                    sport_home_team_name=home_name,
                    sport_away_team_name=away_name,
                    kickoff_ts_utc=start_ts,
                    kickoff_fixture_time=kickoff_ts,  # Official kickoff from API-Football
                    kickoff_inferred_from_odds=None,  # Will be populated later from odds data analysis
                    kickoff_delta_minutes=None,  # Will be calculated when inferred kickoff is available
                    max_hours_after_kickoff=league.max_hours_after_kickoff if hasattr(league, 'max_hours_after_kickoff') else 2.0,
                    pre_match_open_ts_utc=None,
                    settle_ts_utc=None,
                    in_play_supported=True,
                )
            )
        
        # 3) BOTH_TEAMS_TO_SCORE → BTTS
        for m in markets_by_type.get("BOTH_TEAMS_TO_SCORE", []):
            start_time_str = m.get("marketStartTime")
            if start_time_str:
                try:
                    start_ts = datetime.fromisoformat(start_time_str.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    start_ts = kickoff_ts
            else:
                start_ts = kickoff_ts
            
            inst_key = _canonical_instrument_key(
                venue=self.venue,
                market_group="BTTS",
                league=league,
                kickoff_ts_utc=start_ts,
                home_name=home_name,
                away_name=away_name,
            )
            
            instruments.append(
                InstrumentDefinition(
                    instrument_key=inst_key,
                    venue=self.venue,
                    instrument_type="BTTS",
                    symbol=match_slug,
                    venue_type="exchange",
                    asset_class="SPORTS",
                    data_provider="BETFAIR",
                    data_types="sports_odds",
                    base_asset="",
                    quote_asset="",
                    settle_currency="GBP",
                    available_from_datetime=start_ts.isoformat(),
                    available_to_datetime=None,
                    sport="FOOTBALL",
                    competition_code=league.league_code,
                    season=season,
                    match_id=fixture_id,
                    match_slug=match_slug,
                    market_group="BTTS",
                    market_param=None,
                    market_outcomes="YES,NO",
                    sport_home_team_id=home_team_id,
                    sport_away_team_id=away_team_id,
                    sport_home_team_name=home_name,
                    sport_away_team_name=away_name,
                    kickoff_ts_utc=start_ts,
                    kickoff_fixture_time=kickoff_ts,  # Official kickoff from API-Football
                    kickoff_inferred_from_odds=None,  # Will be populated later from odds data analysis
                    kickoff_delta_minutes=None,  # Will be calculated when inferred kickoff is available
                    max_hours_after_kickoff=league.max_hours_after_kickoff if hasattr(league, 'max_hours_after_kickoff') else 2.0,
                    pre_match_open_ts_utc=None,
                    settle_ts_utc=None,
                    in_play_supported=True,
                )
            )
        
        return instruments

