"""
Betfair API adapter for sports betting instruments.

Follows venue adapter pattern from TardisAdapter/DatabentoAdapter.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Try to import centralized credential helpers
try:
    from unified_cloud_services import get_secret_with_fallback, get_config
    SECRET_MANAGER_AVAILABLE = True
except ImportError:
    SECRET_MANAGER_AVAILABLE = False
    import os
    get_config = os.getenv

# Module-level caching for performance (like TardisAdapter)
# OPTIMIZATION: Cache API keys to avoid repeated Secret Manager calls
_BETFAIR_APP_KEY: Optional[str] = None
_BETFAIR_SESSION_TOKEN: Optional[str] = None


def clear_betfair_cache():
    """Clear module-level cache (useful for testing or credential rotation)"""
    global _BETFAIR_APP_KEY, _BETFAIR_SESSION_TOKEN
    _BETFAIR_APP_KEY = None
    _BETFAIR_SESSION_TOKEN = None
    logger.info("🧹 Cleared Betfair module-level cache")


class BetfairAdapter:
    """
    Betfair API adapter for sports betting instruments.
    Follows venue adapter pattern from TardisAdapter/DatabentoAdapter.
    """
    
    def __init__(self, api_key: Optional[str] = None, session_token: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize Betfair adapter with module-level API key caching.
        
        OPTIMIZED: Reuses cached API keys to avoid repeated Secret Manager calls.
        
        Args:
            api_key: Betfair application key (optional, uses cached or Secret Manager)
            session_token: Betfair session token (optional, uses cached or Secret Manager)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        global _BETFAIR_APP_KEY, _BETFAIR_SESSION_TOKEN
        
        # Reuse cached API keys if available (avoid Secret Manager calls)
        if _BETFAIR_APP_KEY and _BETFAIR_SESSION_TOKEN and not api_key and not session_token:
            self.api_key = _BETFAIR_APP_KEY
            self.session_token = _BETFAIR_SESSION_TOKEN
            logger.debug("✅ Reusing cached Betfair credentials")
        else:
            # Try provided credentials first
            self.api_key = api_key
            self.session_token = session_token
            
            # If not provided, try Secret Manager
            if not self.api_key or not self.session_token:
                if SECRET_MANAGER_AVAILABLE:
                    try:
                        secret_name_app_key = get_config("BETFAIR_SECRET_NAME_APP_KEY", "betfair-app-key")
                        secret_name_session = get_config("BETFAIR_SECRET_NAME_SESSION", "betfair-session-token")
                        project_id = project_id or get_config("GCP_PROJECT_ID", "central-element-323112")
                        
                        if not self.api_key:
                            self.api_key = get_secret_with_fallback(
                                project_id=project_id,
                                secret_name=secret_name_app_key,
                                fallback_env_var="BETFAIR_APP_KEY",
                            )
                        if not self.session_token:
                            self.session_token = get_secret_with_fallback(
                                project_id=project_id,
                                secret_name=secret_name_session,
                                fallback_env_var="BETFAIR_SESSION_TOKEN",
                            )
                        
                        if self.api_key and self.session_token:
                            logger.info(
                                f"✅ Retrieved Betfair credentials from Secret Manager "
                                f"(secrets: {secret_name_app_key}, {secret_name_session})"
                            )
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to retrieve Betfair credentials from Secret Manager: {e}")
                        # Fallback to environment variables
                        if not self.api_key:
                            self.api_key = get_config("BETFAIR_APP_KEY")
                        if not self.session_token:
                            self.session_token = get_config("BETFAIR_SESSION_TOKEN")
                else:
                    # Fallback to environment variables
                    if not self.api_key:
                        self.api_key = get_config("BETFAIR_APP_KEY")
                    if not self.session_token:
                        self.session_token = get_config("BETFAIR_SESSION_TOKEN")
            
            if not self.api_key or not self.session_token:
                raise ValueError(
                    "Betfair credentials required. Set BETFAIR_SECRET_NAME_APP_KEY and BETFAIR_SECRET_NAME_SESSION "
                    "env vars (for Secret Manager), BETFAIR_APP_KEY and BETFAIR_SESSION_TOKEN env vars (fallback), "
                    "or pass api_key and session_token parameters."
                )
            
            # Cache credentials for future instances
            _BETFAIR_APP_KEY = self.api_key
            _BETFAIR_SESSION_TOKEN = self.session_token
        
        self.api_url = "https://api.betfair.com/exchange/betting/json-rpc/v1"
        
        # HTTP session with retries (same pattern as TardisAdapter)
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        
        self.headers = {
            "X-Application": self.api_key,
            "X-Authentication": self.session_token,
            "Content-Type": "application/json",
        }
        
        # Cache for market catalogues (TTL: 1 hour, same pattern as TardisAdapter)
        self._cache: Dict[str, List[Dict]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(hours=1)
        
        logger.info("✅ BetfairAdapter initialized")
    
    def list_events_for_date_range(
        self,
        competition_ids: List[int],
        from_utc: datetime,
        to_utc: datetime,
    ) -> List[Dict[str, Any]]:
        """
        List football events in Betfair for competitions + date range.
        
        Args:
            competition_ids: Betfair competition IDs (e.g., [10932509] for Premier League)
            from_utc: Start date (UTC)
            to_utc: End date (UTC)
            
        Returns:
            List of event dictionaries from Betfair API
        """
        # Build JSON-RPC request
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listEvents",
            "id": 1,
            "params": {
                "filter": {
                    "eventTypeIds": ["1"],  # Soccer
                    "competitionIds": [str(cid) for cid in competition_ids],
                    "marketStartTime": {
                        "from": from_utc.isoformat(timespec="seconds") + "Z",
                        "to": to_utc.isoformat(timespec="seconds") + "Z",
                    },
                }
            },
        }
        
        try:
            response = self.session.post(self.api_url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            # Handle JSON-RPC response format
            if "result" in data:
                events = data["result"]
                logger.info(
                    f"Betfair: fetched {len(events)} events for competitions {competition_ids} "
                    f"between {from_utc} and {to_utc}"
                )
                return events
            elif "error" in data:
                error = data["error"]
                logger.error(f"Betfair API error: {error}")
                raise RuntimeError(f"Betfair API error: {error}")
            else:
                logger.warning(f"Unexpected Betfair API response format: {data}")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Betfair events: {e}")
            raise
    
    def list_market_catalogue_for_event(
        self,
        event_id: str,
        market_type_codes: List[str],
    ) -> List[Dict[str, Any]]:
        """
        List market catalogues for a specific Betfair event.
        
        Args:
            event_id: Betfair event ID
            market_type_codes: Market types (e.g., ["MATCH_ODDS", "OVER_UNDER_25", "BOTH_TEAMS_TO_SCORE"])
            
        Returns:
            List of market catalogue dictionaries
        """
        # Check cache first
        cache_key = f"market_catalogue_{event_id}_{','.join(sorted(market_type_codes))}"
        if self._is_cache_valid(cache_key):
            logger.debug(f"Using cached market catalogue for event {event_id}")
            return self._cache[cache_key]
        
        # Build JSON-RPC request
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "id": 1,
            "params": {
                "filter": {
                    "eventIds": [event_id],
                    "marketTypeCodes": market_type_codes,
                },
                "maxResults": 50,
                "marketProjection": ["COMPETITION", "EVENT", "MARKET_START_TIME"],
            },
        }
        
        try:
            response = self.session.post(self.api_url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            # Handle JSON-RPC response format
            if "result" in data:
                markets = data["result"]
                # Cache the result
                self._cache[cache_key] = markets
                self._cache_timestamps[cache_key] = datetime.utcnow()
                logger.debug(f"Betfair: fetched {len(markets)} markets for event {event_id}")
                return markets
            elif "error" in data:
                error = data["error"]
                logger.error(f"Betfair API error: {error}")
                raise RuntimeError(f"Betfair API error: {error}")
            else:
                logger.warning(f"Unexpected Betfair API response format: {data}")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Betfair market catalogue: {e}")
            raise
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self._cache:
            return False
        if cache_key not in self._cache_timestamps:
            return False
        age = datetime.utcnow() - self._cache_timestamps[cache_key]
        return age < self._cache_ttl
    
    def clear_cache(self):
        """Clear the cache."""
        self._cache.clear()
        self._cache_timestamps.clear()
    
    def cleanup(self):
        """Cleanup resources."""
        self.session.close()

