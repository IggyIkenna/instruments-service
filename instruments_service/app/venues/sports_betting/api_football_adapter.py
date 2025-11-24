"""
API-Football adapter for fixtures, teams, stats.

Follows venue adapter pattern from TardisAdapter/DatabentoAdapter.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import httpx

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
# OPTIMIZATION: Cache API key to avoid repeated Secret Manager calls
_API_FOOTBALL_KEY: Optional[str] = None


def clear_api_football_cache():
    """Clear module-level cache (useful for testing or credential rotation)"""
    global _API_FOOTBALL_KEY
    _API_FOOTBALL_KEY = None
    logger.info("🧹 Cleared API-Football module-level cache")


class APIFootballAdapter:
    """
    API-Football adapter for fixtures, teams, stats.
    Follows venue adapter pattern from TardisAdapter/DatabentoAdapter.
    """
    
    def __init__(self, api_key: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize API-Football adapter with module-level API key caching.
        
        OPTIMIZED: Reuses cached API key to avoid repeated Secret Manager calls.
        
        Args:
            api_key: API-Football API key (optional, uses cached or Secret Manager)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        global _API_FOOTBALL_KEY
        
        # Reuse cached API key if available (avoid Secret Manager calls)
        if _API_FOOTBALL_KEY and not api_key:
            self.api_key = _API_FOOTBALL_KEY
            logger.debug("✅ Reusing cached API-Football API key")
        else:
            # Try provided API key first
            self.api_key = api_key
            
            # If not provided, try Secret Manager
            if not self.api_key:
                if SECRET_MANAGER_AVAILABLE:
                    try:
                        secret_name = get_config("API_FOOTBALL_SECRET_NAME", "api-football-key")
                        project_id = project_id or get_config("GCP_PROJECT_ID", "central-element-323112")
                        
                        self.api_key = get_secret_with_fallback(
                            project_id=project_id,
                            secret_name=secret_name,
                            fallback_env_var="API_FOOTBALL_KEY",
                        )
                        
                        if self.api_key:
                            logger.info(
                                f"✅ Retrieved API-Football API key from Secret Manager (secret: {secret_name})"
                            )
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")
                        self.api_key = get_config("API_FOOTBALL_KEY")
                else:
                    self.api_key = get_config("API_FOOTBALL_KEY")
            
            if not self.api_key:
                raise ValueError(
                    "API-Football API key required. Set API_FOOTBALL_SECRET_NAME env var (for Secret Manager), "
                    "API_FOOTBALL_KEY env var (fallback), or pass api_key parameter."
                )
            
            # Cache API key for future instances
            _API_FOOTBALL_KEY = self.api_key
        
        self.base_url = "https://v3.football.api-sports.io"
        self.headers = {"x-apisports-key": self.api_key}
        self._client: Optional[httpx.AsyncClient] = None
        
        logger.info("✅ APIFootballAdapter initialized")
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    
    async def get_fixtures_for_league_season(
        self,
        league_id: int,
        season: int,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch fixtures for a league+season, optionally filtered by date range.
        
        Args:
            league_id: API-Football league ID (e.g., 39 for Premier League)
            season: Season year (e.g., 2024)
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            List of fixture dictionaries from API-Football
        """
        client = await self._get_client()
        
        params: Dict[str, Any] = {
            "league": league_id,
            "season": season,
        }
        if from_date:
            params["from"] = from_date.date().isoformat()
        if to_date:
            params["to"] = to_date.date().isoformat()
        
        url = f"{self.base_url}/fixtures"
        
        try:
            response = await client.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            fixtures = data.get("response", [])
            logger.info(
                f"API-Football: fetched {len(fixtures)} fixtures "
                f"for league_id={league_id}, season={season}"
            )
            return fixtures
        except httpx.HTTPStatusError as e:
            logger.error(f"API-Football HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"API-Football request error: {e}")
            raise
    
    async def aclose(self):
        """Close async HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

