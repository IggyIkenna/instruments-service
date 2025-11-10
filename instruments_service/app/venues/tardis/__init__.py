"""
Tardis Venue Adapter

Fetches crypto exchange instrument definitions from Tardis API.
Supports Binance, Bybit, OKX, Deribit, and other crypto exchanges.

This adapter abstracts Tardis-specific logic from InstrumentProcessingService,
making the architecture consistent with Databento and DeFi adapters.
"""

import logging
import os
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta, date
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from unified_cloud_services import get_secret_with_fallback

    SECRET_MANAGER_AVAILABLE = True
except ImportError:
    SECRET_MANAGER_AVAILABLE = False
    logging.warning("unified-cloud-services not available for Secret Manager")

logger = logging.getLogger(__name__)


class TardisAdapter:
    """
    Adapter for fetching crypto exchange instrument definitions from Tardis API.

    Supports:
    - Binance (spot, futures)
    - Bybit (spot, perpetuals)
    - OKX (spot, futures, swaps)
    - Deribit (futures, options)
    - Other crypto exchanges via Tardis
    """

    def __init__(self, api_key: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize Tardis adapter.

        Args:
            api_key: Tardis API key (optional, uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Try provided API key first
        self.api_key = api_key

        # If not provided, try Secret Manager
        if not self.api_key:
            if SECRET_MANAGER_AVAILABLE:
                try:
                    secret_name = os.getenv("TARDIS_SECRET_NAME", "tardis-api-key")
                    project_id = project_id or os.getenv(
                        "GCP_PROJECT_ID", "central-element-323112"
                    )

                    self.api_key = get_secret_with_fallback(
                        project_id=project_id,
                        secret_name=secret_name,
                        fallback_env_var="TARDIS_API_KEY",
                    )

                    if self.api_key:
                        logger.info(
                            f"✅ Retrieved Tardis API key from Secret Manager (secret: {secret_name})"
                        )
                except Exception as e:
                    logger.warning(
                        f"⚠️ Failed to retrieve API key from Secret Manager: {e}"
                    )
                    self.api_key = os.getenv("TARDIS_API_KEY")
            else:
                self.api_key = os.getenv("TARDIS_API_KEY")

        if not self.api_key:
            raise ValueError(
                "Tardis API key required. Set TARDIS_SECRET_NAME env var (for Secret Manager), "
                "TARDIS_API_KEY env var (fallback), or pass api_key parameter."
            )

        # Setup HTTP session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Cache for exchange instruments (TTL: 1 hour)
        self._cache: Dict[str, List[Dict]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(hours=1)

        logger.info("✅ TardisAdapter initialized")

    def fetch_exchange_instruments(
        self,
        exchange: str,
        target_date: Optional[datetime] = None,
        force_refresh: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fetch instrument data from Tardis API for specific exchange.

        Args:
            exchange: Exchange name (e.g., 'binance-futures', 'bybit', 'okx')
            target_date: Target date for instrument availability filtering
            force_refresh: If True, bypass cache and fetch fresh data

        Returns:
            Tuple of (available_symbols list, date_filtered_count)
        """
        target_date = target_date or datetime.now(timezone.utc)
        date_str = target_date.strftime("%Y-%m-%d")

        # Check cache first
        cache_key = f"{exchange}_instruments"

        if not force_refresh and self._is_cache_valid(cache_key):
            logger.info(f"📋 Using cached Tardis data for {exchange}")
            available_symbols = self._cache[cache_key]
        else:
            # Fetch fresh data from Tardis API
            url = f"https://api.tardis.dev/v1/exchanges/{exchange}"

            try:
                logger.info(f"🔍 Fetching instruments from Tardis API: {exchange}")

                response = self.session.get(url, headers=self.headers, timeout=60)
                response.raise_for_status()

                exchange_info = response.json()
                available_symbols = exchange_info.get("availableSymbols", [])

                # Cache the results
                self._cache[cache_key] = available_symbols
                self._cache_timestamps[cache_key] = datetime.now(timezone.utc)

                logger.info(
                    f"✅ Fetched & cached {len(available_symbols)} instruments from {exchange}"
                )
            except Exception as e:
                logger.error(f"❌ Tardis API failed for {exchange}: {e}")
                return [], 0

        # Filter by date availability
        date_filtered_count = 0
        if target_date:
            original_count = len(available_symbols)

            filtered_symbols = []
            for symbol in available_symbols:
                if self._is_instrument_available_on_date(
                    symbol.get("availableSince", ""),
                    symbol.get("availableTo", ""),
                    date_str,
                    symbol,
                ):
                    filtered_symbols.append(symbol)

            available_symbols = filtered_symbols
            date_filtered_count = original_count - len(available_symbols)

            if date_filtered_count > 0:
                logger.info(
                    f"📅 Date filter: {len(available_symbols)}/{original_count} instruments available on {date_str}"
                )
        else:
            # No target_date: filter out expired instruments only
            today = datetime.now(timezone.utc).date()
            original_count = len(available_symbols)
            available_symbols = [
                symbol
                for symbol in available_symbols
                if self._is_instrument_currently_active(symbol, today)
            ]
            date_filtered_count = original_count - len(available_symbols)

        return available_symbols, date_filtered_count

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid (TTL-based)."""
        if cache_key not in self._cache:
            return False

        cache_time = self._cache_timestamps.get(cache_key)
        if not cache_time:
            return False

        age = datetime.now(timezone.utc) - cache_time
        return age < self._cache_ttl

    def _is_instrument_available_on_date(
        self,
        available_since: str,
        available_to: str,
        target_date_str: str,
        symbol: Dict[str, Any],
    ) -> bool:
        """
        Check if instrument was available on target date.

        Args:
            available_since: ISO datetime string when instrument became available
            available_to: ISO datetime string when instrument expires (empty for perpetuals)
            target_date_str: Target date in YYYY-MM-DD format
            symbol: Symbol dict from Tardis API

        Returns:
            True if instrument was available on target date
        """
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

            # Parse availableSince
            if available_since:
                try:
                    since_date = datetime.fromisoformat(
                        available_since.replace("Z", "+00:00")
                    ).date()
                    if target_date < since_date:
                        return False
                except (ValueError, AttributeError):
                    pass

            # Parse availableTo
            if available_to:
                try:
                    to_date = datetime.fromisoformat(
                        available_to.replace("Z", "+00:00")
                    ).date()
                    if target_date > to_date:
                        return False
                except (ValueError, AttributeError):
                    pass

            return True

        except Exception as e:
            logger.warning(
                f"Error checking date availability for {symbol.get('id', 'unknown')}: {e}"
            )
            return True  # Default to available if parsing fails

    def _is_instrument_currently_active(
        self, symbol: Dict[str, Any], today: date
    ) -> bool:
        """
        Check if instrument is currently active (not expired).

        Args:
            symbol: Symbol dict from Tardis API
            today: Today's date

        Returns:
            True if instrument is currently active
        """
        available_to = symbol.get("availableTo", "")
        if not available_to:
            return True  # Perpetuals don't have availableTo

        try:
            to_date = datetime.fromisoformat(available_to.replace("Z", "+00:00")).date()
            return today <= to_date
        except (ValueError, AttributeError):
            return True  # Default to active if parsing fails

    def clear_cache(self):
        """Clear all cached data."""
        cache_count = len(self._cache)
        self._cache.clear()
        self._cache_timestamps.clear()
        logger.info(f"🧹 Cleared {cache_count} cached exchange datasets")

    def cleanup(self):
        """Cleanup resources and close connections."""
        if hasattr(self, "session"):
            self.session.close()
        self.clear_cache()
        logger.info("🧹 TardisAdapter cleanup completed")
