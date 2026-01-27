"""
Tardis Venue Adapter - REFACTORED

Fetches crypto exchange instrument definitions from Tardis API.
Supports Binance, Bybit, OKX, Deribit, Upbit, Coinbase, and other crypto exchanges.

ARCHITECTURE:
- Uses TardisBaseClient from unified-cloud-services (centralized network layer)
- This adapter handles domain-specific logic (instrument parsing, date filtering)
- Network concerns (sessions, retries, API keys) are handled by TardisBaseClient

This adapter abstracts Tardis-specific logic from InstrumentProcessingService,
making the architecture consistent with Databento and DeFi adapters.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta, date

from unified_cloud_services import TardisBaseClient, TardisClientConfig
from unified_cloud_services.clients.tardis_base_client import (
    get_cached_instruments,
    set_cached_instruments,
    clear_instruments_cache,
    clear_tardis_api_key_cache,
)
from instruments_service.config import instruments_config


logger = logging.getLogger(__name__)


# =============================================================================
# MANUAL METADATA OVERRIDES
# =============================================================================
# Some instruments have incorrect metadata in Tardis API (e.g., missing availableTo
# for delisted tokens). We manually correct these based on known events.
#
# Format: {exchange: {symbol_id: {"availableTo": "ISO_DATE"}}}
#
# FTT (FTX Token) - FTX collapsed November 8-11, 2022. Trading halted on most
# exchanges by November 16, 2022. Tardis incorrectly reports availableTo=None
# for fttusdt on binance, even though no data exists after the collapse.
# =============================================================================
TARDIS_METADATA_OVERRIDES: Dict[str, Dict[str, Dict[str, str]]] = {
    "binance": {
        "fttusdt": {"availableTo": "2022-11-16T00:00:00.000Z"},
    },
    "bybit": {
        "fttusdt": {"availableTo": "2022-11-16T00:00:00.000Z"},
    },
    "okx": {
        "ftt-usdt": {"availableTo": "2022-11-16T00:00:00.000Z"},
        "ftt-usdt-swap": {"availableTo": "2022-11-16T00:00:00.000Z"},
    },
}


def clear_tardis_cache():
    """Clear module-level cache (useful for testing or credential rotation)"""
    clear_tardis_api_key_cache()
    clear_instruments_cache()
    logger.info("🧹 Cleared Tardis module-level cache")


class TardisAdapter:
    """
    Adapter for fetching crypto exchange instrument definitions from Tardis API.

    Uses TardisBaseClient for network management (sessions, retries, API keys).
    This adapter focuses on domain-specific logic:
    - Instrument parsing
    - Date availability filtering
    - Response caching (1-hour TTL)

    Supports:
    - Binance (spot, futures)
    - Bybit (spot, perpetuals)
    - OKX (spot, futures, swaps)
    - Deribit (futures, options)
    - Upbit (spot) - Korean exchange
    - Coinbase (spot)
    - Other crypto exchanges via Tardis
    """

    def __init__(self, api_key: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize Tardis adapter using centralized TardisBaseClient.

        Args:
            api_key: Tardis API key (optional, TardisBaseClient handles Secret Manager)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Create config with instruments-service specific settings
        config = TardisClientConfig(
            secret_name=instruments_config.tardis_secret_name,
            fallback_env_var="TARDIS_API_KEY",
        )

        # Initialize centralized base client
        self._base_client = TardisBaseClient(
            config=config,
            api_key=api_key,
            project_id=project_id or instruments_config.gcp_project_id,
        )

        # Cache for exchange instruments (TTL: 1 hour)
        self._cache: Dict[str, List[Dict]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(hours=1)

        logger.info("✅ TardisAdapter initialized (using TardisBaseClient)")

    def _apply_metadata_overrides(
        self, exchange: str, symbols: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply manual metadata corrections for instruments with known Tardis API issues.

        Args:
            exchange: Exchange name (lowercase)
            symbols: List of symbol dicts from Tardis API

        Returns:
            Updated symbols list with corrected metadata
        """
        overrides = TARDIS_METADATA_OVERRIDES.get(exchange, {})
        if not overrides:
            return symbols

        corrected_count = 0
        for symbol in symbols:
            symbol_id = symbol.get("id", "").lower()
            if symbol_id in overrides:
                for key, value in overrides[symbol_id].items():
                    old_value = symbol.get(key)
                    symbol[key] = value
                    corrected_count += 1
                    logger.info(
                        f"🔧 Manual override: {exchange}:{symbol_id} {key}: {old_value} → {value}"
                    )

        if corrected_count > 0:
            logger.info(f"📝 Applied {corrected_count} manual metadata corrections for {exchange}")

        return symbols

    def warmup(self) -> bool:
        """
        Warmup connection by making a lightweight request.

        Returns:
            bool: True if warmup successful
        """
        return self._base_client.sync_warmup()

    def fetch_exchange_instruments(
        self,
        exchange: str,
        target_date: Optional[datetime] = None,
        force_refresh: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fetch instrument data from Tardis API for specific exchange.

        Args:
            exchange: Exchange name (e.g., 'binance-futures', 'bybit', 'okx', 'upbit', 'coinbase')
            target_date: Target date for instrument availability filtering
            force_refresh: If True, bypass cache and fetch fresh data

        Returns:
            Tuple of (available_symbols list, date_filtered_count)
        """
        target_date = target_date or datetime.now(timezone.utc)
        date_str = target_date.strftime("%Y-%m-%d")

        # CRITICAL: Tardis API expects lowercase exchange names
        # Convert to lowercase to handle cases where canonical venue names (UPBIT, COINBASE) are passed
        exchange = exchange.lower()

        # Check MODULE-LEVEL cache first (persists across days/runs)
        # This saves ~1-2s per exchange by avoiding repeated Tardis API calls
        if not force_refresh:
            cached = get_cached_instruments(exchange)
            if cached is not None:
                logger.info(f"📋 Using module-level cached Tardis data for {exchange} ({len(cached)} instruments)")
                # Apply overrides to cached data (in case cache was populated before fix)
                available_symbols = self._apply_metadata_overrides(exchange, cached)
            else:
                available_symbols = None
        else:
            available_symbols = None

        # Also check instance-level cache (for current session)
        cache_key = f"{exchange}_instruments"
        if available_symbols is None and not force_refresh and self._is_cache_valid(cache_key):
            logger.info(f"📋 Using instance-cached Tardis data for {exchange}")
            # Apply overrides to cached data (in case cache was populated before fix)
            available_symbols = self._apply_metadata_overrides(exchange, self._cache[cache_key])

        if available_symbols is None:
            # Fetch fresh data from Tardis API using centralized client
            url = f"{self._base_client.config.api_base_url}/exchanges/{exchange}"

            try:
                logger.info(f"🔍 Fetching instruments from Tardis API: {exchange}")

                response = self._base_client.sync_get(url, timeout=60)
                response.raise_for_status()

                exchange_info = response.json()
                available_symbols = exchange_info.get("availableSymbols", [])

                # Apply manual metadata corrections for known Tardis API issues
                available_symbols = self._apply_metadata_overrides(exchange, available_symbols)

                # Cache at both levels for performance (with corrections applied)
                self._cache[cache_key] = available_symbols
                self._cache_timestamps[cache_key] = datetime.now(timezone.utc)
                set_cached_instruments(exchange, available_symbols)

                logger.info(
                    f"✅ Fetched & cached {len(available_symbols)} instruments from {exchange}"
                )
            except Exception as e:
                logger.error(f"❌ Tardis API failed for {exchange}: {e}")
                return [], 0

        # Filter by date availability
        # OPTIMIZATION: Use list comprehension instead of for loop (faster for large lists)
        date_filtered_count = 0
        if target_date:
            original_count = len(available_symbols)

            available_symbols = [
                symbol
                for symbol in available_symbols
                if self._is_instrument_available_on_date(
                    symbol.get("availableSince", ""),
                    symbol.get("availableTo", ""),
                    date_str,
                    symbol,
                )
            ]

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
                    to_date = datetime.fromisoformat(available_to.replace("Z", "+00:00")).date()
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

    def _is_instrument_currently_active(self, symbol: Dict[str, Any], today: date) -> bool:
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
        self._base_client.cleanup()
        self.clear_cache()
        logger.info("🧹 TardisAdapter cleanup completed")
