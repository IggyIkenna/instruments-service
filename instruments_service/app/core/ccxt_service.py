"""
Centralized CCXT Service

Provides centralized CCXT integration for all venues that support it.
Caches markets per venue for performance optimization.

Used by:
- HyperliquidAdapter
- AsterAdapter
- TardisAdapter (via InstrumentProcessingService)
- InstrumentProcessingService (for metadata enrichment)
"""

import logging
import json
import ccxt
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from instruments_service.config import VenueMapping

logger = logging.getLogger(__name__)


class CCXTService:
    """
    Centralized CCXT service for market data and metadata.

    Provides:
    - Exchange initialization
    - Market loading with caching
    - Metadata extraction (tick_size, min_size, contract_size)
    """

    def __init__(self, venue_mapping: VenueMapping, cache_ttl_hours: int = 4):
        """
        Initialize CCXT service.

        Args:
            venue_mapping: VenueMapping instance for venue-to-CCXT mapping
            cache_ttl_hours: Cache TTL in hours (default: 4)
        """
        self.venue_mapping = venue_mapping
        self.cache_ttl_hours = cache_ttl_hours

        # Cache markets per venue
        self._markets_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

        # Cache leverage tiers per venue (to avoid repeated API calls)
        self._leverage_tiers_cache: Dict[str, Dict[str, Any]] = {}

        logger.info(f"✅ CCXTService initialized (cache TTL: {cache_ttl_hours}h)")

    def get_ccxt_exchange(self, venue: str) -> Optional[ccxt.Exchange]:
        """
        Get CCXT exchange instance for a venue.

        Args:
            venue: Venue identifier (e.g., 'HYPERLIQUID', 'ASTER', 'BINANCE')

        Returns:
            CCXT exchange instance or None if not supported
        """
        ccxt_exchange_id = self.venue_mapping.venue_to_ccxt.get(venue)
        if not ccxt_exchange_id:
            logger.debug(f"No CCXT mapping for venue: {venue}")
            return None

        exchange_class = getattr(ccxt, ccxt_exchange_id, None)
        if not exchange_class:
            logger.debug(f"CCXT exchange not available: {ccxt_exchange_id}")
            return None

        return exchange_class(
            {
                "enableRateLimit": True,
                "timeout": 15000,  # 15s timeout for initial load
            }
        )

    def load_markets(self, venue: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Load markets for a venue with caching.

        Args:
            venue: Venue identifier
            force_refresh: If True, bypass cache and reload

        Returns:
            Dictionary with 'exchange', 'markets', 'exchange_id' or None
        """
        ccxt_exchange_id = self.venue_mapping.venue_to_ccxt.get(venue)
        if not ccxt_exchange_id:
            logger.debug(f"No CCXT mapping for venue: {venue}")
            return None

        # Check cache
        cache_key = f"{venue}_{ccxt_exchange_id}"
        if not force_refresh and self._is_cache_valid(cache_key):
            logger.debug(
                f"📋 Using cached CCXT markets for {venue} ({len(self._markets_cache[cache_key]['markets'])} markets)"
            )
            return self._markets_cache[cache_key]

        # Initialize exchange
        exchange = self.get_ccxt_exchange(venue)
        if not exchange:
            return None

        try:
            # Load markets ONCE per exchange (major performance optimization)
            markets = exchange.load_markets()
            logger.info(
                f"⚡ Loaded {len(markets)} CCXT markets for {venue} ({ccxt_exchange_id}) - CACHED for reuse"
            )

            # Cache the results
            ccxt_data = {
                "exchange": exchange,
                "markets": markets,
                "exchange_id": ccxt_exchange_id,
            }

            self._markets_cache[cache_key] = ccxt_data
            self._cache_timestamps[cache_key] = datetime.now()

            return ccxt_data

        except Exception as e:
            logger.debug(f"CCXT data unavailable for {venue}: {e}")
            return None

    def _build_symbol_formats(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: Optional[str] = None,
    ) -> list:
        """
        Build possible CCXT symbol formats for a venue.

        Args:
            venue: Venue identifier
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier
            tardis_symbol: Optional Tardis symbol format for better matching

        Returns:
            List of possible symbol formats to try
        """
        possible_symbols = []
        tardis_symbol = tardis_symbol or symbol_id

        if venue == "BYBIT" and base_asset and quote_asset:
            # Bybit perpetuals use BASE/QUOTE:QUOTE format
            possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}")

            # Handle compound symbols that likely don't exist as perpetuals in CCXT
            if len(base_asset) > 5:  # Compound symbols like ETHBTC, SHIB1000
                logger.debug(f"🔍 Bybit compound symbol (likely unavailable in CCXT): {base_asset}")

                # Special mappings for known variations
                special_mappings = {
                    "SHIB1000": "1000SHIB",  # SHIB1000 → 1000SHIB
                    "LUNA2": "LUNC",  # LUNA2 → LUNC
                    "PEPE1000": "1000PEPE",  # 1000x pattern
                }

                if base_asset in special_mappings:
                    alt_base = special_mappings[base_asset]
                    possible_symbols.extend(
                        [
                            f"{alt_base}/{quote_asset}:{quote_asset}",
                            f"{alt_base}/{quote_asset}",
                        ]
                    )

            # Standard Bybit formats
            possible_symbols.extend(
                [
                    f"{base_asset}/{quote_asset}",  # Spot format: BTC/USDT
                    f"{base_asset}{quote_asset}",  # Compressed: BTCUSDT
                ]
            )

        elif venue == "HYPERLIQUID":
            # Hyperliquid CCXT formats: BTC/USDC:USDC for perpetuals
            if base_asset and quote_asset:
                possible_symbols.extend(
                    [
                        f"{base_asset}/{quote_asset}:{quote_asset}",  # BTC/USDC:USDC (perpetual)
                        f"{base_asset}/{quote_asset}",  # BTC/USDC (spot, if exists)
                    ]
                )

        elif venue == "ASTER":
            # Aster doesn't have CCXT support, return empty
            pass

        elif venue == "DERIBIT":
            # Deribit CCXT formats: BTC/USD:BTC for perpetuals
            if "PERPETUAL" in tardis_symbol:
                if quote_asset == "USD":
                    possible_symbols.append(f"{base_asset}/USD:{base_asset}")  # Inverse
                elif quote_asset in ["USDC", "USDT"]:
                    possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}")
            elif "OPTION" in tardis_symbol or "-C" in tardis_symbol or "-P" in tardis_symbol:
                # Deribit options: BTC/USD:BTC-25DEC25-50000-C
                possible_symbols.append(f"{base_asset}/{quote_asset}:{tardis_symbol}")
            elif "FUTURE" in tardis_symbol or any(
                month in tardis_symbol for month in ["JAN", "FEB", "MAR", "DEC"]
            ):
                # Deribit futures: BTC/USD:BTC-25DEC25
                possible_symbols.append(f"{base_asset}/{quote_asset}:{tardis_symbol}")

        # Standard formats for all venues
        if base_asset and quote_asset:
            possible_symbols.extend(
                [
                    f"{base_asset}/{quote_asset}",  # Standard: BTC/USDT
                    f"{base_asset}{quote_asset}",  # Binance: BTCUSDT
                    f"{base_asset}-{quote_asset}",  # Alternative dash format
                ]
            )

        # Add original symbols
        possible_symbols.extend(
            [
                tardis_symbol,
                symbol_id.upper(),
                symbol_id.lower(),
            ]
        )

        return possible_symbols

    def get_metadata(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get CCXT metadata (tick_size, min_size, contract_size) for an instrument.

        Args:
            venue: Venue identifier
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier for lookup
            tardis_symbol: Optional Tardis symbol format for better matching

        Returns:
            Dictionary with metadata fields or empty dict
        """
        ccxt_data = self.load_markets(venue)
        if not ccxt_data or not ccxt_data.get("markets"):
            return {}

        markets = ccxt_data["markets"]

        # Build possible symbol formats based on venue
        possible_symbols = self._build_symbol_formats(
            venue, base_asset, quote_asset, symbol_id, tardis_symbol
        )

        # Try to find market in CCXT
        ccxt_market = None
        for symbol_format in possible_symbols:
            if symbol_format in markets:
                ccxt_market = markets[symbol_format]
                break

        if not ccxt_market:
            logger.debug(
                f"No CCXT market found for {venue}:{symbol_id} (tried {len(possible_symbols)} formats)"
            )
            return {}

        # Extract metadata from CCXT market
        precision = ccxt_market.get("precision", {})
        limits = ccxt_market.get("limits", {})

        metadata = {}

        # Tick size (price precision)
        if "price" in precision:
            tick_size_val = precision["price"]
            if tick_size_val:
                metadata["tick_size"] = str(tick_size_val)

        # Min size (amount precision)
        if "amount" in precision:
            min_size_val = precision["amount"]
            if min_size_val:
                metadata["min_size"] = str(min_size_val)
        elif "cost" in limits and "min" in limits["cost"]:
            # Some exchanges use cost_min instead
            cost_min = limits["cost"]["min"]
            if cost_min:
                metadata["min_size"] = f"cost_min:{cost_min}"

        # Contract size
        contract_size_val = ccxt_market.get("contractSize")
        if contract_size_val:
            metadata["contract_size"] = float(contract_size_val)

        return metadata

    def get_leverage_limits(
        self,
        venue: str,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get leverage limits and risk parameters from CCXT leverage tiers.

        Uses caching per venue to avoid repeated API calls for the same exchange.

        Args:
            venue: Venue identifier
            symbol: CCXT symbol format (e.g., 'BTC/USDT:USDT')
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier for lookup
            tardis_symbol: Optional Tardis symbol format for better matching

        Returns:
            Dictionary with risk parameters: max_leverage, max_position_size,
            initial_margin_rate, maintenance_margin_rate, leverage_tiers_json
        """
        # Check cache first (leverage tiers are usually the same per venue)
        if venue in self._leverage_tiers_cache:
            cached_params = self._leverage_tiers_cache[venue]
            logger.debug(f"Using cached leverage tiers for {venue}")
            return cached_params.copy()  # Return copy to avoid mutation

        ccxt_data = self.load_markets(venue)
        if not ccxt_data or not ccxt_data.get("exchange"):
            # Use fallback and cache it
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params

        exchange = ccxt_data["exchange"]

        # Check if exchange supports fetchMarketLeverageTiers
        if not hasattr(exchange, "fetchMarketLeverageTiers"):
            logger.debug(f"Exchange {venue} does not support fetchMarketLeverageTiers")
            # Try fallback to Context7 documentation lookup
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params

        try:
            # Build possible symbol formats
            possible_symbols = self._build_symbol_formats(
                venue, base_asset, quote_asset, symbol_id, tardis_symbol
            )

            # Try to fetch leverage tiers for each symbol format
            leverage_tiers = None
            matched_symbol = None

            for symbol_format in possible_symbols:
                try:
                    tiers = exchange.fetchMarketLeverageTiers(symbol_format)
                    if tiers:
                        leverage_tiers = tiers
                        matched_symbol = symbol_format
                        break
                except Exception as e:
                    logger.debug(f"Failed to fetch leverage tiers for {symbol_format}: {e}")
                    continue

            if not leverage_tiers:
                logger.debug(
                    f"No leverage tiers found for {venue}:{symbol_id} (tried {len(possible_symbols)} formats)"
                )
                # Try fallback and cache it
                fallback_params = self._get_leverage_limits_fallback(venue)
                if fallback_params:
                    self._leverage_tiers_cache[venue] = fallback_params
                return fallback_params

            # Extract risk parameters from leverage tiers
            risk_params = self._extract_risk_params_from_tiers(leverage_tiers)

            # Cache the results for this venue
            self._leverage_tiers_cache[venue] = risk_params

            logger.info(
                f"✅ Fetched and cached leverage tiers for {venue}: "
                f"max_leverage={risk_params.get('max_leverage')}, "
                f"max_position_size={risk_params.get('max_position_size')}"
            )

            return risk_params

        except Exception as e:
            logger.debug(f"Error fetching leverage tiers for {venue}:{symbol_id}: {e}")
            # Try fallback and cache it
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params

    def _extract_risk_params_from_tiers(self, leverage_tiers: list) -> Dict[str, Any]:
        """
        Extract risk parameters from CCXT leverage tiers structure.

        Args:
            leverage_tiers: List of leverage tier dictionaries from CCXT

        Returns:
            Dictionary with extracted risk parameters
        """
        if not leverage_tiers or len(leverage_tiers) == 0:
            return {}

        risk_params = {}

        # Get tier 1 (first tier, typically has highest leverage)
        tier_1 = leverage_tiers[0] if leverage_tiers else None

        if tier_1:
            # Extract from tier 1
            risk_params["max_leverage"] = tier_1.get("maxLeverage")
            risk_params["initial_margin_rate"] = tier_1.get("initialMargin")
            risk_params["maintenance_margin_rate"] = tier_1.get("maintenanceMargin")

        # Get highest tier (largest maxNotional = max position size)
        highest_tier = None
        max_notional = 0

        for tier in leverage_tiers:
            tier_max_notional = tier.get("maxNotional", 0)
            if isinstance(tier_max_notional, (int, float)):
                if tier_max_notional > max_notional:
                    max_notional = tier_max_notional
                    highest_tier = tier

        if highest_tier:
            risk_params["max_position_size"] = max_notional

        # Serialize all tiers to JSON for advanced calculations
        try:
            # Convert tiers to serializable format
            tiers_serializable = []
            for tier in leverage_tiers:
                tier_dict = {
                    "tier": tier.get("tier"),
                    "minNotional": tier.get("minNotional"),
                    "maxNotional": tier.get("maxNotional"),
                    "initialMargin": tier.get("initialMargin"),
                    "maintenanceMargin": tier.get("maintenanceMargin"),
                    "maxLeverage": tier.get("maxLeverage"),
                }
                tiers_serializable.append(tier_dict)

            risk_params["leverage_tiers_json"] = json.dumps(tiers_serializable)
        except Exception as e:
            logger.debug(f"Error serializing leverage tiers to JSON: {e}")

        return risk_params

    def _get_leverage_limits_fallback(self, venue: str) -> Dict[str, Any]:
        """
        Fallback method to get exchange-specific default leverage limits.

        Uses hardcoded defaults based on common exchange limits.
        In the future, this could use Context7 documentation lookup.

        Args:
            venue: Venue identifier

        Returns:
            Dictionary with default risk parameters
        """
        # Exchange-specific defaults (from common knowledge and Context7 docs)
        # These are conservative defaults - actual values should come from API when available
        exchange_defaults = {
            "BINANCE-FUTURES": {
                "max_leverage": 125.0,
                "initial_margin_rate": 0.008,  # ~1/125
                "maintenance_margin_rate": 0.004,  # ~0.4%
            },
            "BYBIT": {
                "max_leverage": 100.0,
                "initial_margin_rate": 0.01,  # 1/100
                "maintenance_margin_rate": 0.005,  # 0.5%
            },
            "OKX": {
                "max_leverage": 125.0,
                "initial_margin_rate": 0.008,  # ~1/125
                "maintenance_margin_rate": 0.004,  # ~0.4%
            },
            "DERIBIT": {
                "max_leverage": 100.0,
                "initial_margin_rate": 0.01,  # 1/100
                "maintenance_margin_rate": 0.005,  # 0.5%
            },
        }

        defaults = exchange_defaults.get(venue, {})
        if defaults:
            logger.debug(
                f"Using fallback defaults for {venue}: max_leverage={defaults.get('max_leverage')}"
            )

        return defaults

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid."""
        if cache_key not in self._markets_cache:
            return False
        if cache_key not in self._cache_timestamps:
            return False

        cache_age = datetime.now() - self._cache_timestamps[cache_key]
        return cache_age < timedelta(hours=self.cache_ttl_hours)

    def clear_cache(self, venue: Optional[str] = None):
        """
        Clear cache for a venue or all venues.

        Args:
            venue: Venue to clear cache for, or None to clear all
        """
        if venue:
            ccxt_exchange_id = self.venue_mapping.venue_to_ccxt.get(venue)
            if ccxt_exchange_id:
                cache_key = f"{venue}_{ccxt_exchange_id}"
                self._markets_cache.pop(cache_key, None)
                self._cache_timestamps.pop(cache_key, None)
                logger.info(f"Cleared CCXT cache for {venue}")
        else:
            self._markets_cache.clear()
            self._cache_timestamps.clear()
            logger.info("Cleared all CCXT cache")
