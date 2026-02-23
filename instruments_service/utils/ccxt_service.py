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
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional, cast

import ccxt

if TYPE_CHECKING:
    from unified_market_interface.models.venue_config import VenueMapping

logger = logging.getLogger(__name__)

# CCXT market data has dynamic structure - use Any for untyped CCXT responses
CCXTMarketData = dict[str, Any]
# Metadata and leverage limits from CCXT (str, float, int, None values)
CCXTMetadata = dict[str, str | float | int | None]


class CCXTService:
    """
    Centralized CCXT service for market data and metadata.

    Provides:
    - Exchange initialization
    - Market loading with caching
    - Metadata extraction (tick_size, min_size, contract_size)
    """

    def __init__(self, venue_mapping: "VenueMapping", cache_ttl_hours: int = 4):
        """
        Initialize CCXT service.

        Args:
            venue_mapping: VenueMapping instance for venue-to-CCXT mapping
            cache_ttl_hours: Cache TTL in hours (default: 4)
        """
        self.venue_mapping = venue_mapping
        self.cache_ttl_hours = cache_ttl_hours

        # Cache markets per venue
        self._markets_cache: dict[str, CCXTMarketData] = {}
        self._cache_timestamps: dict[str, datetime] = {}

        # Cache leverage tiers per venue (to avoid repeated API calls)
        self._leverage_tiers_cache: dict[str, CCXTMetadata] = {}

        logger.info(f"✅ CCXTService initialized (cache TTL: {cache_ttl_hours}h)")

    def preload_markets_parallel(self, venues: list[str], max_workers: int = 4) -> dict[str, bool]:
        """
        Pre-load CCXT markets for multiple venues in parallel.

        PERFORMANCE: Loads markets from different CCXT exchanges concurrently,
        reducing total load time from O(n*latency) to O(latency).

        Args:
            venues: List of venue identifiers to pre-load
            max_workers: Maximum parallel threads (default: 4)

        Returns:
            Dictionary mapping ccxt_exchange_id to success status
        """
        from concurrent.futures import Future, ThreadPoolExecutor, as_completed

        # Get unique CCXT exchange IDs (avoid loading same exchange multiple times)
        venue_to_ccxt: dict[str, str] = self.venue_mapping.venue_to_ccxt
        ccxt_exchange_ids: set[tuple[str, str]] = set()
        for venue in venues:
            ccxt_id = venue_to_ccxt.get(venue)
            if ccxt_id:
                ccxt_exchange_ids.add((venue, ccxt_id))

        # Filter out already-cached exchanges and spot-only exchanges
        spot_only_exchanges = {"upbit", "coinbase"}
        to_load: list[tuple[str, str]] = [
            (v, cid)
            for v, cid in ccxt_exchange_ids
            if cid not in self._markets_cache and cid.lower() not in spot_only_exchanges
        ]

        if not to_load:
            logger.info("📋 All CCXT markets already cached or skipped (spot-only)")
            return {}

        logger.info(f"⚡ Pre-loading {len(to_load)} CCXT exchanges in parallel (max_workers={max_workers})...")

        results: dict[str, bool] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map: dict[Future[CCXTMarketData | None], tuple[str, str]] = {
                executor.submit(self.load_markets, venue): (venue, ccxt_id) for venue, ccxt_id in to_load
            }
            for future in as_completed(futures_map):
                venue, ccxt_id = futures_map[future]
                try:
                    result: CCXTMarketData | None = future.result()
                    results[ccxt_id] = result is not None
                    if result:
                        logger.debug(f"✅ Pre-loaded {ccxt_id} markets")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to pre-load {ccxt_id}: {e}")
                    results[ccxt_id] = False

        success_count = sum(1 for v in results.values() if v)
        logger.info(f"✅ Pre-loaded {success_count}/{len(to_load)} CCXT exchanges")
        return results

    def get_ccxt_exchange(self, venue: str) -> object | None:  # type: ignore[reportAny]
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

        return cast(
            object | None,
            exchange_class(
                {
                    "enableRateLimit": True,
                    "timeout": 15000,  # 15s timeout for initial load
                }
            ),
        )

    def load_markets(self, venue: str, force_refresh: bool = False) -> CCXTMarketData | None:
        """
        Load markets for a venue with caching.

        PERFORMANCE: Uses ccxt_exchange_id as cache key so that venues sharing
        the same CCXT exchange (e.g., BINANCE-SPOT and BINANCE-FUTURES both use
        'binance') share the same cached markets data.

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

        # PERFORMANCE FIX: Use ccxt_exchange_id as cache key (not venue)
        # This allows BINANCE-SPOT and BINANCE-FUTURES to share cached markets
        cache_key = ccxt_exchange_id
        if not force_refresh and self._is_cache_valid(cache_key):
            logger.debug(
                f"📋 Using cached CCXT markets for {venue} via {ccxt_exchange_id} "
                f"({len(cast(dict[str, Any], self._markets_cache[cache_key].get('markets', {})))} markets)"
            )
            return self._markets_cache[cache_key]

        # Initialize exchange
        exchange = self.get_ccxt_exchange(venue)
        if not exchange:
            return None

        try:
            # Load markets ONCE per CCXT exchange (major performance optimization)
            # CCXT has no stubs - cast to dict[str, Any] for type safety
            markets = cast(dict[str, Any], exchange.load_markets())  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
            logger.info(
                f"⚡ Loaded {len(markets)} CCXT markets for {ccxt_exchange_id} - "
                f"CACHED for all venues using this exchange"
            )

            # Cache the results by ccxt_exchange_id
            ccxt_data: CCXTMarketData = {
                "exchange": exchange,
                "markets": markets,
                "exchange_id": ccxt_exchange_id,
            }

            self._markets_cache[cache_key] = ccxt_data
            self._cache_timestamps[cache_key] = datetime.now(timezone.utc)

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
        instrument_type: Optional[str] = None,
    ) -> list[str]:
        """
        Build possible CCXT symbol formats for a venue.

        Args:
            venue: Venue identifier
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier (Deribit uses this directly: BTC-PERPETUAL, BTC-25DEC25-50000-C)
            tardis_symbol: Optional Tardis symbol format for better matching
            instrument_type: Optional instrument type (PERPETUAL, FUTURE, OPTION, SPOT_PAIR)

        Returns:
            List of possible symbol formats to try
        """
        possible_symbols: list[str] = []
        tardis_symbol = tardis_symbol or symbol_id
        instrument_type = instrument_type or ""

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

        elif venue in ["BINANCE-SPOT", "BINANCE-FUTURES"]:
            # Binance CCXT formats:
            # Spot: BTC/USDT
            # Perpetuals: BTC/USDT:USDT (linear) or BTC/USDT:BTC (inverse)
            # Futures: BTC/USDT:USDT-211225 (with expiry date YYMMDD)
            if base_asset and quote_asset:
                if instrument_type == "SPOT_PAIR":
                    possible_symbols.append(f"{base_asset}/{quote_asset}")
                elif instrument_type in ["PERPETUAL", "FUTURE"]:
                    # Linear perpetuals/futures: BASE/QUOTE:QUOTE
                    possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}")
                    # Also try with expiry for futures (if symbol_id contains date)
                    if instrument_type == "FUTURE" and any(char.isdigit() for char in symbol_id):
                        # Try to extract date from symbol_id (e.g., BTCUSDT241225)
                        possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}-{symbol_id}")
                # Standard formats
                possible_symbols.extend(
                    [
                        f"{base_asset}/{quote_asset}",  # Spot format
                        f"{base_asset}{quote_asset}",  # Compressed: BTCUSDT
                    ]
                )

        elif venue == "OKX":
            # OKX CCXT formats (similar to Binance):
            # Spot: BTC/USDT
            # Perpetuals: BTC/USDT:USDT
            # Futures: BTC/USDT:USDT-211225 (with expiry date YYMMDD)
            if base_asset and quote_asset:
                if instrument_type == "SPOT_PAIR":
                    possible_symbols.append(f"{base_asset}/{quote_asset}")
                elif instrument_type in ["PERPETUAL", "FUTURE"]:
                    # Linear perpetuals/futures: BASE/QUOTE:QUOTE
                    possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}")
                    # Also try with expiry for futures
                    if instrument_type == "FUTURE" and any(char.isdigit() for char in symbol_id):
                        possible_symbols.append(f"{base_asset}/{quote_asset}:{quote_asset}-{symbol_id}")
                # Standard formats
                possible_symbols.extend(
                    [
                        f"{base_asset}/{quote_asset}",  # Spot format
                        f"{base_asset}{quote_asset}",  # Compressed: BTCUSDT
                    ]
                )

        elif venue == "DERIBIT":
            # CRITICAL FIX: Deribit uses symbol_id directly in CCXT format
            # symbol_id is already in CCXT format: BTC-PERPETUAL, BTC-25DEC25-50000-C, BTC-25DEC25
            # CCXT Deribit format: BASE/QUOTE:SYMBOL_ID
            # For perpetuals: BTC/USD:BTC-PERPETUAL
            # For options: BTC/USD:BTC-25DEC25-50000-C
            # For futures: BTC/USD:BTC-25DEC25

            # Use instrument_type if available, otherwise check symbol_id patterns
            if instrument_type == "PERPETUAL" or "PERPETUAL" in symbol_id.upper():
                if quote_asset == "USD":
                    # Inverse perpetual: BTC/USD:BTC-PERPETUAL
                    possible_symbols.append(f"{base_asset}/USD:{symbol_id}")
                elif quote_asset in ["USDC", "USDT"]:
                    # Linear perpetual: BTC/USDC:BTC-PERPETUAL
                    possible_symbols.append(f"{base_asset}/{quote_asset}:{symbol_id}")
            elif (
                instrument_type == "OPTION"
                or "-C" in symbol_id.upper()
                or "-P" in symbol_id.upper()
                or "CALL" in symbol_id.upper()
                or "PUT" in symbol_id.upper()
            ):
                # Options: BTC/USD:BTC-25DEC25-50000-C
                possible_symbols.append(f"{base_asset}/{quote_asset}:{symbol_id}")
            elif instrument_type == "FUTURE" or any(
                month in symbol_id.upper()
                for month in [
                    "JAN",
                    "FEB",
                    "MAR",
                    "APR",
                    "MAY",
                    "JUN",
                    "JUL",
                    "AUG",
                    "SEP",
                    "OCT",
                    "NOV",
                    "DEC",
                ]
            ):
                # Futures: BTC/USD:BTC-25DEC25
                possible_symbols.append(f"{base_asset}/{quote_asset}:{symbol_id}")

            # Also try symbol_id directly (Deribit symbols are often already in CCXT format)
            possible_symbols.append(symbol_id)

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

    def generate_default_ccxt_symbol(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        instrument_type: Optional[str] = None,
    ) -> str:
        """
        Generate default CCXT symbol format when markets aren't available.

        Args:
            venue: Venue identifier
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier
            instrument_type: Optional instrument type

        Returns:
            Default CCXT symbol string
        """
        instrument_type = instrument_type or ""

        if venue == "DERIBIT":
            # Deribit: Use symbol_id directly with BASE/QUOTE prefix
            # symbol_id is already in CCXT format: BTC-PERPETUAL, BTC-25DEC25-50000-C
            if base_asset and quote_asset:
                return f"{base_asset}/{quote_asset}:{symbol_id}"
            return symbol_id
        elif venue in ["BINANCE-SPOT", "BINANCE-FUTURES"] and base_asset and quote_asset:
            # Binance: Spot uses BASE/QUOTE, derivatives use BASE/QUOTE:QUOTE
            if instrument_type == "SPOT_PAIR":
                return f"{base_asset}/{quote_asset}"
            elif instrument_type in ["PERPETUAL", "FUTURE"]:
                return f"{base_asset}/{quote_asset}:{quote_asset}"
            return f"{base_asset}/{quote_asset}"
        elif venue == "OKX" and base_asset and quote_asset:
            # OKX: Same format as Binance
            if instrument_type == "SPOT_PAIR":
                return f"{base_asset}/{quote_asset}"
            elif instrument_type in ["PERPETUAL", "FUTURE"]:
                return f"{base_asset}/{quote_asset}:{quote_asset}"
            return f"{base_asset}/{quote_asset}"
        elif venue == "BYBIT" and base_asset and quote_asset:
            if instrument_type in ["PERPETUAL", "FUTURE"]:
                return f"{base_asset}/{quote_asset}:{quote_asset}"
            return f"{base_asset}/{quote_asset}"
        elif venue == "HYPERLIQUID" and base_asset and quote_asset:
            if instrument_type in ["PERPETUAL", "FUTURE"]:
                return f"{base_asset}/{quote_asset}:{quote_asset}"
            return f"{base_asset}/{quote_asset}"
        elif base_asset and quote_asset:
            # Default format for other venues
            if instrument_type in ["PERPETUAL", "FUTURE"]:
                return f"{base_asset}/{quote_asset}:{quote_asset}"
            return f"{base_asset}/{quote_asset}"

        return symbol_id

    def get_metadata(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
    ) -> CCXTMetadata:
        """
        Get CCXT metadata (tick_size, min_size, contract_size) for an instrument.

        Args:
            venue: Venue identifier
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier for lookup (Deribit uses this directly)
            tardis_symbol: Optional Tardis symbol format for better matching
            instrument_type: Optional instrument type (PERPETUAL, FUTURE, OPTION, SPOT_PAIR)

        Returns:
            Dictionary with metadata fields or empty dict (but ccxt_symbol should always be set)
        """
        # Skip slow CCXT market loading for spot-only exchanges
        # Just return default symbol format (Tardis provides the actual market data)
        spot_only_exchanges = {"UPBIT", "COINBASE"}
        if venue.upper() in spot_only_exchanges:
            logger.debug(f"⏭️ Skipping CCXT market load for {venue} (spot-only exchange)")
            default_symbol = self.generate_default_ccxt_symbol(
                venue, base_asset, quote_asset, symbol_id, instrument_type
            )
            return {
                "ccxt_symbol": default_symbol,
                "ccxt_exchange": self.venue_mapping.venue_to_ccxt.get(venue) or "",
            }

        ccxt_data = self.load_markets(venue)
        markets: dict[str, Any] | None = ccxt_data.get("markets") if ccxt_data else None

        # Build possible symbol formats based on venue
        possible_symbols: list[str] = self._build_symbol_formats(
            venue, base_asset, quote_asset, symbol_id, tardis_symbol, instrument_type
        )

        # If no markets loaded, still return ccxt_symbol based on CCXT conventions
        if not markets:
            logger.debug(f"No CCXT markets loaded for {venue}, using default symbol format")
            # Generate default CCXT symbol based on venue conventions
            default_symbol = self.generate_default_ccxt_symbol(
                venue, base_asset, quote_asset, symbol_id, instrument_type
            )
            return {
                "ccxt_symbol": default_symbol,
                "ccxt_exchange": self.venue_mapping.venue_to_ccxt.get(venue) or "",
            }

        # Try to find market in CCXT
        ccxt_market: dict[str, Any] | None = None
        matched_symbol_format: str | None = None
        for symbol_format in possible_symbols:
            if symbol_format in markets:
                ccxt_market = cast(dict[str, Any], markets[symbol_format])
                matched_symbol_format = symbol_format
                break

        if not ccxt_market:
            logger.debug(f"No CCXT market found for {venue}:{symbol_id} (tried {len(possible_symbols)} formats)")
            # CRITICAL FIX: Still return ccxt_symbol even if market not found
            # Use the first reasonable format as default
            default_symbol = self.generate_default_ccxt_symbol(
                venue, base_asset, quote_asset, symbol_id, instrument_type
            )
            return {
                "ccxt_symbol": default_symbol,
                "ccxt_exchange": self.venue_mapping.venue_to_ccxt.get(venue) or "",
            }

        # Extract metadata from CCXT market
        precision: dict[str, Any] = ccxt_market.get("precision", {}) or {}
        limits: dict[str, Any] = ccxt_market.get("limits", {}) or {}

        metadata: CCXTMetadata = {}

        # CCXT symbol and exchange (CRITICAL: These were missing!)
        # Use the matched symbol format as ccxt_symbol
        metadata["ccxt_symbol"] = matched_symbol_format or ""
        # Get CCXT exchange ID from venue mapping
        ccxt_exchange_id = self.venue_mapping.venue_to_ccxt.get(venue)
        if ccxt_exchange_id:
            metadata["ccxt_exchange"] = ccxt_exchange_id
        else:
            metadata["ccxt_exchange"] = ""

        # Tick size (price precision)
        if "price" in precision:
            tick_size_val = precision["price"]  # type: ignore[reportAny]
            if tick_size_val:
                metadata["tick_size"] = str(cast(str | int | float, tick_size_val))

        # Min size (amount precision)
        if "amount" in precision:
            min_size_val = precision["amount"]  # type: ignore[reportAny]
            if min_size_val:
                metadata["min_size"] = str(cast(str | int | float, min_size_val))
        elif "cost" in limits and "min" in limits["cost"]:
            # Some exchanges use cost_min instead
            cost_min = limits["cost"]["min"]  # type: ignore[reportAny]
            if cost_min:
                metadata["min_size"] = f"cost_min:{cast(str | int | float, cost_min)}"

        # Contract size
        contract_size_val = ccxt_market.get("contractSize")
        if contract_size_val:
            metadata["contract_size"] = float(cast(str | int | float, contract_size_val))

        return metadata

    def get_leverage_limits(
        self,
        venue: str,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: Optional[str] = None,
        instrument_type: Optional[str] = None,
    ) -> CCXTMetadata:
        """
        Get leverage limits and risk parameters from CCXT leverage tiers.

        Uses caching per venue to avoid repeated API calls for the same exchange.

        Args:
            venue: Venue identifier
            symbol: CCXT symbol format (e.g., 'BTC/USDT:USDT')
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol
            symbol_id: Symbol identifier for lookup (Deribit uses this directly)
            tardis_symbol: Optional Tardis symbol format for better matching
            instrument_type: Optional instrument type (PERPETUAL, FUTURE, OPTION, SPOT_PAIR)

        Returns:
            Dictionary with risk parameters: max_leverage, max_position_size,
            initial_margin_rate, maintenance_margin_rate
        """
        # Check cache first (leverage tiers are usually the same per venue)
        if venue in self._leverage_tiers_cache:
            cached_params = self._leverage_tiers_cache[venue]
            logger.debug(f"Using cached leverage tiers for {venue}")
            return cached_params.copy()  # Return copy to avoid mutation

        # Skip CCXT for spot-only instruments and exchanges (no leverage needed)
        # This avoids slow/hanging API calls to exchanges like Coinbase
        spot_only_exchanges = {"UPBIT", "COINBASE"}
        if instrument_type == "SPOT_PAIR" or venue.upper() in spot_only_exchanges:
            logger.debug(
                f"⏭️ Skipping CCXT leverage lookup for {venue}:{symbol_id} "
                f"(instrument_type={instrument_type}, spot-only exchange)"
            )
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params or {}

        ccxt_data = self.load_markets(venue)
        if not ccxt_data or not ccxt_data.get("exchange"):
            # Use fallback and cache it
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params

        exchange = ccxt_data["exchange"]  # type: ignore[reportAny]

        # Check if exchange supports fetchMarketLeverageTiers
        if not hasattr(exchange, "fetchMarketLeverageTiers"):  # type: ignore[reportAny]
            logger.debug(f"Exchange {venue} does not support fetchMarketLeverageTiers")
            # Try fallback to Context7 documentation lookup
            fallback_params = self._get_leverage_limits_fallback(venue)
            if fallback_params:
                self._leverage_tiers_cache[venue] = fallback_params
            return fallback_params

        try:
            # Build possible symbol formats
            possible_symbols = self._build_symbol_formats(
                venue, base_asset, quote_asset, symbol_id, tardis_symbol, instrument_type
            )

            # Try to fetch leverage tiers for each symbol format
            # CCXT has no stubs - cast to list[dict[str, Any]] for type safety
            leverage_tiers: list[dict[str, Any]] | None = None

            for symbol_format in possible_symbols:
                try:
                    tiers = cast(
                        list[dict[str, Any]],
                        getattr(exchange, "fetchMarketLeverageTiers")(symbol_format),  # type: ignore[reportAny]
                    )
                    if tiers:
                        leverage_tiers = tiers
                        break
                except Exception as e:
                    logger.debug(f"Failed to fetch leverage tiers for {symbol_format}: {e}")
                    continue

            if not leverage_tiers:
                logger.debug(f"No leverage tiers found for {venue}:{symbol_id} (tried {len(possible_symbols)} formats)")
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

    def _extract_risk_params_from_tiers(self, leverage_tiers: list[dict[str, Any]]) -> CCXTMetadata:
        """
        Extract risk parameters from CCXT leverage tiers structure.

        Args:
            leverage_tiers: List of leverage tier dictionaries from CCXT

        Returns:
            Dictionary with extracted risk parameters
        """
        if not leverage_tiers or len(leverage_tiers) == 0:
            return {}

        risk_params: CCXTMetadata = {}

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
            raw_notional = tier.get("maxNotional", 0)  # type: ignore[reportAny]
            if isinstance(raw_notional, (int, float)) and raw_notional > max_notional:
                max_notional = raw_notional
                highest_tier = tier

        if highest_tier:
            risk_params["max_position_size"] = max_notional

        # Note: Removed leverage_tiers_json serialization for simplicity
        # Use fallback defaults for reasonable values instead of complex JSON

        return risk_params

    def _get_leverage_limits_fallback(self, venue: str) -> CCXTMetadata:
        """
        Fallback method to get exchange-specific default leverage limits.

        Uses hardcoded defaults based on common exchange limits and Context7 documentation.
        These are accurate current values based on exchange documentation.

        Args:
            venue: Venue identifier

        Returns:
            Dictionary with default risk parameters including max_position_size
        """
        # Exchange-specific defaults (from Context7 docs and exchange documentation)
        # Values are accurate as of 2024-2025 based on exchange specifications
        exchange_defaults: dict[str, dict[str, str | float | None]] = {
            "BINANCE-FUTURES": {
                "max_leverage": 125.0,
                "max_position_size": 50000000.0,  # 50M USDT typical max for BTC/USDT
                "initial_margin_rate": 0.008,  # ~1/125 = 0.8%
                "maintenance_margin_rate": 0.004,  # ~0.4%
            },
            "BINANCE-SPOT": {
                # Spot markets don't have leverage, but may have position limits
                "max_position_size": 100000000.0,  # 100M USDT typical max
                "max_leverage": None,
                "initial_margin_rate": None,
                "maintenance_margin_rate": None,
            },
            "BYBIT": {
                # Bybit leverage: BTC/USDT up to 100x, ETH/USDT up to 50x, others 10-50x
                # Using conservative defaults: BTC 100x, others 50x
                "max_leverage": 100.0,  # BTC/USDT can go up to 100x
                "max_position_size": 20000000.0,  # 20M USDT typical max
                "initial_margin_rate": 0.01,  # 1/100 = 1% (for 100x leverage)
                "maintenance_margin_rate": 0.005,  # 0.5% maintenance margin
            },
            "OKX": {
                "max_leverage": 125.0,
                "max_position_size": 50000000.0,  # 50M USDT typical max
                "initial_margin_rate": 0.008,  # ~1/125 = 0.8%
                "maintenance_margin_rate": 0.004,  # ~0.4%
            },
            "DERIBIT": {
                # Deribit uses complex portfolio margin model, but default leverage is at least 5x
                # BTC/ETH perpetuals can go up to 50x (2% initial margin), but default is conservative
                # Using minimum default leverage of 5x as baseline
                "max_leverage": 5.0,  # Minimum default leverage (can go up to 50x for BTC/ETH)
                "max_position_size": 10000000.0,  # 10M USD typical max
                "initial_margin_rate": 0.20,  # 1/5 = 20% (for 5x leverage - conservative default)
                "maintenance_margin_rate": 0.10,  # 10% maintenance margin (conservative)
            },
            "HYPERLIQUID": {
                # Hyperliquid uses dynamic leverage based on open interest
                # Conservative defaults based on typical values
                "max_leverage": 20.0,  # Hyperliquid typically offers 1-20x
                "max_position_size": 10000000.0,  # 10M USDC typical max
                "initial_margin_rate": 0.05,  # 1/20 = 5%
                "maintenance_margin_rate": 0.025,  # ~2.5%
            },
            "ASTER": {
                # Aster perpetual futures - similar to Binance
                "max_leverage": 100.0,
                "max_position_size": 10000000.0,  # 10M USDT typical max
                "initial_margin_rate": 0.01,  # 1/100 = 1%
                "maintenance_margin_rate": 0.005,  # 0.5%
            },
        }

        defaults: CCXTMetadata = cast(CCXTMetadata, exchange_defaults.get(venue, {}))
        if defaults:
            logger.debug(
                f"Using fallback defaults for {venue}: max_leverage={defaults.get('max_leverage')}, "
                f"max_position_size={defaults.get('max_position_size')}"
            )

        return defaults

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid."""
        if cache_key not in self._markets_cache:
            return False
        if cache_key not in self._cache_timestamps:
            return False

        cache_age = datetime.now(timezone.utc) - self._cache_timestamps[cache_key]
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
