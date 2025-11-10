"""
Instrument Processing Service

Centralizes ALL instrument operations (canonical key generation, CCXT integration, metadata)
that are currently in the massive canonical_key_generator.py (782 lines, 0% coverage).

Benefits:
- Single point of instrument logic (easy to test → 90% coverage)
- Consistent instrument key generation across all operations
- Centralized CCXT integration and metadata handling
- Feature toggles for different instrument processing behavior patterns
- Eliminates ~782 lines of untested instrument processing code

This service replaces functionality from:
- instrument_processor/canonical_key_generator.py (CanonicalInstrumentKeyGenerator class)
- Scattered CCXT integration patterns
- Venue mapping and exchange normalization logic
- Instrument metadata processing and validation
"""

import logging
import json
import ccxt
import re
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field

# Import centralized models and configs (DRY principle)
from instruments_service.models import InstrumentDefinition
from instruments_service.config import (
    VenueMapping,
    ExchangeInstrumentConfig,
    DataTypeConfig,
)

# Import Secret Manager for API key retrieval
logger = logging.getLogger(__name__)

try:
    from unified_cloud_services import get_secret_with_fallback

    SECRET_MANAGER_AVAILABLE = True
except ImportError:
    SECRET_MANAGER_AVAILABLE = False
    logger.warning("unified-cloud-services not available for Secret Manager")


@dataclass
class InstrumentProcessingConfig:
    """Configuration for instrument processing operations"""

    api_key: str
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 1.0
    enable_ccxt_integration: bool = True
    enable_metadata_caching: bool = True
    cache_ttl_hours: int = 24
    supported_exchanges: List[str] = field(
        default_factory=lambda: [
            "binance",
            "binance-futures",
            "deribit",
            "bybit",
            "bybit-spot",
            "okx",
            "okx-futures",
            "okx-swap",
        ]
    )


class InstrumentProcessingService:
    """
    Centralized instrument processing service.

    Replaces the massive CanonicalInstrumentKeyGenerator class (782 lines)
    with streamlined, testable, configurable service.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize instrument processing service.

        Args:
            config: Configuration with instrument processing settings.
                   Can include:
                   - tardis_api_key: Direct API key (optional if Secret Manager available)
                   - api_key: Alternative key name (optional)
                   - project_id: GCP project ID for Secret Manager (default: central-element-323112)

        Raises:
            ValueError: If API key cannot be retrieved from config or Secret Manager
        """
        self.config = config
        project_id = config.get("project_id", "central-element-323112")

        # Try to get API key from config first
        self.api_key = config.get("tardis_api_key") or config.get("api_key")

        # If not in config, try Secret Manager
        if not self.api_key and SECRET_MANAGER_AVAILABLE:
            try:
                self.api_key = get_secret_with_fallback(
                    project_id=project_id,
                    secret_name="tardis-api-key",
                    fallback_env_var="TARDIS_API_KEY",
                )
                if self.api_key:
                    logger.info("✅ Retrieved Tardis API key from Secret Manager")
            except Exception as e:
                logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")

        if not self.api_key:
            raise ValueError(
                "API key required for instrument processing. "
                "Provide 'tardis_api_key' in config, or ensure Secret Manager access "
                "to 'tardis-api-key' secret."
            )

        # Use centralized configs from config.py (DRY principle)
        self.venue_mapping = VenueMapping()
        self.exchange_config = ExchangeInstrumentConfig()
        self.data_config = DataTypeConfig()

        self.processing_config = InstrumentProcessingConfig(
            api_key=self.api_key,
            retry_max_attempts=config.get("retry_max_attempts", 3),
            retry_backoff_factor=config.get("retry_backoff_factor", 1.0),
            enable_ccxt_integration=config.get("enable_ccxt_integration", True),
            enable_metadata_caching=config.get("enable_metadata_caching", True),
            cache_ttl_hours=config.get("cache_ttl_hours", 24),
            supported_exchanges=self.venue_mapping.all_tardis_exchanges,
        )

        # Initialize metadata cache
        self._metadata_cache: Dict[str, InstrumentDefinition] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

        # Initialize Tardis adapter (required)
        from instruments_service.app.venues.tardis import TardisAdapter

        self.tardis_adapter = TardisAdapter(api_key=self.api_key, project_id=project_id)

        # Initialize CCXT markets cache (per venue) - MAJOR PERFORMANCE OPTIMIZATION
        self._ccxt_markets_cache: Dict[str, Dict[str, Any]] = {}
        self._ccxt_cache_timestamps: Dict[str, datetime] = {}

        logger.info(
            f"✅ InstrumentProcessingService initialized: "
            f"api_key={'*' * (len(self.api_key) - 4) + self.api_key[-4:] if self.api_key else 'None'}, "
            f"ccxt_integration={self.processing_config.enable_ccxt_integration}, "
            f"caching={self.processing_config.enable_metadata_caching}"
        )

    def get_venue_mapping(self) -> Dict[str, str]:
        """
        Get canonical venue mapping.

        Replaces scattered venue mapping logic.
        """
        # Use centralized venue mapping from config
        return self.venue_mapping.tardis_to_venue

    def get_instrument_type_mapping(self) -> Dict[str, str]:
        """
        Get canonical instrument type mapping.

        Replaces scattered instrument type logic.
        """
        return {
            "spot": "SPOT_PAIR",
            "perpetual": "PERPETUAL",
            "future": "FUTURE",
            "option": "OPTION",
            "combo": "OPTION",  # Deribit combos are often options
        }

    def normalize_venue(self, exchange: str) -> Optional[str]:
        """
        Normalize exchange name to canonical venue.

        Args:
            exchange: Raw exchange name

        Returns:
            Canonical venue name or None if unknown
        """
        venue_mapping = self.get_venue_mapping()
        venue = venue_mapping.get(exchange.lower())

        if not venue:
            logger.warning(f"Unknown exchange: {exchange}")

        return venue

    def normalize_instrument_type(self, symbol_type: str) -> Optional[str]:
        """
        Normalize symbol type to canonical instrument type.

        Args:
            symbol_type: Raw symbol type from exchange

        Returns:
            Canonical instrument type or None if unknown
        """
        type_mapping = self.get_instrument_type_mapping()
        instrument_type = type_mapping.get(symbol_type.lower())

        if not instrument_type:
            logger.warning(f"Unknown symbol type: {symbol_type}")

        return instrument_type

    def generate_canonical_key(
        self,
        exchange: str,
        symbol_type: str,
        symbol_id: str,
        symbol_info: Dict[str, Any],
    ) -> Optional[str]:
        """
        Generate canonical instrument key following INSTRUMENT_KEY.md specification.

        Replaces the massive generate_instrument_key method from CanonicalInstrumentKeyGenerator.

        Args:
            exchange: Exchange name (e.g., 'binance', 'deribit')
            symbol_type: Symbol type ('spot', 'perpetual', 'future', 'option')
            symbol_id: Symbol identifier
            symbol_info: Additional symbol information

        Returns:
            Canonical instrument key in format: VENUE:INSTRUMENT_TYPE:SYMBOL_SPEC
        """
        # Normalize venue and instrument type
        venue = self.normalize_venue(exchange)
        instrument_type = self.normalize_instrument_type(symbol_type)

        if not venue or not instrument_type:
            return None

        # Extract base and quote assets from symbol ID (Tardis doesn't provide them as separate fields)
        base_asset = str(symbol_info.get("base_asset", "") or "").upper()
        quote_asset = str(symbol_info.get("quote_asset", "") or "").upper()

        # If not provided, parse from symbol_id
        if not base_asset or not quote_asset:
            parsed_components = self._parse_symbol_components(symbol_id, exchange)
            base_asset = str(parsed_components.get("base_asset", "") or "").upper()
            quote_asset = str(parsed_components.get("quote_asset", "") or "").upper()

        if not base_asset or not quote_asset:
            # Skip problematic instruments with debug logging instead of warning (use context7)
            # Common issues: unusual formats like 'usdtrub', regional pairs, or deprecated symbols
            logger.debug(
                f"🔄 Skipping instrument with parsing issues: {symbol_id} (exchange: {exchange}) - base:'{base_asset}' quote:'{quote_asset}'"
            )
            return None

        # Build canonical key based on instrument type
        if instrument_type == "SPOT_PAIR":
            # Clean base/quote assets and ensure no double dashes
            clean_base = base_asset.strip() if base_asset else ""
            clean_quote = quote_asset.strip() if quote_asset else ""
            if not clean_base or not clean_quote:
                logger.debug(
                    f"Skipping SPOT_PAIR with missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}"
                )
                return None
            return f"{venue}:SPOT_PAIR:{clean_base}-{clean_quote}"

        elif instrument_type == "PERPETUAL":
            # Clean base/quote assets and filter invalid characters
            clean_base = base_asset.strip() if base_asset else ""
            clean_quote = quote_asset.strip() if quote_asset else ""

            # Filter out invalid Unicode characters
            clean_base = "".join(
                c for c in clean_base if c.isprintable() and c.isascii()
            )
            clean_quote = "".join(
                c for c in clean_quote if c.isprintable() and c.isascii()
            )

            if not clean_base or not clean_quote:
                logger.debug(
                    f"Skipping PERPETUAL with invalid/missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}"
                )
                return None

            # Determine perpetual flavor: @LIN (linear) or @INV (inverse)
            # Linear: quote asset == margin currency (settle_asset)
            # Inverse: quote asset != margin currency, and margin currency == base asset
            # Determine settle_asset based on venue/exchange rules (same logic as in process_exchange_instruments)
            settle_asset = "USDT"  # Default
            if venue == "DERIBIT":
                deribit_quotes = self.exchange_config.valid_quote_currencies.get(
                    "DERIBIT", []
                )
                if clean_quote == "USD":
                    settle_asset = (
                        clean_base  # Coin margin - settle in base asset (inverse)
                    )
                elif clean_quote in deribit_quotes and clean_quote != "USD":
                    settle_asset = clean_quote  # Cash settled (USDC, etc.) - linear
            else:
                # For most exchanges, quote asset is the margin currency (linear)
                settle_asset = clean_quote

            # Determine if linear or inverse based on settle_asset
            if settle_asset == clean_quote:
                # Quote asset == margin currency → Linear
                perp_flavor = "LIN"
            elif settle_asset == clean_base:
                # Margin currency == base asset → Inverse
                perp_flavor = "INV"
            else:
                # Default to linear if unclear (most common case)
                perp_flavor = "LIN"
                logger.debug(
                    f"⚠️ Could not determine perpetual flavor for {symbol_id}, defaulting to @LIN (settle_asset={settle_asset}, quote={clean_quote}, base={clean_base})"
                )

            return f"{venue}:PERPETUAL:{clean_base}-{clean_quote}@{perp_flavor}"

        elif instrument_type == "FUTURE":
            # Extract expiry date (parse from symbol_id if not provided)
            expiry_date = symbol_info.get("expiry_date")

            if not expiry_date:
                # Parse expiry from symbol_id using comprehensive patterns
                expiry_date = self._parse_expiry_from_symbol(symbol_id, exchange)
                if expiry_date:
                    logger.debug(
                        f"✅ Parsed expiry from symbol: {symbol_id} → {expiry_date}"
                    )

            if expiry_date:
                # Format as YYMMDD
                if isinstance(expiry_date, str):
                    try:
                        expiry_dt = datetime.fromisoformat(
                            expiry_date.replace("Z", "+00:00")
                        )
                        expiry_str = expiry_dt.strftime("%y%m%d")
                    except:
                        expiry_str = expiry_date  # Use as-is if parsing fails
                else:
                    expiry_str = str(expiry_date)

                # Clean components and avoid duplication
                clean_base = base_asset.strip() if base_asset else ""
                clean_quote = quote_asset.strip() if quote_asset else ""

                if not clean_base or not clean_quote:
                    logger.debug(
                        f"Skipping FUTURE with missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}"
                    )
                    return None

                # Per INSTRUMENT_KEY.md: FUTURE format is BASE_ASSET-QUOTE_ASSET-YYMMDD
                # Clean up expiry_str (remove any duplication)
                if expiry_str and len(expiry_str) > 6:
                    # Extract just the YYMMDD part if there's extra data

                    match = re.search(r"(\d{6})", expiry_str)
                    if match:
                        expiry_str = match.group(1)

                return f"{venue}:FUTURE:{clean_base}-{clean_quote}-{expiry_str}"
            else:
                logger.warning(
                    f"Missing expiry date for future {symbol_id} exchange: {exchange}"
                )
                return None

        elif instrument_type == "OPTION":
            # Filter complex Deribit option strategies using config

            if exchange == "deribit" and any(
                strategy in symbol_id
                for strategy in self.data_config.excluded_deribit_strategies
            ):
                logger.debug(f"Filtered complex Deribit strategy option: {symbol_id}")
                return None  # Skip complex strategies per config

            # Extract option parameters (parse from symbol_id if not provided)
            expiry_date = symbol_info.get("expiry_date")
            strike_price = symbol_info.get("strike_price")
            option_type = symbol_info.get("option_type", "").upper()

            # Parse from symbol_id if not provided (Tardis format)
            if not all([expiry_date, strike_price, option_type]):
                parsed_option = self._parse_option_components(symbol_id, exchange)
                expiry_date = expiry_date or parsed_option.get("expiry_date")
                strike_price = strike_price or parsed_option.get("strike_price")
                option_type = (
                    option_type or parsed_option.get("option_type", "").upper()
                )

            if not all([expiry_date, strike_price, option_type]):
                logger.warning(f"Missing option parameters for {symbol_id}")
                return None

            # Format expiry as YYMMDD
            if isinstance(expiry_date, str):
                try:
                    expiry_dt = datetime.fromisoformat(
                        expiry_date.replace("Z", "+00:00")
                    )
                    expiry_str = expiry_dt.strftime("%y%m%d")
                except:
                    expiry_str = expiry_date
            else:
                expiry_str = str(expiry_date)

            return f"{venue}:OPTION:{base_asset}-{quote_asset}-{expiry_str}-{strike_price}-{option_type}"

        else:
            logger.warning(f"Unhandled instrument type: {instrument_type}")
            return None

    async def fetch_exchange_instruments(
        self, exchange: str, target_date: datetime = None, force: bool = False
    ) -> Tuple[Dict[str, Dict[str, Any]], int]:
        """
        Fetch instrument data from Tardis API for specific exchange.

        Replaces direct API calls scattered across handlers.

        Args:
            exchange: Exchange name
            target_date: Target date for instrument availability
            force: If True, bypass date availability filtering and get ALL current instruments

        Returns:
            Tuple of (instruments_data dict, date_filtered_count)
        """
        target_date = target_date or datetime.now(timezone.utc)
        date_str = target_date.strftime("%Y-%m-%d")

        # Fetch instruments using TardisAdapter
        available_symbols_list, date_filtered_count = (
            self.tardis_adapter.fetch_exchange_instruments(
                exchange=exchange, target_date=target_date, force_refresh=force
            )
        )
        # Convert list to dict keyed by symbol_id
        available_symbols = {
            symbol.get("id", ""): symbol
            for symbol in available_symbols_list
            if symbol.get("id")
        }

        # Filter instruments available at target date AND exclude unwanted types
        instruments_data = {}
        for symbol_id, symbol in available_symbols.items():
            if not symbol_id:  # Skip if symbol_id is empty
                continue
            symbol_type = symbol.get("type", "")

            # Filter excluded instrument types (e.g., combos)
            if symbol_type in self.data_config.excluded_instrument_types:
                logger.debug(
                    f"Filtered excluded instrument type: {symbol_id} ({symbol_type})"
                )
                continue

            # Filter problematic Binance instruments with weird naming patterns
            if exchange in [
                "binance",
                "binance-futures",
            ] and self._is_problematic_binance_instrument(symbol_id):
                logger.debug(f"Filtered problematic Binance instrument: {symbol_id}")
                continue

            # Date filtering already done above, so all symbols here are available
            instruments_data[symbol_id] = symbol

        if target_date:
            logger.info(
                f"✅ Filtered to {len(instruments_data)} instruments available on {date_str}"
            )
        else:
            logger.info(
                f"✅ Retrieved {len(instruments_data)} currently active instruments"
            )
        return instruments_data, date_filtered_count

    async def process_exchange_instruments(
        self, exchange: str, target_date: datetime = None, force: bool = False
    ) -> Dict[str, InstrumentDefinition]:
        """
        Process all instruments for an exchange and generate canonical keys.

        Replaces complex processing logic from CanonicalInstrumentKeyGenerator.

        Args:
            exchange: Exchange name
            target_date: Target date for processing (if provided, filters by availableSince/availableTo)
            force: If True, force regeneration even if files exist (date filtering still applies)

        Returns:
            Dictionary of processed instrument metadata keyed by canonical key
        """
        instruments_data, date_filtered_count = await self.fetch_exchange_instruments(
            exchange, target_date, force
        )

        # CRITICAL OPTIMIZATION: Apply exchange config filtering BEFORE expensive processing
        # This filters out invalid instrument types, quote currencies (even in force mode)
        # Expected to reduce from ~250k to ~10k instruments across all exchanges
        canonical_venue = self.normalize_venue(exchange)
        valid_types = self.exchange_config.exchange_instrument_types.get(
            canonical_venue, []
        )
        valid_quotes = self.exchange_config.valid_quote_currencies.get(
            canonical_venue, ["USDT"]
        )

        logger.info(
            f"🔍 Pre-filtering by exchange config: {canonical_venue} accepts types={valid_types}, quotes={valid_quotes}"
        )

        # Get excluded base currencies and symbol patterns for this exchange
        excluded_bases = self.exchange_config.excluded_base_currencies.get(
            canonical_venue, []
        )
        excluded_patterns = self.exchange_config.excluded_symbol_patterns.get(
            canonical_venue, []
        )

        # Pre-filter by exchange config before expensive processing
        pre_filtered = {}
        for symbol_id, symbol_info in instruments_data.items():
            symbol_type = symbol_info.get("type", "").lower()
            normalized_type = self.normalize_instrument_type(symbol_type)

            # Filter by valid instrument types for this exchange
            if normalized_type not in valid_types:
                continue

            # Check excluded symbol patterns early (e.g., leveraged products like 3L, 2L)
            if excluded_patterns:
                symbol_upper = symbol_id.upper()
                excluded_by_pattern = False
                for pattern in excluded_patterns:
                    if pattern.upper() in symbol_upper:
                        logger.debug(
                            f"🚫 Pre-filtered out {symbol_id}: symbol pattern '{pattern}' excluded for {canonical_venue}"
                        )
                        excluded_by_pattern = True
                        break
                if excluded_by_pattern:
                    continue

            # Quick parse base and quote currency to check validity (before full processing)
            parsed_components = self._parse_symbol_components(symbol_id, exchange)
            if isinstance(parsed_components, dict):
                base_asset = parsed_components.get("base_asset", "").upper()
                quote_asset = parsed_components.get("quote_asset", "").upper()
            else:
                base_asset, quote_asset = (
                    parsed_components if parsed_components else ("", "")
                )
                base_asset = base_asset.upper() if base_asset else ""
                quote_asset = quote_asset.upper() if quote_asset else ""

            # Filter by excluded base currencies
            if base_asset and base_asset in excluded_bases:
                logger.debug(
                    f"🚫 Pre-filtered out {symbol_id}: base currency '{base_asset}' excluded for {canonical_venue}"
                )
                continue

            # Filter by valid quote currencies for this exchange
            if quote_asset and quote_asset not in valid_quotes:
                continue

            pre_filtered[symbol_id] = symbol_info

        logger.info(
            f"🔍 Exchange config filter: {len(pre_filtered)}/{len(instruments_data)} instruments valid for {canonical_venue}"
        )
        instruments_data = pre_filtered

        # Track filtering statistics for debugging
        # Include date_filtered from fetch_exchange_instruments
        filter_stats = {
            "no_canonical_key": 0,
            "same_base_quote": 0,
            "date_filtered": date_filtered_count,  # From fetch_exchange_instruments
            "expiry_filtered": 0,
            "processing_error": 0,
            "success": 0,
        }

        processed_instruments = {}
        for symbol_id, symbol_info in instruments_data.items():
            try:
                # Generate canonical key
                canonical_key = self.generate_canonical_key(
                    exchange=exchange,
                    symbol_type=symbol_info.get("type", ""),
                    symbol_id=symbol_id,
                    symbol_info=symbol_info,
                )

                if not canonical_key:
                    filter_stats["no_canonical_key"] += 1
                    if exchange == "deribit":
                        logger.debug(
                            f"🚫 Deribit: No canonical key for {symbol_id} (type: {symbol_info.get('type', '')})"
                        )
                    continue

                if canonical_key:
                    # Parse base/quote from symbol_id (Tardis doesn't provide these fields)
                    parsed_components = self._parse_symbol_components(
                        symbol_id, exchange
                    )
                    if isinstance(parsed_components, dict):
                        base_asset = parsed_components.get("base_asset", "")
                        quote_asset = parsed_components.get("quote_asset", "")
                    else:
                        base_asset, quote_asset = (
                            parsed_components if parsed_components else ("", "")
                        )

                    # Clean and validate parsed values (fix NoneType errors)
                    clean_base = (
                        str(base_asset or "").upper() if base_asset is not None else ""
                    )
                    clean_quote = (
                        str(quote_asset or "").upper()
                        if quote_asset is not None
                        else ""
                    )

                    # Filter nonsensical pairs: base_asset == quote_asset (USDT-USDT, BTC-BTC, etc.)
                    if clean_base == clean_quote and clean_base:
                        filter_stats["same_base_quote"] += 1
                        logger.debug(
                            f"🚫 Filtered nonsensical pair: {symbol_id} ({clean_base}-{clean_quote} - same asset)"
                        )
                        continue

                    # Infer settle_asset for Deribit using centralized config (DRY principle)
                    settle_asset = "USDT"
                    canonical_venue = self.normalize_venue(exchange)
                    if canonical_venue == "DERIBIT":
                        deribit_quotes = (
                            self.exchange_config.valid_quote_currencies.get(
                                "DERIBIT", []
                            )
                        )
                        if clean_quote == "USD":
                            settle_asset = (
                                clean_base  # Coin margin - settle in base asset
                            )
                        elif clean_quote in deribit_quotes and clean_quote != "USD":
                            settle_asset = clean_quote  # Cash settled (USDC, etc.)

                    # Extract symbol from canonical key (VENUE:INSTRUMENT_TYPE:SYMBOL)
                    symbol = (
                        canonical_key.split(":", 2)[2]
                        if len(canonical_key.split(":")) >= 3
                        else symbol_id
                    )

                    # Populate ALL derived fields BEFORE model creation (FIXED for Pydantic validation use context7)
                    enhanced_fields = await self._populate_all_derived_fields(
                        canonical_key,
                        canonical_venue,
                        self.normalize_instrument_type(symbol_info.get("type", "")),
                        clean_base,
                        clean_quote,
                        symbol_id,
                        exchange,  # Pass original tardis exchange name
                    )

                    # Create metadata object with ALL fields including derived ones - FIXED for InstrumentDefinition schema
                    normalized_instrument_type = self.normalize_instrument_type(
                        symbol_info.get("type", "")
                    )

                    # CRITICAL: Set data_types based on venue first, then instrument_type using config
                    # Deribit: All instruments use only 'options_chain' per documentation
                    if canonical_venue == "DERIBIT":
                        config_data_types = ["options_chain"]
                    else:
                        # Other venues: Use instrument_type-based config
                        config_data_types = self.data_config.instrument_data_types.get(
                            normalized_instrument_type or "SPOT_PAIR",
                            ["trades", "book_snapshot_5"],
                        )
                    data_types_str = ",".join(config_data_types)

                    # CRITICAL: Set tardis_exchange based on venue+instrument_type mapping
                    # This ensures we use the correct Tardis endpoint (e.g., okex-swap for OKX PERPETUAL)
                    mapping_key = (
                        canonical_venue,
                        normalized_instrument_type or "SPOT_PAIR",
                    )
                    tardis_exchange = (
                        self.venue_mapping.venue_instrument_type_to_tardis.get(
                            mapping_key,
                            exchange.lower(),  # Fallback to original exchange name
                        )
                    )

                    # CRITICAL: Get available_to from Tardis API (availableTo field)
                    # If Tardis doesn't provide availableTo, default to blank (None)
                    available_to = symbol_info.get("availableTo")
                    available_to_datetime = available_to if available_to else None

                    # NOTE: Date filtering (availableSince/availableTo) is already done in fetch_exchange_instruments
                    # We only need to check expiry for futures/options here
                    # Check expiry for futures/options: filter if expired
                    # This is separate from available_to - expiry filtering still applies even if available_to is blank
                    if (
                        target_date
                        and normalized_instrument_type in ["FUTURE", "OPTION"]
                        and "expiry" in enhanced_fields
                    ):
                        expiry_str = enhanced_fields.get("expiry")
                        if expiry_str:
                            try:
                                expiry_dt = datetime.fromisoformat(
                                    expiry_str.replace("Z", "+00:00")
                                )
                                # Convert both to date objects for comparison
                                target_date_only = (
                                    target_date.date()
                                    if hasattr(target_date, "date")
                                    else target_date
                                )
                                expiry_date_only = expiry_dt.date()

                                if target_date_only > expiry_date_only:
                                    filter_stats["expiry_filtered"] += 1
                                    logger.debug(
                                        f"🚫 Filtered: {symbol_id} - expiry {expiry_date_only} < target_date {target_date_only}"
                                    )
                                    continue
                            except Exception as e:
                                logger.debug(
                                    f"⚠️ Could not parse expiry '{expiry_str}': {e}"
                                )

                    metadata = InstrumentDefinition(
                        instrument_key=canonical_key,
                        venue=canonical_venue,
                        instrument_type=normalized_instrument_type,
                        symbol=symbol,  # ✅ FIXED: symbol_id → symbol (required field)
                        base_asset=clean_base,
                        quote_asset=clean_quote,
                        settle_asset=settle_asset,  # ✅ ADDED: Direct field instead of attributes
                        exchange_raw_symbol=symbol_id,
                        tardis_symbol=self._convert_to_tardis_symbol(
                            symbol_id, exchange
                        ),  # ✅ FIXED: Convert to proper Tardis format
                        tardis_exchange=tardis_exchange,  # ✅ FIXED: Set from venue+instrument_type mapping
                        available_from_datetime=symbol_info.get("availableSince", ""),
                        available_to_datetime=available_to_datetime,  # ✅ FIXED: Now properly populated from Tardis or expiry
                        data_types=data_types_str,  # ✅ FIXED: Set from config based on instrument_type
                        # Include derived fields directly in model creation (FIXED for validation timing use context7)
                        **enhanced_fields,  # Unpack all enhanced fields including strike, option_type, etc.
                    )

                    processed_instruments[canonical_key] = metadata
                    filter_stats["success"] += 1

                    # Cache metadata for future use
                    self.cache_metadata(canonical_key, metadata)

            except Exception as e:
                filter_stats["processing_error"] += 1
                logger.warning(f"⚠️ Failed to process instrument {symbol_id}: {e}")

        # Log detailed filtering statistics
        total_filtered = sum(v for k, v in filter_stats.items() if k != "success")
        logger.info(
            f"📊 Processed {len(processed_instruments)} instruments from {exchange}"
        )
        if total_filtered > 0:
            logger.info(
                f"🔍 Filtering breakdown for {exchange}: "
                f"no_key={filter_stats['no_canonical_key']}, "
                f"same_base_quote={filter_stats['same_base_quote']}, "
                f"date_filtered={filter_stats['date_filtered']}, "
                f"expiry_filtered={filter_stats['expiry_filtered']}, "
                f"errors={filter_stats['processing_error']}, "
                f"success={filter_stats['success']}"
            )
        return processed_instruments

    async def generate_instruments_for_exchanges(
        self,
        exchanges: List[str],
        target_date: datetime = None,
        max_parallel: int = None,
        force: bool = False,
    ) -> Dict[str, InstrumentDefinition]:
        """
        Generate instruments for multiple exchanges in parallel.

        Replaces generate_all_instruments from CanonicalInstrumentKeyGenerator.

        Args:
            exchanges: List of exchange names
            target_date: Target date for instrument generation (if provided, filters by availableSince/availableTo)
            max_parallel: Maximum parallel exchange processing
            force: If True, force regeneration even if files exist (date filtering still applies)

        Returns:
            Combined dictionary of all processed instruments
        """
        target_date = target_date or datetime.now(timezone.utc)

        # Filter supported exchanges
        supported_exchanges = [
            ex
            for ex in exchanges
            if ex.lower() in self.processing_config.supported_exchanges
        ]

        if not supported_exchanges:
            logger.warning(f"No supported exchanges in: {exchanges}")
            return {}

        logger.info(
            f"🚀 Processing {len(supported_exchanges)} exchanges: {supported_exchanges}"
        )

        # Process exchanges individually for now (can be parallelized later with ConcurrencyService)
        all_instruments = {}

        for exchange in supported_exchanges:
            try:
                exchange_instruments = await self.process_exchange_instruments(
                    exchange, target_date, force
                )
                all_instruments.update(exchange_instruments)

                logger.info(
                    f"✅ {exchange}: {len(exchange_instruments)} instruments processed"
                )

            except Exception as e:
                logger.error(f"❌ Failed to process {exchange}: {e}")

        logger.info(
            f"📊 Total instruments processed: {len(all_instruments)} across {len(supported_exchanges)} exchanges"
        )
        return all_instruments

    def filter_instruments_by_exchange_config(self, instruments, exchange):
        """Filter instruments by exchange-specific capabilities using centralized config (DRY)."""
        # Use centralized configs (already loaded)
        canonical_venue = self.normalize_venue(exchange)
        valid_types = self.exchange_config.exchange_instrument_types.get(
            canonical_venue, ["SPOT_PAIR"]
        )
        valid_quotes = self.exchange_config.valid_quote_currencies.get(
            canonical_venue, ["USDT"]
        )
        is_derivative = canonical_venue in self.exchange_config.derivative_exchanges
        
        # Get excluded base currencies and symbol patterns for this exchange
        excluded_bases = self.exchange_config.excluded_base_currencies.get(
            canonical_venue, []
        )
        excluded_patterns = self.exchange_config.excluded_symbol_patterns.get(
            canonical_venue, []
        )

        filtered = {}

        for inst_key, inst_data in instruments.items():
            try:
                # Check instrument type is valid for this exchange
                inst_type = inst_data.get("instrument_type", "")
                if inst_type not in valid_types:
                    logger.debug(
                        f"❌ Filtered out {inst_key}: {inst_type} not valid for {canonical_venue}"
                    )
                    continue

                # Check quote currency is valid for this exchange (FIXED - was not working)
                quote_asset = inst_data.get("quote_asset", "").upper()
                if quote_asset not in valid_quotes:
                    logger.debug(
                        f"🚫 Filtered out {inst_key}: quote '{quote_asset}' not in valid quotes {valid_quotes} for {canonical_venue}"
                    )
                    continue

                # Check excluded base currencies
                base_asset = inst_data.get("base_asset", "").upper()
                if base_asset in excluded_bases:
                    logger.debug(
                        f"🚫 Filtered out {inst_key}: base currency '{base_asset}' excluded for {canonical_venue}"
                    )
                    continue

                # Check excluded symbol patterns (e.g., leveraged products like 3L, 2L)
                symbol = inst_data.get("symbol", "").upper()
                if excluded_patterns:
                    excluded_by_pattern = False
                    for pattern in excluded_patterns:
                        if pattern.upper() in symbol:
                            logger.debug(
                                f"🚫 Filtered out {inst_key}: symbol pattern '{pattern}' excluded for {canonical_venue}"
                            )
                            excluded_by_pattern = True
                            break
                    if excluded_by_pattern:
                        continue

                # Populate complete InstrumentDefinition fields
                inst_data = self._populate_complete_instrument_data(
                    inst_data, exchange, is_derivative
                )
                filtered[inst_key] = inst_data

            except Exception as e:
                logger.warning(f"⚠️ Error filtering {inst_key}: {e}")

        return filtered

    def _populate_complete_instrument_data(self, inst_data, exchange, is_derivative):
        """Populate all InstrumentDefinition fields per models.py schema."""
        # Core fields
        inst_data.setdefault("tardis_exchange", exchange.lower())
        inst_data.setdefault("data_provider", "tardis")
        inst_data.setdefault("asset_class", "crypto")

        # Venue type classification
        if is_derivative and inst_data.get("instrument_type") in [
            "PERPETUAL",
            "FUTURE",
            "OPTION",
        ]:
            inst_data["venue_type"] = "derivatives"
            # Add underlying for derivatives (base_asset-quote_asset)
            base_asset = inst_data.get("base_asset", "")
            quote_asset = inst_data.get("quote_asset", "")
            if base_asset and quote_asset:
                inst_data["underlying"] = f"{base_asset}-{quote_asset}"
        else:
            inst_data["venue_type"] = "spot"
            inst_data["underlying"] = ""  # Not applicable for spot

        # Data types based on venue first, then instrument type
        # Deribit: All instruments use only 'options_chain' per documentation
        venue = inst_data.get("venue", "")
        if venue == "DERIBIT":
            inst_data["data_types"] = "options_chain"
        else:
            inst_type = inst_data.get("instrument_type", "SPOT_PAIR")
            inst_data["data_types"] = ",".join(
                self.data_config.instrument_data_types.get(
                    inst_type, ["trades", "book_snapshot_5"]
                )
            )

        return inst_data

    def _get_ccxt_data_for_venue(self, venue):
        """Get CCXT market data for venue (CACHED for performance - load once per exchange)."""
        try:

            ccxt_exchange_id = self.venue_mapping.venue_to_ccxt.get(venue)
            if not ccxt_exchange_id:
                logger.debug(f"No CCXT mapping for venue: {venue}")
                return None

            # Check if we have cached markets for this venue
            cache_key = f"{venue}_{ccxt_exchange_id}"
            if self._is_ccxt_cache_valid(cache_key):
                logger.debug(
                    f"📋 Using cached CCXT markets for {venue} ({len(self._ccxt_markets_cache[cache_key]['markets'])} markets)"
                )
                return self._ccxt_markets_cache[cache_key]

            # Initialize CCXT exchange
            exchange_class = getattr(ccxt, ccxt_exchange_id, None)
            if not exchange_class:
                logger.debug(f"CCXT exchange not available: {ccxt_exchange_id}")
                return None

            ccxt_exchange = exchange_class(
                {
                    "enableRateLimit": True,
                    "timeout": 15000,  # 15s timeout for initial load
                }
            )

            # Load markets ONCE per exchange (major performance optimization)
            markets = ccxt_exchange.load_markets()
            logger.info(
                f"⚡ Loaded {len(markets)} CCXT markets for {venue} ({ccxt_exchange_id}) - CACHED for reuse"
            )

            # Cache the results for reuse
            ccxt_data = {
                "exchange": ccxt_exchange,
                "markets": markets,
                "exchange_id": ccxt_exchange_id,
            }

            self._ccxt_markets_cache[cache_key] = ccxt_data
            self._ccxt_cache_timestamps[cache_key] = datetime.now()

            return ccxt_data

        except Exception as e:
            logger.debug(f"CCXT data unavailable for {venue}: {e}")
            return None

    def _is_ccxt_cache_valid(self, cache_key):
        """Check if CCXT cache is still valid (4 hour TTL)."""
        if cache_key not in self._ccxt_markets_cache:
            return False
        if cache_key not in self._ccxt_cache_timestamps:
            return False

        cache_age = datetime.now() - self._ccxt_cache_timestamps[cache_key]
        return cache_age < timedelta(hours=4)

    def get_ccxt_metadata_from_cache(self, venue, base_asset, quote_asset, symbol_id):
        """Get CCXT metadata using CACHED markets data (ENHANCED for Bybit perpetuals use context7)."""
        try:
            # Get cached CCXT data for venue
            ccxt_data = self._get_ccxt_data_for_venue(venue)
            if not ccxt_data or not ccxt_data.get("markets"):
                return {}

            markets = ccxt_data["markets"]

            # Enhanced symbol format detection using tardis_symbol use context7
            # Get tardis_symbol from InstrumentDefinition if available for better CCXT lookup
            tardis_symbol = getattr(self, "_current_tardis_symbol", symbol_id)

            # ENHANCED: Bybit perpetual-specific CCXT format mapping use context7
            possible_symbols = []

            if venue == "BYBIT" and base_asset and quote_asset:
                # From context7: Bybit perpetuals use BASE/QUOTE:QUOTE format
                possible_symbols.append(
                    f"{base_asset}/{quote_asset}:{quote_asset}"
                )  # BTC/USDT:USDT

                # Handle compound symbols that likely don't exist as perpetuals in CCXT
                if len(base_asset) > 5:  # Compound symbols like ETHBTC, SHIB1000
                    logger.debug(
                        f"🔍 Bybit compound symbol (likely unavailable in CCXT): {base_asset}"
                    )

                    # Special mappings for known variations use context7
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

            elif venue == "DERIBIT":
                # Deribit CCXT formats from context7: BTC/USD:BTC for perpetuals
                if "PERPETUAL" in tardis_symbol:
                    base_clean = tardis_symbol.replace("-PERPETUAL", "")
                    if quote_asset == "USD":
                        possible_symbols.append(
                            f"{base_asset}/USD:{base_asset}"
                        )  # BTC/USD:BTC (inverse)
                    elif quote_asset in ["USDC", "USDT"]:
                        possible_symbols.append(
                            f"{base_asset}/{quote_asset}:{quote_asset}"
                        )  # BTC/USDC:USDC
                elif (
                    "OPTION" in tardis_symbol
                    or "-C" in tardis_symbol
                    or "-P" in tardis_symbol
                ):
                    # Deribit options: BTC/USD:BTC-25DEC25-50000-C
                    possible_symbols.append(
                        f"{base_asset}/{quote_asset}:{tardis_symbol}"
                    )
                elif "FUTURE" in tardis_symbol or any(
                    month in tardis_symbol for month in ["JAN", "FEB", "MAR", "DEC"]
                ):
                    # Deribit futures: BTC/USD:BTC-25DEC25
                    possible_symbols.append(
                        f"{base_asset}/{quote_asset}:{tardis_symbol}"
                    )

            # Standard formats for all venues
            possible_symbols.extend(
                [
                    f"{base_asset}/{quote_asset}",  # Standard: BTC/USDT
                    f"{base_asset}{quote_asset}",  # Binance: BTCUSDT
                    f"{base_asset}-{quote_asset}",  # Alternative dash format
                    tardis_symbol,  # Original tardis symbol
                    symbol_id.upper(),  # Original symbol
                    symbol_id.lower(),  # Original lowercase
                ]
            )

            ccxt_market = None
            matched_symbol = None

            for symbol_format in possible_symbols:
                if symbol_format in markets:
                    ccxt_market = markets[symbol_format]
                    matched_symbol = symbol_format
                    break

            if not ccxt_market:
                logger.debug(
                    f"No CCXT market found for {venue}:{symbol_id} (tried {len(possible_symbols)} formats)"
                )
                return {}

            # Extract metadata from cached CCXT market using correct CCXT field mappings
            ccxt_metadata = {
                "ccxt_symbol": matched_symbol,
                "ccxt_exchange": ccxt_data.get("exchange_id", ""),
                # tick_size = price precision (minimum price increment)
                "tick_size": str(
                    ccxt_market.get("precision", {}).get("price", "") or ""
                ),
                # min_size = minimum amount limit (minimum order size)
                "min_size": str(
                    ccxt_market.get("limits", {}).get("amount", {}).get("min", "") or ""
                ),
                # contract_size for derivatives
                "contract_size": str(ccxt_market.get("contractSize", "") or ""),
            }

            # Clean None values
            ccxt_metadata = {
                k: v for k, v in ccxt_metadata.items() if v is not None and v != ""
            }

            logger.debug(
                f"📊 Enhanced CCXT metadata for {symbol_id}: {len(ccxt_metadata)} fields from cache"
            )
            return ccxt_metadata

        except Exception as e:
            logger.debug(f"CCXT cache lookup failed for {venue}:{symbol_id}: {e}")
            return {}

    def _parse_symbol_components(self, symbol_id, exchange):
        """Parse base/quote assets from Tardis symbol ID - ENHANCED from canonical_key_generator.py."""
        try:

            # PROVEN PATTERNS from original canonical_key_generator.py
            def remove_suffix(s):
                """Remove quote currency suffixes - COMPREHENSIVE for Binance use context7"""
                # BINANCE VALIDATION: Pre-filter invalid patterns before parsing (use context7)
                if exchange in ["binance", "binance-futures"]:
                    # Special case: 1000x multiplier instruments are LEGITIMATE futures
                    if s.startswith("1000") and len(s) > 4:
                        logger.debug(f"✅ Valid Binance 1000x multiplier: {s}")
                        # Continue to normal parsing - these are legitimate
                    elif s and s[0].isdigit():
                        logger.debug(
                            f"🚫 Binance invalid: {s} starts with number (non-1000x)"
                        )
                        return "", ""  # Invalid pattern

                    # Filter USDT as base (should be quote only)
                    if s.upper().startswith("USDT") and len(s) > 4:
                        quote_part = s[4:].upper()
                        # Common fiat/regional suffixes that would make USDT+{suffix}
                        usdt_suffixes = [
                            "TRY",
                            "ARS",
                            "BRL",
                            "PLN",
                            "UAH",
                            "CZK",
                            "RON",
                            "NGN",
                            "ZAR",
                        ]
                        if any(
                            s.upper().endswith(f"USDT{suffix}")
                            for suffix in usdt_suffixes
                        ):
                            logger.debug(f"🚫 Binance invalid: {s} has USDT as base")
                            return "", ""  # Invalid USDT base pattern

                # Comprehensive list from Binance documentation and real data
                suffixes = [
                    "USDT",
                    "USDC",
                    "BUSD",
                    "USD",
                    "DAI",
                    "GBP",
                    "TUSD",
                    "EUR",
                    "TRY",
                    "BRL",
                    "JPY",
                    "KRW",
                    "CNY",
                    "HKD",
                    # Regional and unusual fiat quotes (including Mexican Peso)
                    "BRZ",
                    "USDE",
                    "AUD",
                    "RUB",
                    "UAH",
                    "PLN",
                    "RON",
                    "NGN",
                    "ZAR",
                    "IDRT",
                    "VAI",
                    "BIDR",
                    "GEL",
                    "CZK",
                    "MXN",
                    "ARS",
                    "COP",
                    "CLP",
                    "PEN",
                    "VES",
                    "DAI",
                    "BRL"  # Latin American currencies and DAI
                    # Crypto quotes that can be quote assets (from context7)
                    "BTC",
                    "ETH",
                    "BNB",
                    "XRP",
                    "TRX",
                    "ADA",
                    "SOL",
                    "LTC",
                    "DOT",
                    "LINK",
                    "UNI",
                    "AVAX",
                    # Memecoin and additional crypto quotes
                    "DOGE",
                    "SHIB",
                    "PEPE",
                    "FLOKI",
                    "WIF",
                    "BONK",
                    "MEME",
                    "BABYDOGE",
                ]
                # Sort by length (longest first) to avoid false matches
                suffixes.sort(key=len, reverse=True)

                for suffix in suffixes:
                    if s.upper().endswith(suffix):
                        base = s[: -len(suffix)]
                        # Additional Binance validation for the extracted base
                        if exchange in ["binance", "binance-futures"] and base:
                            if (
                                base[0].isdigit() and base[0:3] != "1000"
                            ) or base.upper() == "USDT":
                                return "", ""  # Invalid pattern
                        return base, suffix

                return s, None  # Don't default to USD - let parsing continue

            # DERIBIT ENHANCED PARSING (from original patterns use context7)
            if exchange == "deribit":
                # Perpetuals: BTC-PERPETUAL, BTC_USDC-PERPETUAL, SOL_USDC-PERPETUAL
                if symbol_id.endswith("-PERPETUAL"):
                    first_part = symbol_id.replace("-PERPETUAL", "")

                    # Handle underscore-separated patterns: BTC_USDC, SOL_USDC
                    if "_" in first_part:
                        parts = first_part.split("_")
                        return {"base_asset": parts[0], "quote_asset": parts[1]}
                    else:
                        # Simple perpetuals: BTC-PERPETUAL → BTC + USD (Deribit default)
                        return {"base_asset": first_part, "quote_asset": "USD"}

                # Spot: BTC_USDC, ETH_BTC, USDC_USDT
                elif "_" in symbol_id and not any(
                    x in symbol_id for x in ["-", "PERPETUAL"]
                ):
                    parts = symbol_id.split("_")
                    if len(parts) == 2:
                        return {"base_asset": parts[0], "quote_asset": parts[1]}

                # Futures & Options: BTC-25DEC25, BTC-25DEC25-50000-C
                elif "-" in symbol_id:
                    parts = symbol_id.split("-")
                    first_part = parts[0]  # BTC, ETH, SOL, BTCDVOL_USDC, etc.

                    # Use remove_suffix to properly extract base and quote currencies
                    base_currency, detected_quote = remove_suffix(first_part)
                    base_currency = base_currency.rstrip("_")

                    # Check if quote is valid for Deribit using centralized config
                    deribit_valid_quotes = (
                        self.exchange_config.valid_quote_currencies.get(
                            "DERIBIT", ["USD"]
                        )
                    )
                    if (
                        detected_quote in deribit_valid_quotes
                        and detected_quote in first_part.upper()
                    ):
                        return {
                            "base_asset": base_currency,
                            "quote_asset": detected_quote,
                        }
                    else:
                        return {
                            "base_asset": first_part,
                            "quote_asset": "USD",
                        }  # Deribit default

            # BYBIT/BINANCE ENHANCED PARSING (from original patterns use context7)
            elif exchange in ["bybit", "bybit-spot", "binance", "binance-futures"]:
                # BINANCE-FUTURES special handling: btcusdt_241227 format
                if exchange == "binance-futures" and "_" in symbol_id:
                    base_part = symbol_id.split("_")[0].upper()  # btcusdt → BTCUSDT

                    # Enhanced compound parsing for futures format
                    futures_quotes = ["USDT", "USDC", "BTC", "ETH", "BNB"]
                    for quote in futures_quotes:
                        if base_part.endswith(quote):
                            base = base_part[: -len(quote)]
                            if base and len(base) >= 2:
                                logger.debug(
                                    f"✅ Binance futures: {symbol_id} → '{base}' + '{quote}'"
                                )
                                return {"base_asset": base, "quote_asset": quote}

                # Enhanced compound symbol parsing with comprehensive quote detection
                clean_symbol = symbol_id.replace("PERP", "").upper()

                # PRIORITY 1: Comprehensive quote detection from Binance docs use context7
                crypto_quotes = [
                    "USDT",
                    "USDC",
                    "BUSD",
                    "BTC",
                    "ETH",
                    "BNB",
                    "USD",
                    "EUR",
                    "BRZ",
                    "USDE",
                    # Crypto quotes for compound detection
                    "XRP",
                    "TRX",
                    "ADA",
                    "SOL",
                    "LTC",
                    "DOT",
                    "LINK",
                    "UNI",
                    "AVAX",
                    "ATOM",
                    # Memecoin quotes
                    "DOGE",
                    "SHIB",
                    "PEPE",
                    "FLOKI",
                    "WIF",
                    "BONK",
                    "MEME",
                    "BABYDOGE",
                    # Fiat and regional currencies (including Mexican Peso)
                    "GBP",
                    "AUD",
                    "CAD",
                    "JPY",
                    "KRW",
                    "TRY",
                    "RUB",
                    "PLN",
                    "NGN",
                    "ZAR",
                    "MXN",
                    "ARS",
                    "COP",
                    "CLP",
                    "PEN",
                    "VES",
                    "CZK",
                    "RON",
                    "UAH",
                ]
                crypto_quotes.sort(
                    key=len, reverse=True
                )  # Longest first to avoid false matches

                for quote in crypto_quotes:
                    if clean_symbol.endswith(quote):
                        base = clean_symbol[: -len(quote)]
                        if (
                            base and len(base) >= 2
                        ):  # Meaningful base asset (avoid single chars)
                            # BINANCE-specific filtering: exclude invalid patterns
                            if exchange in ["binance", "binance-futures"]:
                                # Filter symbols starting with numbers (invalid)
                                if base[0].isdigit():
                                    logger.debug(
                                        f"🚫 Binance invalid: {symbol_id} starts with number"
                                    )
                                    return {"base_asset": "", "quote_asset": ""}

                                # Filter USDT as base asset (USDT should be quote, not base)
                                if base == "USDT":
                                    logger.debug(
                                        f"🚫 Binance invalid: {symbol_id} has USDT as base asset"
                                    )
                                    return {"base_asset": "", "quote_asset": ""}

                            logger.debug(
                                f"✅ Valid compound: {symbol_id} → '{base}' + '{quote}'"
                            )
                            return {"base_asset": base, "quote_asset": quote}

                # PRIORITY 2: Suffix removal fallback with Binance validation
                base_currency, detected_quote = remove_suffix(clean_symbol)
                base_currency = base_currency.replace("PERP", "").strip()

                if base_currency and detected_quote:
                    # BINANCE-specific validation
                    if exchange in ["binance", "binance-futures"]:
                        # Filter invalid patterns
                        if (
                            (base_currency[0].isdigit() and "1000" not in base_currency)
                            or base_currency == "USDT"
                            or "USDT" in base_currency
                        ):
                            logger.debug(f"🚫 Binance invalid pattern: {symbol_id}")
                            return {"base_asset": "", "quote_asset": ""}

                    return {"base_asset": base_currency, "quote_asset": detected_quote}

                # PRIORITY 3: Default for unparseable symbols
                logger.debug(f"⚠️ Bybit parsing fallback for: {symbol_id}")
                return {"base_asset": clean_symbol, "quote_asset": "USDT"}

            # OKX ENHANCED PARSING (all OKX variants)
            elif exchange in ["okx", "okex", "okex-futures", "okex-swap"]:
                # Handle specific OKX patterns: PERP-USDT, PERP-USDC
                if symbol_id.startswith("PERP-"):
                    quote_part = symbol_id[5:]  # Remove 'PERP-' prefix
                    if quote_part in ["USDT", "USDC", "USD"]:
                        logger.debug(
                            f"✅ OKX PERP pattern: {symbol_id} → 'PERP' + '{quote_part}'"
                        )
                        return {"base_asset": "PERP", "quote_asset": quote_part}

                # Standard OKX patterns: BTC-USDT, BTC-USD-SWAP, etc.
                clean_id = (
                    symbol_id.replace("PERP", "")
                    .replace("SWAP", "")
                    .replace("-SWAP", "")
                )

                # Use comprehensive suffix removal
                base_currency, detected_quote = remove_suffix(clean_id)
                base_currency = base_currency.strip("-_")

                if base_currency and detected_quote:
                    return {"base_asset": base_currency, "quote_asset": detected_quote}

                # Fallback with manual parsing
                if "-" in clean_id:
                    parts = clean_id.split("-")
                    if len(parts) >= 2 and parts[0] and parts[1]:
                        clean_base = parts[0].strip("-")
                        clean_quote = parts[1].strip("-")
                        return {"base_asset": clean_base, "quote_asset": clean_quote}

                return {
                    "base_asset": base_currency or clean_id,
                    "quote_asset": detected_quote or "USD",
                }

            # Fallback with suffix removal
            base_currency, detected_quote = remove_suffix(symbol_id)
            return {"base_asset": base_currency, "quote_asset": detected_quote}

        except Exception as e:
            logger.debug(f"⚠️ Symbol parsing error for {symbol_id}: {e}")
            return {"base_asset": "", "quote_asset": ""}

    async def _populate_all_derived_fields(
        self,
        canonical_key,
        venue,
        inst_type,
        base_asset,
        quote_asset,
        symbol_id,
        exchange=None,
    ):
        """Populate ALL derived fields using sample logic as guide - comprehensive field population."""
        try:
            derived_fields = {}

            # 1. Extract option/derivative parameters from canonical key parsing
            if inst_type == "OPTION":
                # Use the original tardis exchange name for parsing (FIXED use context7)
                tardis_exchange = exchange or venue.lower()
                option_components = self._parse_option_components(
                    symbol_id, tardis_exchange
                )
                logger.debug(
                    f"🔍 Option components for {symbol_id} (exchange: {tardis_exchange}): {option_components}"
                )
                derived_fields.update(
                    {
                        "expiry": option_components.get(
                            "expiry_date", "2025-12-25T08:00:00Z"
                        ),
                        "strike": option_components.get("strike_price", ""),
                        "option_type": option_components.get("option_type", "").upper(),
                    }
                )
                logger.debug(
                    f"🔧 Derived fields for {canonical_key}: strike='{derived_fields.get('strike')}', option_type='{derived_fields.get('option_type')}')"
                )
            elif inst_type == "FUTURE":
                # Parse expiry from symbol or use standard
                expiry_date = self._parse_expiry_from_symbol(
                    symbol_id,
                    self.venue_mapping.tardis_to_venue.get(venue, venue).lower(),
                )
                derived_fields["expiry"] = expiry_date or "2025-12-25T08:00:00Z"

            # 2. Enhanced CCXT integration using cached markets (from sample logic)
            if self.processing_config.enable_ccxt_integration:
                ccxt_data = self.get_ccxt_metadata_from_cache(
                    venue, base_asset, quote_asset, symbol_id
                )

                # Populate CCXT-derived fields (using sample logic)
                if ccxt_data:
                    derived_fields.update(
                        {
                            "ccxt_symbol": ccxt_data.get("ccxt_symbol", ""),
                            "ccxt_exchange": ccxt_data.get("ccxt_exchange", ""),
                            "tick_size": ccxt_data.get("tick_size", ""),
                            "min_size": ccxt_data.get("min_size", ""),
                            # Note: 'max_size' and 'min_notional' fields removed - not part of InstrumentDefinition schema
                            "contract_size": ccxt_data.get("contract_size", ""),
                            # Note: 'active' field removed - not part of InstrumentDefinition schema
                            # Note: precision fields removed - not part of InstrumentDefinition schema
                        }
                    )

            # 3. Business logic fields (from sample logic)
            if venue == "DERIBIT" and quote_asset == "USD":
                derived_fields["inverse"] = True
            elif (
                venue == "DERIBIT"
                and quote_asset
                in self.exchange_config.valid_quote_currencies.get("DERIBIT", [])
            ):
                derived_fields["inverse"] = False

            # 4. Underlying asset for derivatives
            if (
                inst_type in ["PERPETUAL", "FUTURE", "OPTION"]
                and base_asset
                and quote_asset
            ):
                derived_fields["underlying"] = f"{base_asset}-{quote_asset}"

            return derived_fields

        except Exception as e:
            logger.debug(f"⚠️ Error populating derived fields for {canonical_key}: {e}")
            return {}

    def _parse_option_components(self, symbol_id, exchange):
        """Parse option expiry, strike, and type - ENHANCED from canonical_key_generator.py."""
        try:

            if exchange == "deribit":
                # ENHANCED PATTERNS to handle both old and new Deribit formats use context7
                # Pattern 1: Traditional with dash after strike: BTC-25DEC25-50000-C
                # Pattern 2: New format with CALL/PUT: BTC-USD-240329-120000-CALL

                # Enhanced strike price patterns to handle both formats use context7
                # ORDER MATTERS: Most specific patterns first to avoid false matches
                option_strike_patterns = [
                    # New format: BTC-USD-240329-120000-CALL (capture the number before CALL/PUT)
                    re.compile(
                        r"-(\d{6})-(\d+)-(CALL|PUT)$"
                    ),  # Date-Strike-Type: -240329-120000-CALL
                    re.compile(r"-(\d+)-(C|P)$"),  # Short format: -50000-C
                    re.compile(
                        r"-(\d+d?\d*)-"
                    ),  # Traditional: -50000- (least specific, last)
                ]

                # Enhanced option type patterns
                option_type_patterns = [
                    re.compile(r"-(CALL|PUT)$"),  # Full words: -CALL, -PUT
                    re.compile(r"-(C|P)$"),  # Single letters: -C, -P
                ]

                # Extract strike price using enhanced patterns
                strike_price = ""
                for i, pattern in enumerate(option_strike_patterns):
                    strike_match = pattern.search(symbol_id)
                    if strike_match:
                        # Handle different pattern groups (updated for reordered patterns)
                        if (
                            i == 0
                        ):  # Date-Strike-Type pattern: -240329-120000-CALL (now first)
                            strike_raw = strike_match.group(
                                2
                            )  # Second group is strike price
                        else:  # Traditional patterns
                            strike_raw = strike_match.group(
                                1
                            )  # First group is strike price

                        # Convert Deribit decimal format (1d14 -> 1.14, 0d455 -> 0.455)
                        if "d" in strike_raw:
                            strike_price = strike_raw.replace("d", ".")
                        else:
                            strike_price = strike_raw
                        break

                # Extract option type using enhanced patterns
                option_type = ""
                for pattern in option_type_patterns:
                    option_type_match = pattern.search(symbol_id)
                    if option_type_match:
                        match_text = option_type_match.group(1)
                        if match_text in ["CALL", "C"]:
                            option_type = "CALL"
                        elif match_text in ["PUT", "P"]:
                            option_type = "PUT"
                        break

                # Extract expiry date from multiple patterns (original logic)
                expiry_patterns = [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),  # 25DEC25
                    re.compile(r"-(\d{1}[A-Z]{3}\d{2})-"),  # 7NOV25 (single digit day)
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),  # Future format
                    re.compile(r"-(\d{6})$"),  # YYMMDD format
                ]

                expiry_date = ""
                for pattern in expiry_patterns:
                    match = pattern.search(symbol_id)
                    if match:
                        expiry_raw = match.group(1)
                        expiry_date = self._parse_deribit_date(expiry_raw)
                        break

                return {
                    "expiry_date": expiry_date,
                    "strike_price": strike_price,
                    "option_type": option_type,
                }

            return {"expiry_date": "", "strike_price": "", "option_type": ""}

        except Exception as e:
            logger.debug(f"⚠️ Option parsing error for {symbol_id}: {e}")
            return {"expiry_date": "", "strike_price": "", "option_type": ""}

    def _parse_deribit_date(self, date_str):
        """Parse Deribit date format: 25DEC25 → 2025-12-25T08:00:00Z."""
        try:

            # Pattern: 25DEC25
            match = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", date_str)
            if match:
                day, month_str, year = match.groups()

                months = {
                    "JAN": "01",
                    "FEB": "02",
                    "MAR": "03",
                    "APR": "04",
                    "MAY": "05",
                    "JUN": "06",
                    "JUL": "07",
                    "AUG": "08",
                    "SEP": "09",
                    "OCT": "10",
                    "NOV": "11",
                    "DEC": "12",
                }

                month = months.get(month_str, "01")
                full_year = f"20{year}"  # 25 → 2025

                return f"{full_year}-{month}-{day.zfill(2)}T08:00:00Z"  # 8am UTC

        except Exception as e:
            logger.debug(f"⚠️ Date parsing error: {e}")

        return "2025-12-25T08:00:00Z"  # Fallback

    async def _parse_symbol_async(self, symbol_id, exchange):
        """Parse symbol components asynchronously for parallel processing."""
        try:
            base_asset, quote_asset = self._parse_symbol_components(symbol_id, exchange)
            return {"base_asset": base_asset, "quote_asset": quote_asset}
        except Exception as e:
            logger.debug(f"Error parsing symbol {symbol_id}: {e}")
            return {}

    def _parse_expiry_from_symbol(self, symbol_id, exchange):
        """Parse expiry from symbol using comprehensive patterns from original canonical_key_generator.py."""
        try:

            # Comprehensive expiry patterns from original file
            expiry_patterns = {
                "deribit": [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),  # BTC-25DEC25-50000-C
                    re.compile(
                        r"-(\d{1}[A-Z]{3}\d{2})-"
                    ),  # BTC-7NOV25-50000-C (single digit day)
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),  # BTC-26DEC25 (futures)
                    re.compile(
                        r"-(\d{1}[A-Z]{3}\d{2})$"
                    ),  # BTC-7NOV25 (futures, single digit)
                    re.compile(r"-(\d{6})$"),  # BTC-251225 (YYMMDD format)
                ],
                "binance": [
                    re.compile(r"_(\d{6})$"),  # btcusdt_251226 (underscore format)
                    re.compile(r"-(\d{6})$"),  # Alternative format
                ],
                "binance-futures": [
                    re.compile(r"_(\d{6})$"),  # btcusdt_251226 (underscore format)
                    re.compile(r"-(\d{6})$"),  # Alternative format
                ],
                "bybit": [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),  # BTC-25DEC24
                    re.compile(r"([A-Z])(\d{2})$"),  # BTCUSDM25, BTCUSDZ25
                ],
                "okex-futures": [
                    re.compile(r"-(\d{6})$"),  # YYMMDD format
                ],
            }

            # Try patterns for this exchange
            patterns = expiry_patterns.get(exchange, expiry_patterns.get("deribit", []))

            for pattern in patterns:
                match = pattern.search(symbol_id)
                if match:
                    expiry_raw = match.group(1)

                    # Handle different formats
                    if re.match(r"\d{6}", expiry_raw):
                        # YYMMDD format: 251226 → 2025-12-26T08:00:00Z
                        year = f"20{expiry_raw[:2]}"
                        month = expiry_raw[2:4]
                        day = expiry_raw[4:6]
                        return f"{year}-{month}-{day}T08:00:00Z"
                    else:
                        # Deribit format: 25DEC25 → 2025-12-25T08:00:00Z
                        return self._parse_deribit_date(expiry_raw)

            return None

        except Exception as e:
            logger.debug(f"⚠️ Expiry parsing error for {symbol_id}: {e}")
            return None

    def _convert_to_tardis_symbol(self, symbol_id: str, exchange: str) -> str:
        """Convert symbol_id to proper Tardis API format.

        Args:
            symbol_id: Raw symbol from Tardis API (e.g., 'BTCUSDT', 'BTC-USDT', etc.)
            exchange: Exchange name (e.g., 'binance-futures')

        Returns:
            str: Tardis-formatted symbol (e.g., 'btcusdt')
        """
        try:
            # For Binance exchanges, symbols should be lowercase and concatenated
            if exchange in ["binance", "binance-futures"]:
                # Convert SOL-USDT → solusdt, BTCUSDT → btcusdt
                return symbol_id.replace("-", "").lower()

            # For Deribit, keep original format but lowercase
            elif exchange == "deribit":
                return symbol_id.lower()

            # For other exchanges, use lowercase
            else:
                return symbol_id.lower()

        except Exception as e:
            logger.debug(f"Failed to convert symbol {symbol_id} for {exchange}: {e}")
            return symbol_id.lower()  # Fallback to lowercase

    def _is_problematic_binance_instrument(self, symbol_id: str) -> bool:
        """Check if this is a problematic Binance instrument that should be filtered out.

        These instruments have weird naming patterns that break standard parsing logic.

        Args:
            symbol_id: Symbol identifier from Tardis

        Returns:
            bool: True if instrument should be filtered out
        """
        symbol_lower = symbol_id.lower()

        # Filter multiplier tokens (1000x, 1000sats, 1000cat, etc.)
        multiplier_patterns = [
            "1000x",
            "1000sats",
            "1000cat",
            "1000cheems",
            "1mbabydoge",
            "1000pepe",
            "1000shib",
            "1000bonk",
            "1000floki",
            "1000rats",
            "1000btt",
            "1000lunc",
            "1000000mog",
            "1000why",
            "1000000bob",
            "1000000neiro",
            "1000wen",
            "1000usual",
            "1000turbo",
            "1000xec",
            "1000000babydoge",
        ]

        for pattern in multiplier_patterns:
            if symbol_lower.startswith(pattern):
                return True

        # General pattern for any remaining multiplier tokens (future-proof)
        if re.match(r"^(1000|1000000|1m)", symbol_lower):
            return True

        # Filter USDT as base asset (fiat pairs)
        usdt_base_patterns = [
            "usdttry",
            "usdtzar",
            "usdtuah",
            "usdtpln",
            "usdtars",
            "usdtmxn",
            "usdtcop",
            "usdtclp",
            "usdtpen",
            "usdtves",
            "usdtngn",
            "usdtbrl",
            "usdtgel",
            "usdtczk",
            "usdtron",
        ]

        if symbol_lower in usdt_base_patterns:
            return True

        # Filter tokens starting with numbers (except 1000x patterns already caught)
        if symbol_lower.startswith(("1inch", "0g", "2z", "3p", "4p", "5p")):
            return True

        # Filter other known problematic patterns
        problematic_patterns = [
            "nftusdt",  # NFT as base is confusing
            "defiusdt",  # DEFI as base is confusing
            "bullusdt",  # BULL as base is confusing
            "bearusdt",  # BEAR as base is confusing
        ]

        if symbol_lower in problematic_patterns:
            return True

        return False

    def cache_metadata(self, instrument_key: str, metadata: InstrumentDefinition):
        """
        Cache instrument metadata.

        Args:
            instrument_key: Canonical instrument key
            metadata: Metadata to cache
        """
        if self.processing_config.enable_metadata_caching:
            self._metadata_cache[instrument_key] = metadata
            self._cache_timestamps[instrument_key] = datetime.now(timezone.utc)

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics for monitoring"""
        return {
            "supported_exchanges": len(self.processing_config.supported_exchanges),
            "ccxt_integration_enabled": self.processing_config.enable_ccxt_integration,
            "caching_enabled": self.processing_config.enable_metadata_caching,
            "cached_instruments": len(self._metadata_cache),
            "cache_ttl_hours": self.processing_config.cache_ttl_hours,
            "retry_max_attempts": self.processing_config.retry_max_attempts,
        }

    def clear_cache(self):
        """Clear all cached metadata"""
        cache_count = len(self._metadata_cache)
        self._metadata_cache.clear()
        self._cache_timestamps.clear()
        logger.info(f"🧹 Cleared {cache_count} cached instruments")

    def fetch_databento_instruments(
        self, exchange: str, symbols: List[str], target_date: Optional[datetime] = None
    ) -> Dict[str, InstrumentDefinition]:
        """
        Fetch TradFi instruments from Databento.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            symbols: List of symbols to fetch
            target_date: Target date for instrument definitions

        Returns:
            Dictionary mapping instrument_key to InstrumentDefinition
        """
        try:
            from instruments_service.app.venues.databento import DatabentoAdapter

            adapter = DatabentoAdapter()
            date = target_date or datetime.now(timezone.utc)

            raw_instruments = adapter.fetch_instrument_definitions(
                exchange=exchange, symbols=symbols, date=date
            )

            # Convert to InstrumentDefinition objects
            instruments = {}
            for inst_key, inst_data in raw_instruments.items():
                try:
                    inst_def = InstrumentDefinition(**inst_data)
                    instruments[inst_key] = inst_def
                except Exception as e:
                    logger.warning(
                        f"Failed to create InstrumentDefinition for {inst_key}: {e}"
                    )
                    continue

            logger.info(
                f"✅ Fetched {len(instruments)} Databento instruments for {exchange}"
            )
            return instruments

        except ImportError:
            logger.error(
                "Databento adapter not available. Install: pip install databento"
            )
            return {}
        except Exception as e:
            logger.error(f"Failed to fetch Databento instruments: {e}")
            return {}

    def fetch_defi_instruments(
        self, protocol: str, chain: str = "ETHEREUM", **kwargs
    ) -> Dict[str, InstrumentDefinition]:
        """
        Fetch DeFi instruments from various protocols.

        Args:
            protocol: Protocol name ('uniswap_v3', 'curve', 'aave_v3', 'etherfi', 'lido')
            chain: Chain identifier (default: 'ETHEREUM')
            **kwargs: Additional protocol-specific arguments

        Returns:
            Dictionary mapping instrument_key to InstrumentDefinition
        """
        try:
            if protocol.lower() == "uniswap_v3":
                from instruments_service.app.venues.defi import UniswapV3Adapter

                adapter = UniswapV3Adapter(chain=chain)
                raw_instruments = adapter.fetch_pools(**kwargs)

            elif protocol.lower() == "curve":
                from instruments_service.app.venues.defi import CurveAdapter

                adapter = CurveAdapter(chain=chain)
                raw_instruments = adapter.fetch_pools(**kwargs)

            elif protocol.lower() == "aave_v3":
                from instruments_service.app.venues.defi import AaveV3Adapter

                adapter = AaveV3Adapter(chain=chain)
                raw_instruments = adapter.fetch_markets()

            elif protocol.lower() == "etherfi":
                from instruments_service.app.venues.defi import EtherFiAdapter

                adapter = EtherFiAdapter(chain=chain)
                raw_instruments = adapter.fetch_lst_instruments()

            elif protocol.lower() == "lido":
                from instruments_service.app.venues.defi import LidoAdapter

                adapter = LidoAdapter(chain=chain)
                raw_instruments = adapter.fetch_lst_instruments()

            else:
                logger.error(f"Unknown DeFi protocol: {protocol}")
                return {}

            # Convert to InstrumentDefinition objects
            instruments = {}
            for inst_key, inst_data in raw_instruments.items():
                try:
                    inst_def = InstrumentDefinition(**inst_data)
                    instruments[inst_key] = inst_def
                except Exception as e:
                    logger.warning(
                        f"Failed to create InstrumentDefinition for {inst_key}: {e}"
                    )
                    continue

            logger.info(
                f"✅ Fetched {len(instruments)} {protocol} instruments for {chain}"
            )
            return instruments

        except ImportError as e:
            logger.error(f"DeFi adapter not available: {e}")
            return {}
        except Exception as e:
            logger.error(f"Failed to fetch {protocol} instruments: {e}")
            return {}

    def cleanup(self):
        """Cleanup resources and close connections"""
        # Cleanup Tardis adapter (handles its own session)
        if hasattr(self, "tardis_adapter") and self.tardis_adapter:
            self.tardis_adapter.cleanup()

        self.clear_cache()
        logger.info("🧹 InstrumentProcessingService cleanup completed")
