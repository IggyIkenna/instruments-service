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

from __future__ import annotations

import asyncio
import logging
import re
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import unified_market_interface.clients.subgraph_service as sg_module
import unified_market_interface.clients.thegraph_base_client as tgc_module
from unified_cloud_services import (
    determine_market_category,
    get_secret_with_fallback,
    handle_api_errors,
)

# Import centralized models and configs (DRY principle)
from unified_config_interface import (
    DataTypeConfig,
    ExchangeInstrumentConfig,
    VenueMapping,
)
from unified_domain_services.instrument_date_filter import DateFilterService
from unified_market_interface import SubgraphService
from unified_market_interface.adapters.tradfi import DatabentoAdapter, TardisAdapter
from unified_market_interface.models.venue_config import VenueMapping as UMI_VenueMapping

from instruments_service.app.core.processors.canonical_key_generator import (
    generate_canonical_key as _generate_canonical_key,
)
from instruments_service.app.core.processors.ccxt_manual_fallback import get_manual_ccxt_fallback
from instruments_service.app.core.processors.defi_processor import fetch_defi_instruments as _fetch_defi_instruments
from instruments_service.app.core.processors.derived_fields_populator import populate_derived_fields
from instruments_service.app.core.processors.symbol_parser import SymbolParser
from instruments_service.config import instruments_config
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.ccxt_service import CCXTService

logger = logging.getLogger(__name__)


@dataclass
class InstrumentProcessingConfig:
    """Configuration for instrument processing operations"""

    api_key: str
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 1.0
    enable_ccxt_integration: bool = True
    enable_metadata_caching: bool = True
    cache_ttl_hours: int = 24
    supported_exchanges: list[str] = field(
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

    def __init__(self, config: dict[str, Any]):
        """
        Initialize instrument processing service.

        Args:
            config: Configuration with instrument processing settings.
                   Can include:
                   - tardis_api_key: Direct API key (optional if Secret Manager available)
                   - api_key: Alternative key name (optional)
                   - project_id: GCP project ID for Secret Manager (default from config)

        Raises:
            ValueError: If API key cannot be retrieved from config or Secret Manager
        """
        self.config = config
        project_id: str = str(config.get("project_id") or instruments_config.gcp_project_id)

        # Try to get API key from config first
        self.api_key: str | None = cast(str | None, config.get("tardis_api_key") or config.get("api_key"))

        # If not in config, try Secret Manager (only if available)
        if not self.api_key:
            try:
                secret_name = instruments_config.tardis_secret_name
                logger.debug(
                    f"Attempting to retrieve Tardis API key from Secret Manager "
                    f"(secret: {secret_name}, project: {project_id})"
                )
                self.api_key = get_secret_with_fallback(
                    secret_name=secret_name,
                    project_id=project_id,
                    fallback_env_var="TARDIS_API_KEY",
                )
                if self.api_key:
                    self.api_key = self.api_key.strip()  # Remove any trailing newlines
                    logger.info("✅ Retrieved Tardis API key from Secret Manager")
                else:
                    logger.warning("⚠️ Tardis API key retrieval returned None (only needed for CeFi)")
            except Exception as e:
                logger.warning(f"⚠️ Tardis API key not available (only needed for CeFi): {e}")
                self.api_key = None  # Not required for DeFi/TradFi modes
        elif not self.api_key:
            logger.debug("Secret Manager not available - Tardis API key will need to be provided via config or env var")

        # Note: API key is optional - only required when fetching CeFi instruments
        # For DeFi/TradFi modes, we don't need Tardis API key

        # Use centralized configs from config.py (DRY principle)
        self.venue_mapping = VenueMapping()
        self.exchange_config = ExchangeInstrumentConfig()
        self.data_config = DataTypeConfig()
        self._symbol_parser = SymbolParser(self.exchange_config)

        retry_attempts: int = int(cast(int | float, config.get("retry_max_attempts", 3)))
        retry_backoff: float = float(cast(int | float, config.get("retry_backoff_factor", 1.0)))
        cache_ttl: int = int(cast(int | float, config.get("cache_ttl_hours", 24)))
        self.processing_config = InstrumentProcessingConfig(
            api_key=self.api_key or "",  # Empty string if not available (only needed for CeFi)
            retry_max_attempts=retry_attempts,
            retry_backoff_factor=retry_backoff,
            enable_ccxt_integration=cast(bool, config.get("enable_ccxt_integration", True)),
            enable_metadata_caching=cast(bool, config.get("enable_metadata_caching", True)),
            cache_ttl_hours=cache_ttl,
            supported_exchanges=self.venue_mapping.all_tardis_exchanges,
        )

        # Initialize metadata cache
        self._metadata_cache: dict[str, InstrumentDefinition] = {}
        self._cache_timestamps: dict[str, datetime] = {}

        # Tardis adapter - lazy-loaded only when needed for CeFi instruments
        self.tardis_adapter: TardisAdapter | None = None
        self._tardis_project_id = project_id

        # Initialize centralized services

        self.subgraph_service: SubgraphService = SubgraphService()
        self.date_filter_service = DateFilterService()

        # Retrieve Graph API key once at initialization and cache it
        # This avoids repeated Secret Manager calls when fetching DeFi instruments
        self._graph_api_key = None
        try:
            project_id_for_graph = instruments_config.gcp_project_id
            secret_name = instruments_config.graph_secret_name
            self._graph_api_key = get_secret_with_fallback(
                project_id=project_id_for_graph,
                secret_name=secret_name,
                fallback_env_var="THE_GRAPH_API_KEY",
            )
            if self._graph_api_key:
                self._graph_api_key = self._graph_api_key.strip()

                # Also set it in TheGraphClient's module-level cache (runtime injection)
                setattr(tgc_module, "_API_KEY_CACHE", self._graph_api_key)
                setattr(tgc_module, "_API_KEY_PROJECT_ID", project_id_for_graph)
                setattr(sg_module, "_GRAPH_API_KEY_CACHE", self._graph_api_key)
                setattr(sg_module, "_GRAPH_API_KEY_PROJECT_ID", project_id_for_graph)
                logger.info("✅ Retrieved and cached Graph API key at initialization")
        except Exception as e:
            logger.debug(f"Could not retrieve Graph API key at initialization: {e}")

        # Initialize CCXT service if enabled
        if self.processing_config.enable_ccxt_integration:
            self.ccxt_service = CCXTService(
                venue_mapping=cast(UMI_VenueMapping, self.venue_mapping),
                cache_ttl_hours=int(cast(int | float, config.get("cache_ttl_hours", 4))),
            )
            # PERFORMANCE: Pre-load CCXT markets in parallel at startup
            # This saves ~5s by loading all exchanges concurrently instead of sequentially
            if config.get("preload_ccxt_markets", True):
                # Get unique CCXT exchange IDs to pre-load
                venues_to_preload = [
                    v
                    for v in self.venue_mapping.all_tardis_exchanges
                    if self.venue_mapping.venue_to_ccxt.get(v.upper())
                ]
                self.ccxt_service.preload_markets_parallel(venues=[v.upper() for v in venues_to_preload], max_workers=4)
        else:
            self.ccxt_service = None

        logger.info(
            f"✅ InstrumentProcessingService initialized: "
            f"api_key={'*' * (len(self.api_key) - 4) + self.api_key[-4:] if self.api_key else 'None'}, "
            f"ccxt_integration={self.processing_config.enable_ccxt_integration}, "
            f"caching={self.processing_config.enable_metadata_caching}"
        )

    def get_venue_mapping(self) -> dict[str, str]:
        """
        Get canonical venue mapping.

        Replaces scattered venue mapping logic.
        """
        # Use centralized venue mapping from config
        return self.venue_mapping.tardis_to_venue

    def get_instrument_type_mapping(self) -> dict[str, str]:
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

    def normalize_venue(self, exchange: str) -> str | None:
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

    def normalize_instrument_type(self, symbol_type: str) -> str | None:
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
        symbol_info: dict[str, Any],
    ) -> str | None:
        """Generate canonical instrument key following INSTRUMENT_KEY.md specification."""
        return _generate_canonical_key(self, exchange, symbol_type, symbol_id, symbol_info)

    def _get_tardis_adapter(self) -> TardisAdapter:
        """
        Lazy-load Tardis adapter only when needed for CeFi instruments.

        Note: Warmup is handled automatically by TardisBaseClient in unified-cloud-services
        when the session is first created.

        Returns:
            TardisAdapter instance

        Raises:
            ValueError: If API key is not available when trying to fetch CeFi instruments
        """
        if self.tardis_adapter is None:
            if not self.api_key:
                raise ValueError(
                    "Tardis API key required for CeFi instruments. "
                    "Provide 'tardis_api_key' in config, or ensure Secret Manager access "
                    "to 'tardis-api-key' secret."
                )

            self.tardis_adapter = TardisAdapter(api_key=self.api_key, project_id=self._tardis_project_id)
            # Note: TardisBaseClient auto-warms up on first request

        return self.tardis_adapter

    @handle_api_errors(max_retries=3)
    async def fetch_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """
        Fetch instrument data from Tardis API for specific exchange.

        OPTIMIZED: Wraps synchronous Tardis API call in asyncio.to_thread for true parallelism.

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

        # OPTIMIZATION: Wrap synchronous Tardis API call in asyncio.to_thread for true parallelism
        # This allows multiple exchanges to be fetched simultaneously without blocking
        (
            available_symbols_list,
            date_filtered_count,
        ) = await asyncio.to_thread(
            self._get_tardis_adapter().fetch_exchange_instruments,
            exchange=exchange,
            target_date=target_date,
            force_refresh=force,
        )
        # Convert list to dict keyed by symbol_id
        available_symbols: dict[str, dict[str, Any]] = {
            (str(cast(str | None, symbol.get("id"))) or ""): symbol
            for symbol in available_symbols_list
            if symbol.get("id")
        }

        # Filter instruments available at target date AND exclude unwanted types
        instruments_data: dict[str, dict[str, Any]] = {}
        for symbol_id, symbol in available_symbols.items():
            if not symbol_id:  # Skip if symbol_id is empty
                continue
            symbol_type = symbol.get("type") or ""

            # Filter excluded instrument types (e.g., combos)
            if symbol_type in self.data_config.excluded_instrument_types:
                logger.debug(f"Filtered excluded instrument type: {symbol_id} ({symbol_type})")
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
            logger.info(f"✅ Filtered to {len(instruments_data)} instruments available on {date_str}")
        else:
            logger.info(f"✅ Retrieved {len(instruments_data)} currently active instruments")
        return instruments_data, date_filtered_count

    async def process_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process all instruments for an exchange and generate canonical keys.

        .. deprecated::
            CeFi path now uses UMI: ``get_adapter("tardis").fetch_instruments(exchange, normalize=True)``.
            This method will be removed in a future release. Use InstrumentsService.generate_instruments_for_date
            with cefi=True, or UMI get_adapter directly.

        Replaces complex processing logic from CanonicalInstrumentKeyGenerator.

        Args:
            exchange: Exchange name
            target_date: Target date for processing (if provided, filters by availableSince/availableTo)
            force: If True, force regeneration even if files exist (date filtering still applies)

        Returns:
            Dictionary of processed instrument metadata keyed by canonical key
        """
        warnings.warn(
            "process_exchange_instruments is deprecated. CeFi path uses UMI get_adapter('tardis').fetch_instruments. "
            "Use InstrumentsService.generate_instruments_for_date(cefi=True) or UMI directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        fetch_result = await self.fetch_exchange_instruments(exchange, target_date, force)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType,reportUnknownVariableType]
        instruments_data, date_filtered_count = cast(tuple[dict[str, dict[str, Any]], int], fetch_result)

        # CRITICAL OPTIMIZATION: Apply exchange config filtering BEFORE expensive processing
        canonical_venue = self.normalize_venue(exchange) or exchange
        valid_types: list[str] = self.exchange_config.exchange_instrument_types.get(canonical_venue, [])
        valid_quotes: list[str] = self.exchange_config.valid_quote_currencies.get(canonical_venue, ["USDT"])

        logger.info(
            f"🔍 Pre-filtering by exchange config: {canonical_venue} accepts types={valid_types}, quotes={valid_quotes}"
        )

        # Get excluded base currencies and symbol patterns for this exchange
        excluded_bases: list[str] = self.exchange_config.excluded_base_currencies.get(canonical_venue, [])
        excluded_patterns: list[str] = self.exchange_config.excluded_symbol_patterns.get(canonical_venue, [])

        # Pre-filter by exchange config before expensive processing
        pre_filtered: dict[str, dict[str, Any]] = {}
        for _sid, _sinfo in instruments_data.items():
            symbol_id: str = _sid
            symbol_info: dict[str, Any] = _sinfo
            symbol_type: str = (cast(str | None, symbol_info.get("type")) or "").lower()
            normalized_type = self.normalize_instrument_type(symbol_type)

            # Filter by valid instrument types for this exchange
            if normalized_type not in valid_types:
                continue

            # Check excluded symbol patterns early (e.g., leveraged products like 3L, 2L)
            if excluded_patterns:
                symbol_upper: str = symbol_id.upper()
                excluded_by_pattern = False
                for pattern in excluded_patterns:
                    if pattern.upper() in symbol_upper:
                        logger.debug(
                            f"🚫 Pre-filtered out {symbol_id}: symbol pattern '{pattern}' "
                            f"excluded for {canonical_venue}"
                        )
                        excluded_by_pattern = True
                        break
                if excluded_by_pattern:
                    continue

            # Quick parse base and quote currency to check validity (before full processing)
            parsed_components = self.parse_symbol_components(symbol_id, exchange)
            if isinstance(parsed_components, dict):
                base_asset = (parsed_components.get("base_asset") or "").upper()
                quote_asset = (parsed_components.get("quote_asset") or "").upper()
            else:
                base_asset, quote_asset = parsed_components if parsed_components else ("", "")
                base_asset = base_asset.upper() if base_asset else ""
                quote_asset = quote_asset.upper() if quote_asset else ""

            # Filter by excluded base currencies
            if base_asset and base_asset in excluded_bases:
                logger.debug(f"🚫 Pre-filtered out {symbol_id}: base '{base_asset}' excluded for {canonical_venue}")
                continue

            # Filter by valid quote currencies for this exchange
            if quote_asset and quote_asset not in valid_quotes:
                continue

            pre_filtered[symbol_id] = symbol_info

        logger.info(
            f"🔍 Exchange config filter: {len(pre_filtered)}/{len(instruments_data)} instruments "
            f"valid for {canonical_venue}"
        )
        instruments_data = pre_filtered

        # MVP base asset filtering for specific venues (e.g., UPBIT, COINBASE)
        # These venues only include instruments for the 21 MVP base assets (for premium calculations)
        if canonical_venue in self.venue_mapping.spot_mvp_filtered_venues:
            mvp_base_assets = {b.upper() for b in self.venue_mapping.hyperliquid_aster_mvp_base_assets}
            mvp_filtered: dict[str, dict[str, Any]] = {}
            for symbol_id, symbol_info in instruments_data.items():
                # Parse base asset from symbol_id
                parsed_components = self.parse_symbol_components(symbol_id, exchange)
                if isinstance(parsed_components, dict):
                    base_asset = (parsed_components.get("base_asset") or "").upper()
                else:
                    base_asset, _ = parsed_components if parsed_components else ("", "")
                    base_asset = base_asset.upper() if base_asset else ""

                # Only include if base asset is in MVP list
                if base_asset in mvp_base_assets:
                    mvp_filtered[symbol_id] = symbol_info
                else:
                    logger.debug(f"🚫 MVP filter: {symbol_id} excluded (base '{base_asset}' not in MVP list)")

            logger.info(
                f"🔍 MVP base asset filter: {len(mvp_filtered)}/{len(instruments_data)} instruments "
                f"for {canonical_venue} (21 MVP coins)"
            )
            instruments_data = mvp_filtered

        # Track filtering statistics for debugging
        # Include date_filtered from fetch_exchange_instruments
        filter_stats: dict[str, int] = {
            "no_canonical_key": 0,
            "same_base_quote": 0,
            "date_filtered": date_filtered_count,  # From fetch_exchange_instruments
            "expired_filtered": 0,  # Filtered by availableTo (expired/delisted instruments)
            "expiry_filtered": 0,  # Filtered by expiry date (futures/options)
            "processing_error": 0,
            "success": 0,
        }

        # NOTE: Processing loop is CPU-bound (parsing, validation, object creation)
        # Python's GIL prevents true CPU parallelism with asyncio, so sequential is fine
        # The real performance gain is from parallel I/O (API fetches), which we've optimized
        processed_instruments: dict[str, InstrumentDefinition] = {}
        for symbol_id, symbol_info in instruments_data.items():
            try:
                # Generate canonical key
                canonical_key = self.generate_canonical_key(
                    exchange=exchange,
                    symbol_type=cast(str, symbol_info.get("type") or ""),
                    symbol_id=symbol_id,
                    symbol_info=symbol_info,
                )

                if not canonical_key:
                    filter_stats["no_canonical_key"] += 1
                    if exchange == "deribit":
                        logger.debug(
                            f"🚫 Deribit: No canonical key for {symbol_id} (type: {symbol_info.get('type') or ''})"
                        )
                    continue

                if canonical_key:
                    # Parse base/quote from symbol_id (Tardis doesn't provide these fields)
                    parsed_components: dict[str, Any] = self.parse_symbol_components(symbol_id, exchange)
                    if isinstance(parsed_components, dict):
                        base_asset = cast(str, parsed_components.get("base_asset") or "")
                        quote_asset = cast(str, parsed_components.get("quote_asset") or "")
                    else:
                        base_asset, quote_asset = parsed_components if parsed_components else ("", "")

                    # Clean and validate parsed values (fix NoneType errors)
                    clean_base = str(base_asset or "").upper() if base_asset is not None else ""
                    clean_quote = str(quote_asset or "").upper() if quote_asset is not None else ""

                    # Filter nonsensical pairs: base_asset == quote_asset (USDT-USDT, BTC-BTC, etc.)
                    if clean_base == clean_quote and clean_base:
                        filter_stats["same_base_quote"] += 1
                        logger.debug(
                            f"🚫 Filtered nonsensical pair: {symbol_id} ({clean_base}-{clean_quote} - same asset)"
                        )
                        continue

                    # Infer settle_asset for Deribit using centralized config (DRY principle)
                    settle_asset = "USDT"
                    canonical_venue_raw = self.normalize_venue(exchange)
                    canonical_venue = canonical_venue_raw or exchange
                    if canonical_venue == "DERIBIT":
                        deribit_quotes = self.exchange_config.valid_quote_currencies.get("DERIBIT", [])
                        if clean_quote == "USD":
                            settle_asset = clean_base  # Coin margin - settle in base asset
                        elif clean_quote in deribit_quotes and clean_quote != "USD":
                            settle_asset = clean_quote  # Cash settled (USDC, etc.)

                    # Extract symbol from canonical key (VENUE:INSTRUMENT_TYPE:SYMBOL)
                    symbol: str = canonical_key.split(":", 2)[2] if len(canonical_key.split(":")) >= 3 else symbol_id

                    # Populate ALL derived fields BEFORE model creation (FIXED for Pydantic validation use context7)
                    norm_inst_type: str = self.normalize_instrument_type(symbol_info.get("type") or "") or ""
                    enhanced_fields: dict[str, Any] = await self._populate_all_derived_fields(
                        canonical_key,
                        canonical_venue,
                        norm_inst_type,
                        clean_base,
                        clean_quote,
                        symbol_id,
                        exchange,  # Pass original tardis exchange name
                    )

                    # Create metadata object with ALL fields including derived ones (InstrumentDefinition schema)
                    normalized_instrument_type = self.normalize_instrument_type(symbol_info.get("type") or "")

                    # CRITICAL: Set data_types based on instrument_type from config
                    # All instrument types (including OPTION) now use config-based data types
                    config_data_types = self.data_config.instrument_data_types.get(
                        normalized_instrument_type or "SPOT_PAIR",
                        ["trades", "book_snapshot_5"],
                    )
                    data_types_str = ",".join(config_data_types)

                    # CRITICAL: Set tardis_exchange based on venue+instrument_type mapping
                    # This ensures we use the correct Tardis endpoint (e.g., okex-swap for OKX PERPETUAL)
                    mapping_key: tuple[str, str] = (
                        canonical_venue,
                        normalized_instrument_type or "SPOT_PAIR",
                    )
                    tardis_exchange: str = self.venue_mapping.venue_instrument_type_to_tardis.get(
                        mapping_key,
                        exchange.lower(),  # Fallback to original exchange name
                    )

                    # CRITICAL: Get available_to from Tardis API (availableTo field)
                    # Tardis provides availableTo for expired/delisted instruments (including spot pairs)
                    available_to: str | None = cast(str | None, symbol_info.get("availableTo"))
                    available_to_datetime = None

                    if available_to:
                        # Parse and normalize availableTo from Tardis API
                        try:
                            # Ensure proper ISO format with timezone
                            if isinstance(available_to, str):
                                if available_to.endswith("Z"):
                                    available_to_datetime = available_to
                                else:
                                    # Add timezone if missing
                                    available_to_datetime = (
                                        available_to.replace("Z", "+00:00") if "+" not in available_to else available_to
                                    )
                            else:
                                available_to_datetime = str(available_to)
                        except (ValueError, TypeError) as e:
                            logger.debug(f"⚠️ Could not parse availableTo '{available_to}': {e}")

                    # CRITICAL FIX: For options and futures, set available_to_datetime to midnight of day after expiry
                    # if Tardis didn't provide availableTo. This ensures all derivatives have expiration dates.
                    if (
                        not available_to_datetime
                        and normalized_instrument_type in ["FUTURE", "OPTION"]
                        and "expiry" in enhanced_fields
                    ):
                        expiry_str: str = cast(str, enhanced_fields.get("expiry") or "")
                        if expiry_str:
                            try:
                                # Parse expiry datetime
                                expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))

                                # Get the date of expiry and add one day, then set to midnight UTC
                                expiry_date = expiry_dt.date()
                                day_after_expiry = expiry_date + timedelta(days=1)
                                # Set to midnight UTC (00:00:00)
                                available_to_datetime = (
                                    datetime.combine(day_after_expiry, datetime.min.time())
                                    .replace(tzinfo=timezone.utc)
                                    .isoformat()
                                )

                                logger.debug(
                                    f"✅ Set available_to_datetime for {symbol_id} to midnight "
                                    f"after expiry: {available_to_datetime}"
                                )
                            except Exception as e:
                                logger.debug(f"⚠️ Could not parse expiry '{expiry_str}' for available_to_datetime: {e}")

                    # CRITICAL FIX: Filter out expired instruments (ALL types, including spot)
                    # Tardis provides availableTo for expired/delisted instruments - we must respect this
                    if available_to_datetime:
                        try:
                            # Parse available_to_datetime for comparison
                            available_to_dt = datetime.fromisoformat(available_to_datetime.replace("Z", "+00:00"))
                            if available_to_dt.tzinfo is None:
                                available_to_dt = available_to_dt.replace(tzinfo=timezone.utc)

                            # Get comparison date (use target_date if provided, otherwise today)
                            comparison_date = target_date if target_date else datetime.now(timezone.utc)
                            if comparison_date.tzinfo is None:
                                comparison_date = comparison_date.replace(tzinfo=timezone.utc)

                            # Filter if instrument has expired (availableTo is in the past)
                            if comparison_date.date() > available_to_dt.date():
                                filter_stats["expired_filtered"] = filter_stats.get("expired_filtered", 0) + 1
                                logger.debug(
                                    f"🚫 Filtered expired instrument: {symbol_id} - availableTo "
                                    f"{available_to_dt.date()} < comparison_date {comparison_date.date()}"
                                )
                                continue
                        except (ValueError, TypeError) as e:
                            logger.debug(
                                f"⚠️ Could not parse available_to_datetime '{available_to_datetime}' for filtering: {e}"
                            )

                    # NOTE: Date filtering (availableSince/availableTo) is already done in fetch_exchange_instruments
                    # But we also check expiry for futures/options as a secondary filter
                    # Check expiry for futures/options: filter if expired (secondary check)
                    if (
                        target_date
                        and normalized_instrument_type in ["FUTURE", "OPTION"]
                        and "expiry" in enhanced_fields
                    ):
                        expiry_str_filter: str = cast(str, enhanced_fields.get("expiry") or "")
                        if expiry_str_filter:
                            try:
                                expiry_dt = datetime.fromisoformat(expiry_str_filter.replace("Z", "+00:00"))
                                # Convert both to date objects for comparison
                                target_date_only = target_date.date() if hasattr(target_date, "date") else target_date
                                expiry_date_only = expiry_dt.date()

                                if target_date_only > expiry_date_only:
                                    filter_stats["expiry_filtered"] += 1
                                    logger.debug(
                                        f"🚫 Filtered: {symbol_id} - expiry {expiry_date_only} "
                                        f"< target_date {target_date_only}"
                                    )
                                    continue
                            except (ValueError, TypeError) as e:
                                logger.debug(f"⚠️ Could not parse expiry '{expiry_str_filter}': {e}")

                    # Determine market category

                    instrument_dict: dict[str, str | None] = {
                        "databento_symbol": "",  # CeFi instruments don't have databento_symbol
                        "chain": "off-chain",
                    }
                    market_category = determine_market_category(instrument_dict)

                    venue_str: str = canonical_venue
                    inst_type_str: str = normalized_instrument_type or "SPOT_PAIR"
                    metadata = InstrumentDefinition(
                        instrument_key=canonical_key,
                        venue=venue_str,
                        instrument_type=inst_type_str,
                        symbol=symbol,  # ✅ FIXED: symbol_id → symbol (required field)
                        base_asset=clean_base,
                        quote_asset=clean_quote,
                        settle_asset=settle_asset,  # ✅ ADDED: Direct field instead of attributes
                        chain="off-chain",  # CeFi exchanges are off-chain
                        market_category=market_category,  # Auto-populated market category
                        exchange_raw_symbol=symbol_id,
                        tardis_symbol=self._convert_to_tardis_symbol(
                            symbol_id, exchange
                        ),  # ✅ FIXED: Convert to proper Tardis format
                        tardis_exchange=tardis_exchange,  # venue+instrument_type mapping
                        available_from_datetime=symbol_info.get("availableSince") or "",
                        available_to_datetime=available_to_datetime,  # from Tardis or expiry
                        data_types=data_types_str,  # ✅ FIXED: Set from config based on instrument_type
                        # Include derived fields directly in model creation (FIXED for validation timing use context7)
                        **enhanced_fields,  # type: ignore[reportAny]
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
        logger.info(f"📊 Processed {len(processed_instruments)} instruments from {exchange}")
        if total_filtered > 0:
            logger.info(
                f"🔍 Filtering breakdown for {exchange}: "
                f"no_key={filter_stats['no_canonical_key']}, "
                f"same_base_quote={filter_stats['same_base_quote']}, "
                f"date_filtered={filter_stats['date_filtered']}, "
                f"expired_filtered={filter_stats.get('expired_filtered', 0)}, "
                f"expiry_filtered={filter_stats['expiry_filtered']}, "
                f"errors={filter_stats['processing_error']}, "
                f"success={filter_stats['success']}"
            )
        return processed_instruments

    async def generate_instruments_for_exchanges(
        self,
        exchanges: list[str],
        target_date: datetime | None = None,
        max_parallel: int | None = None,
        force: bool = False,
    ) -> dict[str, InstrumentDefinition]:
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
        supported_exchanges = [ex for ex in exchanges if ex.lower() in self.processing_config.supported_exchanges]

        if not supported_exchanges:
            logger.warning(f"No supported exchanges in: {exchanges}")
            return {}

        logger.info(f"🚀 Processing {len(supported_exchanges)} exchanges: {supported_exchanges}")

        # Process exchanges individually for now (can be parallelized later with ConcurrencyService)
        all_instruments: dict[str, InstrumentDefinition] = {}

        for exchange in supported_exchanges:
            try:
                exchange_instruments = await self.process_exchange_instruments(exchange, target_date, force)
                all_instruments.update(exchange_instruments)

                logger.info(f"✅ {exchange}: {len(exchange_instruments)} instruments processed")

            except Exception as e:
                logger.error(f"❌ Failed to process {exchange}: {e}")

        logger.info(
            f"📊 Total instruments processed: {len(all_instruments)} across {len(supported_exchanges)} exchanges"
        )
        return all_instruments

    def filter_instruments_by_exchange_config(
        self, instruments: dict[str, dict[str, Any]], exchange: str
    ) -> dict[str, dict[str, Any]]:
        """Filter instruments by exchange-specific capabilities using centralized config (DRY)."""
        # Use centralized configs (already loaded)
        canonical_venue: str = self.normalize_venue(exchange) or exchange
        valid_types: list[str] = self.exchange_config.exchange_instrument_types.get(canonical_venue, ["SPOT_PAIR"])
        valid_quotes: list[str] = self.exchange_config.valid_quote_currencies.get(canonical_venue, ["USDT"])
        is_derivative = canonical_venue in self.exchange_config.derivative_exchanges

        # Get excluded base currencies and symbol patterns for this exchange
        excluded_bases: list[str] = self.exchange_config.excluded_base_currencies.get(canonical_venue, [])
        excluded_patterns: list[str] = self.exchange_config.excluded_symbol_patterns.get(canonical_venue, [])

        filtered: dict[str, dict[str, Any]] = {}

        for inst_key, inst_data in instruments.items():
            try:
                # Check instrument type is valid for this exchange
                inst_type: str = cast(str, inst_data.get("instrument_type") or "")
                if inst_type not in valid_types:
                    logger.debug(f"❌ Filtered out {inst_key}: {inst_type} not valid for {canonical_venue}")
                    continue

                # Check quote currency is valid for this exchange (FIXED - was not working)
                quote_asset: str = (cast(str | None, inst_data.get("quote_asset")) or "").upper()
                if quote_asset not in valid_quotes:
                    logger.debug(
                        f"🚫 Filtered out {inst_key}: quote '{quote_asset}' not in valid quotes "
                        f"{valid_quotes} for {canonical_venue}"
                    )
                    continue

                # Check excluded base currencies
                base_asset: str = (cast(str | None, inst_data.get("base_asset")) or "").upper()
                if base_asset in excluded_bases:
                    logger.debug(
                        f"🚫 Filtered out {inst_key}: base currency '{base_asset}' excluded for {canonical_venue}"
                    )
                    continue

                # Check excluded symbol patterns (e.g., leveraged products like 3L, 2L)
                symbol: str = (cast(str | None, inst_data.get("symbol")) or "").upper()
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
                inst_data = self._populate_complete_instrument_data(inst_data, exchange, is_derivative)
                filtered[inst_key] = inst_data

            except Exception as e:
                logger.warning(f"⚠️ Error filtering {inst_key}: {e}")

        return filtered

    def _populate_complete_instrument_data(
        self, inst_data: dict[str, Any], exchange: str, is_derivative: bool
    ) -> dict[str, Any]:
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
            base_asset: str = cast(str, inst_data.get("base_asset") or "")
            quote_asset: str = cast(str, inst_data.get("quote_asset") or "")
            if base_asset and quote_asset:
                inst_data["underlying"] = f"{base_asset}-{quote_asset}"
        else:
            inst_data["venue_type"] = "spot"
            inst_data["underlying"] = ""  # Not applicable for spot

        # Data types based on instrument type from config
        # All instrument types (including OPTION) now use config-based data types
        inst_type: str = cast(str, inst_data.get("instrument_type", "SPOT_PAIR"))
        inst_data["data_types"] = ",".join(
            self.data_config.instrument_data_types.get(inst_type, ["trades", "book_snapshot_5"])
        )

        return inst_data

    def get_manual_ccxt_fallback(self, venue: str, base_asset: str) -> dict[str, Any]:
        """Manual fallback mappings when CCXT lookup fails."""
        return get_manual_ccxt_fallback(venue, base_asset)

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, Any]:
        """Parse base/quote assets from Tardis symbol ID."""
        return self._symbol_parser.parse_symbol_components(symbol_id, exchange)

    async def _populate_all_derived_fields(
        self,
        canonical_key: str,
        venue: str,
        inst_type: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        exchange: str | None = None,
    ) -> dict[str, Any]:
        """Populate ALL derived fields for instrument definition."""
        from instruments_service.app.core.processors.derived_fields_populator import (
            DerivedFieldsServiceProtocol,
        )

        return await populate_derived_fields(
            cast(DerivedFieldsServiceProtocol, self),
            canonical_key,
            venue,
            inst_type,
            base_asset,
            quote_asset,
            symbol_id,
            exchange,
        )

    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, Any]:
        """Parse option expiry, strike, and type."""
        return self._symbol_parser.parse_option_components(symbol_id, exchange)

    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None:
        """Parse expiry from symbol using exchange-specific patterns."""
        return self._symbol_parser.parse_expiry_from_symbol(symbol_id, exchange)

    def _parse_deribit_date(self, date_str: str) -> str:
        """Parse Deribit date format: 25DEC25 → 2025-12-25T08:00:00Z."""
        return self._symbol_parser.parse_deribit_date(date_str)

    async def _parse_symbol_async(self, symbol_id: str, exchange: str) -> dict[str, str]:
        """Parse symbol components asynchronously for parallel processing."""
        try:
            parsed = self.parse_symbol_components(symbol_id, exchange)
            base: object = parsed.get("base_asset")  # type: ignore[reportAny]
            quote: object = parsed.get("quote_asset")  # type: ignore[reportAny]
            return {
                "base_asset": str(base) if base is not None else "",
                "quote_asset": str(quote) if quote is not None else "",
            }
        except Exception as e:
            logger.debug(f"Error parsing symbol {symbol_id}: {e}")
            return {}

    def _convert_to_tardis_symbol(self, symbol_id: str, exchange: str) -> str:
        """Convert symbol_id to proper Tardis API format.

        Args:
            symbol_id: Raw symbol from Tardis API (e.g., 'BTCUSDT', 'BTC-USDT', 'KRW-VET')
            exchange: Exchange name (e.g., 'binance-futures', 'upbit', 'coinbase')

        Returns:
            str: Tardis-formatted symbol (e.g., 'btcusdt', 'KRW-VET', 'SOL-USD')
        """
        try:
            # For Binance exchanges, symbols should be lowercase and concatenated
            if exchange in ["binance", "binance-futures"]:
                # Convert SOL-USDT → solusdt, BTCUSDT → btcusdt
                return symbol_id.replace("-", "").lower()

            # For Deribit, keep original format but lowercase
            elif exchange == "deribit":
                return symbol_id.lower()

            # For Upbit and Coinbase, keep uppercase (Tardis expects uppercase for these)
            elif exchange in ["upbit", "coinbase"]:
                return symbol_id.upper()

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

    def get_processing_stats(self) -> dict[str, Any]:
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

    @handle_api_errors(max_retries=3)
    async def fetch_databento_instruments(
        self, exchange: str, symbols: list[str], target_date: datetime | None = None
    ) -> dict[str, InstrumentDefinition]:
        """
        Fetch TradFi instruments from Databento.

        OPTIMIZED: Async wrapper for Databento API calls to enable true parallelism.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            symbols: List of symbols to fetch
            target_date: Target date for instrument definitions

        Returns:
            Dictionary mapping instrument_key to InstrumentDefinition
        """

        try:
            adapter = DatabentoAdapter()
            date = target_date or datetime.now(timezone.utc)

            # OPTIMIZATION: Wrap synchronous Databento API call in asyncio.to_thread for true parallelism
            raw_instruments = await asyncio.to_thread(
                adapter.fetch_instrument_definitions, exchange=exchange, symbols=symbols, date=date
            )

            # Convert to InstrumentDefinition objects
            instruments: dict[str, InstrumentDefinition] = {}
            for inst_key, inst_data in raw_instruments.items():
                try:
                    inst_def = InstrumentDefinition(**inst_data)  # type: ignore[reportAny]
                    instruments[inst_key] = inst_def
                except Exception as e:
                    logger.warning(f"Failed to create InstrumentDefinition for {inst_key}: {e}")
                    continue

            logger.info(f"✅ Fetched {len(instruments)} Databento instruments for {exchange}")
            return instruments

        except ImportError:
            logger.error("Databento adapter not available. Install: pip install databento")
            return {}
        except Exception as e:
            logger.error(f"Failed to fetch Databento instruments: {e}")
            return {}

    def fetch_defi_instruments(
        self,
        protocol: str,
        chain: str = "ETHEREUM",
        target_date: datetime | None = None,
        **kwargs: Any,  # type: ignore[reportAny]
    ) -> dict[str, InstrumentDefinition]:
        """Fetch DeFi instruments from various protocols."""
        from instruments_service.app.core.processors.defi_processor import DefiServiceProtocol

        return _fetch_defi_instruments(
            cast(DefiServiceProtocol, self),
            protocol,
            chain,
            target_date,
            **kwargs,  # type: ignore[reportAny]
        )

    def cleanup(self):
        """Cleanup resources and close connections"""
        # Cleanup Tardis adapter (handles its own session)
        if hasattr(self, "tardis_adapter") and self.tardis_adapter is not None:
            self.tardis_adapter.cleanup()

        # Cleanup CCXT service cache
        if hasattr(self, "ccxt_service") and self.ccxt_service:
            self.ccxt_service.clear_cache()

        # Cleanup subgraph service cache
        if hasattr(self, "subgraph_service") and self.subgraph_service:
            self.subgraph_service.clear_cache()

        # Clear metadata cache
        self._metadata_cache.clear()
        self._cache_timestamps.clear()

        logger.info("🧹 InstrumentProcessingService cleanup completed")
