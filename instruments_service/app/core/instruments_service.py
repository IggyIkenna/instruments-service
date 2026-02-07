"""
Main Orchestration Service for Instruments Service

Coordinates instrument processing, storage, and batch operations.
Follows unified repository structure pattern.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import asyncio

from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.batch_processor import InstrumentBatchProcessor
from unified_cloud_services import VenueMapping
from instruments_service.config import UnifiedInstrumentConfig
from instruments_service.app.venues.databento.databento_adapter import DatabentoAdapter
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class ErrorWarningCounter(logging.Handler):
    """Custom logging handler to count ERROR and WARNING messages."""

    def __init__(self):
        super().__init__()
        self.error_count = 0
        self.warning_count = 0

    def emit(self, record):
        """Count errors and warnings."""
        if record.levelno == logging.ERROR:
            self.error_count += 1
        elif record.levelno == logging.WARNING:
            self.warning_count += 1

    def reset(self):
        """Reset counters."""
        self.error_count = 0
        self.warning_count = 0


class InstrumentsService:
    """
    Main orchestration service that coordinates:
    - Instrument processing (fetching from APIs, generating canonical keys)
    - Cloud storage (GCS and BigQuery)
    - Batch operations (date range processing, memory estimation)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the orchestration service.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - gcs_bucket: GCS bucket name (optional, auto-detected)
                - bigquery_dataset: BigQuery dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config

        # Initialize processing service
        from instruments_service.config import instruments_config

        processing_config = {
            "project_id": config.get("project_id") or instruments_config.gcp_project_id,
            "enable_ccxt_integration": config.get("enable_ccxt_integration", True),
            "enable_metadata_caching": config.get("enable_metadata_caching", True),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping
        self.venue_mapping = VenueMapping()

        logger.info("✅ InstrumentsService initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: Optional[List[str] | str] = None,
        instrument_ids: Optional[List[str] | str] = None,
        tradfi_venues: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate instruments for a specific date.

        Args:
            date: Target date for instrument generation
            exchanges: Optional list of exchanges to process (for CeFi mode)
            force: Force regeneration even if instruments exist
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include
            tradfi_venues: Optional list of specific TradFi venues to process (e.g., ["NASDAQ", "NYSE"])

        Returns:
            Dictionary with generation results including error_count and warning_count
        """
        # Set up error/warning counter for this operation
        error_warning_counter = ErrorWarningCounter()
        root_logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)
        error_warning_counter.reset()

        # Initialize variables that may be used in finally block
        success = False
        instruments_df = None

        try:
            date_str = date.strftime("%Y-%m-%d")
            logger.info(f"📅 Generating instruments for {date_str} (CeFi={cefi}, TradFi={tradfi}, DeFi={defi})")

            # Normalize venues filter
            venues_filter = venues or []
            if isinstance(venues_filter, str):
                venues_filter = [venues_filter]
            venues_filter = [v.upper() for v in venues_filter] if venues_filter else []

            # Extract venues from instrument_ids if provided (early filtering)
            # Format: VENUE:INSTRUMENT_TYPE:SYMBOL or VENUE:INSTRUMENT_TYPE:SYMBOL@LIN
            if instrument_ids:
                instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [instrument_ids]
                venues_from_instrument_ids = set()

                for inst_id in instrument_ids_list:
                    # Parse instrument_id to extract venue (first part before :)
                    parts = str(inst_id).split(":")
                    if len(parts) >= 1:
                        venue_from_id = parts[0].upper()
                        venues_from_instrument_ids.add(venue_from_id)
                        logger.debug(f"  Extracted venue '{venue_from_id}' from instrument_id: {inst_id}")

                if venues_from_instrument_ids:
                    logger.info(f"🔍 Extracted venues from instrument_ids: {sorted(venues_from_instrument_ids)}")

                    # If venues filter is also provided, intersect them
                    if venues_filter:
                        # Intersect: only process venues that are in both filters
                        venues_filter = [v for v in venues_filter if v in venues_from_instrument_ids]
                        if not venues_filter:
                            logger.warning(
                                f"⚠️ No matching venues between --venues {venues} and instrument_ids {venues_from_instrument_ids}. "
                                f"Processing will be skipped."
                            )
                    else:
                        # Use venues from instrument_ids as the filter
                        venues_filter = list(venues_from_instrument_ids)
                        logger.info(f"🔍 Using venues from instrument_ids as venue filter: {venues_filter}")

            # Validate venues against allowed venues for each market type
            if venues_filter:
                # Get allowed venues for each market type from config
                # Use all_cefi_venues which includes both Tardis exchanges AND on-chain CLOBs (Hyperliquid, Aster)
                allowed_cefi_venues = set(
                    self.venue_mapping.all_cefi_venues
                )  # Canonical venues from CEFI (Tardis + on-chain CLOBs)
                allowed_tradfi_venues = set(self.venue_mapping.all_databento_venues)  # TRADFI venues
                allowed_defi_venues = set(self.venue_mapping.all_defi_venues)  # DEFI venues

                # Determine which market types are being processed
                market_types_being_processed = []
                if cefi:
                    market_types_being_processed.append("CEFI")
                if tradfi:
                    market_types_being_processed.append("TRADFI")
                if defi:
                    market_types_being_processed.append("DEFI")

                # If no market types specified, all are processed
                if not market_types_being_processed:
                    market_types_being_processed = ["CEFI", "TRADFI", "DEFI"]

                # Collect all allowed venues for the market types being processed
                allowed_venues_for_processing = set()
                if "CEFI" in market_types_being_processed:
                    allowed_venues_for_processing.update(allowed_cefi_venues)
                if "TRADFI" in market_types_being_processed:
                    allowed_venues_for_processing.update(allowed_tradfi_venues)
                if "DEFI" in market_types_being_processed:
                    allowed_venues_for_processing.update(allowed_defi_venues)

                # Validate each venue against allowed venues
                invalid_venues = []
                valid_venues_by_type = {"CEFI": [], "TRADFI": [], "DEFI": []}

                for venue in venues_filter:
                    venue_valid = False
                    if venue in allowed_cefi_venues and "CEFI" in market_types_being_processed:
                        valid_venues_by_type["CEFI"].append(venue)
                        venue_valid = True
                    if venue in allowed_tradfi_venues and "TRADFI" in market_types_being_processed:
                        valid_venues_by_type["TRADFI"].append(venue)
                        venue_valid = True
                    if venue in allowed_defi_venues and "DEFI" in market_types_being_processed:
                        valid_venues_by_type["DEFI"].append(venue)
                        venue_valid = True

                    if not venue_valid:
                        invalid_venues.append(venue)

                # Reject invalid venues with clear error message
                if invalid_venues:
                    error_msg = (
                        f"❌ Invalid venues for market types {market_types_being_processed}: {invalid_venues}\n"
                        f"   Allowed CEFI venues: {sorted(allowed_cefi_venues)}\n"
                        f"   Allowed TRADFI venues: {sorted(allowed_tradfi_venues)}\n"
                        f"   Allowed DEFI venues: {sorted(allowed_defi_venues)}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Log valid venues by market type
                for market_type, valid_venues in valid_venues_by_type.items():
                    if valid_venues:
                        logger.info(f"✅ Valid {market_type} venues: {valid_venues}")

                # If no valid venues after filtering, skip all processing
                if not any(valid_venues_by_type.values()):
                    logger.warning(
                        f"⚠️ No valid venues found after filtering. Skipping all processing. "
                        f"Requested venues: {venues_filter if venues_filter else 'all'}, "
                        f"Market types: {market_types_being_processed}"
                    )
                    # Get error/warning counts before returning
                    error_count = error_warning_counter.error_count
                    warning_count = error_warning_counter.warning_count
                    root_logger.removeHandler(error_warning_counter)
                    return {
                        "status": "warning",
                        "date": date_str,
                        "instruments_generated": 0,
                        "message": "No valid venues found after filtering",
                        "error_count": error_count,
                        "warning_count": warning_count,
                    }

            all_instruments = {}

            # Process CEFI (Tardis) exchanges
            if cefi:
                # Use specified exchanges or all Tardis exchanges
                if exchanges is None:
                    exchanges = self.venue_mapping.all_tardis_exchanges

                # Apply venue filtering for CEFI
                # venues_filter contains canonical venues (e.g., "BINANCE-SPOT", "DERIBIT")
                # We need to map them to raw Tardis exchange names (e.g., "binance", "deribit")
                # Note: Invalid venues have already been rejected in validation above
                if venues_filter:
                    # Filter to only CEFI venues (venues were validated above)
                    cefi_venues = [v for v in venues_filter if v in self.venue_mapping.tardis_to_venue.values()]

                    if cefi_venues:
                        # Use centralized reverse mapping: canonical venue -> list of Tardis exchange names
                        # Note: One canonical venue can map to multiple raw exchanges (e.g., OKX -> okex, okex-futures, okex-swap)
                        venue_to_exchanges = self.venue_mapping.get_venue_to_tardis_exchanges()

                        # Filter exchanges by canonical venues
                        filtered_exchanges = []
                        for canonical_venue in cefi_venues:
                            # Look up canonical venue in mapping
                            if canonical_venue in venue_to_exchanges:
                                # Get all raw exchange names for this canonical venue
                                raw_exchanges_for_venue = venue_to_exchanges[canonical_venue]
                                for raw_exchange in raw_exchanges_for_venue:
                                    # Only include if it's in the current exchanges list
                                    if raw_exchange in exchanges and raw_exchange not in filtered_exchanges:
                                        filtered_exchanges.append(raw_exchange)
                                        logger.debug(
                                            f"  Mapped canonical venue {canonical_venue} -> raw exchange {raw_exchange}"
                                        )

                        if filtered_exchanges:
                            exchanges = filtered_exchanges
                            logger.info(f"🔍 Filtered CEFI exchanges by canonical venues {cefi_venues}: {exchanges}")
                        else:
                            logger.warning(f"⚠️ No matching CEFI exchanges found for canonical venues: {cefi_venues}")
                            exchanges = []  # Don't process any CEFI exchanges if no match
                    else:
                        # venues_filter provided but no CEFI venues in it, skip CEFI processing
                        logger.info("🔍 No CEFI venues in filter, skipping CEFI processing")
                        exchanges = []
                else:
                    # No venue filter specified, use exchanges as-is (either provided or all Tardis exchanges)
                    logger.debug(f"🔍 No venue filter specified, processing exchanges: {exchanges}")

                if exchanges:
                    # Pre-flight check: Validate venue access before processing
                    # This detects Cloudflare blocks early instead of failing per-venue later
                    # Note: tardis_adapter is lazy-loaded, so we need to call _get_tardis_adapter()
                    # to ensure it's initialized before checking venue access
                    adapter = None
                    if hasattr(self.processing_service, "_get_tardis_adapter"):
                        try:
                            adapter = self.processing_service._get_tardis_adapter()
                        except ValueError as e:
                            # API key not available - skip venue access check
                            logger.warning(f"⚠️ Cannot check venue access: {e}")
                    elif hasattr(self.processing_service, "tardis_adapter"):
                        adapter = self.processing_service.tardis_adapter

                    if adapter is not None:
                        access_results = adapter.check_venues_access(exchanges)
                        blocked = [ex for ex, (ok, _) in access_results.items() if not ok]
                        if blocked:
                            for ex in blocked:
                                _, error_msg = access_results[ex]
                                logger.warning(f"⚠️ Venue access blocked: {ex} - {error_msg}")
                            # Filter out blocked exchanges
                            accessible = [ex for ex in exchanges if ex not in blocked]
                            if not accessible:
                                logger.error(f"❌ All {len(exchanges)} CEFI venues blocked - skipping CEFI processing")
                                exchanges = []
                            else:
                                logger.warning(
                                    f"⚠️ {len(blocked)}/{len(exchanges)} venues blocked, "
                                    f"continuing with {len(accessible)} accessible venues"
                                )
                                exchanges = accessible

                if not exchanges:
                    logger.info("⏭️ Skipping CEFI processing - no exchanges to process")
                else:
                    logger.info(f"🚀 Processing {len(exchanges)} CeFi exchanges in parallel...")

                    # OPTIMIZATION: Process all exchanges in parallel using asyncio.gather
                    # Each exchange is independent, so we can parallelize API calls
                    async def process_single_exchange(exchange: str):
                        try:
                            logger.info(f"🔍 Processing CeFi exchange {exchange}...")
                            exchange_instruments = await self.processing_service.process_exchange_instruments(
                                exchange=exchange, target_date=date, force=force
                            )
                            if exchange_instruments:
                                logger.info(f"✅ Processed {len(exchange_instruments)} instruments from {exchange}")
                                return exchange_instruments
                            return {}
                        except Exception as e:
                            logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
                            return {}

                    # Process all exchanges in parallel
                    results = await asyncio.gather(
                        *[process_single_exchange(ex) for ex in exchanges],
                        return_exceptions=False,  # Let individual exceptions be caught in process_single_exchange
                    )

                    # Merge all results
                    for result in results:
                        if result:
                            all_instruments.update(result)

                # Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI
                # These produce CLOB-style data with chain="off-chain", so they're classified as CEFI
                cefi_onchain_clob_venues = self.venue_mapping.all_cefi_onchain_clob_venues
                cefi_clob_protocols = []

                if venues_filter:
                    # Check if any on-chain CLOB venues are in the filter
                    for venue in venues_filter:
                        if venue.upper() in cefi_onchain_clob_venues:
                            protocol_name = venue.lower()
                            cefi_clob_protocols.append((protocol_name, None))
                else:
                    # Process all on-chain CLOB venues
                    for venue in cefi_onchain_clob_venues:
                        cefi_clob_protocols.append((venue.lower(), None))

                if cefi_clob_protocols:
                    logger.info(
                        f"🚀 Processing {len(cefi_clob_protocols)} on-chain CLOB venues (CEFI): {[p[0] for p in cefi_clob_protocols]}"
                    )
                    for protocol, chain in cefi_clob_protocols:
                        try:
                            # Fetch on-chain CLOB instruments via processing_service
                            clob_instruments = self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                target_date=date,
                            )
                            if clob_instruments:
                                all_instruments.update(clob_instruments)
                                logger.info(
                                    f"✅ Processed {len(clob_instruments)} instruments from {protocol} (CEFI on-chain CLOB)"
                                )
                        except Exception as e:
                            logger.error(f"❌ Failed to process on-chain CLOB {protocol}: {e}", exc_info=True)

            # Process TRADFI (Databento) exchanges
            if tradfi:
                try:
                    databento_config = UnifiedInstrumentConfig()

                    # Get all available TradFi exchanges (canonical venues)
                    # Note: all_databento_venues contains canonical venue names (e.g., "CME", "CBOE", "NASDAQ", "NYSE")
                    # The DatabentoAdapter handles mapping these to Databento dataset identifiers internally
                    all_databento_exchanges = self.venue_mapping.all_databento_venues

                    # Apply venue filtering for TRADFI
                    # Priority: tradfi_venues parameter > venues_filter > defaults
                    # Note: Invalid venues have already been rejected in validation above
                    if tradfi_venues:
                        # Use explicit tradfi_venues parameter (from --exchanges NASDAQ etc.)
                        filtered_tradfi_venues = [v for v in tradfi_venues if v in all_databento_exchanges]
                        if filtered_tradfi_venues:
                            databento_exchanges = filtered_tradfi_venues
                            logger.info(f"🔍 Processing specified TradFi venues: {databento_exchanges}")
                        else:
                            logger.warning(
                                f"⚠️ No valid TradFi venues in tradfi_venues={tradfi_venues}, skipping TRADFI processing"
                            )
                            databento_exchanges = []
                    elif venues_filter:
                        # Filter to only TRADFI venues (venues were validated above)
                        filtered_tradfi_venues = [v for v in venues_filter if v in all_databento_exchanges]

                        if filtered_tradfi_venues:
                            databento_exchanges = filtered_tradfi_venues
                            logger.info(
                                f"🔍 Filtered TRADFI exchanges by canonical venues {filtered_tradfi_venues}: {databento_exchanges}"
                            )
                        else:
                            # No TRADFI venues in filter, don't process TRADFI
                            logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                            databento_exchanges = []
                    else:
                        # Default: All TradFi exchanges + FX venue
                        # FX is OTC forex (KRW/USD via Yahoo Finance data provider)
                        databento_exchanges = [
                            "CME",
                            "ICE",
                            "CBOE",
                            "NASDAQ",
                            "NYSE",
                            "FX",
                        ]
                        logger.info(
                            f"🔍 No venue filter specified, processing default TRADFI exchanges: {databento_exchanges}"
                        )

                    if not databento_exchanges:
                        logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
                    else:
                        logger.info(f"🚀 Processing {len(databento_exchanges)} TradFi venues...")

                        # OPTIMIZATION: Process CME, VIX, Yahoo Finance, and NASDAQ/NYSE in parallel
                        async def process_databento_exchange(exchange: str):
                            try:
                                if exchange == "CBOE":
                                    # CBOE only has VIX index (static definition)
                                    # Create Databento adapter instance (reuses cached client)
                                    databento_adapter = DatabentoAdapter()

                                    # Create VIX instrument definition
                                    vix_def_dict = databento_adapter.create_vix_instrument_definition(date)
                                    if vix_def_dict:
                                        vix_def = InstrumentDefinition(**vix_def_dict)
                                        logger.info(f"✅ Created VIX: {vix_def.instrument_key}")
                                        return {vix_def.instrument_key: vix_def}
                                    return {}
                                elif exchange == "FX":
                                    # FX venue: OTC forex instruments (KRW/USD via Yahoo Finance data provider)
                                    # Create Databento adapter instance (reuses cached client)
                                    databento_adapter = DatabentoAdapter()

                                    # Create KRW/USD instrument definition
                                    krwusd_def_dict = databento_adapter.create_krwusd_instrument_definition(date)
                                    if krwusd_def_dict:
                                        krwusd_def = InstrumentDefinition(**krwusd_def_dict)
                                        logger.info(f"✅ Created KRW/USD: {krwusd_def.instrument_key}")
                                        return {krwusd_def.instrument_key: krwusd_def}
                                    return {}
                                elif exchange in ["NASDAQ", "NYSE"]:
                                    # NASDAQ/NYSE: Process ETFs including Bitcoin ETFs
                                    # Create Databento adapter instance (reuses cached client)
                                    databento_adapter = DatabentoAdapter()
                                    instruments = {}

                                    # Get ETF symbols for this venue from config
                                    symbols = databento_config.get_symbols_for_venue(exchange)

                                    # Process Bitcoin ETFs using static definitions
                                    # (more reliable for new ETFs like IBIT, FBTC, ARKB)
                                    # IMPORTANT: Bitcoin ETFs launched January 11, 2024 - skip for earlier dates
                                    bitcoin_etf_launch_date = datetime(2024, 1, 11, tzinfo=timezone.utc)
                                    bitcoin_etf_tickers = ["IBIT", "FBTC", "ARKB"]

                                    # Only process Bitcoin ETFs if date is on or after launch date
                                    if date >= bitcoin_etf_launch_date:
                                        for ticker in bitcoin_etf_tickers:
                                            # Check if this ticker is in the symbols for this venue
                                            if ticker in symbols:
                                                etf_def_dict = (
                                                    databento_adapter.create_bitcoin_etf_instrument_definition(
                                                        ticker, date
                                                    )
                                                )
                                                if etf_def_dict:
                                                    etf_def = InstrumentDefinition(**etf_def_dict)
                                                    instruments[etf_def.instrument_key] = etf_def
                                                    logger.info(f"✅ Created Bitcoin ETF: {etf_def.instrument_key}")
                                    else:
                                        logger.debug(
                                            f"⏭️ Skipping Bitcoin ETFs - date {date.strftime('%Y-%m-%d')} is before launch (2024-01-11)"
                                        )

                                    # Also fetch any other symbols via Databento API
                                    non_btc_etf_symbols = [s for s in symbols if s not in bitcoin_etf_tickers]
                                    if non_btc_etf_symbols:
                                        databento_instruments = (
                                            await self.processing_service.fetch_databento_instruments(
                                                exchange=exchange,
                                                symbols=non_btc_etf_symbols,
                                                target_date=date,
                                            )
                                        )
                                        if databento_instruments:
                                            instruments.update(databento_instruments)
                                            logger.info(
                                                f"✅ Processed {len(databento_instruments)} additional instruments from {exchange}"
                                            )

                                    if instruments:
                                        logger.info(
                                            f"✅ Processed {len(instruments)} total instruments from {exchange}"
                                        )
                                    return instruments
                                elif exchange == "ICE":
                                    # ICE datasets have different availability dates:
                                    # - IFUS.IMPACT (ICE US - Cotton, Coffee, Sugar, etc.): Dec 23, 2018
                                    # - IFEU.IMPACT (ICE Europe - Brent, Gasoil, WTI): Oct 1, 2024
                                    #
                                    # Since we can't filter per-symbol here, we use the earliest date (IFUS)
                                    # The individual symbol downloads will handle IFEU date filtering
                                    ice_us_launch = datetime(2018, 12, 23, tzinfo=timezone.utc)
                                    if date < ice_us_launch:
                                        logger.info(
                                            f"⏭️ Skipping ICE - date {date.strftime('%Y-%m-%d')} is before ICE US dataset launch (2018-12-23)"
                                        )
                                        return {}

                                    # Get symbols for ICE from config
                                    symbols = databento_config.get_symbols_for_venue(exchange)

                                    if not symbols:
                                        logger.warning(f"⚠️ No symbols configured for {exchange}")
                                        return {}

                                    # Fetch Databento instruments
                                    databento_instruments = await self.processing_service.fetch_databento_instruments(
                                        exchange=exchange,
                                        symbols=symbols,
                                        target_date=date,
                                    )

                                    if databento_instruments:
                                        logger.info(f"✅ Processed {len(databento_instruments)} ICE instruments")
                                    return databento_instruments or {}
                                else:
                                    # Get symbols for CME from config
                                    symbols = databento_config.get_symbols_for_venue(exchange)

                                    if not symbols:
                                        logger.warning(f"⚠️ No symbols configured for {exchange}")
                                        return {}

                                    # Fetch Databento instruments (uses cached client, runs in thread pool)
                                    databento_instruments = await self.processing_service.fetch_databento_instruments(
                                        exchange=exchange,
                                        symbols=symbols,
                                        target_date=date,
                                    )

                                    if databento_instruments:
                                        logger.info(
                                            f"✅ Processed {len(databento_instruments)} instruments from {exchange}"
                                        )
                                    return databento_instruments or {}
                            except Exception as e:
                                logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
                                return {}

                        # Process CME and CBOE in parallel
                        results = await asyncio.gather(
                            *[process_databento_exchange(ex) for ex in databento_exchanges],
                            return_exceptions=False,
                        )

                        # Merge results
                        for result in results:
                            if result:
                                all_instruments.update(result)

                except Exception as e:
                    logger.error(f"❌ Failed to initialize Databento processing: {e}", exc_info=True)

            # Process DEFI protocols
            if defi:
                try:
                    # Map canonical venues to protocol names and chains
                    # venues_filter contains canonical venues (e.g., "UNISWAPV3-ETH", "HYPERLIQUID")
                    # This mapping translates canonical venues to raw protocol names used by the processing service
                    venue_to_protocol = {
                        "HYPERLIQUID": ("hyperliquid", None),
                        "ASTER": ("aster", None),
                        "UNISWAPV2-ETH": ("uniswap_v2", "ETHEREUM"),
                        "UNISWAPV3-ETH": ("uniswap_v3", "ETHEREUM"),
                        "UNISWAPV4-ETH": ("uniswap_v4", "ETHEREUM"),
                        # FUTURE IMPLEMENTATION - NOT MVP (The Graph deprecated, RPC fallback needs work)
                        # "CURVE-ETH": ("curve", "ETHEREUM"),
                        # "BALANCER-ETH": ("balancer", "ETHEREUM"),
                        "AAVE_V3_ETH": ("aave_v3", "ETHEREUM"),
                        "ETHERFI": ("etherfi", "ETHEREUM"),
                        "LIDO": ("lido", "ETHEREUM"),
                        "MORPHO-ETHEREUM": ("morpho", "ETHEREUM"),
                        "EULER-PLASMA": ("euler_plasma", None),
                        "FLUID-PLASMA": ("fluid_plasma", None),
                        "AAVE-PLASMA": ("aave_plasma", None),
                        "ETHENA": ("ethena", "ETHEREUM"),
                    }

                    # Common DeFi protocols
                    all_defi_protocols = [
                        ("uniswap_v2", "ETHEREUM"),
                        ("uniswap_v3", "ETHEREUM"),
                        ("uniswap_v4", "ETHEREUM"),
                        ("curve", "ETHEREUM"),
                        ("balancer", "ETHEREUM"),
                        ("aave_v3", "ETHEREUM"),
                        ("etherfi", "ETHEREUM"),
                        ("lido", "ETHEREUM"),
                        ("morpho", "ETHEREUM"),
                        ("euler_plasma", None),
                        ("fluid_plasma", None),
                        ("aave_plasma", None),
                        ("hyperliquid", None),
                        ("aster", None),
                        ("ethena", "ETHEREUM"),
                    ]

                    # Filter protocols if venues specified
                    # Note: Invalid venues have already been rejected in validation above
                    if venues_filter:
                        # Filter to only DEFI venues (venues were validated above)
                        defi_venues = [v for v in venues_filter if v in self.venue_mapping.all_defi_venues]

                        if defi_venues:
                            defi_protocols = []
                            for venue in defi_venues:
                                # venues_filter is already uppercased, but check both cases for robustness
                                venue_key = venue.upper() if venue else ""
                                if venue_key in venue_to_protocol:
                                    protocol, chain = venue_to_protocol[venue_key]
                                    if (protocol, chain) not in defi_protocols:
                                        defi_protocols.append((protocol, chain))
                            if defi_protocols:
                                logger.info(
                                    f"🔍 Filtered DEFI protocols by venues {defi_venues}: {[p[0] for p in defi_protocols]}"
                                )
                            else:
                                # No matching protocols found (shouldn't happen after validation, but handle gracefully)
                                logger.warning(f"⚠️ No matching DEFI protocols found for venues: {defi_venues}")
                                defi_protocols = []
                        else:
                            # No DEFI venues in filter, don't process DEFI
                            logger.info("🔍 No DEFI venues in filter, skipping DEFI processing")
                            defi_protocols = []
                    else:
                        defi_protocols = all_defi_protocols
                        logger.info("🔍 No venue filter specified, processing all DEFI protocols")

                    if not defi_protocols:
                        logger.info("⏭️ Skipping DEFI processing - no protocols to process")
                    else:
                        for protocol, chain in defi_protocols:
                            try:
                                # Fetch DeFi instruments
                                if chain:
                                    defi_instruments = self.processing_service.fetch_defi_instruments(
                                        protocol=protocol,
                                        chain=chain,
                                        target_date=date,
                                    )
                                else:
                                    defi_instruments = self.processing_service.fetch_defi_instruments(
                                        protocol=protocol,
                                        target_date=date,
                                    )
                                if defi_instruments:
                                    all_instruments.update(defi_instruments)
                                    logger.info(f"✅ Processed {len(defi_instruments)} instruments from {protocol}")
                            except Exception as e:
                                logger.error(f"❌ Failed to process {protocol}: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"❌ Failed to initialize DeFi processing: {e}", exc_info=True)

            # If no mode flags specified, process all three modes
            if not cefi and not tradfi and not defi:
                logger.info("📋 No mode flags specified, processing all modes (CeFi, TradFi, DeFi)")
                return await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=True,
                    tradfi=True,
                    defi=True,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )

            # Apply instrument_id filtering if specified
            if instrument_ids:
                instrument_ids_list = instrument_ids if isinstance(instrument_ids, list) else [instrument_ids]
                instrument_ids_set = set(str(inst_id).upper() for inst_id in instrument_ids_list)

                filtered_instruments = {}
                for inst_key, inst_obj in all_instruments.items():
                    # Match case-insensitively
                    if inst_key.upper() in instrument_ids_set:
                        filtered_instruments[inst_key] = inst_obj

                filtered_count = len(all_instruments) - len(filtered_instruments)
                if filtered_count > 0:
                    logger.info(
                        f"🔍 Filtered {filtered_count} instruments by instrument_ids, "
                        f"{len(filtered_instruments)} matching instruments remaining"
                    )

                if filtered_instruments:
                    logger.info(f"✅ Matching instrument_ids: {list(filtered_instruments.keys())}")
                else:
                    logger.warning(
                        f"⚠️ No instruments matched the specified instrument_ids: {instrument_ids_list}. "
                        f"Processed {len(all_instruments)} instruments but none matched."
                    )

                all_instruments = filtered_instruments

            if not all_instruments:
                # With the always-produce architecture, the instruments service should
                # always generate definitions (even on holidays, by querying previous session).
                # If we reach here with zero instruments, it's a genuine error.
                logger.error(
                    f"❌ No instruments generated for {date_str}. "
                    f"This is unexpected with always-produce architecture - "
                    f"holidays/weekends should fall back to previous session definitions."
                )
                # Get error/warning counts before returning
                error_count = error_warning_counter.error_count
                warning_count = error_warning_counter.warning_count
                root_logger.removeHandler(error_warning_counter)
                return {
                    "status": "error",
                    "date": date_str,
                    "instruments_generated": 0,
                    "message": "No instruments generated - check adapter fallback logic",
                    "error_count": error_count,
                    "warning_count": warning_count,
                }

            # Convert to DataFrame
            instruments_list = []
            for inst_key, inst_obj in all_instruments.items():
                if hasattr(inst_obj, "model_dump"):
                    instruments_list.append(inst_obj.model_dump())
                else:
                    instruments_list.append(inst_obj)

            instruments_df = pd.DataFrame(instruments_list)

            # Ensure every requested TradFi venue has at least one row (holiday/closed placeholder)
            # so that by-venue files are always written and unified_trading_deployment missing-data checks pass
            if tradfi and venues_filter:
                requested_tradfi = [v for v in venues_filter if v in self.venue_mapping.all_databento_venues]
                if requested_tradfi and "venue" in instruments_df.columns:
                    if isinstance(date, datetime):
                        target_dt = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    else:
                        target_dt = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
                    if target_dt.tzinfo is None:
                        target_dt = target_dt.replace(tzinfo=timezone.utc)
                    date_iso = target_dt.isoformat().replace("+00:00", "Z")
                    for venue_name in requested_tradfi:
                        if venue_name not in instruments_df["venue"].values:
                            placeholder = {
                                "instrument_key": f"{venue_name}:MARKET_CLOSED:PLACEHOLDER",
                                "venue": venue_name,
                                "instrument_type": "MARKET_CLOSED",
                                "symbol": "PLACEHOLDER",
                                "available_from_datetime": date_iso,
                                "timestamp": target_dt,
                                "market_category": "TRADFI",
                                "is_trading_day": False,
                                "trading_hours_open": "holiday",
                                "trading_hours_close": "holiday",
                                "holiday_calendar": venue_name,
                            }
                            logger.info(
                                f"📅 Adding holiday/closed placeholder for {venue_name} on {date_str} "
                                f"(no instruments from adapter so file would be missing)"
                            )
                            instruments_df = pd.concat(
                                [instruments_df, pd.DataFrame([placeholder])],
                                ignore_index=True,
                            )

            # Store to cloud
            logger.info(f"📤 Storing {len(instruments_df)} instruments to cloud...")
            success = self.cloud_storage.store_instruments(
                instruments_df=instruments_df, table_name="instruments", date=date
            )

            # Get error/warning counts before returning
            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count

            if success:
                logger.info(f"✅ Successfully stored instruments for {date_str}")
                return {
                    "status": "success",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "exchanges_processed": len(exchanges) if exchanges else 0,
                    "venues": (instruments_df["venue"].unique().tolist() if "venue" in instruments_df.columns else []),
                    "error_count": error_count,
                    "warning_count": warning_count,
                }
            else:
                logger.error(f"❌ Failed to store instruments for {date_str}")
                return {
                    "status": "error",
                    "date": date_str,
                    "instruments_generated": len(instruments_df),
                    "message": "Storage failed",
                    "error_count": error_count,
                    "warning_count": warning_count,
                }

        except Exception as e:
            # Get error/warning counts before returning
            root_logger.removeHandler(error_warning_counter)
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count
            logger.error(f"❌ Exception during instrument generation: {e}", exc_info=True)
            return {
                "status": "error",
                "date": date_str if "date_str" in locals() else date.strftime("%Y-%m-%d"),
                "instruments_generated": 0,
                "message": f"Exception during processing: {str(e)}",
                "error_count": error_count,
                "warning_count": warning_count,
            }
        finally:
            # Ensure handler is removed even if exception occurs
            if error_warning_counter in root_logger.handlers:
                root_logger.removeHandler(error_warning_counter)

    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate instruments for a date range.

        Args:
            start_date: Start date
            end_date: End date
            exchanges: Optional list of exchanges to process
            force: Force regeneration
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (applies to all market types)
            instrument_ids: Optional list of specific instrument IDs to include

        Returns:
            Dictionary with batch processing results
        """
        # Get date range from batch processor
        date_range = self.batch_processor.get_required_periods(start_date)

        # Filter to requested range
        date_range = [d for d in date_range if start_date <= d <= end_date]

        logger.info(f"📅 Processing {len(date_range)} dates from {start_date.date()} to {end_date.date()}")

        results = []
        total_generated = 0
        total_errors = 0

        for date in date_range:
            try:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=cefi,
                    tradfi=tradfi,
                    defi=defi,
                    venues=venues,
                    instrument_ids=instrument_ids,
                )
                results.append(result)
                if result.get("status") == "success":
                    total_generated += result.get("instruments_generated", 0)
                else:
                    total_errors += 1
            except Exception as e:
                logger.error(
                    f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}",
                    exc_info=True,
                )
                total_errors += 1
                results.append(
                    {
                        "status": "error",
                        "date": date.strftime("%Y-%m-%d"),
                        "error": str(e),
                    }
                )

        success_count = len([r for r in results if r.get("status") == "success"])
        success_rate = (success_count / len(date_range) * 100) if date_range else 0

        logger.info(f"📊 Batch processing complete: {success_count}/{len(date_range)} successful ({success_rate:.1f}%)")
        logger.info(f"   Total instruments generated: {total_generated}")
        logger.info(f"   Errors: {total_errors}")

        return {
            "status": "success" if total_errors == 0 else "partial",
            "dates_processed": len(date_range),
            "dates_successful": success_count,
            "dates_failed": total_errors,
            "success_rate_percent": success_rate,
            "total_instruments_generated": total_generated,
            "results": results,
        }

    def query_instruments(
        self,
        venue: Optional[str] = None,
        instrument_type: Optional[str] = None,
        base_asset: Optional[str] = None,
        quote_asset: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Query stored instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            base_asset: Optional base asset filter
            quote_asset: Optional quote asset filter

        Returns:
            DataFrame with instruments
        """
        result = self.cloud_storage.query_instruments(venue=venue, instrument_type=instrument_type)
        # Filter by base_asset and quote_asset if provided
        if base_asset is not None and not result.empty and "base_asset" in result.columns:
            result = result[result["base_asset"] == base_asset]
        if quote_asset is not None and not result.empty and "quote_asset" in result.columns:
            result = result[result["quote_asset"] == quote_asset]
        return result

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            "processing_service": self.processing_service.get_processing_stats(),
            "batch_processor": {
                "max_batch_size": self.batch_processor.max_batch_size,
                "lookback_days": self.batch_processor.lookback_days,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "processing_service"):
            self.processing_service.cleanup()
        logger.info("🧹 InstrumentsService cleanup completed")
