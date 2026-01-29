"""
Databento Venue Adapter - REFACTORED

Fetches TradFi instrument definitions from Databento API.
Supports CME + VIX with performance optimizations:

ARCHITECTURE:
- Uses DatabentoBaseClient from unified-cloud-services (centralized network layer)
- This adapter handles domain-specific logic (instrument parsing, trading hours)
- Network concerns (sessions, retries, API keys) are handled by DatabentoBaseClient
- Cached UnifiedInstrumentConfig instance
- Parallel symbol group queries (asyncio.gather)

See instruments-service/docs/DATABENTO_ADAPTER_GUIDE.md for implementation details
"""

import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo

import pandas as pd
import databento as db
import exchange_calendars as xcals
from instruments_service.config import instruments_config
from unified_cloud_services import DatabentoBaseClient, DatabentoClientConfig, get_config
from instruments_service.config import UnifiedInstrumentConfig

logger = logging.getLogger(__name__)

# Unified config cache (domain-specific, not network-related)
_UNIFIED_CONFIG_CACHE: Optional[Any] = None

# Exchange calendar cache for holiday detection
# Maps our calendar names to exchange_calendars calendar codes
_EXCHANGE_CALENDAR_MAPPING = {
    "NASDAQ": "XNAS",  # NASDAQ Stock Market
    "NYSE": "XNYS",    # New York Stock Exchange
    "CME": "CMES",     # CME (uses same calendar as NYSE with some variations)
    "CBOE": "XNYS",    # CBOE uses NYSE calendar for equity products
    "ICE": "XNYS",     # ICE US uses NYSE calendar
}
_EXCHANGE_CALENDARS_CACHE: Dict[str, Any] = {}


def clear_databento_cache():
    """Clear module-level cache (useful for testing or credential rotation)"""
    global _UNIFIED_CONFIG_CACHE, _EXCHANGE_CALENDARS_CACHE
    from unified_cloud_services import clear_databento_api_key_cache, clear_databento_client_cache
    clear_databento_api_key_cache()
    clear_databento_client_cache()
    _UNIFIED_CONFIG_CACHE = None
    _EXCHANGE_CALENDARS_CACHE.clear()
    logger.info("🧹 Cleared Databento module-level cache (including exchange calendars)")


class DatabentoAdapter:
    """
    Adapter for fetching TradFi instrument definitions from Databento.

    Uses DatabentoBaseClient for network management (sessions, retries, API keys).
    This adapter focuses on domain-specific logic:
    - Instrument definition fetching
    - Trading hours calculation
    - Holiday detection
    - VIX index generation

    Supports:
    - CME (futures, commodities)
    - NASDAQ (equities)
    - NYSE (equities)
    - CBOE (VIX index)
    - Other TradFi exchanges
    """

    def __init__(self, api_key: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize Databento adapter using centralized DatabentoBaseClient.

        Args:
            api_key: Databento API key (optional, DatabentoBaseClient handles Secret Manager)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Create config with instruments-service specific settings
        config = DatabentoClientConfig(
            secret_name=instruments_config.databento_secret_name,
            fallback_env_var="DATABENTO_API_KEY",
            reuse_client=True,  # Enable module-level client caching
        )

        # Initialize centralized base client
        self._base_client = DatabentoBaseClient(
            config=config,
            api_key=api_key,
            project_id=project_id or instruments_config.gcp_project_id,
        )

        # Initialize session
        self._base_client.initialize_session()

        logger.info("✅ DatabentoAdapter initialized (using DatabentoBaseClient)")

    @property
    def client(self) -> db.Historical:
        """Get the underlying Databento Historical client."""
        return self._base_client.client

    @property
    def api_key(self) -> str:
        """Get the API key."""
        return self._base_client.api_key

    # ============================================================================
    # BATCH API - Re-downloads within 30 days are FREE!
    # ============================================================================

    def _fetch_with_batch_api(
        self,
        dataset: str,
        schema: str,
        symbols: List[str],
        stype_in: str,
        start: str,
        end: str,
    ) -> Any:
        """
        Fetch data using Databento's Batch Jobs API.

        KEY BENEFIT: Re-downloading the same data within 30 days is FREE!
        Unlike Historical Streaming which bills every request.

        Args:
            dataset: Databento dataset ID
            schema: Schema name ('definition', 'trades', etc.)
            symbols: List of symbols
            stype_in: Symbol type input
            start: Start date string
            end: End date string

        Returns:
            DBNStore object with the data
        """
        import tempfile

        logger.info(f"📦 Using BATCH API for {dataset} {schema} ({len(symbols)} symbols) - FREE re-download within 30 days!")

        try:
            # Step 1: Check for existing batch job (free re-download!)
            existing_job = self._find_matching_batch_job(
                dataset=dataset,
                schema=schema,
                symbols=symbols,
                stype_in=stype_in,
                start=start,
                end=end,
            )

            if existing_job:
                job_id = existing_job['id']
                logger.info(f"   ♻️ Found existing batch job {job_id} - FREE re-download!")
            else:
                # Step 2: Submit new batch job
                job = self.client.batch.submit_job(
                    dataset=dataset,
                    schema=schema,
                    symbols=symbols,
                    stype_in=stype_in,
                    stype_out="instrument_id",
                    start=start,
                    end=end,
                    encoding='dbn',
                    compression='zstd',
                    delivery='download',
                )
                job_id = job['id']
                logger.info(f"   📤 Submitted new batch job: {job_id}")

                # Step 3: Wait for job completion
                self._wait_for_batch_job(job_id)

            # Step 4: Download to temp directory
            with tempfile.TemporaryDirectory() as tmp_dir:
                downloaded_files = self.client.batch.download(
                    job_id=job_id,
                    output_dir=tmp_dir,
                )

                if not downloaded_files:
                    logger.warning(f"⚠️ No files downloaded for batch job {job_id}")
                    # Return empty DBNStore-like object
                    return type('EmptyData', (), {'to_df': lambda: pd.DataFrame()})()

                # Find the data file (not metadata files)
                data_file = None
                for f in downloaded_files:
                    if str(f).endswith('.dbn.zst') or str(f).endswith('.dbn'):
                        data_file = f
                        break

                if data_file:
                    # Load and return DBNStore
                    return db.DBNStore.from_file(str(data_file))
                else:
                    logger.warning("⚠️ No .dbn data file found in batch download")
                    return type('EmptyData', (), {'to_df': lambda: pd.DataFrame()})()

        except Exception as e:
            logger.warning(f"⚠️ Batch API failed, falling back to streaming: {e}")
            # Fallback to streaming API
            return self.client.timeseries.get_range(
                dataset=dataset,
                schema=db.Schema.DEFINITION if schema == 'definition' else schema,
                symbols=symbols,
                stype_in=stype_in,
                stype_out="instrument_id",
                start=start,
                end=end,
            )

    def _find_matching_batch_job(
        self,
        dataset: str,
        schema: str,
        symbols: List[str],
        stype_in: str,
        start: str,
        end: str,
    ) -> Optional[Dict[str, Any]]:
        """Find an existing batch job matching the query parameters."""
        try:
            # List recent batch jobs (last 30 days are downloadable)
            jobs = self.client.batch.list_jobs(states=['done'])

            # Match job parameters
            symbols_set = set(symbols)
            for job in jobs:
                if (job.get('dataset') == dataset and
                    job.get('schema') == schema and
                    job.get('stype_in') == stype_in and
                    str(job.get('start', '')).startswith(start) and
                    str(job.get('end', '')).startswith(end)):
                    # Check symbols match
                    job_symbols = job.get('symbols', '')
                    if isinstance(job_symbols, str):
                        job_symbols_set = set(job_symbols.split(','))
                    else:
                        job_symbols_set = set(job_symbols) if job_symbols else set()

                    if symbols_set == job_symbols_set or symbols_set.issubset(job_symbols_set):
                        return job

            return None
        except Exception as e:
            logger.debug(f"No matching batch job found: {e}")
            return None

    def _wait_for_batch_job(self, job_id: str, timeout_minutes: int = 30) -> None:
        """Wait for a batch job to complete."""
        import time

        poll_interval = 5  # seconds
        max_polls = (timeout_minutes * 60) // poll_interval

        for i in range(max_polls):
            jobs = self.client.batch.list_jobs()

            # Find our job
            job = next((j for j in jobs if j['id'] == job_id), None)

            if not job:
                raise RuntimeError(f"Batch job {job_id} not found")

            state = job.get('state', 'unknown')
            progress = job.get('progress', 0)

            if state == 'done':
                logger.info(f"   ✅ Batch job {job_id} completed!")
                return
            elif state in ('expired', 'failed'):
                raise RuntimeError(f"Batch job {job_id} {state}")
            else:
                if i % 6 == 0:  # Log every 30 seconds
                    logger.info(f"   ⏳ Batch job {job_id}: {state} ({progress}%)")
                time.sleep(poll_interval)

        raise TimeoutError(f"Batch job {job_id} timed out after {timeout_minutes} minutes")

    def fetch_instrument_definitions(
        self,
        exchange: str,
        symbols: List[str],
        date: datetime,
        dataset: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch instrument definitions from Databento.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            symbols: List of symbols in Databento format (e.g., ['ES.FUT', 'SPY', 'SPY.OPT'])
            date: Target date for instrument definitions
            dataset: Databento dataset ID (e.g., 'GLBX.MDP3', 'DBEQ.BASIC')
                    If None, uses default mapping based on exchange

        Returns:
            Dictionary mapping symbol to instrument definition data
        """
        # OPTIMIZED: Reuse cached UnifiedInstrumentConfig instance
        global _UNIFIED_CONFIG_CACHE

        if _UNIFIED_CONFIG_CACHE is None:

            _UNIFIED_CONFIG_CACHE = UnifiedInstrumentConfig()
            logger.debug("✅ Cached UnifiedInstrumentConfig instance")

        unified_config = _UNIFIED_CONFIG_CACHE

        # Map exchange to Databento dataset
        if dataset is None:
            dataset = self._get_dataset_for_exchange(exchange)

        # Ensure date is timezone-aware (convert to UTC if needed)
        if date.tzinfo is None:
            target_date = date.replace(tzinfo=timezone.utc)
        else:
            target_date = date.astimezone(timezone.utc)

        # For Databento queries, we need to query the target date specifically
        # Databento DEFINITION schema returns instruments available on the queried date
        # If target date is weekend/holiday, adjust query date but preserve target date for available_from_datetime
        query_date = self._get_query_date_for_databento(target_date)

        # Query for target date specifically (not previous day)
        # This ensures we get instruments available on Nov 11th when querying for Nov 11th
        # Databento requires end_date > start_date, so use next day as end_date
        start_date_str = query_date.strftime("%Y-%m-%d")
        end_date = query_date + timedelta(days=1)
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Group symbols by dataset AND stype_in
        # Databento API requires separate calls for different datasets and stype_in values
        symbols_by_dataset_and_stype = {}
        for symbol in symbols:
            # Get instrument definition to determine dataset
            inst = unified_config.get_instrument(symbol, venue=exchange)

            if inst:
                # Use dataset from instrument definition (may differ from exchange default)
                symbol_dataset = inst.dataset
                stype = inst.stype_in
                key = (symbol_dataset, stype)
                if key not in symbols_by_dataset_and_stype:
                    symbols_by_dataset_and_stype[key] = []
                symbols_by_dataset_and_stype[key].append(symbol)
            else:
                # Fallback: use default dataset and infer stype_in
                symbol_dataset = dataset
                if symbol.endswith(".FUT") or symbol.endswith(".OPT"):
                    stype = "parent"
                else:
                    stype = "raw_symbol"
                key = (symbol_dataset, stype)
                if key not in symbols_by_dataset_and_stype:
                    symbols_by_dataset_and_stype[key] = []
                symbols_by_dataset_and_stype[key].append(symbol)
                logger.warning(
                    f"Symbol {symbol} not found in unified config, using dataset={symbol_dataset}, stype_in={stype}"
                )

        if not symbols_by_dataset_and_stype:
            logger.warning(f"No valid symbols found for {exchange} on {start_date_str}")
            return {}

        # Fetch instruments for each (dataset, stype_in) group
        # Use BATCH API if configured (re-downloads within 30 days are FREE!)
        use_batch_api = get_config("DATABENTO_USE_BATCH_API", "true").lower() == "true"

        all_instruments = {}
        for (
            symbol_dataset,
            stype_in,
        ), symbol_group in symbols_by_dataset_and_stype.items():
            try:
                # Fetch instrument definitions for this dataset/stype_in group
                if use_batch_api:
                    zipped_data = self._fetch_with_batch_api(
                        dataset=symbol_dataset,
                        schema="definition",
                        symbols=symbol_group,
                        stype_in=stype_in,
                        start=start_date_str,
                        end=end_date_str,
                    )
                else:
                    # Legacy streaming API (bills every request)
                    zipped_data = self.client.timeseries.get_range(
                        dataset=symbol_dataset,
                    schema=db.Schema.DEFINITION,
                    symbols=symbol_group,
                    stype_in=stype_in,
                    stype_out="instrument_id",
                    start=start_date_str,
                    end=end_date_str,
                )

                # Convert to DataFrame
                df = zipped_data.to_df()

                if df.empty:
                    # Check if this is a US market holiday for better user feedback
                    is_holiday, holiday_name = self.is_us_market_holiday(date.date() if hasattr(date, 'date') else date)
                    if is_holiday:
                        logger.info(
                            f"📅 No data for {exchange} on {start_date_str} - US market holiday ({holiday_name}). "
                            f"This is expected behavior."
                        )
                    else:
                        logger.warning(
                            f"No instrument definitions found for {exchange} {symbol_group} (stype_in={stype_in}) on {start_date_str}"
                        )
                    continue

                # Filter out non-trading instruments
                # Use security_type for reliable filtering instead of instrument_class
                # CME security_type: "OOF" = Options on Futures, "FUT" = Future, "STK" = Stock, "ETF" = ETF
                # DBEQ security_type: "E" = Equity/ETF, "C" = Common Stock, "O" = Ordinary shares
                # DBEQ may also have empty string "" for some Class B shares (e.g., BRK.B, BF.B)
                # CME weekly options use various instrument_class values: "W", "M", "T", "S", "Q", "E"
                if "security_type" in df.columns:
                    pre_filter_count = len(df)
                    # Keep only tradeable instruments:
                    # CME: Options (OOF), Futures (FUT), Stocks (STK), ETFs (ETF)
                    # DBEQ: Equity/ETF (E), Common Stock (C), Ordinary shares (O), Class B shares ("")
                    # Exclude: Settlement-only and spreads
                    df = df[df["security_type"].isin(["OOF", "FUT", "STK", "ETF", "E", "C", "O", ""])]
                    filter_reason = "non-tradeable instruments (keeping OOF=Options, FUT=Futures, STK=Stocks, ETF=ETFs, E=Equity, C=Common, O=Ordinary)"
                    post_filter_count = len(df)
                    if pre_filter_count != post_filter_count:
                        logger.info(
                            f"📊 Filtered out {pre_filter_count - post_filter_count} {filter_reason} "
                            f"({post_filter_count} remaining) for {exchange} (stype_in={stype_in})"
                        )

                # For futures parent symbology (.FUT), filter by instrument_class to exclude spreads
                # Databento's instrument_class field distinguishes:
                # - "F" = Outright Future (what we want)
                # - "S" = Future Spread (exclude)
                # - "C"/"P" = Call/Put option
                # This is the recommended approach per Databento docs for filtering spreads
                if "instrument_class" in df.columns and any(s.endswith(".FUT") for s in symbol_group if isinstance(s, str)):
                    pre_class_count = len(df)
                    # Keep only outright futures (instrument_class == "F")
                    # Note: For options (.OPT), we keep C (Call) and P (Put)
                    # This filter only applies when querying with .FUT parent symbology
                    df = df[df["instrument_class"].astype(str).str.upper().isin(["F", "C", "P", ""])]
                    post_class_count = len(df)
                    if pre_class_count != post_class_count:
                        logger.info(
                            f"📊 Filtered out {pre_class_count - post_class_count} futures spreads "
                            f"(instrument_class != F) ({post_class_count} remaining) for {exchange}"
                        )

                # Filter out calendar spreads, complex products, and internal IDs
                # Spreads/combos contain special characters: dash (-), colon (:), plus (+), asterisk (*), slash (/), period (.)
                # ICE spreads specifically use period notation: "BRN FQF0024.H0024" (quarter spread)
                # Examples:
                # - Calendar spreads: "ESH6-ESM6" (dash between contracts)
                # - ICE calendar spreads: "BRN FQF0024.H0024", "G   FSN0025.Z0025" (period between legs)
                # - ICE internal IDs: "BRN 142   7377732", "G     3  30451873" (numbered format - not tradeable)
                # - Average price products: "CL:SA 02M F6" (colon separator)
                # - Ratio spreads: "CL*NG" (asterisk for ratio)
                if "raw_symbol" in df.columns:
                    pre_spread_count = len(df)
                    # Exclude symbols with special characters indicating combos/spreads
                    # Pattern: dash (-), colon (:), plus (+), asterisk (*), slash (/), period (.)
                    df = df[
                        ~df["raw_symbol"].astype(str).str.contains(r"[-:+*/.]", regex=True, na=False)
                    ]
                    # Also filter out ICE internal/numbered formats (e.g., "BRN 142   7377732")
                    # These have a number sequence after the product code followed by a long ID
                    df = df[
                        ~df["raw_symbol"].astype(str).str.match(r"^[A-Z]+\s+\d+\s+\d+$", na=False)
                    ]
                    post_spread_count = len(df)
                    if pre_spread_count != post_spread_count:
                        logger.info(
                            f"📊 Filtered out {pre_spread_count - post_spread_count} spreads/combos/internal-IDs "
                            f"({post_spread_count} remaining) for {exchange} (stype_in={stype_in})"
                        )

                # Process and merge into all_instruments
                # Pass target_date to ensure available_from_datetime uses target date
                group_instruments = self._process_databento_dataframe(
                    df, exchange, symbol_dataset, symbol_group, stype_in, target_date=target_date
                )
                all_instruments.update(group_instruments)

            except Exception as e:
                logger.error(
                    f"Failed to fetch Databento instruments for {exchange} (stype_in={stype_in}): {e}"
                )
                continue

        return all_instruments

    def create_vix_instrument_definition(self, target_date: datetime) -> Optional[Dict[str, Any]]:
        """
        Create VIX index instrument definition.

        VIX is not available via Databento, but we create it as a static instrument
        definition using CBOE trading hours (same as CBOE options).
        Data source is Barchart (OHLCV 15-minute data), but we handle it here
        to follow convention and reuse CBOE trading hours logic.

        Args:
            target_date: Target date for trading hours calculation

        Returns:
            VIX instrument definition dictionary or None
        """
        venue = "CBOE"
        instrument_type = "INDEX"
        base_asset = "VIX"
        quote_asset = "USD"

        # Build canonical symbol (VIX is an index, no expiry)
        symbol_canonical = f"{base_asset}-{quote_asset}"

        # Build canonical instrument key
        instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"

        # Get CBOE trading hours (same as CBOE options)
        trading_hours = self._get_exchange_trading_hours(venue, instrument_type, target_date)

        # VIX index is calculated continuously during trading hours
        # Available from: Start of regular trading hours
        # Available to: End of regular trading hours (no expiry for index itself)
        available_from = trading_hours.get("session_start_utc")
        if not available_from:
            # Fallback to target date start (00:00:00 UTC) if trading hours not available
            target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_start.tzinfo is None:
                target_date_start = target_date_start.replace(tzinfo=timezone.utc)
            available_from = target_date_start.isoformat()
        available_to = None  # Index doesn't expire

        return {
            "instrument_key": instrument_key,
            "venue": venue,
            "instrument_type": instrument_type,
            "symbol": symbol_canonical,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "settle_asset": quote_asset,
            "expiry": None,  # Index doesn't expire
            "tick_size": "0.01",  # VIX is quoted to 2 decimal places
            "min_size": "0.01",
            "asset_class": "traditional",
            "venue_type": "exchange",
            "chain": "off-chain",  # TradFi exchanges are off-chain
            "market_category": "TRADFI",  # VIX is TradFi
            "data_provider": "barchart",  # Data source is Barchart, not Databento
            "tardis_exchange": "",
            "tardis_symbol": "",
            "databento_symbol": "",  # Not available via Databento
            "exchange_raw_symbol": "VIX",
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": available_to,  # Index doesn't expire
            "data_types": "ohlcv_15m",  # Barchart provides OHLCV 15-minute data (not 1m like Databento)
            "inverse": False,
            "contract_size": None,  # Index doesn't have contract size
            "underlying": base_asset,  # VIX
            "strike": "",  # Not an option
            "option_type": "",  # Not an option
            # Trading hours metadata (CBOE) - reuse from existing CBOE instruments
            "trading_hours_open": trading_hours.get("open"),
            "trading_hours_close": trading_hours.get("close"),
            "trading_session": trading_hours.get("session"),
            "is_trading_day": trading_hours.get("is_trading_day"),
            "holiday_calendar": trading_hours.get("holiday_calendar"),
        }

    def create_krwusd_instrument_definition(self, target_date: datetime) -> Optional[Dict[str, Any]]:
        """
        Create KRW/USD currency pair instrument definition.

        KRW/USD is not available via Databento, but we create it as a static instrument
        definition using Yahoo Finance as the data provider.
        Yahoo Finance provides daily historical data going back many years (free).

        Args:
            target_date: Target date for availability window (not used for forex, but kept for consistency)

        Returns:
            KRW/USD instrument definition dictionary or None
        """
        venue = "YAHOO_FINANCE"
        instrument_type = "SPOT_PAIR"
        base_asset = "KRW"
        quote_asset = "USD"

        # Build canonical symbol
        symbol_canonical = f"{base_asset}-{quote_asset}"

        # Build canonical instrument key
        instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"

        # Currency pairs trade 24/7 (forex market)
        # Available from: 2020-01-01 (as requested)
        available_from = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
        available_to = None  # Currency pairs don't expire

        return {
            # ============================================================================
            # REQUIRED CORE FIELDS
            # ============================================================================
            "instrument_key": instrument_key,
            "venue": venue,
            "instrument_type": instrument_type,
            "symbol": symbol_canonical,
            "available_from_datetime": available_from,

            # ============================================================================
            # METADATA FIELDS (with defaults)
            # ============================================================================
            "venue_type": "exchange",
            "tardis_exchange": "",
            "data_provider": "yahoo_finance",  # Data source is Yahoo Finance
            "asset_class": "traditional",

            # ============================================================================
            # AVAILABILITY WINDOWS
            # ============================================================================
            "available_to_datetime": available_to,  # Currency pairs don't expire

            # ============================================================================
            # DATA TYPES
            # ============================================================================
            "data_types": "ohlcv_24h",  # Yahoo Finance provides daily OHLCV data (free historical from 2020)

            # ============================================================================
            # ASSET INFORMATION
            # ============================================================================
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "settle_asset": quote_asset,

            # ============================================================================
            # EXCHANGE-SPECIFIC IDENTIFIERS
            # ============================================================================
            "exchange_raw_symbol": "KRWUSD=X",  # Yahoo Finance ticker symbol format
            "databento_symbol": "",  # Not available via Databento
            "tardis_symbol": "",

            # ============================================================================
            # TRADING PARAMETERS
            # ============================================================================
            "inverse": False,
            "tick_size": "",  # Not a trading instrument, irrelevant for data-only
            "min_size": "",  # Not a trading instrument, irrelevant for data-only

            # ============================================================================
            # OPTION-SPECIFIC FIELDS (not applicable for forex)
            # ============================================================================
            "strike": "",
            "option_type": "",

            # ============================================================================
            # CONTRACT-SPECIFIC FIELDS (not applicable for forex spot)
            # ============================================================================
            "expiry": None,  # Not applicable for forex spot
            "contract_size": None,  # Not applicable for forex spot
            "underlying": "",  # Not applicable for forex spot

            # ============================================================================
            # CCXT INTEGRATION FIELDS
            # ============================================================================
            "ccxt_symbol": "",  # Not using CCXT for Yahoo Finance
            "ccxt_exchange": "",

            # ============================================================================
            # DEFI-SPECIFIC FIELDS (not applicable for forex)
            # ============================================================================
            "base_asset_contract_address": None,
            "quote_asset_contract_address": None,
            "pool_address": None,
            "pool_fee_tier": None,

            # ============================================================================
            # LENDING PROTOCOL-SPECIFIC FIELDS (not applicable for forex)
            # ============================================================================
            "flash_loan_providers": None,
            "instadapp_routing": None,
            "ltv": None,
            "liquidation_threshold": None,
            "liquidation_bonus": None,
            "reserve_factor": None,
            "emode_category_id": None,
            "emode_label": None,
            "emode_underlying": None,
            "emode_liquidation_threshold": None,
            "emode_liquidation_bonus": None,
            "optimal_utilization_rate": None,
            "base_variable_borrow_rate": None,
            "variable_rate_slope1": None,
            "variable_rate_slope2": None,

            # ============================================================================
            # CEFI RISK PARAMETERS (not applicable for forex spot)
            # ============================================================================
            "max_position_size": None,
            "max_leverage": None,
            "initial_margin_rate": None,
            "maintenance_margin_rate": None,
            "leverage_tiers_json": None,

            # ============================================================================
            # TRADING HOURS METADATA (forex trades 24/7, Sun 5pm ET - Fri 5pm ET)
            # ============================================================================
            "trading_hours_open": "00:00:00+00:00",  # Forex trades 24/7 (Sun-Fri), use midnight UTC
            "trading_hours_close": "23:59:59+00:00",  # Forex trades 24/7, use end of day UTC
            "trading_session": "24/7",  # Forex market trades continuously
            "is_trading_day": True,  # Forex trades every day (including weekends)
            "holiday_calendar": None,  # No holidays for forex market

            # ============================================================================
            # ADDITIONAL METADATA
            # ============================================================================
            "chain": "off-chain",  # Forex is off-chain (traditional finance)
            "market_category": "TRADFI",  # Forex is TradFi
        }

    def create_bitcoin_etf_instrument_definition(
        self, ticker: str, target_date: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Create Bitcoin ETF instrument definition with full metadata.

        Bitcoin ETFs (IBIT, FBTC, ARKB) are TradFi ETFs that track Bitcoin price.
        They trade on US stock exchanges (NASDAQ, NYSE) with standard equity trading hours.

        Follows the VIX/KRW-USD pattern for complete instrument definitions with
        canonical instrument keys in the format: VENUE:ETF:TICKER-USD

        Args:
            ticker: ETF ticker symbol (e.g., "IBIT", "FBTC", "ARKB")
            target_date: Target date for trading hours calculation

        Returns:
            Bitcoin ETF instrument definition dictionary or None if ticker not recognized
        """
        # ETF venue mapping - Bitcoin ETFs accessible via Databento DBEQ.BASIC
        # All use NASDAQ for consistency (same trading hours as all US equity exchanges)
        etf_venues = {
            "IBIT": "NASDAQ",  # BlackRock iShares Bitcoin Trust
            "FBTC": "NASDAQ",  # Fidelity Wise Origin Bitcoin Fund
            "ARKB": "NASDAQ",  # ARK 21Shares Bitcoin ETF
        }

        venue = etf_venues.get(ticker.upper())
        if not venue:
            logger.warning(f"Unknown Bitcoin ETF ticker: {ticker}")
            return None

        ticker_upper = ticker.upper()
        instrument_type = "ETF"
        base_asset = ticker_upper  # The ETF ticker is the base asset (IBIT, FBTC, ARKB)
        quote_asset = "USD"

        # Build canonical symbol (ETF-USD pattern following VIX-USD convention)
        symbol_canonical = f"{base_asset}-{quote_asset}"

        # Build canonical instrument key: VENUE:ETF:TICKER-USD
        instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"

        # Get NASDAQ/NYSE trading hours (converted to UTC via existing logic)
        trading_hours = self._get_exchange_trading_hours(venue, instrument_type, target_date)

        # Bitcoin ETFs started trading in January 2024
        # Use trading session start for available_from_datetime
        available_from = trading_hours.get("session_start_utc")
        if not available_from:
            # Fallback to target date start (00:00:00 UTC) if trading hours not available
            target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if target_date_start.tzinfo is None:
                target_date_start = target_date_start.replace(tzinfo=timezone.utc)
            available_from = target_date_start.isoformat()

        # ETFs don't expire - use session end for available_to_datetime
        available_to = trading_hours.get("session_end_utc")

        return {
            # ============================================================================
            # REQUIRED CORE FIELDS
            # ============================================================================
            "instrument_key": instrument_key,  # e.g., NASDAQ:ETF:IBIT-USD
            "venue": venue,
            "instrument_type": instrument_type,
            "symbol": symbol_canonical,
            "available_from_datetime": available_from,

            # ============================================================================
            # ASSET INFORMATION
            # ============================================================================
            "base_asset": base_asset,  # ETF ticker (IBIT, FBTC, ARKB)
            "quote_asset": quote_asset,
            "settle_asset": quote_asset,
            "underlying": "BTC",  # Bitcoin is the underlying asset for all Bitcoin ETFs

            # ============================================================================
            # METADATA FIELDS
            # ============================================================================
            "asset_class": "traditional",
            "venue_type": "exchange",
            "chain": "off-chain",  # TradFi exchanges are off-chain
            "market_category": "TRADFI",  # Bitcoin ETFs are TradFi instruments

            # ============================================================================
            # DATA PROVIDER
            # ============================================================================
            "data_provider": "databento",
            "databento_symbol": ticker_upper,
            "exchange_raw_symbol": ticker_upper,
            "tardis_exchange": "",
            "tardis_symbol": "",

            # ============================================================================
            # DATA TYPES
            # ============================================================================
            "data_types": "ohlcv_1m",  # Databento provides OHLCV 1-minute data

            # ============================================================================
            # AVAILABILITY WINDOWS
            # ============================================================================
            "available_to_datetime": available_to,

            # ============================================================================
            # TRADING PARAMETERS
            # ============================================================================
            "tick_size": "0.01",  # ETFs are quoted to 2 decimal places
            "min_size": "1",  # Minimum 1 share
            "inverse": False,

            # ============================================================================
            # CONTRACT-SPECIFIC FIELDS (not applicable for ETFs)
            # ============================================================================
            "expiry": None,  # ETFs don't expire
            "contract_size": None,
            "strike": "",
            "option_type": "",

            # ============================================================================
            # CCXT INTEGRATION FIELDS (not applicable for TradFi ETFs)
            # ============================================================================
            "ccxt_symbol": "",
            "ccxt_exchange": "",

            # ============================================================================
            # DEFI-SPECIFIC FIELDS (not applicable for TradFi ETFs)
            # ============================================================================
            "base_asset_contract_address": None,
            "quote_asset_contract_address": None,
            "pool_address": None,
            "pool_fee_tier": None,

            # ============================================================================
            # LENDING PROTOCOL-SPECIFIC FIELDS (not applicable for ETFs)
            # ============================================================================
            "flash_loan_providers": None,
            "instadapp_routing": None,
            "ltv": None,
            "liquidation_threshold": None,
            "liquidation_bonus": None,
            "reserve_factor": None,
            "emode_category_id": None,
            "emode_label": None,
            "emode_underlying": None,
            "emode_liquidation_threshold": None,
            "emode_liquidation_bonus": None,
            "optimal_utilization_rate": None,
            "base_variable_borrow_rate": None,
            "variable_rate_slope1": None,
            "variable_rate_slope2": None,

            # ============================================================================
            # CEFI RISK PARAMETERS (not applicable for spot ETFs)
            # ============================================================================
            "max_position_size": None,
            "max_leverage": None,
            "initial_margin_rate": None,
            "maintenance_margin_rate": None,
            "leverage_tiers_json": None,

            # ============================================================================
            # TRADING HOURS METADATA (UTC converted)
            # ============================================================================
            "trading_hours_open": trading_hours.get("open"),
            "trading_hours_close": trading_hours.get("close"),
            "trading_session": trading_hours.get("session"),
            "is_trading_day": trading_hours.get("is_trading_day"),
            "holiday_calendar": trading_hours.get("holiday_calendar"),
        }

    def _get_exchange_expiry_time(
        self, exchange: str, instrument_type: str, expiry_date: pd.Timestamp
    ) -> Optional[pd.Timestamp]:
        """
        DEPRECATED: This method is no longer used.

        We no longer apply default expiry times when Databento provides date-only expiry,
        as expiry times vary by contract type and we don't want to guess incorrectly.

        This method is kept for reference but should not be called.

        Args:
            exchange: Exchange name (e.g., 'CME', 'CBOE')
            instrument_type: Instrument type (e.g., 'FUTURE', 'OPTION')
            expiry_date: Expiry date (may be midnight from Databento)

        Returns:
            None (method deprecated)
        """
        # Method deprecated - we no longer apply default expiry times
        # Better to leave expiry blank than guess incorrectly
        return None

    def _get_dataset_for_exchange(self, exchange: str) -> str:
        """
        Map exchange name to Databento dataset ID.

        Args:
            exchange: Exchange name

        Returns:
            Databento dataset ID
        """
        dataset_mapping = {
            "CME": "GLBX.MDP3",  # CME Globex (ES, NQ, CL, NG, GC, etc.)
            "ICE": "IFUS.IMPACT",  # ICE Futures US (Cotton CT, Coffee KC, Sugar SB, Cocoa CC, OJ, DX)
            "ICE-EU": "IFEU.IMPACT",  # ICE Europe (Brent, Gas Oil)
            "CBOE": "BARCHART",  # VIX only (handled separately, not via Databento)
            "NASDAQ": "DBEQ.BASIC",  # NASDAQ equities
            "NYSE": "DBEQ.BASIC",  # NYSE equities
        }

        exchange_upper = exchange.upper()
        return dataset_mapping.get(exchange_upper, "GLBX.MDP3")  # Default to CME

    def _get_query_date_for_databento(self, target_date: datetime) -> datetime:
        """
        Get query date for Databento API based on target date.

        Databento DEFINITION schema returns instruments available on the queried date.
        If target date is weekend, we query the previous Friday, but we preserve
        the target date for available_from_datetime to ensure correct filtering.

        Args:
            target_date: Target date (timezone-aware)

        Returns:
            Query date for Databento API (may differ from target_date if weekend)
        """
        # Ensure date is timezone-aware
        if target_date.tzinfo is None:
            target_date = target_date.replace(tzinfo=timezone.utc)

        # Get date only (without time)
        target_date_only = target_date.date()
        weekday = target_date_only.weekday()

        # If weekend, query previous Friday
        if weekday == 6:  # Sunday
            query_date = target_date - timedelta(days=2)  # Go back to Friday
        elif weekday == 5:  # Saturday
            query_date = target_date - timedelta(days=1)  # Go back to Friday
        else:
            # Weekday - query target date directly
            query_date = target_date

        return query_date

    def _filter_symbols(self, dataset: str, symbols: List[str]) -> List[str]:
        """
        Filter symbols based on dataset requirements.

        Args:
            dataset: Databento dataset ID
            symbols: List of symbols to filter

        Returns:
            Filtered list of symbols
        """
        # For now, return all symbols
        # Can add symbol filtering logic here based on allowed_databento_symbols.csv
        return symbols

    def _process_databento_dataframe(
        self,
        df: pd.DataFrame,
        exchange: str,
        dataset: str,
        query_symbols: List[str],
        stype_in: str,
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process Databento DataFrame into instrument definition format.

        Args:
            df: Databento instrument definitions DataFrame
            exchange: Exchange name
            dataset: Databento dataset ID
            query_symbols: List of query symbols used to fetch this data (e.g., ['ES.FUT', 'SPY'])
            stype_in: The stype_in used for the query (e.g., 'parent', 'raw_symbol')

        Returns:
            Dictionary mapping symbol to instrument definition
        """
        instruments = {}

        # Create SEPARATE mappings for futures and options to avoid collisions
        # When querying both ES.FUT and ES.OPT, we need to know which one to use
        # based on the actual instrument type from Databento
        asset_to_fut_symbol = {}
        asset_to_opt_symbol = {}
        asset_to_raw_symbol = {}  # For equities (raw_symbol stype_in)
        for query_sym in query_symbols:
            # Extract base asset from query symbol
            if query_sym.endswith(".FUT"):
                base_asset = query_sym[:-4]  # Remove '.FUT'
                asset_to_fut_symbol[base_asset] = query_sym
            elif query_sym.endswith(".OPT"):
                base_asset = query_sym[:-4]  # Remove '.OPT'
                asset_to_opt_symbol[base_asset] = query_sym
            else:
                # For raw_symbol (equities), the query symbol IS the asset
                asset_to_raw_symbol[query_sym] = query_sym

        # Group by raw_symbol and aggregate
        # Databento uses 'raw_symbol' in definition schema (v0.13.1+)
        if "raw_symbol" in df.columns:
            df_grouped = df.groupby("raw_symbol").first()
            logger.info(
                f"📊 Processing {len(df_grouped)} unique instruments from Databento response (query: {query_symbols[:5]}...)"
            )
        else:
            # No raw_symbol column, use index
            df_grouped = df
            logger.warning("⚠️ No 'raw_symbol' column found in Databento response")

        for symbol, row in df_grouped.iterrows():
            try:
                # Get the query symbol used for this instrument
                asset = row.get("asset", "")
                asset = "" if pd.isna(asset) else str(asset)

                # Get security_type to determine if this is a future or option
                # CME security_type: "FUT" = Future, "OOF" = Options on Futures
                security_type = row.get("security_type", "")
                security_type = "" if pd.isna(security_type) else str(security_type)

                # Determine databento_symbol (the query symbol we used)
                # CRITICAL: Use separate mappings for futures vs options to avoid collision
                # When querying both ES.FUT and ES.OPT, futures should map to ES.FUT
                # and options should map to ES.OPT
                if stype_in == "parent":
                    # For parent symbology, use security_type to choose correct mapping
                    if security_type == "FUT":
                        # This is a future - use futures mapping
                        databento_symbol = asset_to_fut_symbol.get(
                            asset, query_symbols[0] if query_symbols else ""
                        )
                    elif security_type == "OOF":
                        # This is an option on futures - use options mapping
                        databento_symbol = asset_to_opt_symbol.get(
                        asset, query_symbols[0] if query_symbols else ""
                    )
                    else:
                        # Unknown type - try futures first, then options
                        databento_symbol = asset_to_fut_symbol.get(
                            asset, asset_to_opt_symbol.get(
                                asset, query_symbols[0] if query_symbols else ""
                            )
                        )
                else:
                    # For raw_symbol, the asset IS the query symbol
                    databento_symbol = asset_to_raw_symbol.get(asset, asset)

                # exchange_raw_symbol should be the actual Databento symbol (contract symbol)
                # This is what the exchange uses internally, not the asset/base
                # For futures: "ESZ24" (specific contract), for options: "SPY 251219C500", for equities: "AAPL"
                exchange_raw_symbol = str(symbol) if symbol else asset

                inst_def = self._convert_to_instrument_definition(
                    row,
                    exchange,
                    dataset,
                    databento_symbol,
                    exchange_raw_symbol,
                    target_date=target_date,
                )
                # Skip None returns (e.g., incomplete options missing strike/option_type)
                if inst_def is not None:
                    instruments[symbol] = inst_def
            except Exception as e:
                logger.warning(f"Failed to process symbol {symbol}: {e}")
                continue

        return instruments

    def _resolve_instrument_id_to_raw_symbol(
        self,
        instrument_id: int,
        exchange: str,
        dataset: str,
        target_date: Optional[datetime] = None,
    ) -> Optional[str]:
        """
        Resolve instrument_id to raw_symbol using Databento symbology API.

        Args:
            instrument_id: Databento instrument ID
            exchange: Exchange name
            dataset: Databento dataset ID
            target_date: Target date for symbology resolution

        Returns:
            Raw symbol string (e.g., "ESZ0 C3620") or None if resolution fails
        """
        try:
            # Use Databento's symbology resolution API
            # The API requires start_date and end_date parameters
            # end_date must be AFTER start_date (not equal) - span one day to help with overlaps
            if target_date:
                start_date_str = target_date.strftime("%Y-%m-%d")
                # Add one day to end_date to ensure it's after start_date
                end_date = target_date + timedelta(days=1)
                end_date_str = end_date.strftime("%Y-%m-%d")
            else:
                today = datetime.now(timezone.utc)
                start_date_str = today.strftime("%Y-%m-%d")
                end_date = today + timedelta(days=1)
                end_date_str = end_date.strftime("%Y-%m-%d")

            logger.debug(
                f"Calling symbology.resolve: instrument_id={instrument_id}, "
                f"stype_in=instrument_id, stype_out=raw_symbol, dataset={dataset}, "
                f"start_date={start_date_str}, end_date={end_date_str}"
            )

            # Resolve instrument_id to raw_symbol
            # The API returns a mapping dict: {input_symbol: [output_symbols]}
            resolved = self.client.symbology.resolve(
                symbols=[str(instrument_id)],
                stype_in="instrument_id",
                stype_out="raw_symbol",
                dataset=dataset,
                start_date=start_date_str,
                end_date=end_date_str,
            )

            logger.debug(f"Symbology resolution response type: {type(resolved)}, value: {resolved}")

            # Handle different response formats
            if resolved:
                if isinstance(resolved, dict):
                    # Response is a dict mapping input_symbol to list of output symbols
                    input_key = str(instrument_id)
                    if input_key in resolved:
                        output_symbols = resolved[input_key]
                        # Handle different value types with explicit type checks
                        # Avoid boolean evaluation of pandas Series or other special types
                        if isinstance(output_symbols, list):
                            if len(output_symbols) > 0:
                                return str(output_symbols[0])
                        elif isinstance(output_symbols, str):
                            if len(output_symbols) > 0:  # Check length instead of truthiness
                                return output_symbols
                        elif isinstance(output_symbols, dict):
                            # Databento symbology API returns dict with 'S' key for symbol, 'D0'/'D1' for dates
                            # Example: {'D0': '2023-11-09', 'D1': '2023-11-10', 'S': 'ESZ0 C3620'}
                            if "S" in output_symbols:
                                symbol_value = output_symbols["S"]
                                if isinstance(symbol_value, str) and len(symbol_value) > 0:
                                    return symbol_value
                                elif isinstance(symbol_value, list) and len(symbol_value) > 0:
                                    return str(symbol_value[0])
                            # Fallback: try to get the first value that looks like a symbol (not a date)
                            if len(output_symbols) > 0:
                                for key, value in output_symbols.items():
                                    # Skip date keys (D0, D1, etc.)
                                    if key.startswith("D") and key[1:].isdigit():
                                        continue
                                    # Return the first non-date value
                                    if isinstance(value, str) and len(value) > 0:
                                        return value
                                    elif isinstance(value, list) and len(value) > 0:
                                        return str(value[0])
                        else:
                            # Try to convert other types to string (avoid boolean evaluation)
                            try:
                                # Check if it's a pandas Series or similar
                                if hasattr(output_symbols, "__len__"):
                                    if len(output_symbols) > 0:
                                        # Try to get first element
                                        try:
                                            first_elem = (
                                                output_symbols.iloc[0]
                                                if hasattr(output_symbols, "iloc")
                                                else output_symbols[0]
                                            )
                                            return str(first_elem)
                                        except (KeyError, IndexError, TypeError):
                                            pass
                                # Fallback: try direct string conversion
                                result = str(output_symbols)
                                if result and result != "None" and result != "nan":
                                    return result
                            except Exception as e:
                                logger.debug(f"Failed to convert output_symbols to string: {e}")
                                pass
                    # Try iterating through dict values
                    for key, value in resolved.items():
                        if isinstance(value, list):
                            if len(value) > 0:
                                return str(value[0])
                        elif isinstance(value, str):
                            if value:  # Non-empty string
                                return value
                        elif isinstance(value, dict):
                            # Databento symbology API returns dict with 'S' key for symbol, 'D0'/'D1' for dates
                            # Example: {'D0': '2023-11-09', 'D1': '2023-11-10', 'S': 'ESZ0 C3620'}
                            if "S" in value:
                                symbol_value = value["S"]
                                if isinstance(symbol_value, str) and len(symbol_value) > 0:
                                    return symbol_value
                                elif isinstance(symbol_value, list) and len(symbol_value) > 0:
                                    return str(symbol_value[0])
                            # Fallback: try to get the first value that looks like a symbol (not a date)
                            if len(value) > 0:
                                for k, v in value.items():
                                    # Skip date keys (D0, D1, etc.)
                                    if k.startswith("D") and k[1:].isdigit():
                                        continue
                                    # Return the first non-date value
                                    if isinstance(v, str) and len(v) > 0:
                                        return v
                                    elif isinstance(v, list) and len(v) > 0:
                                        return str(v[0])
                        else:
                            # Try to convert other types to string
                            try:
                                result = str(value)
                                if result and result != "None":
                                    return result
                            except Exception:
                                pass
                elif isinstance(resolved, list):
                    # Response is a list of symbols
                    if len(resolved) > 0:
                        return str(resolved[0])
                elif isinstance(resolved, str):
                    # Response is a single string
                    return resolved
                else:
                    # Response is a single value (try to convert to string)
                    return str(resolved)

            logger.warning(
                f"Symbology resolution returned empty result for instrument_id {instrument_id}"
            )
        except Exception as e:
            logger.warning(
                f"Symbology resolution failed for instrument_id {instrument_id}: {e}", exc_info=True
            )

        return None

    def _convert_to_instrument_definition(
        self,
        row: pd.Series,
        exchange: str,
        dataset: str,
        databento_symbol: str,
        exchange_raw_symbol: str = "",
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Convert Databento row to instrument definition format.
        Uses human-readable names from unified config for base_asset, quote_asset, and symbol.

        Args:
            row: Databento DataFrame row
            exchange: Exchange name
            dataset: Databento dataset ID
            databento_symbol: The Databento query symbol used to fetch this instrument (e.g., 'ES.FUT', 'SPY')

        Returns:
            Instrument definition dictionary
        """
        # OPTIMIZED: Reuse cached UnifiedInstrumentConfig instance
        global _UNIFIED_CONFIG_CACHE

        if _UNIFIED_CONFIG_CACHE is None:
            _UNIFIED_CONFIG_CACHE = UnifiedInstrumentConfig()

        unified_config = _UNIFIED_CONFIG_CACHE

        # Extract fields from Databento schema (handle NaNs)
        asset_raw = row.get("asset", "")
        asset_raw = "" if pd.isna(asset_raw) else str(asset_raw)

        # For equities/ETFs, if asset is empty, use the symbol (ticker) from Databento
        # The symbol field is the actual ticker (AAPL, SPY, etc.) for equities
        if not asset_raw and exchange_raw_symbol:
            # Use exchange_raw_symbol (which is the Databento symbol) as fallback for equities
            asset_raw = exchange_raw_symbol

        currency_raw = row.get("currency", "USD")
        currency_raw = "USD" if pd.isna(currency_raw) else str(currency_raw)

        security_type = row.get("security_type", "")
        security_type = "" if pd.isna(security_type) else str(security_type)

        min_price_increment = row.get("min_price_increment", 0.01)
        min_price_increment = 0.01 if pd.isna(min_price_increment) else float(min_price_increment)

        # Determine instrument type FIRST (needed for underlying extraction and quote_asset logic)
        # CRITICAL: Check databento_symbol first (e.g., "ES.OPT", "SPY.OPT") as Databento may not
        # always set security_type correctly for options, especially CME options
        if databento_symbol.endswith(".OPT"):
            instrument_type = "OPTION"
        elif databento_symbol.endswith(".FUT"):
            instrument_type = "FUTURE"
        elif security_type == "FUT":
            instrument_type = "FUTURE"
        elif security_type == "OPT":
            instrument_type = "OPTION"
        elif security_type == "ETF":
            instrument_type = "ETF"
        elif security_type == "STK":
            instrument_type = "EQUITY"
        else:
            instrument_type = "EQUITY"  # Default

        # Override: Known ETFs that Databento returns as STK (Stock)
        # SPY, QQQ, IVV, etc. are ETFs but security_type="STK" from Databento
        KNOWN_ETFS = {
            'SPY', 'QQQ', 'IVV', 'VOO', 'VTI', 'DIA', 'IWM', 'EEM', 'VEA', 'VWO',
            'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'IEF', 'SHY', 'LQD', 'HYG', 'JNK',
            'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLU', 'XLRE',
            'VNQ', 'IBB', 'SMH', 'IBIT', 'FBTC', 'ARKB', 'GBTC', 'BITO'
        }
        # Check if symbol (without -USD suffix) is a known ETF
        symbol_clean = asset_raw.replace('-USD', '').upper() if asset_raw else ''
        if not symbol_clean and exchange_raw_symbol:
            symbol_clean = exchange_raw_symbol.replace('-USD', '').upper()
        if symbol_clean in KNOWN_ETFS:
            instrument_type = "ETF"
            logger.debug(f"Overriding {symbol_clean} from STK to ETF (known ETF)")

        # exchange_raw_symbol is the actual Databento symbol (contract symbol) passed in
        # This is what the exchange uses internally:
        # - For futures: "ESZ24" (specific contract), "CLZ24", etc.
        # - For options: "SPY 251219C500" (specific option contract)
        # - For equities: "AAPL", "SPY" (ticker symbol)
        # If not provided, fall back to asset (base symbol)
        if not exchange_raw_symbol:
            exchange_raw_symbol = asset_raw

        # For equities/ETFs, if asset is still empty, use exchange_raw_symbol (the Databento symbol)
        # This handles cases where Databento doesn't populate the asset field for equities
        if not asset_raw and exchange_raw_symbol and security_type in ["STK", "ETF"]:
            asset_raw = exchange_raw_symbol

        # For options, extract underlying from asset field
        # Options asset field should contain just the underlying (e.g., "SPY" for SPY options)
        # But sometimes Databento returns the full OCC symbol in the asset field
        # OCC format: SPY   230523C00480000 (21 chars: 6-char padded underlying + YYMMDD + C/P + 8-digit strike)
        underlying_asset = asset_raw

        # For options, check if asset_raw looks like an OCC symbol (contains digits and C/P)
        # If so, parse it. Otherwise, if asset is empty, parse from exchange_raw_symbol
        if instrument_type == "OPTION":
            # Check if asset_raw looks like OCC format (has digits and C/P character)
            if underlying_asset and re.search(
                r"\d{6}[CP]\d{8}", str(underlying_asset).strip().upper()
            ):
                # asset_raw contains full OCC symbol, parse underlying from it
                symbol_str = str(underlying_asset).strip().upper()
                match = re.match(r"^([A-Z]+)\s*", symbol_str)
                if match:
                    underlying_asset = match.group(1).strip()
            elif not underlying_asset and exchange_raw_symbol:
                # asset_raw is empty, parse from exchange_raw_symbol
                symbol_str = str(exchange_raw_symbol).strip().upper()
                match = re.match(r"^([A-Z]+)\s*", symbol_str)
                if match:
                    underlying_asset = match.group(1).strip()

        # Convert to human-readable names using unified config
        # For equities/ETFs, asset is already human-readable (AAPL, SPY, etc.), only convert futures codes
        # CME security_type: "STK" = Stock, "ETF" = ETF, "FUT" = Future, "OOF" = Options on Futures
        # DBEQ security_type: "E" = Equity/ETF, "C" = Common Stock, "O" = Ordinary shares, "" = Class B shares
        if security_type in ["STK", "ETF", "E", "C", "O", ""] or (
            not security_type and underlying_asset and len(underlying_asset) <= 5
        ):
            # Equities/ETFs are already human-readable, don't convert
            # Includes DBEQ types: E (Equity), C (Common Stock), O (Ordinary), "" (Class B like BRK.B)
            base_asset = underlying_asset if underlying_asset else exchange_raw_symbol
        elif instrument_type == "OPTION":
            # Options: use underlying asset (already human-readable like SPY)
            base_asset = underlying_asset if underlying_asset else ""
        else:
            # Futures only: convert exchange codes to human-readable names
            # This applies to CME futures (FUT, OOF) where codes like "ES" should become "SP500"
            base_asset = (
                unified_config.get_human_readable_name(underlying_asset) if underlying_asset else ""
            )

        # For TradFi (equities, options, futures), quote currency is always USD
        # Per INSTRUMENT_KEY.md: stocks/equities use USD as quote currency
        if security_type in ["STK", "ETF", "OPT", "FUT"] or instrument_type in [
            "EQUITY",
            "ETF",
            "OPTION",
            "FUTURE",
        ]:
            quote_asset = "USD"
        else:
            quote_asset = currency_raw  # Currency codes are already human-readable (USD, EUR, etc.)

        # Parse expiry if available
        # Databento's expiration field is a uint64 nanosecond timestamp, not a Python datetime
        expiry_time = None
        expiry_str = ""
        expiry_dt = None

        # Debug: Log all relevant expiry fields for ICE futures
        if exchange.upper() == "ICE" and instrument_type == "FUTURE":
            logger.debug(
                f"🔍 ICE Future debug - raw_symbol: {exchange_raw_symbol}, "
                f"expiration field: {row.get('expiration', 'NOT_FOUND')}, "
                f"asset: {row.get('asset', 'NOT_FOUND')}, "
                f"instrument_class: {row.get('instrument_class', 'NOT_FOUND')}"
            )

        if "expiration" in row and pd.notna(row["expiration"]):
            expiry_time = row["expiration"]
            # Format expiry as YYMMDD
            try:
                # Always use pd.to_datetime with unit="ns" to handle nanosecond integers
                expiry_dt = pd.to_datetime(expiry_time, unit="ns", utc=True)

                # Validate: check for epoch timestamp (1970-01-01) which indicates missing/invalid expiry
                # Also check for dates before 1980 as a sanity check
                if expiry_dt.year < 1980:
                    logger.debug(
                        f"Skipping invalid expiry {expiry_time} -> {expiry_dt} for {exchange_raw_symbol} "
                        f"(epoch or pre-1980 date)"
                    )
                    expiry_dt = None
                    expiry_str = ""
                else:
                    expiry_str = expiry_dt.strftime("%y%m%d")
                    logger.debug(
                        f"✅ Parsed expiry from Databento field for {exchange_raw_symbol}: {expiry_time} -> {expiry_str}"
                    )
            except Exception as e:
                logger.warning(f"Failed to parse expiry {expiry_time}: {e}")
                expiry_dt = None
                expiry_str = ""
        elif exchange.upper() == "ICE" and instrument_type == "FUTURE":
            logger.debug(
                f"⚠️ ICE Future {exchange_raw_symbol} has no expiration field or it's NaN"
            )

        # If expiry is still missing for ICE/CME futures, try parsing from raw_symbol
        # ICE Europe format: BRNM25 (product + month code + 2-digit year)
        # CME format: ESM25 (product + month code + 2-digit year)
        # Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
        if not expiry_dt and exchange_raw_symbol and instrument_type == "FUTURE":
            import calendar
            month_codes = {
                'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
            }

            raw_upper = exchange_raw_symbol.upper().strip()

            # Pattern 1: Standard format [A-Z]+[FGHJKMNQUVXZ][0-9]{1,2} (e.g., BRNM25, ESZ4, NGF26)
            match = re.match(r'^([A-Z]+)([FGHJKMNQUVXZ])(\d{1,2})$', raw_upper)

            # Pattern 2: Space-separated format (e.g., "BRN M25", "ES Z4")
            if not match:
                match = re.match(r'^([A-Z]+)\s+([FGHJKMNQUVXZ])(\d{1,2})$', raw_upper)

            # Pattern 3: ICE continuous contract format (just product code, no expiry)
            # For continuous contracts, skip expiry - they don't have one
            if not match and re.match(r'^[A-Z]{1,4}$', raw_upper):
                logger.debug(
                    f"⏭️ Skipping expiry parsing for continuous contract: {exchange_raw_symbol}"
                )

            if match:
                product_code = match.group(1)  # e.g., "BRN", "ES", "NG"
                month_code = match.group(2)    # e.g., "M", "Z"
                year_digits = match.group(3)   # e.g., "25", "4"

                month = month_codes.get(month_code)
                if month:
                    # Handle 1 or 2 digit year
                    if len(year_digits) == 1:
                        # Single digit means 202X (e.g., "4" = 2024)
                        year = 2020 + int(year_digits)
                    else:
                        # Two digits: assume 20XX for now (e.g., "25" = 2025)
                        year = 2000 + int(year_digits)

                    # For futures, expiry is typically last trading day of the month
                    # We'll use the last day of the expiry month as approximation
                    # The exact expiry time will be set later based on exchange rules
                    last_day = calendar.monthrange(year, month)[1]

                    try:
                        expiry_dt = datetime(year, month, last_day, 0, 0, 0, tzinfo=timezone.utc)
                        expiry_str = expiry_dt.strftime("%y%m%d")
                        logger.debug(
                            f"✅ Parsed expiry from raw_symbol {exchange_raw_symbol}: "
                            f"{product_code} {month_code}{year_digits} -> {expiry_str}"
                        )
                    except ValueError as e:
                        logger.debug(f"Failed to construct date from raw_symbol {exchange_raw_symbol}: {e}")
            elif not re.match(r'^[A-Z]{1,4}$', raw_upper):
                # Log a warning if we couldn't parse and it's not a known continuous contract format
                logger.warning(
                    f"⚠️ Could not parse expiry from raw_symbol '{exchange_raw_symbol}' for {instrument_type} "
                    f"(expected format like BRNM25, ESZ4, or 'BRN M25') - exchange={exchange}, dataset={dataset}"
                )

        # Final debug logging if expiry is still missing for futures
        if instrument_type == "FUTURE" and not expiry_dt:
            logger.debug(
                f"📋 ICE/CME Future expiry debug: raw_symbol='{exchange_raw_symbol}', "
                f"expiration_field={row.get('expiration', 'N/A')}, "
                f"asset={row.get('asset', 'N/A')}, exchange={exchange}, dataset={dataset}"
            )

        # Extract option-specific fields
        strike_price = ""
        option_type = ""
        if instrument_type == "OPTION":
            # CRITICAL FIX: Use Databento's direct fields for options (including CME weeklies)
            # CME weekly options have instrument_class like "W", "M", "T", "S", "Q", "E"
            # Instead, use 'right' field (Call/Put) and 'strike_price' field directly

            # Extract strike price from Databento's strike_price field (always present for options)
            if "strike_price" in row and pd.notna(row["strike_price"]):
                strike_price_val = row["strike_price"]
                if isinstance(strike_price_val, (int, float)):
                    strike_price = str(strike_price_val)
                else:
                    strike_price = str(strike_price_val)

            # Extract option type from Databento's 'right' field (Call/Put indicator)
            # 'right' field: "C" = Call, "P" = Put (reliable for all option types including weeklies)
            if "right" in row and pd.notna(row["right"]):
                right_val = str(row["right"]).upper().strip()
                if right_val == "C":
                    option_type = "CALL"
                elif right_val == "P":
                    option_type = "PUT"

            # Fallback: Try instrument_class (only for backwards compatibility)
            if not option_type and "instrument_class" in row and pd.notna(row["instrument_class"]):
                instr_class = str(row["instrument_class"]).upper().strip()
                if instr_class == "C":
                    option_type = "CALL"
                elif instr_class == "P":
                    option_type = "PUT"

            # Log what we have so far for debugging (especially for CME)
            if exchange.upper() == "CME":
                available_fields = [k for k in row.index if pd.notna(row.get(k))]
                logger.debug(
                    f"CME option parsing - exchange_raw_symbol={exchange_raw_symbol}, "
                    f"strike_price={strike_price}, option_type={option_type}, "
                    f"raw_symbol={row.get('raw_symbol', 'N/A')}, "
                    f"instrument_class={row.get('instrument_class', 'N/A')}, "
                    f"available_fields={available_fields[:10]}..."  # Show first 10 fields
                )

            # OPTIMIZATION: For CME options, we already have strike_price and option_type from direct fields
            # Skip symbol parsing for CME to avoid unnecessary complexity for weekly options
            if exchange.upper() == "CME" and strike_price and option_type:
                logger.debug(
                    f"✅ CME option: Using direct fields - strike={strike_price}, type={option_type}, "
                    f"instrument_class={row.get('instrument_class', 'N/A')}"
                )
            # Parse option format from raw_symbol if strike/option_type still not found (fallback for other exchanges)
            # Different exchanges use different formats:
            # - CBOE (OCC): SPY   230523C00480000 (21 chars: 6-char padded underlying + YYMMDD + C/P + 8-digit strike)
            # - CME: ESZ0 C3620 (futures contract + space + C/P + strike)
            # NOTE: When using parent symbology, Databento returns internal format like "UD:1V: VT 2531409"
            # We need to parse from exchange_raw_symbol or use instrument_id to resolve to raw_symbol
            elif not strike_price or not option_type:
                # Try to get raw_symbol from row first
                databento_symbol_raw = row.get("raw_symbol", "")
                if pd.isna(databento_symbol_raw) or not databento_symbol_raw:
                    # If raw_symbol not available, try exchange_raw_symbol
                    databento_symbol_raw = exchange_raw_symbol

                if pd.notna(databento_symbol_raw) and databento_symbol_raw:
                    symbol_str = str(databento_symbol_raw).strip().upper()

                    # Check if symbol_str is Databento internal format (e.g., "UD:1V: 12 2511947")
                    # This format is not parseable and should be skipped
                    if symbol_str.startswith("UD:") or (
                        ":" in symbol_str[:5] and "UD" in symbol_str[:5]
                    ):
                        logger.debug(
                            f"Skipping parsing for Databento internal format: {symbol_str} "
                            f"(exchange_raw_symbol={exchange_raw_symbol})"
                        )
                        # Skip parsing for internal formats - they're not useful
                        symbol_str = None

                    # CME format parsing: ESZ0 C3620 or ESZ0 P3620
                    # Format: [FUTURES_CONTRACT] [C/P][STRIKE]
                    # Example: ESZ0 C3620 (ES December Call strike 3620)
                    # Example: ESZ0 P3620 (ES December Put strike 3620)
                    if symbol_str and exchange.upper() == "CME":
                        # First try standard CME format: futures contract code + space + C/P + strike
                        # Pattern: [A-Z0-9]+ [CP]\d+
                        cme_match = re.search(r"([A-Z0-9]+)\s+([CP])(\d+)", symbol_str)
                        if cme_match:
                            cme_match.group(1)  # e.g., "ESZ0"
                            opt_char = cme_match.group(2)  # C or P
                            strike_str = cme_match.group(3)  # Strike price digits

                            # Parse strike price (CME strikes are typically integers)
                            if not strike_price:
                                strike_price = strike_str

                            # Parse option type
                            if not option_type:
                                option_type = "CALL" if opt_char == "C" else "PUT"

                            logger.debug(
                                f"Parsed CME option from raw_symbol: {symbol_str} -> strike={strike_price}, type={option_type}"
                            )
                        elif instrument_type == "OPTION" and (not strike_price or not option_type):
                            # CRITICAL: Only call symbology API for OPTIONS missing strike/option_type
                            # Check instrument_class from Databento row to avoid calling API for futures
                            # instrument_class: "C" = Call, "P" = Put, "F" = Future, "T" = other
                            instr_class = str(row.get("instrument_class", "")).upper().strip()
                            if instr_class == "F":
                                # This is a future contract, not an option - skip symbology resolution
                                logger.debug(
                                    f"Skipping symbology resolution for future contract: {symbol_str} "
                                    f"(instrument_class=F)"
                                )
                            else:
                                # Try Databento internal format: "UD:1V: VT 2531409" or similar
                                # When using parent symbology, Databento returns internal symbols
                                # We need to use instrument_id to resolve to raw_symbol via symbology API
                                instrument_id = row.get("instrument_id")
                                logger.debug(
                                    f"CME option - instrument_id check: value={instrument_id}, "
                                    f"type={type(instrument_id)}, pd.notna={pd.notna(instrument_id) if instrument_id is not None else 'N/A'}"
                                )
                                if instrument_id is not None and pd.notna(instrument_id):
                                    try:
                                        logger.debug(
                                            f"Attempting symbology resolution for instrument_id={instrument_id}, "
                                            f"exchange={exchange}, dataset={dataset}"
                                        )
                                        # Use Databento symbology resolution to get raw_symbol
                                        # This will give us the actual CME symbol like "ESZ0 C3620"
                                        raw_symbol_resolved = (
                                            self._resolve_instrument_id_to_raw_symbol(
                                                instrument_id, exchange, dataset, target_date
                                            )
                                        )
                                        if raw_symbol_resolved:
                                            # Parse the resolved raw_symbol
                                            resolved_symbol_str = (
                                                str(raw_symbol_resolved).strip().upper()
                                            )
                                            logger.info(
                                                f"✅ Resolved instrument_id {instrument_id} to raw_symbol: {resolved_symbol_str}"
                                            )

                                            # Handle case where symbology returns dict format
                                            if (
                                                isinstance(raw_symbol_resolved, dict)
                                                and "S" in raw_symbol_resolved
                                            ):
                                                resolved_symbol_str = (
                                                    str(raw_symbol_resolved["S"]).strip().upper()
                                                )
                                                logger.debug(
                                                    f"Extracted symbol from dict 'S' key: {resolved_symbol_str}"
                                                )

                                            # Check if resolved symbol is Databento internal format (e.g., "UD:1V: 12 2502245")
                                            # This format is not parseable and indicates the symbology API couldn't resolve to actual exchange symbol
                                            if (
                                                resolved_symbol_str.startswith("UD:")
                                                or ":" in resolved_symbol_str[:5]
                                            ):
                                                logger.debug(
                                                    f"Skipping symbology resolution result - Databento internal format detected: {resolved_symbol_str}"
                                                )
                                                # Don't try to parse internal format - it's not useful
                                                resolved_symbol_str = None

                                            # Try CME format parsing on resolved symbol (only if not internal format)
                                            if resolved_symbol_str:
                                                # CME format: "ESZ0 C3620" (futures contract + space + C/P + strike)
                                                resolved_cme_match = re.search(
                                                    r"([A-Z0-9]+)\s+([CP])(\d+)",
                                                    resolved_symbol_str,
                                                )
                                                if resolved_cme_match:
                                                    opt_char = resolved_cme_match.group(2)  # C or P
                                                    strike_str = resolved_cme_match.group(
                                                        3
                                                    )  # Strike price digits
                                                    if not strike_price:
                                                        strike_price = strike_str
                                                    if not option_type:
                                                        option_type = (
                                                            "CALL" if opt_char == "C" else "PUT"
                                                        )
                                                    logger.info(
                                                        f"✅ Parsed CME option from resolved symbol: "
                                                        f"{resolved_symbol_str} -> strike={strike_price}, type={option_type}"
                                                    )
                                                else:
                                                    # The resolved symbol doesn't match CME format
                                                    # Try to extract any C/P + digits pattern as fallback
                                                    logger.debug(
                                                        f"⚠️ Resolved raw_symbol {resolved_symbol_str} doesn't match CME format pattern, "
                                                        f"trying fallback pattern matching"
                                                    )
                                                    # Fallback: look for C/P followed by digits anywhere
                                                    fallback_match = re.search(
                                                        r"([CP])(\d+)", resolved_symbol_str
                                                    )
                                                    if fallback_match:
                                                        opt_char = fallback_match.group(1)
                                                        strike_str = fallback_match.group(2)
                                                        if not strike_price:
                                                            strike_price = strike_str
                                                        if not option_type:
                                                            option_type = (
                                                                "CALL" if opt_char == "C" else "PUT"
                                                            )
                                                        logger.info(
                                                            f"✅ Parsed CME option (fallback): "
                                                            f"{resolved_symbol_str} -> strike={strike_price}, type={option_type}"
                                                        )
                                                    else:
                                                        logger.debug(
                                                            f"⚠️ Could not parse CME option from resolved symbol: {resolved_symbol_str} "
                                                            f"(likely Databento internal format)"
                                                        )
                                        else:
                                            logger.warning(
                                                f"⚠️ Symbology resolution returned None for instrument_id {instrument_id}"
                                            )
                                    except Exception as e:
                                        logger.warning(
                                            f"❌ Failed to resolve instrument_id {instrument_id} to raw_symbol: {e}",
                                            exc_info=True,
                                        )
                                else:
                                    logger.debug(
                                        f"No instrument_id available for symbology resolution. "
                                        f"Available fields: {list(row.index)}"
                                    )

                            # Fallback: Try to extract number from Databento internal format
                            # Pattern: UD:1V: VT <number> or similar
                            databento_internal_match = re.search(r":\s*VT\s+(\d+)", symbol_str)
                            if databento_internal_match and not strike_price:
                                # The number might be strike price encoded (but likely not directly usable)
                                encoded_strike = databento_internal_match.group(1)
                                # For ES options, strike prices are typically 4-5 digits (e.g., 3620, 4400)
                                # The encoded number (2531409) is likely NOT the strike price directly
                                # But we'll log it for debugging
                                logger.debug(
                                    f"Found number in Databento internal format: {encoded_strike} "
                                    f"(likely not direct strike price for ES options)"
                                )

                            # Only try fallback parsing for options, not futures
                            # Also skip if symbol_str is None (Databento internal format detected)
                            if symbol_str and instrument_type == "OPTION":
                                # Try to find C/P followed by digits (without space) anywhere in the string
                                cme_fallback = re.search(r"([CP])(\d+)", symbol_str)
                                if cme_fallback:
                                    opt_char = cme_fallback.group(1)
                                    strike_str = cme_fallback.group(2)
                                    if not strike_price:
                                        strike_price = strike_str
                                    if not option_type:
                                        option_type = "CALL" if opt_char == "C" else "PUT"
                                    logger.debug(
                                        f"Parsed CME option (fallback): {symbol_str} -> strike={strike_price}, type={option_type}"
                                    )
                                elif not strike_price or not option_type:
                                    # Last resort: warn only if still missing strike/option_type for actual options
                                    # Double-check instrument_class to avoid false warnings for futures
                                    instr_class = (
                                        str(row.get("instrument_class", "")).upper().strip()
                                    )
                                    if instr_class != "F":  # Only warn if not a Future
                                        logger.warning(
                                            f"Could not parse CME option format from symbol: {symbol_str} "
                                            f"(exchange_raw_symbol={exchange_raw_symbol}). "
                                            f"Strike and option_type will be missing. "
                                            f"Consider querying with stype_in='raw_symbol' instead of 'parent' for CME options."
                                        )
                                    else:
                                        logger.debug(
                                            f"Skipping option parsing warning for future contract: {symbol_str} "
                                            f"(instrument_class=F)"
                                        )
                            elif not symbol_str:
                                # symbol_str is None because it's Databento internal format - skip silently
                                logger.debug(
                                    f"Skipping option parsing - Databento internal format detected "
                                    f"(exchange_raw_symbol={exchange_raw_symbol})"
                                )
                            else:
                                # This is not an option (e.g., futures contract), so missing strike/option_type is expected
                                logger.debug(
                                    f"Skipping strike/option_type parsing for non-option instrument: {symbol_str} "
                                    f"(instrument_type={instrument_type})"
                                )
                    # Only try CBOE OCC format if not CME and still missing strike/option_type
                    elif exchange.upper() != "CME" and (not strike_price or not option_type):
                        # CBOE OCC format parsing: extract expiry, option type, and strike
                        # Pattern: [UNDERLYING][YYMMDD][C/P][8_DIGIT_STRIKE]
                        # Example: SPY   230523C00480000
                        # - Underlying: SPY (first part, space-padded to 6 chars)
                        # - Expiry: 230523 (YYMMDD, 6 digits)
                        # - Option type: C (1 char)
                        # - Strike: 00480000 (8 digits, represents 480.000)
                        # Try OCC format: find YYMMDD pattern followed by C/P followed by 8 digits
                        occ_match = re.search(r"(\d{6})([CP])(\d{8})$", symbol_str)
                        if occ_match:
                            expiry_occ = occ_match.group(1)  # YYMMDD
                            opt_char = occ_match.group(2)  # C or P
                            strike_occ = occ_match.group(3)  # 8-digit strike

                            # Parse strike: 8 digits with 3 decimal places (e.g., 00480000 = 480.000)
                            # Only parse if strike_price not already extracted from Databento response
                            if not strike_price:
                                strike_int = int(strike_occ)
                                strike_decimal = strike_int / 1000.0  # 3 decimal places
                                strike_price = (
                                    str(int(strike_decimal))
                                    if strike_decimal.is_integer()
                                    else str(strike_decimal)
                                )

                            # Parse option type (always parse from OCC if not set)
                            if not option_type:
                                option_type = "CALL" if opt_char == "C" else "PUT"

                            # If expiry_str not set yet, use OCC expiry
                            if not expiry_str:
                                expiry_str = expiry_occ
                        else:
                            # Fallback: try to find C/P followed by digits (generic pattern)
                            match = re.search(r"([CP])(\d+)", symbol_str)
                            if match:
                                opt_char = match.group(1)
                                strike_digits = match.group(2)
                                if not option_type:
                                    option_type = "CALL" if opt_char == "C" else "PUT"
                                # Try to parse strike from digits
                                if not strike_price and len(strike_digits) >= 6:
                                    # Assume 8-digit format with 3 decimals
                                    if len(strike_digits) == 8:
                                        strike_int = int(strike_digits)
                                        strike_decimal = strike_int / 1000.0
                                        strike_price = (
                                            str(int(strike_decimal))
                                            if strike_decimal.is_integer()
                                            else str(strike_decimal)
                                        )
                                    else:
                                        # For other exchanges, strikes might be shorter
                                        strike_price = strike_digits

            # If still not found, check for explicit fields
            if not option_type:
                if "option_type" in row and pd.notna(row["option_type"]):
                    opt_type_raw = str(row["option_type"]).upper()
                    if "C" in opt_type_raw or "CALL" in opt_type_raw:
                        option_type = "CALL"
                    elif "P" in opt_type_raw or "PUT" in opt_type_raw:
                        option_type = "PUT"

        # Build symbol with human-readable base_asset
        # Per INSTRUMENT_KEY.md canonical format
        symbol = f"{base_asset}-{quote_asset}"
        if instrument_type == "OPTION":
            # Build canonical option symbol: BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE@LIN
            # TradFi options are always linear (@LIN) - they settle in USD (quote asset)
            # CRITICAL: Options MUST have strike and option_type - skip creating incomplete options
            if not strike_price or not option_type:
                logger.debug(
                    f"Skipping incomplete option: {exchange_raw_symbol} - missing strike={strike_price} or option_type={option_type}"
                )
                return None  # Skip creating incomplete option instruments

            # Clean strike price: remove trailing .0 but preserve integer zeros (e.g., "440.0" -> "440", "480" -> "480")
            strike_clean = ""
            if strike_price:
                # Remove .0 suffix if present, but don't strip trailing zeros from integers
                strike_clean = (
                    strike_price.replace(".0", "").rstrip(".")
                    if "." in strike_price
                    else strike_price
                )

            if strike_clean and option_type and expiry_str:
                symbol = f"{base_asset}-{quote_asset}-{expiry_str}-{strike_clean}-{option_type}@LIN"
            else:
                # Still missing required fields even after validation - skip
                logger.debug(
                    f"Skipping incomplete option: missing strike_clean={strike_clean}, option_type={option_type}, expiry_str={expiry_str}"
                )
                return None
        elif instrument_type == "FUTURE":
            # TradFi futures are always linear (@LIN) - they settle in USD (quote asset)
            if expiry_str:
                symbol = f"{base_asset}-{quote_asset}-{expiry_str}@LIN"
            else:
                # Missing expiry for futures - log debug info to help diagnose
                logger.debug(
                    f"⚠️ Future without expiry_str: exchange={exchange}, dataset={dataset}, "
                    f"raw_symbol={exchange_raw_symbol}, base_asset={base_asset}, "
                    f"expiry_time={expiry_time}, expiry_dt={expiry_dt}"
                )
                symbol = f"{base_asset}-{quote_asset}@LIN"
        elif expiry_str:
            symbol = f"{base_asset}-{quote_asset}-{expiry_str}"

        # Build canonical instrument key
        venue = self._normalize_venue(exchange)
        instrument_key = f"{venue}:{instrument_type}:{symbol}"

        # Handle expiry datetime conversion
        # IMPORTANT: Not all instruments expire at the same time on an exchange!
        # - CME options: 9:00 AM CT (ALL options: quarterly/serial/weekly) - standardized time
        # - CME futures: Varies by contract, typically 4:00 PM CT (trading close)
        # - CBOE options: 4:15 PM ET
        # For CME options, if Databento provides only date (midnight), use standard 9:00 AM CT expiry time.
        # For other exchanges, only use expiry times that Databento explicitly provides with time component.
        expiry_iso = None
        if expiry_dt is not None:
            try:
                # expiry_dt already parsed above using pd.to_datetime with unit="ns"

                # Check if expiry time is midnight (likely date-only from Databento)
                if expiry_dt.hour == 0 and expiry_dt.minute == 0 and expiry_dt.second == 0:
                    # Databento provided date-only (midnight)
                    if exchange.upper() == "CME" and instrument_type == "OPTION":
                        # CME options ALL expire at 9:00 AM CT (standardized across quarterly/serial/weekly)
                        # Reference: CME Group Rulebook - all equity index options (ES, NQ, etc.) expire at 9:00 AM CT
                        # UTC equivalents:
                        #   - 9:00 AM CT (CST, UTC-6) = 3:00 PM UTC (winter, Nov-Mar)
                        #   - 9:00 AM CT (CDT, UTC-5) = 2:00 PM UTC (summer, Mar-Nov)
                        # ZoneInfo automatically handles DST transitions based on the expiry date

                        # Get the expiry date
                        expiry_date = expiry_dt.date()

                        # Create 9:00 AM CT datetime for the expiry date
                        # ZoneInfo("America/Chicago") automatically determines DST based on expiry_date
                        ct_tz = ZoneInfo("America/Chicago")
                        expiry_9am_ct = datetime.combine(expiry_date, time(9, 0, 0)).replace(
                            tzinfo=ct_tz
                        )

                        # Convert to UTC (ZoneInfo handles DST automatically)
                        expiry_iso = expiry_9am_ct.astimezone(timezone.utc).isoformat()

                        logger.debug(
                            f"✅ Set CME option expiry to 9:00 AM CT for {exchange_raw_symbol}: "
                            f"{expiry_date} -> {expiry_iso} (UTC)"
                        )

                    elif exchange.upper() == "CME" and instrument_type == "FUTURE":
                        # CME futures expire at end of trading day, typically 4:00 PM CT
                        # Reference: CME Group - most equity index futures (ES, NQ, RTY, YM) settle at 4:00 PM CT
                        # Some products like agricultural (ZC, ZW, ZS) have different times but 4 PM CT is reasonable default
                        expiry_date = expiry_dt.date()
                        ct_tz = ZoneInfo("America/Chicago")
                        expiry_4pm_ct = datetime.combine(expiry_date, time(16, 0, 0)).replace(
                            tzinfo=ct_tz
                        )
                        expiry_iso = expiry_4pm_ct.astimezone(timezone.utc).isoformat()
                        logger.debug(
                            f"✅ Set CME future expiry to 4:00 PM CT for {exchange_raw_symbol}: "
                            f"{expiry_date} -> {expiry_iso} (UTC)"
                        )

                    elif exchange.upper() in ("ICE", "IFEU", "IFUS"):
                        # ICE futures have specific expiry times
                        # ICE Europe (IFEU) - Brent, Gasoil, WTI Europe - use London time
                        # ICE US (IFUS) - Cotton, Coffee, Sugar, Cocoa, OJ, Dollar Index - use New York time
                        expiry_date = expiry_dt.date()

                        # Determine expiry time based on product code and dataset
                        product_code = exchange_raw_symbol[:1].upper() if exchange_raw_symbol else ""

                        # Check dataset to determine if ICE Europe or ICE US
                        is_ice_europe = dataset and "IFEU" in dataset.upper()
                        is_ice_us = dataset and "IFUS" in dataset.upper()

                        if is_ice_europe:
                            # ICE Europe (London time) - Brent, Gasoil, WTI
                            # Reference: ICE Futures Europe Contract Specifications
                            # - Brent Crude (BRN): 19:30 London
                            # - Gasoil (G): 12:00 London
                            # - WTI (T): 19:30 London
                            if product_code == "G":
                                expiry_hour, expiry_minute = 12, 0  # Gasoil
                            else:
                                expiry_hour, expiry_minute = 19, 30  # Brent, WTI, others

                            # London timezone handles GMT/BST automatically
                            london_tz = ZoneInfo("Europe/London")
                            expiry_local = datetime.combine(
                                expiry_date, time(expiry_hour, expiry_minute, 0)
                            ).replace(tzinfo=london_tz)
                            expiry_iso = expiry_local.astimezone(timezone.utc).isoformat()
                            logger.debug(
                                f"✅ Set ICE Europe expiry to {expiry_hour}:{expiry_minute:02d} London for "
                                f"{exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                            )
                        elif is_ice_us:
                            # ICE US (New York time) - Soft commodities, Dollar Index
                            # Reference: ICE Futures US Contract Specifications
                            # Most ICE US futures expire between 12:00 PM and 3:00 PM ET
                            # Using 2:30 PM ET as reasonable default (many soft commodities)
                            et_tz = ZoneInfo("America/New_York")
                            expiry_local = datetime.combine(
                                expiry_date, time(14, 30, 0)  # 2:30 PM ET
                            ).replace(tzinfo=et_tz)
                            expiry_iso = expiry_local.astimezone(timezone.utc).isoformat()
                            logger.debug(
                                f"✅ Set ICE US expiry to 2:30 PM ET for "
                                f"{exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                            )
                        else:
                            # Fallback for generic ICE - use 4:00 PM ET (safe US default)
                            et_tz = ZoneInfo("America/New_York")
                            expiry_local = datetime.combine(
                                expiry_date, time(16, 0, 0)
                            ).replace(tzinfo=et_tz)
                            expiry_iso = expiry_local.astimezone(timezone.utc).isoformat()
                            logger.debug(
                                f"✅ Set ICE (unknown dataset) expiry to 4:00 PM ET for "
                                f"{exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                            )

                    elif exchange.upper() == "CBOE" and instrument_type == "OPTION":
                        # CBOE options (SPX, VIX options) expire at 4:15 PM ET
                        # Reference: CBOE - options get 15 extra minutes after market close for exercise
                        expiry_date = expiry_dt.date()
                        et_tz = ZoneInfo("America/New_York")
                        expiry_415pm_et = datetime.combine(expiry_date, time(16, 15, 0)).replace(
                            tzinfo=et_tz
                        )
                        expiry_iso = expiry_415pm_et.astimezone(timezone.utc).isoformat()
                        logger.debug(
                            f"✅ Set CBOE option expiry to 4:15 PM ET for {exchange_raw_symbol}: "
                            f"{expiry_date} -> {expiry_iso} (UTC)"
                        )

                    elif exchange.upper() in ("OPRA", "NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "IEX") and instrument_type == "OPTION":
                        # US equity options (OPRA) expire at 4:00 PM ET
                        # Reference: OCC - standard US equity options expire at market close
                        expiry_date = expiry_dt.date()
                        et_tz = ZoneInfo("America/New_York")
                        expiry_4pm_et = datetime.combine(expiry_date, time(16, 0, 0)).replace(
                            tzinfo=et_tz
                        )
                        expiry_iso = expiry_4pm_et.astimezone(timezone.utc).isoformat()
                        logger.debug(
                            f"✅ Set US equity option expiry to 4:00 PM ET for {exchange_raw_symbol}: "
                            f"{expiry_date} -> {expiry_iso} (UTC)"
                        )

                    else:
                        # For other exchanges with date-only expiry, use 4:00 PM ET as reasonable US default
                        # Most US trading ends around this time
                        expiry_date = expiry_dt.date()
                        et_tz = ZoneInfo("America/New_York")
                        expiry_4pm_et = datetime.combine(expiry_date, time(16, 0, 0)).replace(
                            tzinfo=et_tz
                        )
                        expiry_iso = expiry_4pm_et.astimezone(timezone.utc).isoformat()
                        logger.debug(
                            f"Using 4:00 PM ET default for {exchange} {instrument_type} "
                            f"symbol {exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                        )
                else:
                    # Databento provided time, use as-is (it's the correct expiry time for this contract)
                    expiry_iso = expiry_dt.isoformat()
            except Exception as e:
                logger.warning(
                    f"Failed to convert expiry to ISO for {exchange} {exchange_raw_symbol}: {e}"
                )
                expiry_iso = None

        # Extract trading hours metadata using exchange-specific defaults
        # Databento doesn't provide trading hours in DEFINITION schema, so we use defaults
        # Convert to UTC for consistency with other timestamps
        # This also calculates the trading session start/end times
        trading_hours = self._get_exchange_trading_hours(
            exchange, instrument_type, target_date=target_date
        )

        # Handle available_from_datetime and available_to_datetime
        # For TradFi instruments, these should reflect the actual trading session times
        # Priority: ts_event from Databento > trading session start > target_date start
        available_from = None
        if "ts_event" in row and pd.notna(row["ts_event"]):
            try:
                ts_event = row["ts_event"]
                if isinstance(ts_event, pd.Timestamp):
                    available_from = ts_event.isoformat()
                elif isinstance(ts_event, str):
                    available_from = pd.to_datetime(ts_event).isoformat()
            except Exception as e:
                logger.warning(f"Failed to parse ts_event: {e}")

        # If ts_event not available, use trading session start time
        # For sessions that span UTC days (like CME), this will be the previous day
        if not available_from:
            if trading_hours.get("session_start_utc"):
                available_from = trading_hours["session_start_utc"]
            elif target_date:
                # Fallback to target date start (00:00:00 UTC)
                target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                available_from = target_date_start.isoformat()
            else:
                # Fallback to current UTC time (should not happen in normal flow)
                available_from = datetime.now(timezone.utc).isoformat()
                logger.warning(
                    "No target_date provided, using current UTC time for available_from_datetime"
                )

        # Set available_to_datetime to trading session end time
        # For sessions that span UTC days, this will be the same day (closing after midnight UTC)
        available_to = None
        if trading_hours.get("session_end_utc"):
            available_to = trading_hours["session_end_utc"]
        elif expiry_iso:
            # Fallback to expiry if no session end
            available_to = expiry_iso

        return {
            "instrument_key": instrument_key,
            "venue": venue,
            "instrument_type": instrument_type,
            "symbol": symbol,  # Human-readable symbol
            "base_asset": base_asset,  # Human-readable base asset
            "quote_asset": quote_asset,  # Human-readable quote asset
            "settle_asset": quote_asset,
            "expiry": expiry_iso,  # Expiry datetime with exchange-specific time (not midnight)
            "tick_size": str(min_price_increment),
            "min_size": str(min_price_increment),
            "asset_class": "traditional",
            "venue_type": "exchange",
            "chain": "off-chain",  # TradFi exchanges are off-chain
            "market_category": "TRADFI",  # Databento instruments are always TRADFI (databento_symbol is filled)
            "data_provider": "databento",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": exchange_raw_symbol,  # Raw exchange code (e.g., "6A", "6E", "ES", "AAPL")
            "databento_symbol": databento_symbol,  # Databento query symbol (e.g., "6A.FUT", "ES.FUT", "SPY", "SPY.OPT")
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": available_to,  # Trading session end time (or expiry if no session)
            # Data types: OPTIONS use trades (OHLCV not available for options in Databento)
            # See: https://databento.com/docs/examples/options/equity-options-introduction
            "data_types": "trades" if instrument_type == "OPTION" else "ohlcv_1m",
            "inverse": False,
            "contract_size": (
                row.get("contract_size", None) if pd.notna(row.get("contract_size")) else None
            ),
            "underlying": base_asset,  # Human-readable underlying
            "strike": (
                strike_price if instrument_type == "OPTION" else ""
            ),  # Strike price for options
            "option_type": (
                option_type if instrument_type == "OPTION" else ""
            ),  # CALL or PUT for options
            # Trading hours metadata (TradFi only)
            "trading_hours_open": trading_hours.get("open"),
            "trading_hours_close": trading_hours.get("close"),
            "trading_session": trading_hours.get("session"),
            "is_trading_day": trading_hours.get("is_trading_day"),
            "holiday_calendar": trading_hours.get("holiday_calendar"),
        }

    def _normalize_venue(self, exchange: str) -> str:
        """
        Normalize exchange name to canonical venue format.

        Args:
            exchange: Exchange name

        Returns:
            Canonical venue name
        """
        venue_mapping = {
            "CME": "CME",
            "ICE": "ICE",
            "ICE-EU": "ICE-EU",
            "CBOE": "CBOE",
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
        }

        exchange_upper = exchange.upper()
        return venue_mapping.get(exchange_upper, exchange_upper)

    def _get_exchange_trading_hours(
        self, exchange: str, instrument_type: str, target_date: Optional[datetime] = None
    ) -> Dict[str, Optional[str]]:
        """
        Get exchange-specific trading hours defaults in UTC.

        Databento doesn't provide trading hours in DEFINITION schema,
        so we use exchange-specific defaults and convert to UTC for consistency.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            instrument_type: Instrument type (e.g., 'FUTURE', 'EQUITY')
            target_date: Target date for DST calculation (defaults to current date)

        Returns:
            Dictionary with trading hours metadata (times in UTC)
        """
        exchange_upper = exchange.upper()

        # Use target_date for DST calculation, default to current date
        if target_date is None:
            target_date = datetime.now(timezone.utc)
        else:
            # Ensure target_date is timezone-aware
            if target_date.tzinfo is None:
                target_date = target_date.replace(tzinfo=timezone.utc)

        # Exchange-specific trading hours (in exchange local timezone)
        # Format: "HH:MM:SS+TZ" where TZ is timezone offset
        trading_hours_map = {
            "CME": {
                "open_local": "17:00:00-06:00",  # 5:00 PM CT (previous day for next-day trading)
                "close_local": "16:00:00-06:00",  # 4:00 PM CT
                "timezone": "America/Chicago",  # Central Time
                "session": "regular",
                "holiday_calendar": "CME",
            },
            "CBOE": {
                "open_local": "09:30:00-05:00",  # 9:30 AM ET
                "close_local": "16:15:00-05:00",  # 4:15 PM ET (VIX index trading hours)
                "timezone": "America/New_York",  # Eastern Time
                "session": "regular",
                "holiday_calendar": "CBOE",
            },
            "NASDAQ": {
                "open_local": "09:30:00-05:00",  # 9:30 AM ET
                "close_local": "16:00:00-05:00",  # 4:00 PM ET
                "timezone": "America/New_York",  # Eastern Time (DST-aware)
                "session": "regular",
                "holiday_calendar": "NASDAQ",
            },
            "NYSE": {
                "open_local": "09:30:00-05:00",  # 9:30 AM ET
                "close_local": "16:00:00-05:00",  # 4:00 PM ET
                "timezone": "America/New_York",  # Eastern Time (DST-aware)
                "session": "regular",
                "holiday_calendar": "NYSE",
            },
        }

        # Get trading hours for this exchange
        hours_config = trading_hours_map.get(exchange_upper, {})

        if not hours_config:
            return {
                "open": None,
                "close": None,
                "session": "regular",
                "is_trading_day": None,
                "holiday_calendar": None,
            }

        # Convert local time to UTC
        open_utc = None
        close_utc = None

        try:
            # Get timezone object for DST-aware conversion
            exchange_tz = ZoneInfo(hours_config["timezone"])

            # Extract time components from local time string (format: "HH:MM:SS-OO:OO")
            open_time_str = hours_config["open_local"]
            close_time_str = hours_config["close_local"]

            # Remove timezone offset part (everything after '-' or '+')
            # Format is "HH:MM:SS-OO:OO", we want "HH:MM:SS"
            open_time_only = (
                open_time_str.split("-")[0] if "-" in open_time_str else open_time_str.split("+")[0]
            )
            close_time_only = (
                close_time_str.split("-")[0]
                if "-" in close_time_str
                else close_time_str.split("+")[0]
            )

            # Parse time components (HH:MM:SS)
            open_parts = open_time_only.split(":")
            close_parts = close_time_only.split(":")
            open_hour, open_min, open_sec = (
                int(open_parts[0]),
                int(open_parts[1]),
                int(open_parts[2]),
            )
            close_hour, close_min, close_sec = (
                int(close_parts[0]),
                int(close_parts[1]),
                int(close_parts[2]),
            )

            # Determine if open time is on previous day (for sessions that span UTC days)
            # CME: opens at 5pm CT previous day, closes at 4pm CT same day
            # CBOE: opens and closes same day

            # For CME: The session that CLOSES on target_date is the one we want
            # That session STARTS on the previous day (Sunday evening for Monday's session)
            # For CBOE: Session opens and closes same day

            # Check if open time is before close time (same day) or after (previous day)
            open_time_of_day = open_hour * 3600 + open_min * 60 + open_sec
            close_time_of_day = close_hour * 3600 + close_min * 60 + close_sec

            # If open time is after close time, it's on the previous day
            # This means the session that CLOSES on target_date STARTED the previous day
            open_date = target_date.date()
            if open_time_of_day > close_time_of_day:
                # Session spans UTC days: open is previous day
                # This is the session that CLOSES on target_date
                open_date = open_date - timedelta(days=1)

            # Create datetime objects in exchange local timezone (DST-aware)
            # open_date is the day the session STARTS (may be previous day for CME)
            # target_date is the day the session CLOSES (the day we're querying for)
            open_local_dt = datetime(
                open_date.year,
                open_date.month,
                open_date.day,
                open_hour,
                open_min,
                open_sec,
                tzinfo=exchange_tz,
            )
            close_local_dt = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                close_hour,
                close_min,
                close_sec,
                tzinfo=exchange_tz,
            )

            # Convert to UTC
            open_utc_dt = open_local_dt.astimezone(timezone.utc)
            close_utc_dt = close_local_dt.astimezone(timezone.utc)

            # Format as "HH:MM:SS+00:00" for trading hours display
            open_utc = open_utc_dt.strftime("%H:%M:%S+00:00")
            close_utc = close_utc_dt.strftime("%H:%M:%S+00:00")

            # Format as ISO strings for session start/end
            session_start_utc = open_utc_dt.isoformat()
            session_end_utc = close_utc_dt.isoformat()

            # Check if it's a holiday (simplified check - can be enhanced with holiday calendar)
            is_holiday = self._is_trading_holiday(
                target_date.date(), hours_config.get("holiday_calendar")
            )

            # For CME: Sunday is NOT a holiday (Sunday evening is when Monday session starts)
            # But Sunday itself is NOT a trading day - it's when Monday's session starts
            # So if target_date is Sunday, is_trading_day should be False
            is_trading_day = not is_holiday
            holiday_calendar = hours_config.get("holiday_calendar")
            if holiday_calendar == "CME":
                weekday = target_date.date().weekday()
                if weekday == 6:  # Sunday
                    is_trading_day = False  # Sunday is not a trading day (but not a holiday)

            # If holiday, set trading hours to "holiday"
            if is_holiday:
                open_utc = "holiday"
                close_utc = "holiday"

        except Exception as e:
            logger.warning(f"Failed to convert trading hours to UTC for {exchange}: {e}")
            # Fallback to local time if conversion fails
            open_utc = hours_config.get("open_local")
            close_utc = hours_config.get("close_local")
            session_start_utc = None
            session_end_utc = None
            is_holiday = False

        return {
            "open": open_utc,
            "close": close_utc,
            "session": hours_config.get("session", "regular"),
            "is_trading_day": (
                is_trading_day
                if "is_trading_day" in locals()
                else (not is_holiday if "is_holiday" in locals() else None)
            ),
            "holiday_calendar": hours_config.get("holiday_calendar"),
            "session_start_utc": session_start_utc if "session_start_utc" in locals() else None,
            "session_end_utc": session_end_utc if "session_end_utc" in locals() else None,
        }

    def _get_exchange_calendar(self, calendar_name: str) -> Optional[Any]:
        """
        Get exchange calendar instance (cached for performance).

        Uses the exchange_calendars library which provides accurate holiday
        schedules for major exchanges including NYSE, NASDAQ, CME.

        Args:
            calendar_name: Our calendar identifier (e.g., 'NYSE', 'NASDAQ', 'CME')

        Returns:
            Exchange calendar instance or None if not found
        """
        global _EXCHANGE_CALENDARS_CACHE

        if calendar_name in _EXCHANGE_CALENDARS_CACHE:
            return _EXCHANGE_CALENDARS_CACHE[calendar_name]

        # Map our calendar name to exchange_calendars code
        xcal_code = _EXCHANGE_CALENDAR_MAPPING.get(calendar_name)
        if not xcal_code:
            logger.warning(f"No exchange calendar mapping for: {calendar_name}")
            return None

        try:
            # Get the calendar (cached by exchange_calendars internally too)
            cal = xcals.get_calendar(xcal_code)
            _EXCHANGE_CALENDARS_CACHE[calendar_name] = cal
            logger.debug(f"✅ Loaded exchange calendar: {calendar_name} -> {xcal_code}")
            return cal
        except Exception as e:
            logger.warning(f"Failed to load exchange calendar {xcal_code}: {e}")
            return None

    def _is_trading_holiday(self, date: datetime.date, calendar: Optional[str] = None) -> bool:
        """
        Check if a date is a trading holiday using exchange_calendars library.

        Provides accurate holiday detection for US markets including:
        - New Year's Day
        - Martin Luther King Jr. Day
        - Presidents' Day
        - Good Friday
        - Memorial Day
        - Juneteenth (since 2022)
        - Independence Day
        - Labor Day
        - Thanksgiving
        - Christmas

        For exchanges like CME that open on Sunday evening UTC (for Monday trading),
        Sunday is NOT a holiday - it's part of Monday's trading session.

        Args:
            date: Date to check
            calendar: Holiday calendar identifier (e.g., 'CME', 'NYSE', 'NASDAQ')

        Returns:
            True if date is a holiday (market closed), False otherwise (market open)
        """
        weekday = date.weekday()

        # Special handling for CME: Sunday evening UTC starts Monday's session
        # Sunday itself is NOT a trading day, but it's also not a "holiday"
        # The is_trading_day logic in _get_exchange_trading_hours handles this
        if calendar == "CME" and weekday == 6:  # Sunday
            # Sunday is part of Monday's session for CME
            # Return False (not a holiday) - the session check handles this
            return False

        # Get exchange calendar for accurate holiday detection
        xcal = self._get_exchange_calendar(calendar) if calendar else None

        if xcal:
            try:
                # Convert date to pandas Timestamp for exchange_calendars
                ts = pd.Timestamp(date)

                # Check if this date is a valid trading session
                # exchange_calendars.is_session returns True if market is OPEN
                is_open = xcal.is_session(ts)

                # If market is closed and it's a weekday, it's a holiday
                if not is_open:
                    if weekday < 5:  # Monday-Friday
                        # Log the holiday for debugging
                        logger.debug(f"📅 {date} is a US market holiday (calendar: {calendar})")
                    return True  # Closed (holiday or weekend)
                return False  # Open (trading day)

            except Exception as e:
                logger.warning(f"Exchange calendar check failed for {date}: {e}")
                # Fall through to default logic

        # Fallback: weekends are always closed
        if weekday >= 5:  # Saturday (5) or Sunday (6)
            return True

        # If no calendar available and it's a weekday, assume open
        return False

    def is_us_market_holiday(self, date: datetime.date) -> tuple[bool, Optional[str]]:
        """
        Check if a date is a US market holiday and return holiday name if known.

        Useful for logging and user feedback when no data is returned.

        Args:
            date: Date to check

        Returns:
            Tuple of (is_holiday, holiday_name or None)
        """
        xcal = self._get_exchange_calendar("NYSE")
        if not xcal:
            return (False, None)

        try:
            ts = pd.Timestamp(date)
            is_open = xcal.is_session(ts)

            if not is_open and date.weekday() < 5:
                # It's a weekday but market is closed - find the holiday name
                # Check common US holidays
                month, day = date.month, date.day

                # Fixed holidays (approximate - some shift for weekends)
                if month == 1 and day == 1:
                    return (True, "New Year's Day")
                if month == 7 and day in [3, 4, 5]:  # July 4 or observed
                    return (True, "Independence Day")
                if month == 12 and day in [24, 25, 26]:  # Dec 25 or observed
                    return (True, "Christmas")
                if month == 6 and day == 19:
                    return (True, "Juneteenth")

                # Variable holidays (use approximations)
                if month == 1 and 15 <= day <= 21 and date.weekday() == 0:
                    return (True, "Martin Luther King Jr. Day")
                if month == 2 and 15 <= day <= 21 and date.weekday() == 0:
                    return (True, "Presidents' Day")
                if month == 5 and 25 <= day <= 31 and date.weekday() == 0:
                    return (True, "Memorial Day")
                if month == 9 and 1 <= day <= 7 and date.weekday() == 0:
                    return (True, "Labor Day")
                if month == 11 and 22 <= day <= 28 and date.weekday() == 3:
                    return (True, "Thanksgiving")
                if month in [3, 4]:  # Good Friday is in March or April
                    return (True, "Good Friday")

                return (True, "US Market Holiday")

            return (False, None)

        except Exception as e:
            logger.warning(f"Holiday name lookup failed: {e}")
            return (False, None)
