"""
Databento Venue Adapter - REFACTORED

Fetches TradFi instrument definitions from Databento API.
Supports CME + VIX with performance optimizations:

ARCHITECTURE:
- Uses DatabentoBaseClient from unified-cloud-services (centralized network layer)
- This adapter handles domain-specific logic (instrument parsing, trading hours)
- Network concerns (sessions, retries, API keys) are handled by DatabentoBaseClient
- Multi-key rotation: 20 keys (databento-api-key-1..20) via SHARD_INDEX env var
- Cached UnifiedInstrumentConfig instance
- Parallel symbol group queries (asyncio.gather)

See instruments-service/docs/DATABENTO_ADAPTER_GUIDE.md for implementation details
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import databento as db
import exchange_calendars as xcals
import pandas as pd
from databento.common.error import BentoClientError
from unified_cloud_services import DatabentoBaseClient, DatabentoClientConfig, get_config

from instruments_service.app.venues.databento.converters.instrument_converter import (
    convert_to_instrument_definition,
    get_exchange_trading_hours,
)
from instruments_service.app.venues.databento.converters.special_instruments import (
    create_bitcoin_etf_instrument_definition as _create_bitcoin_etf,
)
from instruments_service.app.venues.databento.converters.special_instruments import (
    create_krwusd_instrument_definition as _create_krwusd,
)
from instruments_service.app.venues.databento.converters.symbol_resolver import (
    resolve_instrument_id_to_raw_symbol,
)
from instruments_service.config import UnifiedInstrumentConfig, instruments_config

logger = logging.getLogger(__name__)

# Unified config cache (domain-specific, not network-related)
_UNIFIED_CONFIG_CACHE: Optional[Any] = None

# Exchange calendar cache for holiday detection
# Maps our calendar names to exchange_calendars calendar codes
_EXCHANGE_CALENDAR_MAPPING = {
    "NASDAQ": "XNAS",  # NASDAQ Stock Market
    "NYSE": "XNYS",  # New York Stock Exchange
    "CME": "CMES",  # CME (uses same calendar as NYSE with some variations)
    "CBOE": "XCBF",  # CBOE Options Exchange (use CBOE-specific calendar for VIX)
    "ICE": "XNYS",  # ICE US uses NYSE calendar
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
            api_key: Databento API key (optional). If None, uses multi-key rotation
                     based on SHARD_INDEX env var via DatabentoBaseClient.
                     With 20 keys (databento-api-key-1..20), each shard gets
                     key_index = (shard_index % 20) + 1
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        # Create config with instruments-service specific settings
        # DatabentoBaseClient uses SHARD_INDEX for rotation (same as market-tick-data-handler)
        config = DatabentoClientConfig(
            secret_name_prefix=instruments_config.databento_secret_name,
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
    # BATCH API - Delegates to DatabentoBaseClient for unified batch orchestration
    # Re-downloads within 30 days are FREE!
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
        Fetch data using Databento's Batch Jobs API via DatabentoBaseClient.

        KEY BENEFIT: Re-downloading the same data within 30 days is FREE!
        Unlike Historical Streaming which bills every request.

        Delegates to DatabentoBaseClient.batch_download() which handles:
        - Deterministic key selection (same params -> same API key)
        - Expanded state checking (queued/processing/done)
        - GCS job cache for cross-shard deduplication
        - Falls back to streaming API when batch_download returns None (registry miss, job pending)

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
        logger.info(
            f"Using BATCH API for {dataset} {schema} ({len(symbols)} symbols) - FREE re-download within 30 days!"
        )

        # Delegate to base client's unified batch orchestration
        output_path = self._base_client.batch_download(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            stype_in=stype_in,
            start=start,
            end=end,
        )

        # Fallback to streaming when batch_download returns None (registry miss, job pending, etc.)
        if output_path is None:
            logger.info(
                f"Batch API returned None (no completed job) — falling back to streaming for "
                f"{dataset} {schema} ({len(symbols)} symbols)"
            )
            self._base_client.ip_rate_limiter.acquire("timeseries")
            return self.client.timeseries.get_range(
                dataset=dataset,
                schema=db.Schema.DEFINITION,  # Instrument definitions only use DEFINITION schema
                symbols=symbols,
                stype_in=stype_in,
                stype_out="instrument_id",
                start=start,
                end=end,
            )

        # Find the data file in the downloaded output
        # Databento batch.download() creates output at output_path/JOB_ID/
        # so we must search recursively (rglob) not just the top level (iterdir)
        data_file = None
        for f in output_path.rglob("*.dbn.zst"):
            data_file = f
            break
        if data_file is None:
            for f in output_path.rglob("*.dbn"):
                data_file = f
                break

        if data_file is None:
            raise FileNotFoundError(
                f"No .dbn or .dbn.zst data file found in batch download at {output_path}. "
                f"Contents: {[str(p) for p in output_path.rglob('*')]}"
            )

        return db.DBNStore.from_file(str(data_file))

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

        # ---------------------------------------------------------------
        # T+2 availability check: Databento historical data is only
        # available ~2 calendar days after the trading date (published
        # around UTC midnight, two days later).
        # Skip early to avoid billable 422 errors from the API.
        # ---------------------------------------------------------------
        utc_today = datetime.now(timezone.utc).date()
        databento_earliest_available = utc_today - timedelta(days=2)
        target_date_only = target_date.date() if hasattr(target_date, "date") else target_date
        if target_date_only > databento_earliest_available:
            logger.warning(
                f"⚠️ DATABENTO_T2: Skipping {exchange} on {target_date_only} - "
                f"Databento historical data has T+2 availability "
                f"(earliest queryable date today: {databento_earliest_available}). "
                f"Try again after {target_date_only + timedelta(days=2)} UTC."
            )
            return {}

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
                # NOTE: Databento may raise a 422 "data_no_data_found_for_request"
                # when there is genuinely no data (e.g. holidays). We catch that
                # specifically and treat it as empty data so the holiday fallback runs.
                df = pd.DataFrame()  # default empty
                try:
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
                        self._base_client.ip_rate_limiter.acquire("timeseries")
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
                except BentoClientError as fetch_err:
                    # Databento 4xx client error -- only treat 422 "no data" as empty.
                    # Other 4xx (400 bad request, 401 auth, 403 forbidden) must propagate.
                    if fetch_err.http_status == 422:
                        # Verify it's specifically "no data found" (not some other 422)
                        case = ""
                        if fetch_err.json_body and isinstance(fetch_err.json_body, dict):
                            detail = fetch_err.json_body.get("detail", {})
                            if isinstance(detail, dict):
                                case = detail.get("case", "")
                        if case == "data_no_data_found_for_request" or "no_data" in str(fetch_err).lower():
                            logger.info(
                                f"📅 Databento returned no-data for {exchange} on {start_date_str} "
                                f"(stype_in={stype_in}): {fetch_err}. "
                                f"Treating as empty for holiday fallback."
                            )
                            # df stays empty → holiday fallback below will trigger
                        else:
                            raise  # Unknown 422 → propagate
                    else:
                        raise  # 400/401/403/etc → propagate (bad query, auth failure)

                if df.empty:
                    # Check if this is a US market holiday or weekend
                    target_date_only = date.date() if hasattr(date, "date") else date
                    is_holiday, holiday_name = self.is_us_market_holiday(target_date_only)
                    is_weekend = target_date_only.weekday() >= 5

                    if is_holiday or is_weekend:
                        reason = f"US market holiday ({holiday_name})" if is_holiday else "weekend"
                        logger.info(
                            f"📅 No data for {exchange} on {start_date_str} - {reason}. "
                            f"Querying previous trading session(s) for instrument definitions."
                        )

                        # Try up to 10 previous trading sessions so we always produce a file
                        # (handles consecutive holidays, e.g. Dec 31 + Jan 1, or API delays)
                        session_to_try = target_date_only
                        fallback_success = False
                        for attempt in range(10):
                            prev_session_date = self._get_previous_trading_session(session_to_try, exchange)
                            if not prev_session_date:
                                logger.warning(
                                    f"⚠️ Could not find previous trading session for {exchange} "
                                    f"(attempt {attempt + 1}, from {session_to_try})"
                                )
                                break
                            prev_start_str = prev_session_date.strftime("%Y-%m-%d")
                            prev_end_str = (prev_session_date + timedelta(days=1)).strftime("%Y-%m-%d")
                            logger.info(
                                f"📅 Re-querying {exchange} with previous session date: {prev_start_str} "
                                f"(attempt {attempt + 1}/10)"
                            )
                            try:
                                if use_batch_api:
                                    fallback_data = self._fetch_with_batch_api(
                                        dataset=symbol_dataset,
                                        schema="definition",
                                        symbols=symbol_group,
                                        stype_in=stype_in,
                                        start=prev_start_str,
                                        end=prev_end_str,
                                    )
                                else:
                                    self._base_client.ip_rate_limiter.acquire("timeseries")
                                    fallback_data = self.client.timeseries.get_range(
                                        dataset=symbol_dataset,
                                        schema=db.Schema.DEFINITION,
                                        symbols=symbol_group,
                                        stype_in=stype_in,
                                        stype_out="instrument_id",
                                        start=prev_start_str,
                                        end=prev_end_str,
                                    )
                                df = fallback_data.to_df()
                                if not df.empty:
                                    logger.info(
                                        f"✅ Got {len(df)} instrument definitions from previous session "
                                        f"({prev_start_str}) for {exchange} on {reason} {start_date_str}"
                                    )
                                    fallback_success = True
                                    break
                                logger.warning(
                                    f"⚠️ Previous session {prev_start_str} also returned empty for {exchange}, "
                                    f"trying earlier session"
                                )
                            except Exception as fallback_err:
                                logger.warning(
                                    f"⚠️ Fallback query for {exchange} on {prev_start_str} failed: {fallback_err}, "
                                    f"trying earlier session"
                                )
                            session_to_try = prev_session_date
                        if not fallback_success:
                            logger.warning(
                                f"⚠️ All fallback attempts exhausted for {exchange} on {start_date_str} - "
                                f"no instrument definitions from previous sessions"
                            )
                            continue
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
                if "instrument_class" in df.columns and any(
                    s.endswith(".FUT") for s in symbol_group if isinstance(s, str)
                ):
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
                # IMPORTANT: Only apply to futures/options (parent symbology), NOT equities (raw_symbol)
                # Equities like BRK.B and BF.B have periods but are valid symbols
                # Spreads/combos contain special characters: dash (-), colon (:), plus (+), asterisk (*), slash (/)
                # ICE spreads specifically use period notation: "BRN FQF0024.H0024" (quarter spread)
                # Examples:
                # - Calendar spreads: "ESH6-ESM6" (dash between contracts)
                # - ICE calendar spreads: "BRN FQF0024.H0024", "G   FSN0025.Z0025" (period between legs)
                # - ICE internal IDs: "BRN 142   7377732", "G     3  30451873" (numbered format - not tradeable)
                # - Average price products: "CL:SA 02M F6" (colon separator)
                # - Ratio spreads: "CL*NG" (asterisk for ratio)
                if "raw_symbol" in df.columns and stype_in == "parent":
                    # Only filter spreads for futures/options (parent symbology)
                    # Equities use raw_symbol stype_in and may have periods (BRK.B, BF.B)
                    pre_spread_count = len(df)
                    # Exclude symbols with special characters indicating combos/spreads
                    # Pattern: dash (-), colon (:), plus (+), asterisk (*), slash (/), period (.)
                    df = df[~df["raw_symbol"].astype(str).str.contains(r"[-:+*/.]", regex=True, na=False)]
                    # Also filter out ICE internal/numbered formats (e.g., "BRN 142   7377732")
                    # These have a number sequence after the product code followed by a long ID
                    df = df[~df["raw_symbol"].astype(str).str.match(r"^[A-Z]+\s+\d+\s+\d+$", na=False)]
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
                logger.error(f"Failed to fetch Databento instruments for {exchange} (stype_in={stype_in}): {e}")
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
            # Session boundary times (uniform across all TradFi venues)
            "regular_open_utc": trading_hours.get("regular_open_utc"),
            "regular_close_utc": trading_hours.get("regular_close_utc"),
            "auction_open_utc": trading_hours.get("auction_open_utc"),
            "auction_close_utc": trading_hours.get("auction_close_utc"),
            "early_close_utc": trading_hours.get("early_close_utc"),
        }

    def create_krwusd_instrument_definition(self, target_date: datetime) -> Dict[str, Any]:
        """Create KRW/USD instrument definition. Delegates to converters.special_instruments."""
        return _create_krwusd(target_date)

    def create_bitcoin_etf_instrument_definition(self, ticker: str, target_date: datetime) -> Optional[Dict[str, Any]]:
        """Create Bitcoin ETF instrument definition. Delegates to converters.special_instruments."""
        return _create_bitcoin_etf(ticker, target_date, self._get_exchange_trading_hours)

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
                        databento_symbol = asset_to_fut_symbol.get(asset, query_symbols[0] if query_symbols else "")
                    elif security_type == "OOF":
                        # This is an option on futures - use options mapping
                        databento_symbol = asset_to_opt_symbol.get(asset, query_symbols[0] if query_symbols else "")
                    else:
                        # Unknown type - try futures first, then options
                        databento_symbol = asset_to_fut_symbol.get(
                            asset, asset_to_opt_symbol.get(asset, query_symbols[0] if query_symbols else "")
                        )
                else:
                    # For raw_symbol (equities), the raw_symbol IS the query symbol
                    # Note: DBEQ.BASIC returns empty 'asset' column, so we use the symbol directly
                    # The 'symbol' variable is the row index from df_grouped (raw_symbol)
                    databento_symbol = asset_to_raw_symbol.get(str(symbol), asset_to_raw_symbol.get(asset, str(symbol)))

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
        """Resolve instrument_id to raw_symbol via symbology API. Delegates to converters."""
        return resolve_instrument_id_to_raw_symbol(
            client=self.client,
            base_client=self._base_client,
            instrument_id=instrument_id,
            exchange=exchange,
            dataset=dataset,
            target_date=target_date,
        )

    def _convert_to_instrument_definition(
        self,
        row: pd.Series,
        exchange: str,
        dataset: str,
        databento_symbol: str,
        exchange_raw_symbol: str = "",
        target_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Delegate to instrument_converter.convert_to_instrument_definition."""
        return convert_to_instrument_definition(
            self,
            row,
            exchange,
            dataset,
            databento_symbol,
            exchange_raw_symbol,
            target_date,
        )

    def _get_exchange_trading_hours(
        self, exchange: str, instrument_type: str, target_date: Optional[datetime] = None
    ) -> Dict[str, Optional[str]]:
        """Delegate to instrument_converter.get_exchange_trading_hours."""
        return get_exchange_trading_hours(self, exchange, instrument_type, target_date)

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

    def _get_previous_trading_session(self, date: datetime.date, exchange: str) -> Optional[datetime.date]:
        """
        Find the previous trading session date for an exchange using exchange_calendars.

        Used when Databento returns empty data on holidays/weekends to fall back
        to the previous session's instrument definitions.

        Args:
            date: The date for which Databento returned empty
            exchange: Exchange name (e.g., 'CME', 'NYSE', 'NASDAQ')

        Returns:
            Previous trading session date, or None if not found
        """
        xcal = self._get_exchange_calendar(exchange)
        if not xcal:
            # Fallback: try going back day by day (max 7 days)
            for days_back in range(1, 8):
                prev = date - timedelta(days=days_back)
                if prev.weekday() < 5:  # Weekday
                    return prev
            return None

        try:
            ts = pd.Timestamp(date)
            # Use date_to_session with direction='previous' -- this works correctly
            # even when the input date is NOT a valid session (holidays, weekends).
            # Note: previous_session() requires input to BE a session, which fails
            # on holidays/weekends with NotSessionError.
            prev_session = xcal.date_to_session(ts, direction="previous")
            result = prev_session.date()
            # If date_to_session returned the same date (it IS a session), go back one more
            if result == date:
                # The date itself is a session, find the one before it
                prev_session = xcal.previous_session(ts)
                result = prev_session.date()
            return result
        except Exception as e:
            logger.warning(f"Failed to get previous session for {exchange} from {date}: {e}")
            # Fallback: go back day by day
            for days_back in range(1, 8):
                prev = date - timedelta(days=days_back)
                if prev.weekday() < 5:
                    return prev
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
