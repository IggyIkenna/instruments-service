"""
Databento Venue Adapter

Fetches TradFi instrument definitions from Databento API.
Supports CME, NASDAQ, NYSE, and other TradFi exchanges.

Reference: archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone
import pandas as pd

try:
    import databento as db

    DATABENTO_AVAILABLE = True
except ImportError:
    DATABENTO_AVAILABLE = False
    logging.warning(
        "databento package not available. Install with: pip install databento"
    )

logger = logging.getLogger(__name__)


class DatabentoAdapter:
    """
    Adapter for fetching TradFi instrument definitions from Databento.

    Supports:
    - CME (futures, commodities)
    - NASDAQ (equities)
    - NYSE (equities)
    - Other TradFi exchanges
    """

    def __init__(self, api_key: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize Databento adapter.

        Args:
            api_key: Databento API key (optional, uses Secret Manager if not provided)
            project_id: GCP project ID for Secret Manager (defaults to GCP_PROJECT_ID env var)
        """
        if not DATABENTO_AVAILABLE:
            raise ImportError(
                "databento package not available. Install with: pip install databento"
            )

        # Try provided API key first
        self.api_key = api_key

        # If not provided, try Secret Manager
        if not self.api_key:
            try:
                from unified_cloud_services import get_secret_with_fallback

                secret_name = os.getenv("DATABENTO_SECRET_NAME", "databento-api-key")
                project_id = project_id or os.getenv(
                    "GCP_PROJECT_ID", "central-element-323112"
                )

                self.api_key = get_secret_with_fallback(
                    project_id=project_id,
                    secret_name=secret_name,
                    fallback_env_var="DATABENTO_API_KEY",
                )

                if self.api_key:
                    logger.info(
                        f"✅ Retrieved Databento API key from Secret Manager (secret: {secret_name})"
                    )
            except ImportError:
                logger.warning(
                    "unified-cloud-services not available, falling back to env var"
                )
                self.api_key = os.getenv("DATABENTO_API_KEY")
            except Exception as e:
                logger.warning(f"⚠️ Failed to retrieve API key from Secret Manager: {e}")
                self.api_key = os.getenv("DATABENTO_API_KEY")

        if not self.api_key:
            raise ValueError(
                "Databento API key required. Set DATABENTO_SECRET_NAME env var (for Secret Manager), "
                "DATABENTO_API_KEY env var (fallback), or pass api_key parameter."
            )

        self.client = db.Historical(self.api_key)
        logger.info("✅ DatabentoAdapter initialized")

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
        # Import unified config to get stype_in for each symbol
        from instruments_service.config import UnifiedInstrumentConfig
        
        unified_config = UnifiedInstrumentConfig()
        
        # Map exchange to Databento dataset
        if dataset is None:
            dataset = self._get_dataset_for_exchange(exchange)

        # Adjust date for weekend handling (Databento requires previous trading day)
        adjusted_date = self._adjust_date_for_weekend(date)

        start_date_str = (adjusted_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date_str = adjusted_date.strftime("%Y-%m-%d")

        # Group symbols by stype_in (parent vs raw_symbol)
        # Databento API requires separate calls for different stype_in values
        symbols_by_stype = {}
        for symbol in symbols:
            # For DBEQ.BASIC (NASDAQ/NYSE) and OPRA.PILLAR (CBOE), don't filter by venue since datasets cover multiple venues
            # For other datasets, filter by venue
            if dataset in ["DBEQ.BASIC", "OPRA.PILLAR"]:
                inst = unified_config.get_instrument(symbol, venue=None)  # Search across all venues
            else:
                inst = unified_config.get_instrument(symbol, venue=exchange)
            
            if inst:
                stype = inst.stype_in
                if stype not in symbols_by_stype:
                    symbols_by_stype[stype] = []
                symbols_by_stype[stype].append(symbol)
            else:
                # Fallback: try to infer stype_in from symbol format
                if symbol.endswith(".FUT") or symbol.endswith(".OPT"):
                    stype = "parent"
                else:
                    stype = "raw_symbol"
                if stype not in symbols_by_stype:
                    symbols_by_stype[stype] = []
                symbols_by_stype[stype].append(symbol)
                logger.warning(f"Symbol {symbol} not found in unified config, inferring stype_in={stype}")

        if not symbols_by_stype:
            logger.warning(f"No valid symbols found for {exchange} on {start_date_str}")
            return {}

        # Fetch instruments for each stype_in group
        all_instruments = {}
        for stype_in, symbol_group in symbols_by_stype.items():
            try:
                # Fetch instrument definitions for this stype_in group
                zipped_data = self.client.timeseries.get_range(
                    dataset=dataset,
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
                    logger.warning(
                        f"No instrument definitions found for {exchange} {symbol_group} (stype_in={stype_in}) on {start_date_str}"
                    )
                    continue

                # Filter out non-trading instruments
                if "instrument_class" in df.columns:
                    df = df[df["instrument_class"] != "S"]  # Exclude settlement-only
                
                # Filter by publisher_id == 39 for DBEQ.BASIC (NASDAQ/NYSE equities)
                # Per DATABENTO_TRANSLATION_PLAN.md: Filter DBEQ.BASIC by publisher_id == 39
                if dataset == "DBEQ.BASIC" and "publisher_id" in df.columns:
                    df = df[df["publisher_id"] == 39]
                    if df.empty:
                        logger.warning(
                            f"No instruments found after publisher_id filtering for {exchange} on {start_date_str}"
                        )
                        continue

                # Process and merge into all_instruments
                group_instruments = self._process_databento_dataframe(df, exchange, dataset, symbol_group, stype_in)
                all_instruments.update(group_instruments)

            except Exception as e:
                logger.error(f"Failed to fetch Databento instruments for {exchange} (stype_in={stype_in}): {e}")
                continue

        return all_instruments

    def _get_dataset_for_exchange(self, exchange: str) -> str:
        """
        Map exchange name to Databento dataset ID.

        Args:
            exchange: Exchange name

        Returns:
            Databento dataset ID
        """
        dataset_mapping = {
            "CME": "GLBX.MDP3",
            "NASDAQ": "DBEQ.BASIC",
            "NYSE": "DBEQ.BASIC",
            "ICE": "IFEU.IMPACT",  # Fixed: ICE Europe Commodities uses IFEU.IMPACT, not ICE.NYBOT
            "CBOE": "OPRA.PILLAR",  # CBOE options (SPX, SPY options)
        }

        exchange_upper = exchange.upper()
        return dataset_mapping.get(exchange_upper, "GLBX.MDP3")  # Default to CME

    def _adjust_date_for_weekend(self, date: datetime) -> datetime:
        """
        Adjust date to previous Friday if weekend.

        Databento requires previous trading day for instrument definitions.

        Args:
            date: Input date

        Returns:
            Adjusted date
        """
        if date.weekday() == 6:  # Sunday
            return date - timedelta(days=2)  # Go back to Friday
        elif date.weekday() == 0:  # Monday
            return date - timedelta(days=3)  # Go back to Friday
        return date

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
        self, df: pd.DataFrame, exchange: str, dataset: str, query_symbols: List[str], stype_in: str
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
        
        # Create mapping from asset to query symbol for futures/options
        # For parent symbology, we need to map asset (e.g., 'ES') back to query symbol (e.g., 'ES.FUT')
        asset_to_query_symbol = {}
        for query_sym in query_symbols:
            # Extract base asset from query symbol
            if query_sym.endswith('.FUT'):
                base_asset = query_sym[:-4]  # Remove '.FUT'
                asset_to_query_symbol[base_asset] = query_sym
            elif query_sym.endswith('.OPT'):
                base_asset = query_sym[:-4]  # Remove '.OPT'
                asset_to_query_symbol[base_asset] = query_sym
            else:
                # For raw_symbol (equities), the query symbol IS the asset
                asset_to_query_symbol[query_sym] = query_sym

        # Group by symbol and aggregate
        if "symbol" in df.columns:
            df_grouped = df.groupby("symbol").first()
            logger.info(f"📊 Processing {len(df_grouped)} unique instruments from Databento response (query: {query_symbols[:5]}...)")
        else:
            df_grouped = df

        for symbol, row in df_grouped.iterrows():
            try:
                # Get the query symbol used for this instrument
                asset = row.get("asset", "")
                asset = "" if pd.isna(asset) else str(asset)
                
                # Determine databento_symbol (the query symbol we used)
                if stype_in == "parent":
                    # For parent symbology, map asset back to query symbol
                    databento_symbol = asset_to_query_symbol.get(asset, query_symbols[0] if query_symbols else "")
                else:
                    # For raw_symbol, the asset IS the query symbol
                    databento_symbol = asset_to_query_symbol.get(asset, asset)
                
                inst_def = self._convert_to_instrument_definition(
                    row, exchange, dataset, databento_symbol
                )
                instruments[symbol] = inst_def
            except Exception as e:
                logger.warning(f"Failed to process symbol {symbol}: {e}")
                continue

        return instruments

    def _convert_to_instrument_definition(
        self, row: pd.Series, exchange: str, dataset: str, databento_symbol: str
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
        from instruments_service.config import UnifiedInstrumentConfig
        
        unified_config = UnifiedInstrumentConfig()
        
        # Extract fields from Databento schema (handle NaNs)
        asset_raw = row.get("asset", "")
        asset_raw = "" if pd.isna(asset_raw) else str(asset_raw)
        
        currency_raw = row.get("currency", "USD")
        currency_raw = "USD" if pd.isna(currency_raw) else str(currency_raw)
        
        security_type = row.get("security_type", "")
        security_type = "" if pd.isna(security_type) else str(security_type)
        
        min_price_increment = row.get("min_price_increment", 0.01)
        min_price_increment = 0.01 if pd.isna(min_price_increment) else float(min_price_increment)

        # exchange_raw_symbol = raw exchange code (e.g., "6A", "6E", "ES", "AAPL")
        exchange_raw_symbol = asset_raw

        # Convert to human-readable names using unified config
        # For equities/ETFs, asset is already human-readable (AAPL, SPY, etc.), only convert futures codes
        if security_type in ["STK", "ETF"] or (not security_type and asset_raw and len(asset_raw) <= 5):
            # Equities/ETFs are already human-readable, don't convert
            base_asset = asset_raw
        else:
            # Futures/options: convert exchange codes to human-readable names
            base_asset = unified_config.get_human_readable_name(asset_raw) if asset_raw else ""
        quote_asset = currency_raw  # Currency codes are already human-readable (USD, EUR, etc.)
        
        # Parse expiry if available
        expiry_time = None
        if "expiration" in row and pd.notna(row["expiration"]):
            expiry_time = row["expiration"]

        # Determine instrument type
        if security_type == "FUT":
            instrument_type = "FUTURE"
        elif security_type == "OPT":
            instrument_type = "OPTION"
        elif security_type == "ETF":
            instrument_type = "ETF"
        elif security_type == "STK":
            instrument_type = "EQUITY"
        else:
            instrument_type = "EQUITY"  # Default

        # Build symbol with human-readable base_asset
        symbol = f"{base_asset}-{quote_asset}"
        if expiry_time:
            # Format expiry as YYMMDD
            if isinstance(expiry_time, (str, pd.Timestamp)):
                try:
                    if isinstance(expiry_time, str):
                        expiry_dt = pd.to_datetime(expiry_time)
                    else:
                        expiry_dt = expiry_time
                    expiry_str = expiry_dt.strftime("%y%m%d")
                    symbol = f"{base_asset}-{quote_asset}-{expiry_str}"
                except Exception as e:
                    logger.warning(f"Failed to parse expiry {expiry_time}: {e}")

        # Build canonical instrument key
        venue = self._normalize_venue(exchange)
        instrument_key = f"{venue}:{instrument_type}:{symbol}"

        # Handle expiry datetime conversion
        expiry_iso = None
        if expiry_time:
            try:
                if isinstance(expiry_time, str):
                    expiry_dt = pd.to_datetime(expiry_time)
                elif isinstance(expiry_time, pd.Timestamp):
                    expiry_dt = expiry_time
                else:
                    expiry_dt = pd.to_datetime(expiry_time)
                expiry_iso = expiry_dt.isoformat()
            except Exception as e:
                logger.warning(f"Failed to convert expiry to ISO: {e}")

        # Handle available_from_datetime
        available_from = datetime.now(timezone.utc).isoformat()
        if "ts_event" in row and pd.notna(row["ts_event"]):
            try:
                ts_event = row["ts_event"]
                if isinstance(ts_event, pd.Timestamp):
                    available_from = ts_event.isoformat()
                elif isinstance(ts_event, str):
                    available_from = pd.to_datetime(ts_event).isoformat()
            except Exception as e:
                logger.warning(f"Failed to parse ts_event: {e}")

        return {
            "instrument_key": instrument_key,
            "venue": venue,
            "instrument_type": instrument_type,
            "symbol": symbol,  # Human-readable symbol
            "base_asset": base_asset,  # Human-readable base asset
            "quote_asset": quote_asset,  # Human-readable quote asset
            "settle_asset": quote_asset,
            "expiry": expiry_iso,
            "tick_size": str(min_price_increment),
            "min_size": str(min_price_increment),
            "asset_class": "traditional",
            "venue_type": "exchange",
            "data_provider": "databento",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": exchange_raw_symbol,  # Raw exchange code (e.g., "6A", "6E", "ES", "AAPL")
            "databento_symbol": databento_symbol,  # Databento query symbol (e.g., "6A.FUT", "ES.FUT", "SPY", "SPY.OPT")
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": available_from,
            "available_to_datetime": expiry_iso,
            "data_types": "ohlcv_1m",  # We fetch OHLCV 1m candles from Databento
            "inverse": False,
            "contract_size": row.get("contract_size", None) if pd.notna(row.get("contract_size")) else None,
            "underlying": base_asset,  # Human-readable underlying
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
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
            "ICE": "ICE",
        }

        exchange_upper = exchange.upper()
        return venue_mapping.get(exchange_upper, exchange_upper)
