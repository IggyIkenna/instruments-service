"""
Databento Venue Adapter

Fetches TradFi instrument definitions from Databento API.
Supports CME, NASDAQ, NYSE, and other TradFi exchanges.

Reference: archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py
"""

import logging
import os
import re
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone
import pandas as pd

try:
    import databento as db

    DATABENTO_AVAILABLE = True
except ImportError:
    DATABENTO_AVAILABLE = False
    logging.warning("databento package not available. Install with: pip install databento")

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
                project_id = project_id or os.getenv("GCP_PROJECT_ID", "central-element-323112")

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
                logger.warning("unified-cloud-services not available, falling back to env var")
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

        # Group symbols by dataset AND stype_in
        # Some exchanges (like ICE) have symbols on different datasets:
        # - ICE Europe Commodities (BRN, G) -> IFEU.IMPACT
        # - ICE Futures US Softs (KC, OJ, CC, SB) -> IFUS.IMPACT
        # Databento API requires separate calls for different datasets and stype_in values
        symbols_by_dataset_and_stype = {}
        for symbol in symbols:
            # Get instrument definition to determine dataset
            if dataset in ["DBEQ.BASIC", "OPRA.PILLAR"]:
                inst = unified_config.get_instrument(symbol, venue=None)  # Search across all venues
            else:
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
        all_instruments = {}
        for (
            symbol_dataset,
            stype_in,
        ), symbol_group in symbols_by_dataset_and_stype.items():
            try:
                # Fetch instrument definitions for this dataset/stype_in group
                zipped_data = self.client.timeseries.get_range(
                    dataset=symbol_dataset,  # Use dataset from instrument definition
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
                if symbol_dataset == "DBEQ.BASIC" and "publisher_id" in df.columns:
                    df = df[df["publisher_id"] == 39]
                    if df.empty:
                        logger.warning(
                            f"No instruments found after publisher_id filtering for {exchange} on {start_date_str}"
                        )
                        continue

                # Process and merge into all_instruments
                group_instruments = self._process_databento_dataframe(
                    df, exchange, symbol_dataset, symbol_group, stype_in
                )
                all_instruments.update(group_instruments)

            except Exception as e:
                logger.error(
                    f"Failed to fetch Databento instruments for {exchange} (stype_in={stype_in}): {e}"
                )
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
            "ICE": "IFEU.IMPACT",  # ICE Europe Commodities iMpact (default for ICE)
            # Note: ICE Futures US softs (KC, OJ, CC, SB) use IFUS.IMPACT, handled via symbol lookup
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
        self,
        df: pd.DataFrame,
        exchange: str,
        dataset: str,
        query_symbols: List[str],
        stype_in: str,
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
            if query_sym.endswith(".FUT"):
                base_asset = query_sym[:-4]  # Remove '.FUT'
                asset_to_query_symbol[base_asset] = query_sym
            elif query_sym.endswith(".OPT"):
                base_asset = query_sym[:-4]  # Remove '.OPT'
                asset_to_query_symbol[base_asset] = query_sym
            else:
                # For raw_symbol (equities), the query symbol IS the asset
                asset_to_query_symbol[query_sym] = query_sym

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

                # Determine databento_symbol (the query symbol we used)
                if stype_in == "parent":
                    # For parent symbology, map asset back to query symbol
                    databento_symbol = asset_to_query_symbol.get(
                        asset, query_symbols[0] if query_symbols else ""
                    )
                else:
                    # For raw_symbol, the asset IS the query symbol
                    databento_symbol = asset_to_query_symbol.get(asset, asset)

                # exchange_raw_symbol should be the actual Databento symbol (contract symbol)
                # This is what the exchange uses internally, not the asset/base
                # For futures: "ESZ24" (specific contract), for options: "SPY 251219C500", for equities: "AAPL"
                exchange_raw_symbol = str(symbol) if symbol else asset

                inst_def = self._convert_to_instrument_definition(
                    row, exchange, dataset, databento_symbol, exchange_raw_symbol
                )
                instruments[symbol] = inst_def
            except Exception as e:
                logger.warning(f"Failed to process symbol {symbol}: {e}")
                continue

        return instruments

    def _convert_to_instrument_definition(
        self,
        row: pd.Series,
        exchange: str,
        dataset: str,
        databento_symbol: str,
        exchange_raw_symbol: str = "",
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
        if security_type in ["STK", "ETF"] or (
            not security_type and underlying_asset and len(underlying_asset) <= 5
        ):
            # Equities/ETFs are already human-readable, don't convert
            base_asset = underlying_asset if underlying_asset else exchange_raw_symbol
        elif instrument_type == "OPTION":
            # Options: use underlying asset (already human-readable like SPY)
            base_asset = underlying_asset if underlying_asset else ""
        else:
            # Futures: convert exchange codes to human-readable names
            base_asset = (
                unified_config.get_human_readable_name(underlying_asset) if underlying_asset else ""
            )

        # For TradFi (equities, options, futures), quote currency is always USD
        # Per INSTRUMENT_KEY.md: stocks/equities use USD as quote currency
        if security_type in ["STK", "ETF", "OPT", "FUT"] or instrument_type in [
            "EQUITY",
            "OPTION",
            "FUTURE",
        ]:
            quote_asset = "USD"
        else:
            quote_asset = currency_raw  # Currency codes are already human-readable (USD, EUR, etc.)

        # Parse expiry if available
        expiry_time = None
        expiry_str = ""
        if "expiration" in row and pd.notna(row["expiration"]):
            expiry_time = row["expiration"]
            # Format expiry as YYMMDD
            try:
                if isinstance(expiry_time, str):
                    expiry_dt = pd.to_datetime(expiry_time)
                else:
                    expiry_dt = expiry_time
                expiry_str = expiry_dt.strftime("%y%m%d")
            except Exception as e:
                logger.warning(f"Failed to parse expiry {expiry_time}: {e}")

        # Extract option-specific fields
        strike_price = ""
        option_type = ""
        if instrument_type == "OPTION":
            # Extract strike price from Databento response first
            if "strike_price" in row and pd.notna(row["strike_price"]):
                strike_price_val = row["strike_price"]
                if isinstance(strike_price_val, (int, float)):
                    strike_price = str(strike_price_val)
                else:
                    strike_price = str(strike_price_val)

            # Parse OCC format from raw_symbol if strike/option_type not found
            # OCC format: SPY   230523C00480000 (21 chars)
            # Format: [6-char padded underlying][YYMMDD][C/P][8-digit strike]
            databento_symbol_raw = row.get("raw_symbol", "") or exchange_raw_symbol
            if pd.notna(databento_symbol_raw) and databento_symbol_raw:
                symbol_str = str(databento_symbol_raw).strip().upper()

                # OCC format parsing: extract expiry, option type, and strike
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
                    # Fallback: try to find C/P followed by digits
                    match = re.search(r"([CP])(\d+)", symbol_str)
                    if match:
                        opt_char = match.group(1)
                        strike_digits = match.group(2)
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
            elif expiry_str:
                symbol = f"{base_asset}-{quote_asset}-{expiry_str}@LIN"
        elif instrument_type == "FUTURE":
            # TradFi futures are always linear (@LIN) - they settle in USD (quote asset)
            if expiry_str:
                symbol = f"{base_asset}-{quote_asset}-{expiry_str}@LIN"
            else:
                symbol = f"{base_asset}-{quote_asset}@LIN"
        elif expiry_str:
            symbol = f"{base_asset}-{quote_asset}-{expiry_str}"

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
            "chain": "off-chain",  # TradFi exchanges are off-chain
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
            "CBOE": "CBOE",
        }

        exchange_upper = exchange.upper()
        return venue_mapping.get(exchange_upper, exchange_upper)
