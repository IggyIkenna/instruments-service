"""
Instrument converter - Databento row to instrument definition.

Extracted from DatabentoAdapter._convert_to_instrument_definition and
_get_exchange_trading_hours. Takes adapter for symbology/holiday/calendar lookups.
"""

import calendar
import logging
import re
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from instruments_service.config import UnifiedInstrumentConfig

logger = logging.getLogger(__name__)

# Unified config cache (mirrors databento_adapter)
_UNIFIED_CONFIG_CACHE: Optional[Any] = None


def _normalize_venue(exchange: str) -> str:
    """Normalize exchange name to canonical venue format."""
    venue_mapping = {
        "CME": "CME",
        "ICE": "ICE",
        "ICE-EU": "ICE-EU",
        "CBOE": "CBOE",
        "NASDAQ": "NASDAQ",
        "NYSE": "NYSE",
    }
    return venue_mapping.get(exchange.upper(), exchange.upper())


def convert_to_instrument_definition(
    adapter: Any,
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
        "SPY",
        "QQQ",
        "IVV",
        "VOO",
        "VTI",
        "DIA",
        "IWM",
        "EEM",
        "VEA",
        "VWO",
        "GLD",
        "SLV",
        "USO",
        "UNG",
        "TLT",
        "IEF",
        "SHY",
        "LQD",
        "HYG",
        "JNK",
        "XLF",
        "XLE",
        "XLK",
        "XLV",
        "XLI",
        "XLY",
        "XLP",
        "XLB",
        "XLU",
        "XLRE",
        "VNQ",
        "IBB",
        "SMH",
        "IBIT",
        "FBTC",
        "ARKB",
        "GBTC",
        "BITO",
    }
    # Check if symbol (without -USD suffix) is a known ETF
    symbol_clean = asset_raw.replace("-USD", "").upper() if asset_raw else ""
    if not symbol_clean and exchange_raw_symbol:
        symbol_clean = exchange_raw_symbol.replace("-USD", "").upper()
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
        if underlying_asset and re.search(r"\d{6}[CP]\d{8}", str(underlying_asset).strip().upper()):
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
        base_asset = unified_config.get_human_readable_name(underlying_asset) if underlying_asset else ""

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
        logger.debug(f"⚠️ ICE Future {exchange_raw_symbol} has no expiration field or it's NaN")

    # If expiry is still missing for ICE/CME futures, try parsing from raw_symbol
    # ICE Europe format: BRNM25 (product + month code + 2-digit year)
    # CME format: ESM25 (product + month code + 2-digit year)
    # Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
    if not expiry_dt and exchange_raw_symbol and instrument_type == "FUTURE":
        month_codes = {
            "F": 1,
            "G": 2,
            "H": 3,
            "J": 4,
            "K": 5,
            "M": 6,
            "N": 7,
            "Q": 8,
            "U": 9,
            "V": 10,
            "X": 11,
            "Z": 12,
        }

        raw_upper = exchange_raw_symbol.upper().strip()

        # Pattern 1: Standard format [A-Z]+[FGHJKMNQUVXZ][0-9]{1,2} (e.g., BRNM25, ESZ4, NGF26)
        match = re.match(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d{1,2})$", raw_upper)

        # Pattern 2: Space-separated format (e.g., "BRN M25", "ES Z4")
        if not match:
            match = re.match(r"^([A-Z]+)\s+([FGHJKMNQUVXZ])(\d{1,2})$", raw_upper)

        # Pattern 3: ICE continuous contract format (just product code, no expiry)
        if not match and re.match(r"^[A-Z]{1,4}$", raw_upper):
            logger.debug(f"⏭️ Skipping expiry parsing for continuous contract: {exchange_raw_symbol}")

        if match:
            product_code = match.group(1)  # e.g., "BRN", "ES", "NG"
            month_code = match.group(2)  # e.g., "M", "Z"
            year_digits = match.group(3)  # e.g., "25", "4"

            month = month_codes.get(month_code)
            if month:
                if len(year_digits) == 1:
                    year = 2020 + int(year_digits)
                else:
                    year = 2000 + int(year_digits)

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
        elif not re.match(r"^[A-Z]{1,4}$", raw_upper):
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
                if symbol_str.startswith("UD:") or (":" in symbol_str[:5] and "UD" in symbol_str[:5]):
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
                                f"Skipping symbology resolution for future contract: {symbol_str} (instrument_class=F)"
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
                                    raw_symbol_resolved = adapter._resolve_instrument_id_to_raw_symbol(
                                        instrument_id, exchange, dataset, target_date
                                    )
                                    if raw_symbol_resolved:
                                        # Parse the resolved raw_symbol
                                        resolved_symbol_str = str(raw_symbol_resolved).strip().upper()
                                        logger.info(
                                            f"✅ Resolved instrument_id {instrument_id} to raw_symbol: {resolved_symbol_str}"
                                        )

                                        # Handle case where symbology returns dict format
                                        if isinstance(raw_symbol_resolved, dict) and "S" in raw_symbol_resolved:
                                            resolved_symbol_str = str(raw_symbol_resolved["S"]).strip().upper()
                                            logger.debug(f"Extracted symbol from dict 'S' key: {resolved_symbol_str}")

                                        # Check if resolved symbol is Databento internal format (e.g., "UD:1V: 12 2502245")
                                        # This format is not parseable and indicates the symbology API couldn't resolve to actual exchange symbol
                                        if resolved_symbol_str.startswith("UD:") or ":" in resolved_symbol_str[:5]:
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
                                                strike_str = resolved_cme_match.group(3)  # Strike price digits
                                                if not strike_price:
                                                    strike_price = strike_str
                                                if not option_type:
                                                    option_type = "CALL" if opt_char == "C" else "PUT"
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
                                                fallback_match = re.search(r"([CP])(\d+)", resolved_symbol_str)
                                                if fallback_match:
                                                    opt_char = fallback_match.group(1)
                                                    strike_str = fallback_match.group(2)
                                                    if not strike_price:
                                                        strike_price = strike_str
                                                    if not option_type:
                                                        option_type = "CALL" if opt_char == "C" else "PUT"
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
                                instr_class = str(row.get("instrument_class", "")).upper().strip()
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
                                str(int(strike_decimal)) if strike_decimal.is_integer() else str(strike_decimal)
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
                                        str(int(strike_decimal)) if strike_decimal.is_integer() else str(strike_decimal)
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
            strike_clean = strike_price.replace(".0", "").rstrip(".") if "." in strike_price else strike_price

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
    venue = _normalize_venue(exchange)
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
                    expiry_9am_ct = datetime.combine(expiry_date, time(9, 0, 0)).replace(tzinfo=ct_tz)

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
                    expiry_4pm_ct = datetime.combine(expiry_date, time(16, 0, 0)).replace(tzinfo=ct_tz)
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
                        expiry_local = datetime.combine(expiry_date, time(expiry_hour, expiry_minute, 0)).replace(
                            tzinfo=london_tz
                        )
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
                            expiry_date,
                            time(14, 30, 0),  # 2:30 PM ET
                        ).replace(tzinfo=et_tz)
                        expiry_iso = expiry_local.astimezone(timezone.utc).isoformat()
                        logger.debug(
                            f"✅ Set ICE US expiry to 2:30 PM ET for "
                            f"{exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                        )
                    else:
                        # Fallback for generic ICE - use 4:00 PM ET (safe US default)
                        et_tz = ZoneInfo("America/New_York")
                        expiry_local = datetime.combine(expiry_date, time(16, 0, 0)).replace(tzinfo=et_tz)
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
                    expiry_415pm_et = datetime.combine(expiry_date, time(16, 15, 0)).replace(tzinfo=et_tz)
                    expiry_iso = expiry_415pm_et.astimezone(timezone.utc).isoformat()
                    logger.debug(
                        f"✅ Set CBOE option expiry to 4:15 PM ET for {exchange_raw_symbol}: "
                        f"{expiry_date} -> {expiry_iso} (UTC)"
                    )

                elif (
                    exchange.upper() in ("OPRA", "NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "IEX")
                    and instrument_type == "OPTION"
                ):
                    # US equity options (OPRA) expire at 4:00 PM ET
                    # Reference: OCC - standard US equity options expire at market close
                    expiry_date = expiry_dt.date()
                    et_tz = ZoneInfo("America/New_York")
                    expiry_4pm_et = datetime.combine(expiry_date, time(16, 0, 0)).replace(tzinfo=et_tz)
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
                    expiry_4pm_et = datetime.combine(expiry_date, time(16, 0, 0)).replace(tzinfo=et_tz)
                    expiry_iso = expiry_4pm_et.astimezone(timezone.utc).isoformat()
                    logger.debug(
                        f"Using 4:00 PM ET default for {exchange} {instrument_type} "
                        f"symbol {exchange_raw_symbol}: {expiry_date} -> {expiry_iso} (UTC)"
                    )
            else:
                # Databento provided time, use as-is (it's the correct expiry time for this contract)
                expiry_iso = expiry_dt.isoformat()
        except Exception as e:
            logger.warning(f"Failed to convert expiry to ISO for {exchange} {exchange_raw_symbol}: {e}")
            expiry_iso = None

    # Extract trading hours metadata using exchange-specific defaults
    # Databento doesn't provide trading hours in DEFINITION schema, so we use defaults
    # Convert to UTC for consistency with other timestamps
    # This also calculates the trading session start/end times
    trading_hours = get_exchange_trading_hours(adapter, exchange, instrument_type, target_date=target_date)

    # Handle available_from_datetime and available_to_datetime
    # For TradFi instruments, these should reflect the actual trading session times
    # IMPORTANT: Databento's ts_event is the timestamp when DATABENTO received the definition,
    # NOT the historical date we're querying for. So we MUST use target_date-based trading hours.
    # Priority: trading session start (based on target_date) > target_date start > ts_event (last resort)
    available_from = None

    # First priority: trading session start time (calculated from target_date)
    if trading_hours.get("session_start_utc"):
        available_from = trading_hours["session_start_utc"]
    elif target_date:
        # Second priority: target date start (00:00:00 UTC)
        target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if target_date_start.tzinfo is None:
            target_date_start = target_date_start.replace(tzinfo=timezone.utc)
        available_from = target_date_start.isoformat()
    elif "ts_event" in row and pd.notna(row["ts_event"]):
        # Last resort: ts_event from Databento (WARNING: this is today's date, not historical!)
        try:
            ts_event = row["ts_event"]
            if isinstance(ts_event, pd.Timestamp):
                available_from = ts_event.isoformat()
            elif isinstance(ts_event, str):
                available_from = pd.to_datetime(ts_event, utc=True).isoformat()
            logger.warning("Using ts_event for available_from_datetime - this may be today's date, not target date")
        except Exception as e:
            logger.warning(f"Failed to parse ts_event: {e}")

    # Final fallback
    if not available_from:
        # Fallback to current UTC time (should not happen in normal flow)
        available_from = datetime.now(timezone.utc).isoformat()
        logger.warning("No target_date provided and no ts_event, using current UTC time for available_from_datetime")

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
        "contract_size": (row.get("contract_size", None) if pd.notna(row.get("contract_size")) else None),
        "underlying": base_asset,  # Human-readable underlying
        "strike": (strike_price if instrument_type == "OPTION" else ""),  # Strike price for options
        "option_type": (option_type if instrument_type == "OPTION" else ""),  # CALL or PUT for options
        # Trading hours metadata (TradFi only)
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


def get_exchange_trading_hours(
    adapter: Any,
    exchange: str,
    instrument_type: str,
    target_date: Optional[datetime] = None,
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
    # Uniform format for all 5 TradFi venues: regular open/close + auction open/close
    # Auction times are static per venue (only vary by DST).
    trading_hours_map = {
        "CME": {
            "open_local": "17:00:00",  # 5:00 PM CT (previous day for next-day trading)
            "close_local": "16:00:00",  # 4:00 PM CT
            "timezone": "America/Chicago",  # Central Time
            "session": "regular",
            "holiday_calendar": "CME",
            # CME electronic (Globex) has no formal opening/closing auction
            # but has a settlement window around 4:00 PM CT
            "auction_open_local": None,
            "auction_close_local": None,
        },
        "ICE": {
            "open_local": "20:00:00",  # 8:00 PM ET (previous day for next-day trading)
            "close_local": "17:00:00",  # 5:00 PM ET
            "timezone": "America/New_York",  # Eastern Time
            "session": "regular",
            "holiday_calendar": "ICE",
            # ICE has no formal opening/closing auction for futures
            "auction_open_local": None,
            "auction_close_local": None,
        },
        "CBOE": {
            "open_local": "09:30:00",  # 9:30 AM ET
            "close_local": "16:15:00",  # 4:15 PM ET (VIX index trading hours)
            "timezone": "America/New_York",  # Eastern Time
            "session": "regular",
            "holiday_calendar": "CBOE",
            # CBOE has opening rotation and closing auction
            "auction_open_local": "09:28:00",  # Opening rotation 9:28 AM ET
            "auction_close_local": "16:00:00",  # Closing auction starts at 4:00 PM ET
        },
        "NASDAQ": {
            "open_local": "09:30:00",  # 9:30 AM ET
            "close_local": "16:00:00",  # 4:00 PM ET
            "timezone": "America/New_York",  # Eastern Time (DST-aware)
            "session": "regular",
            "holiday_calendar": "NASDAQ",
            # NASDAQ opening cross at 9:28 AM, closing cross at 3:50 PM
            "auction_open_local": "09:28:00",
            "auction_close_local": "15:50:00",
        },
        "NYSE": {
            "open_local": "09:30:00",  # 9:30 AM ET
            "close_local": "16:00:00",  # 4:00 PM ET
            "timezone": "America/New_York",  # Eastern Time (DST-aware)
            "session": "regular",
            "holiday_calendar": "NYSE",
            # NYSE opening auction at 9:28 AM, closing auction (MOC/LOC) at 3:50 PM
            "auction_open_local": "09:28:00",
            "auction_close_local": "15:50:00",
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
            "session_start_utc": None,
            "session_end_utc": None,
            "regular_open_utc": None,
            "regular_close_utc": None,
            "auction_open_utc": None,
            "auction_close_utc": None,
            "early_close_utc": None,
        }

    # Convert local time to UTC
    open_utc = None
    close_utc = None
    auction_open_utc_iso = None
    auction_close_utc_iso = None
    early_close_utc_iso = None

    try:
        # Get timezone object for DST-aware conversion
        exchange_tz = ZoneInfo(hours_config["timezone"])

        # Parse time components (format: "HH:MM:SS")
        open_time_str = hours_config["open_local"]
        close_time_str = hours_config["close_local"]

        open_parts = open_time_str.split(":")
        close_parts = close_time_str.split(":")
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
        # ICE: opens at 8pm ET previous day, closes at 5pm ET same day
        # CBOE/NASDAQ/NYSE: opens and closes same day

        # Check if open time is before close time (same day) or after (previous day)
        open_time_of_day = open_hour * 3600 + open_min * 60 + open_sec
        close_time_of_day = close_hour * 3600 + close_min * 60 + close_sec

        # If open time is after close time, it's on the previous day
        open_date = target_date.date()
        if open_time_of_day > close_time_of_day:
            open_date = open_date - timedelta(days=1)

        # Create datetime objects in exchange local timezone (DST-aware)
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

        # Compute auction times in UTC (if venue has auctions)
        auction_open_local_str = hours_config.get("auction_open_local")
        auction_close_local_str = hours_config.get("auction_close_local")

        if auction_open_local_str:
            ao_parts = auction_open_local_str.split(":")
            ao_h, ao_m, ao_s = int(ao_parts[0]), int(ao_parts[1]), int(ao_parts[2])
            # Auction open is always on target_date (same day as regular session for equity venues)
            ao_local_dt = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                ao_h,
                ao_m,
                ao_s,
                tzinfo=exchange_tz,
            )
            auction_open_utc_iso = ao_local_dt.astimezone(timezone.utc).isoformat()

        if auction_close_local_str:
            ac_parts = auction_close_local_str.split(":")
            ac_h, ac_m, ac_s = int(ac_parts[0]), int(ac_parts[1]), int(ac_parts[2])
            ac_local_dt = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                ac_h,
                ac_m,
                ac_s,
                tzinfo=exchange_tz,
            )
            auction_close_utc_iso = ac_local_dt.astimezone(timezone.utc).isoformat()

        # Check if it's a holiday
        is_holiday = adapter._is_trading_holiday(target_date.date(), hours_config.get("holiday_calendar"))

        # For CME/ICE: Sunday is NOT a holiday but is NOT a trading day
        is_trading_day = not is_holiday
        holiday_calendar = hours_config.get("holiday_calendar")
        if holiday_calendar in ("CME", "ICE"):
            weekday = target_date.date().weekday()
            if weekday == 6:  # Sunday
                is_trading_day = False

        # If holiday, set trading hours to "holiday"
        if is_holiday:
            open_utc = "holiday"
            close_utc = "holiday"

        # Check for early close days (e.g., day after Thanksgiving, Christmas Eve)
        if is_trading_day and not is_holiday:
            xcal = adapter._get_exchange_calendar(hours_config.get("holiday_calendar"))
            if xcal:
                try:
                    ts = pd.Timestamp(target_date.date())
                    if hasattr(xcal, "early_closes") and ts in xcal.early_closes:
                        if ts in xcal.schedule.index:
                            actual_close = xcal.schedule.loc[ts, "close"]
                            if pd.notna(actual_close):
                                early_close_utc_dt = (
                                    actual_close.tz_convert(timezone.utc)
                                    if actual_close.tzinfo
                                    else actual_close.replace(tzinfo=timezone.utc)
                                )
                                # Update close time to early close
                                close_utc = early_close_utc_dt.strftime("%H:%M:%S+00:00")
                                close_utc_dt = early_close_utc_dt
                                session_end_utc = early_close_utc_dt.isoformat()
                                early_close_utc_iso = early_close_utc_dt.isoformat()
                                logger.info(
                                    f"Early close detected for {exchange} on {target_date.date()}: "
                                    f"closes at {close_utc} UTC"
                                )
                                # On early close days, closing auction may also shift
                                # Nullify auction_close if it would be after early close
                                if auction_close_utc_iso:
                                    ac_dt = datetime.fromisoformat(auction_close_utc_iso)
                                    if ac_dt >= early_close_utc_dt:
                                        auction_close_utc_iso = None
                except Exception as early_close_err:
                    logger.debug(f"Early close check not available for {exchange}: {early_close_err}")

        # Build regular_open_utc / regular_close_utc ISO strings
        regular_open_utc_iso = session_start_utc if not is_holiday else None
        regular_close_utc_iso = session_end_utc if not is_holiday else None

        # Nullify auction times on non-trading days
        if not is_trading_day or is_holiday:
            auction_open_utc_iso = None
            auction_close_utc_iso = None
            early_close_utc_iso = None

    except Exception as e:
        logger.warning(f"Failed to convert trading hours to UTC for {exchange}: {e}")
        open_utc = hours_config.get("open_local")
        close_utc = hours_config.get("close_local")
        session_start_utc = None
        session_end_utc = None
        is_holiday = False
        regular_open_utc_iso = None
        regular_close_utc_iso = None

    return {
        "open": open_utc,
        "close": close_utc,
        "session": hours_config.get("session", "regular"),
        "is_trading_day": (
            is_trading_day if "is_trading_day" in locals() else (not is_holiday if "is_holiday" in locals() else None)
        ),
        "holiday_calendar": hours_config.get("holiday_calendar"),
        "session_start_utc": session_start_utc if "session_start_utc" in locals() else None,
        "session_end_utc": session_end_utc if "session_end_utc" in locals() else None,
        # New uniform session boundary fields
        "regular_open_utc": regular_open_utc_iso if "regular_open_utc_iso" in locals() else None,
        "regular_close_utc": regular_close_utc_iso if "regular_close_utc_iso" in locals() else None,
        "auction_open_utc": auction_open_utc_iso,
        "auction_close_utc": auction_close_utc_iso,
        "early_close_utc": early_close_utc_iso,
    }
