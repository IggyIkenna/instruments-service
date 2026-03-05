"""
Symbol Parser

Parses base/quote assets and option components from Tardis symbol IDs.
Extracted from InstrumentProcessingService for file-size compliance (COD-SIZE).
"""

import logging
import re
from typing import TYPE_CHECKING, TypedDict
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

if TYPE_CHECKING:
    from unified_config_interface import ExchangeInstrumentConfig

logger = logging.getLogger(__name__)


class SymbolComponents(TypedDict):
    """Symbol parsing results with base and quote assets."""

    base_asset: str
    quote_asset: str


class OptionComponents(TypedDict):
    """Option parsing results with expiry, strike, and option type."""

    expiry_date: str
    strike_price: str
    option_type: str


class SymbolParser:
    """Parse symbol components for various exchange formats."""

    def __init__(self, exchange_config: "ExchangeInstrumentConfig") -> None:
        self.exchange_config = exchange_config

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> SymbolComponents:
        """Parse base/quote assets from Tardis symbol ID."""
        try:

            def remove_suffix(s: str) -> tuple[str, str | None]:
                if exchange in ["binance", "binance-futures"]:
                    if s.startswith("1000") and len(s) > 4:
                        pass
                    elif s and s[0].isdigit():
                        return "", ""
                    if s.upper().startswith("USDT") and len(s) > 4:
                        usdt_suffixes = ["TRY", "ARS", "BRL", "PLN", "UAH", "CZK", "RON", "NGN", "ZAR"]
                        if any(s.upper().endswith(f"USDT{suffix}") for suffix in usdt_suffixes):
                            return "", ""

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
                    "DOGE",
                    "SHIB",
                    "PEPE",
                    "FLOKI",
                    "WIF",
                    "BONK",
                    "MEME",
                    "BABYDOGE",
                ]
                suffixes.sort(key=len, reverse=True)
                for suffix in suffixes:
                    if s.upper().endswith(suffix):
                        base = s[: -len(suffix)]
                        if (
                            exchange in ["binance", "binance-futures"]
                            and base
                            and ((base[0].isdigit() and base[0:3] != "1000") or base.upper() == "USDT")
                        ):
                            return "", ""
                        return base, suffix
                return s, None

            if exchange == "deribit":
                if symbol_id.endswith("-PERPETUAL"):
                    first_part = symbol_id.replace("-PERPETUAL", "")
                    if "_" in first_part:
                        parts = first_part.split("_")
                        return {"base_asset": parts[0], "quote_asset": parts[1]}
                    return {"base_asset": first_part, "quote_asset": "USD"}
                elif "_" in symbol_id and not any(x in symbol_id for x in ["-", "PERPETUAL"]):
                    parts = symbol_id.split("_")
                    if len(parts) == 2:
                        return {"base_asset": parts[0], "quote_asset": parts[1]}
                elif "-" in symbol_id:
                    parts = symbol_id.split("-")
                    first_part = parts[0]
                    base_currency, detected_quote = remove_suffix(first_part)
                    base_currency = base_currency.rstrip("_")
                    deribit_valid_quotes = self.exchange_config.valid_quote_currencies.get("DERIBIT", ["USD"])
                    if detected_quote in deribit_valid_quotes and detected_quote in first_part.upper():
                        return {"base_asset": base_currency, "quote_asset": detected_quote}
                    return {"base_asset": first_part, "quote_asset": "USD"}

            elif exchange in ["bybit", "bybit-spot", "binance", "binance-futures"]:
                if exchange == "binance-futures" and "_" in symbol_id:
                    base_part = symbol_id.split("_")[0].upper()
                    for quote in ["USDT", "USDC", "BTC", "ETH", "BNB"]:
                        if base_part.endswith(quote):
                            base = base_part[: -len(quote)]
                            if base and len(base) >= 2:
                                return {"base_asset": base, "quote_asset": quote}
                clean_symbol = symbol_id.replace("PERP", "").upper()
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
                    "DOGE",
                    "SHIB",
                    "PEPE",
                    "FLOKI",
                    "WIF",
                    "BONK",
                    "MEME",
                    "BABYDOGE",
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
                crypto_quotes.sort(key=len, reverse=True)
                for quote in crypto_quotes:
                    if clean_symbol.endswith(quote):
                        base = clean_symbol[: -len(quote)]
                        if base and len(base) >= 2:
                            if exchange in ["binance", "binance-futures"] and (base[0].isdigit() or base == "USDT"):
                                return {"base_asset": "", "quote_asset": ""}
                            return {"base_asset": base, "quote_asset": quote}
                base_currency, detected_quote = remove_suffix(clean_symbol)
                base_currency = base_currency.replace("PERP", "").strip()
                if base_currency and detected_quote:
                    if exchange in ["binance", "binance-futures"] and (
                        (base_currency[0].isdigit() and "1000" not in base_currency)
                        or base_currency == "USDT"
                        or "USDT" in base_currency
                    ):
                        return {"base_asset": "", "quote_asset": ""}
                    return {"base_asset": base_currency, "quote_asset": detected_quote}
                return {"base_asset": clean_symbol, "quote_asset": "USDT"}

            elif exchange == "upbit":
                if "-" in symbol_id:
                    parts = symbol_id.upper().split("-")
                    if len(parts) == 2:
                        return {"base_asset": parts[1], "quote_asset": parts[0]}
                return {"base_asset": symbol_id, "quote_asset": "KRW"}

            elif exchange == "coinbase":
                if "-" in symbol_id:
                    parts = symbol_id.upper().split("-")
                    if len(parts) == 2:
                        return {"base_asset": parts[0], "quote_asset": parts[1]}
                return {"base_asset": symbol_id, "quote_asset": "USD"}

            elif exchange in ["okx", "okex", "okex-futures", "okex-swap"]:
                if symbol_id.startswith("PERP-"):
                    quote_part = symbol_id[5:]
                    if quote_part in ["USDT", "USDC", "USD"]:
                        return {"base_asset": "PERP", "quote_asset": quote_part}
                clean_id = symbol_id.replace("PERP", "").replace("SWAP", "").replace("-SWAP", "")
                base_currency, detected_quote = remove_suffix(clean_id)
                base_currency = base_currency.strip("-_")
                if base_currency and detected_quote:
                    return {"base_asset": base_currency, "quote_asset": detected_quote}
                if "-" in clean_id:
                    parts = clean_id.split("-")
                    if len(parts) >= 2 and parts[0] and parts[1]:
                        return {
                            "base_asset": parts[0].strip("-"),
                            "quote_asset": parts[1].strip("-"),
                        }
                return {
                    "base_asset": base_currency or clean_id,
                    "quote_asset": detected_quote or "USD",
                }

            base_currency, detected_quote = remove_suffix(symbol_id)
            return {"base_asset": base_currency, "quote_asset": detected_quote or ""}

        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Symbol parsing error for %s: %s", symbol_id, e)
            return {"base_asset": "", "quote_asset": ""}

    def parse_option_components(self, symbol_id: str, exchange: str) -> OptionComponents:
        """Parse option expiry, strike, and type."""
        try:
            if exchange == "deribit":
                option_strike_patterns = [
                    re.compile(r"-(\d{6})-(\d+)-(CALL|PUT)$"),
                    re.compile(r"-(\d+)-(C|P)$"),
                    re.compile(r"-(\d+d?\d*)-"),
                ]
                option_type_patterns = [
                    re.compile(r"-(CALL|PUT)$"),
                    re.compile(r"-(C|P)$"),
                ]
                strike_price = ""
                for i, pattern in enumerate(option_strike_patterns):
                    strike_match = pattern.search(symbol_id)
                    if strike_match:
                        strike_raw = strike_match.group(2) if i == 0 else strike_match.group(1)
                        strike_price = strike_raw.replace("d", ".") if "d" in strike_raw else strike_raw
                        break
                option_type = ""
                for pattern in option_type_patterns:
                    option_type_match = pattern.search(symbol_id)
                    if option_type_match:
                        match_text = option_type_match.group(1)
                        option_type = "CALL" if match_text in ["CALL", "C"] else "PUT"
                        break
                expiry_patterns = [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),
                    re.compile(r"-(\d{1}[A-Z]{3}\d{2})-"),
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),
                    re.compile(r"-(\d{6})$"),
                ]
                expiry_date = ""
                for pattern in expiry_patterns:
                    match = pattern.search(symbol_id)
                    if match:
                        expiry_date = self.parse_deribit_date(match.group(1))
                        break
                return {
                    "expiry_date": expiry_date,
                    "strike_price": strike_price,
                    "option_type": option_type,
                }
            return {"expiry_date": "", "strike_price": "", "option_type": ""}
        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Option parsing error for %s: %s", symbol_id, e)
            return {"expiry_date": "", "strike_price": "", "option_type": ""}

    def parse_deribit_date(self, date_str: str) -> str:
        """Parse Deribit date format: 25DEC25 → 2025-12-25T08:00:00Z."""
        try:
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
                return f"20{year}-{month}-{day.zfill(2)}T08:00:00Z"
        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.HIGH,
                recovery_strategy=ErrorRecoveryStrategy.RETRY,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
            raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e
        return "2025-12-25T08:00:00Z"

    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None:
        """Parse expiry from symbol using exchange-specific patterns."""
        try:
            expiry_patterns: dict[str, list[re.Pattern[str]]] = {
                "deribit": [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),
                    re.compile(r"-(\d{1}[A-Z]{3}\d{2})-"),
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),
                    re.compile(r"-(\d{6})$"),
                ],
                "binance": [
                    re.compile(r"_(\d{6})$"),
                    re.compile(r"-(\d{6})$"),
                ],
                "binance-futures": [
                    re.compile(r"_(\d{6})$"),
                    re.compile(r"-(\d{6})$"),
                ],
                "bybit": [
                    re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),
                    re.compile(r"([A-Z])(\d{2})$"),
                ],
                "okex-futures": [
                    re.compile(r"-(\d{6})$"),
                ],
            }
            deribit_patterns = expiry_patterns.get("deribit")
            if deribit_patterns is None:
                raise ValueError("expiry_patterns must have 'deribit' key")
            exchange_patterns = expiry_patterns.get(exchange)
            patterns = exchange_patterns if exchange_patterns is not None else deribit_patterns
            for pattern in patterns:
                match = pattern.search(symbol_id)
                if match:
                    expiry_raw = match.group(1)
                    if re.match(r"\d{6}", expiry_raw):
                        year = f"20{expiry_raw[:2]}"
                        month = expiry_raw[2:4]
                        day = expiry_raw[4:6]
                        return f"{year}-{month}-{day}T08:00:00Z"
                    return self.parse_deribit_date(expiry_raw)
            return None
        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Expiry parsing error for %s: %s", symbol_id, e)
            return None
