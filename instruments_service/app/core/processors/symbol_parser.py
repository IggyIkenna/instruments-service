"""
Symbol Parser

Parses base/quote assets and option components from Tardis symbol IDs.
Extracted from InstrumentProcessingService for file-size compliance (COD-SIZE).
"""

import logging
import re
from typing import TYPE_CHECKING
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

if TYPE_CHECKING:
    from unified_api_contracts import ExchangeInstrumentConfig

logger = logging.getLogger(__name__)

# Shared suffix lists extracted at module level for reuse
_ALL_QUOTE_SUFFIXES: list[str] = sorted(
    [
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
    ],
    key=len,
    reverse=True,
)

_CRYPTO_QUOTE_CURRENCIES: list[str] = sorted(
    [
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
    ],
    key=len,
    reverse=True,
)

_USDT_FIAT_SUFFIXES: list[str] = ["TRY", "ARS", "BRL", "PLN", "UAH", "CZK", "RON", "NGN", "ZAR"]

_EXPIRY_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "deribit": [
        re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),
        re.compile(r"-(\d{1}[A-Z]{3}\d{2})-"),
        re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),
        re.compile(r"-(\d{6})$"),
    ],
    "binance": [re.compile(r"_(\d{6})$"), re.compile(r"-(\d{6})$")],
    "binance-futures": [re.compile(r"_(\d{6})$"), re.compile(r"-(\d{6})$")],
    "bybit": [re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"), re.compile(r"([A-Z])(\d{2})$")],
    "okex-futures": [re.compile(r"-(\d{6})$")],
}


class SymbolParser:
    """Parse symbol components for various exchange formats."""

    def __init__(self, exchange_config: "ExchangeInstrumentConfig") -> None:
        self.exchange_config = exchange_config

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse base/quote assets from Tardis symbol ID."""
        try:
            if exchange == "deribit":
                return self._parse_deribit(symbol_id, exchange)
            elif exchange in ["bybit", "bybit-spot", "binance", "binance-futures"]:
                return self._parse_binance_bybit(symbol_id, exchange)
            elif exchange == "upbit":
                return self._parse_upbit(symbol_id)
            elif exchange == "coinbase":
                return self._parse_coinbase(symbol_id)
            elif exchange in ["okx", "okex", "okex-futures", "okex-swap"]:
                return self._parse_okx(symbol_id, exchange)
            base_currency, detected_quote = self._remove_suffix(symbol_id, exchange)
            return {"base_asset": base_currency, "quote_asset": detected_quote}
        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Symbol parsing error for %s: %s", symbol_id, e)
            return {"base_asset": "", "quote_asset": ""}

    def _remove_suffix(self, s: str, exchange: str) -> tuple[str, str | None]:
        """Remove known quote currency suffix from concatenated symbol string."""
        if exchange in ["binance", "binance-futures"]:
            if s.startswith("1000") and len(s) > 4:
                pass
            elif s and s[0].isdigit():
                return "", ""
            if (
                s.upper().startswith("USDT")
                and len(s) > 4
                and any(s.upper().endswith(f"USDT{suffix}") for suffix in _USDT_FIAT_SUFFIXES)
            ):
                return "", ""

        for suffix in _ALL_QUOTE_SUFFIXES:
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

    def _parse_deribit(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse Deribit symbol components."""
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
            base_currency, detected_quote = self._remove_suffix(first_part, exchange)
            base_currency = base_currency.rstrip("_")
            deribit_valid_quotes = self.exchange_config.valid_quote_currencies.get("DERIBIT", ["USD"])
            if detected_quote in deribit_valid_quotes and detected_quote in first_part.upper():
                return {"base_asset": base_currency, "quote_asset": detected_quote}
            return {"base_asset": first_part, "quote_asset": "USD"}

        base_currency, detected_quote = self._remove_suffix(symbol_id, exchange)
        return {"base_asset": base_currency, "quote_asset": detected_quote}

    def _parse_binance_bybit(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse Binance/Bybit symbol components."""
        if exchange == "binance-futures" and "_" in symbol_id:
            result = self._parse_binance_futures_underscore(symbol_id)
            if result:
                return result

        clean_symbol = symbol_id.replace("PERP", "").upper()
        result = self._match_crypto_quote(clean_symbol, exchange)
        if result:
            return result

        base_currency, detected_quote = self._remove_suffix(clean_symbol, exchange)
        base_currency = base_currency.replace("PERP", "").strip()
        if base_currency and detected_quote:
            if self._is_binance_invalid_base(base_currency, exchange):
                return {"base_asset": "", "quote_asset": ""}
            return {"base_asset": base_currency, "quote_asset": detected_quote}
        return {"base_asset": clean_symbol, "quote_asset": "USDT"}

    @staticmethod
    def _parse_binance_futures_underscore(symbol_id: str) -> dict[str, object] | None:
        """Parse binance-futures symbols with underscore separator."""
        base_part = symbol_id.split("_")[0].upper()
        for quote in ["USDT", "USDC", "BTC", "ETH", "BNB"]:
            if base_part.endswith(quote):
                base = base_part[: -len(quote)]
                if base and len(base) >= 2:
                    return {"base_asset": base, "quote_asset": quote}
        return None

    @staticmethod
    def _match_crypto_quote(clean_symbol: str, exchange: str) -> dict[str, object] | None:
        """Match a crypto quote currency suffix and return parsed components."""
        for quote in _CRYPTO_QUOTE_CURRENCIES:
            if clean_symbol.endswith(quote):
                base = clean_symbol[: -len(quote)]
                if base and len(base) >= 2:
                    if exchange in ["binance", "binance-futures"] and (base[0].isdigit() or base == "USDT"):
                        return {"base_asset": "", "quote_asset": ""}
                    return {"base_asset": base, "quote_asset": quote}
        return None

    @staticmethod
    def _is_binance_invalid_base(base_currency: str, exchange: str) -> bool:
        """Check if a Binance base currency is invalid (digit-start or USDT)."""
        if exchange not in ["binance", "binance-futures"]:
            return False
        return (
            (base_currency[0].isdigit() and "1000" not in base_currency)
            or base_currency == "USDT"
            or "USDT" in base_currency
        )

    @staticmethod
    def _parse_upbit(symbol_id: str) -> dict[str, object]:
        """Parse Upbit symbol components."""
        if "-" in symbol_id:
            parts = symbol_id.upper().split("-")
            if len(parts) == 2:
                return {"base_asset": parts[1], "quote_asset": parts[0]}
        return {"base_asset": symbol_id, "quote_asset": "KRW"}

    @staticmethod
    def _parse_coinbase(symbol_id: str) -> dict[str, object]:
        """Parse Coinbase symbol components."""
        if "-" in symbol_id:
            parts = symbol_id.upper().split("-")
            if len(parts) == 2:
                return {"base_asset": parts[0], "quote_asset": parts[1]}
        return {"base_asset": symbol_id, "quote_asset": "USD"}

    def _parse_okx(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse OKX/OKEx symbol components."""
        if symbol_id.startswith("PERP-"):
            quote_part = symbol_id[5:]
            if quote_part in ["USDT", "USDC", "USD"]:
                return {"base_asset": "PERP", "quote_asset": quote_part}
        clean_id = symbol_id.replace("PERP", "").replace("SWAP", "").replace("-SWAP", "")
        base_currency, detected_quote = self._remove_suffix(clean_id, exchange)
        base_currency = base_currency.strip("-_")
        if base_currency and detected_quote:
            return {"base_asset": base_currency, "quote_asset": detected_quote}
        if "-" in clean_id:
            parts = clean_id.split("-")
            if len(parts) >= 2 and parts[0] and parts[1]:
                return {"base_asset": parts[0].strip("-"), "quote_asset": parts[1].strip("-")}
        return {"base_asset": base_currency or clean_id, "quote_asset": detected_quote or "USD"}

    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse option expiry, strike, and type."""
        try:
            if exchange == "deribit":
                return self._parse_deribit_option(symbol_id)
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
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Option parsing error for %s: %s", symbol_id, e)
            return {"expiry_date": "", "strike_price": "", "option_type": ""}

    def _parse_deribit_option(self, symbol_id: str) -> dict[str, object]:
        """Extract strike, option type, and expiry from a Deribit option symbol."""
        strike_patterns = [
            re.compile(r"-(\d{6})-(\d+)-(CALL|PUT)$"),
            re.compile(r"-(\d+)-(C|P)$"),
            re.compile(r"-(\d+d?\d*)-"),
        ]
        strike_price = ""
        for i, pattern in enumerate(strike_patterns):
            strike_match = pattern.search(symbol_id)
            if strike_match:
                strike_raw = strike_match.group(2) if i == 0 else strike_match.group(1)
                strike_price = strike_raw.replace("d", ".") if "d" in strike_raw else strike_raw
                break
        type_patterns = [re.compile(r"-(CALL|PUT)$"), re.compile(r"-(C|P)$")]
        option_type = ""
        for pattern in type_patterns:
            m = pattern.search(symbol_id)
            if m:
                option_type = "CALL" if m.group(1) in ["CALL", "C"] else "PUT"
                break
        expiry_date = self._extract_deribit_expiry(symbol_id)
        return {"expiry_date": expiry_date, "strike_price": strike_price, "option_type": option_type}

    def _extract_deribit_expiry(self, symbol_id: str) -> str:
        """Extract expiry date string from a Deribit symbol."""
        expiry_patterns = [
            re.compile(r"-(\d{2}[A-Z]{3}\d{2})-"),
            re.compile(r"-(\d{1}[A-Z]{3}\d{2})-"),
            re.compile(r"-(\d{2}[A-Z]{3}\d{2})$"),
            re.compile(r"-(\d{6})$"),
        ]
        for pattern in expiry_patterns:
            match = pattern.search(symbol_id)
            if match:
                return self.parse_deribit_date(match.group(1))
        return ""

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
            patterns = _EXPIRY_PATTERNS.get(exchange) or _EXPIRY_PATTERNS.get("deribit") or []
            return self._match_expiry_pattern(symbol_id, patterns)
        except (ValueError, KeyError, TypeError, IndexError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.debug("⚠️ Expiry parsing error for %s: %s", symbol_id, e)
            return None

    def _match_expiry_pattern(
        self,
        symbol_id: str,
        patterns: list[re.Pattern[str]],
    ) -> str | None:
        """Try each pattern against symbol_id and return parsed expiry or None."""
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
