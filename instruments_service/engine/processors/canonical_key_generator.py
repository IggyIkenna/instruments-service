"""
Canonical Key Generator

Generates canonical instrument keys following INSTRUMENT_KEY.md specification.
Extracted from InstrumentProcessingService.generate_canonical_key.
"""

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, TypedDict

if TYPE_CHECKING:
    from unified_api_contracts import DataTypeConfig, ExchangeInstrumentConfig

logger = logging.getLogger(__name__)


class SymbolComponents(TypedDict):  # CORRECT-LOCAL: private processor-internal parsing result, not a domain contract
    """Symbol parsing results with base and quote assets."""

    base_asset: str
    quote_asset: str


class OptionComponents(TypedDict):  # CORRECT-LOCAL: private processor-internal parsing result, not a domain contract
    """Option parsing results with expiry, strike, and option type."""

    expiry_date: str
    strike_price: str
    option_type: str


class SymbolInfo(TypedDict, total=False):
    """Symbol information from external sources (e.g., CCXT, Tardis)."""

    base_asset: str | None
    quote_asset: str | None
    expiry_date: str | None
    strike_price: str | float | None
    option_type: str | None


class CanonicalKeyServiceProtocol(Protocol):
    """Protocol for InstrumentProcessingService used by generate_canonical_key."""

    def normalize_venue(self, exchange: str) -> str | None: ...
    def normalize_instrument_type(self, symbol_type: str) -> str | None: ...
    def parse_symbol_components(self, symbol_id: str, exchange: str) -> SymbolComponents: ...
    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None: ...
    def parse_option_components(self, symbol_id: str, exchange: str) -> OptionComponents: ...

    exchange_config: "ExchangeInstrumentConfig"
    data_config: "DataTypeConfig"


def _resolve_settle_and_flavor(
    venue: str,
    clean_base: str,
    clean_quote: str,
    deribit_quotes: list[str],
) -> str:
    """Determine settlement flavor (@LIN or @INV) for derivatives."""
    settle_asset = "USDT"
    if venue == "DERIBIT":
        if clean_quote == "USD":
            settle_asset = clean_base
        elif clean_quote in deribit_quotes and clean_quote != "USD":
            settle_asset = clean_quote
    else:
        settle_asset = clean_quote

    if settle_asset == clean_quote:
        return "LIN"
    elif settle_asset == clean_base:
        return "INV"
    return "LIN"


def _format_expiry_str(expiry_date: str | float | None) -> str:
    """Convert expiry date (ISO string or raw) to YYMMDD format."""
    if isinstance(expiry_date, str):
        try:
            expiry_dt = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
            return expiry_dt.strftime("%y%m%d")
        except (ValueError, AttributeError):
            return expiry_date
    return str(expiry_date)


def _get_deribit_quotes(service: CanonicalKeyServiceProtocol) -> list[str]:
    """Retrieve and validate the DERIBIT quote currencies list."""
    deribit_raw = service.exchange_config.valid_quote_currencies.get("DERIBIT")
    if deribit_raw is None:
        raise ValueError("valid_quote_currencies must have entry for DERIBIT")
    if not isinstance(deribit_raw, list):
        raise TypeError(f"valid_quote_currencies[DERIBIT] must be list, got {type(deribit_raw).__name__}")
    return list(deribit_raw)


def _resolve_base_quote(
    service: CanonicalKeyServiceProtocol,
    symbol_id: str,
    exchange: str,
    symbol_info: SymbolInfo,
) -> tuple[str, str] | None:
    """Resolve base/quote from symbol_info, falling back to parser."""
    base_asset = str(symbol_info.get("base_asset") or "").upper()
    quote_asset = str(symbol_info.get("quote_asset") or "").upper()

    if not base_asset or not quote_asset:
        parsed = service.parse_symbol_components(symbol_id, exchange)
        base_asset = str(parsed.get("base_asset") or "").upper()
        quote_asset = str(parsed.get("quote_asset") or "").upper()

    if not base_asset or not quote_asset:
        return None
    return base_asset, quote_asset


def _generate_spot_key(venue: str, base: str, quote: str) -> str | None:
    """Generate canonical key for SPOT_PAIR."""
    clean_base = base.strip()
    clean_quote = quote.strip()
    if not clean_base or not clean_quote:
        return None
    return f"{venue}:SPOT_PAIR:{clean_base}-{clean_quote}"


def _generate_perpetual_key(
    venue: str,
    base: str,
    quote: str,
    deribit_quotes: list[str],
) -> str | None:
    """Generate canonical key for PERPETUAL."""
    clean_base = "".join(c for c in base.strip() if c.isprintable() and c.isascii())
    clean_quote = "".join(c for c in quote.strip() if c.isprintable() and c.isascii())
    if not clean_base or not clean_quote:
        return None
    flavor = _resolve_settle_and_flavor(venue, clean_base, clean_quote, deribit_quotes)
    return f"{venue}:PERPETUAL:{clean_base}-{clean_quote}@{flavor}"


def _generate_future_key(
    service: CanonicalKeyServiceProtocol,
    venue: str,
    base: str,
    quote: str,
    symbol_id: str,
    exchange: str,
    symbol_info: SymbolInfo,
    deribit_quotes: list[str],
) -> str | None:
    """Generate canonical key for FUTURE."""
    expiry_date = symbol_info.get("expiry_date")
    if not expiry_date:
        expiry_date = service.parse_expiry_from_symbol(symbol_id, exchange)
    if not expiry_date:
        logger.warning("Missing expiry date for future %s exchange: %s", symbol_id, exchange)
        return None

    clean_base = base.strip()
    clean_quote = quote.strip()
    if not clean_base or not clean_quote:
        return None

    expiry_str = _format_expiry_str(expiry_date)
    if expiry_str and len(expiry_str) > 6:
        match = re.search(r"(\d{6})", expiry_str)
        if match:
            expiry_str = match.group(1)

    flavor = _resolve_settle_and_flavor(venue, clean_base, clean_quote, deribit_quotes)
    return f"{venue}:FUTURE:{clean_base}-{clean_quote}-{expiry_str}@{flavor}"


def _generate_option_key(
    service: CanonicalKeyServiceProtocol,
    venue: str,
    base: str,
    quote: str,
    symbol_id: str,
    exchange: str,
    symbol_info: SymbolInfo,
    deribit_quotes: list[str],
) -> str | None:
    """Generate canonical key for OPTION."""
    excluded_strategies: list[str] = service.data_config.excluded_deribit_strategies
    if exchange == "deribit" and any(strategy in symbol_id for strategy in excluded_strategies):
        return None

    expiry_date = symbol_info.get("expiry_date")
    strike_price = symbol_info.get("strike_price")
    option_type: str = str(symbol_info.get("option_type") or "").upper()

    if not all([expiry_date, strike_price, option_type]):
        parsed_option = service.parse_option_components(symbol_id, exchange)
        expiry_date = expiry_date or parsed_option.get("expiry_date")
        strike_price = strike_price or parsed_option.get("strike_price")
        option_type = option_type or str(parsed_option.get("option_type") or "").upper()

    if not all([expiry_date, strike_price, option_type]):
        logger.warning("Missing option parameters for %s", symbol_id)
        return None

    expiry_str = _format_expiry_str(expiry_date)
    flavor = _resolve_settle_and_flavor(venue, base.upper(), quote.upper(), deribit_quotes)
    return f"{venue}:OPTION:{base}-{quote}-{expiry_str}-{strike_price}-{option_type}@{flavor}"


def generate_canonical_key(
    service: CanonicalKeyServiceProtocol,
    exchange: str,
    symbol_type: str,
    symbol_id: str,
    symbol_info: SymbolInfo,
) -> str | None:
    """
    Generate canonical instrument key following INSTRUMENT_KEY.md specification.

    Args:
        service: InstrumentProcessingService instance for delegation
        exchange: Exchange name (e.g., 'binance', 'deribit')
        symbol_type: Symbol type ('spot', 'perpetual', 'future', 'option')
        symbol_id: Symbol identifier
        symbol_info: Additional symbol information

    Returns:
        Canonical instrument key in format: VENUE:INSTRUMENT_TYPE:SYMBOL_SPEC
    """
    venue = service.normalize_venue(exchange)
    instrument_type = service.normalize_instrument_type(symbol_type)
    if not venue or not instrument_type:
        return None

    resolved = _resolve_base_quote(service, symbol_id, exchange, symbol_info)
    if resolved is None:
        return None
    base_asset, quote_asset = resolved

    deribit_quotes = _get_deribit_quotes(service)

    if instrument_type == "SPOT_PAIR":
        return _generate_spot_key(venue, base_asset, quote_asset)
    elif instrument_type == "PERPETUAL":
        return _generate_perpetual_key(venue, base_asset, quote_asset, deribit_quotes)
    elif instrument_type == "FUTURE":
        return _generate_future_key(
            service,
            venue,
            base_asset,
            quote_asset,
            symbol_id,
            exchange,
            symbol_info,
            deribit_quotes,
        )
    elif instrument_type == "OPTION":
        return _generate_option_key(
            service,
            venue,
            base_asset,
            quote_asset,
            symbol_id,
            exchange,
            symbol_info,
            deribit_quotes,
        )
    else:
        logger.warning("Unhandled instrument type: %s", instrument_type)
        return None
