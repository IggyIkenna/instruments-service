"""
Canonical Key Generator

Generates canonical instrument keys following INSTRUMENT_KEY.md specification.
Extracted from InstrumentProcessingService.generate_canonical_key.
"""

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from unified_api_contracts import DataTypeConfig, ExchangeInstrumentConfig

logger = logging.getLogger(__name__)


class CanonicalKeyServiceProtocol(Protocol):
    """Protocol for InstrumentProcessingService used by generate_canonical_key."""

    def normalize_venue(self, exchange: str) -> str | None: ...
    def normalize_instrument_type(self, symbol_type: str) -> str | None: ...
    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, object]: ...
    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None: ...
    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, object]: ...

    exchange_config: "ExchangeInstrumentConfig"
    data_config: "DataTypeConfig"


def _resolve_settle_flavor(
    venue: str,
    clean_base: str,
    clean_quote: str,
    deribit_quotes: list[str],
    symbol_id: str,
    inst_label: str,
) -> str:
    """Determine settlement flavor (LIN/INV) for a derivative instrument."""
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
    if settle_asset == clean_base:
        return "INV"
    logger.debug(
        "Could not determine %s flavor for %s, defaulting to @LIN (settle=%s, quote=%s, base=%s)",
        inst_label,
        symbol_id,
        settle_asset,
        clean_quote,
        clean_base,
    )
    return "LIN"


def _format_expiry_str(expiry_date: object) -> str:
    """Convert an expiry date value into a 6-char YYMMDD string."""
    if isinstance(expiry_date, str):
        try:
            expiry_dt = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
            return expiry_dt.strftime("%y%m%d")
        except (ValueError, AttributeError):
            return expiry_date
    return str(cast(str | int | float, expiry_date))


def _truncate_expiry(expiry_str: str) -> str:
    """Truncate expiry string to 6-digit YYMMDD if longer."""
    if expiry_str and len(expiry_str) > 6:
        match = re.search(r"(\d{6})", expiry_str)
        if match:
            return match.group(1)
    return expiry_str


def _clean_ascii(value: str) -> str:
    """Strip and keep only printable ASCII characters."""
    return "".join(c for c in value.strip() if c.isprintable() and c.isascii())


def _generate_spot_key(
    venue: str,
    base_asset: str,
    quote_asset: str,
    symbol_id: str,
) -> str | None:
    """Generate canonical key for a SPOT_PAIR instrument."""
    clean_base = base_asset.strip()
    clean_quote = quote_asset.strip()
    if not clean_base or not clean_quote:
        logger.debug("Skipping SPOT_PAIR with missing base/quote: '%s'/'%s' for %s", base_asset, quote_asset, symbol_id)
        return None
    return f"{venue}:SPOT_PAIR:{clean_base}-{clean_quote}"


def _generate_perpetual_key(
    venue: str,
    base_asset: str,
    quote_asset: str,
    deribit_quotes: list[str],
    symbol_id: str,
) -> str | None:
    """Generate canonical key for a PERPETUAL instrument."""
    clean_base = _clean_ascii(base_asset)
    clean_quote = _clean_ascii(quote_asset)
    if not clean_base or not clean_quote:
        logger.debug("Skipping PERPETUAL with invalid base/quote: '%s'/'%s' for %s", base_asset, quote_asset, symbol_id)
        return None
    flavor = _resolve_settle_flavor(venue, clean_base, clean_quote, deribit_quotes, symbol_id, "perpetual")
    return f"{venue}:PERPETUAL:{clean_base}-{clean_quote}@{flavor}"


def _generate_future_key(
    service: CanonicalKeyServiceProtocol,
    venue: str,
    base_asset: str,
    quote_asset: str,
    deribit_quotes: list[str],
    symbol_id: str,
    exchange: str,
    symbol_info: dict[str, object],
) -> str | None:
    """Generate canonical key for a FUTURE instrument."""
    expiry_date = symbol_info.get("expiry_date")
    if not expiry_date:
        expiry_date = service.parse_expiry_from_symbol(symbol_id, exchange)
        if expiry_date:
            logger.debug("Parsed expiry from symbol: %s -> %s", symbol_id, expiry_date)
    if not expiry_date:
        logger.warning("Missing expiry date for future %s exchange: %s", symbol_id, exchange)
        return None
    expiry_str = _truncate_expiry(_format_expiry_str(expiry_date))
    clean_base = base_asset.strip()
    clean_quote = quote_asset.strip()
    if not clean_base or not clean_quote:
        logger.debug("Skipping FUTURE with missing base/quote: '%s'/'%s' for %s", base_asset, quote_asset, symbol_id)
        return None
    flavor = _resolve_settle_flavor(venue, clean_base, clean_quote, deribit_quotes, symbol_id, "future")
    return f"{venue}:FUTURE:{clean_base}-{clean_quote}-{expiry_str}@{flavor}"


def _generate_option_key(
    service: CanonicalKeyServiceProtocol,
    venue: str,
    base_asset: str,
    quote_asset: str,
    deribit_quotes: list[str],
    symbol_id: str,
    exchange: str,
    symbol_info: dict[str, object],
) -> str | None:
    """Generate canonical key for an OPTION instrument."""
    excluded_strategies: list[str] = service.data_config.excluded_deribit_strategies
    if exchange == "deribit" and any(strategy in symbol_id for strategy in excluded_strategies):
        logger.debug("Filtered complex Deribit strategy option: %s", symbol_id)
        return None
    expiry_date, strike_price, option_type = _resolve_option_params(service, symbol_id, exchange, symbol_info)
    if not all([expiry_date, strike_price, option_type]):
        logger.warning("Missing option parameters for %s", symbol_id)
        return None
    expiry_str = _format_expiry_str(expiry_date)
    clean_base = base_asset.upper()
    clean_quote = quote_asset.upper()
    flavor = _resolve_settle_flavor(venue, clean_base, clean_quote, deribit_quotes, symbol_id, "option")
    return f"{venue}:OPTION:{base_asset}-{quote_asset}-{expiry_str}-{strike_price}-{option_type}@{flavor}"


def _resolve_option_params(
    service: CanonicalKeyServiceProtocol,
    symbol_id: str,
    exchange: str,
    symbol_info: dict[str, object],
) -> tuple[object, object, str]:
    """Resolve expiry_date, strike_price, and option_type from symbol info or parsing."""
    expiry_date = symbol_info.get("expiry_date")
    strike_price = symbol_info.get("strike_price")
    option_type: str = str(symbol_info.get("option_type") or "").upper()
    if not all([expiry_date, strike_price, option_type]):
        parsed_option = service.parse_option_components(symbol_id, exchange)
        expiry_date = expiry_date or parsed_option.get("expiry_date")
        strike_price = strike_price or parsed_option.get("strike_price")
        option_type = option_type or str(parsed_option.get("option_type") or "").upper()
    return expiry_date, strike_price, option_type


def generate_canonical_key(
    service: CanonicalKeyServiceProtocol,
    exchange: str,
    symbol_type: str,
    symbol_id: str,
    symbol_info: dict[str, object],
) -> str | None:
    """Generate canonical instrument key following INSTRUMENT_KEY.md specification."""
    venue = service.normalize_venue(exchange)
    instrument_type = service.normalize_instrument_type(symbol_type)
    if not venue or not instrument_type:
        return None

    base_asset, quote_asset = _resolve_base_quote(service, symbol_id, exchange, symbol_info)
    if not base_asset or not quote_asset:
        logger.debug(
            "Skipping instrument with parsing issues: %s (exchange: %s) - base:'%s' quote:'%s'",
            symbol_id,
            exchange,
            base_asset,
            quote_asset,
        )
        return None

    _raw_deribit = service.exchange_config.valid_quote_currencies.get("DERIBIT")
    deribit_quotes: list[str] = list(_raw_deribit) if _raw_deribit is not None else []

    if instrument_type == "SPOT_PAIR":
        return _generate_spot_key(venue, base_asset, quote_asset, symbol_id)
    if instrument_type == "PERPETUAL":
        return _generate_perpetual_key(venue, base_asset, quote_asset, deribit_quotes, symbol_id)
    if instrument_type == "FUTURE":
        return _generate_future_key(
            service, venue, base_asset, quote_asset, deribit_quotes, symbol_id, exchange, symbol_info
        )
    if instrument_type == "OPTION":
        return _generate_option_key(
            service, venue, base_asset, quote_asset, deribit_quotes, symbol_id, exchange, symbol_info
        )
    logger.warning("Unhandled instrument type: %s", instrument_type)
    return None


def _resolve_base_quote(
    service: CanonicalKeyServiceProtocol,
    symbol_id: str,
    exchange: str,
    symbol_info: dict[str, object],
) -> tuple[str, str]:
    """Resolve base and quote assets from symbol_info, falling back to parsing."""
    base_asset = str(symbol_info.get("base_asset") or "").upper()
    quote_asset = str(symbol_info.get("quote_asset") or "").upper()
    if not base_asset or not quote_asset:
        parsed = service.parse_symbol_components(symbol_id, exchange)
        base_asset = str(parsed.get("base_asset") or "").upper()
        quote_asset = str(parsed.get("quote_asset") or "").upper()
    return base_asset, quote_asset
