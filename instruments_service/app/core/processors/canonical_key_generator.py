"""
Canonical Key Generator

Generates canonical instrument keys following INSTRUMENT_KEY.md specification.
Extracted from InstrumentProcessingService.generate_canonical_key.
"""

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from unified_config_interface import DataTypeConfig, ExchangeInstrumentConfig

logger = logging.getLogger(__name__)


class CanonicalKeyServiceProtocol(Protocol):
    """Protocol for InstrumentProcessingService used by generate_canonical_key."""

    def normalize_venue(self, exchange: str) -> str | None: ...
    def normalize_instrument_type(self, symbol_type: str) -> str | None: ...
    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, Any]: ...
    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None: ...
    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, Any]: ...

    exchange_config: "ExchangeInstrumentConfig"
    data_config: "DataTypeConfig"


def generate_canonical_key(
    service: CanonicalKeyServiceProtocol,
    exchange: str,
    symbol_type: str,
    symbol_id: str,
    symbol_info: dict[str, Any],
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

    base_asset = str(symbol_info.get("base_asset") or "").upper()
    quote_asset = str(symbol_info.get("quote_asset") or "").upper()

    if not base_asset or not quote_asset:
        parsed_components = service.parse_symbol_components(symbol_id, exchange)
        base_asset = str(parsed_components.get("base_asset") or "").upper()
        quote_asset = str(parsed_components.get("quote_asset") or "").upper()

    if not base_asset or not quote_asset:
        logger.debug(
            f"🔄 Skipping instrument with parsing issues: {symbol_id} (exchange: {exchange}) - "
            f"base:'{base_asset}' quote:'{quote_asset}'"
        )
        return None

    deribit_quotes: list[str] = list(service.exchange_config.valid_quote_currencies.get("DERIBIT", []))

    if instrument_type == "SPOT_PAIR":
        clean_base = base_asset.strip() if base_asset else ""
        clean_quote = quote_asset.strip() if quote_asset else ""
        if not clean_base or not clean_quote:
            logger.debug(f"Skipping SPOT_PAIR with missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}")
            return None
        return f"{venue}:SPOT_PAIR:{clean_base}-{clean_quote}"

    elif instrument_type == "PERPETUAL":
        clean_base = base_asset.strip() if base_asset else ""
        clean_quote = quote_asset.strip() if quote_asset else ""

        clean_base = "".join(c for c in clean_base if c.isprintable() and c.isascii())
        clean_quote = "".join(c for c in clean_quote if c.isprintable() and c.isascii())

        if not clean_base or not clean_quote:
            logger.debug(
                f"Skipping PERPETUAL with invalid/missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}"
            )
            return None

        settle_asset = "USDT"
        if venue == "DERIBIT":
            if clean_quote == "USD":
                settle_asset = clean_base
            elif clean_quote in deribit_quotes and clean_quote != "USD":
                settle_asset = clean_quote
        else:
            settle_asset = clean_quote

        if settle_asset == clean_quote:
            perp_flavor = "LIN"
        elif settle_asset == clean_base:
            perp_flavor = "INV"
        else:
            perp_flavor = "LIN"
            logger.debug(
                f"⚠️ Could not determine perpetual flavor for {symbol_id}, defaulting to @LIN "
                f"(settle_asset={settle_asset}, quote={clean_quote}, base={clean_base})"
            )

        return f"{venue}:PERPETUAL:{clean_base}-{clean_quote}@{perp_flavor}"

    elif instrument_type == "FUTURE":
        expiry_date = symbol_info.get("expiry_date")

        if not expiry_date:
            expiry_date = service.parse_expiry_from_symbol(symbol_id, exchange)
            if expiry_date:
                logger.debug(f"✅ Parsed expiry from symbol: {symbol_id} → {expiry_date}")

        if expiry_date:
            if isinstance(expiry_date, str):
                try:
                    expiry_dt = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
                    expiry_str = expiry_dt.strftime("%y%m%d")
                except (ValueError, AttributeError):
                    expiry_str = expiry_date
            else:
                expiry_str = str(cast(str | int | float, expiry_date))

            clean_base = base_asset.strip() if base_asset else ""
            clean_quote = quote_asset.strip() if quote_asset else ""

            if not clean_base or not clean_quote:
                logger.debug(f"Skipping FUTURE with missing base/quote: '{base_asset}'/'{quote_asset}' for {symbol_id}")
                return None

            if expiry_str and len(expiry_str) > 6:
                match = re.search(r"(\d{6})", expiry_str)
                if match:
                    expiry_str = match.group(1)

            settle_asset = "USDT"
            if venue == "DERIBIT":
                if clean_quote == "USD":
                    settle_asset = clean_base
                elif clean_quote in deribit_quotes and clean_quote != "USD":
                    settle_asset = clean_quote
            else:
                settle_asset = clean_quote

            if settle_asset == clean_quote:
                future_flavor = "LIN"
            elif settle_asset == clean_base:
                future_flavor = "INV"
            else:
                future_flavor = "LIN"
                logger.debug(
                    f"⚠️ Could not determine future flavor for {symbol_id}, defaulting to @LIN "
                    f"(settle_asset={settle_asset}, quote={clean_quote}, base={clean_base})"
                )

            return f"{venue}:FUTURE:{clean_base}-{clean_quote}-{expiry_str}@{future_flavor}"
        else:
            logger.warning(f"Missing expiry date for future {symbol_id} exchange: {exchange}")
            return None

    elif instrument_type == "OPTION":
        excluded_strategies: list[str] = service.data_config.excluded_deribit_strategies
        if exchange == "deribit" and any(strategy in symbol_id for strategy in excluded_strategies):
            logger.debug(f"Filtered complex Deribit strategy option: {symbol_id}")
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
            logger.warning(f"Missing option parameters for {symbol_id}")
            return None

        if isinstance(expiry_date, str):
            try:
                expiry_dt = datetime.fromisoformat(expiry_date.replace("Z", "+00:00"))
                expiry_str = expiry_dt.strftime("%y%m%d")
            except (ValueError, AttributeError):
                expiry_str = expiry_date
        else:
            expiry_str = str(cast(str | int | float, expiry_date))

        clean_base = base_asset.upper()
        clean_quote = quote_asset.upper()
        settle_asset = "USDT"
        if venue == "DERIBIT":
            if clean_quote == "USD":
                settle_asset = clean_base
            elif clean_quote in deribit_quotes and clean_quote != "USD":
                settle_asset = clean_quote
        else:
            settle_asset = clean_quote

        if settle_asset == clean_quote:
            option_flavor = "LIN"
        elif settle_asset == clean_base:
            option_flavor = "INV"
        else:
            option_flavor = "LIN"
            logger.debug(
                f"⚠️ Could not determine option flavor for {symbol_id}, defaulting to @LIN "
                f"(settle_asset={settle_asset}, quote={clean_quote}, base={clean_base})"
            )

        return f"{venue}:OPTION:{base_asset}-{quote_asset}-{expiry_str}-{strike_price}-{option_type}@{option_flavor}"

    else:
        logger.warning(f"Unhandled instrument type: {instrument_type}")
        return None
