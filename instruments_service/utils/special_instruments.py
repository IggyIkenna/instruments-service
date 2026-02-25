"""
Special instrument definitions - VIX, KRW/USD, Bitcoin ETFs.

Service-specific utilities for creating non-standard instrument definitions
that are not provided by upstream data adapters.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def create_vix_instrument_definition(target_date: datetime) -> dict[str, Any]:
    """
    Create VIX (Volatility Index) instrument definition.

    VIX is a volatility index (not a tradable instrument itself) calculated by CBOE.
    We create it as a static instrument definition for reference pricing.

    Args:
        target_date: Target date for availability window

    Returns:
        VIX instrument definition dictionary
    """
    venue = "CBOE"
    instrument_type = "INDEX"
    symbol_canonical = "VIX"
    instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"
    available_from = datetime(2020, 1, 1, tzinfo=UTC).isoformat()

    return {
        "instrument_key": instrument_key,
        "venue": venue,
        "instrument_type": instrument_type,
        "symbol": symbol_canonical,
        "available_from_datetime": available_from,
        "venue_type": "exchange",
        "data_provider": "databento",
        "asset_class": "traditional",
        "available_to_datetime": None,
        "data_types": "ohlcv_1m",
        "base_asset": "VIX",
        "quote_asset": "USD",
        "settle_asset": "USD",
        "exchange_raw_symbol": "VIX",
        "databento_symbol": "VIX",
        "tardis_exchange": "",
        "tardis_symbol": "",
        "inverse": False,
        "tick_size": "0.01",
        "min_size": "",
        "strike": "",
        "option_type": "",
        "expiry": None,
        "contract_size": None,
        "underlying": "",
        "ccxt_symbol": "",
        "ccxt_exchange": "",
        "base_asset_contract_address": None,
        "quote_asset_contract_address": None,
        "pool_address": None,
        "pool_fee_tier": None,
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
        "max_position_size": None,
        "max_leverage": None,
        "initial_margin_rate": None,
        "maintenance_margin_rate": None,
        "leverage_tiers_json": None,
        "trading_hours_open": "14:30:00+00:00",
        "trading_hours_close": "21:00:00+00:00",
        "trading_session": "regular",
        "is_trading_day": True,
        "holiday_calendar": "NYSE",
        "regular_open_utc": None,
        "regular_close_utc": None,
        "auction_open_utc": None,
        "auction_close_utc": None,
        "early_close_utc": None,
        "chain": "off-chain",
        "market_category": "TRADFI",
    }


def create_krwusd_instrument_definition(target_date: datetime) -> dict[str, Any]:
    """
    Create KRW/USD currency pair instrument definition.

    KRW/USD is not available via Databento, but we create it as a static instrument
    definition using Yahoo Finance as the data provider.

    Args:
        target_date: Target date for availability window (not used for forex, kept for consistency)

    Returns:
        KRW/USD instrument definition dictionary
    """
    _ = target_date
    venue = "FX"
    instrument_type = "SPOT_PAIR"
    base_asset = "KRW"
    quote_asset = "USD"
    symbol_canonical = f"{base_asset}-{quote_asset}"
    instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"
    available_from = datetime(2020, 1, 1, tzinfo=UTC).isoformat()

    return {
        "instrument_key": instrument_key,
        "venue": venue,
        "instrument_type": instrument_type,
        "symbol": symbol_canonical,
        "available_from_datetime": available_from,
        "venue_type": "otc",
        "tardis_exchange": "",
        "data_provider": "yahoo_finance",
        "asset_class": "traditional",
        "available_to_datetime": None,
        "data_types": "ohlcv_24h",
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "settle_asset": quote_asset,
        "exchange_raw_symbol": "KRWUSD=X",
        "databento_symbol": "",
        "tardis_symbol": "",
        "inverse": False,
        "tick_size": "",
        "min_size": "",
        "strike": "",
        "option_type": "",
        "expiry": None,
        "contract_size": None,
        "underlying": "",
        "ccxt_symbol": "",
        "ccxt_exchange": "",
        "base_asset_contract_address": None,
        "quote_asset_contract_address": None,
        "pool_address": None,
        "pool_fee_tier": None,
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
        "max_position_size": None,
        "max_leverage": None,
        "initial_margin_rate": None,
        "maintenance_margin_rate": None,
        "leverage_tiers_json": None,
        "trading_hours_open": "00:00:00+00:00",
        "trading_hours_close": "23:59:59+00:00",
        "trading_session": "24/7",
        "is_trading_day": True,
        "holiday_calendar": None,
        "regular_open_utc": None,
        "regular_close_utc": None,
        "auction_open_utc": None,
        "auction_close_utc": None,
        "early_close_utc": None,
        "chain": "off-chain",
        "market_category": "TRADFI",
    }


def get_us_equity_trading_hours(venue: str, instrument_type: str, target_date: datetime) -> dict[str, Any]:
    """Return US equity trading hours (9:30-16:00 ET) for NASDAQ/NYSE."""
    _ = (venue, instrument_type)
    target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    if target_date_start.tzinfo is None:
        target_date_start = target_date_start.replace(tzinfo=UTC)
    session_start = target_date_start.replace(hour=14, minute=30, second=0).isoformat()
    session_end = target_date_start.replace(hour=21, minute=0, second=0).isoformat()
    return {
        "session_start_utc": session_start,
        "session_end_utc": session_end,
        "open": "09:30:00-05:00",
        "close": "16:00:00-05:00",
        "session": "regular",
        "is_trading_day": True,
        "holiday_calendar": "NYSE",
        "regular_open_utc": session_start,
        "regular_close_utc": session_end,
        "auction_open_utc": None,
        "auction_close_utc": None,
        "early_close_utc": None,
    }


def create_bitcoin_etf_instrument_definition(
    ticker: str,
    target_date: datetime,
    get_exchange_trading_hours: Callable[[str, str, datetime], dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Create Bitcoin ETF instrument definition with full metadata.

    Bitcoin ETFs (IBIT, FBTC, ARKB) are TradFi ETFs that track Bitcoin price.
    They trade on US stock exchanges with standard equity trading hours.

    Args:
        ticker: ETF ticker symbol (e.g., "IBIT", "FBTC", "ARKB")
        target_date: Target date for trading hours calculation
        get_exchange_trading_hours: Callable(venue, instrument_type, target_date) -> dict

    Returns:
        Bitcoin ETF instrument definition dictionary or None if ticker not recognized
    """
    etf_venues = {
        "IBIT": "NASDAQ",
        "FBTC": "NASDAQ",
        "ARKB": "NASDAQ",
    }

    venue = etf_venues.get(ticker.upper())
    if not venue:
        logger.warning(f"Unknown Bitcoin ETF ticker: {ticker}")
        return None

    ticker_upper = ticker.upper()
    instrument_type = "ETF"
    base_asset = ticker_upper
    quote_asset = "USD"
    symbol_canonical = f"{base_asset}-{quote_asset}"
    instrument_key = f"{venue}:{instrument_type}:{symbol_canonical}"

    trading_hours = get_exchange_trading_hours(venue, instrument_type, target_date)
    available_from = trading_hours.get("session_start_utc")
    if not available_from:
        target_date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if target_date_start.tzinfo is None:
            target_date_start = target_date_start.replace(tzinfo=UTC)
        available_from = target_date_start.isoformat()
    available_to = trading_hours.get("session_end_utc")

    return {
        "instrument_key": instrument_key,
        "venue": venue,
        "instrument_type": instrument_type,
        "symbol": symbol_canonical,
        "available_from_datetime": available_from,
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "settle_asset": quote_asset,
        "underlying": "BTC",
        "asset_class": "traditional",
        "venue_type": "exchange",
        "chain": "off-chain",
        "market_category": "TRADFI",
        "data_provider": "databento",
        "databento_symbol": ticker_upper,
        "exchange_raw_symbol": ticker_upper,
        "tardis_exchange": "",
        "tardis_symbol": "",
        "data_types": "ohlcv_1m",
        "available_to_datetime": available_to,
        "tick_size": "0.01",
        "min_size": "1",
        "inverse": False,
        "expiry": None,
        "contract_size": None,
        "strike": "",
        "option_type": "",
        "ccxt_symbol": "",
        "ccxt_exchange": "",
        "base_asset_contract_address": None,
        "quote_asset_contract_address": None,
        "pool_address": None,
        "pool_fee_tier": None,
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
        "max_position_size": None,
        "max_leverage": None,
        "initial_margin_rate": None,
        "maintenance_margin_rate": None,
        "leverage_tiers_json": None,
        "trading_hours_open": trading_hours.get("open"),
        "trading_hours_close": trading_hours.get("close"),
        "trading_session": trading_hours.get("session"),
        "is_trading_day": trading_hours.get("is_trading_day"),
        "holiday_calendar": trading_hours.get("holiday_calendar"),
        "regular_open_utc": trading_hours.get("regular_open_utc"),
        "regular_close_utc": trading_hours.get("regular_close_utc"),
        "auction_open_utc": trading_hours.get("auction_open_utc"),
        "auction_close_utc": trading_hours.get("auction_close_utc"),
        "early_close_utc": trading_hours.get("early_close_utc"),
    }
