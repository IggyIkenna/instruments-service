#!/usr/bin/env python3
"""Generate mock instrument definitions for local dev / CI mock mode.

instruments-service is Layer 1 (the ROOT) of the mock data dependency chain.
Every downstream service depends on these instrument definitions.

Usage:
    python scripts/seed_mock_data.py --scenario normal --seed 42 --env local
    python scripts/seed_mock_data.py --scenario normal --env dev
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
from unified_internal_contracts import InstrumentDefinition
from unified_internal_contracts.modes import MockScenario
from unified_internal_contracts.testing.scenario_config import ScenarioConfig
from unified_trading_library.core.seed_writer import SeedWriter, get_seed_writer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — path templates match what downstream DependencyCheckers expect
# ---------------------------------------------------------------------------

# GCS path template: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
PATH_PREFIX: Final[str] = "instrument_availability/by_date"

SEED_SPEC_PATH: Final[Path] = (
    Path(__file__).resolve().parents[1]
    / ".."
    / ("unified-internal-contracts/unified_internal_contracts/testing/scenarios/seed_spec.yaml")
)

# Canonical available_from for mock instruments (well before any seed date range)
AVAILABLE_FROM: Final[str] = "2020-01-01T00:00:00Z"

# ---------------------------------------------------------------------------
# Instrument catalogue — mirrors seed_spec.yaml instrument list
# ---------------------------------------------------------------------------

# CeFi crypto instruments
CEFI_INSTRUMENTS: Final[list[dict[str, str]]] = [
    # Binance spot
    {
        "symbol": "BTC-USDT",
        "venue": "BINANCE-SPOT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tardis_exchange": "binance",
        "tardis_symbol": "btcusdt",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "ETH-USDT",
        "venue": "BINANCE-SPOT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "tardis_exchange": "binance",
        "tardis_symbol": "ethusdt",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "SOL-USDT",
        "venue": "BINANCE-SPOT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "SOL",
        "quote_asset": "USDT",
        "tardis_exchange": "binance",
        "tardis_symbol": "solusdt",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "DOGE-USDT",
        "venue": "BINANCE-SPOT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "DOGE",
        "quote_asset": "USDT",
        "tardis_exchange": "binance",
        "tardis_symbol": "dogeusdt",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "XRP-USDT",
        "venue": "BINANCE-SPOT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "XRP",
        "quote_asset": "USDT",
        "tardis_exchange": "binance",
        "tardis_symbol": "xrpusdt",
        "data_types": "trades,book_snapshot_5",
    },
    # Binance futures (perpetual)
    {
        "symbol": "BTC-USDT",
        "venue": "BINANCE-FUTURES",
        "instrument_type": "PERPETUAL",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tardis_exchange": "binance-futures",
        "tardis_symbol": "btcusdt",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
    },
    {
        "symbol": "ETH-USDT",
        "venue": "BINANCE-FUTURES",
        "instrument_type": "PERPETUAL",
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "tardis_exchange": "binance-futures",
        "tardis_symbol": "ethusdt",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
    },
    {
        "symbol": "SOL-USDT",
        "venue": "BINANCE-FUTURES",
        "instrument_type": "PERPETUAL",
        "base_asset": "SOL",
        "quote_asset": "USDT",
        "tardis_exchange": "binance-futures",
        "tardis_symbol": "solusdt",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
    },
    # Deribit perpetuals
    {
        "symbol": "BTC-PERPETUAL",
        "venue": "DERIBIT",
        "instrument_type": "PERPETUAL",
        "base_asset": "BTC",
        "quote_asset": "USD",
        "tardis_exchange": "deribit",
        "tardis_symbol": "BTC-PERPETUAL",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
        "inverse": "true",
    },
    {
        "symbol": "ETH-PERPETUAL",
        "venue": "DERIBIT",
        "instrument_type": "PERPETUAL",
        "base_asset": "ETH",
        "quote_asset": "USD",
        "tardis_exchange": "deribit",
        "tardis_symbol": "ETH-PERPETUAL",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
        "inverse": "true",
    },
    # OKX spot
    {
        "symbol": "BTC-USDT",
        "venue": "OKX",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tardis_exchange": "okex",
        "tardis_symbol": "BTC-USDT",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "ETH-USDT",
        "venue": "OKX",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "tardis_exchange": "okex",
        "tardis_symbol": "ETH-USDT",
        "data_types": "trades,book_snapshot_5",
    },
    # Bybit perpetuals
    {
        "symbol": "BTC-USDT",
        "venue": "BYBIT",
        "instrument_type": "PERPETUAL",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tardis_exchange": "bybit",
        "tardis_symbol": "BTCUSDT",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
    },
    {
        "symbol": "ETH-USDT",
        "venue": "BYBIT",
        "instrument_type": "PERPETUAL",
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "tardis_exchange": "bybit",
        "tardis_symbol": "ETHUSDT",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
    },
    # Coinbase spot
    {
        "symbol": "BTC-USD",
        "venue": "COINBASE",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "BTC",
        "quote_asset": "USD",
        "tardis_exchange": "coinbase",
        "tardis_symbol": "BTC-USD",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "ETH-USD",
        "venue": "COINBASE",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "ETH",
        "quote_asset": "USD",
        "tardis_exchange": "coinbase",
        "tardis_symbol": "ETH-USD",
        "data_types": "trades,book_snapshot_5",
    },
    # Upbit (kimchi premium)
    {
        "symbol": "BTC-KRW",
        "venue": "UPBIT",
        "instrument_type": "SPOT_PAIR",
        "base_asset": "BTC",
        "quote_asset": "KRW",
        "tardis_exchange": "upbit",
        "tardis_symbol": "KRW-BTC",
        "data_types": "trades",
    },
    # Hyperliquid perpetuals
    {
        "symbol": "BTC-USDC",
        "venue": "HYPERLIQUID",
        "instrument_type": "PERPETUAL",
        "base_asset": "BTC",
        "quote_asset": "USDC",
        "tardis_exchange": "",
        "tardis_symbol": "",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
        "data_provider": "hyperliquid",
    },
    {
        "symbol": "ETH-USDC",
        "venue": "HYPERLIQUID",
        "instrument_type": "PERPETUAL",
        "base_asset": "ETH",
        "quote_asset": "USDC",
        "tardis_exchange": "",
        "tardis_symbol": "",
        "data_types": "trades,book_snapshot_5,derivative_ticker",
        "data_provider": "hyperliquid",
    },
]

# TradFi instruments (Databento)
TRADFI_INSTRUMENTS: Final[list[dict[str, str]]] = [
    {
        "symbol": "SPY",
        "venue": "NYSE",
        "instrument_type": "ETF",
        "base_asset": "SPY",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "SPY",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NYSE",
    },
    {
        "symbol": "QQQ",
        "venue": "NASDAQ",
        "instrument_type": "ETF",
        "base_asset": "QQQ",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "QQQ",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NASDAQ",
    },
    {
        "symbol": "AAPL",
        "venue": "NASDAQ",
        "instrument_type": "EQUITY",
        "base_asset": "AAPL",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "AAPL",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NASDAQ",
    },
    {
        "symbol": "TSLA",
        "venue": "NASDAQ",
        "instrument_type": "EQUITY",
        "base_asset": "TSLA",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "TSLA",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NASDAQ",
    },
    {
        "symbol": "GLD",
        "venue": "NYSE",
        "instrument_type": "ETF",
        "base_asset": "GLD",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "GLD",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NYSE",
    },
    {
        "symbol": "ES.FUT",
        "venue": "CME",
        "instrument_type": "FUTURE",
        "base_asset": "ES",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "ES.FUT",
        "asset_class": "traditional",
        "exchange_raw_symbol": "ES",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "NQ.FUT",
        "venue": "CME",
        "instrument_type": "FUTURE",
        "base_asset": "NQ",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "NQ.FUT",
        "asset_class": "traditional",
        "exchange_raw_symbol": "NQ",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "6A.FUT",
        "venue": "CME",
        "instrument_type": "FUTURE",
        "base_asset": "AUD",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "6A.FUT",
        "asset_class": "traditional",
        "exchange_raw_symbol": "6A",
        "data_types": "trades,book_snapshot_5",
    },
    {
        "symbol": "IBIT",
        "venue": "NASDAQ",
        "instrument_type": "ETF",
        "base_asset": "IBIT",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "IBIT",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NASDAQ",
    },
    {
        "symbol": "FBTC",
        "venue": "NYSE",
        "instrument_type": "ETF",
        "base_asset": "FBTC",
        "quote_asset": "USD",
        "data_provider": "databento",
        "databento_symbol": "FBTC",
        "asset_class": "traditional",
        "data_types": "trades,book_snapshot_5",
        "trading_hours_open": "09:30:00-05:00",
        "trading_hours_close": "16:00:00-05:00",
        "holiday_calendar": "NYSE",
    },
]

# DeFi instruments
DEFI_INSTRUMENTS: Final[list[dict[str, str]]] = [
    # Aave V3 lending markets (A_TOKEN = supply side)
    {
        "symbol": "USDC",
        "venue": "AAVE_V3",
        "instrument_type": "A_TOKEN",
        "base_asset": "USDC",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
        "ltv": "0.77",
        "liquidation_threshold": "0.80",
        "liquidation_bonus": "0.05",
        "reserve_factor": "0.10",
        "optimal_utilization_rate": "0.90",
        "base_variable_borrow_rate": "0.00",
        "variable_rate_slope1": "0.04",
        "variable_rate_slope2": "0.60",
    },
    {
        "symbol": "WETH",
        "venue": "AAVE_V3",
        "instrument_type": "A_TOKEN",
        "base_asset": "WETH",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
        "ltv": "0.80",
        "liquidation_threshold": "0.825",
        "liquidation_bonus": "0.05",
        "reserve_factor": "0.15",
        "optimal_utilization_rate": "0.80",
        "base_variable_borrow_rate": "0.00",
        "variable_rate_slope1": "0.038",
        "variable_rate_slope2": "0.80",
    },
    {
        "symbol": "DAI",
        "venue": "AAVE_V3",
        "instrument_type": "A_TOKEN",
        "base_asset": "DAI",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
        "ltv": "0.75",
        "liquidation_threshold": "0.80",
        "liquidation_bonus": "0.05",
        "reserve_factor": "0.10",
    },
    {
        "symbol": "WBTC",
        "venue": "AAVE_V3",
        "instrument_type": "A_TOKEN",
        "base_asset": "WBTC",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
        "ltv": "0.70",
        "liquidation_threshold": "0.75",
        "liquidation_bonus": "0.06",
        "reserve_factor": "0.20",
    },
    # Aave V3 debt tokens (borrow side)
    {
        "symbol": "USDC-DEBT",
        "venue": "AAVE_V3",
        "instrument_type": "DEBT_TOKEN",
        "base_asset": "USDC",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
    },
    {
        "symbol": "WETH-DEBT",
        "venue": "AAVE_V3",
        "instrument_type": "DEBT_TOKEN",
        "base_asset": "WETH",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "aavescan",
        "data_types": "derivative_ticker",
    },
    # Uniswap V3 pools
    {
        "symbol": "WETH-USDC-3000",
        "venue": "UNISWAPV3-ETH",
        "instrument_type": "POOL",
        "base_asset": "WETH",
        "quote_asset": "USDC",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "thegraph",
        "data_types": "trades",
        "pool_fee_tier": "3000",
        "base_asset_contract_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "quote_asset_contract_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    },
    {
        "symbol": "WETH-USDT-500",
        "venue": "UNISWAPV3-ETH",
        "instrument_type": "POOL",
        "base_asset": "WETH",
        "quote_asset": "USDT",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "thegraph",
        "data_types": "trades",
        "pool_fee_tier": "500",
        "base_asset_contract_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "quote_asset_contract_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    },
    # Lido stETH (LST)
    {
        "symbol": "stETH",
        "venue": "LIDO",
        "instrument_type": "LST",
        "base_asset": "stETH",
        "quote_asset": "ETH",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "lido",
        "data_types": "derivative_ticker",
    },
    {
        "symbol": "wstETH",
        "venue": "LIDO",
        "instrument_type": "LST",
        "base_asset": "wstETH",
        "quote_asset": "ETH",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "lido",
        "data_types": "derivative_ticker",
    },
    # EtherFi (restaking)
    {
        "symbol": "eETH",
        "venue": "ETHERFI",
        "instrument_type": "LST",
        "base_asset": "eETH",
        "quote_asset": "ETH",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "etherfi",
        "data_types": "derivative_ticker",
    },
    # Curve 3pool
    {
        "symbol": "3pool",
        "venue": "CURVE-ETH",
        "instrument_type": "POOL",
        "base_asset": "DAI-USDC-USDT",
        "quote_asset": "USD",
        "venue_type": "protocol",
        "asset_class": "crypto",
        "chain": "ethereum",
        "data_provider": "thegraph",
        "data_types": "trades",
    },
]

# Sports instruments
SPORTS_INSTRUMENTS: Final[list[dict[str, str]]] = [
    {
        "symbol": "MATCH_ODDS",
        "venue": "PINNACLE",
        "instrument_type": "FIXED_ODDS",
        "base_asset": "premier_league",
        "quote_asset": "GBP",
        "venue_type": "bookmaker",
        "asset_class": "sports",
        "market_category": "sports",
        "data_types": "trades",
    },
    {
        "symbol": "MATCH_ODDS",
        "venue": "PINNACLE",
        "instrument_type": "FIXED_ODDS",
        "base_asset": "nba",
        "quote_asset": "USD",
        "venue_type": "bookmaker",
        "asset_class": "sports",
        "market_category": "sports",
        "data_types": "trades",
    },
    {
        "symbol": "MATCH_ODDS",
        "venue": "PINNACLE",
        "instrument_type": "FIXED_ODDS",
        "base_asset": "la_liga",
        "quote_asset": "EUR",
        "venue_type": "bookmaker",
        "asset_class": "sports",
        "market_category": "sports",
        "data_types": "trades",
    },
    {
        "symbol": "MATCH_ODDS",
        "venue": "PINNACLE",
        "instrument_type": "FIXED_ODDS",
        "base_asset": "nfl",
        "quote_asset": "USD",
        "venue_type": "bookmaker",
        "asset_class": "sports",
        "market_category": "sports",
        "data_types": "trades",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_instrument_key(venue: str, instrument_type: str, symbol: str) -> str:
    """Return canonical VENUE:TYPE:SYMBOL instrument key."""
    return f"{venue}:{instrument_type}:{symbol}"


def _make_definition(
    raw: dict[str, str],
    timestamp: datetime,
    rng: np.random.Generator,
) -> InstrumentDefinition:
    """Build a validated InstrumentDefinition from a raw dict spec."""
    venue = raw["venue"]
    inst_type = raw["instrument_type"]
    symbol = raw["symbol"]
    instrument_key = _build_instrument_key(venue, inst_type, symbol)

    # Build kwargs from all known fields
    kwargs: dict[str, str | bool | float | None] = {
        "instrument_key": instrument_key,
        "venue": venue,
        "instrument_type": inst_type,
        "symbol": symbol,
        "available_from_datetime": AVAILABLE_FROM,
        "base_asset": raw.get("base_asset", ""),
        "quote_asset": raw.get("quote_asset", ""),
        "tardis_exchange": raw.get("tardis_exchange", ""),
        "tardis_symbol": raw.get("tardis_symbol", ""),
        "data_types": raw.get("data_types"),
        "data_provider": raw.get("data_provider", "tardis"),
        "venue_type": raw.get("venue_type", "exchange"),
        "asset_class": raw.get("asset_class", "crypto"),
        "chain": raw.get("chain", ""),
        "databento_symbol": raw.get("databento_symbol", ""),
        "exchange_raw_symbol": raw.get("exchange_raw_symbol", ""),
        "market_category": raw.get("market_category", ""),
    }

    # Handle inverse flag
    if raw.get("inverse") == "true":
        kwargs["inverse"] = True

    # Handle DeFi lending fields
    for defi_field in [
        "ltv",
        "liquidation_threshold",
        "liquidation_bonus",
        "reserve_factor",
        "optimal_utilization_rate",
        "base_variable_borrow_rate",
        "variable_rate_slope1",
        "variable_rate_slope2",
    ]:
        if defi_field in raw:
            kwargs[defi_field] = float(raw[defi_field])

    # Handle pool fields
    if "pool_fee_tier" in raw:
        kwargs["pool_fee_tier"] = int(raw["pool_fee_tier"])
    for addr_field in ["base_asset_contract_address", "quote_asset_contract_address", "pool_address", "pool_id"]:
        if addr_field in raw:
            kwargs[addr_field] = raw[addr_field]

    # Handle TradFi trading hours
    for tf_field in ["trading_hours_open", "trading_hours_close", "trading_session", "holiday_calendar"]:
        if tf_field in raw:
            kwargs[tf_field] = raw[tf_field]

    # Generate realistic tick_size and min_size based on instrument type
    if inst_type in ("SPOT_PAIR", "PERPETUAL"):
        tick_size = "0.01" if "BTC" in symbol else "0.001"
        min_size = "0.001" if "BTC" in symbol else "0.01"
        kwargs["tick_size"] = tick_size
        kwargs["min_size"] = min_size

        # Mock leverage for perpetuals
        if inst_type == "PERPETUAL":
            kwargs["max_leverage"] = 125.0 if "BTC" in symbol else 100.0
            kwargs["initial_margin_rate"] = 0.008 if "BTC" in symbol else 0.01
            kwargs["maintenance_margin_rate"] = 0.004 if "BTC" in symbol else 0.005

    # Generate CCXT fields for CeFi instruments
    ccxt_exchange_map: dict[str, str] = {
        "BINANCE-SPOT": "binance",
        "BINANCE-FUTURES": "binanceusdm",
        "OKX": "okx",
        "BYBIT": "bybit",
        "DERIBIT": "deribit",
        "COINBASE": "coinbase",
        "UPBIT": "upbit",
    }
    ccxt_ex = ccxt_exchange_map.get(venue, "")
    if ccxt_ex:
        kwargs["ccxt_exchange"] = ccxt_ex
        # Build CCXT symbol format
        base = raw.get("base_asset", "")
        quote = raw.get("quote_asset", "")
        if inst_type == "PERPETUAL":
            kwargs["ccxt_symbol"] = f"{base}/{quote}:{quote}"
        else:
            kwargs["ccxt_symbol"] = f"{base}/{quote}"

    # Suppress vol for dummy rng usage (deterministic jitter on contract_size)
    _ = float(rng.random())

    return InstrumentDefinition.model_validate(kwargs)


def _categorize_venue(venue: str) -> str:
    """Return market category for a venue string."""
    cefi_venues = {
        "BINANCE-SPOT",
        "BINANCE-FUTURES",
        "DERIBIT",
        "OKX",
        "BYBIT",
        "COINBASE",
        "UPBIT",
        "HYPERLIQUID",
        "ASTER",
    }
    tradfi_venues = {"CME", "NASDAQ", "NYSE", "ICE", "CBOE"}
    defi_venues = {
        "AAVE_V3",
        "AAVE_V3_ETH",
        "LIDO",
        "ETHERFI",
        "ETHENA",
        "UNISWAPV3-ETH",
        "UNISWAPV2-ETH",
        "CURVE-ETH",
        "BALANCER-ETH",
        "AERODROME-BASE",
    }
    sports_venues = {"PINNACLE"}

    if venue in cefi_venues:
        return "cefi"
    if venue in tradfi_venues:
        return "tradfi"
    if venue in defi_venues:
        return "defi"
    if venue in sports_venues:
        return "sports"
    return "cefi"


# ---------------------------------------------------------------------------
# Main seed logic
# ---------------------------------------------------------------------------


def generate_instruments(
    scenario_cfg: ScenarioConfig,
    seed: int,
) -> tuple[list[InstrumentDefinition], dict[str, int]]:
    """Generate all mock instrument definitions.

    Returns:
        Tuple of (list of InstrumentDefinition, counts by category).
    """
    rng = np.random.default_rng(seed)
    timestamp = datetime.now(UTC)
    instruments: list[InstrumentDefinition] = []
    counts: dict[str, int] = {"cefi": 0, "tradfi": 0, "defi": 0, "sports": 0}

    all_specs: list[tuple[str, list[dict[str, str]]]] = [
        ("cefi", list(CEFI_INSTRUMENTS)),
        ("tradfi", list(TRADFI_INSTRUMENTS)),
        ("defi", list(DEFI_INSTRUMENTS)),
        ("sports", list(SPORTS_INSTRUMENTS)),
    ]

    for category, spec_list in all_specs:
        for raw in spec_list:
            defn = _make_definition(raw, timestamp, rng)
            instruments.append(defn)
            counts[category] += 1

    log.info(
        "Generated %d instrument definitions: %s",
        len(instruments),
        ", ".join(f"{k}={v}" for k, v in sorted(counts.items())),
    )
    return instruments, counts


def write_parquet_by_venue(
    instruments: list[InstrumentDefinition],
    writer: SeedWriter,
    date_str: str,
) -> list[str]:
    """Write instrument parquet files partitioned by venue (matching GCS layout).

    Path template: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
    """
    # Group instruments by venue
    by_venue: dict[str, list[dict[str, object]]] = {}
    timestamp = datetime.now(UTC)

    for inst in instruments:
        venue = inst.venue
        row = inst.model_dump()
        row["timestamp"] = timestamp
        if venue not in by_venue:
            by_venue[venue] = []
        by_venue[venue].append(row)

    written_paths: list[str] = []

    for venue, rows in sorted(by_venue.items()):
        venue_folder = venue.replace("/", "-").replace("\\", "-")
        relative_path = f"{PATH_PREFIX}/day={date_str}/venue={venue_folder}/instruments.parquet"

        df = pd.DataFrame(rows)
        out = writer.write_parquet(df, relative_path)
        written_paths.append(out)
        log.info("  %s: %d instruments", venue, len(rows))

    return written_paths


def write_combined_parquet(
    instruments: list[InstrumentDefinition],
    writer: SeedWriter,
    date_str: str,
) -> str:
    """Write a single combined parquet file with all instruments (for downstream convenience)."""
    timestamp = datetime.now(UTC)
    rows: list[dict[str, object]] = []
    for inst in instruments:
        row = inst.model_dump()
        row["timestamp"] = timestamp
        rows.append(row)

    relative_path = f"{PATH_PREFIX}/day={date_str}/all_instruments.parquet"
    df = pd.DataFrame(rows)
    out = writer.write_parquet(df, relative_path)
    log.info("Combined: %d instruments", len(instruments))
    return out


def write_seed_manifest(
    writer: SeedWriter,
    counts: dict[str, int],
    written_paths: list[str],
    scenario_name: str,
    seed: int,
    date_str: str,
) -> str:
    """Write a JSON manifest summarising the seed run."""
    manifest: dict[str, object] = {
        "service": "instruments-service",
        "layer": 1,
        "scenario": scenario_name,
        "seed": seed,
        "date": date_str,
        "generated_at": datetime.now(UTC).isoformat(),
        "instrument_counts": counts,
        "total_instruments": sum(counts.values()),
        "files": written_paths,
    }

    return writer.write_json(manifest, "seed_manifest.json")


def write_seed_complete_marker(writer: SeedWriter) -> str:
    """Write .seed-complete marker file (used by validate-mock-upstream.sh)."""
    marker_data = json.dumps(
        {
            "service": "instruments-service",
            "completed_at": datetime.now(UTC).isoformat(),
            "layer": 1,
        }
    )
    return writer.write_text(marker_data, ".seed-complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate mock instrument definitions for local dev / CI mock mode.",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="normal",
        choices=[s.value for s in MockScenario],
        help="Named scenario for deterministic mock generation (default: normal)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic output (default: 42, overridden by scenario if set)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="local",
        choices=["local", "dev"],
        help="Target environment: local (filesystem) or dev (would use GCS in future)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(UTC).strftime("%Y-%m-%d"),
        help="Date string for partitioned output (default: today)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Override output directory (default: $WORKSPACE_ROOT/.local-dev-cache/mock-seed/instruments-service)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for seed_mock_data."""
    args = parse_args(argv)

    # Load scenario config
    scenario_enum = MockScenario(args.scenario)
    scenario_cfg = ScenarioConfig.load(scenario_enum)
    effective_seed: int = scenario_cfg.seed if scenario_cfg.seed != 0 else args.seed

    log.info("=== instruments-service mock data seed ===")
    log.info("Scenario: %s (seed=%d)", scenario_enum.value, effective_seed)
    log.info("Env: %s, Date: %s", args.env, args.date)

    # Create seed writer (local filesystem or cloud storage)
    writer = get_seed_writer(args.env, "instruments-service", args.output_dir)
    log.info("Writer: %s", type(writer).__name__)

    # Generate instruments
    instruments, counts = generate_instruments(scenario_cfg, effective_seed)

    # Write partitioned parquet files (by venue)
    written = write_parquet_by_venue(instruments, writer, args.date)

    # Write combined parquet (all instruments in one file)
    combined_path = write_combined_parquet(instruments, writer, args.date)
    written.append(combined_path)

    # Write manifest
    write_seed_manifest(writer, counts, written, scenario_enum.value, effective_seed, args.date)

    # Write .seed-complete marker
    write_seed_complete_marker(writer)

    # Print summary
    total = sum(counts.values())
    log.info("=== SEED COMPLETE ===")
    log.info("Total instruments: %d", total)
    for cat, count in sorted(counts.items()):
        log.info("  %-8s %d", cat, count)
    log.info("Files written: %d parquet files", len(written))

    return 0


if __name__ == "__main__":
    sys.exit(main())
