"""
Aster Adapter

Fetches Aster perpetual futures instruments and tests historical data availability.
Uses Aster REST API for historical data.

Reference: https://github.com/asterdex/api-docs
"""

import logging
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class AsterAdapter:
    """
    Adapter for fetching Aster perpetual futures instruments.

    Generates instruments in format:
    ASTER:PERPETUAL:BTC-USDT
    """

    def __init__(
        self,
        futures_api_base_url: Optional[str] = None,
        spot_api_base_url: Optional[str] = None,
        base_currency_list: Optional[List[str]] = None,
        mvp_only: bool = True,
    ):
        """
        Initialize Aster adapter.

        Args:
            futures_api_base_url: Optional custom futures API base URL
                (defaults to https://fapi.asterdex.com)
            spot_api_base_url: Optional custom spot API base URL
                (defaults to https://api.asterdex.com)
            base_currency_list: List of MVP base currencies from config (defaults to None, uses all if not provided)
            mvp_only: If True, only include MVP coins (default: True)
        """
        self.venue = "ASTER"
        self.futures_api_base_url = futures_api_base_url or "https://fapi.asterdex.com"
        self.spot_api_base_url = spot_api_base_url or "https://api.asterdex.com"
        self.mvp_only = mvp_only
        # Use provided base_currency_list or empty set (no filtering)
        self.mvp_base_currencies = (
            {c.upper() for c in base_currency_list} if base_currency_list else set()
        )
        logger.info(
            f"✅ AsterAdapter initialized (MVP only: {mvp_only}, base currencies: {len(self.mvp_base_currencies) if self.mvp_base_currencies else 'all'})"
        )

    def fetch_perpetuals(self, test_data_availability: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all perpetual futures from Aster.

        Args:
            test_data_availability: If True, test data availability and populate data_types

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        try:
            # Get exchange info to get all symbols
            response = requests.get(f"{self.futures_api_base_url}/fapi/v1/exchangeInfo", timeout=30)
            response.raise_for_status()
            exchange_info = response.json()

            instruments = {}
            symbols = exchange_info.get("symbols", [])

            for symbol_info in symbols:
                try:
                    symbol = symbol_info.get("symbol", "")
                    status = symbol_info.get("status", "")
                    contract_type = symbol_info.get("contractType", "")

                    # Only include PERPETUAL contracts that are TRADING
                    if contract_type == "PERPETUAL" and status == "TRADING":
                        # Filter to MVP base currencies only if enabled
                        base_asset = symbol_info.get("baseAsset", "")
                        if self.mvp_only and self.mvp_base_currencies:
                            if base_asset.upper() not in self.mvp_base_currencies:
                                continue

                        inst_def = self._convert_symbol_to_instrument(symbol_info)
                        if inst_def:
                            # Set default data types - actual data fetching happens in market tick data handler
                            # Aster supports 1m candles + funding rates (derivative_ticker)
                            inst_def["data_types"] = "ohlcv_1m,derivative_ticker"

                            instruments[inst_def["instrument_key"]] = inst_def
                except Exception as e:
                    logger.warning(
                        f"Failed to convert symbol {symbol_info.get('symbol', 'unknown')}: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} Aster perpetual instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Aster perpetuals: {e}")
            return {}

    def fetch_spot_pairs(self, test_data_availability: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all spot trading pairs from Aster for MVP coins.

        Note: Aster spot markets may be illiquid or the API endpoint may not be publicly accessible.
        This method gracefully handles failures and returns an empty dict if spot trading is unavailable.

        Args:
            test_data_availability: If True, test data availability and populate data_types

        Returns:
            Dictionary mapping instrument_key to instrument definition (empty if spot API unavailable)
        """
        try:
            # Get exchange info from spot API
            # Note: Aster spot API endpoint may not be publicly accessible or may have DNS issues
            response = requests.get(f"{self.spot_api_base_url}/api/v3/exchangeInfo", timeout=30)
            response.raise_for_status()
            exchange_info = response.json()

            instruments = {}
            symbols = exchange_info.get("symbols", [])

            for symbol_info in symbols:
                try:
                    symbol = symbol_info.get("symbol", "")
                    status = symbol_info.get("status", "")

                    # Only include SPOT symbols that are TRADING
                    if status == "TRADING":
                        # Filter to MVP base currencies only if enabled
                        base_asset = symbol_info.get("baseAsset", "")
                        if self.mvp_only and self.mvp_base_currencies:
                            if base_asset not in self.mvp_base_currencies:
                                continue

                        # Create SPOT_PAIR instrument
                        inst_def = self._convert_symbol_to_spot_pair(symbol_info)
                        if inst_def:
                            # Set default data types - actual data fetching happens in market tick data handler
                            # Aster spot supports trades and order book snapshots (may be illiquid)
                            inst_def["data_types"] = "trades,book_snapshot_5"

                            instruments[inst_def["instrument_key"]] = inst_def
                except Exception as e:
                    logger.warning(
                        f"Failed to convert symbol {symbol_info.get('symbol', 'unknown')} to spot pair: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} Aster spot pair instruments")
            return instruments

        except requests.exceptions.RequestException as e:
            # DNS errors, connection errors, etc. - Aster spot API may not be publicly accessible
            logger.debug(f"Aster spot API unavailable (endpoint: {self.spot_api_base_url}): {e}")
            logger.info(
                "ℹ️  Aster spot trading API not accessible - skipping spot pairs (perpetuals only)"
            )
            return {}
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch Aster spot pairs: {e}")
            return {}

    def _convert_symbol_to_spot_pair(self, symbol_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Aster symbol data to SPOT_PAIR instrument definition.

        Args:
            symbol_info: Symbol information from exchangeInfo

        Returns:
            Instrument definition dictionary or None
        """
        symbol = symbol_info.get("symbol", "")
        if not symbol:
            return None

        # Extract base asset
        base_asset = symbol_info.get("baseAsset", "")

        if not base_asset:
            # Try to parse from symbol (e.g., "BTCUSDT" -> "BTC")
            if symbol.endswith("USDT"):
                base_asset = symbol[:-4]
            elif symbol.endswith("USDC"):
                base_asset = symbol[:-4]
            elif symbol.endswith("USD"):
                base_asset = symbol[:-3]
            else:
                logger.warning(f"Could not parse base asset from symbol {symbol}")
                return None

        # Force USDC as quote currency (both Hyperliquid and Aster support USDC)
        # Per user requirement: "quote asset set as USDC i guess if both support else USDT"
        # Both support USDC, so use USDC
        quote_asset = "USDC"

        # Build canonical symbol
        # Per INSTRUMENT_KEY.md: SPOT_PAIR format is BASE-QUOTE
        canonical_symbol = f"{base_asset}-{quote_asset}"

        # Build canonical instrument key with chain suffix
        # Format: VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN
        # Aster is on POLKADOT chain
        instrument_key = f"{self.venue}:SPOT_PAIR:{canonical_symbol}@POLKADOT"

        # Use conservative default date
        available_from = datetime(2021, 8, 1).isoformat()

        # Extract filters
        filters = symbol_info.get("filters", [])
        tick_size = ""
        min_size = ""

        for filter_item in filters:
            if filter_item.get("filterType") == "PRICE_FILTER":
                tick_size = filter_item.get("tickSize", "")
            elif filter_item.get("filterType") == "LOT_SIZE":
                min_size = filter_item.get("stepSize", "")

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "SPOT_PAIR",
            "symbol": canonical_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "settle_asset": quote_asset,
            "chain": "POLKADOT",  # Aster is on Polkadot chain
            "asset_class": "crypto",
            "venue_type": "exchange",  # Aster is an exchange, not a protocol
            "data_provider": "aster_api",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": symbol,  # CCXT may not support Aster, but keep raw symbol
            "ccxt_exchange": "",  # CCXT doesn't support Aster yet
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Spot pairs don't expire
            "data_types": "",  # Will be populated after testing data availability
            "inverse": False,
            "contract_size": None,
            "tick_size": tick_size,
            "min_size": min_size,
            "underlying": base_asset,
        }

    def _convert_symbol_to_instrument(
        self, symbol_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Convert Aster symbol data to instrument definition.

        Args:
            symbol_info: Symbol information from exchangeInfo

        Returns:
            Instrument definition dictionary or None
        """
        symbol = symbol_info.get("symbol", "")
        if not symbol:
            return None

        # Extract base asset
        base_asset = symbol_info.get("baseAsset", "")

        if not base_asset:
            # Try to parse from symbol (e.g., "BTCUSDT" -> "BTC")
            if symbol.endswith("USDT"):
                base_asset = symbol[:-4]
            elif symbol.endswith("USDC"):
                base_asset = symbol[:-4]
            elif symbol.endswith("USD"):
                base_asset = symbol[:-3]
            else:
                logger.warning(f"Could not parse base asset from symbol {symbol}")
                return None

        # Force USDC as quote currency (both Hyperliquid and Aster support USDC)
        # Per user requirement: "quote asset set as USDC i guess if both support else USDT"
        # Both support USDC, so use USDC
        quote_asset = "USDC"

        # Build canonical symbol
        # Per INSTRUMENT_KEY.md: PERPETUAL format is BASE-QUOTE@LIN or BASE-QUOTE@INV
        # Aster perpetuals are linear (USDC margin), so use @LIN
        canonical_symbol = f"{base_asset}-{quote_asset}@LIN"

        # Build canonical instrument key with chain suffix
        # Format: VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN
        # Aster is on POLKADOT chain
        instrument_key = f"{self.venue}:PERPETUAL:{canonical_symbol}@POLKADOT"

        # Use conservative default date - funding rate fetching is handled by market tick data handler service
        # Aster launched in 2021, but use a conservative default (2021-08-01) to avoid filtering out instruments incorrectly
        available_from = datetime(2021, 8, 1).isoformat()

        # Extract filters
        filters = symbol_info.get("filters", [])
        tick_size = ""
        min_size = ""

        for filter_item in filters:
            if filter_item.get("filterType") == "PRICE_FILTER":
                tick_size = filter_item.get("tickSize", "")
            elif filter_item.get("filterType") == "LOT_SIZE":
                min_size = filter_item.get("stepSize", "")

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "PERPETUAL",
            "symbol": canonical_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "settle_asset": quote_asset,
            "chain": "POLKADOT",  # Aster is on Polkadot chain
            "asset_class": "crypto",
            "venue_type": "exchange",  # Aster is an exchange, not a protocol
            "data_provider": "aster_api",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": symbol,
            "ccxt_symbol": symbol,  # CCXT may not support Aster, but keep raw symbol
            "ccxt_exchange": "",  # CCXT doesn't support Aster yet
            "available_from_datetime": available_from,
            "available_to_datetime": None,  # Perpetuals don't expire
            "data_types": "",  # Will be populated after testing data availability
            "inverse": False,
            "contract_size": None,
            "tick_size": tick_size,
            "min_size": min_size,
            "underlying": base_asset,
        }

    def test_candles_availability(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1m",
    ) -> bool:
        """
        Test if historical candles (OHLCV) are available for a symbol.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            start_date: Start date for testing
            end_date: End date for testing
            interval: Candle interval ("1m", "1h", etc.)

        Returns:
            True if candles are available
        """
        try:
            start_ms = int(start_date.timestamp() * 1000)
            end_ms = int(end_date.timestamp() * 1000)

            response = requests.get(
                f"{self.futures_api_base_url}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1,  # Just test if data exists
                },
                timeout=30,
            )

            if response.status_code == 200:
                klines = response.json()
                if klines and len(klines) > 0:
                    logger.info(f"✅ Historical candles ({interval}) available for {symbol}")
                    return True
                else:
                    logger.warning(f"⚠️ No candles found for {symbol} in date range")
            else:
                logger.warning(f"⚠️ Failed to fetch candles for {symbol}: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Error testing candles for {symbol}: {e}")

        return False
