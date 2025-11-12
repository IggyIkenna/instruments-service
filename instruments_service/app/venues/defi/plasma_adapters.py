"""
Plasma Chain Adapters

Fetches lending market instruments from Plasma chain protocols:
- Euler-Plasma
- Fluid-Plasma
- AAVE-Plasma

Plasma is a new L1 blockchain focused on stablecoin lending with high incentives.
Generates canonical instrument keys for Plasma lending positions.

Reference: edeg_strategy.md
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Try to import web3 for contract interaction
try:
    from web3 import Web3

    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    logger.warning("⚠️ web3 not available - Plasma contract interaction disabled")


class EulerPlasmaAdapter:
    """
    Adapter for fetching Euler lending market instruments on Plasma.

    Generates instruments in format:
    EULER-PLASMA:A_TOKEN:AUSDT@PLASMA
    EULER-PLASMA:DEBT_TOKEN:DEBTUSDT@PLASMA
    """

    def __init__(self, rpc_url: Optional[str] = None):
        """
        Initialize Euler-Plasma adapter.

        Args:
            rpc_url: Optional Plasma RPC URL (defaults to env var or Secret Manager)
        """
        self.chain = "PLASMA"
        self.venue = "EULER-PLASMA"

        # Initialize web3 provider for Plasma contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                # Get RPC URL from parameter, env var, or Secret Manager
                if not rpc_url:
                    try:
                        from unified_cloud_services import get_secret_with_fallback

                        # Try to get Alchemy API key from Secret Manager (same as Morpho)
                        alchemy_key = get_secret_with_fallback(
                            project_id=os.getenv("GCP_PROJECT_ID", "central-element-323112"),
                            secret_name="alchemy-api-key",
                            fallback_env_var="ALCHEMY_API_KEY",
                        )
                        if alchemy_key:
                            # Construct Alchemy RPC URL from API key (Plasma is on Ethereum)
                            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
                            logger.info("✅ Constructed Plasma RPC URL from Alchemy API key")
                        else:
                            # Fallback to direct RPC URL from env var
                            rpc_url = os.getenv("PLASMA_RPC_URL") or os.getenv("ETHEREUM_RPC_URL")
                            if rpc_url:
                                logger.info("✅ Using Plasma RPC URL from environment variable")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get Plasma RPC URL from Secret Manager: {e}")
                        rpc_url = os.getenv("PLASMA_RPC_URL") or os.getenv("ETHEREUM_RPC_URL")

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info(f"✅ Connected to Plasma RPC: {rpc_url[:50]}...")
                    else:
                        logger.warning("⚠️ Failed to connect to Plasma RPC")
                        self.web3 = None
                else:
                    logger.warning("⚠️ No Plasma RPC URL provided - contract interaction disabled")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize web3 for Plasma: {e}")

        # Cache for market configurations
        self._market_config_cache: Dict[str, Dict[str, Any]] = {}

        logger.info(f"✅ EulerPlasmaAdapter initialized")

    def fetch_markets(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Euler-Plasma markets and convert to instrument definitions.

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        instruments = {}

        try:
            # Fetch markets (using known markets for MVP)
            # TODO: Integrate with Euler SDK or Plasma API when available
            markets = self._get_mvp_markets()

            for market in markets:
                try:
                    # Generate aToken instrument
                    a_token_def = self._create_a_token_instrument(market)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    # Generate debtToken instrument
                    debt_token_def = self._create_debt_token_instrument(market)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(
                        f"Failed to process Euler-Plasma market {market.get('symbol')}: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} Euler-Plasma instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Euler-Plasma markets: {e}")
            return {}

    def _get_mvp_markets(self) -> List[Dict[str, Any]]:
        """Get MVP markets for Euler-Plasma."""
        # MVP markets: USDT0, syrupUSDT (Plasma stablecoins)
        return [
            {
                "symbol": "USDT0",
                "underlyingAsset": "",  # TODO: Get actual Plasma addresses
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
            {
                "symbol": "syrupUSDT",
                "underlyingAsset": "",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
        ]

    def _extract_lending_metadata(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from market data.

        Note: Plasma protocol metadata should be fetched from:
        - Plasma protocol contracts directly (via web3)
        - The Graph subgraph for Plasma protocols

        Args:
            market: Market data

        Returns:
            Dictionary with lending protocol metadata fields
        """
        return {
            "flash_loan_providers": None,  # TODO: Fetch from Plasma protocol contracts
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": None,  # TODO: Fetch from Plasma protocol contracts
            "liquidation_threshold": None,  # TODO: Fetch from Plasma protocol contracts
            "liquidation_bonus": None,  # TODO: Fetch from Plasma protocol contracts
            "reserve_factor": None,  # TODO: Fetch from Plasma protocol contracts
            "emode_category_id": None,  # Not applicable to Plasma protocols
            "emode_label": None,  # Not applicable to Plasma protocols
            "emode_underlying": None,  # Not applicable to Plasma protocols
            "emode_liquidation_threshold": None,  # Not applicable to Plasma protocols
            "emode_liquidation_bonus": None,  # Not applicable to Plasma protocols
            "optimal_utilization_rate": None,  # TODO: Fetch from Plasma interest rate model
            "base_variable_borrow_rate": None,  # TODO: Fetch from Plasma protocol contracts
            "variable_rate_slope1": None,  # TODO: Fetch from Plasma interest rate model
            "variable_rate_slope2": None,  # TODO: Fetch from Plasma interest rate model
        }

    def _create_a_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create aToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        a_token_address = market.get("aToken", {}).get("address", "")

        if not symbol:
            return None

        a_token_symbol = f"A{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "A_TOKEN",
            "symbol": a_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": a_token_address if a_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "euler_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }

    def _create_debt_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create debtToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        debt_token_address = market.get("variableDebtToken", {}).get("address", "")

        if not symbol:
            return None

        debt_token_symbol = f"DEBT{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "DEBT_TOKEN",
            "symbol": debt_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address if debt_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "euler_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }


class FluidPlasmaAdapter:
    """
    Adapter for fetching Fluid lending market instruments on Plasma.

    Generates instruments in format:
    FLUID-PLASMA:A_TOKEN:AUSDT@PLASMA
    FLUID-PLASMA:DEBT_TOKEN:DEBTUSDT@PLASMA
    """

    def __init__(self, rpc_url: Optional[str] = None):
        """
        Initialize Fluid-Plasma adapter.

        Args:
            rpc_url: Optional Plasma RPC URL (defaults to env var or Secret Manager)
        """
        self.chain = "PLASMA"
        self.venue = "FLUID-PLASMA"

        # Initialize web3 provider for Plasma contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                if not rpc_url:
                    try:
                        from unified_cloud_services import get_secret_with_fallback

                        rpc_url = get_secret_with_fallback(
                            project_id=os.getenv("GCP_PROJECT_ID", "central-element-323112"),
                            secret_name="plasma-rpc-url",
                            fallback_env_var="PLASMA_RPC_URL",
                        )
                    except Exception:
                        rpc_url = os.getenv("PLASMA_RPC_URL")

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info(f"✅ Connected to Plasma RPC: {rpc_url[:50]}...")
                    else:
                        self.web3 = None
            except Exception:
                pass

        self._market_config_cache: Dict[str, Dict[str, Any]] = {}
        logger.info(f"✅ FluidPlasmaAdapter initialized")

    def _fetch_market_config_from_contract(self, market_address: str) -> Optional[Dict[str, Any]]:
        """Fetch market configuration from Plasma protocol contract."""
        if not self.web3 or not WEB3_AVAILABLE or not market_address:
            return None
        if market_address in self._market_config_cache:
            return self._market_config_cache[market_address]
        # TODO: Implement contract interaction based on Fluid-Plasma protocol ABI
        logger.debug(
            f"⚠️ Contract interaction not yet implemented for Fluid-Plasma market: {market_address}"
        )
        return None

    def _extract_lending_metadata(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from market data.

        Fetches data from:
        - Plasma protocol contracts directly (via web3)
        - The Graph subgraph for Plasma protocols (if available)

        Args:
            market: Market data

        Returns:
            Dictionary with lending protocol metadata fields
        """
        market_address = market.get("marketAddress") or market.get("address")
        market_config = None
        if market_address and self.web3:
            market_config = self._fetch_market_config_from_contract(market_address)

        return {
            "flash_loan_providers": None,  # TODO: Fetch from Plasma protocol contracts
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": market_config.get("ltv") if market_config else None,
            "liquidation_threshold": (
                market_config.get("liquidation_threshold") if market_config else None
            ),
            "liquidation_bonus": (
                market_config.get("liquidation_bonus") if market_config else None
            ),
            "reserve_factor": (market_config.get("reserve_factor") if market_config else None),
            "emode_category_id": None,  # Not applicable to Plasma protocols
            "emode_label": None,  # Not applicable to Plasma protocols
            "emode_underlying": None,  # Not applicable to Plasma protocols
            "emode_liquidation_threshold": None,  # Not applicable to Plasma protocols
            "emode_liquidation_bonus": None,  # Not applicable to Plasma protocols
            "optimal_utilization_rate": (
                market_config.get("optimal_utilization_rate") if market_config else None
            ),
            "base_variable_borrow_rate": (
                market_config.get("base_variable_borrow_rate") if market_config else None
            ),
            "variable_rate_slope1": (
                market_config.get("variable_rate_slope1") if market_config else None
            ),
            "variable_rate_slope2": (
                market_config.get("variable_rate_slope2") if market_config else None
            ),
        }

    def fetch_markets(self) -> Dict[str, Dict[str, Any]]:
        """Fetch Fluid-Plasma markets and convert to instrument definitions."""
        instruments = {}

        try:
            markets = self._get_mvp_markets()

            for market in markets:
                try:
                    a_token_def = self._create_a_token_instrument(market)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    debt_token_def = self._create_debt_token_instrument(market)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(
                        f"Failed to process Fluid-Plasma market {market.get('symbol')}: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} Fluid-Plasma instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Fluid-Plasma markets: {e}")
            return {}

    def _get_mvp_markets(self) -> List[Dict[str, Any]]:
        """Get MVP markets for Fluid-Plasma."""
        return [
            {
                "symbol": "USDT0",
                "underlyingAsset": "",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
            {
                "symbol": "syrupUSDT",
                "underlyingAsset": "",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
        ]

    def _create_a_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create aToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        a_token_address = market.get("aToken", {}).get("address", "")

        if not symbol:
            return None

        a_token_symbol = f"A{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "A_TOKEN",
            "symbol": a_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": a_token_address if a_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "fluid_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }

    def _create_debt_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create debtToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        debt_token_address = market.get("variableDebtToken", {}).get("address", "")

        if not symbol:
            return None

        debt_token_symbol = f"DEBT{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "DEBT_TOKEN",
            "symbol": debt_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address if debt_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "fluid_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }


class AavePlasmaAdapter:
    """
    Adapter for fetching AAVE lending market instruments on Plasma.

    Generates instruments in format:
    AAVE-PLASMA:A_TOKEN:AUSDT@PLASMA
    AAVE-PLASMA:DEBT_TOKEN:DEBTUSDT@PLASMA
    """

    def __init__(self, rpc_url: Optional[str] = None):
        """
        Initialize AAVE-Plasma adapter.

        Args:
            rpc_url: Optional Plasma RPC URL (defaults to env var or Secret Manager)
        """
        self.chain = "PLASMA"
        self.venue = "AAVE-PLASMA"

        # Initialize web3 provider for Plasma contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                if not rpc_url:
                    try:
                        from unified_cloud_services import get_secret_with_fallback

                        rpc_url = get_secret_with_fallback(
                            project_id=os.getenv("GCP_PROJECT_ID", "central-element-323112"),
                            secret_name="plasma-rpc-url",
                            fallback_env_var="PLASMA_RPC_URL",
                        )
                    except Exception:
                        rpc_url = os.getenv("PLASMA_RPC_URL")

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info(f"✅ Connected to Plasma RPC: {rpc_url[:50]}...")
                    else:
                        self.web3 = None
            except Exception:
                pass

        self._market_config_cache: Dict[str, Dict[str, Any]] = {}
        logger.info(f"✅ AavePlasmaAdapter initialized")

    def _fetch_market_config_from_contract(self, market_address: str) -> Optional[Dict[str, Any]]:
        """Fetch market configuration from Plasma protocol contract."""
        if not self.web3 or not WEB3_AVAILABLE or not market_address:
            return None
        if market_address in self._market_config_cache:
            return self._market_config_cache[market_address]
        # TODO: Implement contract interaction based on AAVE-Plasma protocol ABI
        logger.debug(
            f"⚠️ Contract interaction not yet implemented for AAVE-Plasma market: {market_address}"
        )
        return None

    def _extract_lending_metadata(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from market data.

        Fetches data from:
        - Plasma protocol contracts directly (via web3)
        - The Graph subgraph for Plasma protocols (if available)

        Args:
            market: Market data

        Returns:
            Dictionary with lending protocol metadata fields
        """
        market_address = market.get("marketAddress") or market.get("address")
        market_config = None
        if market_address and self.web3:
            market_config = self._fetch_market_config_from_contract(market_address)

        return {
            "flash_loan_providers": None,  # TODO: Fetch from Plasma protocol contracts
            "instadapp_routing": None,  # TODO: Fetch from Instadapp if applicable
            "ltv": market_config.get("ltv") if market_config else None,
            "liquidation_threshold": (
                market_config.get("liquidation_threshold") if market_config else None
            ),
            "liquidation_bonus": (
                market_config.get("liquidation_bonus") if market_config else None
            ),
            "reserve_factor": (market_config.get("reserve_factor") if market_config else None),
            "emode_category_id": None,  # Not applicable to Plasma protocols
            "emode_label": None,  # Not applicable to Plasma protocols
            "emode_underlying": None,  # Not applicable to Plasma protocols
            "emode_liquidation_threshold": None,  # Not applicable to Plasma protocols
            "emode_liquidation_bonus": None,  # Not applicable to Plasma protocols
            "optimal_utilization_rate": (
                market_config.get("optimal_utilization_rate") if market_config else None
            ),
            "base_variable_borrow_rate": (
                market_config.get("base_variable_borrow_rate") if market_config else None
            ),
            "variable_rate_slope1": (
                market_config.get("variable_rate_slope1") if market_config else None
            ),
            "variable_rate_slope2": (
                market_config.get("variable_rate_slope2") if market_config else None
            ),
        }

    def fetch_markets(self) -> Dict[str, Dict[str, Any]]:
        """Fetch AAVE-Plasma markets and convert to instrument definitions."""
        instruments = {}

        try:
            markets = self._get_mvp_markets()

            for market in markets:
                try:
                    a_token_def = self._create_a_token_instrument(market)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    debt_token_def = self._create_debt_token_instrument(market)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(
                        f"Failed to process AAVE-Plasma market {market.get('symbol')}: {e}"
                    )
                    continue

            logger.info(f"✅ Generated {len(instruments)} AAVE-Plasma instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch AAVE-Plasma markets: {e}")
            return {}

    def _get_mvp_markets(self) -> List[Dict[str, Any]]:
        """Get MVP markets for AAVE-Plasma."""
        return [
            {
                "symbol": "USDT0",
                "underlyingAsset": "",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
            {
                "symbol": "syrupUSDT",
                "underlyingAsset": "",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
            },
        ]

    def _create_a_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create aToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        a_token_address = market.get("aToken", {}).get("address", "")

        if not symbol:
            return None

        a_token_symbol = f"A{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:A_TOKEN:{a_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "A_TOKEN",
            "symbol": a_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": a_token_address if a_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aave_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }

    def _create_debt_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create debtToken instrument definition."""
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        debt_token_address = market.get("variableDebtToken", {}).get("address", "")

        if not symbol:
            return None

        debt_token_symbol = f"DEBT{symbol}"
        chain_suffix = f"@{self.chain}"
        instrument_key = f"{self.venue}:DEBT_TOKEN:{debt_token_symbol}{chain_suffix}"

        # Extract lending protocol metadata
        lending_metadata = self._extract_lending_metadata(market)

        return {
            "instrument_key": instrument_key,
            "venue": self.venue,
            "instrument_type": "DEBT_TOKEN",
            "symbol": debt_token_symbol,
            "base_asset": symbol,
            "quote_asset": "",
            "settle_asset": symbol,
            "base_asset_contract_address": (underlying_address if underlying_address else None),
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address if debt_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,  # Chain identifier (PLASMA)
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "aave_plasma",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2024, 11, 1
            ).isoformat(),  # Plasma chain launch (Nov 2024)
            "available_to_datetime": None,
            "data_types": "trades,book_snapshot_5",  # Protocol positions - no market data but need valid data_types
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,  # Include all lending protocol metadata fields
        }
