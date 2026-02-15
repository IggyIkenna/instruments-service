"""
Morpho Adapter

Fetches Morpho lending market instruments using Morpho Blue API and on-chain contracts.
Generates canonical instrument keys for Morpho positions.

Reference: instruments-service/docs/MVP_INSTRUMENTS.md (DeFi section)

NOTE: Venue start dates are centralized in unified_cloud_services.models.VenueMapping.venue_start_dates
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from unified_cloud_services import VenueMapping, get_secret_with_fallback, handle_api_errors
from web3 import Web3

from instruments_service.app.venues.defi.base_defi_adapter import BaseDefiAdapter
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)

# Centralized venue config
_venue_mapping = VenueMapping()


WEB3_AVAILABLE = True

# Morpho Blue AdaptiveCurveIRM constants (immutable contract)
# Deployed at: 0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC
# Source: https://github.com/morpho-org/morpho-blue-irm
ADAPTIVE_CURVE_IRM_ADDRESS = "0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC"
ADAPTIVE_CURVE_IRM_PARAMS = {
    "optimal_utilization_rate": 0.90,  # 90% target utilization (immutable)
    "base_variable_borrow_rate": 0.01,  # ~1% at 0% utilization (approximate)
    "variable_rate_slope1": 0.04,  # Gentle slope below 90% (approximate)
    "variable_rate_slope2": 3.0,  # Steep slope above 90% (approximate)
}

# Morpho Blue API
MORPHO_API_URL = "https://blue-api.morpho.org/graphql"

# MVP loan tokens we care about
MVP_LOAN_TOKENS = {
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
}


class MorphoAdapter(BaseDefiAdapter):
    """
    Adapter for fetching Morpho lending market instruments.

    Generates instruments in format:
    MORPHO-ETHEREUM:A_TOKEN:AUSDT@ETHEREUM
    MORPHO-ETHEREUM:DEBT_TOKEN:DEBTWETH@ETHEREUM
    """

    def __init__(
        self,
        chain: str = "ETHEREUM",
        rpc_url: Optional[str] = None,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Initialize Morpho adapter.

        Args:
            chain: Chain identifier (default: 'ETHEREUM')
            rpc_url: Optional RPC URL for contract interaction (defaults to env var or Secret Manager)
            api_key: Optional API key (not used by Morpho but required by base class)
            project_id: GCP project ID for Secret Manager
        """
        super().__init__(chain=chain, api_key=api_key, project_id=project_id)
        self.venue = "MORPHO-ETHEREUM"  # Per config.py

        # Initialize web3 provider for contract interaction
        self.web3 = None
        if WEB3_AVAILABLE:
            try:
                # Get RPC URL from parameter, env var, or Secret Manager
                if not rpc_url:
                    try:
                        # Try to get Alchemy API key from Secret Manager
                        alchemy_key = get_secret_with_fallback(
                            project_id=instruments_config.gcp_project_id,
                            secret_name=instruments_config.alchemy_secret_name,
                            fallback_env_var="ALCHEMY_API_KEY",
                        )
                        if alchemy_key:
                            # Construct Alchemy RPC URL from API key
                            rpc_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key.strip()}"
                            logger.info("✅ Constructed Ethereum RPC URL from Alchemy API key")
                        else:
                            # Fallback to direct RPC URL from env var
                            rpc_url = instruments_config.ethereum_rpc_url
                            if rpc_url:
                                logger.info("✅ Using Ethereum RPC URL from environment variable")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get RPC URL from Secret Manager: {e}")
                        rpc_url = instruments_config.ethereum_rpc_url

                if rpc_url:
                    self.web3 = Web3(Web3.HTTPProvider(rpc_url))
                    if self.web3.is_connected():
                        logger.info(f"✅ Connected to Ethereum RPC: {rpc_url[:50]}...")
                    else:
                        logger.warning("⚠️ Failed to connect to Ethereum RPC")
                        self.web3 = None
                else:
                    logger.warning("⚠️ No RPC URL provided - Morpho contract interaction disabled")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize web3: {e}")

        # Morpho Blue contract addresses (Ethereum mainnet)
        self.morpho_blue_address = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

        # Cache for market configurations
        self._market_config_cache: Dict[str, Dict[str, Any]] = {}

        logger.info(f"✅ MorphoAdapter initialized for chain: {self.chain}")

    @handle_api_errors(max_retries=3)
    async def get_instrument_metadata(self) -> List[Dict[str, Any]]:
        """
        Get instrument metadata for Morpho.

        Returns:
            List of instrument definition dictionaries
        """
        instruments = self.fetch_markets()
        return list(instruments.values())

    def fetch_markets(self, target_date: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Morpho markets and convert to instrument definitions.

        Args:
            target_date: Target date for instrument availability check

        Returns:
            Dictionary mapping instrument_key to instrument definition
        """
        # Check if target_date is before venue launch (using centralized config)
        if target_date and not _venue_mapping.is_venue_available_on_date(self.venue, target_date):
            start_date = _venue_mapping.get_venue_start_date(self.venue)
            logger.info(
                f"ℹ️ {self.venue} not available for {target_date.strftime('%Y-%m-%d')} "
                f"(launched {start_date}). Returning empty instruments - this is expected."
            )
            return {}

        instruments = {}

        try:
            # Fetch markets from Morpho Blue API + on-chain contract
            markets = self._fetch_markets_from_api()

            if not markets:
                logger.warning("⚠️ No markets from Morpho API, falling back to MVP defaults")
                markets = self._get_mvp_markets_fallback()

            for market in markets:
                try:
                    # Generate aToken instrument (supply position)
                    a_token_def = self._create_a_token_instrument(market)
                    if a_token_def:
                        instruments[a_token_def["instrument_key"]] = a_token_def

                    # Generate debtToken instrument (borrow position)
                    debt_token_def = self._create_debt_token_instrument(market)
                    if debt_token_def:
                        instruments[debt_token_def["instrument_key"]] = debt_token_def

                except Exception as e:
                    logger.warning(f"Failed to process Morpho market {market.get('symbol')}: {e}")
                    continue

            logger.info(f"✅ Generated {len(instruments)} Morpho instruments")
            return instruments

        except Exception as e:
            logger.error(f"Failed to fetch Morpho markets: {e}")
            return {}

    def _fetch_markets_from_api(self) -> List[Dict[str, Any]]:
        """
        Fetch Morpho Blue markets from the Morpho API (blue-api.morpho.org).

        Queries for markets where the loan token is one of our MVP tokens (WETH, USDT, USDC).
        Returns enriched market data including market IDs and LLTV.

        Returns:
            List of market dictionaries with real data
        """
        # Query Morpho Blue API for markets with MVP loan tokens
        query = """
        query {
            markets(
                where: {
                    chainId_in: [1]
                    whitelisted: true
                }
                first: 100
            ) {
                items {
                    uniqueKey
                    lltv
                    loanAsset {
                        address
                        symbol
                        decimals
                    }
                    collateralAsset {
                        address
                        symbol
                    }
                    irmAddress
                    oracleAddress
                    state {
                        supplyApy
                        borrowApy
                        utilization
                        supplyAssetsUsd
                        borrowAssetsUsd
                    }
                }
            }
        }
        """

        try:
            response = requests.post(
                MORPHO_API_URL,
                json={"query": query},
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            data = response.json()

            all_markets = data.get("data", {}).get("markets", {}).get("items", [])
            if not all_markets:
                logger.warning("⚠️ No markets returned from Morpho API")
                return []

            logger.info(f"📊 Fetched {len(all_markets)} total markets from Morpho API")

            # Filter to MVP loan tokens and pick the deepest market per loan token
            mvp_addresses = {addr.lower() for addr in MVP_LOAN_TOKENS.values()}
            mvp_markets_by_token: Dict[str, Dict[str, Any]] = {}

            for market in all_markets:
                loan_asset = market.get("loanAsset", {})
                loan_address = (loan_asset.get("address") or "").lower()
                loan_symbol = loan_asset.get("symbol", "")

                if loan_address not in mvp_addresses:
                    continue

                # Pick the market with the most supply (deepest liquidity)
                state = market.get("state", {})
                supply_usd = float(state.get("supplyAssetsUsd") or 0)

                existing = mvp_markets_by_token.get(loan_symbol)
                existing_supply = float((existing.get("state", {}).get("supplyAssetsUsd") or 0)) if existing else 0

                if supply_usd > existing_supply:
                    mvp_markets_by_token[loan_symbol] = market

            # Convert to our market format
            result = []
            for symbol, market in mvp_markets_by_token.items():
                loan_asset = market.get("loanAsset", {})
                collateral_asset = market.get("collateralAsset", {})
                lltv_raw = market.get("lltv")

                # Convert lltv from string (WAD 1e18) to decimal
                lltv_decimal = None
                if lltv_raw:
                    try:
                        lltv_decimal = float(lltv_raw) / 1e18
                    except (ValueError, TypeError):
                        try:
                            lltv_decimal = float(lltv_raw)
                            # If already a decimal < 1, use as-is
                            if lltv_decimal > 1:
                                lltv_decimal = lltv_decimal / 1e18
                        except (ValueError, TypeError):
                            pass

                irm_address = market.get("irmAddress", "")

                # Determine IRM parameters
                irm_params = self._get_irm_params(irm_address)

                # Calculate liquidation parameters from lltv
                liquidation_threshold = lltv_decimal if lltv_decimal else None
                liquidation_bonus = None
                if lltv_decimal and lltv_decimal > 0:
                    # Morpho liquidation incentive: 1/lltv - 1
                    liquidation_bonus = (1.0 / lltv_decimal) - 1.0

                market_entry = {
                    "symbol": loan_asset.get("symbol", ""),
                    "underlyingAsset": loan_asset.get("address", ""),
                    "aToken": {"address": ""},  # Morpho doesn't have aToken contracts
                    "variableDebtToken": {"address": ""},
                    "marketId": market.get("uniqueKey", ""),
                    "collateralAsset": collateral_asset.get("address", ""),
                    "collateralSymbol": collateral_asset.get("symbol", ""),
                    "ltv": lltv_decimal,
                    "liquidation_threshold": liquidation_threshold,
                    "liquidation_bonus": liquidation_bonus,
                    "irm_address": irm_address,
                    "oracle_address": market.get("oracleAddress", ""),
                    **irm_params,
                }

                state = market.get("state", {})
                logger.info(
                    f"  📊 Morpho {symbol}: LLTV={lltv_decimal:.2%}, "
                    f"supply=${float(state.get('supplyAssetsUsd') or 0):,.0f}, "
                    f"utilization={float(state.get('utilization') or 0):.1%}, "
                    f"collateral={collateral_asset.get('symbol', '?')}"
                )

                result.append(market_entry)

            logger.info(f"✅ Selected {len(result)} MVP markets from Morpho API")
            return result

        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Failed to fetch from Morpho API: {e}")
            return []
        except Exception as e:
            logger.warning(f"⚠️ Unexpected error fetching Morpho markets: {e}")
            return []

    def _get_irm_params(self, irm_address: str) -> Dict[str, Any]:
        """
        Get IRM (Interest Rate Model) parameters for a given IRM contract address.

        The AdaptiveCurveIRM is the standard Morpho Blue IRM. Its parameters are
        immutable constants baked into the contract at deployment.

        Args:
            irm_address: IRM contract address

        Returns:
            Dictionary with IRM parameters
        """
        if irm_address and irm_address.lower() == ADAPTIVE_CURVE_IRM_ADDRESS.lower():
            return ADAPTIVE_CURVE_IRM_PARAMS.copy()

        # Unknown IRM — return the standard params as best estimate
        # (virtually all Morpho Blue markets use the AdaptiveCurveIRM)
        logger.debug(f"Unknown IRM address {irm_address}, using AdaptiveCurveIRM defaults")
        return ADAPTIVE_CURVE_IRM_PARAMS.copy()

    def _get_mvp_markets_fallback(self) -> List[Dict[str, Any]]:
        """
        Fallback MVP markets when API is unavailable.
        Uses hardcoded market data with known IRM parameters.

        Returns:
            List of market dictionaries
        """
        return [
            {
                "symbol": "WETH",
                "underlyingAsset": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
                "marketId": "",
                "ltv": 0.86,  # Common LLTV for WETH markets on Morpho
                "liquidation_threshold": 0.86,
                "liquidation_bonus": 0.1628,  # 1/0.86 - 1
                **ADAPTIVE_CURVE_IRM_PARAMS,
            },
            {
                "symbol": "USDT",
                "underlyingAsset": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "aToken": {"address": ""},
                "variableDebtToken": {"address": ""},
                "marketId": "",
                "ltv": 0.86,
                "liquidation_threshold": 0.86,
                "liquidation_bonus": 0.1628,
                **ADAPTIVE_CURVE_IRM_PARAMS,
            },
        ]

    def _extract_lending_metadata(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract lending protocol metadata from market data.

        Uses data from the Morpho API response (already enriched with IRM params).

        Args:
            market: Market data (enriched from API or fallback)

        Returns:
            Dictionary with lending protocol metadata fields
        """
        # Morpho Blue contract address (flash loan provider)
        flash_loan_providers = self.morpho_blue_address if self.web3 else None

        return {
            "flash_loan_providers": flash_loan_providers,
            "instadapp_routing": None,
            "ltv": market.get("ltv"),
            "liquidation_threshold": market.get("liquidation_threshold"),
            "liquidation_bonus": market.get("liquidation_bonus"),
            "reserve_factor": None,  # Not applicable to Morpho
            "emode_category_id": None,
            "emode_label": None,
            "emode_underlying": None,
            "emode_liquidation_threshold": None,
            "emode_liquidation_bonus": None,
            "optimal_utilization_rate": market.get("optimal_utilization_rate"),
            "base_variable_borrow_rate": market.get("base_variable_borrow_rate"),
            "variable_rate_slope1": market.get("variable_rate_slope1"),
            "variable_rate_slope2": market.get("variable_rate_slope2"),
        }

    def _create_a_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create aToken instrument definition (supply position).

        Args:
            market: Market data

        Returns:
            Instrument definition dictionary or None
        """
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        a_token_address = market.get("aToken", {}).get("address", "")

        if not symbol:
            return None

        # Build canonical instrument key
        a_token_symbol = f"A{symbol}"  # e.g., AWETH, AUSDT
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
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": a_token_address if a_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "morpho",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": a_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2023, 6, 1, tzinfo=timezone.utc
            ).isoformat(),  # Morpho Blue launch date (June 2023)
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization,flash_loan_availability",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,
        }

    def _create_debt_token_instrument(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create debtToken instrument definition (borrow position).

        Args:
            market: Market data

        Returns:
            Instrument definition dictionary or None
        """
        symbol = market.get("symbol", "")
        underlying_address = market.get("underlyingAsset", "")
        debt_token_address = market.get("variableDebtToken", {}).get("address", "")

        if not symbol:
            return None

        # Build canonical instrument key
        debt_token_symbol = f"DEBT{symbol}"  # e.g., DEBTWETH, DEBTUSDT
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
            "base_asset_contract_address": underlying_address,
            "quote_asset_contract_address": None,
            "pool_address": debt_token_address if debt_token_address else None,
            "pool_fee_tier": None,
            "chain": self.chain,
            "asset_class": "crypto",
            "venue_type": "protocol",
            "data_provider": "morpho",
            "tardis_exchange": "",
            "tardis_symbol": "",
            "exchange_raw_symbol": debt_token_symbol,
            "ccxt_symbol": "",
            "ccxt_exchange": "",
            "available_from_datetime": datetime(
                2023, 6, 1, tzinfo=timezone.utc
            ).isoformat(),  # Morpho Blue launch date (June 2023)
            "available_to_datetime": None,
            "data_types": "rate_indices,utilization,flash_loan_availability",
            "inverse": False,
            "contract_size": None,
            "tick_size": "",
            "min_size": "",
            "underlying": symbol,
            **lending_metadata,
        }
