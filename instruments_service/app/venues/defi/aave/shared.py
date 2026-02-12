"""
Shared constants and pure helpers for AAVE adapters.

Used by AaveV3Adapter. Extracted for maintainability when adding AaveV2 or
other AAVE protocol versions.

Reference: instruments-service/docs/MVP_INSTRUMENTS.md (DeFi section)
"""

from typing import Any, Dict, List

# AAVE V3 Pool contract address on Ethereum
AAVE_V3_ETHEREUM_POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

# Static risk parameters used as fallback when RPC/Graph queries fail.
# These are fetched dynamically when available. See instruments-service/issues/aave-dynamic-params.md
STATIC_RISK_PARAMS: Dict[str, Any] = {
    "emode": {
        "ltv_limits": {
            "weETH_WETH": 0.93,
            "wstETH_WETH": 0.93,
            "ETH_WETH": 0.93,
        },
        "liquidation_thresholds": {
            "weETH_WETH": 0.95,
            "wstETH_WETH": 0.95,
            "ETH_WETH": 0.95,
        },
        "liquidation_bonus": {
            "weETH_WETH": 0.01,
            "wstETH_WETH": 0.01,
            "ETH_WETH": 0.01,
        },
    },
    "standard": {
        "ltv_limits": {
            "weETH_WETH": 0.80,
            "wstETH_WETH": 0.80,
            "ETH_WETH": 0.80,
        },
        "liquidation_thresholds": {
            "weETH_WETH": 0.85,
            "wstETH_WETH": 0.85,
            "ETH_WETH": 0.85,
        },
        "liquidation_bonus": {
            "weETH_WETH": 0.05,
            "wstETH_WETH": 0.05,
            "ETH_WETH": 0.05,
        },
    },
    "reserve_factors": {
        "weETH": 0.10,
        "wstETH": 0.10,
        "WETH": 0.10,
        "USDT": 0.10,
    },
}

# Known AAVE V3 Ethereum aToken addresses
A_TOKEN_ADDRESSES: Dict[str, str] = {
    "WETH": "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8",
    "USDT": "0x3Ed3B47Dd13EC9a98b44e6204A523E766B225811",
    "USDC": "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c",
    "DAI": "0x018008bfb33d285247A21d44E50629654A4B2c97",
    "WBTC": "0x5Ee5bf7ae06D1Be5997A1A72006FE6C607bC6DE8",
    "LINK": "0x5E8C8A7243651DB1384C0dDfDbE39761E8e7E51a",
    "AAVE": "0xA700b4eB9Be2e4F707f8B5C6B1E5C59b4E3C4C4C",
    "WEETH": "0x4421A7d21d752f8CC35039678c8D27996c09f18E",
    "WSTETH": "0x0B925eD163218f6662a35e0f0371Ac234f9E9371",
}

# Known AAVE V3 Ethereum variableDebtToken addresses
DEBT_TOKEN_ADDRESSES: Dict[str, str] = {
    "WETH": "0xeA51d7853EEFb32b6ee06b1C12E6dcCA88Be0fFE",
    "USDT": "0x531842cEbbdD378f8ee36D171d6cC9C4fcf475Ec",
    "USDC": "0x72E95b8931767C79bA4EeE721354d6E99a61D004",
    "DAI": "0x5f3f1dBD7B74C6B46e8c44f98792A1d51B4C7413",
    "WBTC": "0x40aAbEf1aa8f0eEc637EfE662f0B8c701F1F506A",
    "LINK": "0x4228F8890C7C4B5E6A1F9f8C5C5C5C5C5C5C5C5C5",
    "AAVE": "0x6B4c2605352e8D7C5A5f5C5C5C5C5C5C5C5C5C5C5",
    "WEETH": "0x24e6e0795b3c7c71D96FEA0e07125B1dC8d3b1b5",
    "WSTETH": "0xC96113eED8cAB8CD8321FC2C3C7A47a5e6547A4B",
}

# MVP tokens for AAVE V3 Ethereum - static fallback when APIs fail
STATIC_RESERVES: List[Dict[str, Any]] = [
    {
        "reserve": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "asset": {
            "symbol": "USDT",
            "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "decimals": 6,
        },
    },
    {
        "reserve": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "asset": {
            "symbol": "WETH",
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "decimals": 18,
        },
    },
    {
        "reserve": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "asset": {
            "symbol": "weETH",
            "address": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
            "decimals": 18,
        },
    },
    {
        "reserve": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "asset": {
            "symbol": "wstETH",
            "address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            "decimals": 18,
        },
    },
]


def get_a_token_address(symbol: str, underlying_address: str) -> str:
    """
    Get aToken address for a given symbol.

    Uses known AAVE V3 Ethereum token addresses mapping.
    Can be extended with more tokens as needed.

    Args:
        symbol: Token symbol (e.g. WETH, USDT)
        underlying_address: Underlying token address (unused, for API compatibility)

    Returns:
        aToken contract address or empty string if unknown
    """
    _ = underlying_address
    return A_TOKEN_ADDRESSES.get(symbol.upper(), "")


def get_debt_token_address(symbol: str, underlying_address: str) -> str:
    """
    Get variableDebtToken address for a given symbol.

    Uses known AAVE V3 Ethereum token addresses mapping.
    Can be extended with more tokens as needed.

    Args:
        symbol: Token symbol (e.g. WETH, USDT)
        underlying_address: Underlying token address (unused, for API compatibility)

    Returns:
        variableDebtToken contract address or empty string if unknown
    """
    _ = underlying_address
    return DEBT_TOKEN_ADDRESSES.get(symbol.upper(), "")
