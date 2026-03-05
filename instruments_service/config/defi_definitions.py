"""
DeFi Protocol Definitions

Contains all DeFi protocol configurations including:
- Venue to protocol mappings for TheGraph/subgraph adapters
- List of all DeFi protocols to process
"""

# DeFi protocol configs (Issue #91)
# Maps canonical venue keys to (protocol, chain) for TheGraph/subgraph adapters
DEFI_VENUE_TO_PROTOCOL: dict[str, tuple[str, str | None]] = {
    "HYPERLIQUID": ("hyperliquid", None),
    "ASTER": ("aster", None),
    "UNISWAPV2-ETH": ("uniswap_v2", "ETHEREUM"),
    "UNISWAPV3-ETH": ("uniswap_v3", "ETHEREUM"),
    "UNISWAPV4-ETH": ("uniswap_v4", "ETHEREUM"),
    "CURVE-ETH": ("curve", "ETHEREUM"),
    "AAVE_V3_ETH": ("aave_v3", "ETHEREUM"),
    "ETHERFI": ("etherfi", "ETHEREUM"),
    "LIDO": ("lido", "ETHEREUM"),
    "MORPHO-ETHEREUM": ("morpho", "ETHEREUM"),
    "EULER-PLASMA": ("euler_plasma", None),
    "FLUID-PLASMA": ("fluid_plasma", None),
    "AAVE-PLASMA": ("aave_plasma", None),
    "ETHENA": ("ethena", "ETHEREUM"),
}

# All DeFi protocols to process when no venue filter is specified
DEFI_PROTOCOLS: list[tuple[str, str | None]] = [
    ("uniswap_v2", "ETHEREUM"),
    ("uniswap_v3", "ETHEREUM"),
    ("uniswap_v4", "ETHEREUM"),
    ("curve", "ETHEREUM"),
    ("balancer", "ETHEREUM"),
    ("aave_v3", "ETHEREUM"),
    ("etherfi", "ETHEREUM"),
    ("lido", "ETHEREUM"),
    ("morpho", "ETHEREUM"),
    ("euler_plasma", None),
    ("fluid_plasma", None),
    ("aave_plasma", None),
    ("hyperliquid", None),
    ("aster", None),
    ("ethena", "ETHEREUM"),
]
