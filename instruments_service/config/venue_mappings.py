"""
Venue mappings for TradFi and DeFi protocols.

Maps venues to their configurations and protocol associations.
"""

# TradFi venue mappings for Databento
TRADFI_VENUE_MAPPINGS: list[dict[str, str | None]] = [
    {
        "venue": "CME",
        "description": "Chicago Mercantile Exchange",
        "asset_class": "futures",
        "dataset": "GLBX.MDP3",
    },
    {
        "venue": "ICE",
        "description": "Intercontinental Exchange",
        "asset_class": "futures",
        "dataset": "IFEU.IMPACT",
    },
    {
        "venue": "CBOE",
        "description": "Chicago Board Options Exchange",
        "asset_class": "options",
        "dataset": "OPRA.PILLAR",
    },
    {
        "venue": "NASDAQ",
        "description": "NASDAQ Stock Market",
        "asset_class": "equities",
        "dataset": "XNAS.ITCH",
    },
    {
        "venue": "NYSE",
        "description": "New York Stock Exchange",
        "asset_class": "equities",
        "dataset": "XNYS.TRADES",
    },
    {
        "venue": "FX",
        "description": "Foreign Exchange",
        "asset_class": "fx",
        "dataset": None,
    },
]

# DeFi protocol to venue mapping
DEFI_VENUE_TO_PROTOCOL: dict[str, tuple[str, str | None]] = {
    # Ethereum DEXs
    "UNISWAP": ("uniswap", "ETHEREUM"),
    "UNISWAP_V2": ("uniswap-v2", "ETHEREUM"),
    "UNISWAP_V3": ("uniswap-v3", "ETHEREUM"),
    "SUSHISWAP": ("sushiswap", "ETHEREUM"),
    "CURVE": ("curve", "ETHEREUM"),
    "BALANCER": ("balancer", "ETHEREUM"),
    "PANCAKESWAP": ("pancakeswap", "BSC"),
    "QUICKSWAP": ("quickswap", "POLYGON"),
    # Arbitrum DEXs
    "UNISWAP_ARBITRUM": ("uniswap-v3", "ARBITRUM"),
    "SUSHISWAP_ARBITRUM": ("sushiswap", "ARBITRUM"),
    "CAMELOT": ("camelot", "ARBITRUM"),
    # Polygon DEXs
    "UNISWAP_POLYGON": ("uniswap-v3", "POLYGON"),
    # Base DEXs
    "UNISWAP_BASE": ("uniswap-v3", "BASE"),
    "AERODROME": ("aerodrome", "BASE"),
    # Optimism DEXs
    "UNISWAP_OPTIMISM": ("uniswap-v3", "OPTIMISM"),
    "VELODROME": ("velodrome", "OPTIMISM"),
    # Other chains
    "RAYDIUM": ("raydium", "SOLANA"),
    "ORCA": ("orca", "SOLANA"),
    "TRADER_JOE": ("trader-joe", "AVALANCHE"),
    "SPOOKYSWAP": ("spookyswap", "FANTOM"),
}

# List of DeFi protocols for processing
DEFI_PROTOCOLS: list[tuple[str, str | None]] = [
    ("uniswap-v2", "ETHEREUM"),
    ("uniswap-v3", "ETHEREUM"),
    ("uniswap-v3", "ARBITRUM"),
    ("uniswap-v3", "POLYGON"),
    ("uniswap-v3", "BASE"),
    ("uniswap-v3", "OPTIMISM"),
    ("sushiswap", "ETHEREUM"),
    ("curve", "ETHEREUM"),
    ("balancer", "ETHEREUM"),
    ("pancakeswap", "BSC"),
    ("quickswap", "POLYGON"),
    ("aerodrome", "BASE"),
    ("velodrome", "OPTIMISM"),
    ("raydium", "SOLANA"),
]

TRADFI_INSTRUMENTS_CONFIG: list[dict[str, str | None]] = TRADFI_VENUE_MAPPINGS

__all__ = [
    "DEFI_PROTOCOLS",
    "DEFI_VENUE_TO_PROTOCOL",
    "TRADFI_INSTRUMENTS_CONFIG",
    "TRADFI_VENUE_MAPPINGS",
]
