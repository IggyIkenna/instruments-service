"""
Configuration for Instruments Service

Extracted from market-tick-data-handler for centralized instrument configuration.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os

# Try to import BaseServiceConfig from unified-cloud-services
try:
    from unified_cloud_services import BaseServiceConfig
    from pydantic import Field

    BASE_SERVICE_CONFIG_AVAILABLE = True
except ImportError:
    BASE_SERVICE_CONFIG_AVAILABLE = False
    # Fallback if unified-cloud-services not available
    BaseServiceConfig = None
    Field = None


@dataclass
class VenueMapping:
    """CANONICAL venue to exchange API mappings (centralized business logic)"""

    # ALL possible Tardis exchange endpoints (we'll call each to get complete data)
    all_tardis_exchanges: List[str] = field(
        default_factory=lambda: [
            "binance",
            "binance-futures",  # BINANCE split
            "deribit",  # DERIBIT unified
            "bybit",
            "bybit-spot",  # BYBIT unified
            "okex",
            "okex-futures",
            "okex-swap",  # OKX needs all endpoints for complete data
        ]
    )

    # Canonical TradFi venues (user-friendly names, not data source names)
    all_databento_venues: List[str] = field(
        default_factory=lambda: [
            "CME",  # Chicago Mercantile Exchange
            "NASDAQ",  # NASDAQ Stock Market
            "NYSE",  # New York Stock Exchange
            "ICE",  # Intercontinental Exchange
            "CBOE",  # Cboe Global Markets (for SPX options, VIX options)
        ]
    )

    # DeFi venues (multi-chain support: Ethereum, Plasma, Hyperliquid)
    all_defi_venues: List[str] = field(
        default_factory=lambda: [
            # Ethereum DEX protocols
            "UNISWAPV2-ETH",  # Uniswap V2 Ethereum
            "UNISWAPV3-ETH",  # Uniswap V3 Ethereum
            "UNISWAPV4-ETH",  # Uniswap V4 Ethereum
            "CURVE-ETH",  # Curve Ethereum
            "BALANCER-ETH",  # Balancer V2 Ethereum
            "AAVE_V3_ETH",  # AAVE V3 Ethereum
            "ETHERFI",  # EtherFi LST (Ethereum)
            "LIDO",  # Lido LST (Ethereum)
            "ETHENA",  # Ethena synthetic dollars (Ethereum)
            "MORPHO-ETHEREUM",  # Morpho lending protocol (Ethereum)
            # Plasma lending protocols
            "EULER-PLASMA",  # Euler lending (Plasma)
            "FLUID-PLASMA",  # Fluid lending (Plasma)
            "AAVE-PLASMA",  # AAVE Plasma market (Plasma)
            # Perpetual futures DEX
            "HYPERLIQUID",  # Hyperliquid perpetual futures (HyperEVM)
            "ASTER",  # Aster perpetual futures exchange
        ]
    )

    # All exchanges (Tardis endpoints + Databento venues + DeFi protocols) - default for instrument generation
    all_exchanges: List[str] = field(
        default_factory=lambda: [
            # Tardis exchanges (API endpoints)
            "binance",
            "binance-futures",
            "deribit",
            "bybit",
            "bybit-spot",
            "okex",
            "okex-futures",
            "okex-swap",
            # Databento venues (canonical venue names)
            "CME",
            "NASDAQ",
            "NYSE",
            "ICE",
            "CBOE",
            # DeFi venues (multi-chain: Ethereum, Plasma, Hyperliquid)
            "UNISWAPV2-ETH",
            "UNISWAPV3-ETH",
            "UNISWAPV4-ETH",
            "CURVE-ETH",
            "BALANCER-ETH",
            "AAVE_V3_ETH",
            "ETHERFI",
            "LIDO",
            "ETHENA",
            "MORPHO-ETHEREUM",
            "EULER-PLASMA",
            "FLUID-PLASMA",
            "AAVE-PLASMA",  # Plasma lending
            "HYPERLIQUID",
            "ASTER",  # Perpetual futures DEX
        ]
    )

    # Map canonical venues to Databento dataset identifiers
    # Databento uses dataset IDs (not exchange codes) for API calls
    venue_to_databento: Dict[str, str] = field(
        default_factory=lambda: {
            "CME": "GLBX.MDP3",  # CME Globex Market Data Platform 3.0
            "NASDAQ": "DBEQ.BASIC",  # Databento US Equities Basic (includes NASDAQ data)
            "NYSE": "DBEQ.BASIC",  # Databento US Equities Basic (includes NYSE data)
            "ICE": "IFEU.IMPACT",  # ICE Europe Commodities iMpact (for European commodities)
            "CBOE": "OPRA.PILLAR",  # Cboe Global Markets (SPX options via OPRA.PILLAR dataset)
        }
    )

    # Canonical venues to CCXT exchange IDs
    venue_to_ccxt: Dict[str, str] = field(
        default_factory=lambda: {
            "BINANCE-SPOT": "binance",
            "BINANCE-FUTURES": "binance",  # Same CCXT class, different market types
            "DERIBIT": "deribit",
            "BYBIT": "bybit",  # Unified
            "OKX": "okx",  # Unified
        }
    )

    # Reverse mapping for imports
    tardis_to_venue: Dict[str, str] = field(
        default_factory=lambda: {
            "binance": "BINANCE-SPOT",  # Fixed: binance spot should be BINANCE-SPOT
            "binance-futures": "BINANCE-FUTURES",
            "deribit": "DERIBIT",
            "bybit": "BYBIT",
            "bybit-spot": "BYBIT",
            "okex": "OKX",
            "okex-futures": "OKX",
            "okex-swap": "OKX",
        }
    )

    # MVP token list for DeFi pool discovery (configurable)
    # Can be overridden via environment variable DEFI_MVP_TOKENS (comma-separated)
    # Note: Only non-rebasing tokens (AAVE supports non-rebasing tokens like WSTETH, not STETH)
    defi_mvp_base_currencies: List[str] = field(
        default_factory=lambda: [
            "ETH",  # Native Ethereum
            "WETH",  # Wrapped ETH
            "BTC",  # Bitcoin (WBTC on Ethereum)
            "WBTC",  # Wrapped Bitcoin (explicitly include WBTC)
            "USDT",  # Tether
            "USDC",  # USD Coin
            "DAI",  # Dai stablecoin
            "weETH",  # EtherFi LST (Wrapped eETH) - non-rebasing, contract: 0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee
            "WSTETH",  # Lido LST (non-rebasing, wrapped version)
            # STETH removed - rebasing token, not supported by AAVE
        ]
    )

    def is_databento_venue(self, venue: str) -> bool:
        """Check if venue uses Databento (canonical venue name)."""
        return venue in self.all_databento_venues

    def is_tardis_exchange(self, exchange: str) -> bool:
        """Check if exchange uses Tardis (API endpoint name)."""
        return exchange in self.all_tardis_exchanges

    def is_defi_venue(self, venue: str) -> bool:
        """Check if venue is a DeFi protocol."""
        return venue in self.all_defi_venues

    def get_defi_mvp_tokens(self) -> List[str]:
        """Get MVP token list, checking environment variable first."""
        env_tokens = os.getenv("DEFI_MVP_TOKENS")
        if env_tokens:
            return [t.strip().upper() for t in env_tokens.split(",")]
        return self.defi_mvp_base_currencies

    def get_databento_exchange_id(self, venue: str) -> Optional[str]:
        """Get Databento exchange identifier for canonical venue."""
        return self.venue_to_databento.get(venue)

    # CRITICAL: Map venue+instrument_type → Tardis exchange endpoint
    # This determines which Tardis API endpoint to use for downloads
    venue_instrument_type_to_tardis: Dict[tuple, str] = field(
        default_factory=lambda: {
            # Binance mappings
            ("BINANCE-SPOT", "SPOT_PAIR"): "binance",
            ("BINANCE-FUTURES", "PERPETUAL"): "binance-futures",
            ("BINANCE-FUTURES", "FUTURE"): "binance-futures",
            # OKX mappings (CRITICAL: instrument_type determines endpoint)
            ("OKX", "SPOT_PAIR"): "okex",
            ("OKX", "PERPETUAL"): "okex-swap",
            ("OKX", "FUTURE"): "okex-futures",
            # Bybit mappings
            ("BYBIT", "SPOT_PAIR"): "bybit-spot",
            ("BYBIT", "PERPETUAL"): "bybit",
            ("BYBIT", "FUTURE"): "bybit",
            # Deribit (unified endpoint)
            ("DERIBIT", "SPOT_PAIR"): "deribit",
            ("DERIBIT", "PERPETUAL"): "deribit",
            ("DERIBIT", "FUTURE"): "deribit",
            ("DERIBIT", "OPTION"): "deribit",
        }
    )

    # Which Tardis exchanges map to which instrument types (for filtering)
    tardis_exchange_instrument_types: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "binance": ["SPOT_PAIR"],
            "binance-futures": ["PERPETUAL", "FUTURE"],
            "okex": ["SPOT_PAIR"],
            "okex-swap": ["PERPETUAL"],
            "okex-futures": ["FUTURE"],
            "bybit": ["PERPETUAL", "FUTURE"],
            "bybit-spot": ["SPOT_PAIR"],
            "deribit": ["SPOT_PAIR", "PERPETUAL", "FUTURE", "OPTION"],
        }
    )


@dataclass
class DataTypeConfig:
    """CRITICAL: Data types per instrument type (fixes 66% false positives)"""

    instrument_data_types: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "SPOT_PAIR": ["trades", "book_snapshot_5"],
            "PERPETUAL": [
                "trades",
                "book_snapshot_5",
                "derivative_ticker",
                "liquidations",
            ],
            "FUTURE": [
                "trades",
                "book_snapshot_5",
                "derivative_ticker",
                "liquidations",
            ],
            "OPTION": ["options_chain"],
        }
    )

    default_data_types: List[str] = field(
        default_factory=lambda: [
            "trades",
            "book_snapshot_5",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ]
    )

    # Instrument type filters (exclude complex types we don't want to process)
    excluded_instrument_types: List[str] = field(
        default_factory=lambda: ["combo"]  # Exclude Deribit combo strategies
    )

    # Complex option strategy filters (Deribit specific - exclude complex strategies)
    excluded_deribit_strategies: List[str] = field(
        default_factory=lambda: [
            "PS-",
            "STRG-",
            "CBUT-",
            "CCOND-",
            "PDIAG-",
            "PBUT-",
            "ICOND-",
            "BOX-",
            "FS-",
            "RR-",
            "CSR12-",
            "PSR12-",
            "CSR13-",
            "PSR13-",
            "CCAL-",
            "CDIAG-",
        ]
    )


@dataclass
class ExchangeInstrumentConfig:
    """Valid instrument types and quote currencies per exchange (CORRECTED canonical venues)"""

    exchange_instrument_types: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "BINANCE-SPOT": ["SPOT_PAIR"],  # Spot only (fixed: BINANCE -> BINANCE-SPOT)
            "BINANCE-FUTURES": ["PERPETUAL", "FUTURE"],  # Derivatives only (keep split)
            "DERIBIT": ["PERPETUAL", "FUTURE", "OPTION"],  # Full derivatives exchange
            "BYBIT": ["SPOT_PAIR", "PERPETUAL"],  # Combined (no split per user)
            "OKX": ["SPOT_PAIR", "PERPETUAL", "FUTURE"],  # Combined (no split per user)
        }
    )

    valid_quote_currencies: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "BINANCE-SPOT": [
                "USDT"
            ],  # STRICT: Only USDT (no BNB, ETH, BTC quotes) (fixed: BINANCE -> BINANCE-SPOT)
            "BINANCE-FUTURES": ["USDT"],  # STRICT: Only USDT
            "DERIBIT": ["USD", "USDC"],  # Options exchange (verified real data)
            "BYBIT": ["USDT"],  # STRICT: Only USDT
            "OKX": ["USDT"],  # STRICT: Only USDT (filter out USD quotes)
        }
    )

    derivative_exchanges: List[str] = field(
        default_factory=lambda: [
            "DERIBIT",
            "BINANCE-FUTURES",
            "OKX",
            "BYBIT",
        ]
    )

    # Excluded base currencies per exchange (e.g., deprecated tokens, leveraged products)
    excluded_base_currencies: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "OKX": ["USTC"],  # USTC (Terra Classic) deprecated, no longer needed
            "BYBIT": [],  # No base currency exclusions for BYBIT (handled by symbol patterns)
        }
    )

    # Excluded symbol patterns per exchange (e.g., leveraged products, deprecated instruments)
    excluded_symbol_patterns: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "BYBIT": [
                "3L",  # 3x leveraged products (no longer exist)
                "2L",  # 2x leveraged products (no longer exist)
            ],
            "OKX": [],  # No symbol pattern exclusions for OKX
        }
    )


@dataclass
class DatabentoInstrumentConfig:
    """Extended TradFi instrument symbols for Databento downloads"""

    # Mapping from Databento exchange codes to human-readable names for canonical symbols
    # This is used for our canonical symbol/instrument_key, NOT for exchange_raw_symbol or databento_symbol
    exchange_code_to_name: Dict[str, str] = field(
        default_factory=lambda: {
            # FX Futures (CME) - Base currency is the currency, quote is always USD
            "6A": "AUD",
            "M6A": "AUD",  # Australian Dollar / USD
            "6B": "GBP",
            "M6B": "GBP",  # British Pound / USD
            "6E": "EUR",
            "M6E": "EUR",  # Euro / USD
            "6J": "JPY",
            "M6J": "JPY",  # Japanese Yen / USD
            "6C": "CAD",
            "M6C": "CAD",  # Canadian Dollar / USD
            "6N": "NZD",
            "M6N": "NZD",  # New Zealand Dollar / USD
            "6S": "CHF",
            "M6S": "CHF",  # Swiss Franc / USD
            "6M": "MXN",  # Mexican Peso / USD
            "6Z": "ZAR",  # South African Rand / USD
            "6L": "BRL",  # Brazilian Real / USD
            # Commodities (CME) - All commodities quote in USD
            "CL": "CRUDE",
            "MCL": "CRUDE",  # WTI Crude Oil / USD (CME)
            "GC": "GOLD",
            "MGC": "GOLD",  # Gold / USD
            "NG": "NATGAS",
            "MNG": "NATGAS",  # Natural Gas / USD
            "SI": "SILVER",
            "MSI": "SILVER",  # Silver / USD
            "HG": "COPPER",
            "MHG": "COPPER",  # Copper / USD
            "SB": "SUGAR",  # Sugar / USD (CME)
            "KC": "COFFEE",  # Coffee / USD (CME)
            "CT": "COTTON",  # Cotton / USD
            "CC": "COCOA",  # Cocoa / USD (CME)
            "OJ": "OJ",  # Orange Juice / USD (keep as OJ)
            "ZS": "SOYBEANS",  # Soybeans / USD
            "ZC": "CORN",  # Corn / USD
            "ZW": "WHEAT",  # Wheat / USD
            "ZL": "SOYBEAN_OIL",  # Soybean Oil / USD (using underscore to avoid hyphen conflict)
            "ZM": "SOYBEAN_MEAL",  # Soybean Meal / USD (using underscore to avoid hyphen conflict)
            # Commodities (ICE) - European commodities quote in USD
            "BRN": "BRENT",  # Brent Crude Oil / USD (ICE) - different from CME's WTI (CL)
            "B": "BRENT",  # Alternative Brent symbol
            "G": "GASOIL",  # Gasoil / USD (ICE)
            # Note: ICE also trades Sugar, Coffee, Cocoa but with different contract specs
            # These are typically distinguished by exchange/venue, not symbol code
            # Equity Index Futures (CME) - Index / USD
            "ES": "SP500",
            "MES": "SP500",  # S&P 500 / USD
            "NQ": "NASDAQ100",
            "MNQ": "NASDAQ100",  # Nasdaq 100 / USD
        }
    )

    def get_human_readable_name(self, exchange_code: str) -> str:
        """
        Convert Databento exchange code to human-readable name for canonical symbols.

        Args:
            exchange_code: Exchange code (e.g., '6A', 'CL', 'ES')

        Returns:
            Human-readable name (e.g., 'AUD', 'CRUDE', 'SP500') or original code if not found
        """
        # Check exact match first
        if exchange_code in self.exchange_code_to_name:
            return self.exchange_code_to_name[exchange_code]

        # Check if it's a micro version (M prefix)
        if exchange_code.startswith("M") and len(exchange_code) > 1:
            base_code = exchange_code[1:]
            if base_code in self.exchange_code_to_name:
                return self.exchange_code_to_name[base_code]

        # Return original if no mapping found
        return exchange_code

    # Extended symbol list (comprehensive coverage)
    # NOTE: Using full-size contracts (not micro) for maximum liquidity and longest trading hours
    # CME contracts generally have the longest trading hours and highest liquidity
    extended_symbols: List[str] = field(
        default_factory=lambda: [
            # Core equity index futures (full-size for maximum liquidity)
            "ES.FUT",
            "NQ.FUT",  # E-mini S&P 500, E-mini Nasdaq 100 (most liquid equity index futures)
            # Commodities (full-size for maximum liquidity)
            "GC.FUT",
            "CL.FUT",
            "NG.FUT",
            "SI.FUT",
            "HG.FUT",  # Gold, Crude Oil, Natural Gas, Silver, Copper
            "SB.FUT",
            "KC.FUT",
            "CT.FUT",
            "CC.FUT",  # Sugar, Coffee, Cotton, Cocoa
            "OJ.FUT",
            "ZS.FUT",
            "ZC.FUT",
            "ZW.FUT",
            "ZL.FUT",
            "ZM.FUT",  # Orange Juice, Soybeans, Corn, Wheat, Soybean Oil, Soybean Meal
            # FX Futures (full-size for maximum liquidity - G10 currencies)
            "6E.FUT",
            "6B.FUT",
            "6J.FUT",
            "6A.FUT",
            "6C.FUT",
            "6N.FUT",
            "6S.FUT",  # EUR, GBP, JPY, AUD, CAD, NZD, CHF
            "6M.FUT",
            "6Z.FUT",
            "6L.FUT",  # MXN, ZAR, BRL
            # ETFs
            "SPY",
            "QQQ",
            # Single stocks (top 10 by market cap)
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "TSLA",
            "NVDA",
            "META",
            "BRK.B",
            # ICE commodities (European commodities)
            # ICE uses different symbology: BRN for Brent, G for Gasoil
            "BRN.FUT",  # Brent Crude Oil (ICE) - parent symbol
            "G.FUT",  # Gasoil (ICE) - parent symbol
            # Options (OPRA.PILLAR dataset)
            # Use parent symbology (.OPT suffix) to get all options for an underlying
            # SPY options are more liquid than SPX options (smaller contract size, more accessible)
            # ONLY include SPY options (not SPX) for maximum liquidity
            "SPY.OPT",  # SPY ETF Options (most liquid - 100 shares per contract)
        ]
    )

    # S&P 500 stocks (will be loaded from external source or generated)
    # For now, using top stocks as placeholder - full list should be loaded from CSV or API
    sp500_stocks: List[str] = field(
        default_factory=lambda: [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "TSLA",
            "NVDA",
            "META",
            "BRK.B",
            # Note: Full S&P 500 list should be loaded from external source
            # This is a placeholder - actual implementation should fetch full list
        ]
    )

    # Dataset routing: symbol -> (dataset, stype_in)
    # NOTE: Using full-size contracts for maximum liquidity
    dataset_routing: Dict[str, tuple] = field(
        default_factory=lambda: {
            # Equity Index Futures (full-size for maximum liquidity)
            "ES.FUT": ("GLBX.MDP3", "parent"),  # E-mini S&P 500
            "NQ.FUT": ("GLBX.MDP3", "parent"),  # E-mini Nasdaq 100
            # Commodities (full-size for maximum liquidity)
            "GC.FUT": ("GLBX.MDP3", "parent"),  # Gold
            "CL.FUT": ("GLBX.MDP3", "parent"),  # Crude Oil (WTI)
            "NG.FUT": ("GLBX.MDP3", "parent"),  # Natural Gas
            "SI.FUT": ("GLBX.MDP3", "parent"),  # Silver
            "HG.FUT": ("GLBX.MDP3", "parent"),  # Copper
            "SB.FUT": ("GLBX.MDP3", "parent"),  # Sugar
            "KC.FUT": ("GLBX.MDP3", "parent"),  # Coffee
            "CT.FUT": ("GLBX.MDP3", "parent"),  # Cotton
            "CC.FUT": ("GLBX.MDP3", "parent"),  # Cocoa
            "OJ.FUT": ("GLBX.MDP3", "parent"),  # Orange Juice
            "ZS.FUT": ("GLBX.MDP3", "parent"),  # Soybeans
            "ZC.FUT": ("GLBX.MDP3", "parent"),  # Corn
            "ZW.FUT": ("GLBX.MDP3", "parent"),  # Wheat
            "ZL.FUT": ("GLBX.MDP3", "parent"),  # Soybean Oil
            "ZM.FUT": ("GLBX.MDP3", "parent"),  # Soybean Meal
            # FX Futures (full-size for maximum liquidity)
            "6E.FUT": ("GLBX.MDP3", "parent"),  # EUR/USD
            "6B.FUT": ("GLBX.MDP3", "parent"),  # GBP/USD
            "6J.FUT": ("GLBX.MDP3", "parent"),  # JPY/USD
            "6A.FUT": ("GLBX.MDP3", "parent"),  # AUD/USD
            "6C.FUT": ("GLBX.MDP3", "parent"),  # CAD/USD
            "6N.FUT": ("GLBX.MDP3", "parent"),  # NZD/USD
            "6S.FUT": ("GLBX.MDP3", "parent"),  # CHF/USD
            "6M.FUT": ("GLBX.MDP3", "parent"),  # MXN/USD
            "6Z.FUT": ("GLBX.MDP3", "parent"),  # ZAR/USD
            "6L.FUT": ("GLBX.MDP3", "parent"),  # BRL/USD
            # ICE commodities (IFEU.IMPACT dataset)
            "BRN.FUT": ("IFEU.IMPACT", "parent"),  # Brent Crude Oil (ICE)
            "G.FUT": ("IFEU.IMPACT", "parent"),  # Gasoil (ICE)
            # Options (OPRA.PILLAR dataset)
            # Use parent symbology to get all options for an underlying
            # SPY options are more liquid than SPX options (smaller contract size, more accessible)
            # ONLY include SPY options (not SPX) for maximum liquidity
            "SPY.OPT": ("OPRA.PILLAR", "parent"),  # SPY ETF Options (most liquid - 100 shares per contract)
            # Equities/ETFs (raw symbols)
            "SPY": ("DBEQ.BASIC", "raw_symbol"),
            "QQQ": ("DBEQ.BASIC", "raw_symbol"),
            "AAPL": ("DBEQ.BASIC", "raw_symbol"),
            "MSFT": ("DBEQ.BASIC", "raw_symbol"),
            "GOOGL": ("DBEQ.BASIC", "raw_symbol"),
            "AMZN": ("DBEQ.BASIC", "raw_symbol"),
            "TSLA": ("DBEQ.BASIC", "raw_symbol"),
            "NVDA": ("DBEQ.BASIC", "raw_symbol"),
            "META": ("DBEQ.BASIC", "raw_symbol"),
            "BRK.B": ("DBEQ.BASIC", "raw_symbol"),
        }
    )

    def get_dataset_and_stype(self, symbol: str) -> tuple[str, str]:
        """Get dataset and stype_in for a symbol"""
        if symbol in self.dataset_routing:
            return self.dataset_routing[symbol]

        # Default routing based on symbol pattern
        if symbol.endswith(".FUT"):
            return ("GLBX.MDP3", "parent")
        else:
            # Assume equity/ETF
            return ("DBEQ.BASIC", "raw_symbol")


# Service-level configuration (extends BaseServiceConfig if available)
if BASE_SERVICE_CONFIG_AVAILABLE and BaseServiceConfig is not None:

    class InstrumentsServiceConfig(BaseServiceConfig):
        """
        Service-level configuration for instruments-service.

        Extends BaseServiceConfig with instruments-specific settings.
        """

        service_name: str = Field(
            default="instruments-service", description="Service name"
        )

        # Instruments-specific configuration
        enable_ccxt_integration: bool = Field(
            default=True, description="Enable CCXT metadata enrichment"
        )
        enable_metadata_caching: bool = Field(
            default=True, description="Enable metadata caching"
        )
        cache_ttl_hours: int = Field(default=24, description="Cache TTL in hours")
        max_batch_size: int = Field(
            default=1000, description="Maximum batch size for processing"
        )
        lookback_days: int = Field(
            default=0, description="Lookback days for batch processing"
        )

        # GCS and BigQuery defaults for instruments
        gcs_bucket: str = Field(
            default_factory=lambda: os.getenv(
                "INSTRUMENTS_GCS_BUCKET", "instruments-store"
            ),
            description="GCS bucket for instruments",
        )
        bigquery_dataset: str = Field(
            default_factory=lambda: os.getenv(
                "INSTRUMENTS_BIGQUERY_DATASET", "instruments"
            ),
            description="BigQuery dataset for instruments",
        )

        def get_cloud_target(self):
            """Get CloudTarget for instruments service."""
            from unified_cloud_services import CloudTarget

            return CloudTarget(
                project_id=self.gcp_project_id,
                gcs_bucket=self.gcs_bucket,
                bigquery_dataset=self.bigquery_dataset,
                bigquery_location=self.bigquery_location,
            )

else:
    # Fallback if BaseServiceConfig not available
    class InstrumentsServiceConfig:
        """Fallback service config if BaseServiceConfig not available."""

        def __init__(self, **kwargs):
            self.service_name = kwargs.get("service_name", "instruments-service")
            self.enable_ccxt_integration = kwargs.get("enable_ccxt_integration", True)
            self.enable_metadata_caching = kwargs.get("enable_metadata_caching", True)
            self.cache_ttl_hours = kwargs.get("cache_ttl_hours", 24)
            self.max_batch_size = kwargs.get("max_batch_size", 1000)
            self.lookback_days = kwargs.get("lookback_days", 0)
            self.gcs_bucket = kwargs.get(
                "gcs_bucket", os.getenv("INSTRUMENTS_GCS_BUCKET", "instruments-store")
            )
            self.bigquery_dataset = kwargs.get(
                "bigquery_dataset",
                os.getenv("INSTRUMENTS_BIGQUERY_DATASET", "instruments"),
            )
            self.gcp_project_id = kwargs.get(
                "gcp_project_id", os.getenv("GCP_PROJECT_ID", "central-element-323112")
            )
            self.bigquery_location = kwargs.get(
                "bigquery_location", os.getenv("BIGQUERY_LOCATION", "asia-northeast1")
            )  # Default to asia-northeast1 per .env
