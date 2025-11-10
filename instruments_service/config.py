"""
Configuration for Instruments Service

Unified instrument configuration with all instruments, mappings, and metadata in one place.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
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
class InstrumentDefinition:
    """Single instrument definition with all metadata"""
    symbol: str  # Databento symbol (e.g., "ES.FUT", "SPY", "BRN.FUT", "SPY.OPT")
    venue: str  # Canonical venue (e.g., "CME", "NASDAQ", "ICE")
    instrument_type: str  # "FUTURE", "EQUITY", "OPTION", "ETF"
    dataset: str  # Databento dataset (e.g., "GLBX.MDP3", "DBEQ.BASIC")
    stype_in: str  # "parent" for futures/options, "raw_symbol" for equities/ETFs
    base_asset: Optional[str] = None  # Human-readable base asset name
    quote_asset: str = "USD"  # Quote currency (default USD for TradFi)
    exchange_code: Optional[str] = None  # Databento exchange code (e.g., "ES", "CL")


@dataclass
class UnifiedInstrumentConfig:
    """
    Unified instrument configuration - single source of truth for all instruments.
    
    All TradFi instruments are defined here with their metadata. No duplication.
    Options are handled explicitly via instrument_type="OPTION".
    """
    
    # Single unified list of all TradFi instruments
    # Symbols are in Databento API format: [ROOT].FUT for futures, [ROOT].OPT for options, raw symbols for equities
    instruments: List[InstrumentDefinition] = field(
        default_factory=lambda: [
            # Equity Index Futures (CME) - use .FUT suffix for parent symbology
            InstrumentDefinition("ES.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SP500", "USD", "ES"),
            InstrumentDefinition("NQ.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "NASDAQ100", "USD", "NQ"),
            
            # Commodities (CME) - use .FUT suffix
            InstrumentDefinition("GC.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "GOLD", "USD", "GC"),
            InstrumentDefinition("CL.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "CRUDE", "USD", "CL"),
            InstrumentDefinition("NG.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "NATGAS", "USD", "NG"),
            InstrumentDefinition("SI.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SILVER", "USD", "SI"),
            InstrumentDefinition("HG.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "COPPER", "USD", "HG"),
            InstrumentDefinition("SB.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SUGAR", "USD", "SB"),
            InstrumentDefinition("KC.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "COFFEE", "USD", "KC"),
            InstrumentDefinition("CT.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "COTTON", "USD", "CT"),
            InstrumentDefinition("CC.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "COCOA", "USD", "CC"),
            InstrumentDefinition("OJ.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "OJ", "USD", "OJ"),
            InstrumentDefinition("ZS.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SOYBEANS", "USD", "ZS"),
            InstrumentDefinition("ZC.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "CORN", "USD", "ZC"),
            InstrumentDefinition("ZW.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "WHEAT", "USD", "ZW"),
            InstrumentDefinition("ZL.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SOYBEAN_OIL", "USD", "ZL"),
            InstrumentDefinition("ZM.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "SOYBEAN_MEAL", "USD", "ZM"),
            
            # FX Futures (CME) - use .FUT suffix
            InstrumentDefinition("6E.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "EUR", "USD", "6E"),
            InstrumentDefinition("6B.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "GBP", "USD", "6B"),
            InstrumentDefinition("6J.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "JPY", "USD", "6J"),
            InstrumentDefinition("6A.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "AUD", "USD", "6A"),
            InstrumentDefinition("6C.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "CAD", "USD", "6C"),
            InstrumentDefinition("6N.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "NZD", "USD", "6N"),
            InstrumentDefinition("6S.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "CHF", "USD", "6S"),
            InstrumentDefinition("6M.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "MXN", "USD", "6M"),
            InstrumentDefinition("6Z.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "ZAR", "USD", "6Z"),
            InstrumentDefinition("6L.FUT", "CME", "FUTURE", "GLBX.MDP3", "parent", "BRL", "USD", "6L"),
            
            # ICE Commodities - use .FUT suffix, dataset is IFEU.IMPACT
            InstrumentDefinition("BRN.FUT", "ICE", "FUTURE", "IFEU.IMPACT", "parent", "BRENT", "USD", "BRN"),
            InstrumentDefinition("G.FUT", "ICE", "FUTURE", "IFEU.IMPACT", "parent", "GASOIL", "USD", "G"),
            
            # Equities/ETFs (NASDAQ/NYSE) - use raw_symbol stype_in (no .FUT/.OPT suffix)
            InstrumentDefinition("SPY", "NASDAQ", "ETF", "DBEQ.BASIC", "raw_symbol", "SPY", "USD"),
            InstrumentDefinition("QQQ", "NASDAQ", "ETF", "DBEQ.BASIC", "raw_symbol", "QQQ", "USD"),
            InstrumentDefinition("AAPL", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "AAPL", "USD"),
            InstrumentDefinition("MSFT", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "MSFT", "USD"),
            InstrumentDefinition("GOOGL", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "GOOGL", "USD"),
            InstrumentDefinition("AMZN", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "AMZN", "USD"),
            InstrumentDefinition("TSLA", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "TSLA", "USD"),
            InstrumentDefinition("NVDA", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "NVDA", "USD"),
            InstrumentDefinition("META", "NASDAQ", "EQUITY", "DBEQ.BASIC", "raw_symbol", "META", "USD"),
            InstrumentDefinition("BRK.B", "NYSE", "EQUITY", "DBEQ.BASIC", "raw_symbol", "BRK.B", "USD"),
            
            # Options (CBOE) - use .OPT suffix for parent symbology
            InstrumentDefinition("SPY.OPT", "CBOE", "OPTION", "OPRA.PILLAR", "parent", "SPY", "USD"),
        ]
    )
    
    # Exchange code to human-readable name mapping (for canonical symbols)
    exchange_code_to_name: Dict[str, str] = field(
        default_factory=lambda: {
            # FX Futures
            "6A": "AUD", "M6A": "AUD", "6B": "GBP", "M6B": "GBP",
            "6E": "EUR", "M6E": "EUR", "6J": "JPY", "M6J": "JPY",
            "6C": "CAD", "M6C": "CAD", "6N": "NZD", "M6N": "NZD",
            "6S": "CHF", "M6S": "CHF", "6M": "MXN", "6Z": "ZAR", "6L": "BRL",
            # Commodities
            "CL": "CRUDE", "MCL": "CRUDE", "GC": "GOLD", "MGC": "GOLD",
            "NG": "NATGAS", "MNG": "NATGAS", "SI": "SILVER", "MSI": "SILVER",
            "HG": "COPPER", "MHG": "COPPER", "SB": "SUGAR", "KC": "COFFEE",
            "CT": "COTTON", "CC": "COCOA", "OJ": "OJ",
            "ZS": "SOYBEANS", "ZC": "CORN", "ZW": "WHEAT",
            "ZL": "SOYBEAN_OIL", "ZM": "SOYBEAN_MEAL",
            # ICE
            "BRN": "BRENT", "B": "BRENT", "G": "GASOIL",
            # Equity Index Futures
            "ES": "SP500", "MES": "SP500", "NQ": "NASDAQ100", "MNQ": "NASDAQ100",
        }
    )
    
    def get_symbols_for_venue(self, venue: str) -> List[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        return [inst.symbol for inst in self.instruments if inst.venue == venue.upper()]
    
    def get_symbols_for_dataset(self, dataset: str) -> List[str]:
        """Get all symbols for a dataset (e.g., 'GLBX.MDP3', 'DBEQ.BASIC')"""
        return [inst.symbol for inst in self.instruments if inst.dataset == dataset]
    
    def get_symbols_by_type(self, instrument_type: str) -> List[str]:
        """Get all symbols for an instrument type (e.g., 'FUTURE', 'EQUITY', 'OPTION')"""
        return [inst.symbol for inst in self.instruments if inst.instrument_type == instrument_type.upper()]
    
    def get_dataset_and_stype(self, symbol: str) -> Optional[Tuple[str, str]]:
        """Get dataset and stype_in for a symbol"""
        for inst in self.instruments:
            if inst.symbol == symbol:
                return (inst.dataset, inst.stype_in)
        return None
    
    def get_instrument(self, symbol: str, venue: Optional[str] = None) -> Optional[InstrumentDefinition]:
        """Get instrument definition by symbol (optionally filtered by venue)"""
        for inst in self.instruments:
            if inst.symbol == symbol:
                if venue is None or inst.venue == venue.upper():
                    return inst
        return None
    
    def get_human_readable_name(self, exchange_code: str) -> str:
        """Convert Databento exchange code to human-readable name"""
        if exchange_code in self.exchange_code_to_name:
            return self.exchange_code_to_name[exchange_code]
        # Check micro version (M prefix)
        if exchange_code.startswith("M") and len(exchange_code) > 1:
            base_code = exchange_code[1:]
            if base_code in self.exchange_code_to_name:
                return self.exchange_code_to_name[base_code]
        return exchange_code


# Legacy compatibility: Keep DatabentoInstrumentConfig as a wrapper
@dataclass
class DatabentoInstrumentConfig:
    """
    Legacy wrapper for UnifiedInstrumentConfig.
    
    Maintains backward compatibility while using unified config internally.
    """
    
    def __init__(self):
        self._unified = UnifiedInstrumentConfig()
    
    @property
    def extended_symbols(self) -> List[str]:
        """All symbols (for backward compatibility)"""
        return [inst.symbol for inst in self._unified.instruments]
    
    @property
    def sp500_stocks(self) -> List[str]:
        """S&P 500 stocks (subset of equities)"""
        return self._unified.get_symbols_by_type("EQUITY")
    
    def get_dataset_and_stype(self, symbol: str) -> Tuple[str, str]:
        """Get dataset and stype_in for a symbol"""
        result = self._unified.get_dataset_and_stype(symbol)
        if result:
            return result
        # Default fallback
        if symbol.endswith(".FUT") or any(inst.symbol == symbol.replace(".FUT", "") for inst in self._unified.instruments if inst.instrument_type == "FUTURE"):
            return ("GLBX.MDP3", "parent")
        return ("DBEQ.BASIC", "raw_symbol")
    
    def get_human_readable_name(self, exchange_code: str) -> str:
        """Convert exchange code to human-readable name"""
        return self._unified.get_human_readable_name(exchange_code)
    
    def get_symbols_for_venue(self, venue: str) -> List[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        return self._unified.get_symbols_for_venue(venue)


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

    # All exchanges (computed from above - no duplication)
    @property
    def all_exchanges(self) -> List[str]:
        """All exchanges (Tardis + Databento + DeFi)"""
        return (
            self.all_tardis_exchanges
            + self.all_databento_venues
            + self.all_defi_venues
        )

    # Map canonical venues to Databento dataset identifiers
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
                "3L",  # 3x leveraged LONG products (no longer exist)
                "2L",  # 2x leveraged LONG products (no longer exist)
                "3S",  # 3S (3x leveraged SHORT products)
                "2S",  # 2S (2x leveraged SHORT products)
            ],
            "OKX": [],  # No symbol pattern exclusions for OKX
        }
    )


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
