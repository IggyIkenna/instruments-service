"""
Configuration for Instruments Service

Unified instrument configuration with all instruments, mappings, and metadata in one place.
Includes both domain-specific instrument definitions and service-level runtime configuration.

Note: For InstrumentDefinition Pydantic model (with full validation), use instruments_service.models.
This file contains TradFiInstrument dataclass for static TradFi instrument configuration.
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from pydantic import Field, AliasChoices
from pydantic_settings import SettingsConfigDict

# Import from unified-cloud-services (required dependency)
from unified_cloud_services import (
    UnifiedCloudServicesConfig,
    VenueMapping,
    DataTypeConfig,
    ExchangeInstrumentConfig,
    CloudTarget,
)

logger = logging.getLogger(__name__)

# ============================================================================
# DATA LOADING FROM JSON FILES
# ============================================================================

# Caches for loaded data
_sp500_tickers_cache: Optional[List[str]] = None
_nasdaq_tickers_cache: Optional[List[str]] = None
_tradfi_instruments_cache: Optional[List[Dict]] = None
_exchange_code_to_name_cache: Optional[Dict[str, str]] = None


def _get_data_dir() -> Path:
    """Get the data directory path."""
    return Path(__file__).parent / "data"


def _load_sp500_tickers() -> Tuple[List[str], List[str]]:
    """Load S&P 500 tickers from JSON file."""
    global _sp500_tickers_cache, _nasdaq_tickers_cache
    
    if _sp500_tickers_cache is not None:
        return _sp500_tickers_cache, _nasdaq_tickers_cache or []
    
    try:
        data_file = _get_data_dir() / "sp500_tickers.json"
        if data_file.exists():
            with open(data_file, "r") as f:
                data = json.load(f)
                _sp500_tickers_cache = data.get("tickers", [])
                _nasdaq_tickers_cache = data.get("nasdaq_tickers", [])
                logger.debug(f"Loaded {len(_sp500_tickers_cache)} S&P 500 tickers from {data_file}")
        else:
            logger.warning(f"S&P 500 tickers file not found: {data_file}")
            _sp500_tickers_cache = []
            _nasdaq_tickers_cache = []
    except Exception as e:
        logger.error(f"Failed to load S&P 500 tickers: {e}")
        _sp500_tickers_cache = []
        _nasdaq_tickers_cache = []
    
    return _sp500_tickers_cache, _nasdaq_tickers_cache


def _load_tradfi_instruments() -> Tuple[List[Dict], Dict[str, str]]:
    """Load TradFi instruments and exchange code mappings from JSON file."""
    global _tradfi_instruments_cache, _exchange_code_to_name_cache
    
    if _tradfi_instruments_cache is not None:
        return _tradfi_instruments_cache, _exchange_code_to_name_cache or {}
    
    try:
        data_file = _get_data_dir() / "tradfi_instruments.json"
        if data_file.exists():
            with open(data_file, "r") as f:
                data = json.load(f)
                _tradfi_instruments_cache = data.get("instruments", [])
                _exchange_code_to_name_cache = data.get("exchange_code_to_name", {})
                logger.debug(f"Loaded {len(_tradfi_instruments_cache)} TradFi instruments from {data_file}")
        else:
            logger.warning(f"TradFi instruments file not found: {data_file}")
            _tradfi_instruments_cache = []
            _exchange_code_to_name_cache = {}
    except Exception as e:
        logger.error(f"Failed to load TradFi instruments: {e}")
        _tradfi_instruments_cache = []
        _exchange_code_to_name_cache = {}
    
    return _tradfi_instruments_cache, _exchange_code_to_name_cache


@dataclass
class TradFiInstrument:
    """
    Single TradFi instrument definition with metadata.
    
    Note: This is different from instruments_service.models.InstrumentDefinition (Pydantic model).
    This dataclass is for static TradFi instrument configuration (Databento symbols).
    """

    symbol: str  # Databento symbol (e.g., "ES.FUT", "SPY", "BRN.FUT", "SPY.OPT")
    venue: str  # Canonical venue (e.g., "CME", "NASDAQ", "ICE")
    instrument_type: str  # "FUTURE", "EQUITY", "OPTION", "ETF"
    dataset: str  # Databento dataset (e.g., "GLBX.MDP3", "DBEQ.BASIC")
    stype_in: str  # "parent" for futures/options, "raw_symbol" for equities/ETFs
    base_asset: Optional[str] = None  # Human-readable base asset name
    quote_asset: str = "USD"  # Quote currency (default USD for TradFi)
    exchange_code: Optional[str] = None  # Databento exchange code (e.g., "ES", "CL")
    underlying: Optional[str] = None  # Underlying asset (e.g., "BTC" for Bitcoin ETFs)


# Backward compatibility alias
InstrumentDefinition = TradFiInstrument


@dataclass
class UnifiedInstrumentConfig:
    """
    Unified instrument configuration - single source of truth for all TradFi instruments.
    
    Loads instruments and exchange code mappings from external JSON files.
    All TradFi instruments are loaded from data/tradfi_instruments.json.
    """

    # Cached instruments loaded from JSON (initialized lazily)
    _instruments: Optional[List[TradFiInstrument]] = field(default=None, repr=False)
    _exchange_code_to_name: Optional[Dict[str, str]] = field(default=None, repr=False)

    def __post_init__(self):
        """Load instruments from JSON on first access."""
        self._load_data()

    def _load_data(self) -> None:
        """Load TradFi instruments and exchange mappings from JSON file."""
        if self._instruments is not None:
            return
            
        raw_instruments, exchange_mappings = _load_tradfi_instruments()
        
        # Convert raw JSON to TradFiInstrument objects
        self._instruments = []
        for inst in raw_instruments:
            self._instruments.append(TradFiInstrument(
                symbol=inst["symbol"],
                venue=inst["venue"],
                instrument_type=inst["type"],
                dataset=inst["dataset"],
                stype_in=inst["stype"],
                base_asset=inst.get("base"),
                quote_asset="USD",
                exchange_code=inst.get("code"),
                underlying=inst.get("underlying"),
            ))
        
        self._exchange_code_to_name = exchange_mappings

    @property
    def instruments(self) -> List[TradFiInstrument]:
        """Get base TradFi instruments (futures, options, ETFs)."""
        if self._instruments is None:
            self._load_data()
        return self._instruments or []

    @property
    def exchange_code_to_name(self) -> Dict[str, str]:
        """Get exchange code to human-readable name mapping."""
        if self._exchange_code_to_name is None:
            self._load_data()
        return self._exchange_code_to_name or {}

    def get_symbols_for_venue(self, venue: str) -> List[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.venue == venue.upper()]

    def get_symbols_for_dataset(self, dataset: str) -> List[str]:
        """Get all symbols for a dataset (e.g., 'GLBX.MDP3', 'DBEQ.BASIC')"""
        all_insts = self.get_all_instruments()
        return [inst.symbol for inst in all_insts if inst.dataset == dataset]

    def get_symbols_by_type(self, instrument_type: str) -> List[str]:
        """Get all symbols for an instrument type (e.g., 'FUTURE', 'EQUITY', 'OPTION')"""
        all_insts = self.get_all_instruments()
        return [
            inst.symbol for inst in all_insts if inst.instrument_type == instrument_type.upper()
        ]

    def get_dataset_and_stype(self, symbol: str) -> Optional[Tuple[str, str]]:
        """Get dataset and stype_in for a symbol"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
            if inst.symbol == symbol:
                return (inst.dataset, inst.stype_in)
        return None

    def get_instrument(
        self, symbol: str, venue: Optional[str] = None
    ) -> Optional[InstrumentDefinition]:
        """Get instrument definition by symbol (optionally filtered by venue)"""
        all_insts = self.get_all_instruments()
        for inst in all_insts:
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

    # ETFs that are in the S&P 500 or commonly traded (should NOT be classified as EQUITY)
    KNOWN_ETFS = {
        'SPY', 'QQQ', 'IVV', 'VOO', 'VTI', 'DIA', 'IWM', 'EEM', 'VEA', 'VWO',
        'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'IEF', 'SHY', 'LQD', 'HYG', 'JNK',
        'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLU', 'XLRE',
        'VNQ', 'IBB', 'SMH', 'ARKK', 'ARKG', 'ARKW', 'ARKF', 'ARKQ',
        'IBIT', 'FBTC', 'ARKB', 'GBTC', 'BITO'  # Bitcoin ETFs
    }
    
    # Symbols with spaces that need dot format for Databento API
    # Databento uses "BRK.B" not "BRK B"
    SPACE_TO_DOT_SYMBOLS = {
        'BRK B': 'BRK.B',   # Berkshire Hathaway Class B
        'BF B': 'BF.B',     # Brown-Forman Class B
        'BRK A': 'BRK.A',   # Berkshire Hathaway Class A
        'BF A': 'BF.A'      # Brown-Forman Class A
    }
    
    def _get_sp500_equities(self) -> List[TradFiInstrument]:
        """Generate S&P 500 equity/ETF instrument definitions from external data file."""
        sp500_tickers, nasdaq_tickers = _load_sp500_tickers()
        
        if not sp500_tickers:
            logger.warning("No S&P 500 tickers loaded - returning empty list")
            return []

        instruments = []
        for ticker in sp500_tickers:
            # Convert space symbols to dot format for Databento
            databento_symbol = self.SPACE_TO_DOT_SYMBOLS.get(ticker, ticker)
            
            # Determine venue (NASDAQ for known tech stocks, NYSE for others)
            venue = "NASDAQ" if ticker in nasdaq_tickers else "NYSE"
            
            # Determine instrument type (ETF vs EQUITY)
            instrument_type = "ETF" if ticker in self.KNOWN_ETFS else "EQUITY"
            
            instruments.append(
                TradFiInstrument(
                    symbol=databento_symbol,  # Use Databento-compatible symbol (BRK.B not BRK B)
                    venue=venue, 
                    instrument_type=instrument_type,  # ETF or EQUITY
                    dataset="DBEQ.BASIC", 
                    stype_in="raw_symbol", 
                    base_asset=ticker,  # Keep original ticker as base_asset for display
                    quote_asset="USD"
                )
            )
        return instruments

    def get_all_instruments(self) -> List[TradFiInstrument]:
        """Get all instruments (base instruments + dynamically generated S&P 500 equities)"""
        # Combine base instruments with dynamically generated S&P 500 equities
        all_insts = list(self.instruments)
        sp500_equities = self._get_sp500_equities()
        all_insts.extend(sp500_equities)
        return all_insts


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
        if symbol.endswith(".FUT") or any(
            inst.symbol == symbol.replace(".FUT", "")
            for inst in self._unified.instruments
            if inst.instrument_type == "FUTURE"
        ):
            return ("GLBX.MDP3", "parent")
        return ("DBEQ.BASIC", "raw_symbol")

    def get_human_readable_name(self, exchange_code: str) -> str:
        """Convert exchange code to human-readable name"""
        return self._unified.get_human_readable_name(exchange_code)

    def get_symbols_for_venue(self, venue: str) -> List[str]:
        """Get all symbols for a venue (e.g., 'CME', 'NASDAQ', 'ICE')"""
        return self._unified.get_symbols_for_venue(venue)


# ============================================================================
# SERVICE-LEVEL CONFIGURATION (Pydantic BaseSettings)
# VenueMapping, DataTypeConfig, ExchangeInstrumentConfig
# are imported from unified_cloud_services (see imports at top of file)
# ============================================================================


class InstrumentsServiceConfig(UnifiedCloudServicesConfig):
    """
    Service-level configuration for instruments-service.

    Extends UnifiedCloudServicesConfig with instruments-specific settings.
    Inherited from parent: gcp_project_id, google_application_credentials_path,
    gcs_region, gcs_location, bigquery_location, tardis_secret_name,
    databento_secret_name, alchemy_secret_name, aavescan_secret_name,
    enable_csv_sampling, csv_sample_size, csv_sample_dir, log_level, etc.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore extra fields from .env
    )

    # =========================================================================
    # SERVICE IDENTIFICATION (override parent default)
    # =========================================================================
    service_name: str = Field(default="instruments-service", description="Service name")

    # =========================================================================
    # INSTRUMENTS-SPECIFIC GCS BUCKETS
    # These override the parent's generic bucket with instruments-specific env vars
    # =========================================================================
    gcs_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET"),
        description="Primary GCS bucket for instruments",
    )
    gcs_bucket_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TEST"),
        description="Test GCS bucket for instruments",
    )
    # Category-specific buckets for independent batch processing
    gcs_bucket_cefi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI"),
        description="GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI"),
        description="GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI"),
        description="GCS bucket for DEFI instruments",
    )
    # Category-specific TEST buckets
    gcs_bucket_cefi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_CEFI_TEST"),
        description="Test GCS bucket for CEFI instruments",
    )
    gcs_bucket_tradfi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_TRADFI_TEST"),
        description="Test GCS bucket for TRADFI instruments",
    )
    gcs_bucket_defi_test: str = Field(
        default="",
        validation_alias=AliasChoices("INSTRUMENTS_GCS_BUCKET_DEFI_TEST"),
        description="Test GCS bucket for DEFI instruments",
    )

    # =========================================================================
    # INSTRUMENTS-SPECIFIC BIGQUERY (override parent with instruments-specific env var)
    # =========================================================================
    bigquery_dataset: str = Field(
        default="instruments",
        validation_alias=AliasChoices("INSTRUMENTS_BIGQUERY_DATASET", "BIGQUERY_DATASET"),
        description="BigQuery dataset for instruments",
    )

    # =========================================================================
    # INSTRUMENTS-SPECIFIC PROCESSING CONFIGURATION
    # =========================================================================
    enable_ccxt_integration: bool = Field(
        default=True, description="Enable CCXT metadata enrichment"
    )
    enable_metadata_caching: bool = Field(default=True, description="Enable metadata caching")
    cache_ttl_hours: int = Field(default=24, description="Cache TTL in hours")
    max_batch_size: int = Field(default=1000, description="Maximum batch size for processing")
    lookback_days: int = Field(default=0, description="Lookback days for batch processing")

    # =========================================================================
    # THE GRAPH SECRET NAME (different name than parent's thegraph_secret_name)
    # =========================================================================
    graph_secret_name: str = Field(
        default="graph-api-key",
        validation_alias=AliasChoices("GRAPH_SECRET_NAME"),
        description="Graph API key secret name",
    )

    # =========================================================================
    # DEFI URLS (instruments-specific)
    # =========================================================================
    ethereum_rpc_url: str = Field(
        default="",
        validation_alias=AliasChoices("ETHEREUM_RPC_URL"),
        description="Ethereum RPC URL",
    )
    uniswap_v3_graph_url: str = Field(
        default="",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_URL"),
        description="Uniswap V3 Graph URL",
    )
    uniswap_v3_graph_arb_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-arbitrum/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_ARB_URL"),
        description="The Graph Uniswap V3 URL for Arbitrum",
    )
    uniswap_v3_graph_base_url: str = Field(
        default="https://api.studio.thegraph.com/query/50688/uniswap-v3-base/version/latest",
        validation_alias=AliasChoices("UNISWAP_V3_GRAPH_BASE_URL"),
        description="The Graph Uniswap V3 URL for Base",
    )
    envio_api_url: str = Field(
        default="",
        validation_alias=AliasChoices("ENVIO_API_URL"),
        description="Envio API URL",
    )

    # DeFi MVP tokens configuration
    defi_mvp_tokens: str = Field(
        default="",
        validation_alias=AliasChoices("DEFI_MVP_TOKENS"),
        description="Comma-separated list of DeFi MVP tokens",
    )

    # ClickUp Configuration
    clickup_secret_name: str = Field(
        default="clickup-api-key",
        validation_alias=AliasChoices("CLICKUP_SECRET_NAME"),
        description="ClickUp API key secret name",
    )
    clickup_list_id: str = Field(
        default="",
        validation_alias=AliasChoices("clickup_list_id_instruments_service"),
        description="ClickUp List ID",
    )
    clickup_user_id_ikenna: str = Field(
        default="254573729",
        validation_alias=AliasChoices("clickup_user_id_ikenna"),
        description="ClickUp User ID for Ikenna",
    )
    clickup_user_id_harsh: str = Field(
        default="100698878",
        validation_alias=AliasChoices("clickup_user_id_harsh"),
        description="ClickUp User ID for Harsh",
    )
    clickup_user_id_femi: str = Field(
        default="100698756",
        validation_alias=AliasChoices("clickup_user_id_femi"),
        description="ClickUp User ID for Femi",
    )
    clickup_user_id_daniel: str = Field(
        default="36559682",
        validation_alias=AliasChoices("clickup_user_id_daniel"),
        description="ClickUp User ID for Daniel",
    )

    def get_cloud_target(self, category: str | None = None) -> CloudTarget:
        """
        Get CloudTarget for instruments service.

        Args:
            category: Optional market category ("CEFI", "TRADFI", "DEFI") to use category-specific bucket

        Returns:
            CloudTarget with appropriate bucket for category
        """
        # Determine bucket based on category
        if category:
            category_upper = category.upper()
            if category_upper == "CEFI":
                bucket = self.gcs_bucket_cefi
            elif category_upper == "TRADFI":
                bucket = self.gcs_bucket_tradfi
            elif category_upper == "DEFI":
                bucket = self.gcs_bucket_defi
            else:
                raise ValueError(
                    f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI"
                )
        else:
            bucket = self.gcs_bucket

        return CloudTarget(
            project_id=self.gcp_project_id,
            gcs_bucket=bucket,
            bigquery_dataset=self.bigquery_dataset,
            bigquery_location=self.bigquery_location,
        )

    def is_test_environment(self) -> bool:
        """Check if the current environment is a test environment."""
        return self.environment.lower() in ["test", "testing"]

    # Properties for uppercase bucket names (compatibility with unified-cloud-services)
    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI(self) -> str:
        return self.gcs_bucket_cefi

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI(self) -> str:
        return self.gcs_bucket_tradfi

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI(self) -> str:
        return self.gcs_bucket_defi

    @property
    def INSTRUMENTS_GCS_BUCKET_CEFI_TEST(self) -> str:
        return self.gcs_bucket_cefi_test or f"{self.gcs_bucket_cefi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_TRADFI_TEST(self) -> str:
        return self.gcs_bucket_tradfi_test or f"{self.gcs_bucket_tradfi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET_DEFI_TEST(self) -> str:
        return self.gcs_bucket_defi_test or f"{self.gcs_bucket_defi}-test"

    @property
    def INSTRUMENTS_GCS_BUCKET(self) -> str:
        return self.gcs_bucket

    @property
    def INSTRUMENTS_GCS_BUCKET_TEST(self) -> str:
        return self.gcs_bucket_test

    def get_bucket_for_category(self, category: str, test_mode: bool = False) -> str:
        """
        Get the GCS bucket name for a specific market category.

        Args:
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            test_mode: Whether to use test bucket (default: False)

        Returns:
            Bucket name from environment variables

        Raises:
            ValueError: If category is invalid or bucket not configured
        """
        category_upper = category.upper()

        if category_upper not in ["CEFI", "TRADFI", "DEFI"]:
            raise ValueError(f"Invalid category: {category}. Must be one of: CEFI, TRADFI, DEFI")

        # Determine environment variable name
        if test_mode:
            bucket_name = f"gcs_bucket_{category_upper.lower()}_test"
        else:
            bucket_name = f"gcs_bucket_{category_upper.lower()}"

        # Get bucket from attribute
        bucket = getattr(self, bucket_name, None)

        if bucket:
            logger.debug(f"📦 Using bucket for {category_upper}: {bucket}")
            return bucket

        # Fallback to default bucket if category-specific bucket not configured
        logger.warning(
            f"⚠️ Category-specific bucket not configured for {category_upper}. "
            f"Using default bucket."
        )
        return self.gcs_bucket_test if test_mode else self.gcs_bucket


# Create singleton instance
instruments_config = InstrumentsServiceConfig()
