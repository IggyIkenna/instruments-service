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
