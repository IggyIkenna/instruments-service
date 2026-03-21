"""
Data Source Adapter

Thin I/O adapter that delegates to UCI DataSource protocol.
NO business logic, validation, or transformation - just I/O operations.
"""

import logging
from uuid import uuid4

import pandas as pd
from unified_api_contracts import (
    BinanceFuturesExchangeInfo,
    BinanceInstrumentInfo,
    BinanceOptionInstrumentInfo,
    BybitInstrumentInfo,
    BybitInstrumentsResponse,
    CoinbaseProductInfo,
    CoinbaseProductsResponse,
    DatabentoError,
    DatabentoReferenceInstrument,
    DeribitGetInstrumentResponse,
    DeribitGetInstrumentsResponse,
    DeribitInstrumentInfoFull,
    HyperliquidAssetInfo,
    HyperliquidMeta,
    IBKRAccountValue,
    IBKRCorporateAction,
    OKXInstrumentInfo,
    OKXInstrumentsResponse,
    PolygonOptionContract,
    PolygonOptionContractsResponse,
    PolygonTicker,
    PolygonTickersResponse,
    TardisExchangeDetail,
    TardisInstrumentDetail,
)
from unified_cloud_interface import DataSource, get_data_source
from unified_cloud_interface.constants import get_bucket_name, get_project_id_optional
from unified_internal_contracts import (
    DataSourceConstraint,
    EnhancedError,
    ErrorCategory,
    ErrorContext,
    ErrorRecoveryStrategy,
    ErrorSeverity,
    OHLCVSource,
)

logger = logging.getLogger(__name__)

# UAC contract type references — re-exported for cross-service contract alignment
__all__ = [
    "BinanceFuturesExchangeInfo",
    "BinanceInstrumentInfo",
    "BinanceOptionInstrumentInfo",
    "BybitInstrumentInfo",
    "BybitInstrumentsResponse",
    "CoinbaseProductInfo",
    "CoinbaseProductsResponse",
    "DataSourceAdapter",
    "DataSourceConstraint",
    "DatabentoError",
    "DatabentoReferenceInstrument",
    "DeribitGetInstrumentResponse",
    "DeribitGetInstrumentsResponse",
    "DeribitInstrumentInfoFull",
    "HyperliquidAssetInfo",
    "HyperliquidMeta",
    "IBKRAccountValue",
    "IBKRCorporateAction",
    "OHLCVSource",
    "OKXInstrumentInfo",
    "OKXInstrumentsResponse",
    "PolygonOptionContract",
    "PolygonOptionContractsResponse",
    "PolygonTicker",
    "PolygonTickersResponse",
    "TardisExchangeDetail",
    "TardisInstrumentDetail",
]


class DataSourceAdapter:
    """
    Thin I/O adapter for reading instrument data via UCI DataSource protocol.

    Delegates all actual I/O to UCI DataSource.
    NO business logic - just reads parquet files from storage.
    """

    def __init__(self, routing_key: str = "cefi") -> None:
        """
        Initialize data source adapter.

        Args:
            routing_key: UCI routing key for the category (e.g., "cefi", "tradfi", "defi")
        """
        self._routing_key = routing_key

    def read_parquet(self, gcs_path: str) -> pd.DataFrame:
        """
        Read parquet file from storage using the default routing key.

        Args:
            gcs_path: Path used to derive partition keys
                      (e.g., "instrument_availability/by_date/day=2024-01-01/instruments.parquet")

        Returns:
            DataFrame with data from parquet file (empty if not found)
        """
        # Parse partition from path
        partition: dict[str, str] = {}
        for part in gcs_path.split("/"):
            if "=" in part and not part.endswith(".parquet"):
                k, _, v = part.partition("=")
                partition[k] = v

        try:
            bucket = get_bucket_name("instruments", self._routing_key.upper()) if get_project_id_optional() else None
            data_source: DataSource = get_data_source(
                bucket=bucket, prefix="instrument_availability/by_date"
            )
            raw: object = data_source.read(partition=partition, format="parquet")
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            return raw
        except FileNotFoundError:
            logger.debug("File not found: %s", gcs_path)
            return pd.DataFrame()
        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.debug("File not found (404): %s", gcs_path)
                return pd.DataFrame()
            logger.error("Failed to read parquet from storage: %s", e)
            return pd.DataFrame()

    def read_parquet_from_category(self, gcs_path: str, category: str, test_mode: bool = False) -> pd.DataFrame:
        """
        Read parquet file from category-specific routing key.

        Args:
            gcs_path: Path used to derive partition keys
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            test_mode: Ignored (kept for API compatibility; environment is set via UCI config)

        Returns:
            DataFrame with data from parquet file (empty if not found)
        """
        # Parse partition from path
        partition: dict[str, str] = {}
        for part in gcs_path.split("/"):
            if "=" in part and not part.endswith(".parquet"):
                k, _, v = part.partition("=")
                partition[k] = v

        try:
            bucket = get_bucket_name("instruments", category.upper()) if get_project_id_optional() else None
            data_source: DataSource = get_data_source(
                bucket=bucket, prefix="instrument_availability/by_date"
            )
            raw: object = data_source.read(partition=partition, format="parquet")
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            return raw
        except FileNotFoundError:
            logger.debug("File not found in %s: %s", category, gcs_path)
            return pd.DataFrame()
        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.debug("File not found (404) in %s: %s", category, gcs_path)
                return pd.DataFrame()
            logger.error("Failed to read parquet from %s: %s", category, e)
            return pd.DataFrame()
