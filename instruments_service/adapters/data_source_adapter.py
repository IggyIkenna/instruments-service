"""
Data Source Adapter

Thin I/O adapter that delegates to unified-cloud-services.
NO business logic, validation, or transformation - just I/O operations.
"""

import logging
from uuid import uuid4

import pandas as pd
from unified_cloud_services import CloudTarget, StandardizedDomainCloudService, get_bucket_for_category
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext

logger = logging.getLogger(__name__)


class DataSourceAdapter:
    """
    Thin I/O adapter for reading instrument data from GCS.

    Delegates all actual I/O to unified-cloud-services.
    NO business logic - just reads parquet files from GCS.
    """

    def __init__(self, cloud_target: CloudTarget):
        """
        Initialize data source adapter.

        Args:
            cloud_target: CloudTarget configuration (project, bucket, dataset)
        """
        self.cloud_service: StandardizedDomainCloudService = StandardizedDomainCloudService(
            domain="instruments", cloud_target=cloud_target
        )
        self.cloud_target = cloud_target

    def read_parquet(self, gcs_path: str) -> pd.DataFrame:
        """
        Read parquet file from GCS.

        Args:
            gcs_path: GCS path (e.g., "instrument_availability/by_date/day=2024-01-01/instruments.parquet")

        Returns:
            DataFrame with data from parquet file (empty if not found)
        """
        try:
            raw: pd.DataFrame | object = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            return raw
        except Exception as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            # 404/Not Found is expected when data doesn't exist yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.debug(f"File not found (404): {gcs_path}")
                return pd.DataFrame()
            logger.error(f"Failed to read parquet from GCS: {e}")
            return pd.DataFrame()

    def read_parquet_from_category(self, gcs_path: str, category: str, test_mode: bool = False) -> pd.DataFrame:
        """
        Read parquet file from category-specific bucket.

        Args:
            gcs_path: GCS path within the category bucket
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            test_mode: Whether to use test bucket (default: False)

        Returns:
            DataFrame with data from parquet file (empty if not found)
        """
        try:
            # Get bucket for category
            category_bucket = get_bucket_for_category(category, test_mode=test_mode)

            # Create cloud service for category bucket
            category_cloud_target = CloudTarget(
                project_id=self.cloud_target.project_id,
                gcs_bucket=category_bucket,
                bigquery_dataset=self.cloud_target.bigquery_dataset,
                bigquery_location=self.cloud_target.bigquery_location,
            )
            category_cloud_service: StandardizedDomainCloudService = StandardizedDomainCloudService(
                domain="instruments", cloud_target=category_cloud_target
            )

            raw: pd.DataFrame | object = category_cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            return raw
        except Exception as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            error_msg = str(e)
            # 404/Not Found is expected when data doesn't exist yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.debug(f"File not found (404) in {category} bucket: {gcs_path}")
                return pd.DataFrame()
            logger.error(f"Failed to read parquet from {category} bucket: {e}")
            return pd.DataFrame()
