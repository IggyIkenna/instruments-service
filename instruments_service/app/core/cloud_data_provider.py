"""
Cloud Data Provider

Provides read access to instrument data from unified-trading-library.
Each domain has its own bucket and dataset (instruments domain).
"""

import logging
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from unified_domain_client import CloudTarget, StandardizedDomainCloudService
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_trading_library import get_bucket_for_category

from instruments_service.config import instruments_config
from instruments_service.utils.dump_to_csv import dump_to_csv

logger = logging.getLogger(__name__)


class CloudDataProvider:
    """
    Provides read access to instrument data from unified-trading-library.

    Each domain has its own bucket and dataset (instruments domain).
    """

    def __init__(self, cloud_target: CloudTarget | None = None, testing_mode: bool = False):
        """
        Initialize cloud data provider.

        Args:
            cloud_target: Optional CloudTarget configuration (auto-detects if not provided)
            testing_mode: When True, uses test buckets instead of production buckets
        """
        self._testing_mode = testing_mode
        if cloud_target is None:
            # NOTE: This default is only used when no category is specified.
            # Production flow should always use category-specific buckets via get_bucket_for_category()
            cfg = instruments_config
            cloud_target = CloudTarget(
                project_id=cfg.gcp_project_id,
                gcs_bucket=cfg.get_bucket_for_category("cefi"),
                bigquery_dataset=cfg.bigquery_dataset or "instruments",
                bigquery_location=cfg.bigquery_location or "asia-northeast1",
            )

        # Create instruments service (each domain has its own bucket and dataset)
        # Direct instantiation (canonical pattern per unified architecture)
        self.cloud_service = StandardizedDomainCloudService(domain="instruments", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(
            "✅ CloudDataProvider initialized: project=%s, dataset=%s",
            cloud_target.project_id,
            cloud_target.bigquery_dataset,
        )

    def get_instruments_from_gcs(
        self, date: datetime, gcs_path: str | None = None, category: str | None = None
    ) -> pd.DataFrame:
        """
        Get instruments from GCS for a specific date.

        Args:
            date: Target date
            gcs_path: Optional custom GCS path (default: uses standard path format)
            category: Optional market category ("CEFI", "TRADFI", "DEFI") to read from category-specific bucket

        Returns:
            DataFrame with instruments
        """
        if gcs_path is None:
            date_str = date.strftime("%Y-%m-%d")
            gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"

        try:
            # If category specified, use category-specific bucket
            if category:
                return self.get_instruments_from_category(date, category, gcs_path=gcs_path)

            logger.info("📥 Loading instruments from GCS: %s", gcs_path)
            raw: pd.DataFrame | object = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning("⚠️ No instruments found at %s", gcs_path)
            else:
                logger.info("✅ Loaded %s instruments from GCS", len(df))

                # CSV sampling for instruments data
                dump_to_csv(
                    df,
                    filename=f"instruments_service_data_{date.strftime('%Y%m%d')}_{datetime.now(UTC).strftime('%H%M%S')}.csv",
                )

            return df

        except (OSError, PermissionError) as e:
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
            # Handle 404/Not Found gracefully - this is an expected state when data hasn't been generated yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("No instruments found (404): %s", gcs_path)
                return pd.DataFrame()

            logger.error("❌ Failed to load instruments from GCS: %s", e)
            return pd.DataFrame()

    def get_instruments_from_category(self, date: datetime, category: str, gcs_path: str | None = None) -> pd.DataFrame:
        """
        Get instruments from category-specific bucket for a specific date.

        Args:
            date: Target date
            category: Market category ("CEFI", "TRADFI", or "DEFI")
            gcs_path: Optional custom GCS path (default: uses standard path format)

        Returns:
            DataFrame with instruments from the specified category bucket
        """
        if gcs_path is None:
            date_str = date.strftime("%Y-%m-%d")
            gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"

        # Initialize before try so it's always bound (even in exception handler)
        category_bucket: str = ""
        try:
            # Detect test mode
            environment: str = str(instruments_config.environment or "development").lower()
            is_test = environment in ["test", "testing"] or self._testing_mode

            # Get bucket for category
            category_bucket = get_bucket_for_category(category, test_mode=is_test)

            # Create cloud service for category bucket
            category_cloud_target = CloudTarget(
                project_id=self.cloud_target.project_id,
                gcs_bucket=category_bucket,
                bigquery_dataset=self.cloud_target.bigquery_dataset,
                bigquery_location=self.cloud_target.bigquery_location,
            )
            category_cloud_service = StandardizedDomainCloudService(
                domain="instruments", cloud_target=category_cloud_target
            )

            logger.info("📥 Loading %s instruments from GCS: %s/%s", category, category_bucket, gcs_path)
            raw: pd.DataFrame | object = category_cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning("⚠️ No %s instruments found at %s/%s", category, category_bucket, gcs_path)
            else:
                logger.info("✅ Loaded %s %s instruments from GCS", len(df), category)

                # CSV sampling for category-specific instruments data
                dump_to_csv(
                    df,
                    filename=f"instruments_service_{category.lower()}_data_{date.strftime('%Y%m%d')}_{datetime.now(UTC).strftime('%H%M%S')}.csv",
                )

            return df

        except (OSError, PermissionError) as e:
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
            # Handle 404/Not Found gracefully - this is an expected state when data hasn't been generated yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("No %s instruments found (404): %s/%s", category, category_bucket, gcs_path)
                return pd.DataFrame()

            logger.error("❌ Failed to load %s instruments from GCS: %s", category, e)
            return pd.DataFrame()

    def get_instruments_from_bigquery(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        table_name: str = "instruments",
    ) -> pd.DataFrame:
        """
        Query instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            table_name: BigQuery table name (default: "instruments")

        Returns:
            DataFrame with instruments
        """
        try:
            query = f"""
            SELECT * FROM `{self.cloud_target.bigquery_dataset}.{table_name}`
            WHERE 1=1
            """

            parameters: dict[str, object] = {}

            if venue:
                query += " AND venue = @venue"
                parameters["venue"] = venue

            if instrument_type:
                query += " AND instrument_type = @instrument_type"
                parameters["instrument_type"] = instrument_type

            query += " ORDER BY instrument_key"

            logger.info("📥 Querying instruments from BigQuery: %s", table_name)
            result: pd.DataFrame = self.cloud_service.query_bigquery(query=query, parameters=parameters)

            logger.info("✅ Queried %s instruments from BigQuery", len(result))
            return result

        except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("❌ Failed to query instruments from BigQuery: %s", e)
            return pd.DataFrame()

    def check_instruments_exist(
        self,
        date: datetime,
        categories: list[str] | None = None,
        venues: list[str] | None = None,
    ) -> bool:
        """
        Check if instruments exist for a specific date.

        When venues is specified, checks for venue-level files (new structure).
        When venues is None, checks for date-level aggregate file (legacy behavior).

        Args:
            date: Target date
            categories: Optional list of categories to check (e.g., ["CEFI", "TRADFI"]).
                       If None, checks ALL categories and returns True if ANY exist.
                       If specified, returns True only if ALL specified categories exist.
            venues: Optional list of venues to check (e.g., ["BINANCE-SPOT", "BYBIT"]).
                   When specified, checks venue-level files instead of date-level aggregate.
                   Returns True only if ALL specified venues exist.

        Returns:
            True if instruments exist (logic depends on categories/venues parameters)
        """
        date_str = date.strftime("%Y-%m-%d")

        # Default: check all categories
        if categories is None:
            categories = ["CEFI", "TRADFI", "DEFI"]
            check_all = False  # Return True if ANY exist (legacy behavior)
        else:
            check_all = True  # Return True only if ALL specified categories exist

        # When venues specified, use venue-level paths (new structure)
        # This enables granular skip logic matching the category x venue x date sharding
        if venues:
            logger.debug(
                "🔍 Checking venue-level existence for %s: categories=%s, venues=%s", date_str, categories, venues
            )
            for category in categories:
                for venue in venues:
                    # Sanitize venue name for folder (replace slashes, etc.)
                    venue_folder = venue.replace("/", "-").replace("\\", "-")
                    # Use key=value format for BigQuery hive partitioning
                    gcs_path = (
                        f"instrument_availability/by_date/day={date_str}/venue={venue_folder}/instruments.parquet"
                    )

                    try:
                        df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)

                        if not df.empty:
                            logger.debug("📊 Venue instruments found: %s/%s for %s", category, venue, date_str)
                            # For venue-level checks, we need ALL venues to exist
                            # (matches the sharding logic - each shard is a specific venue)
                        else:
                            logger.debug("📊 Venue instruments NOT found: %s/%s for %s", category, venue, date_str)
                            return False  # Any missing venue means data doesn't exist
                    except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                        _err = EnhancedError(
                            message=str(e),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(e).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        logger.debug("Could not check %s/%s for %s: %s", category, venue, date_str, e)
                        return False  # Error checking = treat as not existing
            # All venues found
            return True

        # Legacy behavior: check date-level aggregate file
        gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"
        found_categories: list[str] = []

        for category in categories:
            try:
                # Use get_instruments_from_category which handles bucket selection (test vs prod)
                # and uses the correct category-specific bucket
                df = self.get_instruments_from_category(date, category, gcs_path=gcs_path)

                if not df.empty:
                    logger.debug("📊 Instruments found in %s for %s", category, date_str)
                    found_categories.append(category)

                    # Legacy behavior: return True on first find
                    if not check_all:
                        return True
            except (OSError, ValueError, RuntimeError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                # Log but continue checking other categories
                logger.debug("Could not check %s for %s: %s", category, date_str, e)
                continue
        if check_all:
            # Only return True if ALL specified categories were found
            all_found = len(found_categories) == len(categories)
            if not all_found:
                missing = set(categories) - set(found_categories)
                logger.debug("📊 Missing categories for %s: %s", date_str, missing)
            return all_found

        logger.debug("📊 No instruments found for %s in any category", date_str)
        return False
