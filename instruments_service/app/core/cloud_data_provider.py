"""
Cloud Data Provider

Provides read access to instrument data from unified-cloud-services.
Each domain has its own bucket and dataset (instruments domain).
"""

import logging
import os
from datetime import datetime

import pandas as pd
from unified_cloud_services import CloudTarget, StandardizedDomainCloudService, get_bucket_for_category

logger = logging.getLogger(__name__)


class CloudDataProvider:
    """
    Provides read access to instrument data from unified-cloud-services.

    Each domain has its own bucket and dataset (instruments domain).
    """

    def __init__(self, cloud_target: CloudTarget | None = None):
        """
        Initialize cloud data provider.

        Args:
            cloud_target: Optional CloudTarget configuration (auto-detects if not provided)
        """
        if cloud_target is None:
            # NOTE: This default is only used when no category is specified.
            # Production flow should always use category-specific buckets via get_bucket_for_category()
            from instruments_service.config import instruments_config

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
            f"✅ CloudDataProvider initialized: project={cloud_target.project_id}, dataset={cloud_target.bigquery_dataset}"
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

            logger.info(f"📥 Loading instruments from GCS: {gcs_path}")
            raw: pd.DataFrame | object = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning(f"⚠️ No instruments found at {gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} instruments from GCS")
            return df

        except Exception as e:
            error_msg = str(e)
            # Handle 404/Not Found gracefully - this is an expected state when data hasn't been generated yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"ℹ️ No instruments found (404): {gcs_path}")
                return pd.DataFrame()

            logger.error(f"❌ Failed to load instruments from GCS: {e}")
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

        try:
            # Detect test mode
            from instruments_service.config import instruments_config

            environment: str = str(instruments_config.environment or "development").lower()
            is_test = environment in ["test", "testing"] or bool(os.environ.get("PYTEST_CURRENT_TEST"))

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

            logger.info(f"📥 Loading {category} instruments from GCS: {category_bucket}/{gcs_path}")
            raw: pd.DataFrame | object = category_cloud_service.download_from_gcs(
                gcs_path=gcs_path, format="parquet", log_errors=False
            )
            if not isinstance(raw, pd.DataFrame):
                return pd.DataFrame()
            df: pd.DataFrame = raw
            if df.empty:
                logger.warning(f"⚠️ No {category} instruments found at {category_bucket}/{gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} {category} instruments from GCS")
            return df

        except Exception as e:
            error_msg = str(e)
            # Handle 404/Not Found gracefully - this is an expected state when data hasn't been generated yet
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"ℹ️ No {category} instruments found (404): {category_bucket}/{gcs_path}")
                return pd.DataFrame()

            logger.error(f"❌ Failed to load {category} instruments from GCS: {e}")
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

            parameters = {}

            if venue:
                query += " AND venue = @venue"
                parameters["venue"] = venue

            if instrument_type:
                query += " AND instrument_type = @instrument_type"
                parameters["instrument_type"] = instrument_type

            query += " ORDER BY instrument_key"

            logger.info(f"📥 Querying instruments from BigQuery: {table_name}")
            result: pd.DataFrame = self.cloud_service.query_bigquery(query=query, parameters=parameters)

            logger.info(f"✅ Queried {len(result)} instruments from BigQuery")
            return result

        except Exception as e:
            logger.error(f"❌ Failed to query instruments from BigQuery: {e}")
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
            logger.debug(f"🔍 Checking venue-level existence for {date_str}: categories={categories}, venues={venues}")
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

                        if df is not None and not df.empty:
                            logger.debug(f"📊 Venue instruments found: {category}/{venue} for {date_str}")
                            # For venue-level checks, we need ALL venues to exist
                            # (matches the sharding logic - each shard is a specific venue)
                        else:
                            logger.debug(f"📊 Venue instruments NOT found: {category}/{venue} for {date_str}")
                            return False  # Any missing venue means data doesn't exist
                    except Exception as e:
                        logger.debug(f"Could not check {category}/{venue} for {date_str}: {e}")
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

                if df is not None and not df.empty:
                    logger.debug(f"📊 Instruments found in {category} for {date_str}")
                    found_categories.append(category)

                    # Legacy behavior: return True on first find
                    if not check_all:
                        return True
            except Exception as e:
                # Log but continue checking other categories
                logger.debug(f"Could not check {category} for {date_str}: {e}")
                continue

        if check_all:
            # Only return True if ALL specified categories were found
            all_found = len(found_categories) == len(categories)
            if not all_found:
                missing = set(categories) - set(found_categories)
                logger.debug(f"📊 Missing categories for {date_str}: {missing}")
            return all_found

        logger.debug(f"📊 No instruments found for {date_str} in any category")
        return False
