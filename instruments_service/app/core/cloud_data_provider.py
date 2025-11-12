"""
Cloud Data Provider

Provides read access to instrument data from unified-cloud-services.
Each domain has its own bucket and dataset (instruments domain).
"""

import logging
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime
from unified_cloud_services import StandardizedDomainCloudService, CloudTarget

logger = logging.getLogger(__name__)


class CloudDataProvider:
    """
    Provides read access to instrument data from unified-cloud-services.

    Each domain has its own bucket and dataset (instruments domain).
    """

    def __init__(self, cloud_target: Optional[CloudTarget] = None):
        """
        Initialize cloud data provider.

        Args:
            cloud_target: Optional CloudTarget configuration (auto-detects if not provided)
        """
        if cloud_target is None:
            import os

            cloud_target = CloudTarget(
                project_id=os.getenv("GCP_PROJECT_ID", "central-element-323112"),
                gcs_bucket=os.getenv("INSTRUMENTS_GCS_BUCKET", "instruments-store"),
                bigquery_dataset=os.getenv("INSTRUMENTS_BIGQUERY_DATASET", "instruments"),
                bigquery_location=os.getenv(
                    "BIGQUERY_LOCATION", "asia-northeast1"
                ),  # Default to asia-northeast1 per .env
            )

        # Create instruments service (each domain has its own bucket and dataset)
        # Direct instantiation (canonical pattern per unified architecture)
        self.cloud_service = StandardizedDomainCloudService(
            domain="instruments", cloud_target=cloud_target
        )
        self.cloud_target = cloud_target

        logger.info(
            f"✅ CloudDataProvider initialized: project={cloud_target.project_id}, dataset={cloud_target.bigquery_dataset}"
        )

    def get_instruments_from_gcs(
        self, date: datetime, gcs_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get instruments from GCS for a specific date.

        Args:
            date: Target date
            gcs_path: Optional custom GCS path (default: uses standard path format)

        Returns:
            DataFrame with instruments
        """
        if gcs_path is None:
            date_str = date.strftime("%Y-%m-%d")
            gcs_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"

        try:
            logger.info(f"📥 Loading instruments from GCS: {gcs_path}")
            df = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")

            if df.empty:
                logger.warning(f"⚠️ No instruments found at {gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} instruments from GCS")

            return df

        except Exception as e:
            logger.error(f"❌ Failed to load instruments from GCS: {e}")
            return pd.DataFrame()

    def get_instruments_from_bigquery(
        self,
        venue: Optional[str] = None,
        instrument_type: Optional[str] = None,
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
            result = self.cloud_service.query_bigquery(
                query=query, parameters=parameters if parameters else None
            )

            logger.info(f"✅ Queried {len(result)} instruments from BigQuery")
            return result

        except Exception as e:
            logger.error(f"❌ Failed to query instruments from BigQuery: {e}")
            return pd.DataFrame()

    def check_instruments_exist(self, date: datetime) -> bool:
        """
        Check if instruments exist for a specific date.

        Args:
            date: Target date

        Returns:
            True if instruments exist, False otherwise
        """
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"

        try:
            # Try to download to check existence
            df = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            exists = df is not None and not df.empty
            logger.debug(f"📊 Instruments exist check for {date_str}: {exists}")
            return exists
        except Exception:
            return False
