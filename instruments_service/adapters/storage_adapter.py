"""Storage Adapter for Instruments Service.

Thin I/O layer - delegates to unified-trading-library for GCS operations.
NO business logic, validation, or transformation - pure I/O only.
"""

import logging

import pandas as pd
from unified_domain_client import CloudTarget, StandardizedDomainCloudService

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class StorageAdapter:
    """
    Thin storage adapter for instrument GCS operations.

    Responsibilities (I/O only):
    - Build GCS paths (by-venue folder structure)
    - Upload to GCS (batch operations)
    - Bucket selection

    NOT responsible for:
    - Schema validation (done by engine)
    - Data transformation (done by engine)
    - Market category determination (done by engine)
    """

    def __init__(self, cloud_target: CloudTarget | None = None):
        """
        Initialize storage adapter.

        Args:
            cloud_target: Optional CloudTarget (auto-configured if None)
        """
        if cloud_target is None:
            cfg = instruments_config
            environment = (cfg.environment or "development").lower()
            is_test = environment in ["test", "testing"]

            if is_test:
                bucket_name = cfg.gcs_bucket_test or cfg.gcs_bucket_cefi_test or "instruments-store-test"
            else:
                bucket_name = cfg.gcs_bucket_cefi or cfg.get_bucket_for_category("cefi")

            cloud_target = CloudTarget(
                project_id=cfg.gcp_project_id,
                gcs_bucket=bucket_name,
                bigquery_dataset=cfg.bigquery_dataset or "instruments",
                bigquery_location=cfg.bigquery_location or "asia-northeast1",
            )

        self.cloud_service: StandardizedDomainCloudService = StandardizedDomainCloudService(
            domain="instruments", cloud_target=cloud_target
        )
        self.cloud_target = cloud_target

        logger.info(
            "Initialized StorageAdapter: project=%s, bucket=%s", cloud_target.project_id, cloud_target.gcs_bucket
        )

    def build_gcs_path(self, date_str: str, venue: str) -> str:
        """
        Build GCS path for venue instruments.

        Args:
            date_str: Date string (YYYY-MM-DD)
            venue: Venue name

        Returns:
            GCS path: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
        """
        venue_folder = venue.replace("/", "-").replace("\\", "-")
        return f"instrument_availability/by_date/day={date_str}/venue={venue_folder}/instruments.parquet"

    def upload_batch(
        self,
        uploads: list[tuple[str, pd.DataFrame, str]],
        bucket_name: str,
    ) -> list[dict[str, bool | str]]:
        """
        Upload multiple DataFrames to GCS bucket.

        Args:
            uploads: List of (gcs_path, dataframe, category_venue) tuples
            bucket_name: Target GCS bucket

        Returns:
            List of result dicts with 'success' and optional 'error' keys
        """
        bucket_cloud_target = CloudTarget(
            project_id=self.cloud_target.project_id,
            gcs_bucket=bucket_name,
            bigquery_dataset=self.cloud_target.bigquery_dataset,
            bigquery_location=self.cloud_target.bigquery_location,
        )
        bucket_cloud_service: StandardizedDomainCloudService = StandardizedDomainCloudService(
            domain="instruments", cloud_target=bucket_cloud_target
        )

        # UDC StandardizedDomainCloudService has single-file upload; iterate for batch.
        results: list[dict[str, bool | str]] = []
        for gcs_path, df, _ in uploads:
            entry: dict[str, bool | str] = {}
            try:
                bucket_cloud_service.upload_to_gcs(data=df, gcs_path=gcs_path, format="parquet")
                entry["success"] = True
            except Exception as exc:
                entry["success"] = False
                entry["error"] = str(exc)
            results.append(entry)
        return results

    def get_bucket_for_category(self, category: str, test_mode: bool = False) -> str:
        """
        Get GCS bucket name for market category.

        Args:
            category: Market category (CEFI, TRADFI, DEFI)
            test_mode: Whether to use test bucket

        Returns:
            Bucket name
        """
        cfg = instruments_config
        return cfg.get_bucket_for_category(category, test_mode=test_mode)
