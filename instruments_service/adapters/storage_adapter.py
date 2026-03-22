"""Storage Adapter for Instruments Service.

Thin I/O layer - delegates to unified-cloud-interface for storage operations.
NO business logic, validation, or transformation - pure I/O only.
"""

import logging

import pandas as pd
from unified_cloud_interface import DataSink, get_data_sink

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class StorageAdapter:
    """
    Thin storage adapter for instrument GCS operations.

    Responsibilities (I/O only):
    - Build GCS paths (by-venue folder structure)
    - Upload to storage (batch operations) via UCI DataSink intent API
    - Bucket selection

    NOT responsible for:
    - Schema validation (done by engine)
    - Data transformation (done by engine)
    - Market category determination (done by engine)
    """

    def __init__(self) -> None:
        """
        Initialize storage adapter.

        Sink backends and buckets are injected at deployment time via
        PROTOCOL_DATA_SINK_BACKEND / PROTOCOL_DATA_SINK_BUCKET_{CATEGORY} env vars.
        Defaults to LocalDataSink when env vars are absent (CI/local).
        """
        logger.info("Initialized StorageAdapter (UCI DataSink)")

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
        category: str,
    ) -> list[dict[str, bool | str]]:
        """
        Upload multiple DataFrames via UCI DataSink intent API.

        Args:
            uploads: List of (gcs_path, dataframe, category_venue) tuples
            category: Market category routing key (e.g. "cefi", "tradfi", "defi")
                      Reads PROTOCOL_DATA_SINK_BUCKET_{CATEGORY} — set by deployment bootstrap

        Returns:
            List of result dicts with 'success' and optional 'error' keys
        """
        # PROTOCOL_DATA_SINK_BUCKET_{CATEGORY} — set by deployment bootstrap
        data_sink: DataSink = get_data_sink(routing_key=category.lower())

        results: list[dict[str, bool | str]] = []
        for gcs_path, df, _ in uploads:
            entry: dict[str, bool | str] = {}
            try:
                # Parse partition info from gcs_path:
                # instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
                parts = gcs_path.split("/")
                partition: dict[str, str] = {}
                for part in parts[:-1]:
                    if "=" in part:
                        k, v = part.split("=", 1)
                        partition[k] = v
                data_sink.write(df, partition=partition if partition else None, format="parquet")
                entry["success"] = True
            except Exception as exc:
                classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="storage_write",
                )
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
