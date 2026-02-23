"""
Aggregate Handler

Aggregates instrument parquet files from GCS into deduplicated aggregated files per category.
Per 02-data/instruments-and-api-keys-standard.md: instruments-service owns aggregation.

Output: aggregated/aggregated_instruments_{date}.parquet per category bucket.
"""

import io
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import cast

import pandas as pd
from unified_cloud_services import get_instruments_bucket_for_category, get_storage_client

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import get_config
from instruments_service.events import log_event

logger = logging.getLogger(__name__)

BASE_PREFIX = "instrument_availability/by_date/"
AGGREGATED_PREFIX = "aggregated/"
OUTPUT_FILENAME_PATTERN = "aggregated_instruments_{date}.parquet"
DEDUP_COL = "instrument_key"
TIMESTAMP_COL = "timestamp"
CATEGORIES = ("CEFI", "TRADFI", "DEFI")


class AggregateHandler(ModeHandler):
    """
    Aggregate instruments from by-date/venue structure into deduplicated files.

    Delta-only (default): merges previous day's data with existing aggregated file.
    --redo-all: full rebuild from all by-date/venue parquet files.
    """

    def run(self, **kwargs: object) -> dict[str, HandlerResultValue]:  # type: ignore[reportAny]
        """Execute aggregation for each category."""
        redo_all = bool(kwargs.get("redo_all", False))
        project_id = cast(str | None, self.config.get("project_id")) or get_config().gcp_project_id

        if not project_id:
            return {"status": "error", "message": "project_id required for aggregation"}

        log_event("AGGREGATION_STARTED", "redo_all" if redo_all else "delta-only")

        total_instruments = 0
        categories_processed = 0

        for category in CATEGORIES:
            try:
                count = self._aggregate_category(
                    category=category,
                    project_id=project_id,
                    redo_all=redo_all,
                )
                if count >= 0:
                    total_instruments += count
                    categories_processed += 1
                    log_event("CATEGORY_AGGREGATION_COMPLETED", f"{category}: {count} instruments")
            except Exception as e:
                logger.error(f"Aggregation failed for {category}: {e}", exc_info=True)
                log_event("CATEGORY_AGGREGATION_FAILED", f"{category}: {e}")

        log_event("AGGREGATION_COMPLETED", f"{categories_processed} categories, {total_instruments} total")

        return {
            "status": "success",
            "message": f"Aggregated {total_instruments} instruments across {categories_processed} categories",
            "categories_processed": categories_processed,
            "total_instruments": total_instruments,
        }

    def _aggregate_category(
        self,
        category: str,
        project_id: str,
        redo_all: bool,
    ) -> int:
        """Aggregate instruments for one category bucket."""
        bucket_name = get_instruments_bucket_for_category(category, test_mode=False)
        client = get_storage_client(project_id=project_id)

        if redo_all:
            df = self._load_all_from_gcs(client, bucket_name)
        else:
            df = self._load_delta_and_merge(client, bucket_name, project_id)

        if df.empty:
            logger.warning(f"No instruments to aggregate for {category}")
            return 0

        # Deduplicate: keep latest by timestamp per instrument_key
        deduped = self._deduplicate(df)
        output_date = date.today().isoformat()
        gcs_path = f"{AGGREGATED_PREFIX}{OUTPUT_FILENAME_PATTERN.format(date=output_date)}"

        # Upload to GCS
        buf = io.BytesIO()
        deduped.to_parquet(buf, index=False)
        client.upload_bytes(
            bucket=bucket_name,
            blob_path=gcs_path,
            data=buf.getvalue(),
        )

        logger.info(f"✅ Wrote {len(deduped)} instruments to gs://{bucket_name}/{gcs_path}")
        return len(deduped)

    def _load_all_from_gcs(self, client: object, bucket_name: str) -> pd.DataFrame:
        """Load all instrument parquet files from GCS (redo-all mode)."""
        blobs = [
            b for b in client.list_blobs(bucket_name, prefix=BASE_PREFIX) if b.name.endswith("instruments.parquet")
        ]

        if not blobs:
            return pd.DataFrame()

        logger.info(f"Loading {len(blobs)} parquet files from gs://{bucket_name}/{BASE_PREFIX}")

        all_dfs: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=min(12, len(blobs))) as executor:
            futures = {executor.submit(self._download_parquet, client, bucket_name, b.name): b for b in blobs}
            for future in as_completed(futures):
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_dfs.append(df)
                except Exception as e:
                    logger.debug(f"Skip blob: {e}")

        if not all_dfs:
            return pd.DataFrame()
        return pd.concat(all_dfs, ignore_index=True)

    def _download_parquet(
        self,
        client: object,
        bucket_name: str,
        blob_path: str,
    ) -> pd.DataFrame | None:
        """Download a single parquet file from GCS."""
        data = client.download_bytes(bucket=bucket_name, blob_path=blob_path)
        return pd.read_parquet(io.BytesIO(data))

    def _load_delta_and_merge(
        self,
        client: object,
        bucket_name: str,
        project_id: str,  # noqa: ARG002
    ) -> pd.DataFrame:
        """Load previous day's data and merge with existing aggregated file (delta-only)."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        base_prefix = f"{BASE_PREFIX}day={yesterday}/"

        # List venue folders via bucket delimiter listing
        bucket = client.bucket(bucket_name)
        iterator = bucket.list_blobs(prefix=base_prefix, delimiter="/")
        list(iterator)  # Consume to populate prefixes
        venue_folders = list(getattr(iterator, "prefixes", []))

        delta_dfs: list[pd.DataFrame] = []
        for vf in venue_folders:
            if "venue=" not in vf:
                continue
            gcs_path = f"{vf}instruments.parquet"
            try:
                df = self._download_parquet(client, bucket_name, gcs_path)
                if df is not None and not df.empty:
                    delta_dfs.append(df)
            except Exception as e:
                logger.debug(f"Skip {gcs_path}: {e}")

        delta_df = pd.concat(delta_dfs, ignore_index=True) if delta_dfs else pd.DataFrame()

        # Load existing aggregated file if present
        existing = self._load_latest_aggregated(client, bucket_name)
        if existing.empty:
            return delta_df

        combined = pd.concat([existing, delta_df], ignore_index=True)
        return combined

    def _load_latest_aggregated(self, client: object, bucket_name: str) -> pd.DataFrame:
        """Load the latest aggregated file from GCS."""
        blobs = list(client.list_blobs(bucket_name, prefix=AGGREGATED_PREFIX))
        pattern = re.compile(r"aggregated_instruments_(\d{4}-\d{2}-\d{2})\.parquet")
        candidates = [(b, pattern.search(b.name)) for b in blobs if pattern.search(b.name)]

        if not candidates:
            return pd.DataFrame()

        latest = max(candidates, key=lambda x: x[1].group(1) if x[1] else "")
        blob = latest[0]
        try:
            data = client.download_bytes(bucket=bucket_name, blob_path=blob.name)
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            logger.warning(f"Could not load existing aggregated {blob.name}: {e}")
            return pd.DataFrame()

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate by instrument_key, keeping latest by timestamp."""
        if DEDUP_COL not in df.columns:
            return df.drop_duplicates(subset=[c for c in df.columns if "instrument" in c.lower()][:1])

        if TIMESTAMP_COL in df.columns:
            df = df.sort_values(TIMESTAMP_COL, ascending=False)
        return df.drop_duplicates(subset=[DEDUP_COL], keep="first")
