"""
Instrument Aggregation Engine

Pure aggregation logic for deduplicating and merging instrument data.
Accepts storage client from adapter layer (no direct GCS calls).

Usage:
    aggregator = InstrumentAggregator()
    result = aggregator.aggregate_category(
        storage_client=storage_client,
        bucket_name="instruments-cefi",
        redo_all=False
    )
"""

import io
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

BASE_PREFIX = "instrument_availability/by_date/"
AGGREGATED_PREFIX = "aggregated/"
OUTPUT_FILENAME_PATTERN = "aggregated_instruments_{date}.parquet"
DEDUP_COL = "instrument_key"
TIMESTAMP_COL = "timestamp"


class InstrumentAggregator:
    """
    Pure aggregation logic for instruments.

    Accepts storage client (from adapter) and contains NO direct GCS calls.
    All storage operations delegated to passed client.
    """

    def __init__(self, max_workers: int = 12):
        """
        Initialize aggregator.

        Args:
            max_workers: Max parallel workers for loading parquet files
        """
        self.max_workers = max_workers
        logger.info(f"InstrumentAggregator initialized (max_workers={max_workers})")

    def aggregate_category(
        self,
        storage_client: object,
        bucket_name: str,
        redo_all: bool = False,
    ) -> int:
        """
        Aggregate instruments for one category bucket.

        Args:
            storage_client: Storage client from adapter (UCS)
            bucket_name: GCS bucket name for category
            redo_all: If True, rebuild from all files; if False, delta merge

        Returns:
            Number of instruments in aggregated file
        """
        if redo_all:
            df = self._load_all_from_storage(storage_client, bucket_name)
        else:
            df = self._load_delta_and_merge(storage_client, bucket_name)

        if df.empty:
            logger.warning(f"No instruments to aggregate for {bucket_name}")
            return 0

        # Deduplicate: keep latest by timestamp per instrument_key
        deduped = self._deduplicate(df)
        output_date = date.today().isoformat()
        gcs_path = f"{AGGREGATED_PREFIX}{OUTPUT_FILENAME_PATTERN.format(date=output_date)}"

        # Upload to storage via client
        buf = io.BytesIO()
        deduped.to_parquet(buf, index=False)
        storage_client.upload_bytes(
            bucket=bucket_name,
            blob_path=gcs_path,
            data=buf.getvalue(),
        )

        logger.info(f"✅ Wrote {len(deduped)} instruments to gs://{bucket_name}/{gcs_path}")
        return len(deduped)

    def _load_all_from_storage(
        self,
        storage_client: object,
        bucket_name: str,
    ) -> pd.DataFrame:
        """Load all instrument parquet files from storage (redo-all mode)."""
        blobs = [
            b
            for b in storage_client.list_blobs(bucket_name, prefix=BASE_PREFIX)
            if b.name.endswith("instruments.parquet")
        ]

        if not blobs:
            return pd.DataFrame()

        logger.info(f"Loading {len(blobs)} parquet files from gs://{bucket_name}/{BASE_PREFIX}")

        all_dfs: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(blobs))) as executor:
            futures = {executor.submit(self._download_parquet, storage_client, bucket_name, b.name): b for b in blobs}
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
        storage_client: object,
        bucket_name: str,
        blob_path: str,
    ) -> pd.DataFrame | None:
        """Download a single parquet file from storage."""
        data = storage_client.download_bytes(bucket=bucket_name, blob_path=blob_path)
        return pd.read_parquet(io.BytesIO(data))

    def _load_delta_and_merge(
        self,
        storage_client: object,
        bucket_name: str,
    ) -> pd.DataFrame:
        """Load previous day's data and merge with existing aggregated file (delta-only)."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        base_prefix = f"{BASE_PREFIX}day={yesterday}/"

        # List venue folders via bucket delimiter listing
        bucket = storage_client.bucket(bucket_name)
        iterator = bucket.list_blobs(prefix=base_prefix, delimiter="/")
        list(iterator)
        venue_folders = list(getattr(iterator, "prefixes", []))

        delta_dfs: list[pd.DataFrame] = []
        for vf in venue_folders:
            if "venue=" not in vf:
                continue
            gcs_path = f"{vf}instruments.parquet"
            try:
                df = self._download_parquet(storage_client, bucket_name, gcs_path)
                if df is not None and not df.empty:
                    delta_dfs.append(df)
            except Exception as e:
                logger.debug(f"Skip {gcs_path}: {e}")

        delta_df = pd.concat(delta_dfs, ignore_index=True) if delta_dfs else pd.DataFrame()

        # Load existing aggregated file if present
        existing = self._load_latest_aggregated(storage_client, bucket_name)
        if existing.empty:
            return delta_df

        combined = pd.concat([existing, delta_df], ignore_index=True)
        return combined

    def _load_latest_aggregated(
        self,
        storage_client: object,
        bucket_name: str,
    ) -> pd.DataFrame:
        """Load the latest aggregated file from storage."""
        blobs = list(storage_client.list_blobs(bucket_name, prefix=AGGREGATED_PREFIX))
        pattern = re.compile(r"aggregated_instruments_(\d{4}-\d{2}-\d{2})\.parquet")
        candidates = [(b, pattern.search(b.name)) for b in blobs if pattern.search(b.name)]

        if not candidates:
            return pd.DataFrame()

        latest = max(candidates, key=lambda x: x[1].group(1) if x[1] else "")
        blob = latest[0]
        try:
            data = storage_client.download_bytes(bucket=bucket_name, blob_path=blob.name)
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
