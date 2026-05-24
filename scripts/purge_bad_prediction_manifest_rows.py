#!/usr/bin/env python3
"""Purge bad IS prediction manifest rows and stale per-VM shards.

Context (data_status_coverage_gaps_and_prediction_manifest_fix_2026_05_22.md Phase 3.2):
  The old IS prediction writer (pre-fix) wrote manifest rows with:
    data_type=BTC/ETH/SOL/OTHER/... (wrong — treating underlying as data_type bucket)
    underlying="" (blank — should hold BTC/ETH/etc.)
    capture_status=None (3,145 rows never finalised) or captured (795 rows)

  The fixed writer (already in production) writes:
    data_type="prediction_canonical_question_group"
    canonical_question_group=<CanonicalQuestionGroup enum value>
    underlying=<base_asset>

  This script:
    1. Snapshots the canonical manifest.
    2. Deletes all per-VM shards that contain ONLY bad-schema rows.
    3. Overwrites the canonical manifest with only good-schema rows.
    4. Logs summary.

Usage:
    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_bad_prediction_manifest_rows.py [--dry-run]

After this runs, launch the IS prediction backfill (Phase 3.4) to regenerate the data.
"""

from __future__ import annotations

import argparse
import io
import logging
import tempfile
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET_NAME = f"instruments-store-prediction-{PROJECT_ID}"
CANONICAL_INDEX = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

# Shards to KEEP (have correct schema from fixed writer)
GOOD_SHARDS = frozenset(
    {
        "_index/per_vm/instr-backfill-pred-20260522.parquet",
        "_index/per_vm/instr-backfill-pred.parquet",
        "_index/per_vm/local_predict_smoke_04fbe0.parquet",  # check below
    }
)

BAD_DATA_TYPES = frozenset(
    {
        "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE",
        "SPX", "NDX", "DJIA", "GOLD", "SILVER", "CRUDE_OIL",
        "FOOTBALL", "OTHER", "",
    }
)


def _has_bad_schema(df: pd.DataFrame) -> bool:
    """Return True if shard contains any bad-schema rows."""
    if "data_type" not in df.columns:
        return False
    bad_mask = df["data_type"].fillna("").isin(BAD_DATA_TYPES)
    return bool(bad_mask.any())


def _has_only_good_schema(df: pd.DataFrame) -> bool:
    """Return True if all rows have the canonical prediction schema."""
    if "data_type" not in df.columns:
        return False
    good_mask = df["data_type"] == "prediction_canonical_question_group"
    return bool(good_mask.all())


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge bad IS prediction manifest rows.")
    parser.add_argument("--dry-run", action="store_true", help="No writes/deletes.")
    args = parser.parse_args()
    dry_run = args.dry_run

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    # Step 1: Snapshot canonical manifest
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    snapshot_path = f"_index/snapshots/pre_prediction_fix_{ts}.parquet"
    blob_canonical = bucket.blob(CANONICAL_INDEX)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        blob_canonical.download_to_filename(f.name)
        df_canonical = pd.read_parquet(f.name)

    logger.info("Canonical manifest: %d rows", len(df_canonical))
    if not dry_run:
        blob_canonical.copy_blob_from_object(
            destination_bucket=bucket,
            new_name=snapshot_path,
            source_generation=blob_canonical.generation,
        ) if False else None  # use upload instead
        # Upload snapshot
        buf = io.BytesIO()
        df_canonical.to_parquet(buf, index=False)
        bucket.blob(snapshot_path).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
        logger.info("Snapshot saved: gs://%s/%s", BUCKET_NAME, snapshot_path)
    else:
        logger.info("[dry-run] Would snapshot to gs://%s/%s", BUCKET_NAME, snapshot_path)

    # Step 2: Enumerate per-VM shards
    all_shards = list(client.list_blobs(BUCKET_NAME, prefix=PER_VM_PREFIX))
    logger.info("Found %d per-VM shards", len(all_shards))

    shards_deleted = 0
    shards_kept = 0
    shards_skipped = 0
    good_dfs: list[pd.DataFrame] = []

    for blob in all_shards:
        shard_name = blob.name
        if shard_name in GOOD_SHARDS:
            # Always keep these
            with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
                blob.download_to_filename(f.name)
                df_shard = pd.read_parquet(f.name)
            if _has_only_good_schema(df_shard):
                good_dfs.append(df_shard)
                shards_kept += 1
                logger.info("KEEP (good schema) %s (%d rows)", shard_name, len(df_shard))
            else:
                # Even named good shards get checked — if mixed, keep only good rows
                good_rows = df_shard[df_shard["data_type"] == "prediction_canonical_question_group"]
                if len(good_rows) > 0:
                    good_dfs.append(good_rows)
                    shards_kept += 1
                    logger.info("KEEP (partial good) %s (%d/%d good rows)", shard_name, len(good_rows), len(df_shard))
                else:
                    shards_skipped += 1
                    logger.warning("SKIP (no good rows despite being in GOOD_SHARDS) %s", shard_name)
            continue

        # Download and check
        with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
            blob.download_to_filename(f.name)
            df_shard = pd.read_parquet(f.name)

        if not _has_bad_schema(df_shard):
            # Has only good schema rows (shouldn't happen for other shards but be safe)
            good_dfs.append(df_shard)
            shards_kept += 1
            logger.info("KEEP (already good) %s (%d rows)", shard_name, len(df_shard))
            continue

        # Delete this shard
        if not dry_run:
            blob.delete()
            logger.info("DELETED %s (%d bad rows)", shard_name, len(df_shard))
        else:
            logger.info("[dry-run] WOULD DELETE %s (%d rows)", shard_name, len(df_shard))
        shards_deleted += 1

    logger.info(
        "Shard summary: %d deleted, %d kept, %d skipped",
        shards_deleted, shards_kept, shards_skipped,
    )

    # Step 3: Build new canonical manifest from good rows only
    df_new = pd.concat(good_dfs, ignore_index=True).drop_duplicates() if good_dfs else pd.DataFrame()

    logger.info("New canonical manifest: %d good rows (was %d)", len(df_new), len(df_canonical))

    # Verify no bad rows crept in
    if len(df_new) > 0 and "data_type" in df_new.columns:
        bad_rows = df_new[df_new["data_type"].isin(BAD_DATA_TYPES)]
        if len(bad_rows) > 0:
            logger.error("BUG: %d bad rows in new canonical — aborting!", len(bad_rows))
            return 1
        logger.info(
            "data_type distribution: %s",
            df_new["data_type"].value_counts().to_dict(),
        )
        logger.info(
            "capture_status distribution: %s",
            df_new["capture_status"].value_counts(dropna=False).to_dict() if "capture_status" in df_new.columns else "N/A",
        )

    if not dry_run:
        buf = io.BytesIO()
        if len(df_new) > 0:
            df_new.to_parquet(buf, index=False)
        else:
            # Write empty parquet with same schema as before
            pd.DataFrame(columns=df_canonical.columns).to_parquet(buf, index=False)
        bucket.blob(CANONICAL_INDEX).upload_from_string(
            buf.getvalue(), content_type="application/octet-stream"
        )
        logger.info("Uploaded clean canonical manifest to gs://%s/%s", BUCKET_NAME, CANONICAL_INDEX)
    else:
        logger.info("[dry-run] Would upload %d-row clean manifest to gs://%s/%s", len(df_new), BUCKET_NAME, CANONICAL_INDEX)

    logger.info("Done. Next step: run IS prediction backfill (Phase 3.4) to regenerate from 2024-01-01.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
