#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after footystats ODDS rows confirmed absent from prod index (verify via audit)
#   (plan sports_manifest_canonical_form_migration_2026_06_25 §GCS wipe TODO)
"""Wipe footystats ODDS rows from sports manifest and their GCS parquet objects.

Context (2026-06-25): task #6 retired ODDS=MTDS in the instruments-service orchestrator
(UAC@8fb1f54f, IS@6404abd+4f6a32ed). ~113,530 footystats ODDS rows remain in the manifest
and ~29,700 are 'captured' (with GCS parquet objects). IS no longer writes ODDS so these are
stale orphans. Must be wiped BEFORE the §0.5 observable backfill relaunch so the new shards
don't see stale ODDS cells in the manifest.

Scope (from prod manifest as of 2026-06-25):
  Total footystats ODDS rows:  113,530
  captured:                     29,700  (have GCS objects)
  empty_confirmed:              74,491
  expected_unattempted:          9,256
  attempted_failed:                 83

KEEP: ODDS rows where source != 'footystats' (e.g. odds_api — 62 rows, untouched).
KEEP: footystats PREDICTIONS rows (untouched throughout #6).

Dry-run by default. Pass --apply to write.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import resolve_bucket_name
from unified_trading_library import gcs_copy_object, gcs_delete_object, get_storage_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wipe_footystats_odds")

INDEX_BLOB = "_index/availability_index.parquet"
SEED_BLOB = "_legacy_seed.parquet"
SNAPSHOT_PREFIX = "_index/snapshots"

_DATA_TYPE = "ODDS"
_SOURCE = "footystats"
_PIPELINE_MODE = "batch_footystats"


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _snapshot_and_write(
    client: object,
    bucket: str,
    blob_path: str,
    df_new: pd.DataFrame,
    snap_label: str,
    dry_run: bool,
) -> None:
    """Snapshot current parquet then write df_new. No-op if dry_run."""
    blob_full = f"{bucket}/{blob_path}"
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_blob = f"gs://{bucket}/{SNAPSHOT_PREFIX}/{snap_label}_{ts}.parquet"

    if dry_run:
        logger.info("DRY-RUN: would snapshot %s → %s and write %d rows", blob_full, snap_blob, len(df_new))
        return

    # Snapshot: server-side GCS copy (no local temp needed)
    gcs_copy_object(f"gs://{bucket}/{blob_path}", snap_blob)
    logger.info("Snapshot written: %s", snap_blob)

    # Write cleaned parquet
    tmp_out = tempfile.mktemp(suffix="_out.parquet")
    df_new.to_parquet(tmp_out, index=False)
    client.upload_file(bucket=bucket, blob_path=blob_path, local_path=tmp_out)
    logger.info("Written %d rows to %s", len(df_new), blob_full)


def _list_footystats_odds_gcs_objects(client: object, bucket: str) -> list[str]:
    """List all GCS objects under the footystats_odds entity prefix. Returns paths without bucket prefix."""
    # All footystats ODDS paths contain 'entity=footystats_odds' — list under that prefix
    prefixes = [
        "sports_reference/by_date/",  # covers both pipeline_mode= and legacy entity= layouts
    ]
    found: list[str] = []
    for prefix in prefixes:
        logger.info("Listing GCS objects under gs://%s/%s (filtering for footystats_odds)", bucket, prefix)
        try:
            blobs = client.list_blobs(bucket, prefix=prefix)
            for blob in blobs:
                if "footystats_odds" in blob.name:
                    found.append(blob.name)
        except Exception as exc:
            logger.warning("Listing failed for prefix %s: %s", prefix, exc)

    logger.info("Found %d GCS objects matching footystats_odds", len(found))
    return found


_GCS_DELETE_WORKERS = 32


def _delete_gcs_objects(
    client: object,
    bucket: str,
    paths: list[str],
    dry_run: bool,
) -> int:
    """Delete the listed GCS objects in parallel. Returns count deleted."""
    if not paths:
        logger.info("GCS deletion: no objects to delete")
        return 0

    if dry_run:
        logger.info("DRY-RUN: would delete %d GCS objects (sample: %s)", len(paths), paths[:3])
        return len(paths)

    deleted = 0
    errors = 0

    def _delete_one(path: str) -> bool:
        gcs_delete_object(f"gs://{bucket}/{path}")
        return True

    with ThreadPoolExecutor(max_workers=_GCS_DELETE_WORKERS) as pool:
        futures = {pool.submit(_delete_one, p): p for p in paths}
        for i, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            try:
                future.result()
                deleted += 1
            except Exception as exc:
                logger.warning("Failed to delete gs://%s/%s: %s", bucket, path, exc)
                errors += 1
            if i % 5000 == 0:
                logger.info("GCS deletion progress: %d/%d", i, len(paths))

    logger.info("GCS objects: deleted=%d  errors=%d", deleted, errors)
    return deleted


def _filter_out_odds_footystats(df: pd.DataFrame, label: str) -> tuple[pd.DataFrame, int]:
    """Remove rows where data_type=ODDS AND source=footystats."""
    mask = (df["data_type"].str.upper() == _DATA_TYPE) & (df["source"] == _SOURCE)
    n = int(mask.sum())
    logger.info("%s: %d rows total, %d footystats ODDS rows to remove", label, len(df), n)
    if n > 0:
        by_status = df.loc[mask, "capture_status"].value_counts().to_dict()
        logger.info("%s: capture_status breakdown of deleted rows: %s", label, by_status)
    return df[~mask].copy(), n


def main() -> None:
    parser = argparse.ArgumentParser(description="Wipe footystats ODDS rows from sports manifest + GCS")
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--skip-seed", action="store_true", help="Skip _legacy_seed.parquet")
    parser.add_argument("--skip-index", action="store_true", help="Skip _index/availability_index.parquet")
    parser.add_argument("--skip-gcs", action="store_true", help="Skip GCS object deletion")
    args = parser.parse_args()

    dry_run = not args.apply
    if dry_run:
        logger.info("DRY-RUN mode (pass --apply to write)")

    bucket = _resolve_bucket()
    logger.info("Bucket: %s", bucket)
    client = get_storage_client()

    # ─── Load index ──────────────────────────────────────────────────────────
    tmp_idx = tempfile.mktemp(suffix="_idx.parquet")
    client.bucket(bucket).blob(INDEX_BLOB).download_to_filename(tmp_idx)
    df_idx = pd.read_parquet(tmp_idx)
    logger.info("Index loaded: %d rows", len(df_idx))

    # ─── GCS deletion (list-based, no per-path probing) ──────────────────────
    if not args.skip_gcs:
        gcs_paths = _list_footystats_odds_gcs_objects(client, bucket)
        n_gcs = _delete_gcs_objects(client, bucket, gcs_paths, dry_run)
        logger.info("GCS objects %s: %d", "would delete" if dry_run else "deleted", n_gcs)

    # ─── Index ────────────────────────────────────────────────────────────────
    if not args.skip_index:
        df_idx_clean, n_idx = _filter_out_odds_footystats(df_idx, "index")
        # Verify we're not deleting odds_api rows
        remaining_odds = df_idx_clean[df_idx_clean["data_type"].str.upper() == _DATA_TYPE]
        if len(remaining_odds) > 0:
            logger.info("Remaining ODDS rows (other sources): %d", len(remaining_odds))
            logger.info("  sources: %s", remaining_odds["source"].value_counts().to_dict())
        _snapshot_and_write(client, bucket, INDEX_BLOB, df_idx_clean, "pre_footystats_odds_wipe_index", dry_run)
        logger.info("Index: %d rows removed, %d rows remain", n_idx, len(df_idx_clean))

    # ─── Seed ─────────────────────────────────────────────────────────────────
    if not args.skip_seed:
        tmp_seed = tempfile.mktemp(suffix="_seed.parquet")
        try:
            client.bucket(bucket).blob(SEED_BLOB).download_to_filename(tmp_seed)
            df_seed = pd.read_parquet(tmp_seed)
            logger.info("Seed loaded: %d rows", len(df_seed))
            if "data_type" in df_seed.columns and "source" in df_seed.columns:
                df_seed_clean, n_seed = _filter_out_odds_footystats(df_seed, "seed")
                _snapshot_and_write(client, bucket, SEED_BLOB, df_seed_clean, "pre_footystats_odds_wipe_seed", dry_run)
                logger.info("Seed: %d rows removed, %d rows remain", n_seed, len(df_seed_clean))
            else:
                logger.info(
                    "Seed: missing data_type/source columns — skipping filter (columns: %s)", list(df_seed.columns)
                )
        except Exception as exc:
            logger.warning("Seed: could not load %s — skipping: %s", SEED_BLOB, exc)

    logger.info("Done. Pass --apply to make changes permanent." if dry_run else "Done (changes applied).")


if __name__ == "__main__":
    main()
