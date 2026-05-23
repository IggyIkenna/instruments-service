#!/usr/bin/env python3
"""Purge IS prediction manifest rows where canonical_question_group=OTHER.

Context (predictions_master.md Phase 5.reclassify):
  The Polymarket classifier bug (UAC pre-228c317a) assigned MONTHLY resolution to daily
  price markets that encode a month+day in their slug (e.g. "btc-up-or-down-may-22").
  No (CRYPTO_PRICE, BTC, MONTHLY) entry existed in the dict so these fell through to
  canonical_question_group=OTHER.

  Fix UAC@228c317a added a RANGE_BRACKET+MONTHLY→DAILY fallback so those markets now
  correctly route to BTC_UP_DOWN_DAILY / SPX_UP_DOWN_DAILY / etc.

  This script removes the stale OTHER rows from the IS prediction manifest
  (data_type=prediction_canonical_question_group, canonical_question_group=OTHER)
  so the backfill VM re-classifies them with the fixed classifier.

Usage:
    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_prediction_other_group_rows.py [--dry-run]

After this runs, rebuild the tarball and launch the IS prediction backfill VM
(Phase 5.reclassify) to regenerate the purged rows with correct canonical groups.
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

# Shards known to have only good-schema (correct data_type) rows from prior fix runs.
GOOD_SHARDS = frozenset(
    {
        "_index/per_vm/instr-backfill-pred-20260522.parquet",
        "_index/per_vm/instr-backfill-pred.parquet",
        "_index/per_vm/local_predict_smoke_04fbe0.parquet",
    }
)

CORRECT_DATA_TYPE = "prediction_canonical_question_group"
OTHER_GROUP_VALUE = "OTHER"

# The IS prediction manifest stores the canonical_question_group value in the
# `underlying` column (not a separate column), so we filter on underlying=OTHER.


def _count_other_rows(df: pd.DataFrame) -> int:
    """Return count of rows with correct data_type but underlying=OTHER (unclassified)."""
    if "data_type" not in df.columns or "underlying" not in df.columns:
        return 0
    mask = (df["data_type"] == CORRECT_DATA_TYPE) & (df["underlying"] == OTHER_GROUP_VALUE)
    return int(mask.sum())


def _strip_other_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe with OTHER underlying rows removed."""
    if "data_type" not in df.columns or "underlying" not in df.columns:
        return df
    keep = ~((df["data_type"] == CORRECT_DATA_TYPE) & (df["underlying"] == OTHER_GROUP_VALUE))
    return df[keep].copy()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Purge OTHER canonical_question_group rows from IS prediction manifest."
    )
    parser.add_argument("--dry-run", action="store_true", help="No writes/deletes.")
    args = parser.parse_args()
    dry_run = args.dry_run

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    # --- Step 1: Snapshot canonical manifest ---
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    snapshot_path = f"_index/snapshots/pre_other_purge_{ts}.parquet"
    blob_canonical = bucket.blob(CANONICAL_INDEX)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        blob_canonical.download_to_filename(f.name)
        df_canonical = pd.read_parquet(f.name)

    other_in_canonical = _count_other_rows(df_canonical)
    logger.info(
        "Canonical manifest: %d total rows, %d OTHER canonical_question_group rows",
        len(df_canonical),
        other_in_canonical,
    )
    if "underlying" in df_canonical.columns:
        logger.info(
            "underlying distribution: %s",
            df_canonical["underlying"].value_counts(dropna=False).to_dict(),
        )

    if not dry_run:
        buf = io.BytesIO()
        df_canonical.to_parquet(buf, index=False)
        bucket.blob(snapshot_path).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
        logger.info("Snapshot saved: gs://%s/%s", BUCKET_NAME, snapshot_path)
    else:
        logger.info("[dry-run] Would snapshot to gs://%s/%s", BUCKET_NAME, snapshot_path)

    # --- Step 2: Walk per-VM shards ---
    all_shards = list(client.list_blobs(BUCKET_NAME, prefix=PER_VM_PREFIX))
    logger.info("Found %d per-VM shards", len(all_shards))

    shards_deleted = 0
    shards_patched = 0
    shards_clean = 0
    good_dfs: list[pd.DataFrame] = []

    for blob in all_shards:
        shard_name = blob.name
        with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
            blob.download_to_filename(f.name)
            df_shard = pd.read_parquet(f.name)

        other_count = _count_other_rows(df_shard)

        if other_count == 0:
            good_dfs.append(df_shard)
            shards_clean += 1
            logger.info("CLEAN %s (%d rows, 0 OTHER)", shard_name, len(df_shard))
            continue

        df_clean = _strip_other_rows(df_shard)
        logger.info(
            "SHARD %s: %d OTHER rows of %d total → %d rows remain",
            shard_name,
            other_count,
            len(df_shard),
            len(df_clean),
        )

        if len(df_clean) == 0:
            # Shard is entirely OTHER — delete it
            if not dry_run:
                blob.delete()
                logger.info("DELETED %s (was all-OTHER)", shard_name)
            else:
                logger.info("[dry-run] WOULD DELETE %s (all-OTHER)", shard_name)
            shards_deleted += 1
        else:
            # Mixed shard — re-upload without OTHER rows
            good_dfs.append(df_clean)
            if not dry_run:
                buf = io.BytesIO()
                df_clean.to_parquet(buf, index=False)
                blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
                logger.info("PATCHED %s → %d rows remain", shard_name, len(df_clean))
            else:
                logger.info("[dry-run] WOULD PATCH %s → %d rows remain", shard_name, len(df_clean))
            shards_patched += 1

    logger.info(
        "Shard summary: %d clean (unchanged), %d patched (OTHER rows removed), %d deleted (all-OTHER)",
        shards_clean,
        shards_patched,
        shards_deleted,
    )

    # --- Step 3: Build new canonical manifest ---
    if good_dfs:
        df_new = pd.concat(good_dfs, ignore_index=True).drop_duplicates()
    else:
        df_new = pd.DataFrame(columns=df_canonical.columns)

    remaining_other = _count_other_rows(df_new)
    if remaining_other > 0:
        logger.error("BUG: %d OTHER rows still in new canonical — aborting!", remaining_other)
        return 1

    logger.info(
        "New canonical manifest: %d rows (was %d, purged %d OTHER rows)",
        len(df_new),
        len(df_canonical),
        len(df_canonical) - len(df_new),
    )
    if "underlying" in df_new.columns and len(df_new) > 0:
        logger.info(
            "New underlying distribution: %s",
            df_new["underlying"].value_counts(dropna=False).to_dict(),
        )

    if not dry_run:
        buf = io.BytesIO()
        if len(df_new) > 0:
            df_new.to_parquet(buf, index=False)
        else:
            pd.DataFrame(columns=df_canonical.columns).to_parquet(buf, index=False)
        bucket.blob(CANONICAL_INDEX).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
        logger.info("Uploaded clean canonical manifest to gs://%s/%s", BUCKET_NAME, CANONICAL_INDEX)
    else:
        logger.info(
            "[dry-run] Would upload %d-row clean manifest to gs://%s/%s",
            len(df_new),
            BUCKET_NAME,
            CANONICAL_INDEX,
        )

    logger.info(
        "Done. Next: rebuild tarball + launch instr-backfill-pred VM with --asset-group PREDICTION "
        "--start 2020-01-01 --end 2026-05-23 to regenerate the purged rows with fixed classifier."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
