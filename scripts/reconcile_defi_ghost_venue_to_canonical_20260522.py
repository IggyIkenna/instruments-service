#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Reconcile DeFi ghost venue names to canonical names in the availability manifest.

Audit 2026-05-22 found ghost venue names (UNISWAP_V3, UNISWAP_V2, AAVE_V3) sitting in
the consolidated manifest alongside 0 canonical rows (UNISWAP_V3, UNISWAP_V2, AAVE_V3).

Root cause: consolidator incremental merge skips ``_index/per_vm/local-10889-bd08.parquet``
(written 2026-05-03) because it predates the current consolidated index (2026-05-22 03:44).
That shard has 187k UNISWAP_V3 rows, 20k UNISWAP_V2 rows, 27k AAVE_V3 rows.
Canonical GCS parquets already exist at split paths for the full date range.

This script writes a new per-VM corrector shard that:
1. Copies canonical rows from local-10889 for UNISWAP_V3/V2/AAVE_V3/V4 (bumps attempted_at
   to now so consolidator last-write-wins picks them up).
2. Adds superseding empty_confirmed rows for ghost venues UNISWAP_V3/UNISWAP_V2/AAVE_V3 with
   reason=EXPECTED_DEPRECATED_DATA_TYPE (newer attempted_at → wins over v7_to_v8_migrate rows).

The manifest consolidator runs every 1 minute. After this script uploads the shard, the
next consolidation run will merge canonical rows + suppress ghost rows automatically.

Issue doc: ``plans/active/issues/defi_coverage_capability_alignment_2026_05_22.md`` § Bug 3
Schema audit: UNISWAP_V3/V2/AAVE_V3 GCS canonical parquets confirmed to exist for all dates.

Usage::

    cd instruments-service
    source .venv/bin/activate

    # Scan mode (default — prints summary, no writes)
    python scripts/reconcile_defi_ghost_venue_to_canonical_20260522.py

    # Apply (writes corrector shard to GCS)
    python scripts/reconcile_defi_ghost_venue_to_canonical_20260522.py --apply

    # Apply to a specific bucket
    python scripts/reconcile_defi_ghost_venue_to_canonical_20260522.py \\
        --apply --bucket market-data-tick-defi-central-element-323112
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"market-data-tick-defi-{PROJECT_ID}"
SOURCE_SHARD = "_index/per_vm/local-10889-bd08.parquet"
MANIFEST_BLOB = "_index/availability_index.parquet"
CORRECTOR_SHARD = "_index/per_vm/ikenna-slot1-ghost-venue-corrector-defi-20260522.parquet"

# Ghost venue → canonical venue mapping
GHOST_TO_CANONICAL: dict[str, str] = {
    "UNISWAP_V3": "UNISWAP_V3",
    "UNISWAP_V2": "UNISWAP_V2",
    "AAVE_V3": "AAVE_V3",
}

# Canonical venues to pull from source shard (includes UNISWAP_V4 as bonus)
CANONICAL_VENUES = {"UNISWAP_V3", "UNISWAP_V2", "AAVE_V3", "UNISWAP_V4"}

# Reason for ghost rows: they are deprecated venue names superseded by canonical forms.
DEPRECATED_REASON = "EXPECTED_DEPRECATED_DATA_TYPE"


def _load_shard(client: storage.Client, bucket_name: str, blob_path: str) -> pd.DataFrame:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    logger.info("Loading %s from gs://%s/%s", blob_path.split("/")[-1], bucket_name, blob_path)
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("  → %d rows loaded", len(df))
    return df


def _build_corrector_shard(
    source_df: pd.DataFrame,
    manifest_df: pd.DataFrame,
    now: datetime,
) -> pd.DataFrame:
    """Build the corrector shard dataframe.

    Returns a dataframe with:
    - Canonical venue rows from source_df (UNISWAP_V3/V2/AAVE_V3/V4) with updated attempted_at
    - Ghost venue superseding rows from manifest_df with empty_confirmed + deprecated reason
    """
    frames: list[pd.DataFrame] = []
    now_str = now.isoformat()

    # 1. Canonical rows from source shard
    canonical_rows = source_df[source_df["venue"].isin(CANONICAL_VENUES)].copy()
    if len(canonical_rows) > 0:
        canonical_rows["attempted_at"] = now_str
        canonical_rows["written_at"] = now_str
        logger.info("Canonical rows from source shard: %d", len(canonical_rows))
        for v, cnt in canonical_rows["venue"].value_counts().items():
            logger.info("  %s: %d rows", v, cnt)
        frames.append(canonical_rows)

    # 2. Ghost superseding rows: one empty_confirmed row per unique ghost row key
    for ghost_venue in GHOST_TO_CANONICAL:
        ghost_rows = manifest_df[manifest_df["venue"] == ghost_venue].copy()
        if len(ghost_rows) == 0:
            logger.info("Ghost venue %s: 0 rows in manifest — skip", ghost_venue)
            continue
        logger.info(
            "Ghost venue %s: %d rows → superseding with empty_confirmed/%s",
            ghost_venue,
            len(ghost_rows),
            DEPRECATED_REASON,
        )
        superseding = ghost_rows.copy()
        superseding["capture_status"] = "empty_confirmed"
        superseding["error_reason"] = DEPRECATED_REASON
        superseding["attempted_at"] = now_str
        superseding["written_at"] = now_str
        superseding["available"] = False
        frames.append(superseding)

    if not frames:
        logger.warning("Nothing to write — corrector shard would be empty")
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _scan(client: storage.Client, bucket_name: str) -> None:
    source_df = _load_shard(client, bucket_name, SOURCE_SHARD)
    manifest_df = _load_shard(client, bucket_name, MANIFEST_BLOB)

    print("\n=== SCAN RESULTS ===")
    print("\n[Source shard canonical venues to ADD to consolidated manifest]")
    for v in sorted(CANONICAL_VENUES):
        cnt = (source_df["venue"] == v).sum()
        in_manifest = (manifest_df["venue"] == v).sum()
        print(f"  {v}: {cnt} rows in source shard, {in_manifest} rows currently in consolidated manifest")

    print("\n[Ghost venues to SUPERSEDE with empty_confirmed]")
    for ghost, canonical in sorted(GHOST_TO_CANONICAL.items()):
        cnt = (manifest_df["venue"] == ghost).sum()
        print(f"  {ghost} → {canonical}: {cnt} ghost rows in consolidated manifest")

    print("\n[Corrector shard would be uploaded to]")
    print(f"  gs://{bucket_name}/{CORRECTOR_SHARD}")
    print("\nRun with --apply to write the corrector shard.")


def _apply(client: storage.Client, bucket_name: str) -> None:
    source_df = _load_shard(client, bucket_name, SOURCE_SHARD)
    manifest_df = _load_shard(client, bucket_name, MANIFEST_BLOB)

    now = datetime.now(UTC)
    corrector_df = _build_corrector_shard(source_df, manifest_df, now)

    if len(corrector_df) == 0:
        logger.warning("Corrector shard is empty — nothing to upload. Exiting.")
        sys.exit(1)

    logger.info("Corrector shard: %d total rows", len(corrector_df))

    buf = io.BytesIO()
    corrector_df.to_parquet(buf, index=False)
    buf.seek(0)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(CORRECTOR_SHARD)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Uploaded corrector shard → gs://%s/%s", bucket_name, CORRECTOR_SHARD)
    logger.info("Manifest consolidator (runs */1 * * * *) will merge this shard within ~1 minute.")
    logger.info("After consolidation: check canonical rows in consolidated manifest + ghost rows gone.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write corrector shard to GCS. Default: scan-only.")
    parser.add_argument("--bucket", default=BUCKET, help=f"GCS bucket (default: {BUCKET})")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    client = storage.Client(project=PROJECT_ID)
    if args.apply:
        _apply(client, args.bucket)
    else:
        _scan(client, args.bucket)


if __name__ == "__main__":
    main()
