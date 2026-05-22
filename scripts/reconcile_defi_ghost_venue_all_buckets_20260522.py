#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Suppress DeFi ghost venue rows in IS and MTDS DeFi manifests.

Extends reconcile_defi_ghost_venue_to_canonical_20260522.py:
  - Original: MTDS DeFi only, covered UNISWAPV3/V2/AAVEV3 (from source shard local-10889)
  - This script: IS DeFi + MTDS residual (UNISWAPV4/SUSHISWAPV3/PANCAKESWAPV3)
    IS DeFi does not need canonical row restoration — the running instr-backfill-defi
    VM already wrote canonical rows (AAVE_V3, UNISWAP_V3, etc.).

Ghost venues (from DEPRECATED_DEFI_GHOST_VENUE_NAMES — UAC@890ac815 expanded list):
  UNISWAPV3, UNISWAPV2, AAVEV3, SUSHISWAPV3, PANCAKESWAPV3, UNISWAPV4
  + legacy camelcase: CAMELOTV3, VELODROMEV2, YEARNV3, MORPHOVAULTS, COMPOUNDV3
  + AAVEV2

For each ghost venue found in the consolidated manifest, the corrector shard:
  - Copies all ghost rows, sets capture_status=empty_confirmed,
    error_reason=EXPECTED_DEPRECATED_DATA_TYPE, bumps attempted_at to now.
  - Uploads to _index/per_vm/ik-slot5-ghost-suppressor-<bucket_tag>-<ts>.parquet.
  - Manifest consolidator (runs */1 * * * *) merges on next tick.

Usage:
    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false

    # Scan IS DeFi
    .venv/bin/python scripts/reconcile_defi_ghost_venue_all_buckets_20260522.py \\
        --bucket instruments-store-defi-central-element-323112

    # Apply IS DeFi
    .venv/bin/python scripts/reconcile_defi_ghost_venue_all_buckets_20260522.py \\
        --bucket instruments-store-defi-central-element-323112 --apply

    # Scan MTDS DeFi (residual UNISWAPV4/SUSHISWAPV3/PANCAKESWAPV3)
    .venv/bin/python scripts/reconcile_defi_ghost_venue_all_buckets_20260522.py \\
        --bucket market-data-tick-defi-central-element-323112

    # Apply MTDS DeFi residual
    .venv/bin/python scripts/reconcile_defi_ghost_venue_all_buckets_20260522.py \\
        --bucket market-data-tick-defi-central-element-323112 --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.registry.capability_declarations._defi_coverage import DEPRECATED_DEFI_GHOST_VENUE_NAMES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
MANIFEST_BLOB = "_index/availability_index.parquet"
DEPRECATED_REASON = "EXPECTED_DEPRECATED_DATA_TYPE"

# Ghost venues that need suppression (from UAC DEPRECATED_DEFI_GHOST_VENUE_NAMES + known extra)
# We use the UAC constant at runtime so this automatically picks up future additions.
EXTRA_GHOST_VENUES: frozenset[str] = frozenset(
    {
        # Era-2 camelcase names not in UAC list but found in manifests
        "CAMELOTV3",
        "VELODROMEV2",
        "YEARNV3",
        "MORPHOVAULTS",
        "COMPOUNDV3",
        "AAVEV2",
    }
)


def _get_ghost_venues() -> frozenset[str]:
    """Return union of UAC deprecated list + known extra ghost names."""
    return DEPRECATED_DEFI_GHOST_VENUE_NAMES | EXTRA_GHOST_VENUES


def _load_manifest(client: storage.Client, bucket_name: str) -> pd.DataFrame:
    blob = client.bucket(bucket_name).blob(MANIFEST_BLOB)
    logger.info("Loading manifest gs://%s/%s", bucket_name, MANIFEST_BLOB)
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("  → %d rows", len(df))
    return df


def _build_corrector_shard(manifest_df: pd.DataFrame, ghost_venues: frozenset[str], now: datetime) -> pd.DataFrame:
    now_str = now.isoformat()
    frames: list[pd.DataFrame] = []

    for ghost_venue in sorted(ghost_venues):
        ghost_rows = manifest_df[manifest_df["venue"] == ghost_venue].copy()
        if len(ghost_rows) == 0:
            continue
        # Only suppress rows currently NOT already empty_confirmed with the right reason
        needs_suppression = ghost_rows[
            ~(
                (ghost_rows["capture_status"] == "empty_confirmed")
                & (ghost_rows["error_reason"].fillna("") == DEPRECATED_REASON)
            )
        ]
        if len(needs_suppression) == 0:
            logger.info("  %s: all %d rows already suppressed — skip", ghost_venue, len(ghost_rows))
            continue
        logger.info(
            "  %s: %d rows need suppression (already ok: %d)",
            ghost_venue,
            len(needs_suppression),
            len(ghost_rows) - len(needs_suppression),
        )
        superseding = needs_suppression.copy()
        superseding["capture_status"] = "empty_confirmed"
        superseding["error_reason"] = DEPRECATED_REASON
        superseding["attempted_at"] = now_str
        superseding["written_at"] = now_str
        superseding["available"] = False
        frames.append(superseding)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _scan(client: storage.Client, bucket_name: str) -> None:
    ghost_venues = _get_ghost_venues()
    manifest_df = _load_manifest(client, bucket_name)

    print(f"\n=== SCAN: gs://{bucket_name} ===")
    print(f"Ghost venues in scope: {sorted(ghost_venues)}")
    print("\n[Ghost venue rows in consolidated manifest]")

    total_needs_suppression = 0
    for ghost_venue in sorted(ghost_venues):
        rows = manifest_df[manifest_df["venue"] == ghost_venue]
        if len(rows) == 0:
            continue
        already_ok = (
            (rows["capture_status"] == "empty_confirmed")
            & (rows["error_reason"].fillna("") == DEPRECATED_REASON)
        ).sum()
        needs = len(rows) - already_ok
        total_needs_suppression += needs
        capture_dist = rows["capture_status"].value_counts(dropna=False).to_dict()
        print(f"  {ghost_venue}: {len(rows)} total, {needs} need suppression — {capture_dist}")

    print(f"\nTotal rows needing suppression: {total_needs_suppression}")
    if total_needs_suppression == 0:
        print("✅ All ghost rows already suppressed — nothing to do.")
    else:
        print("→ Run with --apply to upload corrector shard.")


def _apply(client: storage.Client, bucket_name: str) -> None:
    ghost_venues = _get_ghost_venues()
    manifest_df = _load_manifest(client, bucket_name)
    now = datetime.now(UTC)

    logger.info("Building corrector shard for ghost venues: %s", sorted(ghost_venues))
    corrector_df = _build_corrector_shard(manifest_df, ghost_venues, now)

    if len(corrector_df) == 0:
        logger.info("✅ No ghost rows need suppression — nothing to upload.")
        sys.exit(0)

    logger.info("Corrector shard: %d rows", len(corrector_df))
    for v, cnt in corrector_df["venue"].value_counts().items():
        logger.info("  %s: %d rows → empty_confirmed/%s", v, cnt, DEPRECATED_REASON)

    ts = now.strftime("%Y%m%d-%H%M%S")
    bucket_tag = bucket_name.split("-")[0]  # "instruments" or "market"
    shard_path = f"_index/per_vm/ik-slot5-ghost-suppressor-{bucket_tag}-{ts}.parquet"

    buf = io.BytesIO()
    corrector_df.to_parquet(buf, index=False)
    buf.seek(0)

    bucket = client.bucket(bucket_name)
    bucket.blob(shard_path).upload_from_file(buf, content_type="application/octet-stream")
    logger.info("✅ Uploaded corrector shard → gs://%s/%s", bucket_name, shard_path)
    logger.info("Manifest consolidator (*/1 * * * *) will merge within ~1 minute.")
    logger.info("After consolidation: ghost rows → empty_confirmed/%s", DEPRECATED_REASON)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Upload corrector shard. Default: scan-only.")
    parser.add_argument(
        "--bucket",
        default=f"instruments-store-defi-{PROJECT_ID}",
        help=f"GCS bucket to operate on (default: instruments-store-defi-{PROJECT_ID})",
    )
    args = parser.parse_args()

    client = storage.Client(project=PROJECT_ID)
    if args.apply:
        _apply(client, args.bucket)
    else:
        _scan(client, args.bucket)


if __name__ == "__main__":
    main()
