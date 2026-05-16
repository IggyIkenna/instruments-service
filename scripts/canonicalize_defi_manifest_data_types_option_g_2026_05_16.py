#!/usr/bin/env python3
"""Option G fix: DROP kebab rows from canonical _index (not write parallel snake rows).

Per ``plans/active/issues/vocab_drift_canonicalisation_didnt_stick_2026_05_16.md``:
the original Option A canonicalize script wrote per-VM shards with snake-form
rows, then relied on the manifest_consolidator daemon to merge them. The
consolidator's row-key UPSERT semantics include `data_type`, so
`(date, venue, chain, lending-indices)` and `(date, venue, chain, lending_indices)`
are different rows → both survive merge. Net result: kebab rows still leak into
downstream snake-only queries (112,299 across 4 buckets post-2026-05-16
canonicalize-apply).

This script bypasses the consolidator semantics by rewriting the canonical
``_index/availability_index.parquet`` directly:

1. Read the canonical _index for each affected bucket.
2. DROP every row where `data_type == <kebab>`.
3. Write the cleaned snapshot back to canonical _index.
4. ALSO clear any per-VM canonicalize shards under
   `_index/per_vm/manifest-canonicalize-{bucket}-*` so consolidator merges
   on next cycle don't reintroduce snake-duplicates of the just-dropped rows
   (the snake rows in the canonical _index already cover the same date
   range; the per-VM shards are now redundant).

This is the Option D pattern (proven to work for lst-rates + oracle-prices
2026-05-16 20:00 UTC) generalised to the 4 remaining drift buckets:

    lending-indices  (24,976 kebab rows still present)
    perp-funding     ( 3,298 kebab rows still present)
    dex-swaps        (28,171 kebab rows still present)
    dex-pools        (55,854 kebab rows still present)
    Total: 112,299 rows

Usage:
    python scripts/canonicalize_defi_manifest_data_types_option_g_2026_05_16.py --dry-run
    python scripts/canonicalize_defi_manifest_data_types_option_g_2026_05_16.py --apply --confirm

Idempotent: re-runs on clean buckets find 0 kebab rows.

Closes:
    plans/active/issues/vocab_drift_canonicalisation_didnt_stick_2026_05_16.md

Execution ownership (Runbook SSOT):
  execution:
    owner: slot-4-ikenna (cross-slot pickup of slot-2-filed issue)
    cadence: one-shot
    verifier: each canonical _index groupby data_type returns 1 canonical snake row
    last_executed: 2026-05-16
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"

# (bucket-name-without-pid-suffix, kebab_value, snake_value)
TARGET_BUCKETS: list[tuple[str, str, str]] = [
    ("lending-indices", "lending-indices", "lending_indices"),
    ("perp-funding", "perp-funding", "perp_funding"),
    ("dex-swaps", "dex-swaps", "dex_swaps"),
    ("dex-pools", "dex-pools", "dex_pools"),
]


def process_one(
    *,
    client: storage.Client,
    bucket_name: str,
    kebab: str,
    snake: str,
    apply: bool,
) -> int:
    """Return kebab-row count for the bucket. Drops them if apply."""
    full_bucket = f"{bucket_name}-{PROJECT_ID}"
    bucket = client.bucket(full_bucket)
    blob = bucket.blob(INDEX_BLOB)
    if not blob.exists():
        logger.warning("[%s] canonical _index missing — skipping", full_bucket)
        return 0

    local_path = f"{tempfile.gettempdir()}/{bucket_name}_pre_option_g.parquet"
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("[%s] canonical _index rows: %d", full_bucket, len(df))

    if "data_type" not in df.columns:
        logger.error("[%s] missing data_type column — skipping", full_bucket)
        return 0

    dt = df["data_type"].astype(str)
    kebab_mask = dt == kebab
    kebab_count = int(kebab_mask.sum())
    snake_count = int((dt == snake).sum())
    logger.info(
        "[%s] kebab (%s) rows: %d; snake (%s) rows: %d",
        full_bucket,
        kebab,
        kebab_count,
        snake,
        snake_count,
    )

    if kebab_count == 0:
        logger.info("[%s] nothing to drop — already clean", full_bucket)
        return 0

    if not apply:
        return kebab_count

    # Drop kebab rows from the canonical _index (overwrite blob).
    clean_df = df.loc[~kebab_mask].copy()
    logger.info(
        "[%s] writing cleaned canonical _index: %d rows (was %d; dropped %d kebab)",
        full_bucket,
        len(clean_df),
        len(df),
        kebab_count,
    )

    # Clear the per-VM canonicalize shards so consolidator doesn't reintroduce
    # snake-duplicates on next cycle.
    for shard_name in (
        f"_index/per_vm/manifest-canonicalize-{bucket_name}-kebab-to-snake.parquet",
        # Slot-4 earlier draft wrote a slightly different shard name for lending-indices:
        "_index/per_vm/manifest-canonicalize-data-type-kebab-to-snake.parquet",
    ):
        shard_blob = bucket.blob(shard_name)
        if not shard_blob.exists():
            continue
        shard_local = f"{tempfile.gettempdir()}/{bucket_name}_shard_check.parquet"
        shard_blob.download_to_filename(shard_local)
        shard_df = pd.read_parquet(shard_local)
        logger.info(
            "[%s] clearing canonicalize shard %s (was %d rows)",
            full_bucket,
            shard_name,
            len(shard_df),
        )
        # Write a 0-row shard preserving schema so consolidator merge is a no-op.
        empty_df = shard_df.iloc[0:0].copy()
        sout = io.BytesIO()
        empty_df.to_parquet(sout, index=False)
        sout.seek(0)
        shard_blob.upload_from_file(sout, content_type="application/octet-stream")

    # Write the cleaned canonical _index back.
    out = io.BytesIO()
    clean_df.to_parquet(out, index=False)
    out.seek(0)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("[%s] canonical _index rewritten: %d rows", full_bucket, len(clean_df))
    return kebab_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report counts; no writes")
    parser.add_argument("--apply", action="store_true", help="rewrite canonical _index minus kebab rows")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="required with --apply (safety belt; overwrites canonical _index)",
    )
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        parser.error("pass --dry-run or --apply")
    if args.apply and not args.confirm:
        parser.error("--apply requires --confirm (this overwrites canonical _index)")

    client = storage.Client(project=PROJECT_ID)
    total = 0
    for bucket_name, kebab, snake in TARGET_BUCKETS:
        kebab_count = process_one(
            client=client,
            bucket_name=bucket_name,
            kebab=kebab,
            snake=snake,
            apply=args.apply,
        )
        total += kebab_count

    logger.info("=" * 60)
    logger.info("SUMMARY (%s):", "applied" if args.apply else "dry-run")
    logger.info("  Total kebab rows dropped: %d", total)
    if args.apply and total > 0:
        logger.info(
            "Verify post-apply: each canonical _index groupby data_type should return ONLY canonical snake rows."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
