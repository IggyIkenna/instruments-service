#!/usr/bin/env python3
"""Migrate DeFi bare ``venue=.../`` GCS paths into canonical
``asset_group=defi/venue=.../`` layout.

Per CLAUDE.md SSOT: ``asset_group=`` is the canonical hive vocabulary for
new MTDS writes; ``category=`` is the legacy on-disk form preserved
without a re-keying migration.  However, a small set of 2024-05-era
DeFi parquets were written with NO asset-group hive segment at all —
just ``raw_tick_data/by_date/day={D}/venue={V}/chain={C}/...``.  These
break readers that assume the segment exists.

This script:

  1. Lists every parquet under bare ``raw_tick_data/by_date/day=*/venue=*/``
     in the DeFi bucket (paths whose first segment after ``day=*/`` is
     ``venue=`` rather than ``asset_group=`` / ``category=``).
  2. For each, computes the canonical destination path by inserting
     ``asset_group=defi/`` immediately after the ``day=*/`` segment.
  3. Server-side copy old → new (gsutil rewrite-style; no egress).
  4. Delete old object after successful copy.

Idempotent: re-running on already-migrated paths is a no-op (the bare
listing returns nothing).  Safe to run while writers are active because
new writers emit canonical ``asset_group=defi/`` directly.

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_defi_bare_to_asset_group.py --dry-run
    .venv/bin/python scripts/migrate_defi_bare_to_asset_group.py --workers 32

Set ``--limit N`` to cap the migration size for testing.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
DEFI_BUCKET = f"market-data-tick-defi-{PROJECT_ID}"

# raw_tick_data/by_date/day=YYYY-MM-DD/venue=...   (bare, no hive segment)
# vs
# raw_tick_data/by_date/day=YYYY-MM-DD/asset_group=defi/venue=...
# vs
# raw_tick_data/by_date/day=YYYY-MM-DD/category=defi/venue=...
_BARE_PATH_RE = re.compile(r"^(raw_tick_data/by_date/day=\d{4}-\d{2}-\d{2}/)venue=(?!.*?/(?:asset_group|category)=)")


def _is_bare_defi_path(name: str) -> bool:
    """Return True iff name has venue= directly under day= (no hive segment)."""
    parts = name.split("/")
    # raw_tick_data/by_date/day=YYYY-MM-DD/venue=...
    return (
        len(parts) >= 4
        and parts[0] == "raw_tick_data"
        and parts[1] == "by_date"
        and parts[2].startswith("day=")
        and parts[3].startswith("venue=")
    )


def _canonical_path(bare_path: str) -> str:
    """Insert ``asset_group=defi/`` after ``day=*/``."""
    parts = bare_path.split("/")
    # parts: [raw_tick_data, by_date, day=YYYY-MM-DD, venue=..., ...]
    return "/".join([*parts[:3], "asset_group=defi", *parts[3:]])


def _migrate_one(
    src_bucket: storage.Bucket,
    dst_bucket: storage.Bucket,
    src_name: str,
) -> tuple[str, str, bool, str]:
    """Server-side copy then delete the source.  Returns (src, dst, ok, err)."""
    dst_name = _canonical_path(src_name)
    try:
        src_blob = src_bucket.blob(src_name)
        # rewrite_to is server-side — no client egress.  copy_blob
        # works for same-bucket too.
        src_bucket.copy_blob(src_blob, dst_bucket, new_name=dst_name)
        src_blob.delete()
        return src_name, dst_name, True, ""
    except Exception as exc:  # broad-except-ok: per-shard failure isolation
        return src_name, dst_name, False, str(exc)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="List candidates only.")
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--limit", type=int, default=0, help="Cap migration count (0 = unlimited).")
    args = p.parse_args()

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(DEFI_BUCKET)

    logger.info("Listing bare DeFi paths under gs://%s/raw_tick_data/by_date/", DEFI_BUCKET)
    bare_paths: list[str] = []
    t0 = time.time()
    for blob in bucket.list_blobs(prefix="raw_tick_data/by_date/"):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        if _is_bare_defi_path(name):
            bare_paths.append(name)
            if args.limit and len(bare_paths) >= args.limit:
                break
    logger.info("Found %d bare DeFi parquets in %.1fs", len(bare_paths), time.time() - t0)

    if not bare_paths:
        logger.info("Nothing to migrate. Exiting.")
        return 0

    if args.dry_run:
        logger.info("DRY RUN — first 10 candidates:")
        for s in bare_paths[:10]:
            logger.info("  %s\n    -> %s", s, _canonical_path(s))
        return 0

    logger.info("Migrating %d objects with %d workers...", len(bare_paths), args.workers)
    ok = 0
    failed = 0
    completed = 0
    t1 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_migrate_one, bucket, bucket, s) for s in bare_paths]
        for fut in as_completed(futs):
            src, _dst, success, err = fut.result()
            completed += 1
            if success:
                ok += 1
            else:
                failed += 1
                logger.warning("migrate failed for %s: %s", src, err)
            if completed % 100 == 0:
                rate = completed / max(0.01, time.time() - t1)
                logger.info(
                    "  %d/%d migrated (%.1f/sec, %.1fs remaining)",
                    completed,
                    len(bare_paths),
                    rate,
                    (len(bare_paths) - completed) / max(0.01, rate),
                )

    logger.info("Done. ok=%d failed=%d", ok, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
