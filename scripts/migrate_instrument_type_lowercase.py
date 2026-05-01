#!/usr/bin/env python3
"""Lowercase ``instrument_type=`` GCS hive segments where uppercase exists.

Per CLAUDE.md SSOT, on-disk paths use lowercase instrument_type
(``instrument_type=perpetual``, not ``instrument_type=PERPETUAL``).  A
small subset of writers from a 2026-04-era schema-v6 push left
uppercase segments behind, which causes phantom-row false positives in
manifest audits and breaks per-instrument readers that case-match.

This script:

  1. Lists every parquet whose path contains ``instrument_type={UPPER}/``
     (anything with at least one uppercase letter in the segment value).
  2. Computes the canonical destination path with the segment lowercased.
  3. Server-side copy old → new.
  4. Delete old object after successful copy.

Idempotent and concurrent-safe.

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_instrument_type_lowercase.py \\
        --bucket market-data-tick-cefi-central-element-323112 --dry-run
    .venv/bin/python scripts/migrate_instrument_type_lowercase.py \\
        --bucket market-data-tick-cefi-central-element-323112 --workers 32
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

# Match instrument_type=<value>/ where <value> contains at least one uppercase letter.
_UPPER_INSTR_TYPE_RE = re.compile(r"instrument_type=([^/]*?[A-Z][^/]*)/")


def _has_uppercase_instrument_type(name: str) -> bool:
    return bool(_UPPER_INSTR_TYPE_RE.search(name))


def _lowercased_path(name: str) -> str:
    return _UPPER_INSTR_TYPE_RE.sub(
        lambda m: f"instrument_type={m.group(1).lower()}/",
        name,
    )


def _migrate_one(
    bucket: storage.Bucket,
    src_name: str,
) -> tuple[str, str, bool, str]:
    dst_name = _lowercased_path(src_name)
    if dst_name == src_name:
        return src_name, dst_name, True, ""
    try:
        src_blob = bucket.blob(src_name)
        bucket.copy_blob(src_blob, bucket, new_name=dst_name)
        src_blob.delete()
        return src_name, dst_name, True, ""
    except Exception as exc:  # broad-except-ok: per-shard failure isolation
        return src_name, dst_name, False, str(exc)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True, help="GCS bucket name")
    p.add_argument("--prefix", default="raw_tick_data/by_date/")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--limit", type=int, default=0)
    args = p.parse_args()

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(args.bucket)

    logger.info("Listing uppercase-instrument_type paths under gs://%s/%s", args.bucket, args.prefix)
    candidates: list[str] = []
    t0 = time.time()
    for blob in bucket.list_blobs(prefix=args.prefix):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        if _has_uppercase_instrument_type(name):
            candidates.append(name)
            if args.limit and len(candidates) >= args.limit:
                break
    logger.info("Found %d candidates in %.1fs", len(candidates), time.time() - t0)

    if not candidates:
        logger.info("Nothing to migrate. Exiting.")
        return 0

    if args.dry_run:
        logger.info("DRY RUN — first 10 candidates:")
        for s in candidates[:10]:
            logger.info("  %s\n    -> %s", s, _lowercased_path(s))
        return 0

    logger.info("Migrating %d objects with %d workers...", len(candidates), args.workers)
    ok = 0
    failed = 0
    completed = 0
    t1 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_migrate_one, bucket, s) for s in candidates]
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
                    "  %d/%d migrated (%.1f/sec)",
                    completed,
                    len(candidates),
                    rate,
                )

    logger.info("Done. ok=%d failed=%d", ok, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
