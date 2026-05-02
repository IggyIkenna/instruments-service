#!/usr/bin/env python3
"""Relocate rogue bucket-root ``day=*/...`` parquets under the canonical
``raw_tick_data/by_date/day=*/...`` prefix.

Background (2026-05-02):

UAC ``build_defi_partition_path`` (and CeFi/TradFi/Prediction siblings) used
to return paths starting with ``day=...`` and the docstring said "the writer
prepends raw_tick_data/by_date/". The MTDS orchestrator's
``PartitionedTickWriter`` did add that prefix; but the DeFi handler family
(eigenlayer_rewards, mev_events, dex_swaps, evm_defi, liquidation_events,
gas_fee, staking_yields, bridge_events, flash_loan_events, token_transfers)
called ``write_defi_rows`` and uploaded the bare path directly via
``storage.upload_bytes(bucket, canonical_path, …)``.  That dropped every DeFi
event parquet at the bucket ROOT under ``day=*/category=defi/…``.

Result on the manifest: ``capture_status=captured`` rows for these shards
showed up as 100% phantoms in the audit because the audit (and the deployment
UI) probed the canonical ``raw_tick_data/by_date/day=*/asset_group=defi/…``
prefix.  The data IS captured — just at the wrong place.

UAC has now been corrected (``build_*_partition_path`` returns the FULL
bucket-relative path including ``raw_tick_data/by_date/``).  This one-off
script relocates the existing rogue parquets server-side so the audit and
readers find them.

Idempotent.  Server-side copy + delete (no client egress).  Safe to run
while writers are active because the corrected writers emit to the canonical
prefix directly.

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_rogue_root_to_raw_tick_data.py \\
        --bucket market-data-tick-defi-central-element-323112 --dry-run
    .venv/bin/python scripts/migrate_rogue_root_to_raw_tick_data.py \\
        --bucket market-data-tick-defi-central-element-323112 --workers 32
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

# Match bucket-root ``day=YYYY-MM-DD/...`` paths — the rogue layout — so we
# can probe and relocate them.  The canonical path is the same key with
# ``raw_tick_data/by_date/`` prepended.  Anything already at the canonical
# prefix is left alone.
_ROGUE_DAY_RE = re.compile(r"^day=\d{4}-\d{2}-\d{2}/")


def _is_rogue_root_path(name: str) -> bool:
    return bool(_ROGUE_DAY_RE.match(name))


def _canonical_path(rogue: str) -> str:
    return f"raw_tick_data/by_date/{rogue}"


def _migrate_one(
    bucket: storage.Bucket,
    src_name: str,
) -> tuple[str, str, bool, str]:
    dst_name = _canonical_path(src_name)
    try:
        src_blob = bucket.blob(src_name)
        bucket.copy_blob(src_blob, bucket, new_name=dst_name)
        src_blob.delete()
        return src_name, dst_name, True, ""
    except Exception as exc:  # broad-except-ok: per-shard failure isolation
        return src_name, dst_name, False, str(exc)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket to relocate within (e.g. market-data-tick-defi-{pid}).",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--limit", type=int, default=0, help="Cap migration (0 = unlimited).")
    args = p.parse_args()

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(args.bucket)

    logger.info("Listing rogue root-level day=*/ parquets in gs://%s", args.bucket)
    candidates: list[str] = []
    t0 = time.time()
    # We only need to walk the bucket root with prefix=``day=`` — the
    # canonical raw_tick_data/by_date/... tree is ignored entirely.
    for blob in bucket.list_blobs(prefix="day="):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        if _is_rogue_root_path(name):
            candidates.append(name)
            if args.limit and len(candidates) >= args.limit:
                break
    logger.info(
        "Found %d rogue root-level parquets in %.1fs",
        len(candidates),
        time.time() - t0,
    )

    if not candidates:
        logger.info("Nothing to migrate. Exiting.")
        return 0

    if args.dry_run:
        logger.info("DRY RUN — first 10 candidates:")
        for s in candidates[:10]:
            logger.info("  %s\n    -> %s", s, _canonical_path(s))
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
                    "  %d/%d migrated (%.1f/sec, ETA %.1fs)",
                    completed,
                    len(candidates),
                    rate,
                    (len(candidates) - completed) / max(0.01, rate),
                )

    logger.info("Done. ok=%d failed=%d", ok, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
