#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: defi_dead_storage_shape_b_cleanup_candidate_2026_07_10.md's open todo
#   ("verify the restore is complete") is closed and no further re-verification is expected.
"""READ-ONLY, dry-run verification that the 2026-07-29 mistaken hive delete
(``defi_dead_storage_shape_b_cleanup_candidate_2026_07_10.md``) is fully restored.

Context (see the issue doc's 2026-07-29 entries for the full account): a worker deleted
70,570 real ``instrument_availability`` hive objects for DeFi on the stale premise that the
hive shape was frozen dead storage. It was actually the LIVE canonical shape per the
2026-07-21 R2 full-hive-canonicalisation ruling
(``instrument_availability_hive_canonicalisation_2026_07_21.md``). The delete was reverted
same-day via GCS Soft Delete (72,514 objects restored, live-version-guarded). This script
re-counts the hive shape to confirm the restore left the tree at (or above, given legitimate
new daily writes since) its pre-delete count of ~103,639 objects.

Single bounded listing, ONE bucket (``instruments-store-defi-prd``), ONE prefix
(``instrument_availability/by_date/``) — not a whole-corpus walk. Metadata-only
(``storage.list_blobs`` — name + size), never downloads content. NEVER writes, deletes, or
restores anything — a pure count-and-report dry run. No ``--apply`` flag exists because this
script performs no mutation.

Usage::

    GCP_PROJECT_ID=central-element-323112 \
      .venv/bin/python scripts/verify_defi_hive_instrument_availability_restore_2026_07_29.py
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("verify_defi_hive_restore")

_PREFIX = "instrument_availability/by_date/"
_PRE_DELETE_HIVE_TOTAL = 103_639  # from the issue doc's 2026-07-29 "root cause" entry
_RESTORED_COUNT = 70_570  # objects the mistaken delete removed and the restore put back


def _bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    storage = get_storage_client()
    bucket = _bucket()

    hive_total = 0
    flat_total = 0
    other_total = 0
    hive_days: set[str] = set()
    hive_max_day = ""

    for blob in storage.list_blobs(bucket, prefix=_PREFIX):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        if "/pipeline_mode=" in name:
            hive_total += 1
            day = name.split("day=", 1)[1].split("/", 1)[0] if "day=" in name else ""
            if day:
                hive_days.add(day)
                if day > hive_max_day:
                    hive_max_day = day
        elif name.startswith(_PREFIX) and "/venue=" in name:
            flat_total += 1
        else:
            other_total += 1

    delta = hive_total - _PRE_DELETE_HIVE_TOTAL
    restored_ok = hive_total >= _PRE_DELETE_HIVE_TOTAL

    logger.info(
        "RESULT bucket=%s prefix=%s hive_total=%d flat_total=%d other=%d hive_distinct_days=%d hive_max_day=%s",
        bucket,
        _PREFIX,
        hive_total,
        flat_total,
        other_total,
        len(hive_days),
        hive_max_day or "(none)",
    )
    logger.info(
        "COMPARE pre_delete_hive_total=%d restored_count=%d observed_hive_total=%d delta=%+d "
        "restored_to_or_above_baseline=%s",
        _PRE_DELETE_HIVE_TOTAL,
        _RESTORED_COUNT,
        hive_total,
        delta,
        restored_ok,
    )
    if restored_ok:
        logger.info(
            "VERDICT: restore CONFIRMED complete — hive_total (%d) is at/above the pre-delete "
            "baseline (%d); delta of %+d is consistent with legitimate new daily writes since "
            "2026-06-29/2026-07-29 (hive has been the sole live writer target since the "
            "2026-07-21 R2 cutover, instruments-service@a9be6ce9).",
            hive_total,
            _PRE_DELETE_HIVE_TOTAL,
            delta,
        )
    else:
        logger.error(
            "VERDICT: restore INCOMPLETE — hive_total (%d) is BELOW the pre-delete baseline "
            "(%d) by %d objects. Do not close the issue doc's delete question; escalate.",
            hive_total,
            _PRE_DELETE_HIVE_TOTAL,
            -delta,
        )
    return 0 if restored_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
