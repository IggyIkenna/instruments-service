#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after gs://market-data-tick-defi-prd-central-element-323112/_index/
#   availability_index.parquet is confirmed durably deduplicated AND the root-cause
#   race (consolidator non-atomic shard delete-before-merge, see the 2026-07-10
#   incident write-up) is fixed so it stops recurring.
"""One-off: remove genuine (identical-key AND identical-capture_status) duplicate
rows from the DeFi manifest.

Real incident (2026-07-10): a live spot-check of ALCHEMY/ARBITRUM/gas_fees/
2018-01-01 found the SAME empty_confirmed row written twice, 2.5 weeks apart
(enum-reseed-defi-gas-20260622-113817 and enum-universe-defi-20260710-130231) —
pure denominator-inflating duplication, zero new information. A full scan found
4,630,138 rows in duplicate-key groups; of those, 1,789,793 groups have IDENTICAL
capture_status across every copy (genuine accidental duplication — this script's
target). The other 525,276 groups have DIFFERING capture_status (legitimate
state-transition history, e.g. expected_unattempted -> captured) and are left
untouched.

Root-cause hypothesis (not fixed by this script, see the tracked issue): the
per-VM-shard -> main-index consolidator appears to delete a shard before (or
without atomically) completing its merge into the main index, so a run landing
in that window sees neither the shard nor the merged row and re-enumerates
already-covered honest-absence cells. Duplicate run_ids span 2026-05-07 through
2026-07-10 (~2 months), consistent with the recurring DAILY expected-universe-v2
Cloud Scheduler job repeatedly hitting the same race.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python scripts/defi_manifest_dedup_2026_07_10.py --dry-run
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python scripts/defi_manifest_dedup_2026_07_10.py --apply

SSOT: unified-trading-pm/plans/active/issues/defi_expected_unattempted_backlog_1m_2026_07_03.md
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_KEY_COLS = ["asset_group", "venue", "chain", "data_type", "instrument_type", "instrument_id", "date"]
_MANIFEST_PATH = "_index/availability_index.parquet"
_QUARANTINE_PREFIX = "_migration_backup/defi_manifest_dedup_2026_07_10/"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write the deduplicated manifest (default: report only).")
    args = ap.parse_args(argv)

    bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")
    manifest_uri = f"gs://{bucket}/{_MANIFEST_PATH}"
    logger.info("bucket=%s manifest=%s", bucket, manifest_uri)

    df = pd.read_parquet(manifest_uri)
    logger.info("total manifest rows: %d", len(df))
    key_cols = [c for c in _KEY_COLS if c in df.columns]

    defi_mask = df["asset_group"] == "defi"
    other = df[~defi_mask].copy()
    defi = df[defi_mask].copy()
    logger.info("defi rows: %d, other-AG rows (untouched): %d", len(defi), len(other))

    # A genuine duplicate = identical key AND identical capture_status. Keep the
    # LATEST written_at copy per (key, capture_status) group; drop the rest.
    dedup_cols = [*key_cols, "capture_status"]
    before = len(defi)
    defi_sorted = defi.sort_values("written_at") if "written_at" in defi.columns else defi
    defi_deduped = defi_sorted.drop_duplicates(subset=dedup_cols, keep="last")
    removed = before - len(defi_deduped)
    logger.info("defi rows before=%d after=%d removed=%d genuine duplicates", before, len(defi_deduped), removed)

    # Safety: row count for (key)-DIFFERING-status groups must be unchanged (we
    # only ever collapse identical (key, capture_status) groups, never touch
    # legitimate state-transition rows).
    key_groups_before = defi.groupby(key_cols)["capture_status"].nunique()
    multi_status_keys = int((key_groups_before > 1).sum())
    key_groups_after = defi_deduped.groupby(key_cols)["capture_status"].nunique()
    multi_status_keys_after = int((key_groups_after > 1).sum())
    if multi_status_keys != multi_status_keys_after:
        logger.error(
            "SAFETY CHECK FAILED: multi-status key-group count changed (%d -> %d) — "
            "legitimate state-transition rows may have been touched. Aborting.",
            multi_status_keys,
            multi_status_keys_after,
        )
        return 1
    logger.info("safety check OK: multi-status (legitimate transition) groups unchanged at %d", multi_status_keys)

    if not args.apply:
        logger.info("DRY-RUN: would remove %d genuine duplicate rows. Pass --apply to write.", removed)
        return 0

    if removed == 0:
        logger.info("Nothing to remove.")
        return 0

    st = get_storage_client()
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_dest = f"{_QUARANTINE_PREFIX}availability_index_pre_dedup_{ts}.parquet"
    logger.info("Backing up pre-dedup manifest to gs://%s/%s", bucket, backup_dest)
    raw = st.download_bytes(bucket, _MANIFEST_PATH)
    st.upload_bytes(bucket, backup_dest, raw)
    verify_raw = st.download_bytes(bucket, backup_dest)
    if len(verify_raw) != len(raw):
        logger.error("Backup verify failed (size mismatch) — aborting before touching the live manifest.")
        return 1
    logger.info("Backup verified (%d bytes).", len(raw))

    out = pd.concat([other, defi_deduped], ignore_index=True)
    logger.info("writing deduplicated manifest: %d rows (was %d, removed %d)", len(out), len(df), len(df) - len(out))
    import io

    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    st.upload_bytes(bucket, _MANIFEST_PATH, buf.getvalue())
    logger.info("DONE. Removed %d genuine duplicate rows. Backup: gs://%s/%s", removed, bucket, backup_dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
