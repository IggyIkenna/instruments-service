#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + tradfi below-floor `expected_unattempted`
#   count verified at 0 (no residual pre-floor cells left in `todo` state)
"""reclassify_tradfi_below_floor_expected_unattempted_2026_07_27.py

Reclassify EXISTING tradfi manifest rows that sit below their venue's Databento
discovery floor but are still counted as `expected_unattempted` ("todo").

ROOT CAUSE (issues/tradfi_todo_cells_below_vendor_discovery_floor_2026_07_20.md):
182,407 tradfi cells (NASDAQ/NYSE ohlcv_1m pre-2023-04-15, CME ohlcv_1m
pre-2020-01-01) were materialised as `expected_unattempted` before the
enumerator learned the discovery floor (instruments-service@31cf3952 fixed
that for NEW cells going forward — see that todo). This script is the
writer-side corrective pass for the cells that already existed.

FIX: for each row where
  ``asset_group == "tradfi" AND capture_status == "expected_unattempted"
  AND error_reason == "" (blank) AND date < VenueMapping().get_instrument_discovery_start(venue)``
flip to ``capture_status = "empty_confirmed"``, ``error_reason =
"EXPECTED_PRE_SOURCE_COVERAGE_START"`` — the SAME reason
``_enumerate_v2_tradfi`` now emits for new cells, so no new UAC reason was
needed.

IDEMPOTENT: only rows with a BLANK error_reason are matched; already-flipped
rows carry a reason and are skipped on any re-run. Rows with any other
capture_status (`captured`, `attempted_failed`, other `empty_confirmed`
reasons) are never touched.

SINGLE-WALK: one parquet read of the already-consolidated availability index
(no new whole-corpus GCS object walk) -> classify pass -> one write-back.

Consolidator-pause safety (mirrors sports_manifest_remediation_safety.py's
``assert_consolidator_paused``, inlined here via UTL primitives directly --
instruments-service does not depend on market-tick-data-service (tier
architecture), so the sports wrapper module cannot be imported cross-service):
``--apply`` refuses unless the bucket's consolidator Cloud Scheduler job is
genuinely PAUSED and no consolidation cycle is in flight, so this script's
own read-modify-write of the consolidated index can never race a concurrent
consolidator merge.

Usage::

    # Scan-only (default) -- logs counts + writes audit CSV, no manifest write.
    GCP_PROJECT_ID=central-element-323112 \\
    .venv/bin/python scripts/reclassify_tradfi_below_floor_expected_unattempted_2026_07_27.py

    # Apply (after: gcloud scheduler jobs pause <consolidator-job> --location asia-northeast1)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=tradfi-below-floor-reclass-$(date +%s) \\
    GCP_PROJECT_ID=central-element-323112 \\
    .venv/bin/python scripts/reclassify_tradfi_below_floor_expected_unattempted_2026_07_27.py --apply
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import gcsfs
import pandas as pd
from unified_api_contracts.registry.venue_mapping import VenueMapping
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MANIFEST_BLOB = "_index/availability_index.parquet"
_TARGET_REASON = "EXPECTED_PRE_SOURCE_COVERAGE_START"


class ConsolidatorNotPausedError(RuntimeError):
    """Raised when the consolidator pre-flight check fails ahead of --apply."""


def _assert_consolidator_paused(bucket: str) -> None:
    """Fail-closed pre-flight: refuse to write unless the bucket's manifest
    consolidator Cloud Scheduler job is PAUSED and no cycle is in flight.

    Composes the same UTL primitives ``sports_manifest_remediation_safety.
    assert_consolidator_paused`` uses (verified via STATE, never idleness-
    observation) -- inlined rather than imported because instruments-service
    has no dependency on market-tick-data-service (tier architecture: T4
    services depend only on UTL/UAC).
    """
    from unified_trading_library import get_storage_client
    from unified_trading_library.manifest_consolidator import consolidator_cycle_in_flight  # noqa: qg-deep-import
    from unified_trading_library.monitors.consolidator_liveness import (  # noqa: qg-deep-import
        _scheduler_job_name_for_bucket,  # noqa: reportPrivateUsage
        _scheduler_job_state,  # noqa: reportPrivateUsage
    )

    job_name = _scheduler_job_name_for_bucket(bucket)
    if job_name is None:
        raise ConsolidatorNotPausedError(
            f"cannot resolve the consolidator scheduler job for bucket={bucket!r} -- refusing to "
            "proceed without a positive PAUSED confirmation"
        )
    state = _scheduler_job_state(job_name)
    if state != "PAUSED":
        raise ConsolidatorNotPausedError(
            f"consolidator scheduler job {job_name!r} for bucket={bucket!r} is {state!r}, not PAUSED "
            f"-- pause it first (gcloud scheduler jobs pause {job_name} --location asia-northeast1) "
            "and re-run this check before any remediation write"
        )
    client = get_storage_client()
    if consolidator_cycle_in_flight(client, bucket):
        raise ConsolidatorNotPausedError(
            f"consolidator scheduler job {job_name!r} is PAUSED but a cycle is still IN FLIGHT for "
            f"bucket={bucket!r} -- wait for the in-flight cycle to settle before writing"
        )
    logger.info("_assert_consolidator_paused: %s confirmed PAUSED + idle (job=%s)", bucket, job_name)


def _build_flip_mask(df: pd.DataFrame, floors: dict[str, str | None]) -> pd.Series:
    """Boolean mask: tradfi + expected_unattempted + blank reason + date < venue floor."""
    ag = df["asset_group"].astype("string").fillna("")
    cs = df["capture_status"].astype("string").fillna("")
    reason = (
        df["error_reason"].astype("string").fillna("")
        if "error_reason" in df.columns
        else pd.Series("", index=df.index)
    )
    venue = df["venue"].astype("string").fillna("")
    date_str = df["date"].astype("string").str.slice(0, 10)

    candidate = (ag == "tradfi") & (cs == "expected_unattempted") & (reason == "")
    floor_series = venue.map(floors)
    below_floor = candidate & floor_series.notna() & (date_str < floor_series)
    return below_floor.fillna(False)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write flipped manifest back. Default: dry-run.")
    p.add_argument("--max-flips", type=int, default=300_000, help="Halt safety cap (default 300k).")
    args = p.parse_args()

    if not os.environ.get("GCP_PROJECT_ID"):
        logger.error("GCP_PROJECT_ID must be set. Refusing -- would resolve the wrong bucket.")
        return 1

    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not os.environ.get("VM_NAME")):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> per the manifest "
            "concurrency principle. Refusing to mutate without shard isolation."
        )
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="tradfi")

    if args.apply:
        _assert_consolidator_paused(bucket)

    fs = gcsfs.GCSFileSystem()
    logger.info("Loading tradfi manifest from gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
    logger.info("Manifest rows: %d", len(df))

    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")

    vm = VenueMapping()
    venues = df["venue"].astype("string").fillna("").unique().tolist()
    floors: dict[str, str | None] = {v: vm.get_instrument_discovery_start(v) for v in venues}
    logger.info(
        "Resolved discovery floors for %d venues (non-null: %d)", len(floors), sum(1 for f in floors.values() if f)
    )

    mask = _build_flip_mask(df, floors)
    n_to_flip = int(mask.sum())

    ag = df["asset_group"].astype("string").fillna("")
    cs = df["capture_status"].astype("string").fillna("")
    reason_col = df["error_reason"].astype("string").fillna("")
    n_todo_total = int(((ag == "tradfi") & (cs == "expected_unattempted") & (reason_col == "")).sum())

    logger.info("=" * 60)
    logger.info("Total tradfi expected_unattempted (blank reason, 'todo'): %d", n_todo_total)
    logger.info("Below-floor (will flip to empty_confirmed):               %d", n_to_flip)
    logger.info("=" * 60)

    if n_to_flip == 0:
        logger.info("Nothing to flip -- manifest already clean.")
        return 0

    if n_to_flip > args.max_flips:
        logger.error(
            "n_to_flip=%d exceeds --max-flips=%d halt safety. Investigate before lifting the cap.",
            n_to_flip,
            args.max_flips,
        )
        return 2

    flip_df = df.loc[mask]
    by_venue_dt = flip_df.groupby(["venue", "data_type"], dropna=False).size()
    logger.info("Flip distribution by (venue, data_type):\n%s", by_venue_dt.to_string())
    logger.info("=" * 60)

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = Path(tempfile.gettempdir()) / f"reclassify-tradfi-below-floor-{ts}.csv"
    audit_cols = ["date", "venue", "data_type", "capture_status", "error_reason", "attempted_at"]
    audit_existing = [c for c in audit_cols if c in flip_df.columns]
    flip_df[audit_existing].to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info("CSV audit written to %s (%d rows)", csv_path, n_to_flip)

    if not args.apply:
        logger.info("DRY RUN -- manifest not modified. Re-run with --apply to flip.")
        return 0

    now_iso = datetime.now(UTC).isoformat()

    stamp = datetime.now(UTC).strftime("%Y%m%d")
    snap = f"{bucket}/_index/snapshots/pre_tradfi_below_floor_reclassify_{stamp}.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot -> gs://%s", snap)

    df.loc[mask, "capture_status"] = "empty_confirmed"
    df.loc[mask, "error_reason"] = _TARGET_REASON
    if "attempted_at" in df.columns:
        df.loc[mask, "attempted_at"] = now_iso

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("Uploading flipped manifest (%d rows total, %d flipped)", len(df), n_to_flip)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(out.read())
    logger.info("Done. CSV audit at %s", csv_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
