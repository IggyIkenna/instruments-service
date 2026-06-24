#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 duplicate cell-keys with conflicting capture_status + honest_cov re-measured
"""Collapse duplicate cell-keys in the DeFi manifest _index by capture-status priority.

The canonical ``_index`` carries DUPLICATE rows for the same shard cell-key
(``data_type, venue, chain, instrument_id, date``) with CONFLICTING capture_status —
measured 97,575 cells with BOTH ``captured`` AND ``expected_unattempted`` (the backfill
capture landed as a new row but the stale EU row was never superseded), plus
captured+empty / failed+captured combos. The manifest consolidator's last-write-wins
dedup is lagging/not collapsing them, so honest_cov under-counts: a captured cell still
shows an EU twin.

This collapses each duplicate cell-key to its SINGLE best row by status priority
(captured > empty_confirmed > attempted_failed > expected_unattempted). captured ALWAYS
wins (never dropped). Vectorised, backup-then-write, idempotent (a deduped index is
returned unchanged). Mirrors the reconcile safety pattern.

Usage:
  python scripts/dedup_defi_manifest_status_priority_2026_06_24.py --dry-run
  python scripts/dedup_defi_manifest_status_priority_2026_06_24.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_CELL_KEY = ["data_type", "venue", "chain", "instrument_id", "date"]
# Lower number = higher priority (kept). captured ALWAYS wins; EU is the lowest (droppable).
_STATUS_PRIORITY = {
    "captured": 0,
    "empty_confirmed": 1,
    "attempted_failed": 2,
    "expected_unattempted": 3,
}


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.statusdedup.bak.parquet"
    return f"{blob_name}.{run_ts}.statusdedup.bak"


def dedup_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Keep the single highest-priority-status row per cell-key. Pure + idempotent.

    Returns ``(deduped_df, stats)``. Rows whose cell-key is unique are untouched.
    """
    stats = {"dropped_eu_superseded": 0, "dropped_other_dup": 0, "kept": 0}
    if df.empty or "capture_status" not in df.columns or not all(c in df.columns for c in _CELL_KEY):
        stats["kept"] = len(df)
        return df, stats

    work = df.copy()
    work["_prio"] = work["capture_status"].astype(str).map(_STATUS_PRIORITY).fillna(9).astype(int)
    work["_orig"] = range(len(work))
    # Stable sort by priority within each cell-key; keep="first" → the best status survives.
    work = work.sort_values(["_prio", "_orig"], kind="stable")
    dup_mask = work.duplicated(subset=_CELL_KEY, keep="first")
    dropped = work.loc[dup_mask]
    stats["dropped_eu_superseded"] = int((dropped["capture_status"].astype(str) == "expected_unattempted").sum())
    stats["dropped_other_dup"] = int(len(dropped) - stats["dropped_eu_superseded"])
    kept = work.loc[~dup_mask].sort_values("_orig", kind="stable").drop(columns=["_prio", "_orig"])
    stats["kept"] = len(kept)
    # SAFETY: never drop a captured row — assert captured count preserved.
    cap_before = int((df["capture_status"].astype(str) == "captured").sum())
    cap_after = int((kept["capture_status"].astype(str) == "captured").sum())
    if cap_after < cap_before:
        raise RuntimeError(f"dedup would drop captured rows ({cap_before} → {cap_after}) — ABORT, never lose a capture")
    return kept.reset_index(drop=True), stats


def _process_blob(storage: object, bucket: str, blob_name: str, run_ts: str, *, apply: bool) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    deduped, stats = dedup_frame(df)
    dropped = stats["dropped_eu_superseded"] + stats["dropped_other_dup"]
    if dropped == 0:
        return stats
    logger.info(
        "  %s: dropped_eu_superseded=%d dropped_other_dup=%d (rows %d → %d)",
        blob_name,
        stats["dropped_eu_superseded"],
        stats["dropped_other_dup"],
        len(df),
        len(deduped),
    )
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAttributeAccessIssue]
        out = io.BytesIO()
        deduped.to_parquet(out, index=False, engine="pyarrow")
        out.seek(0)
        storage.upload_bytes(bucket, blob_name, out.read())  # pyright: ignore[reportAttributeAccessIssue]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(deduped))
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply = bool(args.apply)

    bucket = _defi_bucket()
    storage = get_storage_client(project_id=None)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    logger.info("Mode=%s; %d manifest parquet(s) to scan", "APPLY" if apply else "DRY-RUN", len(blobs))

    totals = {"dropped_eu_superseded": 0, "dropped_other_dup": 0}
    for blob_name in blobs:
        s = _process_blob(storage, bucket, blob_name, run_ts, apply=apply)
        for k in totals:
            totals[k] += s.get(k, 0)

    logger.info(
        "TOTAL: dropped_eu_superseded=%d dropped_other_dup=%d (mode=%s)",
        totals["dropped_eu_superseded"],
        totals["dropped_other_dup"],
        "APPLY" if apply else "DRY-RUN",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
