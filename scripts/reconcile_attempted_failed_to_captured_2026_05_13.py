"""Reverse-phantom reconciler — flip ``attempted_failed`` back to ``captured`` when parquet actually exists.

Background
==========

Sister to ``reconcile_phantom_manifest_rows_all.py`` (which detects the
forward case: manifest says ``captured`` but no parquet exists → flip to
``attempted_failed``). This handles the reverse case discovered 2026-05-13
slot 3 audit:

- HYPERLIQUID 30,658 rows in ``attempted_failed/LegacyBlankErrorReasonError``
  — sampled 5/5 ✅ have real parquet data at the canonical GCS path
- LIGHTER-ZKSYNC + PACIFICA-SOLANA: similar mis-flip pattern (15/15 GCS
  spot-check blobs found across 3 recent dates)

These rows were wrongly classified by ``reconcile_legacy_blank_to_typed_reason.py``
when the legacy ``empty_confirmed`` rows were re-classified to
``attempted_failed`` via the SOURCE_RETURNED_ZERO → LegacyBlank wrapper.
The wrapper's assumption ("attempted_failed forces orchestrator re-fetch")
was correct in spirit but the rows actually have data on disk — they don't
need a fetch, they need a corrected status label.

What this does
==============

For each ``attempted_failed`` row in the cefi/defi/tradfi manifest, the
script does a bulk GCS prefix listing per ``(date, venue, [chain])`` tuple
(same one-list-per-prefix optimization as the phantom reconciler) to
determine whether parquet exists at the canonical path. If yes → flip the
row to ``captured`` (no data was lost — only mis-labelled).

Idempotent. Default scan-only; pass ``--apply-flips`` to commit changes.
Per-VM shard isolation enforced (CLAUDE.md HARD RULE). Halt-safety cap
``--max-flips-per-run`` default 100k.

Usage
=====

    cd instruments-service
    VM_NAME=ikenna-slot3-reverse-phantom MANIFEST_PER_VM_SHARDS=true \
      ./.venv/bin/python scripts/reconcile_attempted_failed_to_captured_2026_05_13.py \
        --asset-group cefi --apply-flips --max-flips-per-run 1000000

References
==========

* Plan: ``plans/active/issues/emerging_perp_venue_adapters_broken_2026_05_13.md``
* Sister script: ``instruments-service/scripts/reconcile_phantom_manifest_rows_all.py``
* Pattern: writegate Phase 3.D bulk-listing audit strategy.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage

logger = logging.getLogger("reconcile_attempted_failed_to_captured")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

PROJECT_ID = "central-element-323112"
BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
}


def _log_event(event: str, **kwargs: object) -> None:
    payload = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        "reconciler": "reconcile_attempted_failed_to_captured",
        **kwargs,
    }
    logger.info("EVENT %s", payload)


def _prefix_for(asset_group: str, date_str: str, venue: str, chain: str) -> tuple[str, ...]:
    """Return candidate GCS prefixes (canonical + legacy hive-vocab fallbacks)."""
    bases = [
        f"raw_tick_data/by_date/day={date_str}/asset_group={asset_group}/venue={venue}/",
        f"raw_tick_data/by_date/day={date_str}/category={asset_group}/venue={venue}/",
    ]
    if chain:
        bases.extend([
            f"raw_tick_data/by_date/day={date_str}/asset_group={asset_group}/venue={venue}/chain={chain}/",
            f"raw_tick_data/by_date/day={date_str}/category={asset_group}/venue={venue}/chain={chain}/",
        ])
    return tuple(bases)


def _bulk_prefix_check(
    bucket: storage.Bucket,
    prefixes: list[tuple[str, str, str]],  # (date, venue, chain) tuples
    asset_group: str,
    workers: int = 32,
) -> dict[tuple[str, str, str], bool]:
    """Return a dict mapping (date, venue, chain) → True if any blob exists under any candidate prefix."""
    result: dict[tuple[str, str, str], bool] = {}
    seen: dict[tuple[str, str, str], None] = {}  # ordered dedupe

    def _check_one(key: tuple[str, str, str]) -> tuple[tuple[str, str, str], bool]:
        date_str, venue, chain = key
        for prefix in _prefix_for(asset_group, date_str, venue, chain):
            try:
                blobs = list(bucket.client.list_blobs(
                    bucket, prefix=prefix, max_results=1, timeout=30
                ))
                if blobs:
                    return key, True
            except Exception as exc:  # noqa: BLE001 — defensive against transient GCS errors
                logger.debug("list error for %s: %s", prefix, exc)
                continue
        return key, False

    # Dedupe prefixes
    for p in prefixes:
        seen[p] = None
    unique = list(seen.keys())
    logger.info("Bulk-checking %d unique (date, venue, chain) prefixes with %d workers", len(unique), workers)

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_check_one, k): k for k in unique}
        for fut in as_completed(futures):
            key, exists = fut.result()
            result[key] = exists
            completed += 1
            if completed % 500 == 0:
                logger.info("  %d/%d prefixes checked (%.1f%% present)",
                            completed, len(unique), 100.0 * sum(result.values()) / len(result))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reverse-phantom reconciler — flip attempted_failed back to captured "
            "for rows whose canonical parquet actually exists on disk."
        )
    )
    parser.add_argument("--asset-group", choices=["cefi", "defi", "tradfi"], required=True)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--apply-flips", action="store_true")
    parser.add_argument("--max-flips-per-run", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--venues", default=None, help="Comma-separated venue filter")
    parser.add_argument("--report-dir", default=tempfile.gettempdir())
    args = parser.parse_args()

    if args.apply_flips and os.environ.get("MANIFEST_PER_VM_SHARDS") != "true":
        logger.error("--apply-flips requires MANIFEST_PER_VM_SHARDS=true. Aborting.")
        _log_event("RECONCILER_FAILED", reason="missing_per_vm_shards_env")
        return 1
    vm_name = os.environ.get("VM_NAME")
    if args.apply_flips and not vm_name:
        logger.error("--apply-flips requires VM_NAME. Aborting.")
        _log_event("RECONCILER_FAILED", reason="missing_vm_name")
        return 1

    bucket_name = args.bucket or BUCKETS[args.asset_group]
    run_id = (
        f"recon-att-failed-to-captured-{args.asset_group}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    )
    _log_event(
        "RECONCILER_STARTED",
        asset_group=args.asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        run_id=run_id,
    )

    logger.info("Loading manifest from gs://%s/_index/availability_index.parquet", bucket_name)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("_index/availability_index.parquet")
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    logger.info("Manifest rows: %d", len(df))

    status = df["capture_status"].astype(str).str.strip()
    mask = status == "attempted_failed"
    if args.venues:
        venues = {v.strip().upper() for v in args.venues.split(",")}
        mask = mask & df["venue"].astype(str).str.upper().isin(venues)
    n_candidates = int(mask.sum())
    logger.info(
        "Candidate attempted_failed rows: %d / %d (%.2f%%)",
        n_candidates, len(df), 100.0 * n_candidates / max(len(df), 1),
    )
    if n_candidates == 0:
        logger.info("No candidates.")
        _log_event("RECONCILER_COMPLETED", asset_group=args.asset_group, candidates=0, flipped=0)
        return 0

    candidate_idx = df.index[mask]
    # Build (date, venue, chain) tuples
    prefixes: list[tuple[str, str, str]] = []
    for idx in candidate_idx:
        row = df.loc[idx]
        date_str = str(row.get("date", ""))[:10]
        venue = str(row.get("venue", ""))
        chain = str(row.get("chain", "") or "")
        if date_str and venue:
            prefixes.append((date_str, venue, chain))

    existence = _bulk_prefix_check(bucket, prefixes, args.asset_group, workers=args.workers)
    n_prefixes_with_data = sum(existence.values())
    logger.info(
        "Prefixes with data: %d / %d (%.2f%%)",
        n_prefixes_with_data, len(existence), 100.0 * n_prefixes_with_data / max(len(existence), 1),
    )

    flips: list[dict[str, object]] = []
    for idx in candidate_idx:
        row = df.loc[idx]
        date_str = str(row.get("date", ""))[:10]
        venue = str(row.get("venue", ""))
        chain = str(row.get("chain", "") or "")
        key = (date_str, venue, chain)
        if not existence.get(key, False):
            continue
        flips.append({
            "row_index": int(idx),
            "date": date_str,
            "venue": venue,
            "chain": chain,
            "data_type": str(row.get("data_type", "")),
            "instrument_id": str(row.get("instrument_id", ""))[:50],
            "old_status": "attempted_failed",
            "new_status": "captured",
        })

    n_flips = len(flips)
    logger.info("Flips proposed: %d (attempted_failed → captured)", n_flips)
    if flips:
        report = f"{args.report_dir.rstrip('/')}/{run_id}.csv"
        pd.DataFrame(flips).to_csv(report, index=False)
        logger.info("Report: %s", report)
    _log_event(
        "RECONCILER_PROGRESS",
        asset_group=args.asset_group, candidates=n_candidates, flips=n_flips,
    )
    if n_flips == 0:
        _log_event("RECONCILER_COMPLETED", asset_group=args.asset_group, candidates=n_candidates, flipped=0)
        return 0
    if n_flips > args.max_flips_per_run:
        logger.error("%d flips > cap %d; aborting.", n_flips, args.max_flips_per_run)
        _log_event("RECONCILER_FAILED", reason="max_flips_exceeded", detected=n_flips, cap=args.max_flips_per_run)
        return 1
    if not args.apply_flips:
        logger.info("Dry-run complete. Pass --apply-flips to commit.")
        _log_event("RECONCILER_COMPLETED", asset_group=args.asset_group, candidates=n_candidates, flipped=0, dry_run=True)
        return 0

    # Apply
    started = datetime.now(timezone.utc)
    for f in flips:
        idx = f["row_index"]
        df.at[idx, "capture_status"] = "captured"
        # Clear error_reason on flip-to-captured
        if "error_reason" in df.columns:
            df.at[idx, "error_reason"] = None
        # Clear attempted_at — the row is no longer "attempted-but-failed"
        if "attempted_at" in df.columns:
            df.at[idx, "attempted_at"] = None
    subset = df.loc[[f["row_index"] for f in flips]].copy()
    per_vm_blob_path = f"_index/per_vm/{vm_name}.parquet"
    buf = io.BytesIO()
    subset.to_parquet(buf, index=False)
    bucket.blob(per_vm_blob_path).upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info("Uploaded per-VM shard: gs://%s/%s (%d rows) in %.1fs",
                bucket_name, per_vm_blob_path, n_flips, elapsed)
    _log_event(
        "RECONCILER_COMPLETED",
        asset_group=args.asset_group, candidates=n_candidates, flipped=n_flips,
        per_vm_blob=per_vm_blob_path, run_id=run_id, elapsed_s=round(elapsed, 1),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
