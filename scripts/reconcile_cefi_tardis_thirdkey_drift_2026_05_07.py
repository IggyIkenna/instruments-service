#!/usr/bin/env python3
"""reconcile_cefi_tardis_thirdkey_drift_2026_05_07.py — MTDS Tardis third_key drift cleanup.

Reconciler for the MTDS Tardis third_key drift bug fixed in MTDS@97206bc
(2026-05-07). The pre-fix orchestrator collapsed per-symbol counts into one
venue-aggregate ``captured`` row + emitted N per-instrument ``empty_confirmed``
sentinel rows per ``(venue, data_type)`` pair. This script flips those bogus
per-instrument ``empty_confirmed`` rows back to ``attempted_failed`` so the
next fleet run re-fetches them on FIXED code and the orchestrator's
``_should_skip_shard`` no longer permanently skips them.

Scope (STRICT — all conditions must hold):

* ``asset_group=cefi``
* ``venue ∈ {BITFINEX-SPOT, BITFINEX-FUTURES, BITGET-SPOT, BITGET-FUTURES,
  KRAKEN-SPOT, KRAKEN-FUTURES}`` — the 6 Tier3 Tardis venues only. Other
  CeFi venues (BINANCE / COINBASE / OKX / etc.) are clean per the cross-AG
  audit and out of scope.
* ``capture_status=empty_confirmed``
* ``error_reason`` is blank/null
* ``attempted_at >= 2026-05-01T00:00:00Z`` (limits scope to the broken-code
  era)
* ``instrument_id`` non-empty (per-instrument sentinel rows from the broken
  Tier3 fan-out, NOT the venue-aggregate ``instrument_id=""`` captured rows
  from Bug A — those are a separate phantom-audit follow-up).

Action: flip ``capture_status=attempted_failed``,
``error_reason="LEGACY_THIRDKEY_DRIFT_RECON_2026_05_07"``. Rows are NOT
deleted; the venue-aggregate ``captured`` rows are NOT touched (different
lane). Reason-taxonomy backfill on legitimately-empty rows is the writegate
Phase 2.E + Tier 3D.1 reconciler lane — also not this script's job.

Expected counts per the cross-AG audit: ~36,963 post-2026-05-07T13Z +
~440k pre-bug Tardis-baseline rows = **~480k total**. Dry-run output
should be in this ballpark; >2x off either way → STOP and report (scope
filter is wrong).

**Default mode is SCAN-ONLY**. ``--apply-flips`` is the explicit flag to
mutate the manifest, and requires per-VM shard isolation
(``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique-tag>``).
``--max-flips-per-run`` default 100k halt safety; lift the cap explicitly
once the first batch is reviewed.

Mirrors the pattern in:

* ``reconcile_phantom_manifest_rows_all.py`` — bulk read + upload via
  per-VM shard with the consolidator daemon merging into canonical.
* ``reconcile_expected_absence_reasons.py`` — RECONCILER_* event emission +
  CSV audit + ``--apply-flips`` env-var guards.
* ``reconcile_blank_error_reason_rows.py`` — closest precedent shape; this
  script is its narrow Tardis-scoped sister.

Example::

    cd instruments-service

    # Scan only — produces /tmp/recon-tardis-thirdkey-{ts}.csv
    python scripts/reconcile_cefi_tardis_thirdkey_drift_2026_05_07.py --dry-run

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true \\
    VM_NAME=reconcile-tardis-thirdkey-drift-$(date +%Y%m%d-%H%M%S) \\
    python scripts/reconcile_cefi_tardis_thirdkey_drift_2026_05_07.py \\
        --apply-flips --max-flips-per-run 1000000
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import logging
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
CEFI_BUCKET = f"market-data-tick-cefi-{PROJECT_ID}"
MANIFEST_BLOB = "_index/availability_index.parquet"

# 6 Tier3 Tardis venues — closed set per cross-AG audit 2026-05-07.
# All other CeFi venues (BINANCE / COINBASE / OKX / HYPERLIQUID / etc.)
# have 0 post-13Z bogus empty_confirmed rows and are out of scope.
TIER3_TARDIS_VENUES: frozenset[str] = frozenset({
    "BITFINEX-SPOT",
    "BITFINEX-FUTURES",
    "BITGET-SPOT",
    "BITGET-FUTURES",
    "KRAKEN-SPOT",
    "KRAKEN-FUTURES",
})

# Earliest attempted_at to be in scope. Pre-2026-05-01 rows pre-date the
# broken-code era under audit; leaving them untouched keeps the reconciler
# narrow and idempotent.
SCOPE_ATTEMPTED_AT_MIN = "2026-05-01T00:00:00+00:00"

# Sentinel error_reason stamped on flipped rows. Picked to be greppable +
# self-documenting so a downstream operator looking at the manifest can
# trace the row back to this reconciler.
FLIP_ERROR_REASON = "LEGACY_THIRDKEY_DRIFT_RECON_2026_05_07"


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log mirroring the RECONCILER_* shape."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Flip bogus empty_confirmed -> attempted_failed for ~480k per-instrument "
            "sentinel rows across 6 Tier3 Tardis venues introduced by MTDS orchestrator "
            "shard-key drift (fixed in MTDS@97206bc, 2026-05-07). See module docstring."
        ),
    )
    # Default scan-only — explicit --dry-run is also accepted for clarity in CLI history.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Default mode (scan-only). Print counts + sample rows + CSV audit, no "
            "manifest writes. Mutually exclusive with --apply-flips; if both are "
            "passed, --dry-run wins."
        ),
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        help=(
            "Actually flip the rows. Requires MANIFEST_PER_VM_SHARDS=true + "
            "VM_NAME=<unique-tag> per the per-VM shard isolation rule."
        ),
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=100_000,
        help=(
            "Halt-safety cap (default 100k). Aborts if scan finds more than this. "
            "For the full ~480k flip lift this explicitly to e.g. 1_000_000 only "
            "after reviewing the dry-run distribution."
        ),
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical CEFI bucket (default: per-asset-group SSOT).",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report dir (default: tempfile.gettempdir()).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=10,
        help="Number of in-scope sample rows to print in dry-run mode (default: 10).",
    )
    return parser.parse_args()


def _download_manifest(bucket_name: str) -> tuple[pd.DataFrame, str]:
    """Bulk-download the canonical CEFI manifest. Returns (df, local_path)."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix="recon-tardis-thirdkey-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_scope_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask of rows matching the strict reconciler scope.

    Scope (all must hold):
      * asset_group=cefi (the bucket already enforces this; double-check
        defensively in case of cross-bucket joins).
      * venue in TIER3_TARDIS_VENUES.
      * capture_status=empty_confirmed.
      * error_reason blank/null.
      * attempted_at >= SCOPE_ATTEMPTED_AT_MIN.
      * instrument_id non-empty.
    """
    if "capture_status" not in df.columns:
        logger.warning("Manifest has no capture_status column — nothing to reconcile.")
        return pd.Series([False] * len(df), index=df.index)

    # capture_status=empty_confirmed
    status_mask = df["capture_status"].fillna("") == "empty_confirmed"

    # venue in Tier3 Tardis set
    if "venue" not in df.columns:
        logger.warning("Manifest has no venue column — nothing to reconcile.")
        return pd.Series([False] * len(df), index=df.index)
    venue_mask = df["venue"].fillna("").astype(str).isin(TIER3_TARDIS_VENUES)

    # error_reason blank/null
    if "error_reason" in df.columns:
        blank_reason_mask = df["error_reason"].fillna("").astype(str).str.strip() == ""
    else:
        blank_reason_mask = pd.Series([True] * len(df), index=df.index)

    # asset_group defensive guard (bucket already constrains this, but cheap insurance)
    if "asset_group" in df.columns:
        ag_mask = df["asset_group"].fillna("").astype(str).str.lower() == "cefi"
    else:
        ag_mask = pd.Series([True] * len(df), index=df.index)

    # instrument_id non-empty (per-instrument sentinel rows only; venue-aggregate
    # rows have empty instrument_id and belong to a different cleanup lane).
    if "instrument_id" in df.columns:
        inst_mask = df["instrument_id"].fillna("").astype(str).str.strip() != ""
    else:
        # No instrument_id column at all → can't distinguish per-instrument from
        # venue-aggregate; refuse to flip anything to be safe.
        logger.warning("Manifest has no instrument_id column — refusing to flip (would risk venue-aggregate rows).")
        return pd.Series([False] * len(df), index=df.index)

    # attempted_at >= SCOPE_ATTEMPTED_AT_MIN. Tolerate the column being either
    # parsed datetimes (preferred) or string ISO timestamps. Rows with missing
    # attempted_at are conservatively excluded.
    if "attempted_at" in df.columns:
        attempted_series = df["attempted_at"]
        # Parse to comparable form. errors=coerce → NaT for unparseable, which
        # falls out of the >= comparison and yields False.
        parsed = pd.to_datetime(attempted_series, utc=True, errors="coerce")
        cutoff = pd.Timestamp(SCOPE_ATTEMPTED_AT_MIN)
        attempted_mask = parsed >= cutoff
        attempted_mask = attempted_mask.fillna(False)
    else:
        logger.warning("Manifest has no attempted_at column — refusing to flip (can't bound scope).")
        return pd.Series([False] * len(df), index=df.index)

    return status_mask & venue_mask & blank_reason_mask & ag_mask & inst_mask & attempted_mask


def _print_distribution(df_scope: pd.DataFrame) -> None:
    """Log scope distribution by (venue, year, data_type) for operator review."""
    if df_scope.empty:
        return
    # Year extraction — the row's `date` column is the data day; attempted_at
    # is the manifest write moment. Both are useful; group by data-day year
    # because that's what operators reason about ("how much of 2024 will
    # be re-fetched?").
    if "date" in df_scope.columns:
        try:
            year_series = pd.to_datetime(df_scope["date"], errors="coerce").dt.year.fillna(0).astype(int)
        except (ValueError, TypeError):
            year_series = pd.Series([0] * len(df_scope), index=df_scope.index)
    else:
        year_series = pd.Series([0] * len(df_scope), index=df_scope.index)
    grouped = df_scope.assign(_year=year_series).groupby(
        ["venue", "_year", "data_type"], dropna=False
    ).size().reset_index(name="count")
    grouped = grouped.sort_values("count", ascending=False).head(40)
    logger.info("In-scope distribution (top 40 by (venue, year, data_type)):")
    for _, r in grouped.iterrows():
        logger.info(
            "  %-20s | %4d | %-25s | %d",
            str(r["venue"]),
            int(r["_year"]),
            str(r["data_type"]),
            int(r["count"]),
        )

    # Also print a bare per-venue total — operator wants to see all 6 venues
    # representing the expected ~480k.
    venue_totals = df_scope.groupby("venue").size().sort_values(ascending=False)
    logger.info("In-scope per-venue totals:")
    for venue, count in venue_totals.items():
        logger.info("  %-20s | %d", str(venue), int(count))


def _print_sample_rows(df_scope: pd.DataFrame, n: int) -> None:
    """Log up to n sample in-scope rows for sanity-check."""
    if df_scope.empty:
        return
    cols = [
        c for c in (
            "venue", "data_type", "instrument_type", "instrument_id",
            "date", "capture_status", "error_reason", "attempted_at",
        ) if c in df_scope.columns
    ]
    sample = df_scope[cols].head(n)
    logger.info("Sample in-scope rows (first %d):\n%s", n, sample.to_string(index=False))


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or CEFI_BUCKET
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"recon-tardis-thirdkey-{run_ts}"

    # If neither --dry-run nor --apply-flips passed, treat as dry-run (default).
    # If both passed, --dry-run wins (safe default). Compute a single boolean.
    do_apply = args.apply_flips and not args.dry_run

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recon-tardis-thirdkey-{run_ts}.csv"

    _emit_event(
        "RECONCILER_STARTED",
        reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
        asset_group="cefi",
        bucket=bucket_name,
        apply_flips=do_apply,
        max_flips_per_run=args.max_flips_per_run,
        scope_venues=sorted(TIER3_TARDIS_VENUES),
        scope_attempted_at_min=SCOPE_ATTEMPTED_AT_MIN,
        flip_error_reason=FLIP_ERROR_REASON,
        run_id=run_id,
    )

    if do_apply:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-flips requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation rule, "
                "codex/02-data/availability-manifest-and-data-status.md). Aborting."
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
                reason="missing_per_vm_shards_env",
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag> (per-VM shard isolation rule). Aborting.")
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
                reason="missing_vm_name_env",
            )
            return 4

    start = time.time()
    df, local_manifest = _download_manifest(bucket_name)
    try:
        mask = _build_scope_mask(df)
        n_candidates = int(mask.sum())
        logger.info(
            "Candidate rows (scope filter passed): %d / %d (%.4f%%)",
            n_candidates,
            len(df),
            (n_candidates / len(df) * 100) if len(df) else 0.0,
        )
        if n_candidates == 0:
            logger.info("Nothing to reconcile. Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
                asset_group="cefi",
                candidates=0,
                flipped=0,
            )
            return 0

        df_scope = df.loc[mask]

        # Operator review surfaces.
        _print_distribution(df_scope)
        _print_sample_rows(df_scope, args.sample_rows)

        # CSV audit dump — same row shape as the other reconcilers.
        audit_records: list[dict[str, str | int]] = []
        for idx in df_scope.index:
            row = df.loc[idx]
            audit_records.append(
                {
                    "row_index": int(idx),
                    "date": str(row.get("date", "")),
                    "venue": str(row.get("venue", "")),
                    "chain": str(row.get("chain", "")),
                    "data_type": str(row.get("data_type", "")),
                    "instrument_type": str(row.get("instrument_type", "")),
                    "instrument_id": str(row.get("instrument_id", "")),
                    "attempted_at": str(row.get("attempted_at", "")),
                    "old_capture_status": "empty_confirmed",
                    "old_error_reason": "",
                    "new_capture_status": "attempted_failed",
                    "new_error_reason": FLIP_ERROR_REASON,
                }
            )

        cols = list(audit_records[0].keys())
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols)
            writer.writeheader()
            writer.writerows(audit_records)
        logger.info("Audit CSV written: %s (%d rows)", report_path, len(audit_records))

        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
            asset_group="cefi",
            candidates=n_candidates,
            report_path=str(report_path),
        )

        if not do_apply:
            elapsed = time.time() - start
            logger.info(
                "Scan-only mode (no --apply-flips). Reviewed %d rows in %.1fs. "
                "Re-run with --apply-flips MANIFEST_PER_VM_SHARDS=true VM_NAME=<unique>.",
                n_candidates,
                elapsed,
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
                asset_group="cefi",
                candidates=n_candidates,
                flipped=0,
                report_path=str(report_path),
                elapsed_s=round(elapsed, 1),
            )
            return 0

        if n_candidates > args.max_flips_per_run:
            logger.error(
                "Detected %d candidates > --max-flips-per-run=%d; aborting per halt-safety rule. "
                "Lift the cap explicitly with --max-flips-per-run after dry-run review.",
                n_candidates,
                args.max_flips_per_run,
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
                reason="max_flips_exceeded",
                detected=n_candidates,
                cap=args.max_flips_per_run,
            )
            return 2

        # Apply: stamp capture_status=attempted_failed + error_reason on the
        # in-scope rows. attempted_at is left as the original sentinel write
        # time so the audit trail is preserved; the consolidator's
        # last-writer-wins on row_key will replace these rows in the canonical
        # manifest with the per-VM shard's content.
        now_iso = datetime.now(UTC).isoformat()
        df.loc[mask, "capture_status"] = "attempted_failed"
        df.loc[mask, "error_reason"] = FLIP_ERROR_REASON
        if "reconciler_run_id" in df.columns:
            df.loc[mask, "reconciler_run_id"] = run_id
        # Also bump attempted_at on the flipped rows so anything that filters
        # on "freshness" sees these as the new state. (Optional but matches
        # reconcile_phantom_manifest_rows_all.py's behaviour.)
        if "attempted_at" in df.columns:
            df.loc[mask, "attempted_at"] = now_iso

        # Write back via per-VM shard. The consolidator daemon merges per-VM
        # shards into the canonical manifest with last-writer-wins on
        # identical row_key. Only the in-scope rows are written (smaller
        # blob, faster consolidator turn-around).
        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix="recon-tardis-thirdkey-out-",
            suffix=".parquet",
            delete=False,
        ) as tf:
            out_path = tf.name
        try:
            df.loc[mask].to_parquet(out_path, index=False)
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(bucket_name)
            out_blob = bucket.blob(per_vm_blob)
            out_blob.upload_from_filename(out_path)
            logger.info("Uploaded per-VM shard to gs://%s/%s", bucket_name, per_vm_blob)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(out_path)

        elapsed = time.time() - start
        logger.info(
            "Flipped %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator "
            "will merge into canonical manifest within ~5min.",
            n_candidates,
            elapsed,
            bucket_name,
            per_vm_blob,
        )
        _emit_event(
            "RECONCILER_COMPLETED",
            reconciler="reconcile_cefi_tardis_thirdkey_drift_2026_05_07",
            asset_group="cefi",
            candidates=n_candidates,
            flipped=n_candidates,
            report_path=str(report_path),
            per_vm_blob=per_vm_blob,
            run_id=run_id,
            elapsed_s=round(elapsed, 1),
        )
        return 0
    finally:
        with contextlib.suppress(OSError):
            os.unlink(local_manifest)


if __name__ == "__main__":
    sys.exit(main())
