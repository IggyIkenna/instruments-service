#!/usr/bin/env python3
"""reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py — Wave 3 CeFi corrector.

Mirror of slot 3's defi corrector (``reconcile_correct_legacy_blank_misflips_2026_05_13.py``)
for the CeFi asset group.

**The problem this fixes.** The 2026-05-07 ``reconcile_blank_error_reason_rows.py``
sweep correctly flipped every blank-reason empty_confirmed row in cefi/defi/tradfi
to ``attempted_failed/LegacyBlankErrorReasonError`` — because at the time the
classifier had no way to distinguish "genuinely needed re-fetching" from "was
pre-listing/post-delisting". That conservative flip was right given the available
data.

Now Wave 3 adds per-instrument catalog cross-ref to ``_classify_cefi`` via
``unified_trading_library.instruments_catalog_reader``. This corrector walks the
~789k ``(cefi, attempted_failed, error_reason=LegacyBlankErrorReasonError)`` rows,
re-classifies each via the extended ``classify_blank_reason_row``, and for rows
where the classifier NOW fires ``EXPECTED_INSTRUMENT_NOT_LISTED`` /
``EXPECTED_INSTRUMENT_DELISTED`` / ``EXPECTED_PRE_VENUE_LAUNCH``, flips:

    capture_status: attempted_failed  →  empty_confirmed
    error_reason:   LegacyBlankErrorReasonError  →  EXPECTED_<specific>

Rows that still classify to ``SOURCE_RETURNED_ZERO`` (i.e. the classifier found the
instrument active on that day) are left as ``attempted_failed`` — they are legitimate
re-fetch candidates for the next VM run.

**Idempotency.** Re-running on already-corrected rows is a no-op: corrected rows have
``capture_status=empty_confirmed``, so the candidate mask (which requires
``capture_status=attempted_failed``) won't select them.

**Per-VM shard isolation.** ``--apply-flips`` requires ``MANIFEST_PER_VM_SHARDS=true``
+ ``VM_NAME=<unique>``. The shard is written to
``gs://<cefi-bucket>/_index/per_vm/<VM_NAME>.parquet``.

Workspace SSOT:
* ``plans/active/issues/defi_classifier_missing_catalog_crossref_2026_05_13.md``
* ``unified_trading_library.instruments_catalog_reader`` (Wave 3 helper)
* ``unified_trading_library.legacy_reason_classifier._classify_cefi`` (extended)

Example::

    # Dry-run — count candidates + show distribution (no writes)
    python scripts/reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py \\
        --asset-group cefi

    # Apply (after dry-run review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=ikenna-slot2-corrector-cefi \\
    python scripts/reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py \\
        --asset-group cefi --apply-flips --max-flips 1000000 --confirm

execution:
  owner: ikenna-slot2 (2026-05-13 initial run); one-shot
  cadence: one-shot (run once; re-run for idempotency check)
  verifier: RECONCILER_COMPLETED event + post-run candidate count == 0 for corrected status
  last_executed: NEVER
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import logging
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage
from unified_api_contracts import EMPTY_CONFIRMED_REASONS
from unified_trading_library.legacy_reason_classifier import classify_blank_reason_row  # noqa: qg-deep-import

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
}
MANIFEST_BLOB = "_index/availability_index.parquet"

# The error_reason string that the 2026-05-07 sweep stamped on cefi/defi/tradfi
# rows that the old classifier couldn't fire a specific EXPECTED_* reason for.
LEGACY_BLANK_ERROR_CLASS = "LegacyBlankErrorReasonError"

RECONCILER_NAME = "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13"

# Which EXPECTED_* reasons are valid corrections (non-SOURCE_RETURNED_ZERO empties).
VALID_CORRECTION_REASONS: frozenset[str] = frozenset(r for r in EMPTY_CONFIRMED_REASONS if r.startswith("EXPECTED_"))


def _emit_event(event: str, /, **details: object) -> None:
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Wave 3 CeFi corrector: re-classify manifest rows that the 2026-05-07 sweep "
            "conservatively flipped to attempted_failed/LegacyBlankErrorReasonError, "
            "now that the extended _classify_cefi has per-instrument catalog cross-ref. "
            "Rows that NOW classify to EXPECTED_INSTRUMENT_NOT_LISTED / "
            "EXPECTED_INSTRUMENT_DELISTED / EXPECTED_PRE_VENUE_LAUNCH are flipped back "
            "to empty_confirmed with the specific typed reason."
        ),
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(ASSET_GROUP_BUCKETS.keys()),
        help="Asset group manifest to reconcile (default: cefi).",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical bucket (default: per-asset-group SSOT).",
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        help=("Default dry-run. Pass this flag to actually stamp the corrections + upload the per-VM shard."),
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required alongside --apply-flips. Confirms intent to mutate the manifest.",
    )
    parser.add_argument(
        "--max-flips",
        type=int,
        default=1_000_000,
        help="Halt-safety cap (default 1M). Aborts if scan finds more corrections than this.",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report directory (default: tempfile.gettempdir()).",
    )
    return parser.parse_args()


def _download_manifest(bucket_name: str, asset_group: str) -> tuple[pd.DataFrame, str]:
    """Bulk-download the canonical manifest. Returns (df, local_path)."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix=f"cefi-corrector-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_candidate_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask of attempted_failed rows with LegacyBlankErrorReasonError."""
    if "capture_status" not in df.columns or "error_reason" not in df.columns:
        logger.warning("Manifest missing capture_status / error_reason — nothing to correct.")
        return pd.Series([False] * len(df), index=df.index)
    failed_mask = df["capture_status"].fillna("").astype(str).str.strip() == "attempted_failed"
    reason_norm = df["error_reason"].fillna("").astype(str).str.strip()
    legacy_mask = reason_norm.str.startswith(LEGACY_BLANK_ERROR_CLASS)
    return failed_mask & legacy_mask


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[args.asset_group]
    asset_group: str = args.asset_group
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"{RECONCILER_NAME}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"cefi-corrector-{asset_group}-{run_ts}.csv"

    if args.apply_flips and not args.confirm:
        logger.error("--apply-flips requires --confirm flag. Re-run with both flags. Aborting.")
        _emit_event("RECONCILER_FAILED", reconciler=RECONCILER_NAME, reason="missing_confirm_flag")
        return 1

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error("--apply-flips requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation rule). Aborting.")
            _emit_event("RECONCILER_FAILED", reconciler=RECONCILER_NAME, reason="missing_per_vm_shards_env")
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag>. Aborting.")
            _emit_event("RECONCILER_FAILED", reconciler=RECONCILER_NAME, reason="missing_vm_name_env")
            return 4

    _emit_event(
        "RECONCILER_STARTED",
        reconciler=RECONCILER_NAME,
        asset_group=asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips=args.max_flips,
        run_id=run_id,
    )

    start = time.time()
    df, local_manifest = _download_manifest(bucket_name, asset_group)
    try:
        mask = _build_candidate_mask(df)
        n_candidates = int(mask.sum())
        logger.info(
            "Candidate rows (attempted_failed AND error_reason starts with %s): %d / %d (%.2f%%)",
            LEGACY_BLANK_ERROR_CLASS,
            n_candidates,
            len(df),
            (n_candidates / len(df) * 100) if len(df) else 0.0,
        )

        if n_candidates == 0:
            logger.info("No candidate rows — manifest already clean for this asset_group. Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=asset_group,
                candidates=0,
                corrected=0,
                elapsed_s=round(time.time() - start, 1),
            )
            return 0

        candidate_idx = df.index[mask]
        corrections: list[dict[str, str | int]] = []
        n_no_change = 0
        n_errors = 0

        for idx in candidate_idx:
            row = df.loc[idx]
            try:
                new_status, new_reason = classify_blank_reason_row(asset_group, row)
            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                logger.warning("Classifier failed for row %s: %s — leaving unchanged", idx, exc)
                n_errors += 1
                continue

            # A correction fires when the extended classifier now returns a
            # specific EXPECTED_* reason AND suggests empty_confirmed (meaning
            # the reason is a valid venue-level or per-instrument rule, not
            # SOURCE_RETURNED_ZERO which would stay attempted_failed).
            is_correction = new_status == "empty_confirmed" and new_reason in VALID_CORRECTION_REASONS
            if not is_correction:
                n_no_change += 1
                continue

            corrections.append(
                {
                    "row_index": int(idx),
                    "date": str(row.get("date", "")),
                    "venue": str(row.get("venue", "")),
                    "data_type": str(row.get("data_type", "")),
                    "instrument_id": str(row.get("instrument_id", "") or row.get("instrument_key", "")),
                    "old_capture_status": "attempted_failed",
                    "new_capture_status": new_status,
                    "old_reason": str(row.get("error_reason", "")),
                    "new_reason": new_reason,
                }
            )

        n_corrections = len(corrections)
        logger.info(
            "Candidates: %d | proposed corrections: %d | no-change (legit re-fetch): %d | errors: %d",
            n_candidates,
            n_corrections,
            n_no_change,
            n_errors,
        )

        # Distribution summary.
        transition_counts: dict[str, int] = {}
        for entry in corrections:
            key = f"{entry['old_reason']} -> {entry['new_capture_status']}/{entry['new_reason']}"
            transition_counts[key] = transition_counts.get(key, 0) + 1
        if transition_counts:
            logger.info("Correction transitions:")
            for key, count in sorted(transition_counts.items(), key=lambda kv: -kv[1]):
                logger.info("  %s: %d", key, count)

        # CSV audit.
        if corrections:
            cols = list(corrections[0].keys())
            with report_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=cols)
                writer.writeheader()
                writer.writerows(corrections)
            logger.info("Would-correct report: %s (%d rows)", report_path, n_corrections)

        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler=RECONCILER_NAME,
            asset_group=asset_group,
            candidates=n_candidates,
            corrections=n_corrections,
            no_change=n_no_change,
            transitions=transition_counts,
            report_path=str(report_path),
        )

        if n_corrections == 0:
            logger.info(
                "No rows would be corrected — every candidate still needs re-fetch. "
                "Catalog likely not yet built or all instruments were genuinely active. "
                "Run 'instruments-service build-catalogue --asset-group cefi' first."
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=asset_group,
                candidates=n_candidates,
                corrected=0,
                elapsed_s=round(time.time() - start, 1),
            )
            return 0

        if not args.apply_flips:
            elapsed = time.time() - start
            logger.info(
                "Dry-run mode (no --apply-flips). Found %d proposed corrections in %.1fs. "
                "Re-run with --apply-flips --confirm MANIFEST_PER_VM_SHARDS=true VM_NAME=<unique> "
                "after reviewing the CSV.",
                n_corrections,
                elapsed,
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=asset_group,
                candidates=n_candidates,
                corrected=0,
                proposed_corrections=n_corrections,
                report_path=str(report_path),
                elapsed_s=round(elapsed, 1),
            )
            return 0

        if n_corrections > args.max_flips:
            logger.error(
                "Detected %d proposed corrections > --max-flips=%d; aborting per halt-safety rule.",
                n_corrections,
                args.max_flips,
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler=RECONCILER_NAME,
                reason="max_flips_exceeded",
                detected=n_corrections,
                cap=args.max_flips,
            )
            return 2

        # Apply: stamp new capture_status + error_reason, upload per-VM shard.
        corrected_idx = [entry["row_index"] for entry in corrections]
        for entry in corrections:
            df.at[entry["row_index"], "capture_status"] = entry["new_capture_status"]
            df.at[entry["row_index"], "error_reason"] = entry["new_reason"]
        if "reconciler_run_id" in df.columns:
            for entry in corrections:
                df.at[entry["row_index"], "reconciler_run_id"] = run_id

        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix=f"cefi-corrector-out-{asset_group}-",
            suffix=".parquet",
            delete=False,
        ) as tf:
            out_path = tf.name
        try:
            df.loc[corrected_idx].to_parquet(out_path, index=False)
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(bucket_name)
            bucket.blob(per_vm_blob).upload_from_filename(out_path)
            logger.info(
                "Uploaded per-VM shard to gs://%s/%s (%d corrected rows)",
                bucket_name,
                per_vm_blob,
                n_corrections,
            )
        finally:
            with contextlib.suppress(OSError):
                os.unlink(out_path)

        elapsed = time.time() - start
        logger.info(
            "Corrected %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator merges within ~5min.",
            n_corrections,
            elapsed,
            bucket_name,
            per_vm_blob,
        )
        _emit_event(
            "RECONCILER_COMPLETED",
            reconciler=RECONCILER_NAME,
            asset_group=asset_group,
            candidates=n_candidates,
            corrected=n_corrections,
            no_change=n_no_change,
            transitions=transition_counts,
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
    raise SystemExit(main())
