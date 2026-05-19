"""Correct rows wrongly flipped to attempted_failed/LegacyBlankErrorReasonError.

Background
==========

On 2026-05-13 slot 3 ran ``reconcile_legacy_blank_to_typed_reason.py --apply-flips``
against defi (604,951 rows) + cefi (3,146 rows) before UAC had
``DEFI_VENUE_LAUNCH_DATES``. ``_classify_defi`` only checked chain genesis (Ethereum
2015-07-30, Solana 2020-03-16) but not protocol launch (Aave V3 2022-03-16, Lido
2020-12-19, etc.). All pre-protocol-launch rows defaulted to
``SOURCE_RETURNED_ZERO`` → wrapper flipped to ``attempted_failed/LegacyBlankErrorReasonError``.

This script re-runs ``classify_blank_reason_row`` (now backed by the new
``DEFI_VENUE_LAUNCH_DATES`` shipped at UAC@ca62a19 + UTL@b0c38a21) against rows
currently in ``attempted_failed/LegacyBlankErrorReasonError`` and flips them back
to ``empty_confirmed/EXPECTED_PRE_VENUE_LAUNCH`` (or other ``EXPECTED_*``) where
the corrected classifier now recognises the pre-launch case.

Idempotent: re-running on already-corrected rows is a no-op (the classifier
returns the same reason as the current state).

Usage
=====

    cd instruments-service
    VM_NAME=ikenna-slot3-corrector MANIFEST_PER_VM_SHARDS=true \
      ./.venv/bin/python scripts/reconcile_correct_legacy_blank_misflips_2026_05_13.py \
        --asset-group defi --apply-flips --max-flips-per-run 1000000

References
==========

* Plan: ``plans/active/issues/defi_legacy_blank_reclassification_2026_05_13.md``
* UAC: ``unified_api_contracts/registry/venue_launch_dates.py`` DEFI_VENUE_LAUNCH_DATES
* UTL: ``unified_trading_library/legacy_reason_classifier.py`` _classify_defi
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage

from unified_trading_library.instrument_lifecycle_loader import (  # noqa: qg-deep-import
    load_instrument_lifecycle,
)
from unified_trading_library.legacy_reason_classifier import (  # noqa: qg-deep-import
    classify_blank_reason_row,
)

logger = logging.getLogger("reconcile_correct_legacy_blank_misflips")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

PROJECT_ID = "central-element-323112"
BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    "prediction": f"market-data-tick-prediction-{PROJECT_ID}",
}


def _log_event(event: str, **kwargs: object) -> None:
    """Emit a JSON-ish event line for observability."""
    payload = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        "reconciler": "reconcile_correct_legacy_blank_misflips",
        **kwargs,
    }
    logger.info("EVENT %s", payload)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Re-classify rows currently in attempted_failed/LegacyBlankErrorReasonError. "
            "Flip back to empty_confirmed/EXPECTED_* where the updated UAC/UTL classifier "
            "now recognises the row as a pre-protocol-launch / pre-chain-genesis case."
        )
    )
    parser.add_argument(
        "--asset-group",
        choices=["cefi", "defi", "tradfi", "sports", "prediction"],
        required=True,
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical bucket (default: per-asset-group SSOT).",
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        help="Default scan-only. Pass this flag to actually flip + upload per-VM shard.",
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=100_000,
        help="Halt-safety cap (default 100k). Aborts if scan finds more than this.",
    )
    parser.add_argument(
        "--report-dir",
        default=tempfile.gettempdir(),
        help="Override CSV report dir.",
    )
    args = parser.parse_args()

    # Per-VM shard isolation guard (CLAUDE.md HARD RULE).
    if args.apply_flips and os.environ.get("MANIFEST_PER_VM_SHARDS") != "true":
        logger.error(
            "--apply-flips requires MANIFEST_PER_VM_SHARDS=true "
            "(per-VM shard isolation rule, codex/02-data/availability-manifest-and-data-status.md). Aborting."
        )
        _log_event("RECONCILER_FAILED", reason="missing_per_vm_shards_env")
        return 1

    vm_name = os.environ.get("VM_NAME")
    if args.apply_flips and not vm_name:
        logger.error("--apply-flips requires VM_NAME to be set. Aborting.")
        _log_event("RECONCILER_FAILED", reason="missing_vm_name")
        return 1

    bucket_name = args.bucket or BUCKETS[args.asset_group]
    run_id = f"recon-correct-misflips-{args.asset_group}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    _log_event(
        "RECONCILER_STARTED",
        asset_group=args.asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips_per_run=args.max_flips_per_run,
        run_id=run_id,
    )

    # Read main consolidated manifest.
    logger.info("Loading manifest from gs://%s/_index/availability_index.parquet", bucket_name)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("_index/availability_index.parquet")
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("Manifest rows: %d", len(df))

    # Find candidates: capture_status=attempted_failed AND error_reason contains "LegacyBlankErrorReasonError".
    if "capture_status" not in df.columns or "error_reason" not in df.columns:
        logger.error("Manifest missing required columns. Exiting.")
        _log_event("RECONCILER_FAILED", reason="missing_columns")
        return 1

    status_col = df["capture_status"].astype(str).str.strip()
    reason_col = df["error_reason"].astype(str).str.strip()
    mask = (status_col == "attempted_failed") & reason_col.str.contains("LegacyBlankErrorReasonError", na=False)
    n_candidates = int(mask.sum())
    logger.info(
        "Candidate rows (attempted_failed AND error_reason contains LegacyBlankErrorReasonError): %d / %d (%.2f%%)",
        n_candidates,
        len(df),
        100.0 * n_candidates / max(len(df), 1),
    )

    if n_candidates == 0:
        logger.info("No candidate rows. Exiting.")
        _log_event(
            "RECONCILER_COMPLETED",
            asset_group=args.asset_group,
            candidates=0,
            corrected=0,
        )
        return 0

    # Wave 3: load per-instrument lifecycle map for cefi/defi. Sports/prediction
    # don't need it (legit empty_confirmed at instrument-day grain).
    instrument_lifecycle = None
    if args.asset_group in {"cefi", "defi", "tradfi"}:
        logger.info("Loading per-instrument lifecycle bounds for %s catalog...", args.asset_group)
        instrument_lifecycle = load_instrument_lifecycle(args.asset_group, PROJECT_ID)
        logger.info("Loaded %d (venue, instrument_key) lifecycle entries", len(instrument_lifecycle))

    # Re-classify each candidate.
    candidate_idx = df.index[mask]
    corrections: list[dict[str, str | int]] = []
    n_no_change = 0
    transitions: dict[str, int] = {}

    for idx in candidate_idx:
        row = df.loc[idx]
        try:
            new_status, new_reason = classify_blank_reason_row(
                args.asset_group, row, instrument_lifecycle=instrument_lifecycle
            )
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            logger.warning("Classifier failed for row %s: %s — leaving unchanged", idx, exc)
            n_no_change += 1
            continue
        # We only correct rows where the new classification is different AND
        # represents an upgrade (e.g. attempted_failed/LegacyBlank → empty_confirmed/EXPECTED_*).
        # If the classifier still returns attempted_failed/LegacyBlank (or any
        # attempted_failed variant) for this row, no upgrade is possible —
        # leave it alone. The row genuinely is attempted_failed.
        is_upgrade_to_empty_confirmed = new_status == "empty_confirmed" and new_reason != "SOURCE_RETURNED_ZERO"
        if not is_upgrade_to_empty_confirmed:
            n_no_change += 1
            continue
        transition_key = f"attempted_failed/LegacyBlankErrorReasonError -> {new_status}/{new_reason}"
        transitions[transition_key] = transitions.get(transition_key, 0) + 1
        corrections.append(
            {
                "row_index": int(idx),
                "date": str(row.get("date", ""))[:10],
                "venue": str(row.get("venue", "")),
                "chain": str(row.get("chain", "")),
                "data_type": str(row.get("data_type", "")),
                "instrument_id": str(row.get("instrument_id", ""))[:60],
                "old_status": "attempted_failed",
                "old_reason": "LegacyBlankErrorReasonError",
                "new_status": new_status,
                "new_reason": new_reason,
            }
        )

    n_corrections = len(corrections)
    logger.info(
        "Candidates: %d | proposed corrections: %d | no-change: %d",
        n_candidates,
        n_corrections,
        n_no_change,
    )
    logger.info("Correction transitions:")
    for k, v in sorted(transitions.items(), key=lambda kv: -kv[1]):
        logger.info("  %s: %d", k, v)

    # Write report CSV (always, even on dry-run).
    if corrections:
        report_path = f"{args.report_dir.rstrip('/')}/{run_id}.csv"
        pd.DataFrame(corrections).to_csv(report_path, index=False)
        logger.info("Correction report: %s (%d rows)", report_path, n_corrections)

    _log_event(
        "RECONCILER_PROGRESS",
        asset_group=args.asset_group,
        candidates=n_candidates,
        corrections=n_corrections,
        transitions=transitions,
    )

    if n_corrections == 0:
        logger.info("No corrections to apply.")
        _log_event(
            "RECONCILER_COMPLETED",
            asset_group=args.asset_group,
            candidates=n_candidates,
            corrected=0,
        )
        return 0

    if n_corrections > args.max_flips_per_run:
        logger.error(
            "Detected %d corrections > --max-flips-per-run=%d; aborting per halt-safety rule.",
            n_corrections,
            args.max_flips_per_run,
        )
        _log_event(
            "RECONCILER_FAILED",
            reason="max_flips_exceeded",
            detected=n_corrections,
            cap=args.max_flips_per_run,
        )
        return 1

    if not args.apply_flips:
        logger.info("Dry-run complete. Pass --apply-flips to actually correct.")
        _log_event(
            "RECONCILER_COMPLETED",
            asset_group=args.asset_group,
            candidates=n_candidates,
            corrected=0,
            dry_run=True,
        )
        return 0

    # Apply corrections to the dataframe.
    started = datetime.now(timezone.utc)
    for c in corrections:
        idx = c["row_index"]
        df.at[idx, "capture_status"] = c["new_status"]
        df.at[idx, "error_reason"] = c["new_reason"]
        # Clear attempted_at on revert to empty_confirmed.
        if "attempted_at" in df.columns:
            df.at[idx, "attempted_at"] = None

    # Write per-VM shard.
    corrected_subset = df.loc[[c["row_index"] for c in corrections]].copy()
    per_vm_blob_path = f"_index/per_vm/{vm_name}.parquet"
    per_vm_blob = bucket.blob(per_vm_blob_path)
    buf = io.BytesIO()
    corrected_subset.to_parquet(buf, index=False)
    per_vm_blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(
        "Uploaded per-VM shard to gs://%s/%s (%d corrected rows)",
        bucket_name,
        per_vm_blob_path,
        n_corrections,
    )
    logger.info(
        "Corrected %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator merges within ~5min.",
        n_corrections,
        elapsed,
        bucket_name,
        per_vm_blob_path,
    )
    _log_event(
        "RECONCILER_COMPLETED",
        asset_group=args.asset_group,
        candidates=n_candidates,
        corrected=n_corrections,
        transitions=transitions,
        per_vm_blob=per_vm_blob_path,
        run_id=run_id,
        elapsed_s=round(elapsed, 1),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
