#!/usr/bin/env python3
"""reconcile_blank_error_reason_rows.py — Wave 2.M of writegate Phase 3.D.5.

Walks an asset-group manifest, finds rows where ``capture_status=empty_confirmed``
AND ``error_reason`` is blank/null, and reclassifies each via the UTL
:func:`unified_trading_library.legacy_reason_classifier.classify_blank_reason_row`
helper which honours the per-asset-group empty_confirmed legitimacy rule
(operator directive 2026-05-07 msg 6):

* sports / prediction: keeps ``empty_confirmed`` with the classified reason
  (no-fixtures-today / no-markets-active is legit at instrument-day grain).
* cefi / defi / tradfi: keeps ``empty_confirmed`` ONLY if the classifier
  matches a venue-level EXPECTED_* (HOLIDAY / WEEKEND / PRE_VENUE_LAUNCH /
  PRE_GENESIS_CHAIN / PARTIAL_HALF_DAY). Otherwise flips capture_status to
  ``attempted_failed`` with a typed ``LegacyBlankErrorReasonError`` repr in
  ``error_reason`` — the orchestrator's retry-on-attempted_failed default
  then forces re-attempt on next VM run.

**Why this matters (RED ALERT 2026-05-07).** 5 CeFi VMs wrote 96-100% empty
rows for bitfinex / bitget / kraken with all blank error_reason — a silent
fetch-failure pattern that masked real bugs as honest empties. Plus an
unknown number of legacy rows from prior silent-fallback paths. Running this
reconciler on canonical manifests:

1. Stamps real venue-level absences (pre-launch / weekends / holidays) with
   typed reasons → manifest is now self-describing for those cases.
2. Flips silent-fetch-failure rows in cefi/defi/tradfi to attempted_failed
   → next VM run re-attempts those shards instead of skipping per the
   silent empty_confirmed.

**Default mode is SCAN-ONLY**: produces a CSV report of "would-flip" rows.
``--apply-flips`` is the explicit flag to actually mutate the manifest.
``--max-flips-per-run`` default 100k halt safety; operator confirms first
batch looks right before lifting the cap.

Per-VM shard isolation rule honoured: ``--apply-flips`` requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Example::

    # Scan only — produces /tmp/recon-blank-cefi-{ts}.csv
    python scripts/reconcile_blank_error_reason_rows.py --asset-group cefi

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=recon-blank-cefi-$(date +%s) \\
    python scripts/reconcile_blank_error_reason_rows.py \\
        --asset-group cefi --apply-flips --max-flips-per-run 50000

Sister script to ``reconcile_expected_absence_reasons.py`` — that one
handled the older codepath of stamping empty_confirmed reasons via the
single-return classifier (which the audit found had silently been WRONG
for cefi/defi/tradfi). This script supersedes it for cefi/defi/tradfi
(by also flipping capture_status); for sports/prediction the two scripts
produce the same outcome.

Reference: writegate plan
``writegate_honest_coverage_endtoend_2026_05_06.md`` Phase 3.D.5
Wave 2.M.
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
from unified_trading_library import classify_blank_reason_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    "prediction": f"market-data-tick-prediction-{PROJECT_ID}",
}
MANIFEST_BLOB = "_index/availability_index.parquet"


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log mirroring RECONCILER_* shape."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reclassify legacy empty_confirmed manifest rows that have NULL/blank "
            "error_reason. Writegate Phase 3.D.5 Wave 2.M — see module docstring."
        ),
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(ASSET_GROUP_BUCKETS.keys()),
        help="Asset group manifest to reconcile.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical bucket (default: per-asset-group SSOT).",
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        help="Default scan-only. Pass this flag to actually flip + upload manifest.",
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=100_000,
        help="Halt-safety cap (default 100k). Aborts if scan finds more than this.",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report dir (default: tempfile.gettempdir()).",
    )
    return parser.parse_args()


def _download_manifest(bucket_name: str, asset_group: str) -> tuple[pd.DataFrame, str]:
    """Bulk-download the canonical manifest. Returns (df, local_path)."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix=f"recon-blank-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_blank_reason_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask of empty_confirmed rows with blank error_reason."""
    if "capture_status" not in df.columns:
        logger.warning("Manifest has no capture_status column — nothing to reconcile.")
        return pd.Series([False] * len(df), index=df.index)
    empty_mask = df["capture_status"].fillna("") == "empty_confirmed"
    if "error_reason" not in df.columns:
        return empty_mask
    blank_reason_mask = df["error_reason"].fillna("").astype(str).str.strip() == ""
    return empty_mask & blank_reason_mask


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[args.asset_group]
    asset_group: str = args.asset_group
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"recon-blank-{asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recon-blank-{asset_group}-{run_ts}.csv"

    _emit_event(
        "RECONCILER_STARTED",
        reconciler="reconcile_blank_error_reason_rows",
        asset_group=asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips_per_run=args.max_flips_per_run,
        run_id=run_id,
    )

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error("--apply-flips requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation rule).")
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_blank_error_reason_rows",
                reason="missing_per_vm_shards_env",
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag>.")
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_blank_error_reason_rows",
                reason="missing_vm_name_env",
            )
            return 4

    start = time.time()
    df, local_manifest = _download_manifest(bucket_name, asset_group)
    try:
        mask = _build_blank_reason_mask(df)
        n_candidates = int(mask.sum())
        logger.info(
            "Candidate rows (empty_confirmed AND blank error_reason): %d / %d (%.2f%%)",
            n_candidates,
            len(df),
            (n_candidates / len(df) * 100) if len(df) else 0.0,
        )
        if n_candidates == 0:
            logger.info("Nothing to reclassify. Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler="reconcile_blank_error_reason_rows",
                asset_group=asset_group,
                candidates=0,
                flipped=0,
            )
            return 0

        # Classify each candidate via the discriminated helper.
        candidate_idx = df.index[mask]
        classifications: list[dict[str, str | int]] = []
        for idx in candidate_idx:
            row = df.loc[idx]
            try:
                new_status, new_reason = classify_blank_reason_row(asset_group, row)
            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                logger.warning("Classifier failed for row %s: %s — defaulting to attempted_failed", idx, exc)
                new_status = "attempted_failed"
                new_reason = f"LegacyBlankErrorReasonError(classifier raised: {exc})"
            classifications.append(
                {
                    "row_index": int(idx),
                    "date": str(row.get("date", "")),
                    "venue": str(row.get("venue", "")),
                    "chain": str(row.get("chain", "")),
                    "data_type": str(row.get("data_type", "")),
                    "instrument_type": str(row.get("instrument_type", "")),
                    "instrument_id": str(row.get("instrument_id", "")),
                    "league_id": str(row.get("league_id", "")),
                    "old_status": "empty_confirmed",
                    "old_reason": "",
                    "new_status": new_status,
                    "new_reason": new_reason,
                }
            )

        # Distribution summary for operator review — split by (new_status, new_reason).
        flip_counts: dict[tuple[str, str], int] = {}
        for entry in classifications:
            key = (str(entry["new_status"]), str(entry["new_reason"])[:60])
            flip_counts[key] = flip_counts.get(key, 0) + 1
        logger.info("Classification distribution (new_status / new_reason → count):")
        for (status, reason), count in sorted(flip_counts.items(), key=lambda kv: -kv[1]):
            logger.info("  %-20s | %-60s | %d", status, reason, count)

        # CSV audit.
        cols = list(classifications[0].keys())
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols)
            writer.writeheader()
            writer.writerows(classifications)
        logger.info("Would-flip report: %s (%d rows)", report_path, len(classifications))

        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler="reconcile_blank_error_reason_rows",
            asset_group=asset_group,
            candidates=n_candidates,
            distribution={f"{s}|{r}": c for (s, r), c in flip_counts.items()},
            report_path=str(report_path),
        )

        if not args.apply_flips:
            elapsed = time.time() - start
            logger.info(
                "Scan-only mode (no --apply-flips). Reviewed %d rows in %.1fs. "
                "Re-run with --apply-flips MANIFEST_PER_VM_SHARDS=true VM_NAME=<unique>.",
                n_candidates,
                elapsed,
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler="reconcile_blank_error_reason_rows",
                asset_group=asset_group,
                candidates=n_candidates,
                flipped=0,
                report_path=str(report_path),
                elapsed_s=round(elapsed, 1),
            )
            return 0

        if n_candidates > args.max_flips_per_run:
            logger.error(
                "Detected %d candidates > --max-flips-per-run=%d; aborting per halt-safety rule.",
                n_candidates,
                args.max_flips_per_run,
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_blank_error_reason_rows",
                reason="max_flips_exceeded",
                detected=n_candidates,
                cap=args.max_flips_per_run,
            )
            return 2

        # Apply: flip both capture_status + error_reason on the dataframe.
        for entry in classifications:
            df.at[entry["row_index"], "capture_status"] = entry["new_status"]
            df.at[entry["row_index"], "error_reason"] = entry["new_reason"]
        if "reconciler_run_id" in df.columns:
            for entry in classifications:
                df.at[entry["row_index"], "reconciler_run_id"] = run_id

        # Write back via per-VM shard for atomicity.
        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix=f"recon-blank-out-{asset_group}-",
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
            reconciler="reconcile_blank_error_reason_rows",
            asset_group=asset_group,
            candidates=n_candidates,
            flipped=n_candidates,
            distribution={f"{s}|{r}": c for (s, r), c in flip_counts.items()},
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
