#!/usr/bin/env python3
"""reconcile_legacy_blank_to_typed_reason.py — Wave 3.X Track C reclassifier.

Second-pass companion to ``reconcile_expected_absence_reasons.py`` (Phase 3.D
of the writegate honest-coverage umbrella).

**The problem this fixes.** The 2026-05-07 ``reconcile_blank_error_reason_rows.py``
sweep stamped every legacy ``empty_confirmed`` manifest row that had a blank
``error_reason`` with a *default* reason — typically ``SOURCE_RETURNED_ZERO``
(or ``EXPECTED_INSTRUMENT_NOT_LISTED`` where the catalog cross-product knew the
instrument wasn't listed). At that time the finer SSOTs the classifier needs
to fire the *specific* ``EXPECTED_*`` reasons didn't exist:

* ``HALF_DAY_SESSIONS`` / ``VENUE_SESSION_HOURS`` — added 2026-05-10 UAC@bdc84ed
  (Wave 3.X Track A) → enables ``EXPECTED_PARTIAL_HALF_DAY`` / ``EXPECTED_OUTSIDE_TRADING_HOURS``.
* ``UNDERSTAT_COVERED_LEAGUES`` / per-country transfer windows / FootyStats
  season bounds — added 2026-05-11 UAC@7c8b5ad (Wave 3.X Track B) → enables
  ``EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE`` / ``EXPECTED_OUTSIDE_TRANSFER_WINDOW`` /
  ``EXPECTED_PRE_SEASON`` / ``EXPECTED_POST_SEASON``.

Now that the extended classifier (``unified_trading_library.legacy_reason_classifier``)
can fire those, this reconciler walks the canonical manifest, finds
``empty_confirmed`` rows whose ``error_reason`` is one of the 2026-05-07-sweep
defaults (``SOURCE_RETURNED_ZERO`` / ``EXPECTED_INSTRUMENT_NOT_LISTED``), re-runs
each through :func:`classify_blank_reason_row`, and where the extended classifier
now returns a **more-specific** typed reason (a closed-set ``EXPECTED_*`` that is
neither ``SOURCE_RETURNED_ZERO`` nor the row's current reason), stamps the upgrade.

It does NOT downgrade (an ``EXPECTED_INSTRUMENT_NOT_LISTED`` row that classifies
to ``SOURCE_RETURNED_ZERO`` is left untouched), and it does NOT flip
``capture_status`` (the ``empty_confirmed → attempted_failed`` discrimination for
cefi/defi/tradfi-at-instrument-grain is the writeguard's / the existing reconciler's
job, not a "reason upgrade").

**Default mode is SCAN-ONLY**: produces a CSV of "would-upgrade" rows + a
distribution summary. ``--apply-flips`` is the explicit flag to actually stamp
+ upload (via the per-VM shard so the consolidator merges with last-writer-wins).
``--max-flips-per-run`` (default 100k) is the halt-safety cap.

Example::

    # Scan only — produces /tmp/recon-legacy-typed-tradfi-{ts}.csv
    python scripts/reconcile_legacy_blank_to_typed_reason.py --asset-group tradfi

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=recon-legacy-typed-tradfi-$(date +%s) \\
    python scripts/reconcile_legacy_blank_to_typed_reason.py \\
        --asset-group tradfi --apply-flips --max-flips-per-run 50000

Workspace rules honoured (same shape as ``reconcile_expected_absence_reasons.py``):

* Per-VM shard write isolation (``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=...``)
  per the manifest-concurrency principle.
* ``RECONCILER_STARTED`` / ``RECONCILER_PROGRESS`` / ``RECONCILER_COMPLETED`` /
  ``RECONCILER_FAILED`` events.
* CSV audit listing every upgraded row (shard_key, old_reason → new_reason).
* No new parquets written — manifest ``error_reason`` column edit only.
* Classifier SSOT is in UTL (``classify_blank_reason_row`` — same code the
  reader-side fallback in 8 consumer services uses), so when the classifier
  evolves both paths pick the new behaviour up for free.
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
from unified_trading_library.legacy_reason_classifier import classify_blank_reason_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Asset-group → canonical manifest location (same SSOT as reconcile_expected_absence_reasons.py).
ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    "prediction": f"market-data-tick-prediction-{PROJECT_ID}",
}
MANIFEST_BLOB = "_index/availability_index.parquet"

# The 2026-05-07-sweep default reasons that this reconciler considers candidates
# for an upgrade. Anything else (already-specific EXPECTED_* / a non-empty_confirmed
# capture_status) is left alone.
SWEEP_DEFAULT_REASONS: frozenset[str] = frozenset({"SOURCE_RETURNED_ZERO", "EXPECTED_INSTRUMENT_NOT_LISTED"})
RECONCILER_NAME = "reconcile_legacy_blank_to_typed_reason"


def _emit_event(event: str, /, **details: object) -> None:
    """Emit a structured RECONCILER_* event line. Best-effort logging."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Re-classify legacy empty_confirmed manifest rows that the 2026-05-07 "
            "sweep stamped with a default reason (SOURCE_RETURNED_ZERO / "
            "EXPECTED_INSTRUMENT_NOT_LISTED), upgrading them to a more-specific "
            "EXPECTED_* reason now that the Wave 3.X Track A+B SSOTs are available. "
            "Writegate Phase 3.D / Wave 3.X Track C — see module docstring."
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
        help="Default scan-only. Pass this flag to actually stamp the upgrades + upload the per-VM shard.",
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=100_000,
        help="Halt-safety cap (default 100k). Aborts if scan finds more upgrades than this.",
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
        prefix=f"recon-legacy-typed-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_candidate_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask of empty_confirmed rows whose error_reason is a 2026-05-07-sweep default."""
    if "capture_status" not in df.columns or "error_reason" not in df.columns:
        logger.warning("Manifest missing capture_status / error_reason — nothing to reclassify.")
        return pd.Series([False] * len(df), index=df.index)
    empty_mask = df["capture_status"].fillna("").astype(str).str.strip() == "empty_confirmed"
    reason_norm = df["error_reason"].fillna("").astype(str).str.strip()
    default_mask = reason_norm.isin(SWEEP_DEFAULT_REASONS)
    return empty_mask & default_mask


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[args.asset_group]
    asset_group: str = args.asset_group
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"recon-legacy-typed-{args.asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recon-legacy-typed-{args.asset_group}-{run_ts}.csv"

    _emit_event(
        "RECONCILER_STARTED",
        reconciler=RECONCILER_NAME,
        asset_group=args.asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips_per_run=args.max_flips_per_run,
        run_id=run_id,
    )

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-flips requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation rule, "
                "codex/02-data/availability-manifest-and-data-status.md). Aborting."
            )
            _emit_event("RECONCILER_FAILED", reconciler=RECONCILER_NAME, reason="missing_per_vm_shards_env")
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag> (per-VM shard isolation rule). Aborting.")
            _emit_event("RECONCILER_FAILED", reconciler=RECONCILER_NAME, reason="missing_vm_name_env")
            return 4

    start = time.time()
    df, local_manifest = _download_manifest(bucket_name, args.asset_group)
    try:
        mask = _build_candidate_mask(df)
        n_candidates = int(mask.sum())
        logger.info(
            "Candidate rows (empty_confirmed AND error_reason ∈ %s): %d / %d (%.2f%%)",
            sorted(SWEEP_DEFAULT_REASONS),
            n_candidates,
            len(df),
            (n_candidates / len(df) * 100) if len(df) else 0.0,
        )
        if n_candidates == 0:
            logger.info("No candidate rows. Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=args.asset_group,
                candidates=0,
                upgraded=0,
            )
            return 0

        # Re-classify each candidate; record rows that genuinely upgrade.
        # Three upgrade shapes:
        #   (a) reason upgrade: empty_confirmed stays but gets a more-specific EXPECTED_* reason.
        #   (b) status flip → attempted_failed: cefi/defi/tradfi instrument-day grain rule
        #       OR sports where fixture exists but source returned zero (Phase 1.5).
        #   (c) status flip → expected_unattempted: sports where no fixture exists for the
        #       shard in the instruments-service universe (Phase 1.5 — fixture not in scope).
        fixture_manifest: pd.DataFrame | None = None
        if asset_group == "sports" and "data_type" in df.columns and "capture_status" in df.columns:
            _fix_mask = (df["data_type"].astype(str).str.strip() == "fixtures") & (
                df["capture_status"].astype(str).str.strip() == "captured"
            )
            _fixture_cols = [c for c in ("venue", "league_id", "date") if c in df.columns]
            fixture_manifest = df.loc[_fix_mask, _fixture_cols].copy()
            logger.info(
                "Sports fixture manifest: %d captured fixture rows for fixture-existence check (Phase 1.5)",
                len(fixture_manifest),
            )

        candidate_idx = df.index[mask]
        upgrades: list[dict[str, str | int]] = []
        n_no_change = 0
        for idx in candidate_idx:
            row = df.loc[idx]
            current_reason = str(row.get("error_reason", "")).strip()
            try:
                new_status, new_reason = classify_blank_reason_row(asset_group, row, fixture_manifest=fixture_manifest)
            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                logger.warning("Classifier failed for row %s: %s — leaving row unchanged", idx, exc)
                n_no_change += 1
                continue
            # Shape (a): reason upgrade — same capture_status, better EXPECTED_* reason.
            is_reason_upgrade = (
                new_status == "empty_confirmed"
                and new_reason in EMPTY_CONFIRMED_REASONS
                and new_reason != "SOURCE_RETURNED_ZERO"
                and new_reason != current_reason
            )
            # Shape (b): status flip → attempted_failed (cefi/defi/tradfi instrument-day
            # grain rule; sports where fixture exists but source returned zero — Phase 1.5).
            is_status_flip = new_status == "attempted_failed"
            # Shape (c): sports shard has no fixture in instruments-service universe → expected_unattempted.
            is_expected_unattempted_flip = new_status == "expected_unattempted"
            is_upgrade = is_reason_upgrade or is_status_flip or is_expected_unattempted_flip
            if not is_upgrade:
                n_no_change += 1
                continue
            upgrades.append(
                {
                    "row_index": int(idx),
                    "date": str(row.get("date", "")),
                    "venue": str(row.get("venue", "")),
                    "chain": str(row.get("chain", "")),
                    "data_type": str(row.get("data_type", "")),
                    "instrument_type": str(row.get("instrument_type", "")),
                    "instrument_id": str(row.get("instrument_id", "")),
                    "league_id": str(row.get("league_id", "")),
                    "old_capture_status": "empty_confirmed",
                    "new_capture_status": new_status,
                    "old_reason": current_reason,
                    "new_reason": new_reason,
                }
            )

        n_upgrades = len(upgrades)
        logger.info("Candidates: %d | proposed upgrades: %d | no-change: %d", n_candidates, n_upgrades, n_no_change)

        if n_upgrades == 0:
            logger.info("No rows would upgrade (every candidate still classifies to its current default). Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=args.asset_group,
                candidates=n_candidates,
                upgraded=0,
            )
            return 0

        # Distribution of (old → new) for operator review.
        # Key includes capture_status change for status-flip upgrades.
        transition_counts: dict[str, int] = {}
        for entry in upgrades:
            if entry["new_capture_status"] != entry["old_capture_status"]:
                key = f"{entry['old_capture_status']}/{entry['old_reason']} -> {entry['new_capture_status']}/{entry['new_reason']}"
            else:
                key = f"{entry['old_reason']} -> {entry['new_reason']}"
            transition_counts[key] = transition_counts.get(key, 0) + 1
        logger.info("Upgrade transitions:")
        for key, count in sorted(transition_counts.items(), key=lambda kv: -kv[1]):
            logger.info("  %s: %d", key, count)

        # CSV audit.
        cols = list(upgrades[0].keys())
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols)
            writer.writeheader()
            writer.writerows(upgrades)
        logger.info("Would-upgrade report: %s (%d rows)", report_path, n_upgrades)

        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler=RECONCILER_NAME,
            asset_group=args.asset_group,
            candidates=n_candidates,
            upgrades=n_upgrades,
            transitions=transition_counts,
            report_path=str(report_path),
        )

        if not args.apply_flips:
            elapsed = time.time() - start
            logger.info(
                "Scan-only mode (no --apply-flips). Found %d proposed upgrades in %.1fs. "
                "Re-run with --apply-flips MANIFEST_PER_VM_SHARDS=true VM_NAME=<unique> after CSV review.",
                n_upgrades,
                elapsed,
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                asset_group=args.asset_group,
                candidates=n_candidates,
                upgraded=0,
                proposed_upgrades=n_upgrades,
                report_path=str(report_path),
                elapsed_s=round(elapsed, 1),
            )
            return 0

        if n_upgrades > args.max_flips_per_run:
            logger.error(
                "Detected %d proposed upgrades > --max-flips-per-run=%d; aborting per halt-safety rule.",
                n_upgrades,
                args.max_flips_per_run,
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler=RECONCILER_NAME,
                reason="max_flips_exceeded",
                detected=n_upgrades,
                cap=args.max_flips_per_run,
            )
            return 2

        # Apply: stamp new capture_status + error_reason on the dataframe, upload the
        # upgraded rows back via the per-VM shard (consolidator merges last-writer-wins).
        # Two shapes: reason-upgrade (capture_status unchanged) and status-flip
        # (capture_status changes from empty_confirmed → attempted_failed).
        upgraded_idx = [entry["row_index"] for entry in upgrades]
        for entry in upgrades:
            df.at[entry["row_index"], "error_reason"] = entry["new_reason"]
            if entry["new_capture_status"] != entry["old_capture_status"]:
                df.at[entry["row_index"], "capture_status"] = entry["new_capture_status"]
        if "reconciler_run_id" in df.columns:
            for entry in upgrades:
                df.at[entry["row_index"], "reconciler_run_id"] = run_id

        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix=f"recon-legacy-typed-out-{args.asset_group}-",
            suffix=".parquet",
            delete=False,
        ) as tf:
            out_path = tf.name
        try:
            df.loc[upgraded_idx].to_parquet(out_path, index=False)
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(bucket_name)
            bucket.blob(per_vm_blob).upload_from_filename(out_path)
            logger.info("Uploaded per-VM shard to gs://%s/%s (%d upgraded rows)", bucket_name, per_vm_blob, n_upgrades)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(out_path)

        elapsed = time.time() - start
        logger.info(
            "Upgraded %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator merges within ~5min.",
            n_upgrades,
            elapsed,
            bucket_name,
            per_vm_blob,
        )
        _emit_event(
            "RECONCILER_COMPLETED",
            reconciler=RECONCILER_NAME,
            asset_group=args.asset_group,
            candidates=n_candidates,
            upgraded=n_upgrades,
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
