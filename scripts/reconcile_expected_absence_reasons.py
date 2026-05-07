#!/usr/bin/env python3
"""reconcile_expected_absence_reasons.py — Layer 4 retrospective backfill.

Phase 3.D of the writegate honest-coverage umbrella plan
(``writegate_honest_coverage_endtoend_2026_05_06.plan.md``).

Walks an asset-group manifest, finds ``empty_confirmed`` rows with NULL
``error_reason`` (legacy bare ``record_empty()`` writes from before Phase
2.E.2 shipped 2026-05-07), classifies each via UAC SSOTs, and stamps a
typed reason from the closed set
``unified_api_contracts.canonical.crosscutting.honest_coverage.EMPTY_CONFIRMED_REASONS``.

Why this matters: per the codex SSOT
(``codex/02-data/honest-absence-downstream-handling.md`` §"Per-service
consumer-class audit"), downstream services MUST classify
``empty_confirmed`` rows by reason — execution skips, ML NaN-fills,
rolling-window denominator adjustment all branch on the reason. Legacy
null-reason rows are unclassifiable; this reconciler backfills them in
one pass per asset_group using the same SSOT helpers the writers use.

Per-asset-group classifier dispatch:

* **tradfi** — `non_trading_day_reason(venue, day)` returns
  EXPECTED_HOLIDAY / EXPECTED_WEEKEND for venues registered as
  weekday-only + US-market-holiday-bound. Trading days fall through to
  SOURCE_RETURNED_ZERO.
* **sports** — `is_in_known_gap(source, data_type, day)` returns
  EXPECTED_PAUSED_LEAGUE; `get_source_coverage_start(source, data_type)`
  + day-pre-cutoff check returns EXPECTED_PRE_SOURCE_COVERAGE_START.
  Else SOURCE_RETURNED_ZERO.
* **defi** — `get_chain_genesis_date(chain)` + day-pre-genesis check
  returns EXPECTED_PRE_GENESIS_CHAIN. Else SOURCE_RETURNED_ZERO.
* **cefi** — 24/7 markets; default SOURCE_RETURNED_ZERO unless the row
  carries a venue available_from earlier than the day (then
  EXPECTED_INSTRUMENT_NOT_LISTED).
* **prediction** — per-canonical-question lifecycle SSOT not yet
  populated (UAC ``PREDICTION_GROUPS = {}`` placeholder). Defer
  classification — just stamp SOURCE_RETURNED_ZERO so the row is no
  longer "unclassifiable", and flag the date-range for the predictions
  lifecycle plan to revisit.

**Default mode is SCAN-ONLY**: produces a CSV report of "would-stamp"
rows. ``--apply-flips`` is the explicit flag to actually mutate the
manifest. ``--max-flips-per-run`` default 100k halt safety; operator
confirms the first batch looks right before lifting the cap.

Example::

    # Scan only — produces /tmp/recon-reasons-sports-{ts}.csv
    python scripts/reconcile_expected_absence_reasons.py --asset-group sports

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=recon-sports-$(date +%s) \\
    python scripts/reconcile_expected_absence_reasons.py \\
        --asset-group sports --apply-flips --max-flips-per-run 50000

Workspace rules honoured:

* Per-VM shard write isolation (``MANIFEST_PER_VM_SHARDS=true`` +
  ``VM_NAME=...``) per the manifest concurrency principle. Without
  per-VM isolation a multi-VM reconciler run would clobber the
  canonical CAS.
* Run-lifecycle wrapper emits ``RECONCILER_STARTED`` /
  ``RECONCILER_PROGRESS`` / ``RECONCILER_COMPLETED`` /
  ``RECONCILER_FAILED`` events.
* CSV audit listing every stamped row (per-row: shard_key,
  classified_reason).
* No new parquets written — manifest column edit only. The on-disk
  parquets (or absence thereof) are the existing honest-empty state;
  this reconciler only adds classifier metadata to existing manifest
  rows.
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
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Asset-group → canonical manifest location.
ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
    "defi": f"market-data-tick-defi-{PROJECT_ID}",
    "tradfi": f"market-data-tick-tradfi-{PROJECT_ID}",
    "sports": f"instruments-store-sports-{PROJECT_ID}",
    "prediction": f"market-data-tick-prediction-{PROJECT_ID}",
}
MANIFEST_BLOB = "_index/availability_index.parquet"


def _emit_event(event: str, /, **details: object) -> None:
    """Emit a structured RECONCILER_* event line. Best-effort logging."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _classify_tradfi(row: pd.Series) -> str:
    """TradFi: weekend / US-market-holiday → EXPECTED_*; else SOURCE_RETURNED_ZERO."""
    from unified_api_contracts.registry import non_trading_day_reason

    venue = str(row.get("venue", "") or "").strip()
    day = str(row.get("date", "") or "").strip()
    if not venue or not day:
        return "SOURCE_RETURNED_ZERO"
    reason = non_trading_day_reason(venue, day)
    return reason or "SOURCE_RETURNED_ZERO"


def _classify_sports(row: pd.Series) -> str:
    """Sports: in-known-gap → PAUSED_LEAGUE; pre-source-coverage-start → PRE_*; else ZERO."""
    from unified_api_contracts.sports import (
        get_source_coverage_start,
        is_in_known_gap,
    )

    venue = str(row.get("venue", "") or "").strip().lower()
    data_type = str(row.get("data_type", "") or "").strip()
    day = str(row.get("date", "") or "").strip()
    if not day:
        return "SOURCE_RETURNED_ZERO"
    # ``venue`` for sports manifest rows holds the source key
    # (``api_football`` / ``footystats`` / ``soccer_football_info`` / etc.).
    source = venue
    if source and data_type and is_in_known_gap(source, data_type, day):
        return "EXPECTED_PAUSED_LEAGUE"
    if source:
        floor = get_source_coverage_start(source, data_type=data_type or None)
        if floor is not None and day < floor.isoformat():
            return "EXPECTED_PRE_SOURCE_COVERAGE_START"
    return "SOURCE_RETURNED_ZERO"


def _classify_defi(row: pd.Series) -> str:
    """DeFi: pre-chain-genesis → EXPECTED_PRE_GENESIS_CHAIN; else SOURCE_RETURNED_ZERO."""
    from unified_api_contracts.registry.chain_env import (
        get_chain_genesis_date,
    )

    chain = str(row.get("chain", "") or "").strip()
    venue = str(row.get("venue", "") or "").strip()
    day = str(row.get("date", "") or "").strip()
    if not day:
        return "SOURCE_RETURNED_ZERO"
    # chain may be the explicit ``chain`` column OR the suffix of a
    # legacy combined ``venue=PROTOCOL-CHAIN`` (pre-2026-04 DeFi paths).
    candidate_chain = chain or (venue.rsplit("-", 1)[-1] if "-" in venue else "")
    if candidate_chain:
        genesis = get_chain_genesis_date(candidate_chain)
        if genesis and day < genesis:
            return "EXPECTED_PRE_GENESIS_CHAIN"
    return "SOURCE_RETURNED_ZERO"


def _classify_cefi(_row: pd.Series) -> str:
    """CeFi: 24/7 markets, default SOURCE_RETURNED_ZERO.

    Future enhancement: cross-reference with instruments-service
    available_from_datetime per (venue, instrument_id) → emit
    EXPECTED_INSTRUMENT_NOT_LISTED for pre-listing days. Skipped in
    this initial pass to avoid an extra cross-bucket join; legacy CeFi
    null-reason empties are bound below 1% of total rows per phantom
    audit 2026-05-04, so SOURCE_RETURNED_ZERO is the safe default.
    """
    return "SOURCE_RETURNED_ZERO"


def _classify_prediction(_row: pd.Series) -> str:
    """Prediction: per-canonical-question lifecycle SSOT not yet populated.

    UAC ``PREDICTION_GROUPS = {}`` placeholder until the predictions
    canonical-question SSOT lands (predictions plan Phase 1A successor).
    Stamp SOURCE_RETURNED_ZERO so legacy rows are no longer
    unclassifiable, and the predictions plan can re-stamp
    EXPECTED_INSTRUMENT_DELISTED / EXPECTED_PRE_SOURCE_COVERAGE_START
    once the per-canonical-question lifecycle table lands.
    """
    return "SOURCE_RETURNED_ZERO"


CLASSIFIERS: dict[str, Callable[[pd.Series], str]] = {
    "tradfi": _classify_tradfi,
    "sports": _classify_sports,
    "defi": _classify_defi,
    "cefi": _classify_cefi,
    "prediction": _classify_prediction,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Classify legacy empty_confirmed manifest rows that have NULL "
            "error_reason. Writegate Phase 3.D — see module docstring."
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
        help="Default scan-only. Pass this flag to actually stamp reasons + upload manifest.",
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
        prefix=f"recon-reasons-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_null_reason_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask of empty_confirmed rows with no error_reason."""
    if "capture_status" not in df.columns:
        logger.warning("Manifest has no capture_status column — skipping classification.")
        return pd.Series([False] * len(df), index=df.index)
    empty_mask = df["capture_status"].fillna("") == "empty_confirmed"
    if "error_reason" not in df.columns:
        return empty_mask
    null_reason_mask = df["error_reason"].fillna("").astype(str).str.strip() == ""
    return empty_mask & null_reason_mask


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[args.asset_group]
    classifier = CLASSIFIERS[args.asset_group]
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"recon-reasons-{args.asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recon-reasons-{args.asset_group}-{run_ts}.csv"

    _emit_event(
        "RECONCILER_STARTED",
        reconciler="reconcile_expected_absence_reasons",
        asset_group=args.asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips_per_run=args.max_flips_per_run,
        run_id=run_id,
    )

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-flips requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation "
                "rule, codex/02-data/availability-manifest-and-data-status.md). Aborting."
            )
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_expected_absence_reasons",
                reason="missing_per_vm_shards_env",
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag> (per-VM shard isolation rule). Aborting.")
            _emit_event(
                "RECONCILER_FAILED",
                reconciler="reconcile_expected_absence_reasons",
                reason="missing_vm_name_env",
            )
            return 4

    start = time.time()
    df, local_manifest = _download_manifest(bucket_name, args.asset_group)
    try:
        mask = _build_null_reason_mask(df)
        n_candidates = int(mask.sum())
        logger.info(
            "Candidate rows (empty_confirmed AND null error_reason): %d / %d (%.2f%%)",
            n_candidates,
            len(df),
            (n_candidates / len(df) * 100) if len(df) else 0.0,
        )
        if n_candidates == 0:
            logger.info("Nothing to classify. Exiting.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler="reconcile_expected_absence_reasons",
                asset_group=args.asset_group,
                candidates=0,
                stamped=0,
            )
            return 0

        # Classify each candidate row.
        candidate_idx = df.index[mask]
        classifications: list[dict[str, str | int]] = []
        for idx in candidate_idx:
            row = df.loc[idx]
            try:
                reason = classifier(row)
            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                logger.warning("Classifier failed for row %s: %s — defaulting to ZERO", idx, exc)
                reason = "SOURCE_RETURNED_ZERO"
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
                    "classified_reason": reason,
                }
            )

        # Distribution summary for operator review.
        reason_counts: dict[str, int] = {}
        for entry in classifications:
            reason = str(entry["classified_reason"])
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        logger.info("Classification distribution:")
        for reason, count in sorted(reason_counts.items(), key=lambda kv: -kv[1]):
            logger.info("  %s: %d", reason, count)

        # CSV audit.
        cols = list(classifications[0].keys())
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols)
            writer.writeheader()
            writer.writerows(classifications)
        logger.info("Would-stamp report: %s (%d rows)", report_path, len(classifications))

        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler="reconcile_expected_absence_reasons",
            asset_group=args.asset_group,
            candidates=n_candidates,
            distribution=reason_counts,
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
                reconciler="reconcile_expected_absence_reasons",
                asset_group=args.asset_group,
                candidates=n_candidates,
                stamped=0,
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
                reconciler="reconcile_expected_absence_reasons",
                reason="max_flips_exceeded",
                detected=n_candidates,
                cap=args.max_flips_per_run,
            )
            return 2

        # Apply: stamp error_reason on the dataframe, upload back to canonical
        # manifest. Mirror reconcile_phantom_manifest_rows_all.py write
        # pattern (in-place df edit + upload), since per-row record_empty()
        # would re-emit thousands of CAS round-trips.
        for entry in classifications:
            df.at[entry["row_index"], "error_reason"] = entry["classified_reason"]
        # Stamp run_id audit metadata if column exists.
        if "reconciler_run_id" in df.columns:
            for entry in classifications:
                df.at[entry["row_index"], "reconciler_run_id"] = run_id

        # Write back via per-VM shard for atomicity (per-VM shard isolation
        # rule). The consolidator daemon merges per-VM shards into the
        # canonical manifest with last-writer-wins on identical row_key.
        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix=f"recon-reasons-out-{args.asset_group}-",
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
            "Stamped %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator "
            "will merge into canonical manifest within ~5min.",
            n_candidates,
            elapsed,
            bucket_name,
            per_vm_blob,
        )
        _emit_event(
            "RECONCILER_COMPLETED",
            reconciler="reconcile_expected_absence_reasons",
            asset_group=args.asset_group,
            candidates=n_candidates,
            stamped=n_candidates,
            distribution=reason_counts,
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
