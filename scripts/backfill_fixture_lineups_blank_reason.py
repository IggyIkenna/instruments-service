#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + blank_reason FIXTURE_LINEUPS count = 0 in consolidated _index
"""backfill_fixture_lineups_blank_reason.py — Backfill typed error_reason for FIXTURE_LINEUPS blank-reason rows.

Fixes the ~5,690 of 7,427 golden-window FIXTURE_LINEUPS empty_confirmed manifest rows
that carry a blank/null error_reason (legacy rows that escaped the typed-reason gate).

Classification logic (per sports_league_entity_coverage registry):
  - is_league_entity_covered(league_id, "FIXTURE_LINEUPS") == False
    → EXPECTED_NO_PROVIDER_COVERAGE (API-Football never yields lineups for this league)
  - is_league_entity_covered(league_id, "FIXTURE_LINEUPS") == True
    → SOURCE_RETURNED_ZERO (in coverage but source returned zero — fetch happened)

Default mode is SCAN-ONLY (produces CSV audit). Pass --apply-flips to write
via per-VM shard (requires MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique>).

Example::
    # Scan only
    python scripts/backfill_fixture_lineups_blank_reason.py

    # Apply
    MANIFEST_PER_VM_SHARDS=true VM_NAME=backfill-fl-blank-$(date +%s) \\
    python scripts/backfill_fixture_lineups_blank_reason.py --apply-flips
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import logging
import os
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import storage
from unified_api_contracts.registry.sports_league_entity_coverage import is_league_entity_covered

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("backfill_fixture_lineups_blank_reason")

PROJECT_ID = "central-element-323112"
SPORTS_BUCKET = "instruments-store-sports-central-element-323112"
MANIFEST_BLOB = "_index/availability_index.parquet"
RECONCILER_NAME = "backfill_fixture_lineups_blank_reason"
DATA_TYPE = "FIXTURE_LINEUPS"
TARGET_ENTITY = "FIXTURE_LINEUPS"


def _emit_event(event: str, **details: Any) -> None:
    payload: dict[str, Any] = {
        "event": event,
        "ts": datetime.now(UTC).isoformat(),
        **details,
    }
    logger.info("%s", json.dumps(payload))


def _download_manifest(bucket_name: str) -> tuple[pd.DataFrame, str]:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix="manifest-sports-fl-blank-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Downloaded manifest: %d rows from gs://%s/%s", len(df), bucket_name, MANIFEST_BLOB)
    return df, local_path


def _build_candidate_mask(df: pd.DataFrame) -> pd.Series:
    if "capture_status" not in df.columns or "error_reason" not in df.columns:
        logger.warning("Manifest missing capture_status / error_reason — nothing to backfill.")
        return pd.Series([False] * len(df), index=df.index)
    empty_mask = df["capture_status"].fillna("").astype(str).str.strip() == "empty_confirmed"
    dtype_mask = df["data_type"].fillna("").astype(str).str.strip() == DATA_TYPE
    blank_mask = df["error_reason"].fillna("").astype(str).str.strip() == ""
    return empty_mask & dtype_mask & blank_mask


def _classify_row(league_id: str) -> str:
    if is_league_entity_covered(str(league_id), TARGET_ENTITY):
        return "SOURCE_RETURNED_ZERO"
    return "EXPECTED_NO_PROVIDER_COVERAGE"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill typed error_reason for FIXTURE_LINEUPS blank-reason empty_confirmed rows."
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        default=False,
        help="Write upgraded rows to per-VM shard (requires MANIFEST_PER_VM_SHARDS=true + VM_NAME).",
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=100_000,
        help="Safety cap — abort if candidate count exceeds this (default 100000).",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Directory for CSV audit report (default: tempfile.gettempdir()).",
    )
    args = parser.parse_args()

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-flips requires MANIFEST_PER_VM_SHARDS=true (set in env) to prevent "
                "accidental writes without per-VM shard isolation."
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag> (set in env) for per-VM shard path.")
            return 4

    run_id = str(uuid.uuid4())
    start = time.time()
    _emit_event(
        "RECONCILER_STARTED",
        reconciler=RECONCILER_NAME,
        data_type=DATA_TYPE,
        apply=args.apply_flips,
        run_id=run_id,
    )

    local_manifest = ""
    try:
        df, local_manifest = _download_manifest(SPORTS_BUCKET)
        candidate_mask = _build_candidate_mask(df)
        n_candidates = int(candidate_mask.sum())
        logger.info("Candidate blank-reason FIXTURE_LINEUPS empty_confirmed rows: %d", n_candidates)

        if n_candidates == 0:
            logger.info("No blank-reason rows found — nothing to do.")
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                data_type=DATA_TYPE,
                candidates=0,
                upgraded=0,
                run_id=run_id,
                elapsed_s=round(time.time() - start, 1),
            )
            return 0

        if n_candidates > args.max_flips_per_run:
            logger.error(
                "Candidate count %d exceeds --max-flips-per-run=%d — aborting (safety cap).",
                n_candidates,
                args.max_flips_per_run,
            )
            return 3

        candidate_df = df[candidate_mask].copy()
        upgrades: list[dict[str, Any]] = []
        reason_counts: dict[str, int] = {}

        for idx, row in candidate_df.iterrows():
            league_id = str(row.get("league_id", "")).strip()
            new_reason = _classify_row(league_id)
            upgrades.append(
                {
                    "row_index": idx,
                    "league_id": league_id,
                    "date": str(row.get("date", "")),
                    "new_reason": new_reason,
                }
            )
            reason_counts[new_reason] = reason_counts.get(new_reason, 0) + 1

        n_upgrades = len(upgrades)
        logger.info("Classification complete: %s", reason_counts)
        _emit_event(
            "RECONCILER_PROGRESS",
            reconciler=RECONCILER_NAME,
            data_type=DATA_TYPE,
            candidates=n_candidates,
            classified=n_upgrades,
            reason_counts=reason_counts,
            run_id=run_id,
        )

        report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"backfill_fixture_lineups_blank_reason_{run_id[:8]}.csv"
        with report_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["row_index", "league_id", "date", "new_reason"])
            writer.writeheader()
            writer.writerows(upgrades)
        logger.info("Audit CSV written: %s (%d rows)", report_path, n_upgrades)

        if not args.apply_flips:
            logger.info(
                "SCAN-ONLY mode — %d rows would be upgraded. Re-run with --apply-flips to write.",
                n_upgrades,
            )
            _emit_event(
                "RECONCILER_COMPLETED",
                reconciler=RECONCILER_NAME,
                data_type=DATA_TYPE,
                candidates=n_candidates,
                upgraded=0,
                reason_counts=reason_counts,
                report_path=str(report_path),
                dry_run=True,
                run_id=run_id,
                elapsed_s=round(time.time() - start, 1),
            )
            return 0

        upgraded_idx = [entry["row_index"] for entry in upgrades]
        for entry in upgrades:
            df.at[entry["row_index"], "error_reason"] = entry["new_reason"]
        if "reconciler_run_id" in df.columns:
            for entry in upgrades:
                df.at[entry["row_index"], "reconciler_run_id"] = run_id

        vm_name = os.environ["VM_NAME"]
        per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
        with tempfile.NamedTemporaryFile(
            prefix="backfill-fl-blank-out-",
            suffix=".parquet",
            delete=False,
        ) as tf:
            out_path = tf.name
        try:
            df.loc[upgraded_idx].to_parquet(out_path, index=False)
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(SPORTS_BUCKET)
            bucket.blob(per_vm_blob).upload_from_filename(out_path)
            logger.info(
                "Uploaded per-VM shard to gs://%s/%s (%d rows)",
                SPORTS_BUCKET,
                per_vm_blob,
                n_upgrades,
            )
        finally:
            with contextlib.suppress(OSError):
                os.unlink(out_path)

        elapsed = time.time() - start
        logger.info(
            "Upgraded %d rows in %.1fs; per-VM shard at gs://%s/%s. Consolidator merges within ~5min.",
            n_upgrades,
            elapsed,
            SPORTS_BUCKET,
            per_vm_blob,
        )
        _emit_event(
            "RECONCILER_COMPLETED",
            reconciler=RECONCILER_NAME,
            data_type=DATA_TYPE,
            candidates=n_candidates,
            upgraded=n_upgrades,
            reason_counts=reason_counts,
            report_path=str(report_path),
            per_vm_blob=per_vm_blob,
            run_id=run_id,
            elapsed_s=round(elapsed, 1),
        )
        return 0

    except Exception as exc:
        logger.exception("RECONCILER_FAILED: %s", exc)
        _emit_event(
            "RECONCILER_FAILED",
            reconciler=RECONCILER_NAME,
            data_type=DATA_TYPE,
            error=str(exc),
            run_id=run_id,
            elapsed_s=round(time.time() - start, 1),
        )
        return 1
    finally:
        with contextlib.suppress(OSError):
            if local_manifest:
                os.unlink(local_manifest)


if __name__ == "__main__":
    raise SystemExit(main())
