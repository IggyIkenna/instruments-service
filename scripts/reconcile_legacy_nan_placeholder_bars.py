#!/usr/bin/env python3
"""reconcile_legacy_nan_placeholder_bars.py — Phase 3C audit reconciler.

Scans the availability manifest for ``capture_status=captured`` rows where
the corresponding parquet slice consists entirely of NaN OHLC values — the
"silent garbage" pattern from the 2026-05-05 MDPS incident where 1440
placeholder bars were written per (venue, data_type, day) with all-NaN fields
but the manifest said ``captured``.

Reclassification rule (per CLAUDE.md four-category empty-output decision):

* Row sample has **≥ NAN_RATIO_THRESHOLD** NaN ratio for OHLC columns
  → ``capture_status=attempted_failed`` with ``error_reason="LEGACY_NAN_PLACEHOLDER"``.
  The intent: next VM run re-attempts the shard; a clean upstream fetch will
  either produce real data or correctly record ``empty_confirmed`` / ``attempted_failed``.

* Row sample has **< NAN_RATIO_THRESHOLD** NaN ratio (real data present)
  → skip (false positive).

The script does NOT flip rows to Category D (zero-activity bar). Zero-activity
bar conversion requires writing a new parquet with O=H=L=C=prior_LTP, volume=0
— that is the adapter's responsibility on re-run, not the reconciler's.

**Default mode: SCAN-ONLY.** Produces a CSV report at
``{tempfile.gettempdir()}/recon-nan-{asset_group}-{ts}.csv``.
Pass ``--apply-flips`` to actually mutate the manifest (requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``).

Note: Phase 3A audit (2026-05-12) found all 18 currently-implemented CeFi
venues compliant (Category A with typed reasons, no NaN placeholders in new
writes). This reconciler targets historical rows written before the writegate
Wave 2.M + Wave 3.M fixes landed.

Supported asset_groups: cefi only (Phase 3 scope; other groups out of scope
for this plan per catalogue_audit_2026_05_10.md Phase 3).

execution:
  owner: Cron VM or manual operator run on same-region GCE VM
  cadence: one-shot (run once after writegate Wave 3.M ships; re-run monthly
           until NaN placeholder count reaches 0)
  verifier: gs://market-data-tick-cefi-central-element-323112/_index/availability_index.parquet
            sample: confirmed LEGACY_NAN_PLACEHOLDER rows = 0 post-apply
  last_executed: NEVER

Reference: cross_asset_group_catalogue_audit_2026_05_10.md Phase 3C.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

ASSET_GROUP_BUCKETS: dict[str, str] = {
    "cefi": f"market-data-tick-cefi-{PROJECT_ID}",
}

MANIFEST_BLOB = "_index/availability_index.parquet"

NAN_RATIO_THRESHOLD = 0.95

OHLC_COLUMNS = ("open", "high", "low", "close")

MAX_SAMPLE_ROWS = 200


def _emit_event(event: str, /, **details: object) -> None:
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan / reconcile manifest rows where capture_status=captured "
            "but parquet has all-NaN OHLC bars. Phase 3C of "
            "cross_asset_group_catalogue_audit_2026_05_10.md."
        ),
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(ASSET_GROUP_BUCKETS.keys()),
        help="Asset group manifest to scan.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override canonical bucket (default: per-asset-group SSOT).",
    )
    parser.add_argument(
        "--apply-flips",
        action="store_true",
        help="Default scan-only. Pass to actually flip + upload manifest.",
    )
    parser.add_argument(
        "--max-flips-per-run",
        type=int,
        default=50_000,
        help="Halt-safety cap (default 50k).",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=MAX_SAMPLE_ROWS,
        help="Max parquet rows to read per shard when probing for NaN ratio.",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report dir (default: tempfile.gettempdir()).",
    )
    parser.add_argument(
        "--date-from",
        default=None,
        help="ISO YYYY-MM-DD — only scan rows on or after this date.",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="ISO YYYY-MM-DD — only scan rows on or before this date.",
    )
    return parser.parse_args()


def _download_manifest(bucket_name: str, asset_group: str) -> tuple[pd.DataFrame, str]:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix=f"recon-nan-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _filter_candidate_rows(
    df: pd.DataFrame,
    date_from: str | None,
    date_to: str | None,
) -> pd.DataFrame:
    """Return captured rows within optional date window."""
    if "capture_status" not in df.columns:
        logger.warning("No capture_status column — manifest too old to scan.")
        return df.iloc[0:0]
    mask = df["capture_status"] == "captured"
    if "date" in df.columns:
        if date_from:
            mask = mask & (df["date"].astype(str) >= date_from)
        if date_to:
            mask = mask & (df["date"].astype(str) <= date_to)
    return df[mask].copy()


def _iter_parquet_paths(
    bucket_name: str,
    candidates: pd.DataFrame,
) -> Iterator[tuple[int, str]]:
    """Yield (original_index, gcs_path) for each candidate row.

    Constructs the parquet path from hive-partition columns in the manifest.
    Falls back to the ``parquet_path`` column if present.
    """
    for idx, row in candidates.iterrows():
        if "parquet_path" in candidates.columns and pd.notna(row.get("parquet_path")):
            yield int(idx), str(row["parquet_path"])
            continue
        parts: list[str] = []
        for col in ("asset_group", "venue", "data_type", "date"):
            val = row.get(col)
            if pd.notna(val):
                parts.append(f"{col}={val}")
        if not parts:
            continue
        yield int(idx), f"gs://{bucket_name}/" + "/".join(parts) + "/data.parquet"


def _compute_nan_ratio(
    gcs_path: str,
    sample_limit: int,
) -> float | None:
    """Return OHLC NaN ratio for the parquet shard; None if unreadable."""
    try:
        df = pd.read_parquet(gcs_path)
        if df.empty:
            return 1.0
        sample = df.head(sample_limit)
        present = [c for c in OHLC_COLUMNS if c in sample.columns]
        if not present:
            return None
        total = len(sample) * len(present)
        if total == 0:
            return 1.0
        nan_count = sum(int(sample[c].isna().sum()) for c in present)
        return nan_count / total
    except Exception as exc:
        logger.debug("Could not read %s: %s", gcs_path, exc)
        return None


def _write_manifest(
    bucket_name: str,
    df: pd.DataFrame,
    local_path: str,
) -> None:
    df.to_parquet(local_path, index=False)
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    blob.upload_from_filename(local_path)
    logger.info("Uploaded updated manifest to gs://%s/%s", bucket_name, MANIFEST_BLOB)


def main() -> int:
    args = _parse_args()
    bucket_name: str = args.bucket or ASSET_GROUP_BUCKETS[args.asset_group]
    asset_group: str = args.asset_group
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"recon-nan-{asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recon-nan-{asset_group}-{run_ts}.csv"

    _emit_event(
        "RECONCILER_STARTED",
        reconciler="reconcile_legacy_nan_placeholder_bars",
        asset_group=asset_group,
        bucket=bucket_name,
        apply_flips=args.apply_flips,
        max_flips_per_run=args.max_flips_per_run,
        run_id=run_id,
    )

    if args.apply_flips:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error("--apply-flips requires MANIFEST_PER_VM_SHARDS=true.")
            _emit_event("RECONCILER_FAILED", reason="missing_per_vm_shards_env")
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-flips requires VM_NAME=<unique-tag>.")
            _emit_event("RECONCILER_FAILED", reason="missing_vm_name_env")
            return 4

    df, local_path = _download_manifest(bucket_name, asset_group)
    candidates = _filter_candidate_rows(df, args.date_from, args.date_to)
    logger.info(
        "Candidate captured rows in date window: %d (of %d total)",
        len(candidates),
        len(df),
    )

    flip_indices: list[int] = []
    rows_scanned = 0
    rows_probed = 0

    with open(report_path, "w", newline="") as csvf:
        writer = csv.writer(csvf)
        writer.writerow(
            [
                "manifest_index",
                "asset_group",
                "venue",
                "data_type",
                "date",
                "parquet_path",
                "nan_ratio",
                "action",
            ]
        )

        for orig_idx, gcs_path in _iter_parquet_paths(bucket_name, candidates):
            rows_scanned += 1
            if rows_scanned % 1000 == 0:
                logger.info(
                    "Progress: scanned=%d probed=%d flip_candidates=%d",
                    rows_scanned,
                    rows_probed,
                    len(flip_indices),
                )

            row = candidates.loc[orig_idx]
            nan_ratio = _compute_nan_ratio(gcs_path, args.sample_limit)
            rows_probed += 1

            if nan_ratio is None:
                action = "unreadable"
            elif nan_ratio >= NAN_RATIO_THRESHOLD:
                action = "flip_to_attempted_failed"
                flip_indices.append(orig_idx)
            else:
                action = "skip_real_data"

            writer.writerow(
                [
                    orig_idx,
                    row.get("asset_group", ""),
                    row.get("venue", ""),
                    row.get("data_type", ""),
                    row.get("date", ""),
                    gcs_path,
                    f"{nan_ratio:.4f}" if nan_ratio is not None else "n/a",
                    action,
                ]
            )

            if len(flip_indices) >= args.max_flips_per_run:
                logger.warning(
                    "max_flips_per_run=%d reached — stopping scan early. "
                    "Re-run with a narrower date window or raise the cap after review.",
                    args.max_flips_per_run,
                )
                break

    logger.info(
        "Scan complete: scanned=%d probed=%d flip_candidates=%d report=%s",
        rows_scanned,
        rows_probed,
        len(flip_indices),
        report_path,
    )
    _emit_event(
        "RECONCILER_SCAN_COMPLETE",
        rows_scanned=rows_scanned,
        rows_probed=rows_probed,
        flip_candidates=len(flip_indices),
        report=str(report_path),
    )

    if not args.apply_flips:
        logger.info(
            "Scan-only mode. Review %s then re-run with --apply-flips to mutate.",
            report_path,
        )
        return 0

    if not flip_indices:
        logger.info("No NaN-placeholder rows found — nothing to flip.")
        _emit_event("RECONCILER_FINISHED", flipped=0)
        return 0

    logger.info("Applying %d flips to manifest…", len(flip_indices))
    now_ts = datetime.now(UTC).isoformat()
    df.loc[flip_indices, "capture_status"] = "attempted_failed"
    df.loc[flip_indices, "error_reason"] = "LEGACY_NAN_PLACEHOLDER"
    if "reconciled_at" not in df.columns:
        df["reconciled_at"] = None
    df.loc[flip_indices, "reconciled_at"] = now_ts

    _write_manifest(bucket_name, df, local_path)

    _emit_event(
        "RECONCILER_FINISHED",
        flipped=len(flip_indices),
        bucket=bucket_name,
    )
    logger.info(
        "Done. Flipped %d rows to attempted_failed. Manifest updated at gs://%s/%s",
        len(flip_indices),
        bucket_name,
        MANIFEST_BLOB,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
