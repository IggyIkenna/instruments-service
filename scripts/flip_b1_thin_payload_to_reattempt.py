#!/usr/bin/env python3
"""B.1 migration — flip FIXTURE_STATS / EVENTS / LINEUPS / INJURIES manifest rows.

Before flattening normalizers shipped (UAC@c76e6d0, 2026-05-13), the four
api_football per-fixture data types were written via stub pass-through:

    return {"league_id": ..., "data_type": ..., **raw}

PyArrow inconsistently handled the nested JSON, producing near-empty parquets
(no per-stat, per-event, per-lineup-slot columns). These thin-payload shards
must be re-fetched so the new normalizers produce the full flat schema.

This script:
1. Reads the canonical availability index (``_index/availability_index.parquet``).
2. Identifies ``capture_status='captured'`` rows for the 4 data types written
   before the normalizer ship date (``--cutoff-date``, default 2026-05-14).
3. Flips them to ``attempted_failed`` with
   ``error_reason='INCOMPLETE_PAYLOAD_PRE_FLATTENING'``.
4. Optionally deletes the corresponding thin parquets (``--delete-parquets``).

After running, launch a dedicated backfill VM::

    deployment-service/scripts/vm/launch-af-backfill-flatten.sh

which will re-fetch the 4 endpoints for all flipped (date, league) pairs.

Usage::

    # Dry-run — see what would change
    python scripts/flip_b1_thin_payload_to_reattempt.py \\
        --bucket gs://instruments-store-sports-prd-central-element-323112 \\
        --dry-run

    # Apply manifest flip only
    python scripts/flip_b1_thin_payload_to_reattempt.py \\
        --bucket gs://instruments-store-sports-prd-central-element-323112 \\
        --apply

    # Apply manifest flip + delete thin parquets
    python scripts/flip_b1_thin_payload_to_reattempt.py \\
        --bucket gs://instruments-store-sports-prd-central-element-323112 \\
        --apply --delete-parquets

Reference plan: ``plans/epics/sports_master.md`` § B.1.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_TARGET_CAPTURE_STATUS = "attempted_failed"
_TARGET_ERROR_REASON = "INCOMPLETE_PAYLOAD_PRE_FLATTENING"

# Normalizers shipped 2026-05-13 (UAC@c76e6d0). Only flip rows written BEFORE
# this date so we don't invalidate good data from the new normalizers.
_DEFAULT_CUTOFF_DATE = "2026-05-14"

# The 4 data types that used stub pass-through normalizers pre-UAC@c76e6d0.
_TARGET_DATA_TYPES: tuple[str, ...] = (
    "FIXTURE_STATS",
    "FIXTURE_EVENTS",
    "FIXTURE_LINEUPS",
    "INJURIES",
)

# Entity-name → parquet filename mapping for deletion.
_ENTITY_PARQUET: dict[str, str] = {
    "FIXTURE_STATS": "fixture_stats.parquet",
    "FIXTURE_EVENTS": "fixture_events.parquet",
    "FIXTURE_LINEUPS": "fixture_lineups.parquet",
    "INJURIES": "injuries.parquet",
}

# GCS prefix where per-fixture entity parquets live.
# Path pattern: sports_reference/by_date/day={day}/entity={entity}/league={league}/{filename}
_ENTITY_PREFIX = "sports_reference/by_date/"


def _bucket_name(bucket_arg: str) -> str:
    """Strip gs:// prefix if present."""
    return bucket_arg.removeprefix("gs://")


def _backup_path(run_ts: str) -> str:
    return f"_index/availability_index.{run_ts}.b1_flip.bak.parquet"


def _delete_parquets(
    storage_client: object,
    bucket: str,
    rows: pd.DataFrame,
    dry_run: bool,
    workers: int = 16,
) -> int:
    """Delete thin parquets for the flipped rows.

    Returns count of deleted (or would-delete) objects.
    """
    # Build GCS object paths from (date, data_type, league_id) triples.
    blobs_to_delete: list[str] = []
    for _, row in rows.iterrows():
        day = str(row.get("date", "")).strip()
        dt = str(row.get("data_type", "")).strip().upper()
        league = str(row.get("league_id", "")).strip()
        if not day or dt not in _ENTITY_PARQUET or not league:
            continue
        entity_slug = dt.lower().replace("_", "_")
        filename = _ENTITY_PARQUET[dt]
        # Canonical path: sports_reference/by_date/day={day}/entity={entity}/league={league}/{filename}
        blob = f"{_ENTITY_PREFIX}day={day}/entity={entity_slug}/league={league}/{filename}"
        blobs_to_delete.append(blob)

    if not blobs_to_delete:
        logger.info("No parquet paths to delete.")
        return 0

    logger.info(
        "%s %d thin parquets…",
        "[dry-run] Would delete" if dry_run else "Deleting",
        len(blobs_to_delete),
    )

    if dry_run:
        for blob in blobs_to_delete[:20]:
            logger.info("    [dry-run] gs://%s/%s", bucket, blob)
        if len(blobs_to_delete) > 20:
            logger.info("    … and %d more", len(blobs_to_delete) - 20)
        return len(blobs_to_delete)

    deleted = 0
    errors = 0

    def _delete_one(blob: str) -> bool:
        try:
            storage_client.delete_blob(bucket, blob)  # type: ignore[attr-defined]
            return True
        except Exception as exc:
            logger.warning("Failed to delete gs://%s/%s: %s", bucket, blob, exc)
            return False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_delete_one, b): b for b in blobs_to_delete}
        for fut in as_completed(futures):
            if fut.result():
                deleted += 1
            else:
                errors += 1

    logger.info("Deleted %d / %d parquets (%d errors).", deleted, len(blobs_to_delete), errors)
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket URI, e.g. gs://instruments-store-sports-prd-central-element-323112",
    )
    parser.add_argument(
        "--project-id",
        default="central-element-323112",
        help="GCP project id (default: central-element-323112).",
    )
    parser.add_argument(
        "--cutoff-date",
        default=_DEFAULT_CUTOFF_DATE,
        help=(
            f"Only flip rows where date < cutoff (default: {_DEFAULT_CUTOFF_DATE} — "
            "day after normalizers shipped UAC@c76e6d0 2026-05-13)."
        ),
    )
    parser.add_argument(
        "--delete-parquets",
        action="store_true",
        default=False,
        help="Also delete the thin parquets from GCS (requires --apply).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Parallel workers for parquet deletion (default 16).",
    )
    parser.add_argument(
        "--data-types",
        nargs="+",
        default=list(_TARGET_DATA_TYPES),
        metavar="DT",
        help=(
            "Data types to flip (default: FIXTURE_STATS FIXTURE_EVENTS FIXTURE_LINEUPS INJURIES). "
            "Override to target other types, e.g. --data-types STANDINGS PLAYER_VALUES."
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report changes without touching GCS.")
    mode.add_argument("--apply", action="store_true", help="Apply the manifest flip (and optional parquet deletion).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = _bucket_name(args.bucket)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = _backup_path(run_ts)

    storage = get_storage_client(project_id=args.project_id)
    logger.info("Reading manifest gs://%s/%s", bucket, _INDEX_PATH)
    raw_bytes = storage.download_bytes(bucket, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    required_cols = ("capture_status", "data_type")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error("Manifest missing required columns %s — aborting.", missing)
        return 2

    # Build flip mask: captured + target data_type + date < cutoff
    target_dts = tuple(dt.upper() for dt in args.data_types)
    captured_mask = df["capture_status"].astype(str) == "captured"
    dt_mask = df["data_type"].astype(str).isin(target_dts)

    # Date filter — only rows captured before the normalizer ship date.
    if "date" in df.columns:
        date_mask = pd.to_datetime(df["date"].astype(str), errors="coerce") < pd.Timestamp(args.cutoff_date)
    else:
        logger.warning("No 'date' column in manifest — skipping date filter (flipping ALL captured rows).")
        date_mask = pd.Series([True] * len(df))

    flip_mask = captured_mask & dt_mask & date_mask

    total = int(flip_mask.sum())
    logger.info(
        "Rows to flip (capture_status=captured, data_type in %s, date < %s): %d",
        target_dts,
        args.cutoff_date,
        total,
    )

    if total == 0:
        logger.info("Nothing to flip — manifest already canonical. Exiting.")
        return 0

    # Per-data-type breakdown
    by_dt = df.loc[flip_mask, "data_type"].value_counts()
    for dt_name, n in by_dt.items():
        logger.info("  %-25s %d rows", dt_name, n)

    if args.dry_run:
        logger.info(
            "[dry-run] Would flip %d rows → capture_status='%s' error_reason='%s'",
            total,
            _TARGET_CAPTURE_STATUS,
            _TARGET_ERROR_REASON,
        )
        if args.delete_parquets:
            _delete_parquets(storage, bucket, df.loc[flip_mask], dry_run=True, workers=args.workers)
        logger.info("[dry-run] Would write backup to gs://%s/%s", bucket, backup_path)
        return 0

    # Write backup before modifying
    logger.info("Writing backup to gs://%s/%s", bucket, backup_path)
    storage.upload_bytes(bucket, backup_path, raw_bytes)

    # Apply the flip
    df.loc[flip_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    df.loc[flip_mask, "error_reason"] = _TARGET_ERROR_REASON
    if "attempted_at" in df.columns:
        df.loc[flip_mask, "attempted_at"] = datetime.now(UTC).isoformat()

    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(bucket, _INDEX_PATH, out_buf.read())
    logger.info(
        "Flipped %d rows → '%s' / '%s'; manifest re-uploaded. Backup: gs://%s/%s",
        total,
        _TARGET_CAPTURE_STATUS,
        _TARGET_ERROR_REASON,
        bucket,
        backup_path,
    )

    if args.delete_parquets:
        _delete_parquets(storage, bucket, df.loc[flip_mask], dry_run=False, workers=args.workers)

    return 0


if __name__ == "__main__":
    sys.exit(main())
