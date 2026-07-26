# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after flip confirmed via re-census + parent issue doc archived
"""Resolve the 4,996 phantom `capture_status=captured` FIXTURE_EVENTS manifest
rows (issues/sports_fixture_events_phantom_manifest_rows_2026_07_25.md).

Root cause (established by
`census_fixture_events_phantom_missing_2026_07_26.py`, an exhaustive 3-retry
existence check across all 3 candidate paths per row): these 4,996 rows'
``written_at`` timestamps cluster entirely within 2026-07-15..2026-07-25 —
ZERO overlap with the 38,264 genuinely-backed FIXTURE_EVENTS captured rows'
``written_at`` values. This is NOT the original 2019-era writer-generation bug
hypothesized in the issue doc; it is a manifest-only artifact of one of the
several 2026-07 sports manifest migration/reconcile passes (e.g. the CF11
api_football reconcile @ 2026-07-15, commit 87d1a353) that recorded a plausible
non-zero ``instrument_count`` without a paired successful GCS write for these
specific (date, league_id) cells. No archived process log or source dataframe
ties the count back to a real object, and this census independently re-confirms
absence at every candidate path — so recovery within this task is not possible
by direct verification; the honest-absence action per
`/codex/02-data/honest-absence-downstream-handling.md` is to flip these rows to
``attempted_failed`` (never leave them silently mis-marked ``captured``),
which re-opens them to the standard api_football per-fixture orchestrator's
normal re-fetch path (mirrors the sibling precedent
``flip_phantom_to_attempted_failed.py``).

Reads the exact (date, league_id) identity set from
``scripts/_fixture_events_phantom_missing_rows_2026_07_26.parquet`` (the
census's durable output) rather than re-deriving a heuristic mask — this is a
targeted flip of a known row set, not a fresh pattern-match.

Backup-then-write pattern. Idempotent — re-running after --apply finds 0 rows
(the exact-key mask no longer matches once capture_status is no longer
'captured').

Usage:
  python scripts/reflip_fixture_events_phantom_rows_2026_07_26.py --dry-run
  python scripts/reflip_fixture_events_phantom_rows_2026_07_26.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"
_MISSING_ROWS_PATH = "scripts/_fixture_events_phantom_missing_rows_2026_07_26.parquet"
_TARGET_CAPTURE_STATUS = "attempted_failed"
_TARGET_ERROR_REASON = "fixture_events_phantom_manifest_reflip_2026_07_26"


def _backup_path(run_ts: str) -> str:
    return f"_index/availability_index.{run_ts}.bak.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report what would change without touching GCS.")
    mode.add_argument("--apply", action="store_true", help="Write the patched manifest back to GCS (after backup).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    missing = pd.read_parquet(_MISSING_ROWS_PATH)
    target_keys = set(zip(missing["date"].astype(str), missing["league_id"].astype(str), strict=False))
    logger.info("Loaded %d target (date, league_id) rows from %s", len(target_keys), _MISSING_ROWS_PATH)

    storage = get_storage_client()
    logger.info("Reading manifest gs://%s/%s", _BUCKET, _INDEX_PATH)
    raw_bytes = storage.download_bytes(_BUCKET, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    required_cols = ("capture_status", "data_type", "date", "league_id")
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error("Manifest is missing required columns %s — aborting.", missing_cols)
        return 2

    fe_captured_mask = (df["data_type"].astype(str) == "FIXTURE_EVENTS") & (
        df["capture_status"].astype(str) == "captured"
    )
    row_keys = list(zip(df["date"].astype(str), df["league_id"].astype(str), strict=False))
    key_match_mask = pd.Series([k in target_keys for k in row_keys], index=df.index)
    target_mask = fe_captured_mask & key_match_mask
    target_count = int(target_mask.sum())
    logger.info("Rows matching target (date, league_id) set AND still capture_status=captured: %d", target_count)

    if target_count != len(target_keys):
        logger.warning(
            "Matched %d rows but loaded %d target keys — some target rows may have already been "
            "reconciled by another process, or a duplicate-key collapse occurred. Proceeding with the "
            "%d rows actually matched.",
            target_count,
            len(target_keys),
            target_count,
        )

    if target_count == 0:
        logger.info("Nothing to flip — manifest already reconciled. Exiting.")
        return 0

    if args.dry_run:
        logger.info(
            "[dry-run] Would flip %d rows: capture_status -> '%s', error_reason -> '%s'",
            target_count,
            _TARGET_CAPTURE_STATUS,
            _TARGET_ERROR_REASON,
        )
        by_year = df.loc[target_mask, "date"].astype(str).str[:4].value_counts().sort_index()
        logger.info("[dry-run] By year: %s", by_year.to_dict())
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = _backup_path(run_ts)
    logger.info("Writing backup to gs://%s/%s", _BUCKET, backup_path)
    storage.upload_bytes(_BUCKET, backup_path, raw_bytes)

    df.loc[target_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    df.loc[target_mask, "error_reason"] = _TARGET_ERROR_REASON
    if "attempted_at" in df.columns:
        df.loc[target_mask, "attempted_at"] = datetime.now(UTC).isoformat()

    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(_BUCKET, _INDEX_PATH, out_buf.read())
    logger.info(
        "Flipped %d rows to '%s'; manifest re-uploaded. Backup retained at gs://%s/%s",
        target_count,
        _TARGET_CAPTURE_STATUS,
        _BUCKET,
        backup_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
