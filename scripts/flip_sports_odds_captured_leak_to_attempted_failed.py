# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after flip confirmed durable (>=2 consolidator cycles) + parent plan archived
"""Correct 3 stale `captured` sports-odds manifest rows to `attempted_failed`.

Context (see
``unified-trading-pm/plans/active/issues/sports_odds_manifest_captured_outranks_blocks_legacy_leak_correction_2026_07_24.md``):
``reprocess_sports_odds.py --force`` correctly re-classified 2025-12-18,
2025-12-24, and 2025-12-31 as ``attempted_failed`` (``ADAPTER_RETURNED_EMPTY_OUTPUT``
-- raw odds exist but every observation falls in the T-12h/T-24h dead-zone, so
the bucketed frame is genuinely empty for all horizons that day) and wrote that
verdict via ``ManifestWriter.record_failed()``. But the write never durably
surfaces: ``_merge_shard_frames``'s captured-outranks-recency tie-break
(``unified-trading-library@17ee38de``) ranks ANY ``capture_status='captured'``
row above ANY non-captured row for the same dedup key, unconditionally --
regardless of recency. Because the CONSOLIDATED index
(``_index/availability_index.parquet``) still carries the original (buggy)
``captured`` rows for these 3 dates -- both the coarse per-day key
(``league_id``/``timeframe`` blank) and several per-league ``T-0`` shard keys,
all dated 2026-07-14T04:1x:xx -- the consolidator's next incremental cycle
re-merges (stale canonical `captured` row) + (new shard `attempted_failed`
row) and the tie-break makes the stale `captured` row win again every time.

This is a **backup-then-write** direct hand-edit of the CONSOLIDATED index
(mirrors ``instruments-service/scripts/flip_phantom_to_attempted_failed.py``'s
pattern) -- the only way to correct a `captured` row the reader/consolidator
tie-break otherwise protects unconditionally. Targets rows currently
``capture_status='captured'`` for ``venue=ODDS_API`` /
``data_type=odds_horizon_bucket`` on the 3 target dates **written by
``market-data-processing-service``** (``reprocess_sports_odds.py``'s own
``_SERVICE_NAME``) -- the coarse per-day key AND the per-league ``T-0`` shard
keys from that SAME buggy run, all dated 2026-07-14T04:1x:xx.

IMPORTANT -- there IS a legitimate-vs-leaked split within the (venue,
data_type, date, league_id) key space: a live dry-run against PROD
(2026-07-24) found 56 EXTRA rows sharing these dates/venue/data_type but
written by ``market-tick-data-service`` (2026-07-13T06:1x, uppercase
``league_id``, blank ``timeframe``, ``instrument_count=1``) and
``instruments-service`` (2026-07-13T23:4x-23:5x, uppercase ``league_id``,
literal-string ``timeframe='None'``, ``instrument_count=1``) -- a day EARLIER
and from services unrelated to the MDPS odds-bucketing pipeline this
correction targets. Those are NOT part of this leak and must NOT be touched;
the ``service_name`` filter below exists specifically to exclude them.

Idempotent -- re-running after --apply finds 0 rows (already corrected).

Usage:
  python scripts/flip_sports_odds_captured_leak_to_attempted_failed.py --dry-run
  python scripts/flip_sports_odds_captured_leak_to_attempted_failed.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"

# The 3 dates the parent audit's reprocess run confirmed are genuinely
# adapter-empty across every horizon (T-12h/T-24h structural dead-zone, all
# raw observations for the day fall inside it).
_TARGET_DATES: tuple[str, ...] = ("2025-12-18", "2025-12-24", "2025-12-31")

# Same (venue, data_type, service_name) reprocess_sports_odds.py itself
# writes under -- see its ``_MANIFEST_VENUE`` / ``_MANIFEST_DATA_TYPE`` /
# ``_SERVICE_NAME`` constants. service_name is REQUIRED: market-tick-data-service
# and instruments-service also write captured rows sharing this same
# (venue, data_type, date, league_id) key space for an unrelated purpose (see
# module docstring) -- omitting this filter would wrongly flip 56 rows that
# are not part of this leak (verified live against PROD 2026-07-24).
_TARGET_VENUE = "ODDS_API"
_TARGET_DATA_TYPE = "odds_horizon_bucket"
_TARGET_SERVICE_NAME = "market-data-processing-service"

_TARGET_CAPTURE_STATUS = "attempted_failed"
_TARGET_ERROR_REASON = "legacy_captured_leak_corrected_per_sports_odds_manifest_captured_outranks_2026_07_24"

# Every row this script targets was originally written 2026-07-14T04:1x:xx
# per the issue doc's own read. Not an exclusion filter (day-level adapter
# failure makes ANY currently-captured row for these keys wrong regardless of
# write date) -- just an audit signal if a match surprises that assumption.
_EXPECTED_LEAK_WRITE_DATE_PREFIX = "2026-07-14"


def _backup_path(run_ts: str) -> str:
    return f"_index/availability_index.{run_ts}.bak.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-id",
        default="central-element-323112",
        help="GCP project id (default: central-element-323112).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without touching GCS.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Write the patched manifest back to GCS (after backup).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = resolve_bucket_name(
        cloud="gcp",
        kind="instruments-store",
        asset_group="sports",
        deployment_env="prod",
    )
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = _backup_path(run_ts)

    storage = get_storage_client(project_id=args.project_id)
    logger.info("Reading manifest gs://%s/%s", bucket, _INDEX_PATH)
    raw_bytes = storage.download_bytes(bucket, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    required_cols = ("capture_status", "venue", "data_type", "date", "instrument_count", "error_reason", "service_name")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error("Manifest is missing required columns %s — aborting.", missing)
        return 2

    target_mask = (
        (df["capture_status"].astype(str) == "captured")
        & (df["venue"].astype(str) == _TARGET_VENUE)
        & (df["data_type"].astype(str) == _TARGET_DATA_TYPE)
        & (df["service_name"].astype(str) == _TARGET_SERVICE_NAME)
        & (df["date"].astype(str).isin(_TARGET_DATES))
    )
    target_count = int(target_mask.sum())
    logger.info(
        "Rows to flip (venue=%s data_type=%s service_name=%s date in %s, capture_status='captured'): %d",
        _TARGET_VENUE,
        _TARGET_DATA_TYPE,
        _TARGET_SERVICE_NAME,
        _TARGET_DATES,
        target_count,
    )

    if target_count > 0:
        breakdown = (
            df.loc[target_mask]
            .groupby(
                [
                    "date",
                    df.get("league_id", pd.Series(dtype=str)).astype(str),
                    df.get("timeframe", pd.Series(dtype=str)).astype(str),
                ],
                dropna=False,
            )
            .size()
        )
        for (date_val, league_val, timeframe_val), n in breakdown.items():
            key_desc = f"league_id={league_val or '<coarse>'} timeframe={timeframe_val or '<coarse>'}"
            logger.info("    %s  %s  rows=%d", date_val, key_desc, n)

        ts_col = (
            "attempted_at" if "attempted_at" in df.columns else "written_at" if "written_at" in df.columns else None
        )
        if ts_col is not None:
            surprising = df.loc[target_mask & ~df[ts_col].astype(str).str.startswith(_EXPECTED_LEAK_WRITE_DATE_PREFIX)]
            if not surprising.empty:
                logger.warning(
                    "%d matched row(s) carry a %s NOT starting with %s — verify these are genuinely part of "
                    "the same leak before proceeding.",
                    len(surprising),
                    ts_col,
                    _EXPECTED_LEAK_WRITE_DATE_PREFIX,
                )

    if target_count == 0:
        logger.info("Nothing to flip — manifest already canonical. Exiting.")
        return 0

    if args.dry_run:
        logger.info(
            "[dry-run] Would flip %d rows: capture_status -> '%s', instrument_count -> 0, error_reason -> '%s'",
            target_count,
            _TARGET_CAPTURE_STATUS,
            _TARGET_ERROR_REASON,
        )
        logger.info("[dry-run] Would write backup to gs://%s/%s", bucket, backup_path)
        return 0

    logger.info("Writing backup to gs://%s/%s", bucket, backup_path)
    storage.upload_bytes(bucket, backup_path, raw_bytes)

    df.loc[target_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    df.loc[target_mask, "instrument_count"] = 0
    df.loc[target_mask, "error_reason"] = _TARGET_ERROR_REASON
    if "row_count" in df.columns:
        df.loc[target_mask, "row_count"] = 0
    now_iso = datetime.now(UTC).isoformat()
    if "attempted_at" in df.columns:
        df.loc[target_mask, "attempted_at"] = now_iso

    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(bucket, _INDEX_PATH, out_buf.read())
    logger.info(
        "Flipped %d rows to '%s'; manifest re-uploaded. Backup retained at gs://%s/%s",
        target_count,
        _TARGET_CAPTURE_STATUS,
        bucket,
        backup_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
