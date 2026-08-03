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

**Paused-consolidator CAS write** (codex SSOT
``/codex/05-infrastructure/manifest-consolidator-ssot.md`` § "Surgical ROW REMOVAL from the
canonical -- a paused-consolidator CAS drop, never a force-rebuild"): a plain read-modify-write
against `_index/availability_index.parquet` RACES the live `*/1 * * * *` consolidator cron --
proven live 2026-07-24, an --apply run reverted after >=2 consolidator cycles even though the
write itself succeeded and verified durable immediately. ``--apply`` therefore REFUSES unless the
caller has already paused the bucket's consolidator cron (``--i-have-paused-the-consolidator-cron``)
and confirmed no in-flight execution; it snapshots the pre-edit generation to
``_index/snapshots/pre_<slug>_<ts>.parquet``, edits at the Arrow level to preserve the EXACT source
schema, and writes back via a generation-match compare-and-set (``gcs_conditional_put``) that
ABORTS loudly on any concurrent write instead of silently losing the race. It then calls
``manifest_consolidator.consolidate(bucket, force=True)`` once to re-stamp the
``consolidator_content_write_at``/``consolidator_run_at`` markers the CAS write cannot carry
(the sanctioned CAS helpers take no ``metadata`` kwarg) -- skipping this step leaves a narrow
resurrection window on the cron's first post-edit cycle. The operator/caller MUST re-enable the
cron after verifying durability across >=2 cycles.

Usage:
  python scripts/flip_sports_odds_captured_leak_to_attempted_failed.py --dry-run
  # pause the cron + verify no in-flight execution FIRST, then:
  python scripts/flip_sports_odds_captured_leak_to_attempted_failed.py --apply --i-have-paused-the-consolidator-cron
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unified_trading_library import (
    gcs_conditional_put,
    gcs_read_object_with_generation,
    resolve_bucket_name,
)

# Not re-exported at the unified_trading_library top level (verified against
# __init__.py's __all__) -- the import-pattern checker's suggested shallow
# import for this symbol is a false positive; deep import is correct here.
from unified_trading_library.manifest_consolidator import consolidate  # noqa: qg-deep-import

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


def _snapshot_path(run_ts: str) -> str:
    return f"_index/snapshots/pre_sports_odds_captured_leak_{run_ts}.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without touching GCS.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="CAS-write the patched manifest back to GCS (after snapshot).",
    )
    parser.add_argument(
        "--i-have-paused-the-consolidator-cron",
        action="store_true",
        help=(
            "Required with --apply. Attests the caller already paused "
            "uts-prod-manifest-consolidator-instruments-sports-cron and confirmed no in-flight "
            "execution — see the manifest-consolidator-ssot.md paused-CAS recipe in the module "
            "docstring. --apply refuses without this flag."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.apply and not args.i_have_paused_the_consolidator_cron:
        logger.error(
            "--apply requires --i-have-paused-the-consolidator-cron — a plain read-modify-write "
            "races the live */1 consolidator cron and silently loses (proven live 2026-07-24). "
            "Pause uts-prod-manifest-consolidator-instruments-sports-cron, verify no in-flight "
            "execution, then re-run with the flag."
        )
        return 2

    bucket = resolve_bucket_name(
        cloud="gcp",
        kind="instruments-store",
        asset_group="sports",
        deployment_env="prod",
    )
    index_uri = f"gs://{bucket}/{_INDEX_PATH}"
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    snapshot_path = _snapshot_path(run_ts)

    logger.info("Reading manifest %s", index_uri)
    raw_bytes, generation = gcs_read_object_with_generation(index_uri)
    if raw_bytes is None:
        logger.error("Manifest object missing at %s — aborting.", index_uri)
        return 2
    source_table = pq.read_table(io.BytesIO(raw_bytes))
    df = source_table.to_pandas()
    logger.info("Manifest rows: %d (generation=%d)", len(df), generation)

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
        logger.info(
            "[dry-run] Would snapshot to gs://%s/%s then CAS-write generation=%d", bucket, snapshot_path, generation
        )
        return 0

    logger.info("Snapshotting pre-edit generation=%d to gs://%s/%s", generation, bucket, snapshot_path)
    snapshot_gen = gcs_conditional_put(f"gs://{bucket}/{snapshot_path}", raw_bytes, if_generation_match=0)
    if snapshot_gen is None:
        logger.error(
            "Snapshot write failed (object already exists at that path?) — aborting before touching canonical."
        )
        return 2

    df.loc[target_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    df.loc[target_mask, "instrument_count"] = 0
    df.loc[target_mask, "error_reason"] = _TARGET_ERROR_REASON
    if "row_count" in df.columns:
        df.loc[target_mask, "row_count"] = 0
    now_iso = datetime.now(UTC).isoformat()
    if "attempted_at" in df.columns:
        df.loc[target_mask, "attempted_at"] = now_iso

    # Arrow-level schema preservation (manifest-consolidator-ssot.md step 4): a plain
    # pandas round-trip can silently promote/alter dtypes (e.g. schema_version int64).
    # Rebuild against the SOURCE schema and assert equality before writing a single byte.
    new_table = pa.Table.from_pandas(df, schema=source_table.schema, preserve_index=False)
    if not new_table.schema.equals(source_table.schema):
        logger.error(
            "Post-edit schema does not match source schema — aborting rather than risk a schema "
            "regression. source=%s new=%s",
            source_table.schema,
            new_table.schema,
        )
        return 2

    out_buf = io.BytesIO()
    pq.write_table(new_table, out_buf)
    out_buf.seek(0)
    new_bytes = out_buf.read()

    logger.info("CAS-writing %s with if_generation_match=%d", index_uri, generation)
    new_generation = gcs_conditional_put(index_uri, new_bytes, if_generation_match=generation)
    if new_generation is None:
        logger.error(
            "CAS write REFUSED — generation drifted from %d (a concurrent writer landed). "
            "No canonical bytes were touched. Re-read the current state and re-run from scratch "
            "(do NOT retry blindly — re-verify the target rows first).",
            generation,
        )
        return 3
    logger.info(
        "Flipped %d rows to '%s'; manifest CAS-written (generation %d -> %d). Snapshot at gs://%s/%s",
        target_count,
        _TARGET_CAPTURE_STATUS,
        generation,
        new_generation,
        bucket,
        snapshot_path,
    )

    logger.info("Re-stamping consolidator markers via consolidate(bucket, force=True) (CAS write cannot carry them)")
    report = consolidate(bucket, force=True)
    logger.info("consolidate(force=True) report: %s", report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
