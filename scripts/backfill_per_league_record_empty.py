#!/usr/bin/env python3
"""Manifest-only backfill of per-(date, league) record_empty rows.

Adapter fix on 2026-05-06 made the footystats matches and transfermarkt
player-values adapters emit ``record_empty`` per-(date, league) for every
expected league with no data on a given date — mirroring the pattern XG /
SFI_PROGRESSIVE_STATS already use. Going forward the live orchestrator + future
backfill VMs write these rows on every attempt.

This script repairs the *historical* manifest where past adapter runs only
emitted captured rows (or a single date-aggregate empty row). For each
``(date, league_id)`` shard the data-status reconciler expects but the manifest
currently lacks, write a ``record_empty`` row attributed to today's UTC time.

No API calls are made — we know from the manifest's existing captured rows
that the historical fetch already happened. The missing leagues on captured
dates are by construction empty (the adapter would have written a row had the
source returned any data). Date-aggregate empty rows are kept as-is.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_per_league_record_empty.py --dry-run
    .venv/bin/python scripts/backfill_per_league_record_empty.py --data-types MATCHES
    .venv/bin/python scripts/backfill_per_league_record_empty.py            # full run
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime
from datetime import date as date_type

import pandas as pd
from google.cloud import storage
from unified_api_contracts.canonical.domain.sports.league_data import (
    SOURCE_COVERAGE_START,
    clip_dates_to_source_coverage,
    get_expected_leagues_for_source,
    get_prediction_leagues,
    is_in_known_gap,
)
from unified_trading_library import ManifestWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
CANONICAL_INDEX = "_index/availability_index.parquet"

# (data_type, source_key, [classifications]) — drives league set + denominator clip.
TARGETS: list[tuple[str, str, list[str]]] = [
    ("MATCHES", "footystats", ["Prediction", "Features"]),
    ("PLAYER_VALUES", "transfermarkt", ["Prediction", "Features"]),
]


def _read_canonical_manifest() -> pd.DataFrame:
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(CANONICAL_INDEX)
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    logger.info("Read manifest: %d rows", len(df))
    return df


def _expected_leagues_with_prediction_merge(source: str, classifications: list[str]) -> list[str]:
    """Mirror the orchestrator's ``_merged_leagues`` build for transfermarkt."""
    expected = {lg.league_id for lg in get_expected_leagues_for_source(source, classifications=classifications)}
    if source == "transfermarkt":
        # Orchestrator merges prediction leagues into the transfermarkt expected set.
        for lg in get_prediction_leagues():
            expected.add(lg.league_id)
    return sorted(expected)


def _expected_dates(source: str, data_type: str, end_iso: str) -> list[str]:
    """All dates from source coverage start (data-type override aware) through end."""
    start_iso = SOURCE_COVERAGE_START.get(source, date_type(2018, 1, 1)).isoformat()
    clipped_start, clipped_end = clip_dates_to_source_coverage(source, start_iso, end_iso, data_type=data_type)
    if not clipped_end or clipped_start > clipped_end:
        return []
    rng = pd.date_range(start=clipped_start, end=clipped_end, freq="D")
    return [str(d.date()) for d in rng if not is_in_known_gap(source, data_type, str(d.date()))]


def _existing_keys_for_data_type(df: pd.DataFrame, data_type: str) -> set[tuple[str, str]]:
    """Set of (date, league_id) tuples already in the manifest for this data_type.

    We treat ANY existing row (captured / empty_confirmed / attempted_failed) as
    "no need to write empty" — the adapter's prior decision stands. We also
    treat a date-aggregate row (league_id == "") as "this date is already
    accounted for at the date level" and skip the entire date so we don't
    introduce per-league empties that conflict with the date-aggregate row.
    """
    sub = df[df["data_type"] == data_type].copy()
    sub["date"] = sub["date"].astype(str)
    sub["league_id"] = sub["league_id"].fillna("").astype(str)
    return set(zip(sub["date"], sub["league_id"], strict=True))


def _dates_with_aggregate_only(df: pd.DataFrame, data_type: str) -> set[str]:
    """Dates whose only manifest row for ``data_type`` is the legacy date-aggregate
    (``league_id == ""``).  These are eligible for *upgrade* to per-league rows —
    we keep the aggregate but ALSO emit one empty per expected league so the
    new per-league denominator counts them.
    """
    sub = df[df["data_type"] == data_type].copy()
    sub["date"] = sub["date"].astype(str)
    sub["league_id"] = sub["league_id"].fillna("").astype(str)
    by_date: dict[str, set[str]] = {}
    for d, lid in zip(sub["date"], sub["league_id"], strict=True):
        by_date.setdefault(d, set()).add(lid)
    return {d for d, lids in by_date.items() if lids == {""}}


def _backfill_data_type(
    df: pd.DataFrame,
    data_type: str,
    source: str,
    classifications: list[str],
    end_iso: str,
    *,
    dry_run: bool,
) -> dict[str, int]:
    expected_leagues = _expected_leagues_with_prediction_merge(source, classifications)
    expected_dates = _expected_dates(source, data_type, end_iso)
    existing = _existing_keys_for_data_type(df, data_type)
    aggregate_only_dates = _dates_with_aggregate_only(df, data_type)

    logger.info(
        "%s: %d expected leagues, %d expected dates, %d existing keys, %d aggregate-only dates",
        data_type,
        len(expected_leagues),
        len(expected_dates),
        len(existing),
        len(aggregate_only_dates),
    )

    attempt_ts = datetime.now(UTC)
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=BUCKET,
        per_vm_shards=True,
    )
    written = 0
    skipped_already_present = 0
    skipped_no_date_attempted = 0

    for date_iso in expected_dates:
        # If a date has NO existing row at all (no aggregate, no per-league),
        # we can't tell whether the adapter ever attempted that date. Don't
        # invent empty rows — the live VM (or a normal backfill) will fill
        # them on its next run. This script only repairs PARTIAL coverage.
        date_keys = {lid for (d, lid) in existing if d == date_iso}
        if not date_keys:
            skipped_no_date_attempted += 1
            continue

        for league_id in expected_leagues:
            if (date_iso, league_id) in existing:
                skipped_already_present += 1
                continue
            if not dry_run:
                writer.record_empty(
                    row_key={"date": date_iso, "data_type": data_type, "league_id": league_id},
                    attempted_at=attempt_ts,
                )
            written += 1

    if not dry_run:
        writer.write()

    logger.info(
        "%s: would_write=%d  skipped_already_present=%d  skipped_no_date_attempted=%d",
        data_type,
        written,
        skipped_already_present,
        skipped_no_date_attempted,
    )
    return {
        "would_write": written,
        "skipped_already_present": skipped_already_present,
        "skipped_no_date_attempted": skipped_no_date_attempted,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Compute counts without writing")
    parser.add_argument(
        "--data-types",
        type=str,
        default="MATCHES,PLAYER_VALUES",
        help="Comma-separated data_type subset (default: MATCHES,PLAYER_VALUES)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=date_type.today().isoformat(),
        help="Inclusive end date for expected-date generation (default: today UTC)",
    )
    args = parser.parse_args()

    target_set = {dt.strip() for dt in args.data_types.split(",") if dt.strip()}
    targets = [t for t in TARGETS if t[0] in target_set]
    if not targets:
        logger.error("No matching data types in TARGETS for --data-types=%s", args.data_types)
        return 2

    df = _read_canonical_manifest()
    summary: dict[str, dict[str, int]] = {}
    for data_type, source, classifications in targets:
        summary[data_type] = _backfill_data_type(
            df,
            data_type=data_type,
            source=source,
            classifications=classifications,
            end_iso=args.end_date,
            dry_run=args.dry_run,
        )

    print("\n=== Summary ===")
    for dt, counts in summary.items():
        print(f"  {dt}: {counts}")
    print(f"\n  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    if not args.dry_run:
        print("\n  Next: run the manifest consolidator to merge per-vm shards into canonical:")
        print("    python -m unified_trading_library.manifest_consolidator --bucket " + BUCKET)
    return 0


if __name__ == "__main__":
    sys.exit(main())
