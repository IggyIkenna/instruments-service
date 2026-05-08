#!/usr/bin/env python3
"""Backfill the availability manifest for the 398 fixture_stats parquets created by Phase 3.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan``.

Phase 3's ``migrate_sports_fixtures_legacy_to_new.py`` wrote
``sports_reference/by_date/day=D/entity=fixture_stats/fixture_stats.parquet``
directly via ``Blob.upload_from_file`` for performance — bypassing
``ManifestWriter``. As a result the data-status UI shows
``FIXTURE_STATS 0/10053`` for the migrated 2018 range even though the
parquets exist on disk and downloads work end-to-end.

This script reads each migrated fixture_stats parquet, groups by canonical
league via ``fixtures.parquet`` sibling (af_fixture_id → af_league_id →
canonical league_id), and emits per-league ``ManifestWriter.add()``
records of ``data_type="FIXTURE_STATS"``.

Idempotent: ``ManifestWriter`` deduplicates on
``(date, data_type, league_id, …)`` so re-running is safe.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_sports_fixture_stats_manifest.py            # full run
    .venv/bin/python scripts/backfill_sports_fixture_stats_manifest.py --dry-run  # plan only
    .venv/bin/python scripts/backfill_sports_fixture_stats_manifest.py --limit 5  # spot-check
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date as date_type
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage
from unified_api_contracts.canonical.domain.sports.league_data import get_league_by_api_football_id
from unified_trading_library import ManifestWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
SRC_PREFIX = "sports_reference/by_date/"
MIGRATION_REPORT = Path(__file__).parent / "sports_legacy_migration_report.json"
OUT_PATH = Path(__file__).parent / "sports_fixture_stats_manifest_backfill_report.json"


@dataclass(frozen=True)
class DayBackfill:
    day: str
    status: str  # "succeeded" | "skipped_no_stats" | "skipped_no_fixtures" | "failed"
    rows_written: int = 0
    leagues: tuple[str, ...] = ()
    error: str | None = None


def _load_migrated_days() -> list[str]:
    """Read the migration report; return days that succeeded in Phase 3."""
    if not MIGRATION_REPORT.exists():
        msg = f"migration report not found at {MIGRATION_REPORT}"
        raise FileNotFoundError(msg)
    data = json.loads(MIGRATION_REPORT.read_text())
    return sorted(d for d, info in data["days"].items() if info["status"] == "succeeded")


def _read_parquet(client: storage.Client, path: str) -> pd.DataFrame | None:
    try:
        raw = client.bucket(BUCKET).blob(path).download_as_bytes()
    except (NotFound, FileNotFoundError):
        return None
    try:
        return pd.read_parquet(io.BytesIO(raw))
    except (OSError, RuntimeError, ValueError):
        return None


def _backfill_one_day(client: storage.Client, manifest: ManifestWriter, day: str, *, dry_run: bool) -> DayBackfill:
    """Read fixtures+fixture_stats parquets, emit per-league FIXTURE_STATS manifest rows."""
    fixtures_path = f"{SRC_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    stats_path = f"{SRC_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet"

    stats_df = _read_parquet(client, stats_path)
    if stats_df is None or stats_df.empty:
        return DayBackfill(day=day, status="skipped_no_stats")
    fixtures_df = _read_parquet(client, fixtures_path)
    if fixtures_df is None or fixtures_df.empty:
        return DayBackfill(day=day, status="skipped_no_fixtures", error="fixtures sibling missing or empty")

    # Build af_fixture_id → af_league_id map from the fixtures sibling.
    if "af_fixture_id" not in fixtures_df.columns or "af_league_id" not in fixtures_df.columns:
        return DayBackfill(day=day, status="failed", error="fixtures missing af_fixture_id or af_league_id")
    fid_to_af_league: dict[int, int] = {}
    for _, row in fixtures_df[["af_fixture_id", "af_league_id"]].dropna().iterrows():
        try:
            fid_to_af_league[int(row["af_fixture_id"])] = int(row["af_league_id"])
        except (TypeError, ValueError):
            continue

    # Resolve canonical UAC league_id per row + group rows.
    counts: dict[str, int] = {}
    for _, row in stats_df.iterrows():
        try:
            af_fid = int(row["af_fixture_id"])
        except (TypeError, ValueError):
            continue
        af_lid = fid_to_af_league.get(af_fid)
        if af_lid is None:
            continue
        league = get_league_by_api_football_id(af_lid)
        if league is None:
            continue
        counts[league.league_id] = counts.get(league.league_id, 0) + 1

    if not counts:
        return DayBackfill(day=day, status="skipped_no_stats", error="no rows mapped to canonical league")

    if dry_run:
        return DayBackfill(
            day=day,
            status="succeeded",
            rows_written=sum(counts.values()),
            leagues=tuple(sorted(counts)),
        )

    proc_date = date_type.fromisoformat(day)
    for lid, count in counts.items():
        manifest.add(
            processing_date=proc_date,
            row_count=count,
            data_type="FIXTURE_STATS",
            league_id=lid,
            venue="api_football",
        )

    return DayBackfill(
        day=day,
        status="succeeded",
        rows_written=sum(counts.values()),
        leagues=tuple(sorted(counts)),
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(argv)

    days = _load_migrated_days()
    if args.limit:
        days = days[: args.limit]
    logger.info("backfilling FIXTURE_STATS manifest for %d days (dry_run=%s)", len(days), args.dry_run)

    client = storage.Client()
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)

    results: dict[str, DayBackfill] = {}
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_backfill_one_day, client, manifest, d, dry_run=args.dry_run): d for d in days}
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            results[day] = fut.result()
            if i % 50 == 0:
                logger.info("progress: %d/%d in %.1fs", i, len(days), time.monotonic() - t0)
    logger.info("processed: %d in %.1fs", len(results), time.monotonic() - t0)

    if not args.dry_run:
        logger.info("flushing manifest …")
        manifest.flush()
        logger.info("manifest flushed")

    summary: dict[str, int] = {}
    for r in results.values():
        summary[r.status] = summary.get(r.status, 0) + 1
    total_rows = sum(r.rows_written for r in results.values())
    logger.info("summary: %s | total rows: %d", json.dumps(summary, sort_keys=True), total_rows)

    out = {
        "_meta": {
            "bucket": BUCKET,
            "src_prefix": SRC_PREFIX,
            "data_type": "FIXTURE_STATS",
            "dry_run": args.dry_run,
            "total_days": len(results),
            "summary": summary,
            "total_rows": total_rows,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "days": {day: asdict(results[day]) for day in sorted(results)},
    }
    OUT_PATH.write_text(json.dumps(out, indent=2))
    logger.info("report: %s (%d bytes)", OUT_PATH, OUT_PATH.stat().st_size)

    failed = [d for d, r in results.items() if r.status == "failed"]
    if failed:
        logger.warning("%d days failed — see report", len(failed))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
