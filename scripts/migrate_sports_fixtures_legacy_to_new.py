#!/usr/bin/env python3
"""Phase 3: rewrite legacy sports fixtures parquets into the new flat schema.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``.

Reads ``sports_legacy_schema_audit.json`` for the LEGACY day set (~398 days
post-Phase-0.6), then for each day:

  1. Download ``sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet``
  2. Run ``map_legacy_to_new(df, day=D)`` (instruments_service.sports.legacy_schema_mapper)
  3. Write the two output DataFrames side-by-side at:
       - ``sports_reference_v2/by_date/day={D}/entity=fixtures/fixtures.parquet``
       - ``sports_reference_v2/by_date/day={D}/entity=fixture_stats/fixture_stats.parquet``

NEW + ORPHAN_NEW + MISSING days are not touched here — Phase 4 atomic rename
will swap the prefixes; days outside the LEGACY set keep their existing parquets
which already match (or are pass-through-correct for) the new schema.

Per-day shard-level failure isolation: per-day exceptions emit a
``MIGRATION_SHARD_FAILED`` log line + are recorded in the migration report JSON,
NEVER raised. Job continues with the next day.

Output: ``instruments-service/scripts/sports_legacy_migration_report.json`` —
per-day status (succeeded / failed / skipped) + row counts + error reason.

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_sports_fixtures_legacy_to_new.py            # full run
    .venv/bin/python scripts/migrate_sports_fixtures_legacy_to_new.py --dry-run  # plan only
    .venv/bin/python scripts/migrate_sports_fixtures_legacy_to_new.py --limit 5  # spot-check
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
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage

from instruments_service.sports.legacy_schema_mapper import (
    is_legacy_schema,
    map_legacy_to_new,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
SRC_PREFIX = "sports_reference/by_date/"
DST_PREFIX = "sports_reference_v2/by_date/"

AUDIT_PATH = Path(__file__).parent / "sports_legacy_schema_audit.json"
REPORT_PATH = Path(__file__).parent / "sports_legacy_migration_report.json"


@dataclass(frozen=True)
class DayResult:
    day: str
    status: str  # "succeeded" | "failed" | "skipped" | "dry_run"
    legacy_rows: int = 0
    fixtures_out_rows: int = 0
    fixture_stats_out_rows: int = 0
    error: str | None = None


def _load_legacy_days() -> list[str]:
    """Read the audit JSON; return sorted list of days classified as LEGACY."""
    if not AUDIT_PATH.exists():
        msg = f"audit JSON not found at {AUDIT_PATH}; run sports_legacy_schema_audit.py first"
        raise FileNotFoundError(msg)
    data = json.loads(AUDIT_PATH.read_text())
    return sorted(d for d, schema in data["days"].items() if schema == "LEGACY")


def _migrate_one_day(client: storage.Client, day: str, *, dry_run: bool) -> DayResult:
    """Read one LEGACY day's fixtures.parquet, run the mapper, write v2 outputs.

    Shard-level failure isolation: any read / map / write exception is
    captured in the returned DayResult and the job continues.
    """
    src_path = f"{SRC_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    fixtures_dst = f"{DST_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    stats_dst = f"{DST_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet"

    try:
        src_blob = client.bucket(BUCKET).blob(src_path)
        raw = src_blob.download_as_bytes()
        df_legacy = pd.read_parquet(io.BytesIO(raw))
    except (NotFound, FileNotFoundError) as exc:
        return DayResult(day=day, status="failed", error=f"src missing: {exc}")
    except (OSError, RuntimeError, ValueError) as exc:
        return DayResult(day=day, status="failed", error=f"src read failed: {exc}")

    if not is_legacy_schema(df_legacy):
        return DayResult(
            day=day,
            status="skipped",
            legacy_rows=len(df_legacy),
            error="not legacy schema after re-read (audit drift?)",
        )

    try:
        fixtures_df, stats_df = map_legacy_to_new(df_legacy, day=day)
    except (KeyError, TypeError, ValueError) as exc:
        return DayResult(day=day, status="failed", legacy_rows=len(df_legacy), error=f"map failed: {exc}")

    if dry_run:
        return DayResult(
            day=day,
            status="dry_run",
            legacy_rows=len(df_legacy),
            fixtures_out_rows=len(fixtures_df),
            fixture_stats_out_rows=len(stats_df),
        )

    try:
        fixtures_buf = io.BytesIO()
        fixtures_df.to_parquet(fixtures_buf, index=False)
        fixtures_buf.seek(0)
        client.bucket(BUCKET).blob(fixtures_dst).upload_from_file(fixtures_buf, content_type="application/octet-stream")

        stats_buf = io.BytesIO()
        stats_df.to_parquet(stats_buf, index=False)
        stats_buf.seek(0)
        client.bucket(BUCKET).blob(stats_dst).upload_from_file(stats_buf, content_type="application/octet-stream")
    except (OSError, RuntimeError, ValueError) as exc:
        return DayResult(
            day=day,
            status="failed",
            legacy_rows=len(df_legacy),
            fixtures_out_rows=len(fixtures_df),
            fixture_stats_out_rows=len(stats_df),
            error=f"write failed: {exc}",
        )

    return DayResult(
        day=day,
        status="succeeded",
        legacy_rows=len(df_legacy),
        fixtures_out_rows=len(fixtures_df),
        fixture_stats_out_rows=len(stats_df),
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="don't write anything")
    parser.add_argument("--limit", type=int, default=0, help="cap to first N days for spot-check")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(argv)

    legacy_days = _load_legacy_days()
    if args.limit:
        legacy_days = legacy_days[: args.limit]
    logger.info("migrating %d LEGACY days (dry_run=%s, workers=%d)", len(legacy_days), args.dry_run, args.workers)

    client = storage.Client()
    results: dict[str, DayResult] = {}
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_migrate_one_day, client, d, dry_run=args.dry_run): d for d in legacy_days}
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            results[day] = fut.result()
            if i % 50 == 0:
                logger.info("progress: %d/%d in %.1fs", i, len(legacy_days), time.monotonic() - t0)
    logger.info("done: %d in %.1fs", len(results), time.monotonic() - t0)

    summary: dict[str, int] = {}
    for r in results.values():
        summary[r.status] = summary.get(r.status, 0) + 1
    total_in_rows = sum(r.legacy_rows for r in results.values())
    total_out_fixtures = sum(r.fixtures_out_rows for r in results.values())
    total_out_stats = sum(r.fixture_stats_out_rows for r in results.values())
    logger.info(
        "summary: %s | rows: %d → %d fixtures + %d fixture_stats",
        json.dumps(summary, sort_keys=True),
        total_in_rows,
        total_out_fixtures,
        total_out_stats,
    )

    out = {
        "_meta": {
            "bucket": BUCKET,
            "src_prefix": SRC_PREFIX,
            "dst_prefix": DST_PREFIX,
            "dry_run": args.dry_run,
            "total_days": len(results),
            "summary": summary,
            "row_totals": {
                "legacy_in": total_in_rows,
                "fixtures_out": total_out_fixtures,
                "fixture_stats_out": total_out_stats,
            },
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "days": {day: asdict(results[day]) for day in sorted(results)},
    }
    REPORT_PATH.write_text(json.dumps(out, indent=2))
    logger.info("report: %s (%d bytes)", REPORT_PATH, REPORT_PATH.stat().st_size)

    failed = [d for d, r in results.items() if r.status == "failed"]
    if failed:
        logger.warning("%d days FAILED — see report for details", len(failed))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
