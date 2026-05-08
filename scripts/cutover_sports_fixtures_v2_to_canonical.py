#!/usr/bin/env python3
"""Phase 4 cutover: per-day in-place overwrite of legacy fixtures with v2 outputs.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.md``.

Per-day GCS-internal copy from ``sports_reference_v2/by_date/day=D/...``
to ``sports_reference/by_date/day=D/...`` for every day that
:func:`validate_sports_fixtures_v2_parity` confirmed OK. This:

  - Overwrites the legacy nested-struct fixtures.parquet with the v2 flat schema
  - Adds the new ``entity=fixture_stats/fixture_stats.parquet`` partition

Sibling entities (``entity=teams``, ``entity=leagues``, ``entity=injuries``,
``entity=understat_xg``, ``entity=progressive_stats``, ``entity=footystats_*``,
etc.) are NOT touched — they're out of scope for this migration.

The original legacy parquet is moved to
``sports_reference_v1_archive/by_date/day=D/entity=fixtures/fixtures.parquet``
before the overwrite so the cutover is reversible: a reverse run swaps
back from the archive prefix.

Read-side coordination: producers (live sports adapter via
``launch-api-football-backfill-vm.sh`` or any Cloud Scheduler-driven cycle)
should be paused during the cutover window. Per-day cutover keeps the
window small (~5 min for 398 days), but a producer mid-write would race
the per-day archive→overwrite sequence.

Usage::

    cd instruments-service
    .venv/bin/python scripts/cutover_sports_fixtures_v2_to_canonical.py --dry-run    # plan only
    .venv/bin/python scripts/cutover_sports_fixtures_v2_to_canonical.py              # do it
    .venv/bin/python scripts/cutover_sports_fixtures_v2_to_canonical.py --reverse    # roll back from _v1_archive/

Pre-requisite: ``sports_legacy_parity_report.json`` summary must show every
day status="ok". The script bails immediately if any day is non-ok.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
SRC_PREFIX = "sports_reference/by_date/"  # canonical (overwrite target)
V2_PREFIX = "sports_reference_v2/by_date/"  # Phase 3 output
ARCHIVE_PREFIX = "sports_reference_v1_archive/by_date/"  # rollback safety net

PARITY_PATH = Path(__file__).parent / "sports_legacy_parity_report.json"
REPORT_PATH = Path(__file__).parent / "sports_legacy_cutover_report.json"


@dataclass(frozen=True)
class DayCutover:
    day: str
    status: str  # "succeeded" | "failed" | "dry_run"
    error: str | None = None


def _load_ok_days() -> list[str]:
    if not PARITY_PATH.exists():
        msg = f"parity report not found at {PARITY_PATH} — run validate_sports_fixtures_v2_parity.py first"
        raise FileNotFoundError(msg)
    data = json.loads(PARITY_PATH.read_text())
    summary = data["_meta"]["summary"]
    not_ok = {k: v for k, v in summary.items() if k != "ok"}
    if not_ok:
        msg = f"parity report has non-ok statuses {not_ok!r}; refuse to cut over"
        raise RuntimeError(msg)
    return sorted(d for d, info in data["days"].items() if info["status"] == "ok")


def _copy_blob(client: storage.Client, src_path: str, dst_path: str) -> None:
    src_blob = client.bucket(BUCKET).blob(src_path)
    src_blob.reload()  # populate generation for if_source_generation_match later if desired
    client.bucket(BUCKET).copy_blob(src_blob, client.bucket(BUCKET), new_name=dst_path)


def _delete_blob(client: storage.Client, path: str) -> None:
    import contextlib

    with contextlib.suppress(NotFound):
        client.bucket(BUCKET).blob(path).delete()


def _cutover_one_day(client: storage.Client, day: str, *, dry_run: bool, reverse: bool) -> DayCutover:
    fixtures_canonical = f"{SRC_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    fixtures_v2 = f"{V2_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    fixtures_archive = f"{ARCHIVE_PREFIX}day={day}/entity=fixtures/fixtures.parquet"

    stats_canonical = f"{SRC_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet"
    stats_v2 = f"{V2_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet"

    if dry_run:
        return DayCutover(day=day, status="dry_run")

    try:
        if reverse:
            # Restore: archive/fixtures → canonical/fixtures; delete v2-fixture_stats parquet.
            _copy_blob(client, fixtures_archive, fixtures_canonical)
            _delete_blob(client, stats_canonical)
        else:
            # Forward (idempotent): only archive if not already archived. The
            # archive holds the ORIGINAL legacy parquet — once captured, never
            # overwrite, so re-running the cutover after a partial completion
            # doesn't replace the preserved legacy with a flat-schema parquet.
            archive_blob = client.bucket(BUCKET).blob(fixtures_archive)
            if not archive_blob.exists():
                _copy_blob(client, fixtures_canonical, fixtures_archive)
            _copy_blob(client, fixtures_v2, fixtures_canonical)
            _copy_blob(client, stats_v2, stats_canonical)
    except (NotFound, OSError, RuntimeError, ValueError) as exc:
        return DayCutover(day=day, status="failed", error=str(exc))
    return DayCutover(day=day, status="succeeded")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="don't copy anything")
    parser.add_argument("--reverse", action="store_true", help="roll back from _v1_archive/")
    parser.add_argument("--limit", type=int, default=0, help="cap to first N days for spot-check")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(argv)

    days = _load_ok_days()
    if args.limit:
        days = days[: args.limit]
    direction = "REVERSE" if args.reverse else "FORWARD"
    logger.info("cutover %s on %d days (dry_run=%s)", direction, len(days), args.dry_run)

    client = storage.Client()
    results: dict[str, DayCutover] = {}
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_cutover_one_day, client, d, dry_run=args.dry_run, reverse=args.reverse): d for d in days
        }
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            results[day] = fut.result()
            if i % 50 == 0:
                logger.info("progress: %d/%d in %.1fs", i, len(days), time.monotonic() - t0)
    logger.info("done: %d in %.1fs", len(results), time.monotonic() - t0)

    summary: dict[str, int] = {}
    for r in results.values():
        summary[r.status] = summary.get(r.status, 0) + 1
    logger.info("summary: %s", json.dumps(summary, sort_keys=True))

    out = {
        "_meta": {
            "bucket": BUCKET,
            "src_prefix": SRC_PREFIX,
            "v2_prefix": V2_PREFIX,
            "archive_prefix": ARCHIVE_PREFIX,
            "direction": direction,
            "dry_run": args.dry_run,
            "total_days": len(results),
            "summary": summary,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "days": {day: asdict(results[day]) for day in sorted(results)},
    }
    REPORT_PATH.write_text(json.dumps(out, indent=2))
    logger.info("report: %s (%d bytes)", REPORT_PATH, REPORT_PATH.stat().st_size)

    failed = [d for d, r in results.items() if r.status == "failed"]
    if failed:
        logger.error("%d days FAILED — see report; rerun with --reverse to roll back", len(failed))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
