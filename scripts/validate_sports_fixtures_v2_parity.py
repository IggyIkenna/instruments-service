#!/usr/bin/env python3
"""Phase 4 parity validation: compare legacy ↔ rewritten fixtures parquets.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_legacy_schema_migration_2026_04_28.plan.md``.

Reads each LEGACY day from ``sports_legacy_schema_audit.json``, downloads
both the original legacy parquet (``sports_reference/by_date/...``) and
the migrated v2 parquet (``sports_reference_v2/by_date/...``), then
asserts:

  1. **Row count match** — ``len(legacy_df) == len(v2_fixtures_df) == len(v2_stats_df)``
  2. **af_league_id derivation** — for each row, ``v2_fixtures_df.af_league_id``
     equals the int parsed from ``legacy_df.league.logo_url`` (regex
     ``/leagues/(\\d+)\\.png``).
  3. **af_fixture_id consistency** — fixtures + fixture_stats share the same
     af_fixture_id set (join key for downstream consumers).

Read-only; safe to run any time. Exits non-zero if any day fails parity.

Output: ``instruments-service/scripts/sports_legacy_parity_report.json``.

Usage::

    cd instruments-service
    .venv/bin/python scripts/validate_sports_fixtures_v2_parity.py
"""

from __future__ import annotations

import io
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
SRC_PREFIX = "sports_reference/by_date/"
DST_PREFIX = "sports_reference_v2/by_date/"

AUDIT_PATH = Path(__file__).parent / "sports_legacy_schema_audit.json"
REPORT_PATH = Path(__file__).parent / "sports_legacy_parity_report.json"

_AF_LOGO_RE = re.compile(r"/(?:leagues|teams)/(\d+)\.png")


@dataclass(frozen=True)
class DayParity:
    day: str
    status: (
        str  # "ok" | "row_count_mismatch" | "af_league_id_mismatch" | "fixture_id_mismatch" | "missing_v2" | "error"
    )
    legacy_rows: int = 0
    v2_fixtures_rows: int = 0
    v2_stats_rows: int = 0
    error: str | None = None


def _load_legacy_days() -> list[str]:
    if not AUDIT_PATH.exists():
        msg = f"audit JSON not found at {AUDIT_PATH}"
        raise FileNotFoundError(msg)
    data = json.loads(AUDIT_PATH.read_text())
    return sorted(d for d, schema in data["days"].items() if schema == "LEGACY")


def _read_parquet(client: storage.Client, path: str) -> pd.DataFrame | None:
    try:
        raw = client.bucket(BUCKET).blob(path).download_as_bytes()
    except (NotFound, FileNotFoundError):
        return None
    try:
        return pd.read_parquet(io.BytesIO(raw))
    except (OSError, RuntimeError, ValueError):
        return None


def _af_id_from_legacy_struct(cell: object) -> int | None:
    if not isinstance(cell, dict):
        return None
    url = cell.get("logo_url")
    if not isinstance(url, str):
        return None
    match = _AF_LOGO_RE.search(url)
    return int(match.group(1)) if match else None


def _validate_one_day(client: storage.Client, day: str) -> DayParity:
    legacy_path = f"{SRC_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    fixtures_v2_path = f"{DST_PREFIX}day={day}/entity=fixtures/fixtures.parquet"
    stats_v2_path = f"{DST_PREFIX}day={day}/entity=fixture_stats/fixture_stats.parquet"

    legacy_df = _read_parquet(client, legacy_path)
    if legacy_df is None:
        return DayParity(day=day, status="error", error="legacy parquet read failed")

    fixtures_v2 = _read_parquet(client, fixtures_v2_path)
    stats_v2 = _read_parquet(client, stats_v2_path)
    if fixtures_v2 is None or stats_v2 is None:
        return DayParity(
            day=day,
            status="missing_v2",
            legacy_rows=len(legacy_df),
            error=f"missing v2: fixtures={fixtures_v2 is not None}, stats={stats_v2 is not None}",
        )

    legacy_rows = len(legacy_df)
    if len(fixtures_v2) != legacy_rows or len(stats_v2) != legacy_rows:
        return DayParity(
            day=day,
            status="row_count_mismatch",
            legacy_rows=legacy_rows,
            v2_fixtures_rows=len(fixtures_v2),
            v2_stats_rows=len(stats_v2),
            error=f"row count drift: {legacy_rows} → {len(fixtures_v2)} fixtures + {len(stats_v2)} stats",
        )

    # Derive af_league_id from legacy logo_url and confirm it matches v2.
    legacy_af_league = legacy_df["league"].apply(_af_id_from_legacy_struct)
    v2_af_league = fixtures_v2["af_league_id"]
    # Treat NaN==NaN as equal for this check.
    legacy_set = set(legacy_af_league.dropna().astype(int).tolist())
    v2_set = set(v2_af_league.dropna().astype(int).tolist())
    if legacy_set != v2_set:
        return DayParity(
            day=day,
            status="af_league_id_mismatch",
            legacy_rows=legacy_rows,
            v2_fixtures_rows=len(fixtures_v2),
            v2_stats_rows=len(stats_v2),
            error=f"af_league_id sets differ: legacy={sorted(legacy_set)[:5]}… v2={sorted(v2_set)[:5]}…",
        )

    # af_fixture_id must be identical between fixtures + fixture_stats (join key).
    if set(fixtures_v2["af_fixture_id"].dropna().astype(int).tolist()) != set(
        stats_v2["af_fixture_id"].dropna().astype(int).tolist()
    ):
        return DayParity(
            day=day,
            status="fixture_id_mismatch",
            legacy_rows=legacy_rows,
            v2_fixtures_rows=len(fixtures_v2),
            v2_stats_rows=len(stats_v2),
            error="af_fixture_id sets differ between fixtures and fixture_stats",
        )

    return DayParity(
        day=day,
        status="ok",
        legacy_rows=legacy_rows,
        v2_fixtures_rows=len(fixtures_v2),
        v2_stats_rows=len(stats_v2),
    )


def main() -> int:
    legacy_days = _load_legacy_days()
    logger.info("validating parity on %d days", len(legacy_days))

    client = storage.Client()
    results: dict[str, DayParity] = {}
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(_validate_one_day, client, d): d for d in legacy_days}
        for i, fut in enumerate(as_completed(futures), 1):
            day = futures[fut]
            results[day] = fut.result()
            if i % 50 == 0:
                logger.info("progress: %d/%d in %.1fs", i, len(legacy_days), time.monotonic() - t0)
    logger.info("done: %d in %.1fs", len(results), time.monotonic() - t0)

    summary: dict[str, int] = {}
    for r in results.values():
        summary[r.status] = summary.get(r.status, 0) + 1
    logger.info("summary: %s", json.dumps(summary, sort_keys=True))

    out = {
        "_meta": {
            "bucket": BUCKET,
            "src_prefix": SRC_PREFIX,
            "dst_prefix": DST_PREFIX,
            "total_days": len(results),
            "summary": summary,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "days": {day: asdict(results[day]) for day in sorted(results)},
    }
    REPORT_PATH.write_text(json.dumps(out, indent=2))
    logger.info("report: %s (%d bytes)", REPORT_PATH, REPORT_PATH.stat().st_size)

    not_ok = [d for d, r in results.items() if r.status != "ok"]
    if not_ok:
        logger.error("%d days FAILED parity — see report for details", len(not_ok))
        for day in not_ok[:5]:
            logger.error("  %s: %s — %s", day, results[day].status, results[day].error)
        return 2
    logger.info("ALL %d days passed parity", len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
