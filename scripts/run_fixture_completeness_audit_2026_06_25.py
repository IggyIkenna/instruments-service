#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after every shortfall in output CSV actioned + parent plan
#   sports_fixture_completeness_oracle_2026_06_24.md archives.
"""Fixture-completeness audit — golden window + 2014→2026 backfill shortfall scan.

Plan: ``unified-trading-pm/plans/active/sports_fixture_completeness_oracle_2026_06_24.md``
      Phase 5: Run it over the golden window + the 2014→2026 backfill.

What this does
--------------
1.  Reads the sports instruments-store availability index (no per-parquet GCS
    reads — the manifest already stores ``row_count`` per (date, league_id) shard).
2.  For each (league_id, season_year) group in the registered universe
    (UAC ``SEASON_STRUCTURE_REGISTRY``, seasons 2019-2026), sums captured
    ``row_count`` across FIXTURES shards and compares against
    ``get_expected_fixture_count``.
3.  Reports every league/season where ``captured < expected`` as a shortfall
    with the gap count.
4.  For shortfalls, lists the specific ``(date, league_id)`` shards that are
    either ``attempted_failed`` OR never written (missing shard) — these become
    the targeted fixture re-fetch inputs.

Two outputs (both written locally + to GCS ``_audits/``):
  - ``fixture_completeness_season_summary_<ts>.csv`` — per-(league, season) totals.
  - ``fixture_completeness_targeted_refetch_<ts>.csv`` — per-(date, league) pairs
    to re-fetch, ordered by league/date.

The golden window (2025-09-01..2025-11-30) is included in the main scan.
The 2014-2018 range pre-dates the registry (no expected counts seeded yet) —
those seasons appear in the index scan with ``expected_fixtures=None`` and
``depth_coverage=None``; they are not shortfall-flagged but are still surfaced
as "unknown denominator" rows so you can see raw captured counts.

Usage
-----
::

    # Dry-run: prints summary, no GCS writes:
    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd \\
      .venv-workspace/bin/python scripts/run_fixture_completeness_audit_2026_06_25.py

    # Write outputs to GCS as well:
    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd \\
      .venv-workspace/bin/python scripts/run_fixture_completeness_audit_2026_06_25.py --upload

    # Restrict to a single league for debugging:
    ... --league EPL

    # Restrict date window:
    ... --start-date 2025-09-01 --end-date 2025-11-30

    # Output to a custom local directory:
    ... --out-dir /tmp/fixture_audit
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
from datetime import UTC, date, datetime

import pandas as pd
from unified_api_contracts.canonical.domain.sports import (
    get_all_league_ids,
    get_expected_fixture_count,
)
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fixture_completeness_audit")

_INDEX_BLOB = "_index/availability_index.parquet"
_AUDITS_PREFIX = "_audits"
_DT_FIXTURES = "FIXTURES"
_CAP_CAPTURED = "captured"
_CAP_FAILED = "attempted_failed"

# Seasons covered by the UAC registry — pre-2019 have no expected counts.
_REGISTRY_START_YEAR = 2019
_REGISTRY_END_YEAR = 2026  # inclusive

# Football season: Aug–Jul, so month >= 8 → season_year = calendar_year.
_SEASON_MONTH_CUTOFF = 8


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _season_year(d: date) -> int:
    """Football season start-year for *d* (Aug–Jul boundary)."""
    return d.year if d.month >= _SEASON_MONTH_CUTOFF else d.year - 1


def _resolve_bucket(bucket_override: str | None) -> str:
    if bucket_override:
        return bucket_override
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _read_index(bucket: str) -> pd.DataFrame:
    """Download and return the availability index DataFrame."""
    storage = get_storage_client()
    raw: bytes = storage.download_bytes(bucket, _INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded index: %d rows from gs://%s/%s", len(df), bucket, _INDEX_BLOB)
    return df


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------


def _build_fixtures_index(
    df: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
    league_filter: str | None,
) -> pd.DataFrame:
    """Return the FIXTURES slice of the index with season_year computed."""
    filt = df[df["data_type"] == _DT_FIXTURES].copy()

    # Normalise date column.
    date_col = next((c for c in ("date", "processing_date", "day") if c in filt.columns), None)
    if date_col is None:
        raise SystemExit("No date column found in index — cannot proceed.")
    filt["_date_str"] = filt[date_col].astype(str).str.strip().str[:10]

    if start_date:
        filt = filt[filt["_date_str"] >= start_date]
    if end_date:
        filt = filt[filt["_date_str"] <= end_date]

    parsed = pd.to_datetime(filt["_date_str"], errors="coerce")
    filt["season_year"] = parsed.apply(
        lambda ts: _season_year(ts.date()) if pd.notna(ts) else None
    )
    filt = filt.dropna(subset=["season_year", "league_id"])
    filt["season_year"] = filt["season_year"].astype(int)

    if league_filter:
        filt = filt[filt["league_id"].astype(str).str.upper() == league_filter.upper()]

    # IS writes instrument_count; row_count is a legacy/unused field (always 0).
    # Use instrument_count for fixture counting; keep row_count as fallback.
    if "instrument_count" in filt.columns:
        filt["instrument_count"] = pd.to_numeric(
            filt["instrument_count"], errors="coerce"
        ).fillna(0)
    else:
        filt["instrument_count"] = 0
    if "row_count" in filt.columns:
        filt["row_count"] = pd.to_numeric(filt["row_count"], errors="coerce").fillna(0).astype(int)
    else:
        filt["row_count"] = 0

    return filt


def _compute_season_summary(
    filt: pd.DataFrame,
) -> list[dict[str, object]]:
    """Compute per-(league, season) summary rows."""
    rows: list[dict[str, object]] = []
    groups = filt.groupby(["league_id", "season_year"], sort=True)
    for (league_id, season_year), grp in groups:
        captured_count = int(
            grp.loc[grp["capture_status"] == _CAP_CAPTURED, "instrument_count"].sum()
        )
        expected_count: int | None = get_expected_fixture_count(str(league_id), int(season_year))
        if expected_count is not None and expected_count > 0:
            shortfall = max(0, expected_count - captured_count)
            depth = round(captured_count / expected_count, 6)
            in_registry = True
        else:
            shortfall = None
            depth = None
            in_registry = False

        failed_shards = int((grp["capture_status"] == _CAP_FAILED).sum())
        total_shards = len(grp)
        captured_shards = int((grp["capture_status"] == _CAP_CAPTURED).sum())

        rows.append(
            {
                "league_id": str(league_id),
                "season_year": int(season_year),
                "in_registry": in_registry,
                "captured_fixtures": captured_count,
                "expected_fixtures": expected_count,
                "shortfall": shortfall,
                "depth_coverage": depth,
                "total_shards": total_shards,
                "captured_shards": captured_shards,
                "failed_shards": failed_shards,
            }
        )
    return rows


def _compute_targeted_refetch(
    filt: pd.DataFrame,
    summary: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Return (date, league_id) pairs that need a targeted fixture re-fetch.

    A shard needs re-fetching if:
    - Its capture_status is ``attempted_failed`` (fetch attempted but failed).
    - Its capture_status is NOT ``captured`` / ``empty_confirmed`` AND the league
      is in the registry (missing shard on a league with known expected fixtures).

    We only include leagues that have a non-zero shortfall in the season summary
    to avoid re-fetching shards for leagues already at 100% coverage.
    """
    shortfall_leagues: set[str] = {
        str(r["league_id"])
        for r in summary
        if r["in_registry"] and r["shortfall"] is not None and r["shortfall"] > 0
    }

    refetch_rows: list[dict[str, object]] = []
    for _, row in filt.iterrows():
        lid = str(row["league_id"])
        if lid not in shortfall_leagues:
            continue
        status = str(row.get("capture_status", ""))
        if status in (_CAP_CAPTURED, "empty_confirmed"):
            continue
        refetch_rows.append(
            {
                "date": str(row["_date_str"]),
                "league_id": lid,
                "season_year": int(row["season_year"]),
                "capture_status": status,
                "row_count": int(row.get("row_count", 0)),
            }
        )

    refetch_rows.sort(key=lambda r: (r["league_id"], r["date"]))
    return refetch_rows


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _write_csv(rows: list[dict[str, object]], path: str) -> None:
    if not rows:
        logger.info("No rows to write to %s", path)
        return
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    logger.info("Wrote %d rows → %s", len(rows), path)


def _upload_csv(bucket: str, blob_path: str, local_path: str) -> None:
    with open(local_path, "rb") as fh:
        content = fh.read()
    storage = get_storage_client()
    storage.upload_bytes(bucket, blob_path, content)
    logger.info("Uploaded → gs://%s/%s", bucket, blob_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload output CSVs to GCS _audits/ (default: local only)",
    )
    parser.add_argument("--bucket", default=None, help="Override instruments-store bucket.")
    parser.add_argument("--league", default=None, help="Restrict to one league (e.g. EPL).")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Only include shards on/after YYYY-MM-DD (default: all history).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Only include shards on/before YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Local output directory (default: system tempdir).",
    )
    parser.add_argument(
        "--shortfalls-only",
        action="store_true",
        help="Print only leagues/seasons with shortfall > 0 in the summary.",
    )
    args = parser.parse_args(argv)

    bucket = _resolve_bucket(args.bucket)
    logger.info("Sports instruments bucket: %s", bucket)

    # Load manifest.
    index_df = _read_index(bucket)

    # Filter to FIXTURES rows.
    filt = _build_fixtures_index(
        index_df,
        start_date=args.start_date,
        end_date=args.end_date,
        league_filter=args.league,
    )
    logger.info(
        "FIXTURES rows after filters: %d (date range %s..%s, league=%s)",
        len(filt),
        args.start_date or "all",
        args.end_date or "today",
        args.league or "all",
    )
    if filt.empty:
        logger.warning("No FIXTURES rows found — nothing to audit.")
        return 0

    # Compute season summary.
    summary = _compute_season_summary(filt)

    # Compute targeted re-fetch list.
    refetch = _compute_targeted_refetch(filt, summary)

    # --- Print summary to stdout ---
    total_shortfall_rows = sum(1 for r in summary if r.get("shortfall") and r["shortfall"] > 0)
    registered_rows = [r for r in summary if r["in_registry"]]
    overall_captured = sum(int(r["captured_fixtures"]) for r in registered_rows)
    overall_expected = sum(
        int(r["expected_fixtures"])
        for r in registered_rows
        if r["expected_fixtures"] is not None
    )
    if overall_expected > 0:
        overall_depth = round(overall_captured / overall_expected, 6)
    else:
        overall_depth = None

    print(
        f"\n{'='*72}\n"
        f"  FIXTURE COMPLETENESS AUDIT\n"
        f"  Leagues in summary:  {len(summary)}\n"
        f"  Registered (have expected count): {len(registered_rows)}\n"
        f"  Leagues/seasons with shortfall:   {total_shortfall_rows}\n"
        f"  Total captured fixtures:          {overall_captured:,}\n"
        f"  Total expected fixtures:          {overall_expected:,}\n"
        f"  Overall depth coverage:           "
        f"{overall_depth:.4%}\n" if overall_depth is not None else "  Overall depth coverage: N/A\n",
        end="",
    )
    print(f"  Targeted re-fetch shards:         {len(refetch)}\n{'='*72}\n")

    # Print per-season rows with shortfalls.
    if total_shortfall_rows > 0:
        print("  SHORTFALLS:")
        display = summary if not args.shortfalls_only else [
            r for r in summary if r.get("shortfall") and r["shortfall"] > 0
        ]
        for r in sorted(display, key=lambda x: (str(x["league_id"]), int(x["season_year"]))):
            if r["shortfall"] is not None and r["shortfall"] > 0:
                print(
                    f"    {r['league_id']:30s}  season={r['season_year']}"
                    f"  captured={r['captured_fixtures']:4d}"
                    f"  expected={r['expected_fixtures']:4d}"
                    f"  shortfall={r['shortfall']:4d}"
                    f"  depth={r['depth_coverage']:.2%}"
                )
        print()
    else:
        print("  No shortfalls found — all registered leagues/seasons are complete.\n")

    # Write CSV outputs.
    out_dir = args.out_dir or tempfile.gettempdir()
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    summary_path = os.path.join(out_dir, f"fixture_completeness_season_summary_{ts}.csv")
    refetch_path = os.path.join(out_dir, f"fixture_completeness_targeted_refetch_{ts}.csv")

    _write_csv(summary, summary_path)
    _write_csv(refetch, refetch_path)

    if args.upload:
        _upload_csv(bucket, f"{_AUDITS_PREFIX}/fixture_completeness_season_summary_{ts}.csv", summary_path)
        _upload_csv(bucket, f"{_AUDITS_PREFIX}/fixture_completeness_targeted_refetch_{ts}.csv", refetch_path)
    else:
        logger.info("Dry-run: outputs written to %s (no GCS upload). Use --upload to push.", out_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
