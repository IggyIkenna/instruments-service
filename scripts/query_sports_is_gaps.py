#!/usr/bin/env python3
"""Query the instruments-service sports manifest for attempted_failed gaps.

Reads the live manifest, finds (date, data_type) cells with attempted_failed
capture_status, clusters them by data_type into contiguous date ranges, and
emits targeted launch commands for deployment-service/scripts/vm/launch-sports-is-gap-fill.sh.

Usage:
    python3 scripts/query_sports_is_gaps.py [--dry-run] [--min-failures N] [--project PROJECT_ID]

Output (stdout):
    Gap summary table + ready-to-paste gcloud launch commands.

Prerequisites:
    GCP ADC credentials with read access to instruments-store-sports-prd-*.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

import pandas as pd

_BUCKET = "instruments-store-sports-prd-central-element-323112"

# data_type → provider that owns it (used in --sports-provider flag)
_ENTITY_PROVIDER: dict[str, str] = {
    "FIXTURE_STATS": "API_FOOTBALL",
    "FIXTURE_LINEUPS": "API_FOOTBALL",
    "FIXTURE_EVENTS": "API_FOOTBALL",
    "PLAYER_STATS": "API_FOOTBALL",
    "FIXTURES": "API_FOOTBALL",
    "INJURIES": "API_FOOTBALL",
    "STANDINGS": "API_FOOTBALL",
    "MATCHES": "FOOTYSTATS",
    "PREDICTIONS": "FOOTYSTATS",
    "ODDS": "API_FOOTBALL",
    "WEATHER": "OPEN_METEO",
    "PLAYER_VALUES": "TRANSFERMARKT",
    "XG": "UNDERSTAT",
    "SFI_PROGRESSIVE_STATS": "SOCCER_FOOTBALL_INFO",
}

# Max gap (days) between consecutive failed dates before we start a new cluster.
_CLUSTER_GAP_DAYS = 30


def _cluster_dates(sorted_dates: list[date]) -> list[tuple[date, date]]:
    if not sorted_dates:
        return []
    ranges: list[tuple[date, date]] = []
    start = sorted_dates[0]
    prev = sorted_dates[0]
    for d in sorted_dates[1:]:
        if (d - prev).days > _CLUSTER_GAP_DAYS:
            ranges.append((start, prev))
            start = d
        prev = d
    ranges.append((start, prev))
    return ranges


def _load_failed(bucket: str) -> pd.DataFrame:
    from unified_trading_library import read_availability_index  # type: ignore[reportMissingImports]

    print(f"Reading manifest from gs://{bucket}/ ...", file=sys.stderr)
    df: pd.DataFrame = read_availability_index(bucket)  # type: ignore[reportUnknownMemberType]
    failed = df[df["capture_status"] == "attempted_failed"].copy()
    print(f"  {len(df):,} total rows  |  {len(failed):,} attempted_failed", file=sys.stderr)
    return failed


def _build_gap_report(
    failed: pd.DataFrame,
    min_failures: int,
) -> list[dict[str, object]]:
    failed = failed.copy()
    failed["date"] = pd.to_datetime(failed["date"])

    rows: list[dict[str, object]] = []
    for data_type, group in failed.groupby("data_type"):
        dt = str(data_type)
        dates: list[date] = sorted({d.date() for d in group["date"].dropna()})
        if len(dates) < min_failures:
            continue
        clusters = _cluster_dates(dates)
        for start, end in clusters:
            cluster_count = sum(1 for d in dates if start <= d <= end)
            if cluster_count < min_failures:
                continue
            rows.append(
                {
                    "data_type": dt,
                    "provider": _ENTITY_PROVIDER.get(dt, "API_FOOTBALL"),
                    "start": start,
                    "end": end,
                    "failed_dates": cluster_count,
                }
            )
    return sorted(rows, key=lambda r: (-int(str(r["failed_dates"])), str(r["data_type"])))


def _print_report(report: list[dict[str, object]]) -> None:
    if not report:
        print("No gaps found (all attempted_failed below min-failures threshold).")
        return

    print()
    print(f"{'DATA_TYPE':<22} {'PROVIDER':<18} {'START':>12} {'END':>12} {'FAILED_DATES':>14}")
    print("-" * 82)
    for r in report:
        print(
            f"{r['data_type']:<22} {r['provider']:<18} "
            f"{str(r['start']):>12} {str(r['end']):>12} {r['failed_dates']:>14,}"
        )
    print()
    total = sum(int(str(r["failed_dates"])) for r in report)
    print(f"Total: {len(report)} gap range(s)  |  {total:,} failed dates to retry")


def _print_launch_commands(
    report: list[dict[str, object]],
    project_id: str,
    dry_run: bool,
) -> None:
    launcher = "deployment-service/scripts/vm/launch-sports-is-gap-fill.sh"
    flag = "--dry-run" if dry_run else ""

    print()
    print("=" * 82)
    print("LAUNCH COMMANDS — run from workspace root:")
    print("=" * 82)
    print(f"# Ensure tarball is up-to-date first:")
    print(f"# bash deployment-service/scripts/vm/create-code-tarballs.sh --include instruments-service")
    print()

    for r in report:
        entity = str(r["data_type"])
        provider = str(r["provider"])
        start = str(r["start"])
        end = str(r["end"])
        n = int(str(r["failed_dates"]))
        print(f"# {entity} — {n:,} failed dates  {start} → {end}")
        print(
            f"bash {launcher} "
            f"--entity {entity} "
            f"--provider {provider} "
            f"--start-date {start} "
            f"--end-date {end} "
            f"--project {project_id} "
            f"{flag}".strip()
        )
        print()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print launch commands without creating VMs")
    parser.add_argument("--min-failures", type=int, default=5, help="Skip clusters with fewer failed dates")
    parser.add_argument("--project", default="central-element-323112", help="GCP project ID")
    parser.add_argument("--bucket", default=_BUCKET, help="Override manifest bucket name")
    args = parser.parse_args(argv)

    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("GCP_PROJECT_ID", args.project)

    failed = _load_failed(args.bucket)
    report = _build_gap_report(failed, min_failures=args.min_failures)
    _print_report(report)
    _print_launch_commands(report, project_id=args.project, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
