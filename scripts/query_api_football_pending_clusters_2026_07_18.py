#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: temporary
# Delete-when: api_football_backfill_chronological_scan_never_reaches_pending_tail-001 lands its narrow relaunch
"""Cluster api_football pending_fetch cells into contiguous date ranges per entity.

Read the sports availability manifest (slim columns), filter to
source=api_football, capture_status=pending_fetch (expected_unattempted still
requiring a fetch attempt), and cluster the pending dates per data_type into
contiguous ranges. Emits a summary table + ready-to-paste narrow-window launch
commands for deployment-service/scripts/vm/launch-api-football-backfill-vm.sh.

Usage:
    python3 scripts/query_api_football_pending_clusters_2026_07_18.py [--project PROJECT_ID]

Prerequisites: GCP ADC credentials with read access to instruments-store-sports-prd-*.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import pandas as pd

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_ENTITIES = ["FIXTURE_EVENTS", "FIXTURE_LINEUPS", "FIXTURE_STATS", "PLAYER_STATS"]
_CLUSTER_GAP_DAYS = 14


def _cluster_dates(sorted_dates: list[date]) -> list[tuple[date, date, int]]:
    if not sorted_dates:
        return []
    ranges: list[tuple[date, date, int]] = []
    start = sorted_dates[0]
    prev = sorted_dates[0]
    count = 1
    for d in sorted_dates[1:]:
        if (d - prev).days > _CLUSTER_GAP_DAYS:
            ranges.append((start, prev, count))
            start = d
            count = 0
        prev = d
        count += 1
    ranges.append((start, prev, count))
    return ranges


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="central-element-323112", help="GCP project ID")
    parser.add_argument("--bucket", default=_BUCKET, help="Override manifest bucket name")
    args = parser.parse_args(argv)

    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("GCP_PROJECT_ID", args.project)

    from unified_trading_library import read_availability_index  # type: ignore[reportMissingImports]

    print(f"Reading manifest (slim columns) from gs://{args.bucket}/ ...", file=sys.stderr)
    df: pd.DataFrame = read_availability_index(  # type: ignore[reportUnknownMemberType]
        args.bucket,
        columns=["date", "data_type", "source", "capture_status", "error_reason"],
    )
    print(f"  {len(df):,} total rows read", file=sys.stderr)

    af = df[(df["source"] == "api_football") & (df["data_type"].isin(_ENTITIES))].copy()
    # pending_fetch = expected_unattempted rows whose error_reason does NOT start with
    # "EXPECTED_" (a typed honest-absence) — SSOT: codex/02-data/availability-manifest-and-data-status.md
    # CaptureStatusCounts.expected_unattempted_pending_fetch split.
    is_eu = af["capture_status"] == "expected_unattempted"
    reason = af["error_reason"].fillna("")
    pending = af[is_eu & ~reason.str.startswith("EXPECTED_")].copy()
    print(f"  {len(af):,} api_football enrichment rows | {len(pending):,} pending_fetch", file=sys.stderr)

    if pending.empty:
        print("No pending_fetch cells for the 4 enrichment entities — gate may already be closed.")
        return

    pending["date"] = pd.to_datetime(pending["date"])

    all_rows: list[dict[str, object]] = []
    for entity in _ENTITIES:
        ent_dates: list[date] = sorted({d.date() for d in pending.loc[pending["data_type"] == entity, "date"].dropna()})
        total = len(ent_dates)
        if total == 0:
            print(f"{entity}: 0 pending dates")
            continue
        clusters = _cluster_dates(ent_dates)
        clusters.sort(key=lambda c: -c[2])
        print(f"\n{entity}: {total} distinct pending dates across {len(clusters)} cluster(s)")
        for start, end, n in clusters:
            pct = 100.0 * n / total
            print(f"  {start} .. {end}  ({n} dates, {pct:.1f}% of {entity} pending mass)")
            all_rows.append({"entity": entity, "start": start, "end": end, "count": n})

    print("\n" + "=" * 90)
    print("NARROW-WINDOW LAUNCH COMMANDS (top clusters per entity):")
    print("=" * 90)
    by_entity: dict[str, list[dict[str, object]]] = {}
    for r in all_rows:
        by_entity.setdefault(str(r["entity"]), []).append(r)
    for entity, rows in by_entity.items():
        rows.sort(key=lambda r: -int(str(r["count"])))
        for r in rows[:3]:
            print(
                f"# {entity} {r['start']}..{r['end']} ({r['count']} dates)\n"
                f"bash deployment-service/scripts/vm/launch-api-football-backfill-vm.sh "
                f"--skip-lock --entity {entity} --fleet-vms <N> {r['start']} {r['end']}"
            )


if __name__ == "__main__":
    main()
