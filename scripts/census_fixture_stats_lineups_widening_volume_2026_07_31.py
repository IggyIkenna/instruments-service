#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the FIXTURE_STATS/FIXTURE_LINEUPS all-leagues widening backfill launches
"""Census the (date, league_id) shard volume the FIXTURE_STATS/FIXTURE_LINEUPS
all-leagues widening backfill (sports_satellite_ao_dispatch_batch2_2026_07_24.md
todo, SPORTS_ENTITY_LEAGUE_COVERAGE widening) still needs to fetch.

Single-walk discipline: ONE read of the consolidated
``_index/availability_index.parquet`` (same pattern as
``census_fixture_events_schema_variants_2026_07_25.py`` /
deployment-api's ``_axis_census.py``) -- never a raw per-object GCS walk.
Reports EXACT (date, league_id) shard counts (the manifest's own capture
granularity); converts to an ESTIMATED fixture-call count via the empirical
fixtures-per-shard ratio already measured by the fixture_events recovery
campaign (19,850 fixtures / 4,327 shards = ~4.587/shard), since the manifest
tracks shard capture, not per-fixture row counts, and re-deriving that ratio
from raw parquet content would be exactly the kind of full-corpus walk this
script exists to avoid.

A shard is "resolved" (not needed) once its capture_status is EITHER
`captured` (real data) OR `empty_confirmed` (writer already checked and
recorded honest absence, e.g. EXPECTED_NO_PROVIDER_COVERAGE) -- treating
only `captured` as done (2026-07-31 first cut of this script) silently
counted 266,758 already-resolved FIXTURE_STATS empty_confirmed shards
(vs. only 37,024 genuinely captured) as still needed, and the analogous
gap for FIXTURE_LINEUPS. Found 2026-08-04 after a real, multi-hour
FIXTURE_STATS backfill run showed zero shard-count movement despite
genuine fetch activity. SSOT: plans/active/issues/sports_af_full_entity_completion_2026_08_03.md.

Manifest read routes through the UTL storage client (not a bare
pandas/pyarrow ``gs://`` reader) -- a bare native read resolves ADC on a
separate code path that has gone stale mid-session before
(``census_fixture_events_schema_variants_2026_07_25.py``'s 2026-08-03
finding) and is markedly slower under concurrent-read contention.
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import get_mvp_football_league_ids
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
DATA_FLOOR = "2020-06-06"  # SSOT: codex/02-data/sports-2020-06-data-floor.md
FIXTURES_PER_SHARD = 19850 / 4327  # empirical, sports_fixture_events_refetch_progress_2026_07_25.md 2026-07-30 census

# Schedule-defining data_type is NOT a single label: the writer migrated
# "FIXTURES" -> "FIXTURES_SCHEDULE" mid-history (2026-07-14 cutover, SSOT
# unified_api_contracts.canonical.crosscutting._honest_coverage_logic
# .SCHEDULE_DEFINING_DATA_TYPES) -- querying "FIXTURES" alone silently drops
# ~90% of the real denominator (a first pass of this script did exactly that,
# undercounting distinct non-MVP leagues 567 vs the true 749).
SCHEDULE_DEFINING_DATA_TYPES = frozenset({"FIXTURES", "FIXTURES_SCHEDULE"})


def main() -> int:
    mvp = get_mvp_football_league_ids()
    print(f"MVP league count: {len(mvp)}", file=sys.stderr)
    today = datetime.now(UTC).date().isoformat()

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    manifest = pd.read_parquet(io.BytesIO(raw), columns=["date", "league_id", "data_type", "capture_status"])
    # Cap at today: FIXTURES_SCHEDULE already carries months of future
    # scheduled fixtures, which cannot have STATS/LINEUPS yet (unplayed).
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    print(f"manifest rows (post-floor, <=today): {len(manifest)}", file=sys.stderr)

    captured = manifest[manifest["capture_status"] == "captured"]
    resolved = manifest[manifest["capture_status"].isin(("captured", "empty_confirmed"))]

    fixtures = captured[captured["data_type"].isin(SCHEDULE_DEFINING_DATA_TYPES)].drop_duplicates(
        subset=["date", "league_id"]
    )
    fixtures_non_mvp = fixtures[~fixtures["league_id"].isin(mvp)]
    distinct_non_mvp_leagues_with_fixtures = fixtures_non_mvp["league_id"].nunique()

    results: dict[str, dict[str, int]] = {}
    for entity in ("FIXTURE_STATS", "FIXTURE_LINEUPS"):
        ent = resolved[resolved["data_type"] == entity].drop_duplicates(subset=["date", "league_id"])
        ent_non_mvp = ent[~ent["league_id"].isin(mvp)]
        already_shards = set(zip(ent_non_mvp["date"], ent_non_mvp["league_id"], strict=False))
        needed_shards = set(zip(fixtures_non_mvp["date"], fixtures_non_mvp["league_id"], strict=False)) - already_shards
        results[entity] = {
            "already_resolved_non_mvp_shards": len(already_shards),
            "needed_non_mvp_shards": len(needed_shards),
        }

    print("\n=== FIXTURE_STATS/FIXTURE_LINEUPS non-MVP widening volume census ===", file=sys.stderr)
    print(
        f"Distinct non-MVP leagues with >=1 captured schedule shard: {distinct_non_mvp_leagues_with_fixtures}",
        file=sys.stderr,
    )
    print(
        f"Total non-MVP (date, league_id) schedule shards ({DATA_FLOOR}..{today}): {len(fixtures_non_mvp)}",
        file=sys.stderr,
    )
    total_needed_shards = 0
    for entity, r in results.items():
        print(f"\n{entity}:", file=sys.stderr)
        print(
            f"  already resolved (non-MVP, captured+empty_confirmed): {r['already_resolved_non_mvp_shards']} shards",
            file=sys.stderr,
        )
        print(f"  still needed (non-MVP):     {r['needed_non_mvp_shards']} shards", file=sys.stderr)
        total_needed_shards += r["needed_non_mvp_shards"]

    est_calls = total_needed_shards * FIXTURES_PER_SHARD
    print(f"\nTotal needed shards across both entities: {total_needed_shards}", file=sys.stderr)
    print(f"Empirical fixtures/shard ratio: {FIXTURES_PER_SHARD:.4f}", file=sys.stderr)
    print(f"Estimated total API calls needed: {est_calls:,.0f}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
