#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the full AF-entity completion campaign closes (0 needed shards remaining across all entities)
"""Census EVERY API-Football entity's completion against its own scope
(SPORTS_ENTITY_LEAGUE_COVERAGE: all-383 or MVP-96), so the sports satellite
campaign can genuinely finish "everything AF-related" rather than only the
3 entities (FIXTURE_EVENTS/FIXTURE_STATS/FIXTURE_LINEUPS) already tracked.

Denominator per entity = distinct (date, league_id) pairs with a captured
FIXTURES/FIXTURES_SCHEDULE row (a genuine fixture existed), intersected with
that entity's own league-coverage scope (get_entity_league_coverage) --
mirrors emit_empty_gaps_for_entity's own expected-set logic
(sports_reference_core.py:338-341), so this reports the same "needed" gap
the writer's own empty-gap emission targets.

Single-walk discipline: ONE UTL-client-backed read of the consolidated
availability_index.parquet (see census_fixture_events_schema_variants_2026_07_25.py
for the ADC-staleness gotcha this avoids).
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
SCHEDULE_DEFINING_DATA_TYPES = frozenset({"FIXTURES", "FIXTURES_SCHEDULE"})

# entity -> MVP-only (True) or all-383 (False), per SPORTS_ENTITY_LEAGUE_COVERAGE
# LEAGUES excluded: writer path retired 2026-05-07 (manifest_migration_master C.1 audit) and
# its shard atom never had a league_id axis (SportsPathLayout.PER_DAY_BARE) -- a per-(date,
# league_id) denominator is categorically wrong for it. See
# plans/active/issues/sports_af_full_entity_completion_2026_08_03.md for the full verdict.
ENTITIES: dict[str, bool] = {
    "PLAYER_STATS": True,
    "INJURIES": False,
    "STANDINGS": False,
    "TEAMS": False,
}


def main() -> int:
    mvp = get_mvp_football_league_ids()
    today = datetime.now(UTC).date().isoformat()

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    manifest = pd.read_parquet(io.BytesIO(raw), columns=["date", "league_id", "data_type", "capture_status"])
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    print(f"manifest rows (post-floor, <=today): {len(manifest)}", file=sys.stderr)

    captured = manifest[manifest["capture_status"] == "captured"]
    fixtures = captured[captured["data_type"].isin(SCHEDULE_DEFINING_DATA_TYPES)].drop_duplicates(
        subset=["date", "league_id"]
    )
    fixtures_all = set(zip(fixtures["date"], fixtures["league_id"], strict=False))
    fixtures_mvp_only = set(
        zip(
            fixtures[fixtures["league_id"].isin(mvp)]["date"],
            fixtures[fixtures["league_id"].isin(mvp)]["league_id"],
            strict=False,
        )
    )
    print(f"Total genuine fixture-day shards (all leagues): {len(fixtures_all)}", file=sys.stderr)
    print(f"Total genuine fixture-day shards (MVP-only leagues): {len(fixtures_mvp_only)}", file=sys.stderr)

    print("\n=== Full AF-entity completion census (2026-08-03) ===", file=sys.stderr)
    grand_total_needed = 0
    for entity, mvp_only in ENTITIES.items():
        denom = fixtures_mvp_only if mvp_only else fixtures_all
        ent_rows = captured[captured["data_type"] == entity].drop_duplicates(subset=["date", "league_id"])
        ent_shards = set(zip(ent_rows["date"], ent_rows["league_id"], strict=False))
        needed = denom - ent_shards
        scope_label = "MVP-96" if mvp_only else "all-383"
        print(
            f"\n{entity} (scope={scope_label}): expected={len(denom)} already_captured={len(ent_shards & denom)} "
            f"needed={len(needed)}",
            file=sys.stderr,
        )
        grand_total_needed += len(needed)

    print(f"\nGrand total needed shards across all {len(ENTITIES)} entities: {grand_total_needed}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
