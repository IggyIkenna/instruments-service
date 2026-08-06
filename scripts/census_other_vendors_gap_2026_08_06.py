#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the footystats/weather/sfi/odds_api backfill campaign closes (0 needed shards remaining)
"""Census the other sports vendors' entities against each vendor's own
Prediction-tier league scope -- NOT api_football's expanded 96-league MVP list
(operator ruling 2026-08-06: "its api football that has an expanded list" --
weather/sfi/footystats/understat's "MVP" is their Prediction-tier leagues via
get_expected_leagues_for_source(source, classifications=["Prediction"])).

Mirrors census_all_af_entities_completion_2026_08_03.py's denominator logic
(distinct (date, league_id) with a captured FIXTURES/FIXTURES_SCHEDULE row,
intersected with the entity's own coverage set) and its resolved-status fix
(captured OR empty_confirmed).

Single-walk discipline: ONE UTL-client-backed read of the consolidated
availability_index.parquet, same as the AF census script.
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import get_expected_leagues_for_source
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
DATA_FLOOR = "2020-06-06"  # SSOT: codex/02-data/sports-2020-06-data-floor.md
SCHEDULE_DEFINING_DATA_TYPES = frozenset({"FIXTURES", "FIXTURES_SCHEDULE"})

# entity -> source_key (LeagueDefinition.data_sources key) for the Prediction-tier lookup
ENTITY_SOURCE: dict[str, str] = {
    "WEATHER": "open_meteo",
    "SFI_LEAGUES": "soccer_football_info",
    "SFI_PROGRESSIVE_STATS": "soccer_football_info",
    "MATCHES": "footystats",
    "PREDICTIONS": "footystats",
    "ODDS": "footystats",
    "XG": "understat",
    "XG_SHOTS": "understat",
}


def main() -> int:
    today = datetime.now(UTC).date().isoformat()

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, "_index/availability_index.parquet")
    manifest = pd.read_parquet(io.BytesIO(raw), columns=["date", "league_id", "data_type", "capture_status"])
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    print(f"manifest rows (post-floor, <=today): {len(manifest)}", file=sys.stderr)

    captured = manifest[manifest["capture_status"] == "captured"]
    resolved = manifest[manifest["capture_status"].isin(("captured", "empty_confirmed"))]
    fixtures = captured[captured["data_type"].isin(SCHEDULE_DEFINING_DATA_TYPES)].drop_duplicates(
        subset=["date", "league_id"]
    )

    print("\n=== Other-vendor entity gap census (2026-08-06), per-source Prediction-tier scope ===", file=sys.stderr)
    for entity, source_key in ENTITY_SOURCE.items():
        pred_leagues = frozenset(lg.league_id for lg in get_expected_leagues_for_source(source_key, ["Prediction"]))
        denom_fixtures = fixtures[fixtures["league_id"].isin(pred_leagues)]
        scope_label = f"{source_key} Prediction-tier ({len(pred_leagues)} leagues)"
        denom = set(zip(denom_fixtures["date"], denom_fixtures["league_id"], strict=False))

        ent_rows = resolved[resolved["data_type"] == entity].drop_duplicates(subset=["date", "league_id"])
        ent_shards = set(zip(ent_rows["date"], ent_rows["league_id"], strict=False))
        needed = denom - ent_shards
        needed_dates = sorted({d for d, _lg in needed})
        date_range = f"{needed_dates[0]}..{needed_dates[-1]}" if needed_dates else "(none)"
        print(
            f"\n{entity} (scope={scope_label}): expected={len(denom)} already_resolved={len(ent_shards & denom)} "
            f"needed={len(needed)} needed_date_range={date_range}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
