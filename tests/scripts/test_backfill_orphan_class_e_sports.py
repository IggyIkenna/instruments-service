"""Unit tests for backfill_orphan_class_e_sports.py (R8 — the sports recorder).

Credential-free + GCS-free: exercises the PURE surfaces — pipeline_mode convention
(the operator-ratified ``backfill_sports_per_entity_manifest`` mapping incl. the
ODDS→BATCH_ODDS_API special case), report-row re-verification against a synthetic
covered index, and cell grouping (entity cells vs the instrument-definitions split).

Plan refs: ``migration_verification_orphan_safety_2026_06_10.md`` § V2 (R8) +
``master_data_canonicalisation_migration_catalogue_2026_06_07.md`` R8 todo.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from unified_api_contracts import PipelineMode


def _load(stem: str) -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(stem, repo_root / "scripts" / f"{stem}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[stem] = module
    spec.loader.exec_module(module)
    return module


_mod = _load("backfill_orphan_class_e_sports")
_sweep = _load("migration_orphan_sweep_sports")

_BUCKET = "instruments-store-sports-prd-test"


def _row(uri_path: str, **kw: str) -> dict[str, str]:
    base = {
        "uri": f"gs://{_BUCKET}/{uri_path}",
        "entity": "fixtures",
        "data_type": "FIXTURES",
        "league_id": "EPL",
        "day": "2026-05-02",
        "venue": "",
        "timeframe": "",
        "size_bytes": "1000",
    }
    base.update(kw)
    return base


def test_source_and_mode_resolution() -> None:
    # SOURCE_PRIORITY-allowed entity-map source is kept; mode follows source
    assert _mod.resolve_source_and_mode("ODDS", "footystats") == ("footystats", PipelineMode.BATCH_FOOTYSTATS)
    assert _mod.resolve_source_and_mode("PREDICTIONS", "footystats") == ("footystats", PipelineMode.BATCH_FOOTYSTATS)
    assert _mod.resolve_source_and_mode("FIXTURES", "api_football") == ("api_football", PipelineMode.BATCH_API_FOOTBALL)
    assert _mod.resolve_source_and_mode("WEATHER", "open_meteo") == ("open_meteo", PipelineMode.BATCH_OPEN_METEO)
    # entity-map source OUTSIDE SOURCE_PRIORITY → the registered primary wins
    # (STANDINGS/TEAMS: entity map says footystats; UAC allows only api_football)
    assert _mod.resolve_source_and_mode("STANDINGS", "footystats") == (
        "api_football",
        PipelineMode.BATCH_API_FOOTBALL,
    )
    assert _mod.resolve_source_and_mode("TEAMS", "footystats") == ("api_football", PipelineMode.BATCH_API_FOOTBALL)


def test_reverify_drops_now_covered_rows() -> None:
    index = _sweep.build_sports_covered_index(
        [
            {
                "date": "2026-05-01",
                "data_type": "FIXTURES",
                "venue": "",
                "league_id": "EPL",
                "timeframe": "",
                "capture_status": "captured",
            }
        ]
    )
    index.definition_days.add(("2026-03-21", "API_FOOTBALL"))
    rows = [
        _row("a.parquet", day="2026-05-01"),  # covered since the report was written
        _row("b.parquet", day="2026-05-02"),  # still orphan
        _row(
            "day=2026-03-21/venue=API_FOOTBALL/x.parquet",
            entity="instrument_definitions",
            data_type="instrument_definitions",
            day="2026-03-21",
            venue="API_FOOTBALL",
        ),  # covered by the definitions availability surface
        _row(
            "day=2026-03-21/venue=BETFAIR/y.parquet",
            entity="instrument_definitions",
            data_type="instrument_definitions",
            day="2026-03-21",
            venue="BETFAIR",
        ),  # still orphan
    ]
    still, covered = _mod.reverify(_sweep, index, rows)
    assert covered == 2
    assert [r["day"] for r in still] == ["2026-05-02", "2026-03-21"]


def test_build_cells_groups_by_day_dt_league_and_splits_definitions() -> None:
    rows = [
        # two fetched_at_hour twins of the SAME (day, ODDS, EPL) cell
        _row(
            "sports_reference/by_date/day=2018-01-01/entity=footystats_odds/fetched_at_hour=A/league=EPL/footystats_odds.parquet",
            entity="footystats_odds",
            data_type="ODDS",
            day="2018-01-01",
        ),
        _row(
            "sports_reference/by_date/day=2018-01-01/entity=footystats_odds/fetched_at_hour=B/league=EPL/footystats_odds.parquet",
            entity="footystats_odds",
            data_type="ODDS",
            day="2018-01-01",
        ),
        _row(
            "sports_reference/by_date/day=2018-01-01/entity=fixtures/league=EPL/fixtures.parquet",
            day="2018-01-01",
        ),
        _row(
            "day=2026-03-21/venue=BETFAIR/y.parquet",
            entity="instrument_definitions",
            data_type="instrument_definitions",
            day="2026-03-21",
            venue="BETFAIR",
        ),
    ]
    cells, definition_rows = _mod.build_cells(_BUCKET, rows)
    assert len(cells) == 2
    odds_cell = next(c for c in cells if c.data_type == "ODDS")
    assert len(odds_cell.members) == 2  # twins grouped into ONE cell
    assert odds_cell.source == "footystats"
    assert all(not m[0].startswith("gs://") for c in cells for m in c.members)  # bucket prefix stripped
    assert len(definition_rows) == 1 and definition_rows[0]["venue"] == "BETFAIR"
