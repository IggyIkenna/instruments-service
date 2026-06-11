"""Unit tests for migration_orphan_sweep_sports.py (CF-17 / R8 — the sports sweep).

Credential-free + GCS-free: every test exercises the PURE classification surface
(``classify_odds_object`` / ``classify_reference_object`` / ``parse_sports_segments``
/ ``build_sports_covered_index`` / ``is_covered_sports`` / ``taxonomy_label``) on
representative REAL path shapes from both sports buckets. Covers the forced taxonomy
(A/B/B2/C/C2/D/E), the league-axis grain-aware wildcard covering, the ODDS_API
aggregate-era data_type-equivalence, the FLAT singleton, the v1-archive B2
disposition, the instrument-definitions tree, and the (E)/(D) zero-row split.

Plan refs: ``migration_verification_orphan_safety_2026_06_10.md`` § V2 (R8) +
``master_data_canonicalisation_migration_catalogue_2026_06_07.md`` R8 todo.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "migration_orphan_sweep_sports.py"
    module_name = "_migration_orphan_sweep_sports_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_script()
OC = _mod.SportsObjectClass


def _odds_index() -> object:
    rows = [
        # modern per-bookmaker trades row
        {
            "date": "2024-06-06",
            "data_type": "trades",
            "venue": "BETONLINEAG",
            "league_id": "PRIMERA_DIVISION",
            "timeframe": "",
            "capture_status": "captured",
        },
        # 2020-2023 aggregate-era row: data_type=ODDS at (day, ODDS_API, league)
        {
            "date": "2020-06-06",
            "data_type": "ODDS",
            "venue": "ODDS_API",
            "league_id": "BUNDESLIGA",
            "timeframe": "",
            "capture_status": "captured",
        },
        # processed horizon-bucket row (venue=ODDS_API provenance stamp)
        {
            "date": "2020-06-06",
            "data_type": "odds_horizon_bucket",
            "venue": "ODDS_API",
            "league_id": "BUNDESLIGA",
            "timeframe": "T-10m",
            "capture_status": "captured",
        },
        # a non-captured row must NOT seed the covered set
        {
            "date": "2024-06-07",
            "data_type": "trades",
            "venue": "PINNACLE",
            "league_id": "EPL",
            "timeframe": "",
            "capture_status": "empty_confirmed",
        },
    ]
    return _mod.build_sports_covered_index(rows)


def _ref_index() -> object:
    rows = [
        {
            "date": "2026-05-01",
            "data_type": "FIXTURES",
            "venue": "API_FOOTBALL",
            "league_id": "EPL",
            "timeframe": "",
            "capture_status": "captured",
        },
        # league-blank (coarser manifest) WEATHER row — wildcard over leagues
        {
            "date": "2026-05-01",
            "data_type": "WEATHER",
            "venue": "open_meteo",
            "league_id": "",
            "timeframe": "",
            "capture_status": "captured",
        },
        # FLAT singleton coverage: any captured VENUES row
        {
            "date": "2024-09-11",
            "data_type": "VENUES",
            "venue": "",
            "league_id": "",
            "timeframe": "",
            "capture_status": "captured",
        },
        {
            "date": "2018-01-02",
            "data_type": "FIXTURE_STATS",
            "venue": "",
            "league_id": "EPL",
            "timeframe": "",
            "capture_status": "captured",
        },
        # PER_DAY_PER_SEASON entity (season is path-only — not a manifest axis)
        {
            "date": "2026-05-01",
            "data_type": "PLAYER_VALUES",
            "venue": "transfermarkt",
            "league_id": "",
            "timeframe": "",
            "capture_status": "captured",
        },
    ]
    index = _mod.build_sports_covered_index(rows)
    index.definition_days.add(("2026-03-21", "API_FOOTBALL"))
    return index


# ---------------------------------------------------------------------------
# Covered-index semantics
# ---------------------------------------------------------------------------


def test_covered_exact_trades_row() -> None:
    assert _mod.is_covered_sports(
        _odds_index(),
        day="2024-06-06",
        data_type="trades",
        venue="BETONLINEAG",
        source="ODDS_API",
        league="PRIMERA_DIVISION",
    )


def test_covered_via_aggregate_era_odds_row() -> None:
    # bookmaker object on a 2020 day covered by the (ODDS_API, league) aggregate row
    assert _mod.is_covered_sports(
        _odds_index(),
        day="2020-06-06",
        data_type="trades",
        venue="BETVICTOR",
        source="ODDS_API",
        league="BUNDESLIGA",
    )


def test_venue_is_identity_when_both_sides_carry_one() -> None:
    # a different bookmaker's row never covers this object
    assert not _mod.is_covered_sports(
        _odds_index(),
        day="2024-06-06",
        data_type="trades",
        venue="PINNACLE",
        source="",
        league="PRIMERA_DIVISION",
    )


def test_non_captured_rows_do_not_cover() -> None:
    assert not _mod.is_covered_sports(
        _odds_index(), day="2024-06-07", data_type="trades", venue="PINNACLE", league="EPL"
    )


def test_league_blank_manifest_is_wildcard() -> None:
    assert _mod.is_covered_sports(_ref_index(), day="2026-05-01", data_type="WEATHER", league="EPL")


def test_blank_object_league_matches_finer_manifest() -> None:
    # the bare entity={F}/{F}.parquet aggregate is a probe target for every league cell
    assert _mod.is_covered_sports(_ref_index(), day="2026-05-01", data_type="FIXTURES", league="")


def test_flat_singleton_covered_by_any_captured_row() -> None:
    assert _mod.is_covered_sports(_ref_index(), day="", data_type="VENUES")
    assert not _mod.is_covered_sports(_ref_index(), day="", data_type="TEAMS")


# ---------------------------------------------------------------------------
# MDPS odds bucket classification
# ---------------------------------------------------------------------------

_ODDS_OBJ = (
    "raw_tick_data/by_date/day=2024-06-06/category=sports/data_source=ODDS_API/"
    "venue=BETONLINEAG/league_id=PRIMERA_DIVISION/instrument_type=odds/data_type=trades/ticks.parquet"
)


def test_odds_object_manifested_is_class_a() -> None:
    cls, fields, _ = _mod.classify_odds_object(_ODDS_OBJ, _odds_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[0] == "BETONLINEAG" and fields[1] == "PRIMERA_DIVISION" and fields[3] == "trades"


def test_odds_object_aggregate_era_is_class_a() -> None:
    path = (
        "raw_tick_data/by_date/day=2020-06-06/category=sports/data_source=ODDS_API/"
        "venue=BETVICTOR/league_id=BUNDESLIGA/instrument_type=odds/data_type=trades/ticks.parquet"
    )
    cls, _fields, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED


def test_odds_object_uncovered_is_class_e_then_d_on_zero_rows() -> None:
    path = (
        "raw_tick_data/by_date/day=2024-06-06/category=sports/data_source=ODDS_API/"
        "venue=PINNACLE/league_id=EPL/instrument_type=odds/data_type=trades/ticks.parquet"
    )
    cls, _f, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL
    cls, _f, reason = _mod.classify_odds_object(path, _odds_index(), is_parquet=True, row_count=0)
    assert cls is OC.JUNK and "zero-row" in reason


def test_processed_horizon_bucket_covered() -> None:
    path = (
        "processed/by_date/day=2020-06-06/data_type=odds_horizon_bucket/"
        "league_id=BUNDESLIGA/timeframe=T-10m/bucketed.parquet"
    )
    cls, fields, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[6] == "T-10m"


def test_processed_horizon_bucket_wrong_timeframe_is_e() -> None:
    path = (
        "processed/by_date/day=2020-06-06/data_type=odds_horizon_bucket/"
        "league_id=BUNDESLIGA/timeframe=T-24h/bucketed.parquet"
    )
    cls, _f, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL


def test_odds_infra_and_tooling_labels() -> None:
    assert _mod.classify_odds_object("_index/availability_index.parquet", _odds_index(), is_parquet=True)[0] is (
        OC.MANIFEST_INFRA
    )
    assert _mod.classify_odds_object("scripts/oddspapi_runner.py", _odds_index(), is_parquet=False)[0] is OC.NON_DATA
    assert _mod.classify_odds_object("_vm_staging/code.tar.gz", _odds_index(), is_parquet=False)[0] is OC.NON_DATA


def test_wire_league_map_derived_from_uac() -> None:
    assert _mod.canonical_league_token("soccer_epl") == "EPL"
    assert _mod.canonical_league_token("EPL") == "EPL"  # identity for canonical tokens
    assert _mod.canonical_league_token("UNMAPPED_TOKEN") == "UNMAPPED_TOKEN"


def test_legacy_2022_source_league_shape_covered() -> None:
    # source=/league= aliases + missing data_type inferred as trades; covered by the
    # aggregate-era ODDS row for that (day, league)
    path = "raw_tick_data/by_date/day=2020-06-06/source=ODDS_API/league=BUNDESLIGA/ticks.parquet"
    cls, fields, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[3] == "trades"


def test_legacy_2025_wire_league_shape_covered() -> None:
    # venue=ODDS_API + ODDS_API wire league key (soccer_*) remapped to canonical
    rows = [
        {
            "date": "2025-07-31",
            "data_type": "ODDS",
            "venue": "ODDS_API",
            "league_id": "EPL",
            "timeframe": "",
            "capture_status": "captured",
        }
    ]
    index = _mod.build_sports_covered_index(rows)
    path = "raw_tick_data/by_date/day=2025-07-31/venue=ODDS_API/league=soccer_epl/ticks.parquet"
    cls, fields, _ = _mod.classify_odds_object(path, index, is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[1] == "EPL"
    # the bare no-league aggregate file the same day is covered too (league wildcard)
    bare = "raw_tick_data/by_date/day=2025-07-31/venue=ODDS_API/ticks.parquet"
    assert _mod.classify_odds_object(bare, index, is_parquet=True)[0] is OC.CANONICAL_MANIFESTED


def test_new_pipeline_mode_shape_parses_and_orphans_without_row() -> None:
    # the 2026-06 canonical pipeline_mode=batch_odds_api shape (R5-probe writes)
    path = (
        "raw_tick_data/by_date/day=2026-06-09/pipeline_mode=batch_odds_api/asset_group=sports/"
        "venue=BETFAIR_EX_EU/league_id=SEGUNDA_DIVISION/instrument_type=odds/data_type=trades/ticks.parquet"
    )
    cls, fields, _ = _mod.classify_odds_object(path, _odds_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL
    assert fields[0] == "BETFAIR_EX_EU" and fields[1] == "SEGUNDA_DIVISION"


def test_odds_unknown_prefix_is_junk_and_unknown_taxonomy() -> None:
    cls, _f, reason = _mod.classify_odds_object("mystery/blob.parquet", _odds_index(), is_parquet=True)
    assert cls is OC.JUNK and "unlabelled" in reason
    assert _mod.taxonomy_label("mystery/blob.parquet", _mod.ODDS_PREFIXES) == "unknown:mystery/"
    assert _mod.taxonomy_label(_ODDS_OBJ, _mod.ODDS_PREFIXES) == "service-data"


# ---------------------------------------------------------------------------
# Reference bucket classification
# ---------------------------------------------------------------------------


def test_reference_canonical_per_league_is_class_a() -> None:
    path = "sports_reference/by_date/day=2026-05-01/entity=fixtures/league=EPL/fixtures.parquet"
    cls, fields, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[1] == "EPL" and fields[2] == "fixtures" and fields[3] == "FIXTURES"


def test_reference_bare_aggregate_covered_by_finer_manifest() -> None:
    path = "sports_reference/by_date/day=2026-05-01/entity=fixtures/fixtures.parquet"
    cls, _f, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED


def test_reference_extra_hive_segment_tolerated() -> None:
    # the footystats_odds fetched_at_hour= intermediate segment must not break parsing
    path = (
        "sports_reference/by_date/day=2026-05-01/entity=fixtures/"
        "fetched_at_hour=2026-04-29T09/league=EPL/fixtures.parquet"
    )
    cls, _f, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED


def test_reference_uncovered_day_is_class_e() -> None:
    path = "sports_reference/by_date/day=2026-05-02/entity=fixtures/league=EPL/fixtures.parquet"
    cls, _f, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL


def test_reference_player_values_season_layout() -> None:
    path = "sports_reference/by_date/day=2026-05-01/entity=player_values/season=2026/player_values.parquet"
    cls, fields, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED
    assert fields[5] == "2026"


def test_reference_v2_tree_covered_is_legacy_duplicate() -> None:
    path = "sports_reference_v2/by_date/day=2018-01-02/entity=fixture_stats/fixture_stats.parquet"
    cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.LEGACY_DUPLICATE and "v2" in reason


def test_reference_v2_tree_uncovered_is_class_e() -> None:
    path = "sports_reference_v2/by_date/day=2018-01-05/entity=fixtures/fixtures.parquet"
    cls, _f, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL


def test_v1_archive_is_b2_never_e() -> None:
    path = "sports_reference_v1_archive/by_date/day=2018-01-05/entity=fixtures/fixtures.parquet"
    cls, _f, _ = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.V1_ARCHIVE_SUPERSEDED


def test_flat_singleton_venues_is_class_a() -> None:
    path = "sports_reference/venues/venues.parquet"
    cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED and "FLAT" in reason


def test_legacy_flat_by_day_twin_is_class_b() -> None:
    path = "sports_reference/fixtures/day=2026-05-01/league=EPL/fixtures.parquet"
    cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.LEGACY_DUPLICATE and "legacy flat-by-day" in reason


def test_reference_aux_mapping_corpora_are_c2() -> None:
    for path in (
        "sports_reference/mappings/fixture_mapping.parquet",
        "sports_reference/teams_in_league/season=2020/teams.parquet",
        "sports_reference/footystats_league_ids/season=2019/ids.parquet",
    ):
        cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
        assert cls is OC.NON_DATA and "reference-aux" in reason


def test_retired_entity_is_c2_labelled() -> None:
    path = "sports_reference/by_date/day=2026-05-01/entity=transfermarkt_leagues/transfermarkt_leagues.parquet"
    cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.NON_DATA and "retired" in reason


def test_unknown_entity_folder_is_junk() -> None:
    path = "sports_reference/by_date/day=2026-05-01/entity=mystery_entity/mystery_entity.parquet"
    cls, _f, reason = _mod.classify_reference_object(path, _ref_index(), is_parquet=True)
    assert cls is OC.JUNK and "not in SPORTS_DATA_TYPE_TO_FOLDER" in reason


def test_definitions_tree_covered_and_orphan() -> None:
    covered = "day=2026-03-21/venue=API_FOOTBALL/abc123.parquet"
    cls, fields, _ = _mod.classify_reference_object(covered, _ref_index(), is_parquet=True)
    assert cls is OC.CANONICAL_MANIFESTED and fields[3] == "instrument_definitions"
    stray = "day=2026-03-21/venue=BETFAIR/5e72da0d.parquet"
    cls, _f, reason = _mod.classify_reference_object(stray, _ref_index(), is_parquet=True)
    assert cls is OC.ORPHAN_REAL and "availability row" in reason
    cls, _f, _ = _mod.classify_reference_object(stray, _ref_index(), is_parquet=True, row_count=0)
    assert cls is OC.JUNK


def test_reference_infra_labels_and_taxonomy() -> None:
    for path, label in (
        ("_index/availability_index.parquet", "manifest-infra"),
        ("availability_index/instruments-service.parquet", "manifest-infra"),
        ("instrument_availability/by-date/day-2020-06-06/x.json", "manifest-infra"),
        ("_catalogue/instruments-service/catalog.parquet", "catalogue-infra"),
        ("_audits/fixtures_diff.csv", "run-artifact"),
        ("_vm_staging/codebase.tar.gz", "staging"),
        ("sports_reference_v1_archive/by_date/day=2018-01-05/entity=fixtures/fixtures.parquet", "v1-archive"),
    ):
        assert _mod.taxonomy_label(path, _mod.REFERENCE_PREFIXES) == label
    assert _mod.taxonomy_label("unexpected/thing.bin", _mod.REFERENCE_PREFIXES) == "unknown:unexpected/"


def test_parse_sports_segments() -> None:
    seg = _mod.parse_sports_segments(_ODDS_OBJ)
    assert seg["day"] == "2024-06-06"
    assert seg["venue"] == "BETONLINEAG"
    assert seg["data_source"] == "ODDS_API"
    assert seg["league_id"] == "PRIMERA_DIVISION"
    assert seg["data_type"] == "trades"
