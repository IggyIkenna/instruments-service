#!/usr/bin/env python3
"""migration_orphan_sweep_sports.py — the SPORTS GCS→manifest orphan sweep (R8).

Sports is the one asset group EXCLUDED from ``migration_orphan_sweep.py`` by design:
sports paths are NOT hive-keyed (``canonical_path_templates('sports') == []``). The
path SSOT is ``unified_api_contracts.sports.candidate_parquet_paths()`` —
``sports_reference/by_date/day={D}/entity={F}/[league={L}|season={S}]/{F}.parquet``
plus the FLAT singletons — and the identity axis is **league**, not venue/chain.

This sweep walks BOTH sports buckets ONCE each (single-walk discipline) and forces
every object into exactly one class (the hive sweep's A/B/C/C2/D/E taxonomy, plus the
sports-only B2 disposition for the operator-verified v1 archive):

  (A)  CANONICAL_MANIFESTED   — SSOT-shape path, a captured manifest row covers it
  (B)  LEGACY_DUPLICATE       — legacy-shape path whose cell IS manifested
  (B2) V1_ARCHIVE_SUPERSEDED  — ``sports_reference_v1_archive/`` — row-coverage gate
                                proven GREEN 2026-06-11 (398/398 days, 72,522/72,522
                                rows covered via source_fixture_id↔af_fixture_id);
                                its OWN disposition, never (E) — G4.5 delete-list
                                candidate, operator-gated, nothing dropped here
  (C)  MANIFEST_INFRA         — ``_index/`` / ``availability_index/`` /
                                ``instrument_availability/`` / ``*.tmp`` etc
  (C2) NON_DATA               — staging / logs / tooling / audits / catalogue /
                                reference-aux mappings / retired entities (labelled,
                                NEVER deleted — operator 2026-06-10)
  (D)  JUNK                   — unparseable shape / zero-row shell with no row
  (E)  ORPHAN_REAL            — real data (rows>0) with NO covering manifest row →
                                ``record_captured`` backfill, NEVER delete

**Buckets + their manifests** (both resolved via the bucket-name SSOT):

* ``market-data-tick-sports-…`` (MDPS odds): ``raw_tick_data/by_date/day={D}/
  category=sports/data_source={S}/venue={V}/league_id={L}/instrument_type=odds/
  data_type=trades/ticks.parquet`` + ``processed/by_date/day={D}/
  data_type=odds_horizon_bucket/league_id={L}/timeframe={T}/bucketed.parquet``.
  Manifest = ``_index/availability_index.parquet`` (786k rows). Grain notes: modern
  ``trades`` rows are per-(day, bookmaker-venue, league); the 2020-2023 era is
  manifested as ``data_type=ODDS`` at (day, venue=ODDS_API, league) AGGREGATE grain —
  a trades object is covered by EITHER its per-venue row OR the era aggregate row for
  its ``data_source`` (the data_type-equivalence probe below).

* ``instruments-store-sports-…`` (reference): ``sports_reference/`` (the
  candidate_parquet_paths SSOT tree + flat legacy twins + aux mappings),
  ``sports_reference_v2/`` (the 2026-04-28 v1→v2 migration staging tree — legacy
  twins of the canonical by_date tree), ``sports_reference_v1_archive/`` (B2), the
  bare ``day=/venue=`` instrument-DEFINITIONS tree (own availability surface:
  ``availability_index/instruments-service.parquet`` keyed (date, venue)), and infra.
  Manifest = ``_index/availability_index.parquet`` (2.68M rows, league_id grain).

**Grain-aware covering** (the sports analogue of the hive sweep's ``is_covered``):
the covered index is keyed ``(date, data_type.lower())`` → set of
``(venue, league_id, timeframe)`` captured patterns. An axis matches when the
manifest side is blank (coarser manifest = wildcard), the OBJECT side is blank
(coarser legacy object — e.g. the bare ``entity={F}/{F}.parquet`` aggregates leagues,
and ``candidate_parquet_paths`` itself names the bare path as a probe target for
every league cell), or the values are equal. The venue axis additionally accepts the
object's ``data_source`` segment (the ODDS_API aggregate-era rows). FLAT singletons
(no ``day=``) are covered by ANY captured row of their data_type.

Acceptance (CF-17, per bucket): ``orphan_class_E == 0`` AND ``unknown_prefixes == 0``.
Default scan-only; ``--report-out`` writes the audit parquet (B/B2/E rows — the same
SweptObject report shape as the hive sweep, plus the sports league/entity axes).

Usage::

    cd instruments-service
    .venv/bin/python scripts/migration_orphan_sweep_sports.py --bucket odds \\
        --report-out gs://<bucket>/_index/audit/orphan_sweep_sports.parquet
    .venv/bin/python scripts/migration_orphan_sweep_sports.py --bucket reference --limit 50000

Plan refs: ``migration_verification_orphan_safety_2026_06_10.md`` § V2 (R8) +
``master_data_canonicalisation_migration_catalogue_2026_06_07.md`` R8 todo.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import ModuleType

from unified_api_contracts.sports import SPORTS_DATA_TYPE_TO_FOLDER, SPORTS_DATA_TYPE_TO_SOURCE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_HIVE_CACHE: list[ModuleType] = []


def _hive_sweep() -> ModuleType:
    """Load the sibling hive sweep for the shared surfaces (``SizingRollup`` +
    ``_footer_row_count`` lazy footer read) — single-source via importlib, the same
    pattern as ``backfill_orphan_class_e._load_sweep_module`` (``scripts/`` is not a
    package)."""
    if not _HIVE_CACHE:
        spec = importlib.util.spec_from_file_location(
            "migration_orphan_sweep", Path(__file__).parent / "migration_orphan_sweep.py"
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load sibling migration_orphan_sweep.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("migration_orphan_sweep", mod)
        spec.loader.exec_module(mod)
        _HIVE_CACHE.append(mod)
    return _HIVE_CACHE[0]


class SportsObjectClass(StrEnum):
    """The hive sweep's forced taxonomy + the sports-only B2/C3 dispositions."""

    CANONICAL_MANIFESTED = "A_canonical_manifested"
    LEGACY_DUPLICATE = "B_legacy_duplicate"
    V1_ARCHIVE_SUPERSEDED = "B2_v1_archive_superseded"
    MANIFEST_INFRA = "C_manifest_infra"
    NON_DATA = "C2_non_data"
    PRE_LAUNCH_WINDOW = "C3_pre_launch_window"
    """Real data whose ``(data_type, day)`` is BEFORE the UAC sports coverage window
    (``SOURCE_COVERAGE_START`` / ``DATA_TYPE_COVERAGE_START``): the manifest
    CONTRACTUALLY excludes it — ``ManifestWriter`` silently refuses such rows (the
    pre-launch guard, born of the 2026-05-04 229,224-phantom-purge incident), so a
    ``record_captured`` backfill is structurally impossible without first extending
    the UAC window (an operator-gated contract change — tracked plan todo; the
    corpus is the 2026-04/05 footystats HISTORICAL fetches over 2018 days + the
    api_football fixture sub-entities before their 2020-06-06 window). Understood,
    labelled, NEVER deleted — and deliberately NOT class (E): E means "the manifest
    SHOULD cover this and doesn't"; this is "the manifest REFUSES this by design"."""
    JUNK = "D_junk"
    ORPHAN_REAL = "E_orphan_real"


# entity folder → canonical data_type (the reverse of the UAC SSOT map).
FOLDER_TO_DATA_TYPE: dict[str, str] = {v: k for k, v in SPORTS_DATA_TYPE_TO_FOLDER.items()}

# Entity folders RETIRED from captured-GCS-data status (gcs_paths.py, 2026-05-05):
# provider-catalog mappings live in UAC (TRANSFERMARKT_IDS / SOCCER_FOOTBALL_INFO_IDS);
# on-disk remnants are understood + labelled, never raw-data-orphans, never deleted.
RETIRED_ENTITY_FOLDERS: frozenset[str] = frozenset({"transfermarkt_leagues", "sfi_leagues", "transfermarkt_teams"})

# data_type-equivalence probe (object data_type → manifest data_types that can cover
# it). The MDPS 2020-2023 era manifested per-day ODDS_API AGGREGATE rows as
# ``data_type=ODDS`` while the objects carry ``data_type=trades`` at bookmaker grain.
DT_EQUIVALENCE: dict[str, tuple[str, ...]] = {"trades": ("trades", "odds")}


def _odds_api_wire_league_map() -> dict[str, str]:
    """ODDS_API wire league key (``soccer_epl``) → canonical league_id (``EPL``),
    DERIVED from the two UAC SSOTs (never hand-listed): ``LEAGUE_REGISTRY``
    (canonical → api_football_id) joined to ``DEFAULT_CLASSIFICATION_REGISTRY``
    (api_football_id → ``odds_api_name``). The 2025-era legacy shape
    ``raw_tick_data/by_date/day={D}/venue=ODDS_API/league=soccer_epl/ticks.parquet``
    carries the WIRE key while the manifest rows carry the canonical token."""
    from unified_api_contracts.sports import DEFAULT_CLASSIFICATION_REGISTRY, LEAGUE_REGISTRY

    wire: dict[str, str] = {}
    for canonical_id, league_def in LEAGUE_REGISTRY.items():
        if league_def.api_football_id is None:
            continue
        cls = DEFAULT_CLASSIFICATION_REGISTRY.get_league(league_def.api_football_id)
        name = getattr(cls, "odds_api_name", None) if cls is not None else None
        if name:
            wire[str(name).lower()] = canonical_id
    return wire


ODDS_API_WIRE_TO_CANONICAL_LEAGUE: dict[str, str] = _odds_api_wire_league_map()


def canonical_league_token(league: str) -> str:
    """Remap an ODDS_API wire league key to its canonical league_id for coverage
    matching (identity for already-canonical tokens)."""
    return ODDS_API_WIRE_TO_CANONICAL_LEAGUE.get(league.lower(), league)


# Object suffixes that are manifest-infra regardless of prefix (mirrors the hive set).
_INFRA_SUFFIXES: tuple[str, ...] = (".tmp", ".partial", "_SUCCESS", ".lock")

_PRE_LAUNCH_REASON = (
    "pre-launch-window data — manifest contractually excludes (UAC coverage SSOT + "
    "ManifestWriter pre-launch guard); window extension is a tracked operator-gated todo"
)


def _is_pre_launch(data_type: str, day: str) -> bool:
    """UAC sports coverage-window check (``is_pre_launch_date``); non-sports-registry
    data_types and the ``day=all`` sentinel return False."""
    if not data_type or not day or day == "all":
        return False
    from unified_api_contracts.sports import is_pre_launch_date

    return bool(is_pre_launch_date(data_type, day))


# ---------------------------------------------------------------------------
# Per-bucket prefix taxonomy (CF-17: every top-level prefix labelled; 0 unknown bar)
# ---------------------------------------------------------------------------

# label → (SportsObjectClass for non-data classes, None = data corpus to classify)
ODDS_PREFIXES: dict[str, tuple[str, SportsObjectClass | None]] = {
    "_index/": ("manifest-infra", SportsObjectClass.MANIFEST_INFRA),
    "_vm_staging/": ("staging", SportsObjectClass.NON_DATA),
    "scripts/": ("tooling", SportsObjectClass.NON_DATA),
    "raw_tick_data/": ("service-data", None),
    "processed/": ("service-data", None),
}

REFERENCE_PREFIXES: dict[str, tuple[str, SportsObjectClass | None]] = {
    "_index/": ("manifest-infra", SportsObjectClass.MANIFEST_INFRA),
    "availability_index/": ("manifest-infra", SportsObjectClass.MANIFEST_INFRA),
    "instrument_availability/": ("manifest-infra", SportsObjectClass.MANIFEST_INFRA),
    "_catalogue/": ("catalogue-infra", SportsObjectClass.NON_DATA),
    "_audits/": ("run-artifact", SportsObjectClass.NON_DATA),
    "_smoke_test/": ("run-artifact", SportsObjectClass.NON_DATA),
    "_vm_staging/": ("staging", SportsObjectClass.NON_DATA),
    "sports_reference_v1_archive/": ("v1-archive", SportsObjectClass.V1_ARCHIVE_SUPERSEDED),
    "sports_reference_v2/": ("service-data", None),
    "sports_reference/": ("service-data", None),
    # the bare day=/venue= instrument-DEFINITIONS tree (IS definitions shape; its
    # availability surface is availability_index/instruments-service.parquet)
    "day=": ("instrument-definitions", None),
}

# ``sports_reference/<sub>/`` aux subtrees that are NOT day-partitioned entity data:
# provider-catalog / mapping corpora (reference-aux; understood, never deleted).
REFERENCE_AUX_SUBTREES: frozenset[str] = frozenset({"footystats_league_ids", "mappings", "teams_in_league"})


# ---------------------------------------------------------------------------
# Covered index (grain-aware, league-axis) — the sports analogue of the hive
# build_covered_index/is_covered pair
# ---------------------------------------------------------------------------


def _norm(value: str) -> str:
    return value.strip().upper()


@dataclass
class SportsCoveredIndex:
    """Captured manifest cells at (date, data_type) → (venue, league, timeframe)
    patterns (blank = wildcard), + the any-date set for FLAT singletons, + the
    instrument-DEFINITIONS availability days (reference bucket only)."""

    cells: dict[tuple[str, str], set[tuple[str, str, str]]] = field(default_factory=dict)
    any_captured_dts: set[str] = field(default_factory=set)
    definition_days: set[tuple[str, str]] = field(default_factory=set)


def build_sports_covered_index(rows: list[dict[str, str]]) -> SportsCoveredIndex:
    """Build the covered index from ``_index`` rows (captured only)."""
    index = SportsCoveredIndex(cells=defaultdict(set))
    for r in rows:
        if str(r.get("capture_status", "")).lower() != "captured":
            continue
        date = str(r.get("date", "") or "")
        dt = str(r.get("data_type", "") or "").lower()
        if not date or not dt:
            continue
        pattern = (
            _norm(str(r.get("venue", "") or "")),
            _norm(str(r.get("league_id", "") or "")),
            str(r.get("timeframe", "") or "").strip(),
        )
        index.cells[(date, dt)].add(pattern)
        index.any_captured_dts.add(dt)
    return index


def _axis_match(manifest_value: str, object_value: str) -> bool:
    """Blank on EITHER side is a wildcard (manifest coarser than object — e.g.
    league-blank WEATHER rows; object coarser than manifest — e.g. the bare
    ``entity={F}/{F}.parquet`` league-aggregate file which ``candidate_parquet_paths``
    names as a probe target for every league cell)."""
    return manifest_value == "" or object_value == "" or manifest_value == object_value


def is_covered_sports(
    index: SportsCoveredIndex,
    *,
    day: str,
    data_type: str,
    league: str = "",
    venue: str = "",
    source: str = "",
    timeframe: str = "",
) -> bool:
    """Does a captured manifest row COVER this object (grain-aware)?

    ``day == ''`` = FLAT singleton (no date partition) → covered by ANY captured row
    of the data_type. The venue axis accepts the object's ``data_source`` segment too
    (the ODDS_API aggregate-era ``data_type=ODDS`` rows cover bookmaker-venue trades
    objects of that source — see DT_EQUIVALENCE).
    """
    dt = data_type.lower()
    if not day:
        return dt in index.any_captured_dts
    league_n, tf = _norm(league), timeframe.strip()
    venue_candidates = {_norm(venue), _norm(source)}
    for probe_dt in DT_EQUIVALENCE.get(dt, (dt,)):
        bucket = index.cells.get((day, probe_dt))
        if not bucket:
            continue
        for mv, ml, mt in bucket:
            # venue is identity when BOTH sides carry one (a bookmaker object never
            # reads covered by a different bookmaker's row); the object's data_source
            # is an accepted alias (the ODDS_API aggregate-era rows).
            if mv and venue and mv not in venue_candidates:
                continue
            if not _axis_match(ml, league_n) or not _axis_match(mt, tf):
                continue
            return True
    return False


# ---------------------------------------------------------------------------
# Pure parsing + classification (unit-tested without GCS)
# ---------------------------------------------------------------------------


def parse_sports_segments(object_path: str) -> dict[str, str]:
    """Extract every ``k=v`` segment (day, entity, league, league_id, season, venue,
    data_source, data_type, timeframe, pipeline_mode, fetched_at_hour, …).
    Last-wins on duplicates; tolerant of extra segments (``fetched_at_hour=``)."""
    segments: dict[str, str] = {}
    for part in object_path.split("/"):
        if "=" in part:
            key, _sep, value = part.partition("=")
            segments[key] = value
    return segments


@dataclass(frozen=True)
class SportsSweptObject:
    """One classified object — the hive SweptObject report shape + the sports axes
    (league_id / entity / season / timeframe replace the hive chain axis)."""

    uri: str
    asset_group: str
    obj_class: SportsObjectClass
    venue: str
    league_id: str
    entity: str
    instrument_type: str
    data_type: str
    day: str
    season: str
    timeframe: str
    pipeline_mode: str
    source: str
    size_bytes: int
    crc32c: str
    reason: str


_EMPTY = ("", "", "", "", "", "", "")  # venue, league, entity, dt, day, season, tf


def _prefix_entry(
    object_path: str, prefixes: dict[str, tuple[str, SportsObjectClass | None]]
) -> tuple[str, str, SportsObjectClass | None] | None:
    for prefix, (label, cls) in prefixes.items():
        if object_path.startswith(prefix):
            return prefix, label, cls
    return None


def classify_odds_object(
    object_path: str, index: SportsCoveredIndex, *, is_parquet: bool, row_count: int | None = None
) -> tuple[SportsObjectClass, tuple[str, str, str, str, str, str, str], str]:
    """Classify ONE MDPS-odds-bucket object (pure). Returns (class, (venue, league,
    entity, data_type, day, season, timeframe), reason)."""
    entry = _prefix_entry(object_path, ODDS_PREFIXES)
    for suffix in _INFRA_SUFFIXES:
        if object_path.endswith(suffix):
            return SportsObjectClass.MANIFEST_INFRA, _EMPTY, "manifest-infra suffix"
    if entry is None:
        return SportsObjectClass.JUNK, _EMPTY, "unlabelled top-level prefix"
    _prefix, label, fixed_cls = entry
    if fixed_cls is not None:
        return fixed_cls, _EMPTY, label
    if not is_parquet:
        return SportsObjectClass.NON_DATA, _EMPTY, "non-parquet under data prefix"
    seg = parse_sports_segments(object_path)
    # legacy segment aliases: 2022-era ``source=ODDS_API/league=LA_LIGA`` and
    # 2025-era ``venue=ODDS_API/league=soccer_epl`` shapes (no data_type=/league_id=)
    day = seg.get("day", "")
    league = canonical_league_token(seg.get("league_id", seg.get("league", "")))
    venue, source = seg.get("venue", ""), seg.get("data_source", seg.get("source", ""))
    dt, tf = seg.get("data_type", ""), seg.get("timeframe", "")
    if not dt and object_path.startswith("raw_tick_data/") and object_path.endswith("/ticks.parquet"):
        dt = "trades"  # the pre-data_type-segment odds tick shapes
    fields = (venue, league, "", dt, day, "", tf)
    if not dt or not day:
        return SportsObjectClass.JUNK, fields, "missing data_type/day segment"
    if is_covered_sports(index, day=day, data_type=dt, league=league, venue=venue, source=source, timeframe=tf):
        return SportsObjectClass.CANONICAL_MANIFESTED, fields, "canonical+manifested"
    if _is_pre_launch(dt, day):
        return SportsObjectClass.PRE_LAUNCH_WINDOW, fields, _PRE_LAUNCH_REASON
    if row_count == 0:
        return SportsObjectClass.JUNK, fields, "zero-row object with no manifest row (footer-read)"
    return SportsObjectClass.ORPHAN_REAL, fields, "real data (rows>0) with NO covering manifest row"


def classify_reference_object(
    object_path: str, index: SportsCoveredIndex, *, is_parquet: bool, row_count: int | None = None
) -> tuple[SportsObjectClass, tuple[str, str, str, str, str, str, str], str]:
    """Classify ONE reference-bucket object (pure). Same return shape as
    :func:`classify_odds_object`."""
    for suffix in _INFRA_SUFFIXES:
        if object_path.endswith(suffix):
            return SportsObjectClass.MANIFEST_INFRA, _EMPTY, "manifest-infra suffix"
    entry = _prefix_entry(object_path, REFERENCE_PREFIXES)
    if entry is None:
        return SportsObjectClass.JUNK, _EMPTY, "unlabelled top-level prefix"
    prefix, label, fixed_cls = entry
    if fixed_cls is not None:
        return fixed_cls, _EMPTY, label
    if prefix == "day=":
        return _classify_definitions_object(object_path, index, is_parquet=is_parquet, row_count=row_count)
    if not is_parquet:
        return SportsObjectClass.NON_DATA, _EMPTY, "non-parquet under data prefix"
    seg = parse_sports_segments(object_path)
    day, entity = seg.get("day", ""), seg.get("entity", "")
    league, season = seg.get("league", seg.get("league_id", "")), seg.get("season", "")
    is_canonical_tree = object_path.startswith("sports_reference/")

    if is_canonical_tree and not object_path.startswith("sports_reference/by_date/"):
        # flat subtrees: aux mappings, the FLAT singleton, legacy flat-by-day twins
        subtree = object_path.split("/", 2)[1]
        if subtree in REFERENCE_AUX_SUBTREES:
            return SportsObjectClass.NON_DATA, _EMPTY, "reference-aux mapping corpus (UAC-owned vocabulary)"
        dt = FOLDER_TO_DATA_TYPE.get(subtree, "")
        if not dt:
            return SportsObjectClass.JUNK, _EMPTY, f"unknown flat subtree {subtree!r}"
        fields = ("", league, subtree, dt, day, season, "")
        covered = is_covered_sports(index, day=day, data_type=dt, league=league)
        if covered:
            # day-partitioned flat twin (fixtures/day=…) = legacy shape (B); the
            # day-less FLAT singleton (venues/venues.parquet) IS the SSOT shape (A)
            cls = SportsObjectClass.CANONICAL_MANIFESTED if not day else SportsObjectClass.LEGACY_DUPLICATE
            return cls, fields, "FLAT singleton manifested" if not day else "legacy flat-by-day twin of manifested cell"
        if _is_pre_launch(dt, day):
            return SportsObjectClass.PRE_LAUNCH_WINDOW, fields, _PRE_LAUNCH_REASON
        if row_count == 0:
            return SportsObjectClass.JUNK, fields, "zero-row object with no manifest row (footer-read)"
        return SportsObjectClass.ORPHAN_REAL, fields, "real data (rows>0) with NO covering manifest row"

    # by_date trees (canonical sports_reference/by_date + legacy sports_reference_v2)
    if not entity or not day:
        return SportsObjectClass.JUNK, _EMPTY, "missing entity/day segment under by_date tree"
    if entity in RETIRED_ENTITY_FOLDERS:
        return (
            SportsObjectClass.NON_DATA,
            ("", league, entity, "", day, season, ""),
            "retired entity (provider-catalog mapping; UAC-owned since 2026-05-05)",
        )
    dt = FOLDER_TO_DATA_TYPE.get(entity, "")
    fields = ("", league, entity, dt, day, season, "")
    if not dt:
        return SportsObjectClass.JUNK, fields, f"entity folder {entity!r} not in SPORTS_DATA_TYPE_TO_FOLDER"
    if is_covered_sports(index, day=day, data_type=dt, league=league):
        cls = SportsObjectClass.CANONICAL_MANIFESTED if is_canonical_tree else SportsObjectClass.LEGACY_DUPLICATE
        return cls, fields, "canonical+manifested" if is_canonical_tree else "v2 staging twin of manifested cell"
    if _is_pre_launch(dt, day):
        return SportsObjectClass.PRE_LAUNCH_WINDOW, fields, _PRE_LAUNCH_REASON
    if row_count == 0:
        return SportsObjectClass.JUNK, fields, "zero-row object with no manifest row (footer-read)"
    return SportsObjectClass.ORPHAN_REAL, fields, "real data (rows>0) with NO covering manifest row"


def _classify_definitions_object(
    object_path: str, index: SportsCoveredIndex, *, is_parquet: bool, row_count: int | None = None
) -> tuple[SportsObjectClass, tuple[str, str, str, str, str, str, str], str]:
    """The bare ``day={D}/venue={V}/<uuid>.parquet`` instrument-DEFINITIONS shape —
    the IS definitions corpus, whose availability surface is
    ``availability_index/instruments-service.parquet`` keyed (date, venue)."""
    if not is_parquet:
        return SportsObjectClass.NON_DATA, _EMPTY, "non-parquet under definitions tree"
    seg = parse_sports_segments(object_path)
    day, venue = seg.get("day", ""), seg.get("venue", "")
    fields = (venue, "", "instrument_definitions", "instrument_definitions", day, "", "")
    if not day or not venue:
        return SportsObjectClass.JUNK, fields, "missing day/venue segment under definitions tree"
    if (day, _norm(venue)) in index.definition_days:
        return SportsObjectClass.CANONICAL_MANIFESTED, fields, "definitions object with availability row"
    if row_count == 0:
        return SportsObjectClass.JUNK, fields, "zero-row definitions object with no availability row (footer-read)"
    return SportsObjectClass.ORPHAN_REAL, fields, "instrument-definitions object with NO availability row"


def taxonomy_label(object_path: str, prefixes: dict[str, tuple[str, SportsObjectClass | None]]) -> str:
    """The bucket-prefix taxonomy label (CF-17): 0 ``unknown`` is the acceptance bar."""
    entry = _prefix_entry(object_path, prefixes)
    if entry is not None:
        return entry[1]
    head = object_path.split("/", 1)[0]
    return f"unknown:{head}/"


# ---------------------------------------------------------------------------
# GCS walk orchestration (thin; the heavy lifting is the pure classifiers above)
# ---------------------------------------------------------------------------

BUCKET_KINDS: dict[str, str] = {"odds": "market-data", "reference": "instruments-store"}


def _resolve_sports_bucket(which: str, cloud: str = "gcp") -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud=cloud, kind=BUCKET_KINDS[which], asset_group="sports")


def _load_covered_index(client: object, bucket: str, *, with_definitions: bool) -> SportsCoveredIndex:
    import io

    import pandas as pd

    index_path = "_index/availability_index.parquet"
    if not client.blob_exists(bucket, index_path):  # type: ignore[attr-defined]
        logger.warning("sports orphan-sweep: no %s in %s", index_path, bucket)
        index = SportsCoveredIndex(cells=defaultdict(set))
    else:
        raw = client.download_bytes(bucket, index_path)  # type: ignore[attr-defined]
        df = pd.read_parquet(io.BytesIO(raw))
        index = build_sports_covered_index(df.to_dict("records"))  # type: ignore[arg-type]
    if with_definitions:
        defs_path = "availability_index/instruments-service.parquet"
        if client.blob_exists(bucket, defs_path):  # type: ignore[attr-defined]
            raw = client.download_bytes(bucket, defs_path)  # type: ignore[attr-defined]
            ddf = pd.read_parquet(io.BytesIO(raw))
            for r in ddf.to_dict("records"):
                index.definition_days.add((str(r.get("date", "")), _norm(str(r.get("venue", "")))))
    return index


def run_sweep(
    which: str,
    *,
    cloud: str = "gcp",
    limit: int | None = None,
    workers: int = 32,
) -> tuple[Counter[str], Counter[str], object, list[SportsSweptObject]]:
    """Walk ONE sports bucket, classify every object, roll up sizing + the prefix
    taxonomy. Returns (class_counts, prefix_taxonomy, sizing, actionable_objects).

    The (E)/(D) zero-row footer split runs as a PARALLEL post-pass (``workers``
    threads) over the <256KB would-be-(E) set — an inline serial read stalled the
    first full reference walk at ~10 obj/s through its uncovered region.
    """
    from unified_trading_library import get_storage_client

    hive = _hive_sweep()
    bucket = _resolve_sports_bucket(which, cloud)
    client = get_storage_client()
    prefixes = ODDS_PREFIXES if which == "odds" else REFERENCE_PREFIXES
    classify = classify_odds_object if which == "odds" else classify_reference_object
    logger.info("sports orphan-sweep [%s]: bucket=%s — loading manifest cells", which, bucket)
    index = _load_covered_index(client, bucket, with_definitions=(which == "reference"))
    logger.info(
        "sports orphan-sweep [%s]: %d captured (day, data_type) cells, %d definition days",
        which,
        len(index.cells),
        len(index.definition_days),
    )

    class_counts: Counter[str] = Counter()
    prefix_taxonomy: Counter[str] = Counter()
    sizing = hive.SizingRollup.empty()
    actionable: list[SportsSweptObject] = []
    t0 = time.time()
    for seen, blob in enumerate(client.list_blobs(bucket), start=1):
        name = blob.name
        prefix_taxonomy[taxonomy_label(name, prefixes)] += 1
        is_parquet = name.endswith(".parquet")
        cls, fields, reason = classify(name, index, is_parquet=is_parquet, row_count=None)
        class_counts[cls.value] += 1
        venue, league, entity, dt, day, season, tf = fields
        obj = SportsSweptObject(
            uri=f"gs://{bucket}/{name}",
            asset_group="sports",
            obj_class=cls,
            venue=venue,
            league_id=league,
            entity=entity,
            instrument_type=parse_sports_segments(name).get("instrument_type", ""),
            data_type=dt,
            day=day,
            season=season,
            timeframe=tf,
            pipeline_mode=parse_sports_segments(name).get("pipeline_mode", ""),
            source=SPORTS_DATA_TYPE_TO_SOURCE.get(dt, ""),
            size_bytes=int(getattr(blob, "size", 0) or 0),
            crc32c=str(getattr(blob, "crc32c", "") or ""),
            reason=reason,
        )
        if is_parquet and cls not in (SportsObjectClass.NON_DATA, SportsObjectClass.MANIFEST_INFRA):
            sizing.add(obj)
        if cls in (
            SportsObjectClass.ORPHAN_REAL,
            SportsObjectClass.LEGACY_DUPLICATE,
            SportsObjectClass.V1_ARCHIVE_SUPERSEDED,
            SportsObjectClass.PRE_LAUNCH_WINDOW,
        ):
            actionable.append(obj)
        if seen % 50000 == 0:
            logger.info("  %d objects swept (%.0f/s)", seen, seen / max(0.01, time.time() - t0))
        if limit is not None and seen >= limit:
            logger.info("  --limit %d reached — stopping walk", limit)
            break
    actionable = _split_zero_row_shells(client, bucket, hive, class_counts, actionable, workers=workers)
    return class_counts, prefix_taxonomy, sizing, actionable


def _split_zero_row_shells(
    client: object,
    bucket: str,
    hive: ModuleType,
    class_counts: Counter[str],
    actionable: list[SportsSweptObject],
    *,
    workers: int,
) -> list[SportsSweptObject]:
    """The lazy footer read (parallel post-pass): a would-be-(E) object with no row
    evidence may be a 0-row schema shell — class (D) junk, not a backfill target.
    Only small objects can be shells, so reads are bounded to <256KB candidates;
    large objects are certainly rows>0 and stay (E) without a read. A failed read
    keeps the safe non-zero assumption (object stays (E) for inspection)."""
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import replace

    prefix_len = len(f"gs://{bucket}/")
    candidates = [
        (i, o)
        for i, o in enumerate(actionable)
        if o.obj_class is SportsObjectClass.ORPHAN_REAL and o.size_bytes < 262144
    ]
    if not candidates:
        return actionable
    logger.info("  footer-reading %d would-be-E candidates (<256KB, %d workers)", len(candidates), workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        rows_per_obj = list(
            pool.map(lambda c: hive._footer_row_count(client, bucket, c[1].uri[prefix_len:]), candidates)
        )
    out = list(actionable)
    flipped = 0
    for (i, obj), rows in zip(candidates, rows_per_obj, strict=True):
        if rows == 0:
            out[i] = replace(
                obj, obj_class=SportsObjectClass.JUNK, reason="zero-row object with no manifest row (footer-read)"
            )
            class_counts[SportsObjectClass.ORPHAN_REAL.value] -= 1
            class_counts[SportsObjectClass.JUNK.value] += 1
            flipped += 1
    if flipped:
        logger.info("  zero-row split: %d would-be-E objects reclassified to D", flipped)
    # JUNK shells leave the actionable report (mirrors the hive sweep: D is counted,
    # never a backfill/cleanup target).
    return [o for o in out if o.obj_class is not SportsObjectClass.JUNK]


def _print_report(
    which: str,
    class_counts: Counter[str],
    prefix_taxonomy: Counter[str],
    sizing: object,
    actionable: list[SportsSweptObject],
) -> int:
    """Print the human report; return the exit code (0 only if class-E == 0 AND no
    unknown prefixes — the CF-17 acceptance bar)."""
    logger.info("=== sports orphan sweep report: %s bucket ===", which)
    for cls in SportsObjectClass:
        logger.info("  %-28s %d", cls.value, class_counts.get(cls.value, 0))
    logger.info("--- bucket prefix taxonomy ---")
    unknown = 0
    for label, count in sorted(prefix_taxonomy.items()):
        logger.info("  %-30s %d", label, count)
        if label.startswith("unknown:"):
            unknown += count
    logger.info(
        "--- sizing: total=%.2f GiB across %d cells ---",
        sizing.total_bytes() / 2**30,  # type: ignore[attr-defined]
        len(sizing.by_cell),  # type: ignore[attr-defined]
    )
    for cell, nbytes, nobj in sizing.biggest(15):  # type: ignore[attr-defined]
        logger.info("  %-55s %.2f GiB (%d objs)", "/".join(cell), nbytes / 2**30, nobj)
    n_e = class_counts.get(SportsObjectClass.ORPHAN_REAL.value, 0)
    logger.info(
        "=== ACCEPTANCE [%s]: orphan_class_E=%d (target 0), unknown_prefixes=%d (target 0) ===", which, n_e, unknown
    )
    if n_e:
        logger.warning("orphan-E objects (record_captured backfill — NEVER delete); first 25:")
        for obj in [o for o in actionable if o.obj_class == SportsObjectClass.ORPHAN_REAL][:25]:
            logger.warning(
                "  E %s (%s/%s/%s/%s)", obj.uri, obj.venue or obj.entity, obj.league_id, obj.data_type, obj.day
            )
    return 0 if (n_e == 0 and unknown == 0) else 1


def _write_report(report_out: str, actionable: list[SportsSweptObject]) -> None:
    """Write the actionable objects (E orphans + B legacy twins + B2 archive) to a
    parquet at ``report_out`` (gs:// URI) — the backfill + CF-21 consumers read it."""
    import io

    import pandas as pd
    from unified_trading_library import get_storage_client

    df = pd.DataFrame([{**vars(o), "obj_class": o.obj_class.value} for o in actionable])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    assert report_out.startswith("gs://")
    bucket, _sep, blob_path = report_out[len("gs://") :].partition("/")
    get_storage_client().upload_from_file_obj(bucket, blob_path, buf)  # type: ignore[attr-defined]
    counts = Counter(o.obj_class.value for o in actionable)
    logger.info("wrote %d actionable rows %s to %s", len(actionable), dict(counts), report_out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="SPORTS GCS→manifest orphan sweep + bucket taxonomy + sizing (CF-17 / R8)."
    )
    parser.add_argument("--bucket", required=True, choices=["odds", "reference", "both"])
    parser.add_argument("--cloud", choices=["gcp", "aws"], default="gcp")
    parser.add_argument("--workers", type=int, default=32, help="footer-read post-pass parallelism")
    parser.add_argument("--limit", type=int, default=None, help="stop after N objects (smoke / partial sweep)")
    parser.add_argument("--dry-run", action="store_true", help="scan + report only; never write/delete (default)")
    parser.add_argument(
        "--report-out",
        type=str,
        default="",
        help="gs:// URI for the orphan_sweep parquet (single-bucket runs only); "
        "with --bucket both each bucket writes to its own _index/audit/orphan_sweep_sports.parquet",
    )
    args = parser.parse_args(argv)
    targets = ["odds", "reference"] if args.bucket == "both" else [args.bucket]
    if args.report_out and len(targets) > 1:
        parser.error("--report-out is single-bucket; use --bucket odds|reference")

    worst = 0
    for which in targets:
        class_counts, prefix_taxonomy, sizing, actionable = run_sweep(
            which, cloud=args.cloud, limit=args.limit, workers=args.workers
        )
        code = _print_report(which, class_counts, prefix_taxonomy, sizing, actionable)
        worst = max(worst, code)
        report_out = args.report_out
        if not report_out and args.bucket == "both":
            report_out = f"gs://{_resolve_sports_bucket(which, args.cloud)}/_index/audit/orphan_sweep_sports.parquet"
        if report_out:
            _write_report(report_out, actionable)
    return worst


if __name__ == "__main__":
    sys.exit(main())
