#!/usr/bin/env python3
# SCHEMA_PROVENANCE_EXEMPT — script-local result dataclass for backfill operation output.
"""Backfill availability manifest with canonical league_id for ALL sports entities.

Fixes the data-status UI showing 0% / undercount for SPORTS data_types where
``rescan_sports_manifest.py`` wrote rows with ``league_id=""``. The parquets
exist in GCS — only the manifest's canonical-league dimension is missing.

Three scopes:

* **fixture** — entity parquet has a ``fixture_id`` (or ``af_fixture_id``)
  column. Join with the day's ``fixtures.parquet`` sibling to derive
  ``af_league_id`` per row, then resolve to canonical UAC league_id via
  ``get_league_by_api_football_id``. One manifest row per
  ``(date, data_type, league_id)``.

* **league_direct** — entity parquet has a per-source league code per row.
  We emit one manifest row per ``(date, data_type, canonical_league_id)``
  for every UAC league declared by that source (per
  ``get_expected_leagues_for_source``). Parquet PRESENCE on a day proves
  the periodic snapshot was taken; we don't try to map the per-row source
  league code back to canonical (some sources have leagues UAC doesn't
  track, and vice versa — an over-approximation here matches the periodic
  semantics).

* **singleton** — entity at a flat path (``sports_reference/<name>/<name>.parquet``).
  Emit one manifest row.

Idempotent: ``ManifestWriter`` deduplicates on the shard tuple. Re-running
is safe.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_sports_per_entity_manifest.py --dry-run
    .venv/bin/python scripts/backfill_sports_per_entity_manifest.py --entities WEATHER,FIXTURE_LINEUPS
    .venv/bin/python scripts/backfill_sports_per_entity_manifest.py            # full run
    .venv/bin/python scripts/backfill_sports_per_entity_manifest.py --limit 5  # spot-check
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from datetime import date as date_type
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage
from unified_api_contracts import PipelineMode
from unified_api_contracts.canonical.domain.sports.league_data import (
    get_league_by_api_football_id,
    get_league_fixture_calendar,
)
from unified_api_contracts.sports import get_expected_leagues_for_source
from unified_trading_library import ManifestWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
ROOT = "sports_reference/by_date/"
OUT_PATH = Path(__file__).parent / "sports_per_entity_manifest_backfill_report.json"


@dataclass(frozen=True)
class EntitySpec:
    """Per-data_type backfill spec."""

    data_type: str  # canonical manifest data_type (e.g. "WEATHER")
    folder: str  # GCS folder under day=D/entity=<folder>
    scope: str  # "fixture" | "league_direct" | "singleton"
    fid_cols: tuple[str, ...] = ()  # candidate fixture-id column names
    source: str = ""  # UAC source key for league_direct (e.g. "transfermarkt")


SPECS: tuple[EntitySpec, ...] = (
    # fixture-scoped
    # WEATHER joins via venue_id (no fixture_id column) — handled specially.
    EntitySpec("WEATHER", "weather", "weather", source="open_meteo"),
    EntitySpec(
        "FIXTURE_LINEUPS",
        "fixture_lineups",
        "fixture",
        ("fixture_id", "af_fixture_id"),
        source="api_football",
    ),
    EntitySpec(
        "FIXTURE_EVENTS",
        "fixture_events",
        "fixture",
        ("fixture_id", "af_fixture_id"),
        source="api_football",
    ),
    EntitySpec(
        "PLAYER_STATS",
        "player_stats",
        "fixture",
        ("fixture_id", "af_fixture_id"),
        source="api_football",
    ),
    # INJURIES `fixture` column is a struct {id,date,timestamp} — handled specially.
    EntitySpec("INJURIES", "injuries", "injuries", source="api_football"),
    EntitySpec(
        "MATCHES",
        "footystats_matches",
        "fixture",
        ("canonical_fixture_id", "fixture_id", "af_fixture_id"),
        source="footystats",
    ),
    EntitySpec(
        "ODDS",
        "footystats_odds",
        "fixture",
        ("canonical_fixture_id", "fixture_id", "af_fixture_id"),
        source="footystats",
    ),
    EntitySpec(
        "PREDICTIONS",
        "footystats_predictions",
        "fixture",
        ("canonical_fixture_id", "fixture_id", "af_fixture_id"),
        source="footystats",
    ),
    # XG carries canonical UAC league_id directly in `league_league_id` column.
    EntitySpec("XG", "understat_xg", "xg", source="understat"),
    EntitySpec(
        "SFI_PROGRESSIVE_STATS",
        "progressive_stats",
        "fixture",
        ("fixture_id",),
        source="soccer_football_info",
    ),
    EntitySpec(
        "FIXTURE_STATS",
        "fixture_stats",
        "fixture",
        ("af_fixture_id", "fixture_id"),
        source="api_football",
    ),
    # league_direct (one row per (day, source-league) → emit per canonical UAC league)
    EntitySpec("TRANSFERMARKT_LEAGUES", "transfermarkt_leagues", "league_direct", source="transfermarkt"),
    EntitySpec("SFI_LEAGUES", "sfi_leagues", "league_direct", source="soccer_football_info"),
    EntitySpec("SFI_STANDINGS", "sfi_standings", "league_direct", source="soccer_football_info"),
    # fixture_denorm — emit (day, canonical_league) per fixture-day for breakdown
    # cohesion. PLAYER_VALUES is conceptually a weekly transfermarkt snapshot,
    # but we denorm to per-fixture-date so the per-fixture drilldown can show
    # PLAYER_VALUES alongside FIXTURE_LINEUPS / EVENTS coherently. The "captured"
    # semantic = "the latest transfermarkt snapshot covers this fixture's teams";
    # we trust source coverage rather than re-checking the weekly snapshot.
    EntitySpec("PLAYER_VALUES", "transfermarkt_teams", "fixture_denorm", source="transfermarkt"),
    # singleton (flat path: sports_reference/venues/venues.parquet, not under by_date/)
    EntitySpec("VENUES", "venues", "singleton"),
)


# Source → batch PipelineMode mapping. footystats has no dedicated enum value
# in UAC (closed-set; missing BATCH_FOOTYSTATS) — its rows tag with
# BATCH_API_FOOTBALL per the workaround documented in
# ``plans/active/issues/footystats_pipeline_mode_gap_2026_05_12.md``. ODDS slice
# from footystats odds adapter tags BATCH_ODDS_API per UAC SOURCE_PRIORITY for
# ``ODDS_SNAPSHOT`` / ``ODDS_MOVEMENT`` / ``ARBITRAGE``.
_SOURCE_TO_PIPELINE_MODE: dict[str, PipelineMode] = {
    "api_football": PipelineMode.BATCH_API_FOOTBALL,
    "footystats": PipelineMode.BATCH_API_FOOTBALL,
    "understat": PipelineMode.BATCH_UNDERSTAT,
    "open_meteo": PipelineMode.BATCH_OPEN_METEO,
    "transfermarkt": PipelineMode.BATCH_TRANSFERMARKT,
    "soccer_football_info": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
}


def _pipeline_mode_for_spec(spec: EntitySpec) -> PipelineMode:
    """Return the batch PipelineMode for an EntitySpec.

    Special-case ODDS data_type: tagged BATCH_ODDS_API per SOURCE_PRIORITY
    even though spec.source == "footystats" (the odds adapter wraps the
    odds_api source). Singleton VENUES spec carries empty source — fall
    back to BATCH_INSTRUMENTS_SERVICE since the venues table is the
    instruments-service catalog's own reference data.
    """
    if spec.data_type == "ODDS":
        return PipelineMode.BATCH_ODDS_API
    if not spec.source:
        return PipelineMode.BATCH_INSTRUMENTS_SERVICE
    return _SOURCE_TO_PIPELINE_MODE[spec.source]


# Singleton flat-path layout — emit one row dated to a stable date inside any
# query window. Today we use the file's blob ``updated`` time; the data-status
# global_season axis treats date as opaque and just counts presence.
SINGLETON_SENTINEL_DATE = "2024-01-01"


@dataclass
class EntityResult:
    data_type: str
    rows_written: int = 0
    leagues: set[str] = field(default_factory=set)
    days_with_data: set[str] = field(default_factory=set)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "data_type": self.data_type,
            "rows_written": self.rows_written,
            "league_count": len(self.leagues),
            "day_count": len(self.days_with_data),
            "leagues": sorted(self.leagues),
            "errors": self.errors[:10],
        }


def _list_days(client: storage.Client) -> list[str]:
    """Return ['2018-01-12', ...] via delimiter listing.

    Excludes non-ISO-date partitions (e.g. ``day=all`` sentinel for the
    VENUES singleton). Only proper YYYY-MM-DD partitions are returned.
    """
    bucket = client.bucket(BUCKET)
    iterator = bucket.list_blobs(prefix=ROOT, delimiter="/")
    list(iterator)
    days: list[str] = []
    for p in iterator.prefixes:  # type: ignore[attr-defined]
        seg = p.removeprefix(ROOT).removesuffix("/")
        if not seg.startswith("day="):
            continue
        candidate = seg.removeprefix("day=")
        try:
            date_type.fromisoformat(candidate)
        except ValueError:
            continue
        days.append(candidate)
    return sorted(days)


def _read_parquet(client: storage.Client, path: str) -> pd.DataFrame | None:
    try:
        raw = client.bucket(BUCKET).blob(path).download_as_bytes()
    except (NotFound, FileNotFoundError):
        return None
    try:
        return pd.read_parquet(io.BytesIO(raw))
    except (OSError, RuntimeError, ValueError):
        return None


def _pick_fid_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _strip_float_suffix(value: object) -> str:
    """``int64`` columns can render as ``'12345.0'`` after object-coercion; strip it."""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


# Cache for "leagues UAC declares for source S that play on date D".
_PER_SOURCE_DAY_CACHE: dict[tuple[str, str], frozenset[str]] = {}
_PER_SOURCE_LEAGUES_CACHE: dict[str, list[str]] = {}


def _expected_leagues_in_season(source_key: str, day: str) -> frozenset[str]:
    """Canonical leagues that source ``source_key`` declares to cover AND that
    are in-season on ``day`` per UAC fixture calendar. Ground-truth denominator
    for honest-coverage v5 — matches the data_status reader's expected_shards.
    """
    cached = _PER_SOURCE_DAY_CACHE.get((source_key, day))
    if cached is not None:
        return cached
    leagues = _PER_SOURCE_LEAGUES_CACHE.get(source_key)
    if leagues is None:
        leagues = [lg.league_id for lg in get_expected_leagues_for_source(source_key)]
        _PER_SOURCE_LEAGUES_CACHE[source_key] = leagues
    out: set[str] = set()
    for lid in leagues:
        if get_league_fixture_calendar(lid, day, day):
            out.add(lid)
    frozen = frozenset(out)
    _PER_SOURCE_DAY_CACHE[(source_key, day)] = frozen
    return frozen


def _build_fid_to_canonical(fixtures_df: pd.DataFrame) -> dict[str, str]:
    """Build af_fixture_id → canonical UAC league_id from the day's fixtures sibling.

    Prefer the canonical ``league_id`` column (already canonical, written by
    ``rescan_sports_fixtures_canonical.py``). Fall back to UAC lookup via
    ``af_league_id`` if the canonical column is absent.
    """
    if "af_fixture_id" not in fixtures_df.columns:
        return {}

    out: dict[str, str] = {}
    if "league_id" in fixtures_df.columns:
        sub = fixtures_df[["af_fixture_id", "league_id"]].dropna()
        for _, row in sub.iterrows():
            try:
                af_fid = _strip_float_suffix(row["af_fixture_id"])
                lid = str(row["league_id"]).strip().upper()
            except (TypeError, ValueError):
                continue
            if lid:
                out[af_fid] = lid
        if out:
            return out

    if "af_league_id" not in fixtures_df.columns:
        return out
    sub = fixtures_df[["af_fixture_id", "af_league_id"]].dropna()
    af_lid_to_canonical: dict[int, str] = {}
    for _, row in sub.iterrows():
        try:
            af_fid = _strip_float_suffix(row["af_fixture_id"])
            af_lid = int(row["af_league_id"])
        except (TypeError, ValueError):
            continue
        canonical = af_lid_to_canonical.get(af_lid)
        if canonical is None:
            league = get_league_by_api_football_id(af_lid)
            if league is None:
                continue
            canonical = league.league_id
            af_lid_to_canonical[af_lid] = canonical
        out[af_fid] = canonical
    return out


def _backfill_fixture_scoped_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    specs: list[EntitySpec],
    *,
    dry_run: bool,
) -> dict[str, dict[str, int]]:
    """Process all fixture-scoped entities for a single day in one pass.

    For each entity, emits one ``add(row_count=N)`` per (date, canonical_league)
    that has rows in the entity parquet, AND one ``record_empty()`` per
    (date, canonical_league) where fixtures played that day but the entity
    parquet had no rows for it. Honest-coverage v5 — the parquet existing
    on disk proves the day's job ran, so leagues without rows are
    attempted-but-empty, not missing.

    Returns ``{data_type: {canonical_league_id: row_count_or_0}}``. Zero
    means the empty_confirmed branch fired.
    """
    fixtures_path = f"{ROOT}day={day}/entity=fixtures/fixtures.parquet"
    fixtures_df = _read_parquet(client, fixtures_path)
    if fixtures_df is None or fixtures_df.empty:
        return {}
    fid_to_canonical = _build_fid_to_canonical(fixtures_df)
    if not fid_to_canonical:
        return {}
    # Set of canonical leagues that played that day (fixtures sibling = ground truth).
    day_leagues: set[str] = set(fid_to_canonical.values())

    out: dict[str, dict[str, int]] = {}
    proc_date = date_type.fromisoformat(day)
    attempted_at = datetime.combine(proc_date, datetime.min.time(), tzinfo=UTC)
    for spec in specs:
        path = f"{ROOT}day={day}/entity={spec.folder}/{spec.folder}.parquet"
        ent_df = _read_parquet(client, path)
        if ent_df is None or ent_df.empty:
            # No parquet — orchestrator never ran this entity for this day.
            # Emit nothing; reader will count as missing.
            continue
        fid_col = _pick_fid_col(ent_df, spec.fid_cols)
        if fid_col is None:
            continue

        counts: dict[str, int] = {}
        for fid_raw in ent_df[fid_col].dropna().tolist():
            fid = _strip_float_suffix(fid_raw)
            canonical = fid_to_canonical.get(fid)
            if canonical is None:
                continue
            counts[canonical] = counts.get(canonical, 0) + 1

        # Expected = leagues UAC declares the source covers AND that are in-season
        # on this day per UAC fixture calendar. Matches the data_status reader's
        # expected_shards exactly. Leagues UAC says "expected" but with no rows in
        # the entity parquet → attempted-but-empty.
        expected = _expected_leagues_in_season(spec.source, day) if spec.source else day_leagues
        empty_leagues = expected - counts.keys()

        out[spec.data_type] = {**counts, **dict.fromkeys(empty_leagues, 0)}
        if not dry_run and manifest is not None:
            for lid, count in counts.items():
                manifest.add(
                    processing_date=proc_date,
                    row_count=count,
                    data_type=spec.data_type,
                    league_id=lid,
                    venue="",
                )
            for lid in empty_leagues:
                manifest.record_empty(
                    row_key={
                        "date": str(proc_date),
                        "data_type": spec.data_type,
                        "league_id": lid,
                        "venue": "",
                    },
                    attempted_at=attempted_at,
                    pipeline_mode=_pipeline_mode_for_spec(spec),
                )
    return out


def _build_venue_to_canonical(fixtures_df: pd.DataFrame) -> dict[str, set[str]]:
    """Build SCREAMING_SNAKE(venue_name) → {canonical_league_id, ...} map.

    Replicates the orchestrator's WEATHER emission keying (venue_name →
    SCREAMING_SNAKE'd venue_id). A venue can host multiple leagues on the
    same day (cup + league doubles), so the map is set-valued. Prefer
    canonical ``league_id`` column; fall back to UAC lookup via
    ``af_league_id``.
    """
    out: dict[str, set[str]] = {}
    if "venue_name" not in fixtures_df.columns:
        return out

    if "league_id" in fixtures_df.columns:
        for _, row in fixtures_df[["venue_name", "league_id"]].dropna().iterrows():
            try:
                vname = str(row["venue_name"]).strip()
                lid = str(row["league_id"]).strip().upper()
            except (TypeError, ValueError):
                continue
            if not vname or not lid:
                continue
            snake = re.sub(r"\s+", "_", re.sub(r"[^A-Za-z0-9 ]", "", vname).strip()).upper()
            out.setdefault(snake, set()).add(lid)
        if out:
            return out

    if "af_league_id" not in fixtures_df.columns:
        return out
    af_lid_to_canonical: dict[int, str] = {}
    for _, row in fixtures_df[["venue_name", "af_league_id"]].dropna().iterrows():
        try:
            vname = str(row["venue_name"]).strip()
            af_lid = int(row["af_league_id"])
        except (TypeError, ValueError):
            continue
        if not vname:
            continue
        canonical = af_lid_to_canonical.get(af_lid)
        if canonical is None:
            league = get_league_by_api_football_id(af_lid)
            if league is None:
                continue
            canonical = league.league_id
            af_lid_to_canonical[af_lid] = canonical
        snake = re.sub(r"\s+", "_", re.sub(r"[^A-Za-z0-9 ]", "", vname).strip()).upper()
        out.setdefault(snake, set()).add(canonical)
    return out


def _backfill_weather_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """WEATHER joins via venue_id → fixtures.venue_name → af_league_id → canonical."""
    weather_path = f"{ROOT}day={day}/entity=weather/weather.parquet"
    weather_df = _read_parquet(client, weather_path)
    if weather_df is None or weather_df.empty or "venue_id" not in weather_df.columns:
        return {}
    fixtures_df = _read_parquet(client, f"{ROOT}day={day}/entity=fixtures/fixtures.parquet")
    if fixtures_df is None or fixtures_df.empty:
        return {}
    venue_to_leagues = _build_venue_to_canonical(fixtures_df)
    if not venue_to_leagues:
        return {}

    counts: dict[str, int] = {}
    for vid_raw in weather_df["venue_id"].dropna().astype(str).unique():
        for lid in venue_to_leagues.get(vid_raw, set()):
            counts[lid] = counts.get(lid, 0) + 1
    # Expected = open_meteo's UAC leagues x in-season-on-day filter.
    expected = _expected_leagues_in_season("open_meteo", day)
    empty_leagues = expected - counts.keys()

    if not dry_run and manifest is not None and (counts or empty_leagues):
        proc_date = date_type.fromisoformat(day)
        attempted_at = datetime.combine(proc_date, datetime.min.time(), tzinfo=UTC)
        for lid, count in counts.items():
            manifest.add(
                processing_date=proc_date,
                row_count=count,
                data_type="WEATHER",
                league_id=lid,
                venue="",
            )
        for lid in empty_leagues:
            manifest.record_empty(
                row_key={
                    "date": str(proc_date),
                    "data_type": "WEATHER",
                    "league_id": lid,
                    "venue": "",
                },
                attempted_at=attempted_at,
                pipeline_mode=PipelineMode.BATCH_OPEN_METEO,
            )
    return {**counts, **dict.fromkeys(empty_leagues, 0)}


def _backfill_injuries_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """INJURIES `fixture` column is a struct {id,date,timestamp}; extract id."""
    inj_path = f"{ROOT}day={day}/entity=injuries/injuries.parquet"
    inj_df = _read_parquet(client, inj_path)
    if inj_df is None or inj_df.empty or "fixture" not in inj_df.columns:
        return {}
    fixtures_df = _read_parquet(client, f"{ROOT}day={day}/entity=fixtures/fixtures.parquet")
    if fixtures_df is None or fixtures_df.empty:
        return {}
    fid_to_canonical = _build_fid_to_canonical(fixtures_df)
    if not fid_to_canonical:
        return {}

    counts: dict[str, int] = {}
    for raw in inj_df["fixture"].dropna().tolist():
        # struct dict from arrow → either dict or pandas Series
        try:
            fid_val = raw["id"] if hasattr(raw, "__getitem__") else None
        except (KeyError, TypeError):
            fid_val = None
        if fid_val is None:
            continue
        fid = _strip_float_suffix(fid_val)
        canonical = fid_to_canonical.get(fid)
        if canonical is None:
            continue
        counts[canonical] = counts.get(canonical, 0) + 1
    expected = _expected_leagues_in_season("api_football", day)
    empty_leagues = expected - counts.keys()

    if not dry_run and manifest is not None and (counts or empty_leagues):
        proc_date = date_type.fromisoformat(day)
        attempted_at = datetime.combine(proc_date, datetime.min.time(), tzinfo=UTC)
        for lid, count in counts.items():
            manifest.add(
                processing_date=proc_date,
                row_count=count,
                data_type="INJURIES",
                league_id=lid,
                venue="",
            )
        for lid in empty_leagues:
            manifest.record_empty(
                row_key={
                    "date": str(proc_date),
                    "data_type": "INJURIES",
                    "league_id": lid,
                    "venue": "",
                },
                attempted_at=attempted_at,
                pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            )
    return {**counts, **dict.fromkeys(empty_leagues, 0)}


def _backfill_xg_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """understat_xg.parquet has canonical UAC league_id in `league_league_id`."""
    xg_path = f"{ROOT}day={day}/entity=understat_xg/understat_xg.parquet"
    df = _read_parquet(client, xg_path)
    if df is None or df.empty or "league_league_id" not in df.columns:
        return {}
    counts: dict[str, int] = {}
    for raw in df["league_league_id"].dropna().tolist():
        lid = str(raw).strip().upper()
        if not lid:
            continue
        counts[lid] = counts.get(lid, 0) + 1
    if not counts:
        return {}
    if not dry_run and manifest is not None:
        proc_date = date_type.fromisoformat(day)
        for lid, count in counts.items():
            manifest.add(
                processing_date=proc_date,
                row_count=count,
                data_type="XG",
                league_id=lid,
                venue="",
            )
    return counts


def _backfill_fixture_denorm_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    spec: EntitySpec,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Per-fixture-day denormalisation for entities like PLAYER_VALUES whose
    physical parquet is a weekly snapshot but whose breakdown semantic is
    fixture-scoped. Emits one ``add(row_count=1)`` per (canonical_league, day)
    where the league is in-season per UAC and declared by the source.
    """
    expected = _expected_leagues_in_season(spec.source, day) if spec.source else frozenset()
    if not expected:
        return {}
    counts = dict.fromkeys(expected, 1)
    if not dry_run and manifest is not None:
        proc_date = date_type.fromisoformat(day)
        for lid in expected:
            manifest.add(
                processing_date=proc_date,
                row_count=1,
                data_type=spec.data_type,
                league_id=lid,
                venue="",
            )
    return counts


def _backfill_league_direct_day(
    client: storage.Client,
    manifest: ManifestWriter | None,
    day: str,
    spec: EntitySpec,
    canonical_leagues: list[str],
    *,
    dry_run: bool,
) -> dict[str, int]:
    """For a league_direct entity: if the parquet exists on this day, emit one
    manifest row per (day, canonical_league) for every UAC league this source
    publishes. Uses parquet presence as proof that the source's periodic
    snapshot landed for that day.
    """
    path = f"{ROOT}day={day}/entity={spec.folder}/{spec.folder}.parquet"
    ent_df = _read_parquet(client, path)
    if ent_df is None or ent_df.empty:
        return {}
    proc_date = date_type.fromisoformat(day)
    counts: dict[str, int] = dict.fromkeys(canonical_leagues, 1)
    if not dry_run and manifest is not None:
        for lid in canonical_leagues:
            manifest.add(
                processing_date=proc_date,
                row_count=1,
                data_type=spec.data_type,
                league_id=lid,
                venue="",
            )
    return counts


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--entities",
        type=str,
        default="",
        help="Comma-separated subset of data_types (default = all).",
    )
    parser.add_argument(
        "--day",
        type=str,
        default="",
        help="Single day YYYY-MM-DD (smoke test). Mutually exclusive with --limit.",
    )
    args = parser.parse_args(argv)

    requested = {e.strip().upper() for e in args.entities.split(",") if e.strip()}
    active_specs = [s for s in SPECS if not requested or s.data_type in requested]
    if requested - {s.data_type for s in active_specs}:
        unknown = requested - {s.data_type for s in active_specs}
        logger.warning("ignoring unknown entities: %s", ",".join(sorted(unknown)))

    fixture_specs = [s for s in active_specs if s.scope == "fixture"]
    weather_specs = [s for s in active_specs if s.scope == "weather"]
    injuries_specs = [s for s in active_specs if s.scope == "injuries"]
    xg_specs = [s for s in active_specs if s.scope == "xg"]
    fixture_denorm_specs = [s for s in active_specs if s.scope == "fixture_denorm"]
    league_direct_specs = [s for s in active_specs if s.scope == "league_direct"]
    singleton_specs = [s for s in active_specs if s.scope == "singleton"]

    # Pre-resolve canonical league sets per source for league_direct specs.
    source_to_canonical: dict[str, list[str]] = {}
    for spec in league_direct_specs:
        if spec.source in source_to_canonical:
            continue
        leagues = get_expected_leagues_for_source(spec.source)
        source_to_canonical[spec.source] = [lg.league_id for lg in leagues]
        logger.info(
            "source=%s declares %d canonical leagues",
            spec.source,
            len(source_to_canonical[spec.source]),
        )

    client = storage.Client(project="central-element-323112")
    if args.day:
        days = [args.day]
    else:
        days = _list_days(client)
        if args.limit:
            days = days[: args.limit]
    logger.info(
        "backfilling %d days, %d fixture specs, %d league_direct specs (dry_run=%s)",
        len(days),
        len(fixture_specs),
        len(league_direct_specs),
        args.dry_run,
    )

    manifest = None if args.dry_run else ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)

    results: dict[str, EntityResult] = {s.data_type: EntityResult(s.data_type) for s in active_specs}

    t0 = time.monotonic()

    # ---- Fixture-scoped pass ----------------------------------------------------------------
    if fixture_specs:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(
                    _backfill_fixture_scoped_day,
                    client,
                    manifest,
                    d,
                    fixture_specs,
                    dry_run=args.dry_run,
                ): d
                for d in days
            }
            for i, fut in enumerate(as_completed(futures), 1):
                day = futures[fut]
                try:
                    day_out = fut.result()
                except (RuntimeError, ValueError, OSError) as exc:
                    logger.warning("fixture-scoped day=%s failed: %s", day, exc)
                    continue
                for dt, counts in day_out.items():
                    r = results[dt]
                    r.rows_written += sum(counts.values()) if args.dry_run else len(counts)
                    r.leagues.update(counts.keys())
                    if counts:
                        r.days_with_data.add(day)
                if i % 200 == 0:
                    logger.info("fixture pass: %d/%d in %.1fs", i, len(days), time.monotonic() - t0)
        logger.info("fixture pass done in %.1fs", time.monotonic() - t0)

    # ---- League-direct pass -----------------------------------------------------------------
    if league_direct_specs:
        t1 = time.monotonic()
        for spec in league_direct_specs:
            canonical = source_to_canonical[spec.source]
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(
                        _backfill_league_direct_day,
                        client,
                        manifest,
                        d,
                        spec,
                        canonical,
                        dry_run=args.dry_run,
                    ): d
                    for d in days
                }
                for fut in as_completed(futures):
                    day = futures[fut]
                    try:
                        counts = fut.result()
                    except (RuntimeError, ValueError, OSError) as exc:
                        logger.warning("league-direct %s day=%s failed: %s", spec.data_type, day, exc)
                        continue
                    if counts:
                        r = results[spec.data_type]
                        r.rows_written += len(counts)
                        r.leagues.update(counts.keys())
                        r.days_with_data.add(day)
            logger.info(
                "league-direct %s done in %.1fs (%d rows across %d days x %d leagues)",
                spec.data_type,
                time.monotonic() - t1,
                results[spec.data_type].rows_written,
                len(results[spec.data_type].days_with_data),
                len(results[spec.data_type].leagues),
            )

    # ---- WEATHER pass (venue→league join) -------------------------------------------------
    if weather_specs:
        t1 = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_backfill_weather_day, client, manifest, d, dry_run=args.dry_run): d for d in days}
            for fut in as_completed(futures):
                day = futures[fut]
                try:
                    counts = fut.result()
                except (RuntimeError, ValueError, OSError) as exc:
                    logger.warning("weather day=%s failed: %s", day, exc)
                    continue
                if counts:
                    r = results["WEATHER"]
                    r.rows_written += sum(counts.values()) if args.dry_run else len(counts)
                    r.leagues.update(counts.keys())
                    r.days_with_data.add(day)
        logger.info("weather pass done in %.1fs", time.monotonic() - t1)

    # ---- XG pass (canonical league_id directly in league_league_id) -----------------------
    if xg_specs:
        t1 = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_backfill_xg_day, client, manifest, d, dry_run=args.dry_run): d for d in days}
            for fut in as_completed(futures):
                day = futures[fut]
                try:
                    counts = fut.result()
                except (RuntimeError, ValueError, OSError) as exc:
                    logger.warning("xg day=%s failed: %s", day, exc)
                    continue
                if counts:
                    r = results["XG"]
                    r.rows_written += sum(counts.values()) if args.dry_run else len(counts)
                    r.leagues.update(counts.keys())
                    r.days_with_data.add(day)
        logger.info("xg pass done in %.1fs", time.monotonic() - t1)

    # ---- INJURIES pass (struct extract) ----------------------------------------------------
    if injuries_specs:
        t1 = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_backfill_injuries_day, client, manifest, d, dry_run=args.dry_run): d for d in days}
            for fut in as_completed(futures):
                day = futures[fut]
                try:
                    counts = fut.result()
                except (RuntimeError, ValueError, OSError) as exc:
                    logger.warning("injuries day=%s failed: %s", day, exc)
                    continue
                if counts:
                    r = results["INJURIES"]
                    r.rows_written += sum(counts.values()) if args.dry_run else len(counts)
                    r.leagues.update(counts.keys())
                    r.days_with_data.add(day)
        logger.info("injuries pass done in %.1fs", time.monotonic() - t1)

    # ---- Fixture-denorm pass (PLAYER_VALUES per-fixture-date for cohesion) -----------------
    if fixture_denorm_specs:
        t1 = time.monotonic()
        for spec in fixture_denorm_specs:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {
                    pool.submit(
                        _backfill_fixture_denorm_day,
                        client,
                        manifest,
                        d,
                        spec,
                        dry_run=args.dry_run,
                    ): d
                    for d in days
                }
                for fut in as_completed(futures):
                    day = futures[fut]
                    try:
                        counts = fut.result()
                    except (RuntimeError, ValueError, OSError) as exc:
                        logger.warning("fixture-denorm %s day=%s failed: %s", spec.data_type, day, exc)
                        continue
                    if counts:
                        r = results[spec.data_type]
                        r.rows_written += len(counts)
                        r.leagues.update(counts.keys())
                        r.days_with_data.add(day)
            logger.info(
                "fixture-denorm %s done in %.1fs",
                spec.data_type,
                time.monotonic() - t1,
            )

    # ---- Singleton pass --------------------------------------------------------------------
    # The data-status reader for axis=global_season returns
    # ``min(found_dates, expected_dates)`` where expected_dates=1. Writing the
    # singleton at every day in the data range means any non-empty date window
    # catches it (cross-asset_group date filter happens before
    # ``_sports_honest_coverage`` runs). The ``min(N, 1)`` cap ensures the
    # pct stays at 100% regardless of the duplication.
    for spec in singleton_specs:
        flat_path = f"sports_reference/{spec.folder}/{spec.folder}.parquet"
        try:
            exists = client.bucket(BUCKET).blob(flat_path).exists()
        except (NotFound, OSError) as exc:
            logger.warning("singleton %s probe failed: %s", spec.data_type, exc)
            continue
        if not exists:
            logger.info("singleton %s: file absent", spec.data_type)
            continue
        # Singleton — emit ONE manifest row at SINGLETON_SENTINEL_DATE.
        # Pre-2026-05-04 we emitted one row per day in the data range as a
        # workaround for date-window queries. That polluted the manifest
        # with 3,627+ duplicate sentinel rows that the phantom audit kept
        # flagging because its day-listing strategy can't see the flat
        # singleton path. The audit was hardened to probe singleton flat
        # paths directly via UAC's ``candidate_parquet_paths`` SSOT, so
        # date-window matching is no longer the writer's concern — readers
        # that need date-window filtering for singletons should use the
        # SSOT probe, not the manifest row's date.
        if not args.dry_run and manifest is not None:
            sentinel_date = date_type.fromisoformat(SINGLETON_SENTINEL_DATE)
            manifest.add(
                processing_date=sentinel_date,
                row_count=1,
                data_type=spec.data_type,
                league_id="",
                venue="",
            )
        r = results[spec.data_type]
        r.rows_written += 1
        r.days_with_data.add(SINGLETON_SENTINEL_DATE)
        logger.info(
            "singleton %s: emitted 1 sentinel row at %s (was per-day; collapsed 2026-05-04)",
            spec.data_type,
            SINGLETON_SENTINEL_DATE,
        )

    if not args.dry_run and manifest is not None:
        logger.info("flushing manifest …")
        manifest.flush()
        logger.info("manifest flushed")

    # ---- Report -----------------------------------------------------------------------------
    logger.info("=" * 80)
    for spec in active_specs:
        r = results[spec.data_type]
        logger.info(
            "  %-25s rows=%6d  days=%4d  leagues=%3d",
            spec.data_type,
            r.rows_written,
            len(r.days_with_data),
            len(r.leagues),
        )
    logger.info("=" * 80)

    out = {
        "_meta": {
            "bucket": BUCKET,
            "src_root": ROOT,
            "dry_run": args.dry_run,
            "total_days": len(days),
            "fixture_specs": [s.data_type for s in fixture_specs],
            "league_direct_specs": [s.data_type for s in league_direct_specs],
            "elapsed_seconds": round(time.monotonic() - t0, 1),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "by_data_type": {dt: r.to_dict() for dt, r in sorted(results.items())},
    }
    OUT_PATH.write_text(json.dumps(out, indent=2))
    logger.info("report: %s (%d bytes)", OUT_PATH, OUT_PATH.stat().st_size)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
