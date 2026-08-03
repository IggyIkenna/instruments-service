#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after plans/active/issues/sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md is archived
"""Reconcile §U's exact population against the current football-only non-canonical manifest cut.

WHY (``sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md``, [DIAG] P2 todo): §U's approved purge
(489 (league,season) pairs / 10,869 blank-``round`` rows, ``sports_features_layer_findings_sweep_2026_07_18.md``
§ U) was measured over raw FIXTURES parquet CONTENT — blank ``round``, in-window season (2019-2027), league
identified by the NUMERIC ``af_league_id`` and judged "not in the registry universe" via a 2026-07-19 registry
snapshot that (unbeknownst at the time) only covered 94 leagues. The CURRENT manifest-index non-canonical census
(the authoritative baseline in the issue doc, 11,403 rows / 755 league_ids) is keyed by the STRING ``league_id``
column and the FULL, now-383-league registry. Neither the key (numeric vs string) nor the registry snapshot
(94 vs 383 leagues) match, so §U's population was never provably a subset of the manifest-index cut.

THIS SCRIPT closes that gap with a single fresh walk of the raw ``fixtures_schedule`` corpus:

1. Reproduces §U's population LIVE, against the CURRENT (383-league) registry: blank ``round``, season in
   [2019, 2027], ``af_league_id`` unresolvable via ``get_league_by_api_football_id`` (i.e. absent from the
   registry today, not in 2026-07-19).
2. For every qualifying ``af_league_id``, reads the GCS path's ``league=<X>`` partition segment — the exact
   string the FIXTURES writer uses to key that league on disk, and (per ``canonicalize_manifest_league_ids.py``)
   the same string space the manifest ``league_id`` column carries for this data_type. Checks whether that
   string is a member of the CURRENT canonical set (``get_expected_leagues_for_source("api_football")`` —
   the same frozenset ``delete_noncanonical_sports_leagues_2026_06_25.py`` already uses).
3. If every one of §U's leagues resolves to a path-league-id OUTSIDE the current canonical set, §U's population
   is a subset of what the current (fixed) delete script would remove — answered YES. Any exception is
   reported by name, not averaged away.

DEDUP: a day carries BOTH a bare multi-league ``fixtures_schedule.parquet`` AND per-league
``league=<L>/fixtures_schedule.parquet`` siblings holding the SAME fixtures — every row is deduped by
``af_fixture_id`` (single walk, both legs, count once).

SAFETY: read-only. No ``--apply``, no writes, no deletes, no GCS mutation of any kind.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger("reconcile_sports_g1_sec_u_population")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_PREFIX = "sports_reference/by_date/"
_ENTITY = "/entity=fixtures_schedule/"
_ENTITY_FILE = "fixtures_schedule.parquet"
_COLUMNS = ["af_fixture_id", "af_league_id", "season", "round"]
_WINDOW_MIN_SEASON = 2019
_WINDOW_MAX_SEASON = 2027


def _storage():
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    return get_storage_client()


def _path_league_id(blob_name: str) -> str:
    """Extract the ``league=<X>`` path partition value; "" for the bare multi-league file."""
    marker = "/league="
    i = blob_name.find(marker)
    if i == -1:
        return ""
    start = i + len(marker)
    end = blob_name.find("/", start)
    return blob_name[start:end] if end != -1 else blob_name[start:]


def _is_blank_round(value: object) -> bool:
    return str(value).strip() in {"", "none", "None", "nan", "NaN", "<NA>"}


def _in_window(season: object) -> bool:
    try:
        s = int(season)
    except (TypeError, ValueError):
        return False
    return _WINDOW_MIN_SEASON <= s <= _WINDOW_MAX_SEASON


def _corpus_blobs(bucket: str) -> list[str]:
    """SINGLE listing of the fixtures_schedule corpus (bare + per-league legs)."""
    client = _storage()
    names: list[str] = []
    for blob in client.list_blobs(bucket, prefix=_PREFIX):
        name = str(blob.name)
        if _ENTITY in name and name.endswith(_ENTITY_FILE):
            names.append(name)
    return names


@dataclass
class _FixtureRow:
    af_league_id: int
    season: int
    blank_round: bool
    path_league_id: str


def _load_one(bucket: str, name: str) -> list[_FixtureRow]:
    client = _storage()
    try:
        raw = client.download_bytes(bucket=bucket, blob_path=name)
        df = pd.read_parquet(io.BytesIO(raw), columns=_COLUMNS)
    except Exception as exc:
        logger.warning("unreadable %s: %s", name, exc)
        return []
    plid = _path_league_id(name)
    out: list[_FixtureRow] = []
    for rec in df.itertuples(index=False):
        out.append(
            _FixtureRow(
                af_league_id=int(rec.af_league_id),
                season=int(rec.season),
                blank_round=_is_blank_round(rec.round),
                path_league_id=plid,
            )
        )
    return out


@dataclass
class _Census:
    # af_fixture_id -> row, deduped across bare/per-league siblings
    by_fixture: dict[int, _FixtureRow] = field(default_factory=dict)


def _walk(bucket: str, max_workers: int, max_files: int | None) -> _Census:
    names = _corpus_blobs(bucket)
    if max_files:
        names = names[:max_files]
    logger.info("single walk: %d fixtures_schedule parquet file(s)", len(names))

    census = _Census()
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for name in names:
            client = _storage()
            raw_ok = True
            try:
                raw = client.download_bytes(bucket=bucket, blob_path=name)
            except Exception as exc:
                logger.warning("unreadable %s: %s", name, exc)
                raw_ok = False
            if not raw_ok:
                continue
            futures[pool.submit(_parse_bytes, raw, name)] = name
        for fut in as_completed(futures):
            done += 1
            rows = fut.result()
            for af_fixture_id, row in rows:
                census.by_fixture[af_fixture_id] = row
            if done % 5000 == 0:
                logger.info("walked %d/%d files, %d unique fixtures so far", done, len(names), len(census.by_fixture))
    return census


def _parse_bytes(raw: bytes, name: str) -> list[tuple[int, _FixtureRow]]:
    try:
        df = pd.read_parquet(io.BytesIO(raw), columns=[*_COLUMNS])
    except Exception as exc:
        logger.warning("unparseable %s: %s", name, exc)
        return []
    plid = _path_league_id(name)
    out: list[tuple[int, _FixtureRow]] = []
    for rec in df.itertuples(index=False):
        fid = int(rec.af_fixture_id)
        out.append(
            (
                fid,
                _FixtureRow(
                    af_league_id=int(rec.af_league_id),
                    season=int(rec.season),
                    blank_round=_is_blank_round(rec.round),
                    path_league_id=plid,
                ),
            )
        )
    return out


def _registry_lookup(af_league_id: int) -> bool:
    """True if af_league_id resolves to a CURRENT registry league (383-league set)."""
    from unified_api_contracts.sports import get_league_by_api_football_id  # noqa: qg-inside-import

    return get_league_by_api_football_id(af_league_id) is not None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", default=_BUCKET)
    ap.add_argument("--max-workers", type=int, default=32)
    ap.add_argument("--max-files", type=int, default=None, help="Cap files scanned (pilot only).")
    ap.add_argument("--out", default=None, help="Optional path to write the JSON result.")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("read-only single walk of %s%s ...", args.bucket, _PREFIX)

    census = _walk(args.bucket, args.max_workers, args.max_files)
    logger.info("walk done: %d unique fixtures", len(census.by_fixture))

    # Reproduce §U live: blank round, in-window season, af_league_id NOT in the CURRENT registry.
    pair_blanks: dict[tuple[int, int], int] = {}
    league_path_ids: dict[int, set[str]] = {}
    registry_cache: dict[int, bool] = {}

    for row in census.by_fixture.values():
        if not row.blank_round or not _in_window(row.season):
            continue
        if row.af_league_id not in registry_cache:
            registry_cache[row.af_league_id] = _registry_lookup(row.af_league_id)
        if registry_cache[row.af_league_id]:
            continue  # in registry today -> not part of §U's "absent from registry" population
        key = (row.af_league_id, row.season)
        pair_blanks[key] = pair_blanks.get(key, 0) + 1
        league_path_ids.setdefault(row.af_league_id, set()).add(row.path_league_id)

    n_pairs = len(pair_blanks)
    n_rows = sum(pair_blanks.values())
    logger.info("LIVE §U-equivalent population: %d (league,season) pairs / %d blank rows", n_pairs, n_rows)

    # Subset check against the current canonical set (same frozenset the fixed delete script uses).
    from unified_api_contracts.sports import get_expected_leagues_for_source  # noqa: qg-inside-import

    canonical_ids = frozenset(lg.league_id for lg in get_expected_leagues_for_source("api_football"))
    logger.info("current canonical set: %d league_ids", len(canonical_ids))

    exceptions: list[dict[str, object]] = []
    for af_id, plids in league_path_ids.items():
        for plid in plids:
            if plid in canonical_ids:
                exceptions.append({"af_league_id": af_id, "path_league_id": plid})

    subset = len(exceptions) == 0
    result = {
        "live_sec_u_pairs": n_pairs,
        "live_sec_u_blank_rows": n_rows,
        "original_sec_u_pairs": 489,
        "original_sec_u_blank_rows": 10869,
        "distinct_af_league_ids": len(league_path_ids),
        "canonical_set_size": len(canonical_ids),
        "subset_of_current_noncanonical_cut": subset,
        "exceptions": exceptions,
        "files_walked": len(census.by_fixture),
    }
    logger.info("RESULT: %s", json.dumps(result, indent=2))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        logger.info("wrote result to %s", args.out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
