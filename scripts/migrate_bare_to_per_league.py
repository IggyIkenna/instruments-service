#!/usr/bin/env python3
"""Migrate legacy bare-path date-aggregate sports parquets to per-league subpartitions.

Background:
    Sports manifest historically had two on-disk shapes for league-axis data:
      - bare:        sports_reference/by_date/day={D}/entity={F}/{F}.parquet  (one file per day, all leagues mixed)
      - per-league:  sports_reference/by_date/day={D}/entity={F}/league={L}/{F}.parquet

    The orchestrator now writes per-league only. Existing bare files are
    legacy from earlier versions. This script reads each bare parquet,
    splits rows by league_id (direct column, fixture_id->FIXTURES join, or
    competition_id parse for footystats), writes per-league parquets at
    canonical paths, updates the manifest with per-league captured rows,
    and deletes the bare parquet.

    Idempotent: re-runs are safe — already-migrated dates are skipped.

Usage:
    cd instruments-service
    .venv/bin/python scripts/migrate_bare_to_per_league.py --dry-run
    .venv/bin/python scripts/migrate_bare_to_per_league.py --data-types FIXTURE_EVENTS,FIXTURE_LINEUPS
    .venv/bin/python scripts/migrate_bare_to_per_league.py            # full migration

Skips data types with no league axis (LEAGUES, VENUES, SFI_LEAGUES,
SFI_STANDINGS, TRANSFERMARKT_LEAGUES) — those correctly stay bare.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import (
    FOOTYSTATS_HISTORICAL_SEASON_IDS,
    SOCCER_FOOTBALL_INFO_IDS,
    SPORTS_DATA_TYPE_TO_FOLDER,
    get_leagues_by_classification,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"

# Data types that genuinely have no league axis — keep bare-only, skip migration.
GLOBAL_AXIS_DATA_TYPES = frozenset(
    {
        "LEAGUES",
        "VENUES",
        "SFI_LEAGUES",
        "SFI_STANDINGS",
        "TRANSFERMARKT_LEAGUES",
        "TEAMS",
    }
)


def _af_league_to_canonical_map() -> dict[int, str]:
    """UAC: api_football_id -> canonical league_id (Prediction + Features + Reference).

    The orchestrator captures all 95 api-football leagues (33 Prediction + 22
    Features + 40 Reference) so the migration must cover all classifications,
    not just Prediction — otherwise non-prediction fixture_ids resolve to no
    league and we leave most rows in the bare format.
    """
    out: dict[int, str] = {}
    for cls in ("Prediction", "Features", "Reference"):
        for ld in get_leagues_by_classification(cls):
            if ld.api_football_id is not None:
                out[ld.api_football_id] = ld.league_id
    return out


# Date-keyed cache for fixtures league maps. Multiple data types per date
# (FIXTURE_EVENTS / LINEUPS / STATS / PLAYER_STATS / WEATHER) share the same
# fixtures lookup — caching turns 5N downloads/date into 1N. Lock guards
# concurrent access from ThreadPoolExecutor workers.
_FIXTURES_MAP_CACHE: dict[str, dict[str, str]] = {}
_FIXTURES_MAP_LOCK = threading.Lock()


def _load_fixtures_league_map(
    bucket: storage.Bucket,
    date: str,
    af_to_canonical: dict[int, str],
) -> dict[str, str]:
    """Return ``{fixture_id_str: canonical_league_id}`` for the given date.

    Probes per-league fixtures parquets (modern path) AND the bare fixtures
    parquet (legacy) so we can map fixture_id -> league across all sports
    data types that are keyed by fixture. Date-keyed cached.
    """
    with _FIXTURES_MAP_LOCK:
        if date in _FIXTURES_MAP_CACHE:
            return _FIXTURES_MAP_CACHE[date]
    result: dict[str, str] = {}
    # Try per-league fixtures first (modern format).
    prefix = f"sports_reference/by_date/day={date}/entity=fixtures/"
    for blob in bucket.list_blobs(prefix=prefix):
        try:
            local = f"{tempfile.gettempdir()}/mig_fix_{date}_{blob.name.replace('/', '_')}"
            blob.download_to_filename(local)
            df = pd.read_parquet(local)
            id_col = next((c for c in ("af_fixture_id", "fixture_id", "id") if c in df.columns), None)
            if id_col is None:
                continue
            league_col = "league_id" if "league_id" in df.columns else None
            if league_col is not None:
                for _, row in df[[id_col, league_col]].dropna().iterrows():
                    fid = str(row[id_col]).split(".")[0]  # int-as-float -> int str
                    result[fid] = str(row[league_col])
            elif "af_league_id" in df.columns:
                for _, row in df[[id_col, "af_league_id"]].dropna().iterrows():
                    fid = str(row[id_col]).split(".")[0]
                    af_lid_raw = row["af_league_id"]
                    try:
                        af_lid = int(float(af_lid_raw))
                    except (TypeError, ValueError):
                        continue
                    canonical = af_to_canonical.get(af_lid)
                    if canonical:
                        result[fid] = canonical
        except Exception as exc:
            logger.debug("fixtures parquet read failed for %s: %s", blob.name, exc)
            continue
    with _FIXTURES_MAP_LOCK:
        _FIXTURES_MAP_CACHE[date] = result
    return result


def _resolve_league_for_row(
    row: pd.Series,
    data_type: str,
    fixtures_league_map: dict[str, str],
    sfi_hex_to_canonical: dict[str, str],
) -> str:
    """Determine canonical league_id for a single bare-parquet row."""
    # 1) Direct column.
    for col in ("league_id", "league", "competition", "championship_id"):
        if col in row.index:
            v = row[col]
            if isinstance(v, dict):
                lid = v.get("id") or v.get("league_id")
                if lid:
                    return str(lid)
            elif v not in (None, "") and not (isinstance(v, float) and pd.isna(v)):
                return str(v)
    # 2) FootyStats fixture_id format: "{competition_id}:{HOME}_v_{AWAY}:..."
    fid_raw = row.get("fixture_id") if "fixture_id" in row.index else None
    if fid_raw and ":" in str(fid_raw):
        comp_str = str(fid_raw).split(":")[0]
        if comp_str.isdigit():
            canonical = FOOTYSTATS_HISTORICAL_SEASON_IDS.get(int(comp_str), "")
            if canonical:
                return canonical
    # 3) FIXTURES join — if fixture_id is purely numeric AF fixture_id.
    if fid_raw and not (isinstance(fid_raw, float) and pd.isna(fid_raw)):
        fid_str = str(fid_raw).split(".")[0]
        if fid_str in fixtures_league_map:
            return fixtures_league_map[fid_str]
    # 4) SFI: match_id is a hex string (16 chars). Map via SFI championship cache.
    if data_type == "SFI_PROGRESSIVE_STATS":
        match_id = str(row.get("match_id") or row.get("fixture_id") or "")
        if match_id in sfi_hex_to_canonical:
            return sfi_hex_to_canonical[match_id]
    return ""


def _migrate_one_shard(
    bucket: storage.Bucket,
    data_type: str,
    date: str,
    af_to_canonical: dict[int, str],
    sfi_hex_to_canonical: dict[str, str],
    dry_run: bool,
) -> tuple[dict[str, int], set[str]]:
    """Migrate a single (data_type, date) bare parquet to per-league subpartitions.

    Returns ``(counters, leagues)`` where ``leagues`` is the set of canonical
    league_ids that the bare parquet was split into.  Caller uses ``leagues``
    to emit per-league manifest rows — keeping the resolution in this single
    function avoids a second pass that re-computes them with stale state.
    """
    counters: Counter[str] = Counter()
    leagues: set[str] = set()
    folder = SPORTS_DATA_TYPE_TO_FOLDER.get(data_type, data_type.lower())
    bare_path = f"sports_reference/by_date/day={date}/entity={folder}/{folder}.parquet"
    bare_blob = bucket.blob(bare_path)
    if not bare_blob.exists():
        return dict(counters), leagues

    local = f"{tempfile.gettempdir()}/mig_bare_{data_type}_{date}.parquet"
    bare_blob.download_to_filename(local)
    df = pd.read_parquet(local)
    counters["rows_in"] = len(df)
    if df.empty:
        return dict(counters), leagues

    fixtures_map: dict[str, str] = {}
    if data_type in {"FIXTURE_EVENTS", "FIXTURE_LINEUPS", "FIXTURE_STATS", "PLAYER_STATS", "WEATHER"}:
        fixtures_map = _load_fixtures_league_map(bucket, date, af_to_canonical)

    df["_resolved_league"] = df.apply(
        lambda r: _resolve_league_for_row(r, data_type, fixtures_map, sfi_hex_to_canonical), axis=1
    )
    counters["rows_mapped"] = int((df["_resolved_league"] != "").sum())

    if counters["rows_mapped"] == 0:
        logger.info("  %s/%s: 0 rows could be mapped (skipping; bare retained)", data_type, date)
        return dict(counters), leagues

    grouped = df[df["_resolved_league"] != ""].groupby("_resolved_league")
    for lid, ldf in grouped:
        lid_str = str(lid)
        leagues.add(lid_str)
        per_league_path = f"sports_reference/by_date/day={date}/entity={folder}/league={lid_str}/{folder}.parquet"
        # Already-migrated check (idempotent).
        if bucket.blob(per_league_path).exists():
            counters["already_present"] += 1
            continue
        ldf_clean = ldf.drop(columns=["_resolved_league"])
        if dry_run:
            counters["would_write"] += 1
        else:
            buf = io.BytesIO()
            ldf_clean.to_parquet(buf, index=False)
            buf.seek(0)
            bucket.blob(per_league_path).upload_from_file(buf, content_type="application/octet-stream")
            counters["parquets_written"] += 1
        counters["leagues_written"] += 1

    counters["unmapped_rows"] = len(df) - counters["rows_mapped"]
    return dict(counters), leagues


def _update_manifest_after_migration(
    bucket: storage.Bucket,
    migrated: list[tuple[str, str, set[str], int]],
    dry_run: bool,
) -> None:
    """Add per-league captured rows + drop bare rows for fully-mapped shards.

    ``migrated`` is a list of ``(data_type, date, leagues_set, unmapped_rows)``.
    A bare row is only dropped when ``unmapped_rows == 0`` — partial mappings
    keep the bare row alongside the new per-league rows so unmapped data
    isn't silently orphaned.

    Idempotent on per-league rows: if a (date, data_type, league_id) row
    already exists with capture_status=captured we skip emitting a duplicate.
    """
    if not migrated:
        logger.info("Manifest update skipped — nothing migrated.")
        return
    blob = bucket.blob(INDEX_BLOB)
    blob.download_to_filename(f"{tempfile.gettempdir()}/canonical_pre_migration.parquet")
    df = pd.read_parquet(f"{tempfile.gettempdir()}/canonical_pre_migration.parquet")
    logger.info("Manifest: %d rows pre-migration", len(df))

    # Build a fast existence index for (date, data_type, league_id, captured)
    # so we don't emit duplicate per-league captured rows.
    captured_mask = df["capture_status"].fillna("").eq("captured")
    existing_keys: set[tuple[str, str, str]] = {
        (str(r.date), str(r.data_type), str(r.league_id))
        for r in df[captured_mask].itertuples()
        if str(getattr(r, "league_id", "") or "") != ""
    }

    rows_to_add: list[dict[str, object]] = []
    bare_rows_to_drop: list[tuple[str, str]] = []
    now_iso = datetime.now(UTC).isoformat()
    skipped_idempotent = 0
    for data_type, date, leagues, unmapped_rows in migrated:
        # Only drop bare row when ALL rows mapped — partial-mapped dates keep
        # both for honest accounting until a follow-up reconciliation lands.
        if unmapped_rows == 0:
            bare_rows_to_drop.append((data_type, date))
        for lid in leagues:
            key = (date, data_type, lid)
            if key in existing_keys:
                skipped_idempotent += 1
                continue
            existing_keys.add(key)
            rows_to_add.append(
                {
                    "date": date,
                    "venue": "",
                    "data_type": data_type,
                    "service_name": "instruments-service",
                    "instrument_count": 0,
                    "written_at": now_iso,
                    "schema_version": 5,
                    "timeframe": "",
                    "league_id": lid,
                    "chain": "",
                    "instrument_type": "",
                    "capture_status": "captured",
                    "error_reason": "migrated_from_bare_path",
                    "attempted_at": now_iso,
                    "expected": True,
                    "available": True,
                    "underlying": "",
                    "feature_group": "",
                    "model_family": "",
                    "training_period": "",
                    "strategy_id": "",
                    "client_id": "",
                    "instruction_type": "",
                    "instrument_id": "",
                }
            )

    drop_keys = {(dt, d) for (dt, d) in bare_rows_to_drop}
    if drop_keys:
        bare_mask = (
            df["league_id"].fillna("").eq("")
            & df["data_type"].isin({dt for (dt, _) in drop_keys})
            & df["date"].isin({d for (_, d) in drop_keys})
        )
        bare_to_drop_idx = df[bare_mask].apply(lambda r: (str(r["data_type"]), str(r["date"])) in drop_keys, axis=1)
        n_drop = int(bare_to_drop_idx.sum()) if not bare_to_drop_idx.empty else 0
    else:
        bare_mask = pd.Series([False] * len(df), index=df.index)
        bare_to_drop_idx = pd.Series([False] * 0)
        n_drop = 0
    n_add = len(rows_to_add)
    logger.info(
        "Manifest: dropping %d bare rows, adding %d per-league captured rows (skipped %d duplicates)",
        n_drop,
        n_add,
        skipped_idempotent,
    )

    if dry_run:
        logger.info("DRY RUN — manifest not modified")
        return

    if n_drop:
        df = df[~(bare_mask & bare_to_drop_idx)]
    if rows_to_add:
        df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Manifest: %d rows post-migration", len(df))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--data-types",
        type=str,
        default="",
        help="Comma-separated list to migrate (default: all league-axis types)",
    )
    parser.add_argument(
        "--delete-bare",
        action="store_true",
        help="Delete bare parquet after per-league writes succeed (default: keep for audit)",
    )
    args = parser.parse_args()
    dry = bool(args.dry_run)

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)

    blob = bucket.blob(INDEX_BLOB)
    blob.download_to_filename(f"{tempfile.gettempdir()}/canonical_audit.parquet")
    df = pd.read_parquet(f"{tempfile.gettempdir()}/canonical_audit.parquet")
    captured_bare = df[(df["capture_status"] == "captured") & (df["league_id"].fillna("") == "")]
    by_dt = captured_bare.groupby("data_type").size().sort_values(ascending=False)
    logger.info("Bare captured rows by data_type:\n%s", by_dt.to_string())

    target_types: set[str]
    if args.data_types:
        target_types = {t.strip() for t in args.data_types.split(",") if t.strip()}
    else:
        target_types = {dt for dt in by_dt.index if dt not in GLOBAL_AXIS_DATA_TYPES}
    logger.info("Target data types for migration: %s", sorted(target_types))

    af_to_canonical = _af_league_to_canonical_map()
    sfi_hex_to_canonical = {hex_id: canonical for canonical, hex_id in SOCCER_FOOTBALL_INFO_IDS.items()}

    total_counters: Counter[str] = Counter()
    # ``(data_type, date, leagues, unmapped_rows)`` so manifest update can
    # decide whether to drop bare rows (only when unmapped_rows == 0).
    migrated_for_manifest: list[tuple[str, str, set[str], int]] = []

    def _process_one(data_type: str, date: str) -> tuple[str, str, dict[str, int], set[str]]:
        """Run shard migration with retry-on-network-error; return leagues for manifest."""
        attempts = 0
        last_exc: Exception | None = None
        while attempts < 3:
            try:
                counters, leagues = _migrate_one_shard(
                    bucket, data_type, date, af_to_canonical, sfi_hex_to_canonical, dry
                )
                # Optional bare-file deletion (only when ALL rows mapped + we're committed).
                if leagues and args.delete_bare and not dry and counters.get("unmapped_rows", 0) == 0:
                    folder = SPORTS_DATA_TYPE_TO_FOLDER.get(data_type, data_type.lower())
                    bare_path = f"sports_reference/by_date/day={date}/entity={folder}/{folder}.parquet"
                    bucket.blob(bare_path).delete()
                return data_type, date, counters, leagues
            except Exception as exc:
                last_exc = exc
                attempts += 1
                logger.warning("  %s/%s: attempt %d failed (%s) — retrying", data_type, date, attempts, exc)
                time.sleep(min(60, 2**attempts * 5))
        logger.error("  %s/%s: all 3 attempts failed: %s", data_type, date, last_exc)
        return data_type, date, {"errors": 1}, set()

    work: list[tuple[str, str]] = []
    for data_type in sorted(target_types):
        rows_for_dt = captured_bare[captured_bare["data_type"] == data_type]
        dates = sorted(rows_for_dt["date"].unique().tolist())
        logger.info("=== %s: %d bare-captured dates queued ===", data_type, len(dates))
        for date in dates:
            work.append((data_type, str(date)))

    logger.info("Total work items: %d (parallel workers: 16)", len(work))
    completed = 0
    t_start = time.time()
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(_process_one, dt, dt_date): (dt, dt_date) for (dt, dt_date) in work}
        for fut in as_completed(futures):
            data_type, date, counters, leagues = fut.result()
            for k, v in counters.items():
                total_counters[k] += v
            if leagues:
                migrated_for_manifest.append((data_type, date, leagues, int(counters.get("unmapped_rows", 0))))
            completed += 1
            if completed % 200 == 0:
                rate = completed / max(1.0, time.time() - t_start)
                eta_min = (len(work) - completed) / max(0.01, rate) / 60
                logger.info(
                    "Progress: %d/%d (%.1f shards/sec, ETA %.1f min) totals=%s",
                    completed,
                    len(work),
                    rate,
                    eta_min,
                    dict(total_counters),
                )
    logger.info("=" * 60)
    logger.info("Migration totals: %s", dict(total_counters))
    logger.info("Shards queued for manifest update: %d", len(migrated_for_manifest))
    _update_manifest_after_migration(bucket, migrated_for_manifest, dry_run=dry)
    return 0


if __name__ == "__main__":
    sys.exit(main())
