#!/usr/bin/env python3
"""Migrate local SFI bulk dumps into canonical ``progressive_stats`` parquets.

Two local files in ``~/Downloads`` carry SFI in-match data dumped from a
sibling system:

* ``sf_match_dominance.parquet``      — 10.3M rows: ``id, sf_match_id, timer, team_a_dominance, team_b_dominance``
* ``sf_match_progressive_odds.parquet`` — 10.3M rows: 1X2 / AH / OU / AC odds + first-half (h1_) variants per timer

The canonical sports manifest stores SFI in-match data at::

    gs://instruments-store-sports-{pid}/sports_reference/by_date/
        day={D}/entity=progressive_stats/league={L}/progressive_stats.parquet

Schema (one row per ``(fixture_id, timer_seconds)``):

* ``fixture_id`` — same hex format as ``sf_match_id`` (verified 2026-05-04: direct overlap with canonical fixtures)
* ``timer_seconds`` — integer seconds (we convert ``"MM:SS"`` -> ``MM*60 + SS``)
* ``odds_1x2_home / draw / away``, ``odds_ou_*``, ``odds_ah_*``, ``odds_asian_corner_*``
* ``dominance_index_home / away``, ``dominance_pct``, ``dominance_avg_*``
* ``league_id``, ``available_at``

Mapping (local -> canonical):

| Local                                | Canonical                |
| sf_match_id                          | fixture_id               |
| timer ("42:30")                      | timer_seconds (2550)     |
| odds_1, odds_x, odds_2               | odds_1x2_home/draw/away  |
| ah_home, ah_away, ah_line            | odds_ah_home/away/line   |
| ou_over, ou_under, ou_line           | odds_ou_over/under/line  |
| ac_over, ac_under, ac_line           | odds_asian_corner_*      |
| team_a_dominance, team_b_dominance   | dominance_index_home/away (team_a=home convention) |
| h1_*                                 | dropped (not in canonical schema; could be added later) |

Two-phase work:

1. **Build lookup** — scan every canonical ``progressive_stats.parquet`` in
   GCS to derive ``fixture_id -> (day, league_id)``.  ~5k-50k unique fixtures
   spread across ~3 years x ~50 leagues.

2. **Group + merge + upload** — for each ``(day, league_id)`` partition with
   matching local data, do an outer-merge with the existing canonical parquet
   on ``(fixture_id, timer_seconds)``: local fills NaN cells in canonical,
   never overwrites a non-NaN canonical value.  Idempotent CAS-write so the
   running ``sfi-backfill`` VM doesn't race us.

Usage::

    cd instruments-service
    .venv/bin/python scripts/migrate_local_sfi_to_canonical.py --dry-run
    .venv/bin/python scripts/migrate_local_sfi_to_canonical.py
    .venv/bin/python scripts/migrate_local_sfi_to_canonical.py --limit-partitions 10  # spot-check
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from requests.adapters import HTTPAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"instruments-store-sports-{PROJECT_ID}"
ROOT = "sports_reference/by_date/"
LOCAL_DIR = Path.home() / "Downloads"
DOMINANCE_FILE = LOCAL_DIR / "sf_match_dominance.parquet"
ODDS_FILE = LOCAL_DIR / "sf_match_progressive_odds.parquet"

# Column rename maps (local -> canonical).
ODDS_RENAME: dict[str, str] = {
    "odds_1": "odds_1x2_home",
    "odds_x": "odds_1x2_draw",
    "odds_2": "odds_1x2_away",
    "ah_home": "odds_ah_home",
    "ah_away": "odds_ah_away",
    "ah_line": "odds_ah_line",
    "ou_over": "odds_ou_over",
    "ou_under": "odds_ou_under",
    "ou_line": "odds_ou_line",
    "ac_over": "odds_asian_corner_over",
    "ac_under": "odds_asian_corner_under",
    "ac_line": "odds_asian_corner_line",
}
DOMINANCE_RENAME: dict[str, str] = {
    "team_a_dominance": "dominance_index_home",
    "team_b_dominance": "dominance_index_away",
}

# Drop columns from local that don't map to canonical schema (h1_* first-half
# variants, raw row id, timestamp metadata).
ODDS_DROP_COLS: tuple[str, ...] = (
    "id",
    "h1_ah_home",
    "h1_ah_away",
    "h1_ah_line",
    "h1_ou_over",
    "h1_ou_under",
    "h1_ou_line",
    "h1_ac_over",
    "h1_ac_under",
    "h1_ac_line",
    "h1_odds_1",
    "h1_odds_x",
    "h1_odds_2",
    "created_at",
    "updated_at",
)
DOMINANCE_DROP_COLS: tuple[str, ...] = ("id",)


def _make_storage_client() -> storage.Client:
    """Storage client with a bigger HTTP pool — matches manifest pattern."""
    client = storage.Client(project=PROJECT_ID)
    try:
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=3)
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
    except (AttributeError, TypeError):
        pass
    return client


def _timer_to_seconds(timer: str) -> int | None:
    """Convert SFI ``"MM:SS"`` (or ``"M:SS"``) -> integer seconds.

    Returns ``None`` for malformed values so the caller can drop them.
    """
    if not isinstance(timer, str) or ":" not in timer:
        return None
    try:
        m, s = timer.split(":", 1)
        return int(m) * 60 + int(s)
    except ValueError:
        return None


def build_fixture_lookup(client: storage.Client) -> pd.DataFrame:
    """Scan all canonical ``progressive_stats.parquet`` files; return a
    dataframe with columns ``[fixture_id, day, league_id]``.

    Uses a thread pool for parallel reads; each file is small (<1 MB
    typically) so this is I/O-bound but parallelisable.
    """
    bucket = client.bucket(BUCKET)
    logger.info("Listing canonical progressive_stats parquets…")
    blobs: list[str] = []
    for b in bucket.list_blobs(prefix=ROOT):
        if b.name.endswith("/progressive_stats.parquet"):
            blobs.append(b.name)
    logger.info("Found %d canonical progressive_stats parquets", len(blobs))

    rows: list[pd.DataFrame] = []

    def _read(name: str) -> pd.DataFrame:
        # Extract day + league from path before reading.
        parts = name.split("/")
        day = next((p[len("day=") :] for p in parts if p.startswith("day=")), "")
        league = next((p[len("league=") :] for p in parts if p.startswith("league=")), "")
        try:
            raw = bucket.blob(name).download_as_bytes()
            df = pd.read_parquet(io.BytesIO(raw), columns=["fixture_id"])
            return pd.DataFrame(
                {
                    "fixture_id": df["fixture_id"].dropna().astype(str).unique(),
                }
            ).assign(day=day, league_id=league)
        except Exception as exc:  # broad-except-ok: per-file isolation
            logger.warning("read failed for %s: %s", name, exc)
            return pd.DataFrame(columns=["fixture_id", "day", "league_id"])

    completed = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=32) as ex:
        for fut in as_completed([ex.submit(_read, n) for n in blobs]):
            rows.append(fut.result())
            completed += 1
            if completed % 1000 == 0:
                rate = completed / max(0.01, time.time() - t0)
                logger.info("  %d/%d (%.1f/sec)", completed, len(blobs), rate)
    out = (
        pd.concat(rows, ignore_index=True)
        if rows
        else pd.DataFrame(
            columns=["fixture_id", "day", "league_id"],
        )
    )
    out = out.drop_duplicates(subset=["fixture_id"], keep="first")
    logger.info(
        "Lookup built: %d unique fixture_ids across %d (day, league_id) partitions",
        len(out),
        out.groupby(["day", "league_id"]).ngroups,
    )
    return out


def _load_local_files() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load + map both local parquets to canonical column names."""
    if not DOMINANCE_FILE.exists():
        logger.error("missing %s", DOMINANCE_FILE)
        sys.exit(1)
    if not ODDS_FILE.exists():
        logger.error("missing %s", ODDS_FILE)
        sys.exit(1)

    logger.info("Loading %s", DOMINANCE_FILE)
    dom = pd.read_parquet(DOMINANCE_FILE)
    dom = dom.drop(columns=list(DOMINANCE_DROP_COLS), errors="ignore")
    dom = dom.rename(columns=DOMINANCE_RENAME)
    dom["timer_seconds"] = dom["timer"].map(_timer_to_seconds)
    dom = dom.drop(columns=["timer"]).dropna(subset=["timer_seconds", "sf_match_id"])
    dom["timer_seconds"] = dom["timer_seconds"].astype("int64")
    dom = dom.rename(columns={"sf_match_id": "fixture_id"})
    dom["fixture_id"] = dom["fixture_id"].astype(str)
    logger.info("  dominance rows after clean: %d", len(dom))

    logger.info("Loading %s", ODDS_FILE)
    odds = pd.read_parquet(ODDS_FILE)
    odds = odds.drop(columns=list(ODDS_DROP_COLS), errors="ignore")
    odds = odds.rename(columns=ODDS_RENAME)
    odds["timer_seconds"] = odds["timer"].map(_timer_to_seconds)
    odds = odds.drop(columns=["timer"]).dropna(subset=["timer_seconds", "sf_match_id"])
    odds["timer_seconds"] = odds["timer_seconds"].astype("int64")
    odds = odds.rename(columns={"sf_match_id": "fixture_id"})
    odds["fixture_id"] = odds["fixture_id"].astype(str)
    logger.info("  odds rows after clean: %d", len(odds))
    return dom, odds


def _merge_into_canonical(
    bucket: storage.Bucket,
    blob_path: str,
    new_rows: pd.DataFrame,
) -> tuple[bool, int, int]:
    """Read existing canonical parquet, outer-merge with ``new_rows``,
    write back with generation-match CAS.

    The merge prefers existing canonical values over local: local data only
    fills NaN cells in canonical, never overwrites a non-NaN canonical value.
    Returns ``(success, rows_before, rows_after)``.
    """
    blob = bucket.blob(blob_path)
    try:
        blob.reload()
        existing_raw = blob.download_as_bytes()
        existing = pd.read_parquet(io.BytesIO(existing_raw))
        generation = blob.generation
    except Exception:
        existing = pd.DataFrame()
        generation = 0  # if-generation-match=0 means "create only if absent"

    rows_before = len(existing)

    if existing.empty:
        merged = new_rows
    else:
        # Coerce merge keys to consistent dtypes — existing parquets sometimes
        # store ``timer_seconds`` as object/float (depending on which writer
        # produced them); ``fixture_id`` should be string everywhere.
        if "timer_seconds" in existing.columns:
            existing["timer_seconds"] = pd.to_numeric(existing["timer_seconds"], errors="coerce")
            existing = existing.dropna(subset=["timer_seconds"])
            existing["timer_seconds"] = existing["timer_seconds"].astype("int64")
        if "fixture_id" in existing.columns:
            existing["fixture_id"] = existing["fixture_id"].astype(str)
        joined = existing.merge(
            new_rows,
            on=["fixture_id", "timer_seconds"],
            how="outer",
            suffixes=("", "_local"),
        )
        for col in new_rows.columns:
            if col in ("fixture_id", "timer_seconds"):
                continue
            local_col = f"{col}_local"
            if local_col in joined.columns:
                if col in joined.columns:
                    joined[col] = joined[col].combine_first(joined[local_col])
                else:
                    joined[col] = joined[local_col]
                joined = joined.drop(columns=[local_col])
        merged = joined

    # Coerce all SFI-data columns to float64 so to_parquet doesn't choke on
    # mixed-dtype object columns (existing canonical sometimes stores odds as
    # bytes/string; local stores them as float — combine_first leaves the
    # result as ``object`` with both types, which pyarrow rejects).
    _id_cols = {"fixture_id", "timer_seconds", "league_id", "available_at"}
    for col in merged.columns:
        if col in _id_cols:
            continue
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    out = io.BytesIO()
    merged.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    try:
        blob.upload_from_string(
            out.getvalue(),
            content_type="application/octet-stream",
            if_generation_match=generation,
        )
        return True, rows_before, len(merged)
    except PreconditionFailed:
        return False, rows_before, len(merged)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--limit-partitions",
        type=int,
        default=0,
        help="Process only N (day, league_id) partitions (0 = all). For spot-check.",
    )
    p.add_argument("--workers", type=int, default=8, help="Parallel partition writes.")
    p.add_argument(
        "--lookup-cache",
        type=str,
        default=str(Path(tempfile.gettempdir()) / "sfi_fixture_lookup.parquet"),
        help="Local parquet cache for the canonical fixture lookup. Re-runs skip the 22-min scan.",
    )
    p.add_argument("--rebuild-lookup", action="store_true", help="Force rebuild of lookup cache.")
    args = p.parse_args()

    started = datetime.now(UTC)
    client = _make_storage_client()
    bucket = client.bucket(BUCKET)

    # Phase 1: load + clean local data.
    dom, odds = _load_local_files()
    local = pd.merge(odds, dom, on=["fixture_id", "timer_seconds"], how="outer")
    logger.info("Combined local rows (odds + dominance): %d", len(local))

    # Phase 2: build canonical fixture lookup (cache to disk so re-runs skip).
    cache_path = Path(args.lookup_cache)
    if cache_path.exists() and not args.rebuild_lookup:
        logger.info("Loading cached fixture lookup from %s", cache_path)
        lookup = pd.read_parquet(cache_path)
        logger.info("  cached lookup rows: %d", len(lookup))
    else:
        lookup = build_fixture_lookup(client)
        logger.info("Caching lookup to %s", cache_path)
        lookup.to_parquet(cache_path, index=False)

    # Phase 3: join lookup -> drop rows we can't map.
    local_with_meta = local.merge(lookup, on="fixture_id", how="inner")
    dropped = len(local) - len(local_with_meta)
    logger.info(
        "Local rows joined: %d kept, %d dropped (no canonical fixture match)",
        len(local_with_meta),
        dropped,
    )
    # Add available_at — set to MAX(updated_at) per fixture, fall back to day midnight.
    # Keep simple: stamp all rows with ``now`` as the migration time.
    now = datetime.now(UTC)
    local_with_meta["available_at"] = now

    # Phase 4: group + merge + upload.
    groups = list(local_with_meta.groupby(["day", "league_id"], sort=False))
    if args.limit_partitions:
        groups = groups[: args.limit_partitions]
    logger.info("Partitions to write: %d", len(groups))

    if args.dry_run:
        logger.info("--dry-run: showing first 5 partitions:")
        for (day, league), g in groups[:5]:
            logger.info(
                "  day=%s league=%s rows=%d unique_fixtures=%d cols=%s",
                day,
                league,
                len(g),
                g["fixture_id"].nunique(),
                sorted(c for c in g.columns if c not in ("day", "league_id")),
            )
        return 0

    # Each partition: drop the partition keys + write merge.
    success = 0
    cas_failures = 0

    def _process(item: tuple[tuple[str, str], pd.DataFrame]) -> tuple[str, str, bool]:
        (day, league), g = item
        path = f"{ROOT}day={day}/entity=progressive_stats/league={league}/progressive_stats.parquet"
        new_rows = g.drop(columns=["day", "league_id"])
        ok, _, _ = _merge_into_canonical(bucket, path, new_rows)
        return day, league, ok

    completed = 0
    write_t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_process, item) for item in groups]
        for fut in as_completed(futs):
            day, league, ok = fut.result()
            if ok:
                success += 1
            else:
                cas_failures += 1
                logger.warning("CAS conflict for day=%s league=%s -- skipped", day, league)
            completed += 1
            if completed <= 10 or completed % 50 == 0:
                rate = completed / max(0.01, time.time() - write_t0)
                logger.info(
                    "  %d/%d done (%d ok, %d cas_failed, %.1f/sec)",
                    completed,
                    len(groups),
                    success,
                    cas_failures,
                    rate,
                )

    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info(
        "Done in %.1fs. Success=%d cas_failures=%d total_partitions=%d",
        elapsed,
        success,
        cas_failures,
        len(groups),
    )
    if cas_failures:
        logger.warning(
            "Re-run the script — CAS conflicts indicate the sfi-backfill VM "
            "wrote to those partitions concurrently. Idempotent (combine_first "
            "preserves existing canonical values).",
        )
    return 0 if cas_failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
