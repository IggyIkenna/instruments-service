"""Backfill ``round`` into the historical sports fixture day-parquets (2019->2026).

# Epic: sports_fixture_round_not_captured_competition_phase_unknown_2026_07_17
# Lifecycle: one-off backfill — DELETE after the full 2019->2026 run is verified.
# Delete-when: catalogue competition_phase distribution stops being ~100% UNKNOWN.

WHY
---
``_flatten_canonical_fixture_for_disk`` defaulted ``round`` to "" on every row
(CanonicalFixture has no round field) — so the ML competition-phase classifier
saw UNKNOWN everywhere and ``is_promotion_relegation`` was a false-not-null. The
writer is now fixed (reads ``league.round`` from the raw af_response,
instruments-service@19ae5890), correcting NEW captures; this corrects HISTORY.
The raw was never persisted, so ``round`` must be RE-FETCHED. The fetch is BULK
per (league, season) (``GET /fixtures?league&season``, cached) — proven
2026-07-17: EPL 2024 = 380/380 fixtures carry round. So the corpus is ~600-700
calls, not per-fixture.

SURGICAL, not a re-capture
--------------------------
Updates ONLY the ``round`` column of each existing day-parquet, keyed by
``af_fixture_id``, and only where the cell is currently blank. Scores/status/
kickoff are left untouched (already correct; re-deriving risks drifting them
against later API edits). Fixtures the fresh fetch does not cover stay untouched.

SINGLE-WALK: ONE list of the fixtures corpus (grouped by league) — never a
per-(league,season) re-walk.

RATE LIMIT: the api-football key is minute-limited (the adapter's own LiveQuota
sleeps to the next minute). ~600-700 league-seasons => MULTI-HOUR. Run in the
background / on a job, not interactively. ``--leagues`` / ``--max-leagues`` /
``--seasons`` scope a pilot; ALWAYS pilot one league-season with ``--apply``
before the full run.

SAFETY: ``--dry-run`` (default) fetches + reports, writes nothing. ``--apply``
snapshots each parquet to ``<path>.pre_round_backfill.bak`` before overwriting,
then re-downloads to verify. Key is read from Secret Manager ``api-football-api-key``
(ADC).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_BUCKET_KIND = "instruments-store"
_FIXTURES_PREFIX = "sports_reference/by_date/"
_BACKUP_SUFFIX = ".pre_round_backfill.bak"
_SECRET_NAME = "api-football-api-key"


def _api_key() -> str:
    """Resolve the api-football key via UTL's ``get_secret`` (NOT a raw Secret
    Manager client — TID251) — the same secret the daily repoll / adapter use."""
    from unified_trading_library import get_secret  # noqa: qg-inside-import

    key = get_secret(_SECRET_NAME)
    if not key:
        raise RuntimeError(f"secret {_SECRET_NAME!r} did not resolve (need ADC + Secret Manager access)")
    return key.strip()


def _league_blob_index(bucket: str) -> dict[str, list[str]]:
    """ONE walk of the fixtures corpus → ``{canonical_league_id: [blob, ...]}``.

    league is the ``/league=<id>/`` path segment (it sits AFTER ``day=``, so it
    can't be a list_blobs prefix — one full scan, grouped in memory)."""
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    out: dict[str, list[str]] = {}
    for b in get_storage_client().list_blobs(bucket, prefix=_FIXTURES_PREFIX):
        name = str(getattr(b, "name", b))
        if "/entity=fixtures/" not in name or not name.endswith("fixtures.parquet"):
            continue
        marker = "/league="
        i = name.find(marker)
        if i == -1:
            continue
        cid = name[i + len(marker) : name.find("/", i + len(marker))]
        out.setdefault(cid, []).append(name)
    return out


async def _rounds_for_league(adapter: object, af_league_id: int, seasons: list[int]) -> dict[str, str]:
    """``{af_fixture_id: round}`` across all ``seasons`` for one league — one
    cached bulk fetch per season. Shard-isolated (a bad season is skipped)."""
    from instruments_service.engine.orchestrator import _round_from_af_response  # noqa: qg-inside-import

    out: dict[str, str] = {}
    for season in seasons:
        try:
            pairs = await adapter._fetch_season_fixtures_with_raw(af_league_id, season)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception as exc:
            logger.warning("fetch failed league_af=%s season=%s (%s) — skipping", af_league_id, season, exc)
            continue
        for _canonical, raw in pairs:
            if not isinstance(raw, dict):
                continue
            fid = raw.get("fixture", {}).get("id")
            rnd = _round_from_af_response(raw)
            if fid is not None and rnd:
                out[str(fid)] = rnd
    return out


def _apply_rounds_to_parquet(bucket: str, blob: str, rounds: dict[str, str], *, apply: bool) -> tuple[int, int]:
    """Fill blank ``round`` cells of one day-parquet from ``rounds``. Returns
    ``(rows_filled, rows_total)``. Snapshots the ORIGINAL before writing."""
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    sc = get_storage_client()
    raw = sc.download_bytes(bucket, blob)
    df = pd.read_parquet(io.BytesIO(raw))
    if df.empty or "af_fixture_id" not in df.columns or "round" not in df.columns:
        return 0, len(df)
    fresh = df["af_fixture_id"].astype(str).map(rounds)
    blank = df["round"].astype(str).str.strip().str.len() == 0
    mask = fresh.notna() & (fresh.astype(str).str.len() > 0) & blank
    n = int(mask.sum())
    if n and apply:
        df.loc[mask, "round"] = fresh[mask]
        sc.upload_bytes(bucket, blob + _BACKUP_SUFFIX, raw)  # snapshot ORIGINAL first
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        sc.upload_bytes(bucket, blob, buf.getvalue())
        # re-download verify: the freshly-written object carries the fills
        back = pd.read_parquet(io.BytesIO(sc.download_bytes(bucket, blob)))
        verified = int((back["round"].astype(str).str.strip().str.len() > 0).sum())
        logger.info("  wrote %s (+%d round; %d now populated)", blob.rsplit("/by_date/", 1)[-1], n, verified)
    return n, len(df)


async def main_async(args: argparse.Namespace) -> int:
    from unified_trading_library import MockEventSink, resolve_bucket_name, setup_events  # noqa: qg-inside-import

    from instruments_service.reference_data.adapters.sports import (
        create_sports_reference_adapter,  # noqa: qg-inside-import
    )

    setup_events(service_name="sports-round-backfill", mode="batch", sink=MockEventSink())
    bucket = resolve_bucket_name(cloud="gcp", kind=_BUCKET_KIND, asset_group="sports")
    adapter = create_sports_reference_adapter("api_football", api_key=_api_key())

    # league universe: canonical_id -> af_id, across all classifications
    from unified_api_contracts.sports import get_leagues_by_classification  # noqa: qg-inside-import

    universe: dict[str, int] = {}
    for cls in ("prediction", "reference", "features"):
        try:
            for ld in get_leagues_by_classification(cls):
                af = getattr(ld, "api_football_id", None)
                cid = getattr(ld, "league_id", None)
                if af is not None and cid:
                    universe[str(cid)] = int(af)
        except Exception as exc:
            logger.warning("classification %s failed: %s", cls, exc)

    if args.leagues:
        want = {x.strip().upper() for x in args.leagues.split(",")}
        universe = {cid: af for cid, af in universe.items() if cid.upper() in want}
    if args.max_leagues:
        universe = dict(sorted(universe.items())[: args.max_leagues])
    seasons = [int(s) for s in args.seasons.split(",")] if args.seasons else list(range(2019, 2027))

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("[%s] indexing fixtures corpus (single walk)...", mode)
    blob_index = _league_blob_index(bucket)
    logger.info(
        "[%s] %d leagues in scope x %d seasons; corpus has %d leagues with parquets",
        mode,
        len(universe),
        len(seasons),
        len(blob_index),
    )

    grand_fill = grand_rows = 0
    for cid, af in sorted(universe.items()):
        blobs = blob_index.get(cid, [])
        if not blobs:
            continue
        rounds = await _rounds_for_league(adapter, af, seasons)
        if not rounds:
            logger.info("%-26s no rounds fetched (out-of-coverage seasons?) — %d parquet(s) untouched", cid, len(blobs))
            continue
        lf = lr = 0
        for blob in blobs:
            n, tot = _apply_rounds_to_parquet(bucket, blob, rounds, apply=args.apply)
            lf += n
            lr += tot
        logger.info(
            "%-26s %s +%d/%d rows across %d parquet(s) (%d fixtures fetched)",
            cid,
            "APPLIED" if args.apply else "would-fill",
            lf,
            lr,
            len(blobs),
            len(rounds),
        )
        grand_fill += lf
        grand_rows += lr

    logger.info(
        "[%s] DONE — %d rows %s across %d scanned",
        mode,
        grand_fill,
        "filled" if args.apply else "would-fill",
        grand_rows,
    )
    if not args.apply:
        logger.info("Re-run with --apply to write (snapshots each parquet to *%s).", _BACKUP_SUFFIX)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Backfill sports fixture round (one-off).")
    p.add_argument("--apply", action="store_true", help="Write (default: dry-run). Snapshots each parquet first.")
    p.add_argument("--leagues", default=None, help="Comma-sep canonical league_ids to scope (pilot).")
    p.add_argument("--seasons", default=None, help="Comma-sep seasons. Default 2019..2026.")
    p.add_argument("--max-leagues", type=int, default=None, help="Cap league count (pilot).")
    return asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
