# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after all 36 canonical target leagues verified holding the correct
#   fixtures schema for day=2026-04-14, and the 85 raw-named contaminated objects have
#   been human-deleted (out of this script's scope — prod-bucket delete is a
#   human-only hard stop per codex/02-data/gcs-and-manifest-delete-safety-protocol.md)
"""Recover the real `day=2026-04-14` fixtures content for the 36 CANONICAL league_ids
affected by the `entity=fixtures_schedule` cross-domain-contamination incident.

**Corrected target model (2026-07-25)**: the ORIGINAL `_AFFECTED_LEAGUES` set below used
to be the 85 literal `league=<X>` folder names pulled directly off the 85 bad shards
(e.g. `ENGLAND_CHAMPIONSHIP`). A live PROD dry-run found these raw strings are NOT
registered UAC `api_football` canonical league_ids under any tier — they are the
unabbreviated `build_league_id(country, league_name)` output a `CanonicalFixture` is
born with, misrouted into the fixtures_schedule path by the (now-guarded) cross-domain
write-path bug. **Main's ruling on `BLK-7e0a3faa`** explicitly REJECTED writing real
fixtures under those 85 non-canonical literal names — it would target a partition that
never should have existed, multiplying the exact duplicate-shard problem this recovery
is meant to fix. The issue doc's DIAG + gate-(b)/(c) work (slots 11/2) instead derived
the AUTHORITATIVE 36-item canonical target list this script now uses: 11 canonical
league folders with NO fixtures_schedule data at all for this date (`_MISSING_LEAGUES`),
and 25 canonical league folders that are THEMSELVES also contaminated with the wrong
instrument-catalogue schema (`_CONTAMINATED_CANONICAL_LEAGUES`) — both need a real
re-fetch + write under their correct canonical id. (14 other canonical folders already
hold correct fixtures data — excluded. 35 of the original 85 raw names have no canonical
UAC registry entry at all — structurally unrecoverable to any legitimate target, out of
scope pending a separate league-registration decision.) The 85 raw-named contaminated
objects themselves are NOT touched by this script — they are snapshot-then-delete
candidates once their canonical counterpart is confirmed good, and any prod-bucket
delete is a human-only hard stop (see the delete-safety-protocol doc above); that step
is deliberately left for the operator, not this script.

Context (see
``unified-trading-pm/plans/active/issues/sports_fixtures_schedule_wrong_schema_day_2026_04_14.md``):
the write-path bug is fully understood and structurally closed (`_assert_not_cross_domain_
contamination`, `instruments-service@b3cb6f8c`), so re-running the normal fixtures write
path for this one date is safe going forward.

This script: (1) snapshots each of the 25 CONTAMINATED canonical shards before
overwriting them (the 11 MISSING ones have nothing to snapshot), (2) fetches
`api_football`'s real fixtures for 2026-04-14 ONCE (a single day-level call, not
per-league — mirrors `_ensure_canonical_fixtures_for_override`'s pattern), (3) filters
the result to exactly the 36 target canonical league_ids, (4) writes via the real
`_write_fixtures_per_league()` — now guarded — so `entity=fixtures_schedule` +
`entity=fixtures_outcomes` land correctly under each league's CANONICAL id, (5) verifies
every one of the 36 target shards now parses with the fixtures schema, reporting any
league the real fetch didn't cover as an explicit, named gap (never silently left
broken).

Usage:
  python scripts/recover_fixtures_schedule_wrong_schema_day_2026_04_14.py --dry-run
  python scripts/recover_fixtures_schedule_wrong_schema_day_2026_04_14.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import ApiKeyReloader, get_storage_client, resolve_bucket_name

from instruments_service.engine.orchestrator import (
    _canonical_league_id,
    _flatten_canonical_fixture_for_disk,
    _sports_ref_sink_for,
    _write_fixtures_per_league,
)
from instruments_service.reference_data.adapters.sports.factory import create_sports_reference_adapter

logger = logging.getLogger(__name__)

_DATE = "2026-04-14"
_PROJECT_ID = "central-element-323112"
_PREFIX = f"sports_reference/by_date/day={_DATE}/pipeline_mode=batch_api_football/entity=fixtures_schedule/"

# 11 canonical league_ids with NO fixtures_schedule data at all for day=2026-04-14
# (gate (c), slot 2, 2026-07-25 — bounded single-day-prefix GCS listing, not a corpus walk).
_MISSING_LEAGUES: frozenset[str] = frozenset(
    {
        "AFC_CHAMPIONS_LEAGUE_ELITE",
        "BOLIVIA_PRIMERA",
        "CYPRUS_FIRST_DIVISION",
        "ECUADOR_CUP",
        "IRAQ_LEAGUE",
        "KENYA_PREMIER_LEAGUE",
        "LIBERIA_FIRST_DIVISION",
        "PANAMA_LPF",
        "PERU_PRIMERA",
        "SLOVENIA_PRVALIGA",
        "TANZANIA_LIGI_KUU",
    }
)

# 25 canonical league_ids whose OWN canonical-folder fixtures_schedule.parquet is
# ITSELF also contaminated with the wrong instrument-catalogue schema for
# day=2026-04-14 (gate (c), slot 2, 2026-07-25 — schema-checked by downloading each).
_CONTAMINATED_CANONICAL_LEAGUES: frozenset[str] = frozenset(
    {
        "ARMENIA_FIRST_LEAGUE",
        "ARMENIA_PREMIER_LEAGUE",
        "ARUBA_DIVISION_DI_HONOR",
        "BANGLADESH_FEDERATION_CUP",
        "BARBADOS_PREMIER_LEAGUE",
        "BULGARIA_FIRST_LEAGUE",
        "COLOMBIA_PRIMERA_B",
        "EGYPT_PREMIER_LEAGUE",
        "ETHIOPIA_PREMIER_LEAGUE",
        "FINLAND_SUOMEN_CUP",
        "HONDURAS_LIGA_NACIONAL",
        "HUNGARY_NB_I",
        "ISRAEL_LIGA_LEUMIT",
        "JORDAN_LEAGUE",
        "KENYA_SUPER_LEAGUE",
        "LATVIA_VIRSLIGA",
        "LIECHTENSTEIN_CUP",
        "MACEDONIA_FIRST_LEAGUE",
        "MALTA_PREMIER_LEAGUE",
        "NIGERIA_NPFL",
        "ROMANIA_LIGA_II",
        "SAUDI_ARABIA_DIVISION_1",
        "SAUDI_ARABIA_PRO_LEAGUE",
        "SLOVAKIA_CUP",
        "UZBEKISTAN_SUPER_LEAGUE",
    }
)

# The full 36-league write target — union of the two disjoint sets above.
_AFFECTED_LEAGUES: frozenset[str] = _MISSING_LEAGUES | _CONTAMINATED_CANONICAL_LEAGUES


def _bad_shard_paths(bucket: str) -> dict[str, str]:
    """Return ``{canonical_league_id: blob_path}`` for the 25 CONTAMINATED canonical
    shards currently present under ``_PREFIX`` (the 11 MISSING leagues have nothing to
    snapshot — there is no existing object at their canonical path)."""
    storage = get_storage_client(project_id=_PROJECT_ID)
    out: dict[str, str] = {}
    for blob in storage.list_blobs(bucket, prefix=_PREFIX):
        if "league=" not in blob.name or not blob.name.endswith("fixtures_schedule.parquet"):
            continue
        league = blob.name.split("league=")[1].split("/")[0]
        if league in _CONTAMINATED_CANONICAL_LEAGUES:
            out[league] = blob.name
    return out


def _snapshot(bucket: str, path: str, run_ts: str) -> None:
    storage = get_storage_client(project_id=_PROJECT_ID)
    raw = storage.download_bytes(bucket, path)
    backup_path = path.replace(
        "fixtures_schedule.parquet",
        f"fixtures_schedule.{run_ts}.wrong_schema.bak.parquet",
    )
    storage.upload_bytes(bucket, backup_path, raw)
    logger.info("Snapshotted gs://%s/%s -> gs://%s/%s", bucket, path, bucket, backup_path)


async def _fetch_and_filter() -> pd.DataFrame:
    key_reloader = ApiKeyReloader(venues=["api_football"], project_id=_PROJECT_ID)
    key_reloader.start()
    try:
        api_key = key_reloader.current_keys.get("api_football")
        if not api_key:
            raise RuntimeError("No api_football API key available from Secret Manager — aborting.")
        adapter = create_sports_reference_adapter("api_football", api_key=api_key)
        logger.info("Fetching api_football fixtures for %s (single day-level call)...", _DATE)
        fx_pairs = await adapter.get_fixtures_with_raw(_DATE)
        logger.info("Fetched %d raw fixtures for %s", len(fx_pairs), _DATE)
    finally:
        key_reloader.stop()

    fx_dicts = [_flatten_canonical_fixture_for_disk(fx, _DATE, af_response=raw) for fx, raw in fx_pairs]
    fx_df = pd.DataFrame(fx_dicts)
    if "timestamp" in fx_df.columns:
        fx_df["available_at"] = pd.to_datetime(fx_df["timestamp"], utc=True, errors="coerce") - pd.Timedelta(days=7)

    fx_df["_canonical_league_id"] = fx_df["af_league_id"].apply(
        lambda v: _canonical_league_id(v) if pd.notna(v) else None
    )
    filtered = fx_df[fx_df["_canonical_league_id"].isin(_AFFECTED_LEAGUES)].drop(columns=["_canonical_league_id"])
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Fetch + report only, no snapshot/write.")
    mode.add_argument(
        "--apply", action="store_true", help="Snapshot the 25 contaminated canonical shards then write real fixtures."
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
    bad_shards = _bad_shard_paths(bucket)
    logger.info(
        "Contaminated canonical shards currently present: %d of %d expected (_CONTAMINATED_CANONICAL_LEAGUES); "
        "%d leagues have no existing shard at all (_MISSING_LEAGUES, nothing to snapshot)",
        len(bad_shards),
        len(_CONTAMINATED_CANONICAL_LEAGUES),
        len(_MISSING_LEAGUES),
    )
    unexpected_absent = _CONTAMINATED_CANONICAL_LEAGUES - set(bad_shards)
    if unexpected_absent:
        logger.warning(
            "Expected-but-absent contaminated shard for %d league(s) (re-verify gate (c) before proceeding): %s",
            len(unexpected_absent),
            sorted(unexpected_absent),
        )

    filtered_df = asyncio.run(_fetch_and_filter())
    fetched_leagues = set(filtered_df["af_league_id"].apply(_canonical_league_id)) if not filtered_df.empty else set()
    not_covered = _AFFECTED_LEAGUES - fetched_leagues
    logger.info(
        "Real fetch covers %d of %d affected leagues (%d fixture rows total)",
        len(fetched_leagues),
        len(_AFFECTED_LEAGUES),
        len(filtered_df),
    )
    if not_covered:
        logger.warning(
            "NOT COVERED by today's real fetch (no fixtures returned for %s on this date) — will remain "
            "un-recovered this pass, needs its own follow-up: %s",
            _DATE,
            sorted(not_covered),
        )
    if not filtered_df.empty:
        counts = filtered_df["af_league_id"].apply(_canonical_league_id).value_counts()
        for league, n in sorted(counts.items()):
            logger.info("  %s: %d fixtures", league, n)

    if args.dry_run:
        logger.info(
            "[dry-run] Would snapshot %d bad shards then write %d fixture rows.", len(bad_shards), len(filtered_df)
        )
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    for path in bad_shards.values():
        _snapshot(bucket, path, run_ts)

    if filtered_df.empty:
        logger.error("No fixtures fetched for the affected leagues — nothing to write. Investigate before re-running.")
        return 2

    sink = _sports_ref_sink_for(bucket, _DATE, "fixtures_schedule")
    _write_fixtures_per_league(
        sink,
        filtered_df,
        _DATE,
        source_label="wrong-schema-recovery-2026-07-25",
        bucket=bucket,
    )
    logger.info(
        "Write complete. Verifying all %d target shards now parse with the fixtures schema...", len(_AFFECTED_LEAGUES)
    )

    storage = get_storage_client(project_id=_PROJECT_ID)
    still_bad: list[str] = []
    for league in sorted(_AFFECTED_LEAGUES):
        path = f"{_PREFIX}league={league}/fixtures_schedule.parquet"
        try:
            raw = storage.download_bytes(bucket, path)
            pd.read_parquet(io.BytesIO(raw), columns=["af_league_id"])
        except Exception as exc:
            still_bad.append(league)
            logger.error("STILL BAD after recovery: %s (%s)", league, exc)

    if still_bad:
        logger.error(
            "%d of %d leagues remain unrecovered (either no fixtures today or a residual write issue): %s",
            len(still_bad),
            len(_AFFECTED_LEAGUES),
            still_bad,
        )
        return 3

    logger.info(
        "All %d affected leagues now read with the correct fixtures schema for %s.", len(_AFFECTED_LEAGUES), _DATE
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
