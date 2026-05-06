"""Phase 1 — Smart audit: api_football per-(league, season) truth-set.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.plan.md``.

Why this exists
---------------
The prior phantom-recovery iterations trusted URDI/disk and reconciled manifest
rows. They could not detect:

  1. **League-mapping breakage**: leagues with ``data_sources`` containing
     ``api_football`` but ``api_football_id is None`` were never fetched, and
     the orchestrator records that absence as ``empty_confirmed`` indistinguishable
     from a genuinely-empty day.
  2. **Phantom-write on fixture-day**: the pre-``f36651c`` writer bug emitted
     ``manifest.add(row_count=0)`` for some (date, league) pairs even when
     api_football had real fixtures.

Both are only detectable by querying api_football directly.

What this script does
---------------------
For every league with ``"api_football" in data_sources`` and a non-None
``api_football_id``, call ``GET /fixtures?league={af_id}&season={year}`` for
each season in the range. Persist the truth-set to GCS, diff against the
canonical manifest, and emit a recovery-set parquet that Phase 2 consumes
without further api_football calls.

API budget
----------
~119 affected leagues x 9 seasons (2018-2026 inclusive) ≈ **1,071 calls**.
Pro-tier daily quota = 7,500 → ~3-4h with the existing adapter throttle.

Output
------
* Truth-set parquet:        ``gs://{bucket}/_audits/fixtures_truthset_{run_ts}.parquet``
* Diff CSV:                 ``gs://{bucket}/_audits/fixtures_diff_{run_ts}.csv``
* Recovery-set parquet:     ``gs://{bucket}/_audits/fixtures_recovery_set_{run_ts}.parquet``
* Mapping-breakage CSV:     ``gs://{bucket}/_audits/fixtures_mapping_breakage_{run_ts}.csv``
* Local copies of all four under ``${TMPDIR}/fixtures_truthset_audit/`` for inspection.

Resume / idempotency
--------------------
The truth-set parquet is loaded incrementally — if the script is re-run with
``--resume {run_ts}``, leagues already present in the prior partial parquet
are skipped. Useful when a run is interrupted mid-quota (3h is enough time
for transient network blips).

Usage
-----
::

  # Smoke test on a single league x single season (no GCS writes):
  python scripts/audit_fixtures_via_api_football.py --smoke EPL --smoke-season 2024

  # Full dry-run (reads manifest + LEAGUE_REGISTRY, no api calls):
  python scripts/audit_fixtures_via_api_football.py --dry-run

  # Apply (real api_football calls + GCS writes):
  python scripts/audit_fixtures_via_api_football.py --apply

  # Resume an interrupted run:
  python scripts/audit_fixtures_via_api_football.py --apply --resume 20260506-150000
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import LEAGUE_REGISTRY
from unified_trading_library import get_storage_client
from unified_trading_library.startup_validation import validate_api_keys_for_venues

from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
)

logger = logging.getLogger(__name__)

_DEFAULT_PROJECT_ID = "central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"
_AUDIT_PREFIX = "_audits"
_DEFAULT_SEASON_RANGE: tuple[int, int] = (2018, 2026)  # inclusive on both ends
_TRUTH_SET_DATA_TYPE = "FIXTURES"

# Status values from api_football response for fixtures we count as "real".
# All others (CANC=cancelled, ABD=abandoned, AWD=tech-walkover, WO=walkover,
# PST=postponed, TBD=date not set, NS=not started) are real planned fixtures
# too — the only "not a fixture" status would be a hypothetical preview entry,
# which api_football never returns. So we count every row.


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _audit_uri(bucket: str, name: str, run_ts: str, suffix: str) -> str:
    return f"gs://{bucket}/{_AUDIT_PREFIX}/{name}_{run_ts}.{suffix}"


def _audit_blob(name: str, run_ts: str, suffix: str) -> str:
    return f"{_AUDIT_PREFIX}/{name}_{run_ts}.{suffix}"


def _local_audit_dir() -> Path:
    base = Path(tempfile.gettempdir()) / "fixtures_truthset_audit"
    base.mkdir(parents=True, exist_ok=True)
    return base


# ---------------------------------------------------------------------------
# League selection — leagues with api_football as a declared data source
# ---------------------------------------------------------------------------


def _select_audit_leagues() -> tuple[list[tuple[str, int]], list[str]]:
    """Return (leagues_with_mapping, leagues_without_mapping).

    leagues_with_mapping is a list of (canonical_league_id, api_football_id).
    leagues_without_mapping is a list of canonical_league_ids whose
    ``data_sources`` mention ``api_football`` but whose ``api_football_id``
    is None — these are mapping breakage cases reported separately.
    """
    with_map: list[tuple[str, int]] = []
    without_map: list[str] = []
    for league_id, league_def in LEAGUE_REGISTRY.items():
        if "api_football" not in league_def.data_sources:
            continue
        if league_def.api_football_id is None:
            without_map.append(league_id)
        else:
            with_map.append((league_id, int(league_def.api_football_id)))
    with_map.sort()
    without_map.sort()
    return with_map, without_map


# ---------------------------------------------------------------------------
# Fixture fetching
# ---------------------------------------------------------------------------


async def _fetch_league_season_fixtures(
    adapter: ApiFootballAdapter,
    af_league_id: int,
    season: int,
) -> list[dict[str, object]]:
    """Call ``/fixtures?league={af_id}&season={year}`` and return the raw response list.

    Returns the api_football ``response`` array as parsed JSON dicts. Empty list
    when api_football has no fixtures for that (league, season) pair (which
    happens for leagues that didn't run in that calendar window).
    """
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"league": str(af_league_id), "season": str(season)}
    async with adapter._make_session() as session:
        raw = await adapter._get_with_retry(session, url, params=params, headers=adapter._headers())
    if isinstance(raw, dict):
        response = raw.get("response")
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
    return []


def _truth_rows_from_response(
    canonical_league_id: str,
    af_league_id: int,
    season: int,
    response: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Flatten an api_football ``/fixtures?league&season`` response to truth-set rows."""
    rows: list[dict[str, object]] = []
    for item in response:
        fixture_obj = item.get("fixture") if isinstance(item, dict) else None
        if not isinstance(fixture_obj, dict):
            continue
        date_str = fixture_obj.get("date")
        if not isinstance(date_str, str) or len(date_str) < 10:
            continue
        day = date_str[:10]  # YYYY-MM-DD
        af_fixture_id = fixture_obj.get("id")
        status_obj = fixture_obj.get("status")
        status_short = ""
        if isinstance(status_obj, dict):
            ss = status_obj.get("short")
            status_short = str(ss) if ss is not None else ""

        # Optional: home/away score for diagnostics
        goals_obj = item.get("goals") if isinstance(item, dict) else None
        home_score: int | None = None
        away_score: int | None = None
        if isinstance(goals_obj, dict):
            h = goals_obj.get("home")
            a = goals_obj.get("away")
            home_score = int(h) if isinstance(h, int) else None
            away_score = int(a) if isinstance(a, int) else None

        rows.append(
            {
                "canonical_league_id": canonical_league_id,
                "af_league_id": af_league_id,
                "season": season,
                "date": day,
                "af_fixture_id": int(af_fixture_id) if isinstance(af_fixture_id, int) else None,
                "status_short": status_short,
                "home_score": home_score,
                "away_score": away_score,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Manifest diff
# ---------------------------------------------------------------------------


def _classify_diff(
    truth_df: pd.DataFrame,
    manifest_df: pd.DataFrame,
) -> pd.DataFrame:
    """Cross-join truth-set with manifest FIXTURES rows; classify per (date, league)."""
    # Truth: presence per (date, canonical_league_id) with a fixtures count.
    truth_grouped = (
        truth_df.groupby(["date", "canonical_league_id"], as_index=False)
        .size()
        .rename(columns={"size": "fixtures_in_truth"})
    )

    # Manifest: filter to FIXTURES rows; index by (date, league_id).
    fixtures_mask = manifest_df["data_type"].astype(str) == _TRUTH_SET_DATA_TYPE
    fix_df = manifest_df.loc[fixtures_mask].copy()
    fix_df["date"] = fix_df["date"].astype(str)
    fix_df["league_id"] = fix_df["league_id"].astype(str)

    # Coalesce instrument_count → int (NaN → -1 sentinel for missing).
    if "instrument_count" in fix_df.columns:
        fix_df["instrument_count"] = pd.to_numeric(fix_df["instrument_count"], errors="coerce").fillna(-1).astype(int)
    else:
        fix_df["instrument_count"] = -1

    # Outer-merge on (date, league)
    merged = truth_grouped.merge(
        fix_df[["date", "league_id", "capture_status", "instrument_count", "error_reason"]],
        how="outer",
        left_on=["date", "canonical_league_id"],
        right_on=["date", "league_id"],
    )
    merged["canonical_league_id"] = merged["canonical_league_id"].fillna(merged["league_id"])
    merged.drop(columns=["league_id"], inplace=True)
    merged["fixtures_in_truth"] = merged["fixtures_in_truth"].fillna(0).astype(int)
    merged["instrument_count"] = merged["instrument_count"].fillna(-1).astype(int)
    merged["capture_status"] = merged["capture_status"].fillna("__missing__").astype(str)
    merged["error_reason"] = merged["error_reason"].fillna("").astype(str)

    def _classify(row: pd.Series) -> str:
        n_truth = int(row["fixtures_in_truth"])
        cs = str(row["capture_status"])
        ic = int(row["instrument_count"])
        if n_truth > 0:
            # Truth says fixtures exist
            if cs == "captured" and ic > 0:
                return "CORRECT"
            if cs == "empty_confirmed":
                return "SILENT_DROP"
            if cs == "attempted_failed":
                return "RETRY"
            if cs == "captured" and ic == 0:
                return "PHANTOM_ZERO"
            if cs == "__missing__":
                return "MISSING"
            return f"UNEXPECTED({cs})"
        # Truth absent
        if cs == "__missing__":
            return "ABSENT_AND_MISSING"
        if cs == "empty_confirmed":
            return "CORRECT_EMPTY"
        if cs == "captured" and ic == 0:
            return "ZERO_BOTH"
        if cs == "captured" and ic > 0:
            return "STRANGE_CAPTURED_BUT_TRUTH_ABSENT"
        if cs == "attempted_failed":
            return "ATTEMPTED_FAILED_NO_TRUTH"
        return f"UNEXPECTED({cs})"

    merged["classification"] = merged.apply(_classify, axis=1)
    return merged


def _recovery_set_from_classified(classified: pd.DataFrame) -> pd.DataFrame:
    """Filter classified diff to rows that need Phase 2 recovery."""
    target = {"SILENT_DROP", "RETRY", "MISSING"}
    return classified.loc[classified["classification"].isin(target)].copy()


# ---------------------------------------------------------------------------
# Resume / persistence
# ---------------------------------------------------------------------------


def _load_partial_truth_set(
    storage_client: object,
    bucket: str,
    run_ts: str,
) -> pd.DataFrame:
    """Load the partial truth-set parquet from GCS for a resume run."""
    blob_name = _audit_blob("fixtures_truthset", run_ts, "parquet")
    raw = storage_client.download_bytes(bucket, blob_name)  # type: ignore[attr-defined]
    return pd.read_parquet(io.BytesIO(raw))


def _checkpoint_truth_set(
    storage_client: object,
    bucket: str,
    run_ts: str,
    truth_df: pd.DataFrame,
) -> None:
    """Upload the in-progress truth-set parquet so the run is resumable."""
    if truth_df.empty:
        return
    buf = io.BytesIO()
    truth_df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    storage_client.upload_bytes(  # type: ignore[attr-defined]
        bucket, _audit_blob("fixtures_truthset", run_ts, "parquet"), buf.read()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace) -> int:
    project_id = args.project_id
    bucket = _bucket_for_project(project_id)
    run_ts = args.resume or datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    season_start, season_end = args.season_start, args.season_end
    seasons = list(range(season_start, season_end + 1))

    # ------------------------------------------------------------------
    # League selection (always — used by smoke + dry-run + apply)
    # ------------------------------------------------------------------
    leagues, mapping_breakage = _select_audit_leagues()
    logger.info(
        "League selection: %d with api_football_id, %d with broken mapping",
        len(leagues),
        len(mapping_breakage),
    )
    if mapping_breakage:
        logger.warning(
            "Mapping breakage candidates (data_sources mentions api_football but api_football_id is None): %s",
            ", ".join(mapping_breakage),
        )

    # ------------------------------------------------------------------
    # Smoke test path — single league x single season, no GCS, no manifest read
    # ------------------------------------------------------------------
    if args.smoke:
        target = args.smoke.upper()
        league_def = LEAGUE_REGISTRY.get(target)
        if league_def is None:
            logger.error("Smoke league %r not in LEAGUE_REGISTRY", target)
            return 2
        if league_def.api_football_id is None:
            logger.error("Smoke league %r has no api_football_id mapping", target)
            return 2
        keys = validate_api_keys_for_venues(venues=["api_football"], project_id=project_id)
        api_key = keys.get("api_football")
        if not api_key:
            logger.error(
                "Could not fetch api_football api key from Secret Manager (got: %s)",
                sorted(keys.keys()),
            )
            return 2
        adapter = ApiFootballAdapter(api_key=api_key)
        season = args.smoke_season
        logger.info(
            "[smoke] %s (af_id=%d) season=%d",
            target,
            league_def.api_football_id,
            season,
        )
        response = await _fetch_league_season_fixtures(adapter, int(league_def.api_football_id), season)
        rows = _truth_rows_from_response(target, int(league_def.api_football_id), season, response)
        logger.info(
            "[smoke] api_football returned %d fixture entries → %d truth-set rows",
            len(response),
            len(rows),
        )
        for row in rows[:5]:
            logger.info("[smoke]   %s", row)
        return 0

    # ------------------------------------------------------------------
    # Read manifest (dry-run + apply both need it for diff)
    # ------------------------------------------------------------------
    storage = get_storage_client(project_id=project_id)
    logger.info("Reading manifest gs://%s/%s", bucket, _INDEX_PATH)
    manifest_bytes = storage.download_bytes(bucket, _INDEX_PATH)
    manifest_df = pd.read_parquet(io.BytesIO(manifest_bytes))
    logger.info(
        "Manifest rows: %d  (FIXTURES rows: %d)",
        len(manifest_df),
        int((manifest_df["data_type"].astype(str) == _TRUTH_SET_DATA_TYPE).sum()),
    )

    # ------------------------------------------------------------------
    # Dry-run: skip api calls, just print the work plan
    # ------------------------------------------------------------------
    if args.dry_run:
        n_calls = len(leagues) * len(seasons)
        logger.info(
            "[dry-run] Would call api_football for %d (league, season) pairs (%d leagues x %d seasons)",
            n_calls,
            len(leagues),
            len(seasons),
        )
        logger.info("[dry-run] Pro-tier @ 7500/day → %.1fh total", n_calls / (7500 / 24))
        logger.info("[dry-run] Output paths (would be written under run_ts=%s):", run_ts)
        logger.info("[dry-run]   %s", _audit_uri(bucket, "fixtures_truthset", run_ts, "parquet"))
        logger.info("[dry-run]   %s", _audit_uri(bucket, "fixtures_diff", run_ts, "csv"))
        logger.info("[dry-run]   %s", _audit_uri(bucket, "fixtures_recovery_set", run_ts, "parquet"))
        logger.info("[dry-run]   %s", _audit_uri(bucket, "fixtures_mapping_breakage", run_ts, "csv"))
        return 0

    # ------------------------------------------------------------------
    # Apply: full audit run
    # ------------------------------------------------------------------
    keys = validate_api_keys_for_venues(venues=["api_football"], project_id=project_id)
    api_key = keys.get("api_football")
    if not api_key:
        logger.error(
            "Could not fetch api_football api key from Secret Manager (got: %s)",
            sorted(keys.keys()),
        )
        return 2
    adapter = ApiFootballAdapter(api_key=api_key)

    # Resume support: load partial truth-set and skip already-fetched (league, season)
    accumulated: list[dict[str, object]] = []
    fetched_pairs: set[tuple[str, int]] = set()
    if args.resume:
        try:
            partial = _load_partial_truth_set(storage, bucket, run_ts)
            for _idx, row in partial.iterrows():
                accumulated.append(dict(row))
            fetched_pairs = {(str(row["canonical_league_id"]), int(row["season"])) for _idx, row in partial.iterrows()}
            logger.info(
                "[resume] loaded %d truth-set rows covering %d (league, season) pairs",
                len(accumulated),
                len(fetched_pairs),
            )
        except Exception as exc:
            logger.warning("[resume] could not load partial truth-set: %s — starting fresh", exc)

    total = len(leagues) * len(seasons)
    done = 0
    last_checkpoint = datetime.now(UTC)
    checkpoint_interval = 60.0  # seconds

    for canonical_league_id, af_league_id in leagues:
        for season in seasons:
            done += 1
            if (canonical_league_id, season) in fetched_pairs:
                logger.info(
                    "[%d/%d] %-30s season=%d  SKIP (already fetched)",
                    done,
                    total,
                    canonical_league_id,
                    season,
                )
                continue
            try:
                response = await _fetch_league_season_fixtures(adapter, af_league_id, season)
            except Exception as exc:
                logger.warning(
                    "[%d/%d] %-30s season=%d  FAIL %s",
                    done,
                    total,
                    canonical_league_id,
                    season,
                    exc,
                )
                continue
            rows = _truth_rows_from_response(canonical_league_id, af_league_id, season, response)
            accumulated.extend(rows)
            fetched_pairs.add((canonical_league_id, season))
            logger.info(
                "[%d/%d] %-30s season=%d  fixtures=%d",
                done,
                total,
                canonical_league_id,
                season,
                len(rows),
            )
            # Periodic checkpoint to GCS so resume is cheap
            now = datetime.now(UTC)
            if (now - last_checkpoint).total_seconds() >= checkpoint_interval:
                truth_df_partial = pd.DataFrame(accumulated) if accumulated else pd.DataFrame()
                _checkpoint_truth_set(storage, bucket, run_ts, truth_df_partial)
                last_checkpoint = now
                logger.info(
                    "checkpoint: truth-set has %d rows / %d (league, season) pairs",
                    len(accumulated),
                    len(fetched_pairs),
                )

    # ------------------------------------------------------------------
    # Final write — truth-set, diff CSV, recovery-set, mapping-breakage
    # ------------------------------------------------------------------
    truth_df = pd.DataFrame(accumulated)
    if truth_df.empty:
        logger.warning("No truth-set rows collected — nothing to diff. Exiting.")
        return 1
    logger.info("Truth-set: %d rows / %d (league, season) pairs", len(truth_df), len(fetched_pairs))

    # Final truth-set checkpoint
    _checkpoint_truth_set(storage, bucket, run_ts, truth_df)

    # Diff against manifest
    classified = _classify_diff(truth_df, manifest_df)
    by_class = classified.groupby("classification").size().sort_values(ascending=False)
    logger.info("Classification breakdown:")
    for cls, n in by_class.items():
        logger.info("  %-40s %d", cls, n)

    # Recovery-set
    recovery_df = _recovery_set_from_classified(classified)
    logger.info("Recovery-set: %d rows", len(recovery_df))

    # Write diff CSV
    diff_buf = io.StringIO()
    classified.to_csv(diff_buf, index=False, quoting=csv.QUOTE_MINIMAL)
    storage.upload_bytes(bucket, _audit_blob("fixtures_diff", run_ts, "csv"), diff_buf.getvalue().encode())

    # Local copy
    local_dir = _local_audit_dir()
    classified.to_csv(local_dir / f"fixtures_diff_{run_ts}.csv", index=False)
    truth_df.to_parquet(local_dir / f"fixtures_truthset_{run_ts}.parquet", index=False, engine="pyarrow")

    # Recovery-set parquet
    if not recovery_df.empty:
        rec_buf = io.BytesIO()
        recovery_df.to_parquet(rec_buf, index=False, engine="pyarrow")
        rec_buf.seek(0)
        storage.upload_bytes(bucket, _audit_blob("fixtures_recovery_set", run_ts, "parquet"), rec_buf.read())
        recovery_df.to_parquet(local_dir / f"fixtures_recovery_set_{run_ts}.parquet", index=False, engine="pyarrow")

    # Mapping-breakage CSV
    if mapping_breakage:
        breakage_buf = io.StringIO()
        breakage_writer = csv.writer(breakage_buf, quoting=csv.QUOTE_MINIMAL)
        breakage_writer.writerow(["canonical_league_id", "issue", "data_sources"])
        for lid in mapping_breakage:
            league = LEAGUE_REGISTRY[lid]
            breakage_writer.writerow(
                [
                    lid,
                    "data_sources includes api_football but api_football_id is None",
                    ",".join(sorted(league.data_sources)),
                ]
            )
        storage.upload_bytes(
            bucket,
            _audit_blob("fixtures_mapping_breakage", run_ts, "csv"),
            breakage_buf.getvalue().encode(),
        )
        (local_dir / f"fixtures_mapping_breakage_{run_ts}.csv").write_text(breakage_buf.getvalue())

    logger.info("Wrote: %s", _audit_uri(bucket, "fixtures_truthset", run_ts, "parquet"))
    logger.info("Wrote: %s", _audit_uri(bucket, "fixtures_diff", run_ts, "csv"))
    if not recovery_df.empty:
        logger.info("Wrote: %s", _audit_uri(bucket, "fixtures_recovery_set", run_ts, "parquet"))
    if mapping_breakage:
        logger.info("Wrote: %s", _audit_uri(bucket, "fixtures_mapping_breakage", run_ts, "csv"))
    logger.info("Local copies in: %s", local_dir)
    logger.info("run_ts=%s — pass to --resume to re-run idempotently.", run_ts)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default=_DEFAULT_PROJECT_ID)
    parser.add_argument(
        "--season-start",
        type=int,
        default=_DEFAULT_SEASON_RANGE[0],
        help="First season year to query (default: 2018).",
    )
    parser.add_argument(
        "--season-end",
        type=int,
        default=_DEFAULT_SEASON_RANGE[1],
        help="Last season year to query, inclusive (default: 2026).",
    )
    parser.add_argument(
        "--resume",
        default=None,
        help="If set, resume an earlier run by reusing its run_ts (re-uses already-fetched pairs).",
    )
    parser.add_argument(
        "--smoke",
        default=None,
        help="Smoke test: fetch a single league x single season without GCS writes "
        "(arg = canonical league_id, e.g. 'EPL').",
    )
    parser.add_argument(
        "--smoke-season",
        type=int,
        default=2024,
        help="Smoke test season year (default: 2024).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="No api calls, no GCS writes.")
    mode.add_argument("--apply", action="store_true", help="Run the full audit (api calls + GCS writes).")
    args = parser.parse_args()

    if not args.smoke and not args.dry_run and not args.apply:
        parser.error("Pass one of --dry-run / --apply / --smoke <LEAGUE>.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
