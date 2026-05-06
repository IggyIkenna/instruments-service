"""Phase 2 — Targeted FIXTURES recovery using the Phase 1 truth-set.

Plan: ``unified-trading-pm/plans/active/sports_fixtures_truthset_recovery_2026_05_06.plan.md``.

Reads the Phase 1 recovery-set parquet (gs://.../_audits/fixtures_recovery_set_{run_ts}.parquet)
and for every unique ``(canonical_league_id, season)`` pair represented in it:

  1. Calls api_football ``/fixtures?league&season`` once (re-using Phase 1's
     access pattern so we hit ~200-500 calls total instead of 39k).
  2. Filters the response down to fixtures whose ``(date, canonical_league_id)``
     pair is in the recovery set.
  3. For each such ``(date, canonical_league_id)``, writes a per-league
     sub-partition parquet at
     ``sports_reference/by_date/day={D}/entity=fixtures/league={L}/fixtures.parquet``
     using the canonical 32-column ``SPORTS_FIXTURES`` schema (same shape the
     orchestrator emits).
  4. Records a ``manifest.add(...)`` row keyed on (date, FIXTURES, league_id)
     with ``capture_status='captured'`` so the orchestrator's pre-flight
     stops asking api_football to re-fetch on every cycle.

Empty-confirmed flip (optional, ``--flip-empty-attempts``):
  The audit also surfaced ``ATTEMPTED_FAILED_NO_TRUTH`` rows — phantom-flipped
  rows where api_football *also* says no fixtures exist for that (date, league).
  These are honestly empty and should be ``empty_confirmed`` rather than left
  as ``attempted_failed``. The flip burns no api quota.

Per-VM shard isolation
----------------------
Sets ``MANIFEST_PER_VM_SHARDS=true`` and ``VM_NAME=fixtures-recovery-{run_ts}``
inside the ManifestWriter init so the per-VM shard at
``_index/per_vm/fixtures-recovery-{run_ts}.parquet`` is the write target.
The consolidator merges into canonical with last-writer-wins on row_key.

Usage
-----
::

  # Dry-run — report (league, season) pairs that need re-fetch + total counts:
  python scripts/recover_fixtures_from_truthset.py \\
      --truthset-run-ts 20260506-153914 --dry-run

  # Apply (real api_football calls + GCS writes):
  python scripts/recover_fixtures_from_truthset.py \\
      --truthset-run-ts 20260506-153914 --apply

  # Apply + flip ATTEMPTED_FAILED_NO_TRUTH to empty_confirmed:
  python scripts/recover_fixtures_from_truthset.py \\
      --truthset-run-ts 20260506-153914 --apply --flip-empty-attempts
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import (
    LEAGUE_REGISTRY,
)
from unified_api_contracts.external.api_football.normalize import (
    normalize_api_football_fixture,
)
from unified_trading_library import ManifestWriter, get_storage_client
from unified_trading_library.startup_validation import validate_api_keys_for_venues

from instruments_service.engine.orchestrator import (
    _flatten_canonical_fixture_for_disk,
)
from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
    _parse_fixture_response,
)

logger = logging.getLogger(__name__)

_DEFAULT_PROJECT_ID = "central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"
_AUDIT_PREFIX = "_audits"
_FIXTURES_PARQUET_TEMPLATE = "sports_reference/by_date/day={day}/entity=fixtures/league={league_id}/fixtures.parquet"
_DATA_AVAILABLE_LEAD_DAYS = 7  # fixtures: announced_at = kickoff - 7d (matches existing on-disk values)


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _audit_blob(name: str, run_ts: str, suffix: str) -> str:
    return f"{_AUDIT_PREFIX}/{name}_{run_ts}.{suffix}"


def _read_recovery_set(storage: object, bucket: str, run_ts: str) -> pd.DataFrame:
    raw = cast("bytes", storage.download_bytes(bucket, _audit_blob("fixtures_recovery_set", run_ts, "parquet")))  # type: ignore[attr-defined]
    return pd.read_parquet(io.BytesIO(raw))


def _read_truth_set(storage: object, bucket: str, run_ts: str) -> pd.DataFrame:
    raw = cast("bytes", storage.download_bytes(bucket, _audit_blob("fixtures_truthset", run_ts, "parquet")))  # type: ignore[attr-defined]
    return pd.read_parquet(io.BytesIO(raw))


def _read_diff(storage: object, bucket: str, run_ts: str) -> pd.DataFrame:
    raw = cast("bytes", storage.download_bytes(bucket, _audit_blob("fixtures_diff", run_ts, "csv")))  # type: ignore[attr-defined]
    return pd.read_csv(io.BytesIO(raw))


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


async def _fetch_full_response(
    adapter: ApiFootballAdapter,
    af_league_id: int,
    season: int,
) -> list[dict[str, object]]:
    """Identical to the audit script's helper but kept self-contained."""
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"league": str(af_league_id), "season": str(season)}
    async with adapter._make_session() as session:
        raw = await adapter._get_with_retry(session, url, params=params, headers=adapter._headers())
    if isinstance(raw, dict):
        response = raw.get("response")
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
    return []


def _build_fixture_rows_for_league_season(
    canonical_league_id: str,
    response: list[dict[str, object]],
    target_dates: set[str],
) -> dict[str, list[dict[str, object]]]:
    """Filter response to target dates + flatten to disk-shape dicts grouped by date.

    Each output dict is the 32-column SPORTS_FIXTURES row dict produced by
    ``_flatten_canonical_fixture_for_disk``. ``data_available_at`` is filled
    by the caller (post-batch operation on the per-day DataFrame) so we can
    apply the kickoff - 7d formula vectorised.
    """
    by_day: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in response:
        fixture_obj = item.get("fixture") if isinstance(item, dict) else None
        if not isinstance(fixture_obj, dict):
            continue
        date_str = fixture_obj.get("date")
        if not isinstance(date_str, str) or len(date_str) < 10:
            continue
        day = date_str[:10]
        if day not in target_dates:
            continue
        try:
            af_fixture_raw = _parse_fixture_response(item)
            if af_fixture_raw is None:
                continue
            canonical_fx = normalize_api_football_fixture(af_fixture_raw)
            row = _flatten_canonical_fixture_for_disk(canonical_fx, day)
        except Exception as exc:
            logger.warning("normalize/flatten failed for %s on %s: %s", canonical_league_id, day, exc)
            continue
        by_day[day].append(row)
    return by_day


def _post_fill_data_available_at(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised post-fill of ``data_available_at`` = kickoff - 7d.

    The orchestrator's mainline writer does the same on its per-day DataFrame
    (see ``orchestrator.py:3241+`` and many other call sites). Schema
    expectations match the on-disk SPORTS_FIXTURES contract.
    """
    if "timestamp" in df.columns and df["timestamp"].notna().any():
        df["data_available_at"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce") - pd.Timedelta(
            days=_DATA_AVAILABLE_LEAD_DAYS
        )
    elif "date" in df.columns:
        df["data_available_at"] = pd.to_datetime(df["date"], utc=True, errors="coerce") - pd.Timedelta(
            days=_DATA_AVAILABLE_LEAD_DAYS
        )
    else:
        df["data_available_at"] = pd.NaT
    return df


def _write_per_league_parquet(
    storage: object,
    bucket: str,
    day: str,
    canonical_league_id: str,
    rows: list[dict[str, object]],
) -> int:
    df = pd.DataFrame(rows)
    df = _post_fill_data_available_at(df)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob = _FIXTURES_PARQUET_TEMPLATE.format(day=day, league_id=canonical_league_id)
    storage.upload_bytes(bucket, blob, buf.read())  # type: ignore[attr-defined]
    return len(df)


# ---------------------------------------------------------------------------
# Flip ATTEMPTED_FAILED_NO_TRUTH → empty_confirmed (optional)
# ---------------------------------------------------------------------------


def _flip_attempted_failed_to_empty_confirmed(
    storage: object,
    bucket: str,
    diff_df: pd.DataFrame,
    run_ts: str,
    apply: bool,
) -> int:
    """Flip ATTEMPTED_FAILED_NO_TRUTH rows by writing a per-VM shard ONLY.

    Per the workspace rule that the consolidator owns canonical, this no longer
    mutates ``_index/availability_index.parquet`` directly — it reads canonical
    to identify the rows that need flipping, then writes the flipped rows to
    ``_index/per_vm/recover-fixtures-flip-{run_ts}.parquet``. The next
    consolidator cycle merges the shard with last-attempted-write-wins, which
    works because we stamp the flipped rows' ``attempted_at`` with NOW.

    Reference incident 2026-05-06: an earlier version of this function wrote
    canonical directly. That bumped canonical's ``blob.updated`` mtime past
    the recovery script's per-VM shard mtime, and the consolidator's
    incremental-merge cutoff (which used ``blob.updated``) silently skipped
    the recovery shard on every subsequent cycle. UTL ``manifest_consolidator``
    now reads the ``consolidator_run_at`` metadata marker instead of
    ``blob.updated`` (commit 0ab7432a), but the cleanest belt-and-braces fix
    is to stop writing canonical out-of-band entirely.
    """
    af_no_truth = diff_df.loc[diff_df["classification"] == "ATTEMPTED_FAILED_NO_TRUTH"].copy()
    if af_no_truth.empty:
        logger.info("ATTEMPTED_FAILED_NO_TRUTH set is empty — nothing to flip")
        return 0

    target_pairs: set[tuple[str, str]] = {
        (str(row["date"]), str(row["canonical_league_id"])) for _idx, row in af_no_truth.iterrows()
    }
    logger.info(
        "ATTEMPTED_FAILED_NO_TRUTH flip target: %d (date, league) pairs",
        len(target_pairs),
    )

    raw = cast("bytes", storage.download_bytes(bucket, _INDEX_PATH))  # type: ignore[attr-defined]
    canonical = pd.read_parquet(io.BytesIO(raw))

    # Build mask: capture_status==attempted_failed AND data_type==FIXTURES AND
    # (date, league_id) in target_pairs.
    cs = canonical["capture_status"].astype(str)
    dt = canonical["data_type"].astype(str)
    pairs = list(zip(canonical["date"].astype(str), canonical["league_id"].astype(str), strict=False))
    in_target = pd.Series([p in target_pairs for p in pairs], index=canonical.index)
    mask = (cs == "attempted_failed") & (dt == "FIXTURES") & in_target
    n_flip = int(mask.sum())
    logger.info(
        "Canonical rows matching flip target: %d (subset of target pairs that have FIXTURES attempted_failed in canonical)",
        n_flip,
    )

    if n_flip == 0:
        return 0
    if not apply:
        logger.info("[dry-run] Would write per-VM shard with %d flipped rows", n_flip)
        return 0

    # Build the flipped subset (copy to avoid SettingWithCopyWarning).
    now_iso = datetime.now(UTC).isoformat()
    flipped = canonical.loc[mask].copy()
    flipped["capture_status"] = "empty_confirmed"
    flipped["error_reason"] = f"flipped_via_recover_fixtures_from_truthset_{run_ts}__truth_says_empty"
    flipped["attempted_at"] = now_iso
    # Bump written_at too so consolidator's last-write-wins picks our flipped
    # rows over the original attempted_failed rows still in other shards.
    if "written_at" in flipped.columns:
        flipped["written_at"] = now_iso

    # Write per-VM shard. Consolidator merges this on next cycle with
    # last-attempted-at wins → flipped rows replace the original
    # attempted_failed rows in canonical without us touching canonical.
    shard_blob = f"_index/per_vm/recover-fixtures-flip-{run_ts}.parquet"
    out = io.BytesIO()
    flipped.to_parquet(out, index=False, engine="pyarrow")
    out.seek(0)
    storage.upload_bytes(bucket, shard_blob, out.read())  # type: ignore[attr-defined]
    logger.info("Wrote per-VM shard to gs://%s/%s (%d rows)", bucket, shard_blob, n_flip)
    logger.info(
        "Consolidator will merge this shard into canonical on next cycle "
        "(last-attempted-at wins → flipped rows replace attempted_failed rows)"
    )

    logger.info(
        "Flipped %d ATTEMPTED_FAILED_NO_TRUTH FIXTURES rows -> empty_confirmed (via per-VM shard)",
        n_flip,
    )
    return n_flip


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace) -> int:
    project_id = args.project_id
    bucket = _bucket_for_project(project_id)
    run_ts = args.truthset_run_ts
    new_run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    storage = get_storage_client(project_id=project_id)

    # ------------------------------------------------------------------
    # Read recovery + truth-set
    # ------------------------------------------------------------------
    logger.info(
        "Reading recovery set / truth-set from gs://%s/_audits/*_%s.{parquet,csv}",
        bucket,
        run_ts,
    )
    recovery_df = _read_recovery_set(storage, bucket, run_ts)
    logger.info("Recovery set: %d rows", len(recovery_df))
    by_class = recovery_df.groupby("classification").size().sort_values(ascending=False)
    for cls, n in by_class.items():
        logger.info("  %-20s %d", cls, n)

    # Build target_dates per (canonical_league_id) and the (league, season)
    # set we need to re-fetch.
    target_by_league: dict[str, set[str]] = defaultdict(set)
    league_seasons: set[tuple[str, int]] = set()
    truth_df = _read_truth_set(storage, bucket, run_ts)
    truth_lookup_seasons: dict[tuple[str, str], int] = {
        (str(row["canonical_league_id"]), str(row["date"])): int(row["season"]) for _idx, row in truth_df.iterrows()
    }
    for _idx, row in recovery_df.iterrows():
        league = str(row["canonical_league_id"])
        day = str(row["date"])
        target_by_league[league].add(day)
        season_int = truth_lookup_seasons.get((league, day))
        if season_int is None:
            logger.warning("No season in truth-set for (%s, %s) — skipping", league, day)
            continue
        league_seasons.add((league, season_int))
    logger.info(
        "Distinct (league, season) pairs to re-fetch: %d (%d unique leagues, %d total target dates)",
        len(league_seasons),
        len(target_by_league),
        sum(len(v) for v in target_by_league.values()),
    )

    if args.dry_run:
        logger.info("[dry-run] Would call api_football for %d (league, season) pairs", len(league_seasons))
        logger.info("[dry-run] Would write per-league fixtures parquets + manifest captured rows")
        if args.flip_empty_attempts:
            diff_df = _read_diff(storage, bucket, run_ts)
            _flip_attempted_failed_to_empty_confirmed(storage, bucket, diff_df, new_run_ts, apply=False)
        return 0

    # ------------------------------------------------------------------
    # Fetch + write
    # ------------------------------------------------------------------
    keys = validate_api_keys_for_venues(venues=["api_football"], project_id=project_id)
    api_key = keys.get("api_football")
    if not api_key:
        logger.error("Could not fetch api_football api key from Secret Manager (got: %s)", sorted(keys.keys()))
        return 2
    adapter = ApiFootballAdapter(api_key=api_key)

    # ManifestWriter with per-VM shard isolation (workspace rule for one-shot scripts).
    vm_name = f"fixtures-recovery-{new_run_ts}"
    os.environ["VM_NAME"] = vm_name  # noqa: qg-os-environ — ManifestWriter reads via UnifiedCloudConfig at init
    os.environ["MANIFEST_PER_VM_SHARDS"] = "true"  # noqa: qg-os-environ — same reason
    manifest = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )

    total = len(league_seasons)
    pairs_done = 0
    fixtures_written = 0
    days_written = 0
    failed_pairs: list[tuple[str, int, str]] = []

    for canonical_league_id, season in sorted(league_seasons):
        pairs_done += 1
        league_def = LEAGUE_REGISTRY.get(canonical_league_id)
        if league_def is None or league_def.api_football_id is None:
            logger.warning("[%d/%d] %s season=%d  no af_id — skipping", pairs_done, total, canonical_league_id, season)
            failed_pairs.append((canonical_league_id, season, "no_af_id"))
            continue
        af_league_id = int(league_def.api_football_id)

        try:
            response = await _fetch_full_response(adapter, af_league_id, season)
        except Exception as exc:
            logger.warning(
                "[%d/%d] %s season=%d  FETCH FAILED %s",
                pairs_done,
                total,
                canonical_league_id,
                season,
                exc,
            )
            failed_pairs.append((canonical_league_id, season, f"fetch_failed:{exc}"))
            continue

        target_dates = target_by_league.get(canonical_league_id, set())
        by_day = _build_fixture_rows_for_league_season(canonical_league_id, response, target_dates)
        n_days_this_pair = 0
        n_fixtures_this_pair = 0
        for day, rows in sorted(by_day.items()):
            try:
                n = _write_per_league_parquet(storage, bucket, day, canonical_league_id, rows)
                manifest.add(
                    processing_date=date_type.fromisoformat(day),
                    row_count=n,
                    data_type="FIXTURES",
                    league_id=canonical_league_id,
                    venue="api_football",
                )
                n_days_this_pair += 1
                n_fixtures_this_pair += n
            except Exception as exc:
                logger.warning(
                    "%s %s  WRITE FAILED %s",
                    canonical_league_id,
                    day,
                    exc,
                )

        days_written += n_days_this_pair
        fixtures_written += n_fixtures_this_pair
        logger.info(
            "[%d/%d] %-30s season=%d  af_response=%d  recovered_days=%d  fixtures_written=%d",
            pairs_done,
            total,
            canonical_league_id,
            season,
            len(response),
            n_days_this_pair,
            n_fixtures_this_pair,
        )

    # Final flush — write the per-VM manifest shard to GCS.
    manifest.flush()
    logger.info(
        "Recovery complete: %d (league, season) pairs processed, %d days written, %d fixtures written, %d failed pairs",
        total,
        days_written,
        fixtures_written,
        len(failed_pairs),
    )
    if failed_pairs:
        logger.info("Failed pairs:")
        for lid, ssn, reason in failed_pairs:
            logger.info("  %s season=%d  %s", lid, ssn, reason)

    # ------------------------------------------------------------------
    # Optional: flip ATTEMPTED_FAILED_NO_TRUTH → empty_confirmed
    # ------------------------------------------------------------------
    if args.flip_empty_attempts:
        diff_df = _read_diff(storage, bucket, run_ts)
        _flip_attempted_failed_to_empty_confirmed(storage, bucket, diff_df, new_run_ts, apply=True)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default=_DEFAULT_PROJECT_ID)
    parser.add_argument(
        "--truthset-run-ts",
        required=True,
        help="The run_ts identifier from Phase 1 (e.g. '20260506-153914').",
    )
    parser.add_argument(
        "--flip-empty-attempts",
        action="store_true",
        help="Also flip ATTEMPTED_FAILED_NO_TRUTH rows to empty_confirmed (no api calls).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
