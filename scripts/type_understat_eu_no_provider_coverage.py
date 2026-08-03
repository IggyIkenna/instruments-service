#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: understat XG/XG_SHOTS pending_fetch (expected_unattempted blank reason source=understat) == 0 for 7 consecutive daily runs (verified post-apply + consolidator merge)
"""type_understat_eu_no_provider_coverage.py — type the small, self-renewing understat
XG/XG_SHOTS blank-reason ``expected_unattempted`` residual (~30 rows as of 2026-07-12) as
``empty_confirmed(EXPECTED_NO_FIXTURE)`` when the (league, date) genuinely has no scheduled
matchday, per issue doc ``plans/active/issues/sports_is_manifest_eu_regression_overwrite_2026_06_29.md``.

ROOT CAUSE: the daily forward-poll enum (``scripts/enumerate_expected_universe.py``
``_enumerate_v2_sports``) seeds ``expected_unattempted`` for every (league, data_type, date)
cell not yet present in the consolidated manifest. Its per-day source-rule gate
(``is_expected_for_source``) catches season/transfer-window gaps but is NOT matchday-aware —
it does not check whether a fixture is actually scheduled on that day. For understat XG/
XG_SHOTS this means off-season / no-fixture trailing-edge dates (e.g. the July off-season
across the big-5 leagues understat covers) keep re-materialising as blank-reason
``expected_unattempted`` every ~24h instead of being typed ``EXPECTED_NO_FIXTURE`` — the
same bug family as the weather/SFI residuals (``type_weather_eu_no_provider_coverage_2026_06_27.py``,
``type_sfi_eu_no_provider_coverage_2026_06_27.py``) and the legacy blank-``empty_confirmed``
reconciliation (``reconcile_sports_blank_empty_reason_2026_06_24.py``), whose fixture-index +
``does_understat_cover`` classification this script reuses.

This script does NOT fix the writer (that is the deeper, still-open durable fix tracked in the
issue doc) — it is the same one-off typing-sweep pattern as its two sibling scripts, scoped to
understat only.

MATCHDAY-AWARE MASK: unlike the weather/SFI scripts (which key off league-coverage alone),
understat's big-5 leagues ARE covered — the gap is per-DATE (no fixture that day), not
per-LEAGUE. So this script builds a (canonical_league, day) fixture-existence index from the
captured api_football FIXTURES parquets (mirrors ``reconcile_sports_blank_empty_reason_2026_06_24.py``
``_build_fixture_index``) and only types a row when there is genuinely NO fixture that day. A
covered league WITH a fixture that day is left untouched — that is a real pending_fetch, never
silently retyped.

CONSOLIDATOR-SAFE WRITE: writes ONLY the re-typed rows as a per-VM shard at
``_index/per_vm/{VM_NAME}.parquet``. The consolidator's last-write-wins merge picks the shard
rows (fresher ``attempted_at``) over the canonical index rows -> the typing WINS without
touching the canonical blob.

DRY-RUN by default. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=type-understat-eu-$(date +%s) \\
      .venv/bin/python scripts/type_understat_eu_no_provider_coverage.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import get_league_by_api_football_id
from unified_api_contracts.canonical.domain.sports.provider_league_ids import does_understat_cover
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("type_understat_eu_no_provider_coverage")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
_FIXTURES_TPL = "sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet"
_UNDERSTAT_DATA_TYPES = frozenset({"XG", "XG_SHOTS"})
_NO_FIXTURE_REASON = "EXPECTED_NO_FIXTURE"
_NO_COVER_REASON = "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE"


def _build_base_mask(df: pd.DataFrame) -> pd.Series:
    """True for XG/XG_SHOTS expected_unattempted rows with blank reason, source=understat."""
    cs = df["capture_status"].astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    reason = df["error_reason"].astype("string").fillna("")
    source = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    return dt.isin(_UNDERSTAT_DATA_TYPES) & (cs == "expected_unattempted") & (reason == "") & (source == "understat")


def _build_fixture_index(fs: gcsfs.GCSFileSystem, bucket: str, days: list[str]) -> set[tuple[str, str]]:
    """Return the set of ``(canonical_league_upper, day)`` that have >=1 captured
    api_football fixture, mirroring ``reconcile_sports_blank_empty_reason_2026_06_24.py``.
    Days with no fixtures parquet contribute nothing -> those (league, day) cells have
    genuinely no matchday."""

    def _read_day(day: str) -> list[tuple[str, str]]:
        path = f"{bucket}/{_FIXTURES_TPL.format(day=day)}"
        try:
            with fs.open(path, "rb") as fh:
                fdf = pd.read_parquet(fh, columns=["af_league_id"])
        except (FileNotFoundError, OSError, ValueError):
            return []
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw in fdf["af_league_id"].astype("string").dropna().unique():
            try:
                af_id = int(str(raw).strip())
            except (ValueError, TypeError):
                continue
            league = get_league_by_api_football_id(af_id)
            if league is None:
                continue
            name = str(league.league_id).upper()
            if name in seen:
                continue
            seen.add(name)
            out.append((name, day))
        return out

    index: set[tuple[str, str]] = set()
    logger.info("Building fixture-existence index from %d day parquets ...", len(days))
    with ThreadPoolExecutor(max_workers=32) as pool:
        for pairs in pool.map(_read_day, days):
            index.update(pairs)
    logger.info("Fixture index: %d (canonical_league, day) pairs across %d days", len(index), len(days))
    return index


def _classify(league: str, has_fixture: bool) -> str:
    """Honest EmptyConfirmedReason for a candidate blank-reason understat EU row."""
    if league and not does_understat_cover(league):
        return _NO_COVER_REASON
    return _NO_FIXTURE_REASON if not has_fixture else ""


def _build_type_mask_and_reasons(
    df: pd.DataFrame, base_mask: pd.Series, fixture_index: set[tuple[str, str]]
) -> tuple[pd.Series, dict[int, str]]:
    """Among ``base_mask`` rows, keep only those with NO matchday that day (or genuinely
    out of understat coverage) -> those get typed. A covered league WITH a fixture that
    day is a real pending_fetch and is excluded (reason == "" -> not typed)."""
    league_col = df["league_id"].astype("string").fillna("").str.upper()
    day_col = df["date"].astype("string").fillna("").str.slice(0, 10)

    reasons: dict[int, str] = {}
    for i in df.index[base_mask.values]:
        league = str(league_col[i])
        day = str(day_col[i])
        has_fix = (league, day) in fixture_index
        reason = _classify(league, has_fix)
        if reason:
            reasons[i] = reason

    type_mask = pd.Series(False, index=df.index)
    if reasons:
        type_mask.loc[list(reasons.keys())] = True
    return type_mask, reasons


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the re-typed rows as a per-VM shard.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    instance = os.environ.get("VM_NAME", "")  # noqa: qg-empty-fallback — unset VM_NAME => `not instance` refuses --apply below
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique>. Refusing.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved bucket %s is not the expected env-short shape. Refusing.", bucket)
        return 1

    fs = gcsfs.GCSFileSystem()
    logger.info("Reading live _index gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)

    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")

    base_mask = _build_base_mask(df)
    n_candidates = int(base_mask.sum())
    logger.info("understat XG/XG_SHOTS blank-reason expected_unattempted candidates: %d", n_candidates)
    if n_candidates == 0:
        logger.info("Nothing to type — manifest already clean for understat EU rows.")
        return 0

    days = sorted(df.loc[base_mask.values, "date"].astype("string").fillna("").str.slice(0, 10).unique())
    days = [d for d in days if d]
    fixture_index = _build_fixture_index(fs, bucket, days)

    type_mask, reasons = _build_type_mask_and_reasons(df, base_mask, fixture_index)
    n = int(type_mask.sum())
    logger.info("Rows to type (no matchday / out of coverage): %d", n)
    logger.info("Rows left untouched (real pending_fetch — fixture exists): %d", n_candidates - n)

    if n == 0:
        logger.info("Nothing to type — all candidate rows have a genuine matchday (real pending_fetch).")
        return 0

    pending = df[type_mask]
    logger.info("  Unique leagues: %d", pending["league_id"].nunique())
    logger.info("  Date range: %s to %s", pending["date"].min(), pending["date"].max())
    dist = pd.Series(reasons).value_counts()
    logger.info("  Reason distribution: %s", dist.to_dict())

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    shard_df = df.loc[type_mask].copy()
    shard_df["capture_status"] = "empty_confirmed"
    for i in shard_df.index:
        shard_df.loc[i, "error_reason"] = reasons[i]
    shard_df["attempted_at"] = now_iso
    if "written_at" in shard_df.columns:
        shard_df["written_at"] = now_iso
    if "schema_version" in shard_df.columns:
        shard_df["schema_version"] = 9

    shard_path = _PER_VM_PATH_TEMPLATE.format(instance=instance)
    sbuf = io.BytesIO()
    shard_df.to_parquet(sbuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    sbuf.seek(0)
    with fs.open(f"{bucket}/{shard_path}", "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info(
        "APPLIED. Wrote %d re-typed rows as per-VM shard gs://%s/%s (consolidator merges next cycle).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
