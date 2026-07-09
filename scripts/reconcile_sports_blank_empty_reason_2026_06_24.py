#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: blank-reason empty_confirmed count in sports _index == ~0 (verified post-apply + consolidator merge)
"""reconcile_sports_blank_empty_reason_2026_06_24.py — type the legacy blank-reason
sports ``empty_confirmed`` cells with the CORRECT closed-set ``EmptyConfirmedReason``.

THE FINDING (2026-06-24): the live sports manifest
``instruments-store-sports-prd-{pid}/_index/availability_index.parquet`` carries
~296k ``empty_confirmed`` cells with a **blank** ``error_reason`` — a single bulk
write 2026-04-21..29 that PRE-DATES the ``record_empty`` blank-reason gate
(``LegacyBlankErrorReasonError``, landed 2026-05-07). ``EmptyConfirmedReason`` is a
closed typed set; these legacy empties slipped through. A blank reason →
``is_out_of_coverage_window("")`` is False → they are mis-counted as in-window gaps
and aren't honestly classified.

PART 1 (this script) — TYPE the legacy blanks, data-type-aware per the SSOT maps:

* **api_football per-fixture / league-axis entities**
  (FIXTURE_LINEUPS / FIXTURE_STATS / FIXTURE_EVENTS / PLAYER_STATS / INJURIES /
  TEAMS / STANDINGS / WEATHER / PLAYER_VALUES):
    - ``not is_league_entity_covered(league, entity)`` (observed corpus shows this
      (league, entity) pair NEVER produced a row) → ``EXPECTED_NO_PROVIDER_COVERAGE``.
    - in-coverage but NO api_football fixture on that (canonical_league, day)
      (per the captured FIXTURES parquets, joined by ``af_league_id`` →
      canonical name) → ``EXPECTED_NO_FIXTURE``.
    - in-coverage AND a fixture exists → ``SOURCE_RETURNED_ZERO`` (source genuinely
      returned zero for a real fixture).
* **XG** (understat): ``not does_understat_cover(league)`` →
  ``EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE``; else fixture-pin → NO_FIXTURE /
  SOURCE_RETURNED_ZERO.
* **PREDICTIONS / ODDS / MATCHES** (footystats) + **SFI_PROGRESSIVE_STATS** (sfi):
  fixture-pinned sources — NO fixture on that (league, day) → ``EXPECTED_NO_FIXTURE``;
  fixture exists → ``SOURCE_RETURNED_ZERO``.

The fixture-existence index is built ONCE from the captured api_football FIXTURES
parquets (``sports_reference/by_date/day={d}/entity=fixtures/fixtures.parquet``,
column ``af_league_id``) mapped to canonical league names via
``get_league_by_api_football_id`` — so the (league, day) fixture join is reliable
despite the manifest using canonical NAMES while the parquet uses numeric AF ids.
Days with no fixtures parquet (pre-coverage / genuinely no matches) yield NO
fixture for any league → ``EXPECTED_NO_FIXTURE`` (the honest out-of-window state).

PART 2 — the LEAK is already CLOSED in code: every orchestrator sports callsite
passes a typed reason (``record_empty(reason=EXPECTED_NO_FIXTURE / ...)``) and the
UTL ``record_empty`` writer gate (``_writer_record.py``) HARD-RAISES
``LegacyBlankErrorReasonError`` on a blank reason. These 296k blanks are legacy
(single 2026-04 bulk write) — no live code path produces new blank empties. This
script only reconciles the legacy rows.

CONSOLIDATOR-SAFE WRITE (mirrors ``migrate_sports_retired_types_2026_05_13.py`` /
``relabel_sports_no_provider_coverage_2026_06_21.py``): ``--apply`` writes ONLY the
relabeled rows as a per-VM shard at ``_index/per_vm/{VM_NAME}.parquet`` (the
canonical fleet write path). The consolidator's DuckDB last-write-wins merge
collapses each (canonical blank row, shard typed row) pair and picks the shard row
(fresher ``attempted_at``) → the typing WINS for its keys without touching the
canonical blob. A live-odds VM + the consolidator are running; a full ``_index``
overwrite would RACE them — never do that. Untouched cells (captured / failed /
expected_unattempted / already-typed empties) have different keys / are never in the
shard → never lost. A pre-apply snapshot is written to ``_index/snapshots/``.

DRY-RUN by default (prints before/after blank-reason count + the typed-reason
distribution + the projection blob). ``--apply`` requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/reconcile_sports_blank_empty_reason_2026_06_24.py [--apply]
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
from unified_api_contracts.canonical.crosscutting.honest_coverage import EMPTY_CONFIRMED_REASONS
from unified_api_contracts.canonical.domain.sports.league_data import get_league_by_api_football_id
from unified_api_contracts.canonical.domain.sports.provider_league_ids import does_understat_cover
from unified_api_contracts.registry import LEAGUE_ENTITY_COVERAGE_ENTITIES, is_league_entity_covered
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reconcile_sports_blank_empty_reason")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
_FIXTURES_TPL = "sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet"

# Reason constants (validated against EMPTY_CONFIRMED_REASONS at import).
_R_NO_PROVIDER_COVERAGE = "EXPECTED_NO_PROVIDER_COVERAGE"
_R_NO_FIXTURE = "EXPECTED_NO_FIXTURE"
_R_SOURCE_ZERO = "SOURCE_RETURNED_ZERO"
_R_SOURCE_NO_COVER_LEAGUE = "EXPECTED_SOURCE_DOES_NOT_COVER_LEAGUE"
for _r in (
    _R_NO_PROVIDER_COVERAGE,
    _R_NO_FIXTURE,
    _R_SOURCE_ZERO,
    _R_SOURCE_NO_COVER_LEAGUE,
):
    if _r not in EMPTY_CONFIRMED_REASONS:
        raise RuntimeError(f"{_r} not a valid EmptyConfirmedReason")  # import-time invariant

# Per-fixture / league-axis api_football entities that participate in the observed
# (league x entity) coverage map.
_AF_ENTITIES = frozenset(LEAGUE_ENTITY_COVERAGE_ENTITIES)
# Sources whose blank empties are fixture-pinned (no fixture → EXPECTED_NO_FIXTURE).
_FOOTYSTATS_DTS = frozenset({"PREDICTIONS", "ODDS", "MATCHES"})
_SFI_DTS = frozenset({"SFI_PROGRESSIVE_STATS"})


def _build_fixture_index(fs: gcsfs.GCSFileSystem, bucket: str, days: list[str]) -> set[tuple[str, str]]:
    """Return the set of ``(canonical_league_upper, day)`` that have >=1 captured
    api_football fixture, read from the per-day fixtures parquets (``af_league_id``
    mapped to canonical name via the UAC registry). Days with no parquet contribute
    nothing → those (league, day) cells classify as EXPECTED_NO_FIXTURE."""

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
        for i, pairs in enumerate(pool.map(_read_day, days), 1):
            index.update(pairs)
            if i % 500 == 0:
                logger.info("  fixture index: %d/%d days read, %d (league,day) pairs", i, len(days), len(index))
    logger.info("Fixture index: %d (canonical_league, day) pairs across %d days", len(index), len(days))
    return index


def _classify(
    dt: str,
    league: str,
    has_fixture: bool,
) -> str:
    """Return the typed EmptyConfirmedReason for a blank ``empty_confirmed`` row."""
    dt_u = dt.upper()
    league_u = league.upper()
    # 1. api_football per-entity coverage: out-of-coverage league → NO_PROVIDER_COVERAGE.
    if dt_u in _AF_ENTITIES:
        if league_u and not is_league_entity_covered(league_u, dt_u):
            return _R_NO_PROVIDER_COVERAGE
        # in-coverage: fixture-pinned — fixture exists → source zero; else no fixture.
        return _R_SOURCE_ZERO if has_fixture else _R_NO_FIXTURE
    # 2. understat XG: league outside Understat coverage → SOURCE_DOES_NOT_COVER_LEAGUE.
    if dt_u in ("XG", "XG_SHOTS"):
        if league_u and not does_understat_cover(league_u):
            return _R_SOURCE_NO_COVER_LEAGUE
        return _R_SOURCE_ZERO if has_fixture else _R_NO_FIXTURE
    # 3. footystats + sfi fixture-pinned sources.
    if dt_u in _FOOTYSTATS_DTS or dt_u in _SFI_DTS:
        return _R_SOURCE_ZERO if has_fixture else _R_NO_FIXTURE
    # 4. anything else (shouldn't happen for sports blanks) — safest honest absence.
    return _R_NO_FIXTURE if not has_fixture else _R_SOURCE_ZERO


def reconcile(
    df: pd.DataFrame, fixture_index: set[tuple[str, str]]
) -> tuple[pd.DataFrame, pd.Series, dict[str, object]]:
    er = df["error_reason"].astype("string").fillna("").str.strip()
    cs = df["capture_status"].astype("string")
    blank_mask = (cs == "empty_confirmed") & (er == "")
    out = df.copy()

    dt_col = df["data_type"].astype("string").fillna("").str.upper()
    lg_col = df["league_id"].astype("string").fillna("").str.upper()
    d_col = df["date"].astype("string").fillna("").str.slice(0, 10)

    reasons: dict[int, str] = {}
    for i in df.index[blank_mask.values]:
        dt = str(dt_col[i])
        lg = str(lg_col[i])
        day = str(d_col[i])
        has_fix = (lg, day) in fixture_index
        reasons[i] = _classify(dt, lg, has_fix)

    idx = list(reasons.keys())
    out.loc[idx, "error_reason"] = [reasons[i] for i in idx]
    if "schema_version" in out.columns:
        out.loc[idx, "schema_version"] = 9

    dist = pd.Series([reasons[i] for i in idx]).value_counts()
    stats: dict[str, object] = {
        "rows_total": len(df),
        "blank_before": int(blank_mask.sum()),
        "typed": len(idx),
        "typed_reason_distribution": dist.to_dict(),
    }
    return out, blank_mask, stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the typed rows as a per-VM shard (snapshots first).")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing — would resolve the stale env-less bucket.")
        return 1

    instance = os.environ.get("VM_NAME", "")  # noqa: qg-empty-fallback — unset env => `not instance` fails below
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique>. The typing is written as a "
            "per-VM shard the consolidator merges (never a full-index overwrite). Refusing."
        )
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

    # Days that need a fixture lookup = distinct days of blank rows.
    cs = df["capture_status"].astype("string")
    er = df["error_reason"].astype("string").fillna("").str.strip()
    blank = (cs == "empty_confirmed") & (er == "")
    days = sorted(df.loc[blank.values, "date"].astype("string").fillna("").str.slice(0, 10).unique())
    days = [d for d in days if d]
    fixture_index = _build_fixture_index(fs, bucket, days)

    out, mask, stats = reconcile(df, fixture_index)
    logger.info("Reconcile stats:")
    for k, v in stats.items():
        logger.info("  %s = %s", k, v)
    # Recompute blank-after over the WHOLE frame as a sanity check.
    er_after = out["error_reason"].astype("string").fillna("").str.strip()
    blank_after = int(((out["capture_status"].astype("string") == "empty_confirmed") & (er_after == "")).sum())
    logger.info("  blank_after (whole frame) = %d", blank_after)

    # Audit projection blob (safe to write any time — separate, non-canonical path).
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    proj = f"{bucket}/_index/audit/projected_sports_blank_empty_reason_2026_06_24.parquet"
    with fs.open(proj, "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("Wrote projection -> gs://%s", proj)

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    n = int(mask.sum())
    if n == 0:
        logger.info("Nothing to type — manifest already honest (no blank-reason empty_confirmed).")
        return 0

    # Snapshot the canonical blob before applying.
    snap = f"{bucket}/_index/snapshots/pre_blank_reason_typing_2026_06_24.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot of canonical _index -> gs://%s", snap)

    # Consolidator-safe per-VM shard: ONLY the typed rows, fresh attempted_at/written_at.
    now_iso = datetime.now(UTC).isoformat()
    shard_df = out.loc[mask].copy()
    shard_df["attempted_at"] = now_iso
    if "written_at" in shard_df.columns:
        shard_df["written_at"] = now_iso

    shard_path = _PER_VM_PATH_TEMPLATE.format(instance=instance)
    shbuf = io.BytesIO()
    shard_df.to_parquet(shbuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    shbuf.seek(0)
    with fs.open(f"{bucket}/{shard_path}", "wb") as fh:
        fh.write(shbuf.getvalue())
    logger.info(
        "APPLIED. Wrote %d typed rows as per-VM shard gs://%s/%s (consolidator merges next cycle).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
