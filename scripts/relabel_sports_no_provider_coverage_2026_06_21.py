#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: out-of-coverage entity cells mislabelled attempted_failed == 0
"""Relabel mislabeled sports per-fixture-entity manifest rows -> ``empty_confirmed``.

ROOT CAUSE (audit 2026-06-21): the instruments-service sports enrichment fetches
the per-fixture / enrichment entities (PLAYER_STATS / FIXTURE_LINEUPS /
FIXTURE_EVENTS / FIXTURE_STATS / TEAMS / STANDINGS / INJURIES) for EVERY captured
fixture, but API-Football only PROVIDES them for SOME leagues (measured ~57% of
``/fixtures/players`` calls return 0 — 729 of 790 leagues never yield
PLAYER_STATS). When a league that never has the entity returned zero rows, the
cell either (a) hit the live-instrument guard and landed ``attempted_failed``
(coverage looks falsely incomplete) or (b) landed ``empty_confirmed`` with a
generic ``SOURCE_RETURNED_ZERO`` / blank / ``EXPECTED_NO_FIXTURE`` reason (which
falsely implies a re-fetch could fill it). Both mis-classify a genuine "this
league has no provider coverage for this entity" honest absence.

FIX (this script): for each per-fixture-entity cell where the (league, entity)
pair is observed-OUT-of-coverage per the UAC observed-coverage map
(``is_league_entity_covered`` == False) AND the cell is currently
``attempted_failed`` OR ``empty_confirmed`` with a NON-canonical reason
(blank / ``SOURCE_RETURNED_ZERO`` / ``EXPECTED_NO_FIXTURE``), re-label to
``empty_confirmed(reason=EXPECTED_NO_PROVIDER_COVERAGE)``. IN-coverage
``attempted_failed`` rows (a league that DOES yield the entity but a real fetch
failed) are LEFT untouched — those are genuine failures to retry. ``captured``
and ``expected_unattempted`` cells are never touched.

IDEMPOTENT: a second run is a no-op (already-``EXPECTED_NO_PROVIDER_COVERAGE``
cells aren't re-matched). SINGLE-WALK: one read of the ``_index``, one
classification pass, one write-back. The Part-2 write-path fix (coverage-aware
skip + emit) makes NEW writes produce the same empty_confirmed, so live writes
reinforce — but ship Part-2 BEFORE --apply here.

DRY-RUN by default: prints count to re-label + projected attempted_failed% per
entity (before/after) + projected wasted-fetch reduction, writes the projection
to ``_index/audit/``. ``--apply`` snapshots the live ``_index`` to
``_index/snapshots/`` first, then writes back.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \
    .venv-workspace/bin/python scripts/relabel_sports_no_provider_coverage_2026_06_21.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_api_contracts.registry import LEAGUE_ENTITY_COVERAGE_ENTITIES, is_league_entity_covered
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("relabel_sports_no_provider_coverage")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_NO_COVERAGE_REASON = "EXPECTED_NO_PROVIDER_COVERAGE"
# Per-fixture-entity empty_confirmed reasons that are NON-canonical for an
# out-of-coverage (league, entity) cell — these are the wrong-empty reasons we
# also correct (a covered cell's calendar/source gap is NOT in this set).
_WRONG_EMPTY_REASONS = frozenset({"", "SOURCE_RETURNED_ZERO", "EXPECTED_NO_FIXTURE"})


def _af_pct(df: pd.DataFrame) -> float:
    cs = df["capture_status"].astype("string")
    denom = cs.isin(("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")).sum()
    if denom == 0:
        return 0.0
    return 100.0 * (cs == "attempted_failed").sum() / denom


def _build_relabel_mask(df: pd.DataFrame) -> pd.Series:
    """True for per-fixture-entity rows whose (league, entity) is out-of-coverage
    and whose current status/reason is a fetch-failure or a wrong-empty."""
    entities = frozenset(LEAGUE_ENTITY_COVERAGE_ENTITIES)
    dt = df["data_type"].astype("string").str.upper()
    cs = df["capture_status"].astype("string")
    reason = df["error_reason"].astype("string").fillna("")
    is_entity = dt.isin(entities)
    is_failed = cs == "attempted_failed"
    is_wrong_empty = (cs == "empty_confirmed") & reason.isin(_WRONG_EMPTY_REASONS)
    candidate = is_entity & (is_failed | is_wrong_empty)
    leagues = df["league_id"].astype("string").fillna("")
    cand_idx = df.index[candidate.values]
    # Resolve coverage ONCE per distinct (entity, league) pair.
    pairs = {(str(dt[i]), str(leagues[i])) for i in cand_idx}
    out_of_cov = {(e, lg) for (e, lg) in pairs if lg and not is_league_entity_covered(lg, e)}
    mask = pd.Series(False, index=df.index)
    for i in cand_idx:
        if (str(dt[i]), str(leagues[i])) in out_of_cov:
            mask[i] = True
    return mask


def relabel(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    mask = _build_relabel_mask(df)
    n = int(mask.sum())
    out = df.copy()
    # How many of the relabeled were attempted_failed (the false-failure win)?
    was_failed = int((mask & (df["capture_status"].astype("string") == "attempted_failed")).sum())
    out.loc[mask, "capture_status"] = "empty_confirmed"
    out.loc[mask, "error_reason"] = _NO_COVERAGE_REASON
    if "schema_version" in out.columns:
        out.loc[mask, "schema_version"] = 9

    entities = frozenset(LEAGUE_ENTITY_COVERAGE_ENTITIES)
    ent = df[df["data_type"].astype("string").str.upper().isin(entities)]
    ent_out = out[out["data_type"].astype("string").str.upper().isin(entities)]
    stats: dict[str, object] = {
        "rows_total": len(df),
        "entity_rows": len(ent),
        "relabeled": n,
        "relabeled_from_attempted_failed": was_failed,
        "relabeled_from_wrong_empty": n - was_failed,
        "entity_af_pct_before": round(_af_pct(ent), 3),
        "entity_af_pct_after": round(_af_pct(ent_out), 3),
        "overall_af_pct_before": round(_af_pct(df), 3),
        "overall_af_pct_after": round(_af_pct(out), 3),
    }

    # Per-entity attempted_failed count + relabel breakdown.
    per_entity: dict[str, object] = {}
    dt = df["data_type"].astype("string").str.upper()
    for e in sorted(entities):
        sub_b = df[dt == e]
        sub_a = out[out["data_type"].astype("string").str.upper() == e]
        af_b = int((sub_b["capture_status"].astype("string") == "attempted_failed").sum())
        af_a = int((sub_a["capture_status"].astype("string") == "attempted_failed").sum())
        relbl = int((mask & (dt == e)).sum())
        per_entity[e] = {"attempted_failed": f"{af_b}->{af_a}", "relabeled": relbl}
    stats["per_entity"] = per_entity
    return out, stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the relabeled index back (snapshots first).")
    args = p.parse_args(argv)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    fs = gcsfs.GCSFileSystem()
    logger.info("Reading live _index gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
    if "error_reason" not in df.columns:
        df["error_reason"] = pd.array([None] * len(df), dtype="string")

    out, stats = relabel(df)
    logger.info("Relabel stats:")
    for k, v in stats.items():
        logger.info("  %s = %s", k, v)

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    proj = f"{bucket}/_index/audit/projected_sports_no_provider_coverage_2026_06_21.parquet"
    with fs.open(proj, "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("Wrote projection -> gs://%s", proj)

    if not args.apply:
        logger.info("DRY RUN — live _index untouched.")
        return 0

    stamp = datetime.now(UTC).strftime("%Y%m%d")
    snap = f"{bucket}/_index/snapshots/pre_sports_no_provider_coverage_{stamp}.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot -> gs://%s", snap)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("APPLIED. Relabeled %s rows.", stats["relabeled"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
