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
classification pass, one shard write. The Part-2 write-path fix (coverage-aware
skip + emit) makes NEW writes produce the same empty_confirmed, so live writes
reinforce — but ship Part-2 BEFORE --apply here.

CONSOLIDATOR-SAFE WRITE (2026-06-23 — supersedes the full-``_index`` overwrite).
Live sports MTDS + backfill VMs write per-VM shards that the manifest
consolidator (Cloud Run cron) merges into the canonical
``_index/availability_index.parquet`` every minute. A full-index overwrite would
RACE that merge and drop live rows written since this script's read (the
pre-migration-drain HARD RULE). Instead ``--apply`` writes ONLY the relabeled
rows as a **per-VM shard** at ``_index/per_vm/{VM_NAME}.parquet`` (the canonical
fleet write path). The consolidator's DuckDB last-write-wins merge
(``PARTITION BY (date, venue, data_type, service_name, <dims…>) ORDER BY
attempted_at DESC NULLS LAST, written_at DESC NULLS LAST``) collapses each
(canonical row, shard relabeled row) pair into one group and picks the shard row
because its ``attempted_at`` is fresher → the relabel WINS for its keys without
touching the canonical blob. The shard carries the SAME dedup-key dims as the
canonical rows it replaces. Untouched cells (captured / in-coverage failures /
expected_unattempted) have different keys / are never in the shard → never lost.
SSOT: ``codex/05-infrastructure/manifest-consolidator-ssot.md`` § "Merge engine".

DRY-RUN by default: prints count to re-label + projected attempted_failed% per
entity (before/after) + projected wasted-fetch reduction, writes the projection
to ``_index/audit/``. ``--apply`` requires ``MANIFEST_PER_VM_SHARDS=true`` +
``VM_NAME=<unique>`` and writes the relabeled rows as that VM's per-VM shard.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    MANIFEST_PER_VM_SHARDS=true VM_NAME=relabel-sports-nocov-$(date +%s) \
    .venv/bin/python scripts/relabel_sports_no_provider_coverage_2026_06_21.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_api_contracts.registry import LEAGUE_ENTITY_COVERAGE_ENTITIES, is_league_entity_covered
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("relabel_sports_no_provider_coverage")

_MANIFEST_BLOB = "_index/availability_index.parquet"
# Per-VM shard path template — matches the canonical fleet write path
# ``unified_trading_library.manifest_writer._state._PER_VM_PATH_TEMPLATE`` so the
# consolidator lists + merges this shard exactly like a writer-VM's shard.
_PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
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


def relabel(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object], pd.Series]:
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
    return out, stats, mask


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the relabeled index back (snapshots first).")
    args = p.parse_args(argv)

    # Fail loud if the env-short isn't set — otherwise resolve_bucket_name falls
    # back to the env-LESS name (stale bucket frozen 2026-06-08).
    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing — would resolve the stale env-less bucket.")
        return 1

    instance = os.environ.get("VM_NAME", "")  # noqa: qg-empty-fallback — optional flag, absent == disabled (falsy `not instance` check below)
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique>. The relabel is written as a "
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

    out, stats, mask = relabel(df)
    logger.info("Relabel stats:")
    for k, v in stats.items():
        logger.info("  %s = %s", k, v)

    # Audit projection (a separate, non-canonical blob — safe to write any time).
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    proj = f"{bucket}/_index/audit/projected_sports_no_provider_coverage_2026_06_21.parquet"
    with fs.open(proj, "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("Wrote projection -> gs://%s", proj)

    if not args.apply:
        logger.info("DRY RUN — live _index untouched. Re-run with --apply to write the per-VM shard.")
        return 0

    n = int(mask.sum())
    if n == 0:
        logger.info("Nothing to relabel — manifest already honest for out-of-coverage entity cells.")
        return 0

    # CONSOLIDATOR-SAFE APPLY: write ONLY the relabeled rows (carrying every
    # canonical dedup-key dim verbatim, with fresh attempted_at/written_at so the
    # consolidator's last-write-wins picks them) as this VM's per-VM shard. The
    # canonical blob is NEVER overwritten → no race with live writers.
    now_iso = datetime.now(UTC).isoformat()
    shard_df = out.loc[mask].copy()
    shard_df["attempted_at"] = now_iso
    if "written_at" in shard_df.columns:
        shard_df["written_at"] = now_iso

    shard_path = _PER_VM_PATH_TEMPLATE.format(instance=instance)
    sbuf = io.BytesIO()
    shard_df.to_parquet(sbuf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    sbuf.seek(0)
    with fs.open(f"{bucket}/{shard_path}", "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info(
        "APPLIED. Wrote %d relabeled rows as per-VM shard gs://%s/%s (consolidator merges next cycle).",
        n,
        bucket,
        shard_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
