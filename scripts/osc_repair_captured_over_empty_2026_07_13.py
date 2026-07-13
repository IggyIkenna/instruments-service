#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: the 21 oscillation-flipped atoms (2026-07-13) read captured in the
#   consolidated sports index across >=2 consolidator cycles (--verify green twice)
"""osc_repair_captured_over_empty_2026_07_13.py — re-stamp the 21 captured atoms
that the 2026-07-13 captured->empty_confirmed oscillation erased.

WHAT HAPPENED (diagnosed 2026-07-13):

1. The v2 sports enumerator's per-day source-rule gate (``is_expected_for_source``
   -> EXPECTED_PRE_SEASON / EXPECTED_POST_SEASON) emitted ``empty_confirmed``
   WITHOUT consulting capture evidence (fixed the same day: the enumerate_v2
   oscillation guard — a seeder never overrides a captured atom). The nightly
   ``expected-universe-v2-sports`` Cloud Run job (01:30 UTC) re-stamped those
   seeds daily with a fresh uniform ``written_at``.
2. The atoms' REAL captured rows carried ``service_name=market-tick-data-service``
   (written by the sports_manifest_canonicalisation E4 migration fleet), i.e. a
   DIFFERENT consolidator dedup group (service_name is a base dedup key), so the
   captured-outranks-recency dedup guard never saw the two rows side by side.
3. ``dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py`` then dropped
   every market-tick-data-service row that had an instruments-service identity
   twin — for these 21 atoms the twin was the enumerator's (dishonest)
   ``empty_confirmed`` seed, so the drop erased the only capture evidence.

The data objects are still on disk (footystats MATCHES at the plain
``league=`` shape; ODDS / PREDICTIONS under ``fetched_at_hour=<ts>/league=``).
This script verifies each atom's object BY CONTENT (download + parse + >=1 row)
and, only for verified atoms, calls ``record_captured`` with the on-disk
DataFrame at the canonical LEAGUE-grain identity
(``service_name=instruments-service``, venue/instrument dims blank) so the next
consolidator cycle collapses the ``empty_confirmed`` seed in the SAME dedup
group and captured wins durably (captured-outranks tie-break, UTL 2026-07-12).

Writes a per-VM shard only (VM_NAME=osc-repair-20260713) — the ``*/1``
consolidator cron absorbs it; NEVER hand-edits the consolidated index and NEVER
triggers a consolidator execution (prune-race issue 2026-07-13).

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/osc_repair_captured_over_empty_2026_07_13.py            # dry-run probe
    ... --apply    # record_captured + explicit .write() of the per-VM shard
    ... --verify   # post-consolidation: assert the atoms read captured by content
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
logger = logging.getLogger("osc_repair_captured_over_empty")

_VM_NAME = "osc-repair-20260713"


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="record_captured + write the per-VM shard")
    p.add_argument("--verify", action="store_true", help="verify the atoms read captured in the consolidated index")
    p.add_argument("--project", default="central-element-323112")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = _VM_NAME
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

from unified_api_contracts import PipelineMode
from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths
from unified_trading_library import (
    ManifestWriter,
    get_storage_client,
    resolve_bucket_name,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

#: The 21 atoms (data_type, league_id, date) whose captured rows were erased —
#: exact set measured from availability_index.20260713-160041.dereg_24_leagues.bak
#: (captured, service_name=market-tick-data-service, written 2026-07-13T06:2x)
#: vs the post-16:33 canonical (empty_confirmed only). All footystats-sourced.
_ATOMS: tuple[tuple[str, str, str], ...] = (
    ("MATCHES", "BRASILEIRAO", "2026-03-18"),
    ("ODDS", "BRASILEIRAO", "2026-03-18"),
    ("PREDICTIONS", "BRASILEIRAO", "2026-03-18"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-06"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-06"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-06"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-07"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-07"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-07"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-09"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-09"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-09"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-10"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-10"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-10"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-14"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-14"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-14"),
    ("MATCHES", "SEGUNDA_DIVISION", "2026-06-20"),
    ("ODDS", "SEGUNDA_DIVISION", "2026-06-20"),
    ("PREDICTIONS", "SEGUNDA_DIVISION", "2026-06-20"),
)


def _resolve_object(storage: object, data_type: str, league_id: str, day: str) -> tuple[str, pd.DataFrame] | None:
    """Return (blob_path, parsed_df) for the atom's on-disk object, else None.

    Probes every UAC ``candidate_parquet_paths`` candidate (batch_footystats
    pipeline_mode first); ``fetched_at_hour=*`` wildcards are resolved by prefix
    listing and the LATEST hour partition wins (latest-wins read convention).
    Content verification = parquet parses AND >=1 row.
    """
    cands = candidate_parquet_paths(data_type, day, league_id, pipeline_mode="batch_footystats")
    resolved: list[str] = []
    for cand in cands:
        if "fetched_at_hour=*" not in cand:
            resolved.append(cand)
            continue
        prefix, suffix = cand.split("fetched_at_hour=*", 1)
        hits = sorted(
            str(getattr(b, "name", b))
            for b in storage.list_blobs(BUCKET, prefix=prefix)  # pyright: ignore[reportAttributeAccessIssue]
            if str(getattr(b, "name", b)).endswith(suffix.lstrip("/"))
            and "fetched_at_hour=" in str(getattr(b, "name", b))
        )
        resolved.extend(reversed(hits))  # latest hour partition first
    for path in resolved:
        try:
            if not storage.blob_exists(BUCKET, path):  # pyright: ignore[reportAttributeAccessIssue]
                continue
            raw = storage.download_bytes(BUCKET, path)  # pyright: ignore[reportAttributeAccessIssue]
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("probe %s: unreadable (%s) — trying next candidate", path, exc)
            continue
        if len(df) >= 1:
            return path, df
        logger.warning("probe %s: exists but 0 rows — not capture evidence", path)
    return None


def run_repair(apply: bool) -> int:
    storage = get_storage_client(project_id=ARGS.project)
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    verified = 0
    unverified: list[tuple[str, str, str]] = []
    for data_type, league_id, day in _ATOMS:
        hit = _resolve_object(storage, data_type, league_id, day)
        if hit is None:
            unverified.append((data_type, league_id, day))
            logger.error("UNVERIFIED %s %s %s — no readable on-disk object; NOT stamping", data_type, league_id, day)
            continue
        path, df = hit
        verified += 1
        logger.info("VERIFIED %s %s %s -> %s (%d rows)", data_type, league_id, day, path, len(df))
        if not apply:
            continue
        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
            row_key={"date": day, "data_type": data_type, "league_id": league_id},
            df=df,
            asset_group="sports",
            instrument_type="",
            data_type=data_type,
            league_id=league_id,
            pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
            source="footystats",
            service_emission_state=None,
        )
    logger.info("Probe summary: %d/%d atoms verified on disk; %d unverified", verified, len(_ATOMS), len(unverified))
    if not apply:
        logger.info("DRY-RUN — nothing written. Pass --apply to record_captured + write the per-VM shard.")
        return 0 if not unverified else 2
    if verified:
        manifest.write()
        logger.info(
            "Wrote per-VM shard for VM_NAME=%s (%d captured rows). The */1 consolidator cron absorbs it; "
            "run --verify after >=1 cycle.",
            _VM_NAME,
            verified,
        )
    return 0 if not unverified else 2


def run_verify() -> int:
    """Content-verify: latest row per atom in the consolidated index must be captured."""
    storage = get_storage_client(project_id=ARGS.project)
    raw = storage.download_bytes(BUCKET, "_index/availability_index.parquet")
    idx = pd.read_parquet(io.BytesIO(raw))
    idx = idx.sort_values(["attempted_at", "written_at"], kind="stable", na_position="first")
    bad = 0
    for data_type, league_id, day in _ATOMS:
        rows = idx[
            (idx["data_type"].astype(str) == data_type)
            & (idx["league_id"].fillna("").astype(str) == league_id)
            & (idx["date"].astype(str) == day)
        ]
        if rows.empty:
            logger.error("VERIFY %s %s %s: NO rows in index", data_type, league_id, day)
            bad += 1
            continue
        winner = rows.iloc[-1]
        statuses = set(rows["capture_status"].astype(str))
        if str(winner["capture_status"]) != "captured":
            logger.error(
                "VERIFY %s %s %s: winner=%s (statuses=%s) — NOT captured",
                data_type,
                league_id,
                day,
                winner["capture_status"],
                sorted(statuses),
            )
            bad += 1
        else:
            logger.info("VERIFY %s %s %s: captured ✓ (row_count=%s)", data_type, league_id, day, winner["row_count"])
    logger.info("Verify summary: %d/%d atoms captured", len(_ATOMS) - bad, len(_ATOMS))
    return 0 if bad == 0 else 1


def main() -> int:
    run_ts = datetime.now(UTC).isoformat()
    logger.info("osc repair run at %s (bucket=%s, apply=%s, verify=%s)", run_ts, BUCKET, ARGS.apply, ARGS.verify)
    if ARGS.verify:
        return run_verify()
    return run_repair(ARGS.apply)


if __name__ == "__main__":
    sys.exit(main())
