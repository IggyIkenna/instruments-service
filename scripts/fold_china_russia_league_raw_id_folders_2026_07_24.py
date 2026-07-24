#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: after the fold below is confirmed applied in prod (verify via a manifest census for
#   league_id in (169, 235) returning 0 rows)
"""Fold the 12 non-canonical `league=<raw_af_id>` fixtures_schedule shards for CHINA_SUPER_LEAGUE (af_id=169)
and RUSSIA_PREMIER_LEAGUE (af_id=235) into their canonical `league=<CANONICAL_ID>` paths.

Per `sports_fixtures_schedule_noncanonical_raw_league_id_folders_2026_07_24.md`'s corpus-wide census: of 485
non-canonical numeric league_ids, only these 2 are now resolvable via `get_league_by_api_football_id()` (the
other 483 are a separate, already-diagnosed-and-fixed write-side leak closed by the 2026-06-27
`_is_in_canonical_write_universe()` gate — NOT touched here). These 2 leagues' writes predate the registry
adding them (`unified-api-contracts@beec78aa`, 2026-07-21) — a registry-growth timing-lag, self-healing on any
write AFTER that date, leaving only the pre-existing shards stranded under the raw-id path.

Verified live (this script, dry-run) before any `--apply`: NO canonical sibling exists for ANY of the 12
(date, league) pairs below — this is a pure MOVE, never a merge-by-af_fixture_id.

Flow per shard (hard-rule compliant, mirrors `dereg_rekey_la_liga_2_2026_07_13.py`):
  1. describe source (raw-id) object;
  2. describe canonical target: ABSENT -> copy; IDENTICAL -> skip-copy; DIFFERENT -> HARD-ABORT that shard only
     (never overwrite; the pure-move premise is violated for that pair, needs its own look);
  3. gcs_copy_object + re-describe both sides, verify size+crc32c equality;
  4. download the canonical object, verify it parses + non-empty;
  5. record_captured under the canonical league_id via a single shared manifest-writer per-VM shard
     (VM_NAME=league-fold-20260724, MANIFEST_PER_VM_SHARDS=true), one write per shard, ONE explicit close()
     after the loop;
  6. verify the per-VM shard object exists + carries every row.

ONLY THEN (this bucket has NO soft-delete — retentionDurationSeconds=0, confirmed live, unlike the
market-data-tick bucket precedent — so an explicit backup, not soft-delete, is the safety net):
  7. gcs_copy_object the raw-id object to a `_purge_backups/` snapshot path;
  8. verify the backup matches (size+crc32c);
  9. gcs_delete_object the raw-id original;
  10. verify it is gone (gcs_describe_object returns None).

Post-run: verify `league=169` and `league=235` are completely empty across all 12 dates.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    VM_NAME=league-fold-20260724 MANIFEST_PER_VM_SHARDS=true \
    .venv/bin/python scripts/fold_china_russia_league_raw_id_folders_2026_07_24.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from dataclasses import dataclass

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_trading_library import (
    GcsEventSink,
    ManifestWriter,
    get_storage_client,
    resolve_bucket_name,
    setup_events,
)
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_copy_object,
    gcs_delete_object,
    gcs_describe_object,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fold_china_russia_league_raw_id_folders")

_ENTITY = "fixtures_schedule"
_DATA_TYPE = "FIXTURES_SCHEDULE"
_SOURCE = "api_football"
_SHARD_INSTANCE = "league-fold-20260724"
_PER_VM_SHARD_PATH = f"_index/per_vm/{_SHARD_INSTANCE}.parquet"
_BACKUP_PREFIX = "sports_reference/_purge_backups/2026_07_24_league_fold"


@dataclass(frozen=True)
class Shard:
    date: str
    raw_league_id: str
    canonical_league_id: str


SHARDS: tuple[Shard, ...] = (
    # CHINA_SUPER_LEAGUE (af_league_id=169) — 11 dates
    *(
        Shard(date=d, raw_league_id="169", canonical_league_id="CHINA_SUPER_LEAGUE")
        for d in (
            "2026-05-05",
            "2026-05-06",
            "2026-05-19",
            "2026-05-20",
            "2026-05-29",
            "2026-05-30",
            "2026-06-26",
            "2026-06-27",
            "2026-06-28",
            "2026-07-03",
            "2026-07-04",
        )
    ),
    # RUSSIA_PREMIER_LEAGUE (af_league_id=235) — 1 date
    Shard(date="2026-05-20", raw_league_id="235", canonical_league_id="RUSSIA_PREMIER_LEAGUE"),
)


def _obj_path(date: str, league: str) -> str:
    return (
        f"sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/"
        f"entity={_ENTITY}/league={league}/{_ENTITY}.parquet"
    )


def _fold_one(bucket: str, shard: Shard, writer: ManifestWriter | None, *, apply: bool) -> str:
    """Returns a one-word outcome token: copied|skipped_identical|aborted|dry_run."""
    src_uri = f"gs://{bucket}/{_obj_path(shard.date, shard.raw_league_id)}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    tgt_uri = f"gs://{bucket}/{_obj_path(shard.date, shard.canonical_league_id)}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name

    src_meta = gcs_describe_object(src_uri)
    if src_meta is None:
        logger.error("%s: source object missing — %s", shard.date, src_uri)
        return "aborted"
    logger.info("%s: SOURCE size=%s crc32c=%s", shard.date, src_meta.size, src_meta.crc32c)

    tgt_meta = gcs_describe_object(tgt_uri)
    if tgt_meta is not None:
        if (tgt_meta.size, tgt_meta.crc32c) == (src_meta.size, src_meta.crc32c):
            logger.info("%s: TARGET already identical — skip-copy.", shard.date)
        else:
            logger.error(
                "%s: HARD-ABORT — canonical target exists with DIFFERENT content "
                "(size=%s crc32c=%s vs source size=%s crc32c=%s); this pair is NOT a pure move, skipping.",
                shard.date,
                tgt_meta.size,
                tgt_meta.crc32c,
                src_meta.size,
                src_meta.crc32c,
            )
            return "aborted"
    elif apply:
        gcs_copy_object(src_uri, tgt_uri)
        logger.info("%s: COPIED %s -> %s", shard.date, src_uri, tgt_uri)
    else:
        logger.info("%s: DRY-RUN — would copy %s -> %s", shard.date, src_uri, tgt_uri)
        return "dry_run"

    if not apply:
        # Reached only via the "already identical" branch above — no write happened, but the
        # remaining steps (record_captured, backup, delete) are write operations and must never
        # run in dry-run mode regardless of which branch got here.
        logger.info("%s: DRY-RUN — target already identical, would proceed to manifest+backup+delete.", shard.date)
        return "dry_run"

    # Post-copy parity verification.
    src_meta2 = gcs_describe_object(src_uri)
    tgt_meta2 = gcs_describe_object(tgt_uri)
    if (
        src_meta2 is None
        or tgt_meta2 is None
        or (src_meta2.size, src_meta2.crc32c)
        != (
            tgt_meta2.size,
            tgt_meta2.crc32c,
        )
    ):
        logger.error("%s: HARD-ABORT — post-copy parity check failed.", shard.date)
        return "aborted"
    logger.info("%s: VERIFIED copy parity (size=%s crc32c=%s).", shard.date, tgt_meta2.size, tgt_meta2.crc32c)

    storage = get_storage_client()
    df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, _obj_path(shard.date, shard.canonical_league_id))))
    logger.info("%s: canonical object parses — %d rows.", shard.date, len(df))
    if df.empty:
        logger.error("%s: HARD-ABORT — canonical object parsed to 0 rows.", shard.date)
        return "aborted"

    assert writer is not None, "writer is only None in dry-run mode, which always returns before this point"
    writer.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": shard.date, "data_type": _DATA_TYPE, "league_id": shard.canonical_league_id},
        df=df,
        asset_group="sports",
        instrument_type="",
        data_type=_DATA_TYPE,
        league_id=shard.canonical_league_id,
        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        source=_SOURCE,
        service_emission_state=None,
    )

    # Backup-then-delete the raw-id original — this bucket has NO soft-delete
    # (retentionDurationSeconds=0, confirmed live), so an explicit copy is the only recovery net.
    backup_uri = f"gs://{bucket}/{_BACKUP_PREFIX}/day={shard.date}/league={shard.raw_league_id}/{_ENTITY}.parquet"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    gcs_copy_object(src_uri, backup_uri)
    backup_meta = gcs_describe_object(backup_uri)
    if backup_meta is None or (backup_meta.size, backup_meta.crc32c) != (src_meta2.size, src_meta2.crc32c):
        logger.error("%s: HARD-ABORT — backup copy verification failed, NOT deleting the raw-id original.", shard.date)
        return "aborted"
    logger.info("%s: BACKED UP raw-id original to %s", shard.date, backup_uri)

    gcs_delete_object(src_uri)
    if gcs_describe_object(src_uri) is not None:
        logger.error("%s: HARD-ABORT — raw-id original still present after delete.", shard.date)
        return "aborted"
    logger.info("%s: DELETED raw-id original %s", shard.date, src_uri)
    return "copied"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Default dry-run. Pass to actually copy/record/delete.")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")

    if not args.apply:
        outcomes = [_fold_one(bucket, s, writer=None, apply=False) for s in SHARDS]
        logger.info("DRY-RUN complete — %d shard(s), no writes performed. Outcomes: %s", len(SHARDS), outcomes)
        return 0

    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(
            project_id="central-element-323112",
            bucket="central-element-323112-events",
            service_name="instruments-service",
        ),
    )

    writer = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket, per_vm_shards=True)
    outcomes = [_fold_one(bucket, s, writer=writer, apply=True) for s in SHARDS]
    writer.close()
    logger.info("Manifest writer closed (per-VM shard mode).")

    aborted = [s.date for s, o in zip(SHARDS, outcomes, strict=True) if o == "aborted"]
    if aborted:
        logger.error("HARD-ABORT on %d shard(s): %s — inspect before re-running.", len(aborted), aborted)
        return 3

    storage = get_storage_client()
    if not storage.blob_exists(bucket, _PER_VM_SHARD_PATH):
        logger.error("HARD-ABORT: per-VM shard %s not found after close().", _PER_VM_SHARD_PATH)
        return 3
    shard_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, _PER_VM_SHARD_PATH)))
    for s in SHARDS:
        match = shard_df[
            (shard_df["league_id"] == s.canonical_league_id)
            & (shard_df["date"].astype(str) == s.date)
            & (shard_df["data_type"] == _DATA_TYPE)
            & (shard_df["capture_status"] == "captured")
        ]
        if match.empty:
            logger.error("HARD-ABORT: %s/%s row not present in the per-VM shard.", s.date, s.canonical_league_id)
            return 3
    logger.info("Per-VM shard %s: all %d row(s) verified present.", _PER_VM_SHARD_PATH, len(SHARDS))

    # Post-run: confirm the raw-id folders are completely empty across every affected date.
    remaining = []
    for raw_id in {"169", "235"}:
        for s in SHARDS:
            if s.raw_league_id != raw_id:
                continue
            if gcs_describe_object(f"gs://{bucket}/{_obj_path(s.date, raw_id)}") is not None:  # noqa: gs-uri — one-off script
                remaining.append((s.date, raw_id))
    if remaining:
        logger.error("HARD-ABORT: raw-id objects still present: %s", remaining)
        return 3

    logger.info(
        "FOLD COMPLETE — %d/%d shard(s) copied+recorded+deleted, 0 remaining raw-id objects.", len(SHARDS), len(SHARDS)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
