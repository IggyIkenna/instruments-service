#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: after the fold below is confirmed applied in prod (verify via a manifest census for
#   data_type IN (FIXTURES, FIXTURES_OUTCOMES) AND league_id IN (169, 235) returning 0 rows)
"""Fold the 21 non-canonical `league=<raw_af_id>` `entity=fixtures`/`entity=fixtures_outcomes` shards for
CHINA_SUPER_LEAGUE (af_id=169) and RUSSIA_PREMIER_LEAGUE (af_id=235) into their canonical
`league=<CANONICAL_ID>` paths.

Sibling of `fold_china_russia_league_raw_id_folders_2026_07_24.py` (already applied,
`instruments-service@4412e576`, `entity=fixtures_schedule` only) — same root cause
(`_canonical_league_id()`'s registry-lookup-miss fallback, `sports.py:57-85`, these 2 leagues' writes
predate `unified-api-contracts@beec78aa` adding them to the registry 2026-07-21), same recipe, different
entities. Per `sports_fixtures_schedule_noncanonical_raw_league_id_folders_2026_07_24.md`'s follow-up todo
("NEW 2026-07-24, slot 9"): the sibling `entity=fixtures`/`entity=fixtures_outcomes` objects for the SAME 2
leagues were deliberately left out of that fold's scope, tracked here instead.

Population (single manifest read, 2026-07-24, `instruments-store-sports-prd-central-element-323112`,
5,500,675 total rows, filtered to `data_type IN (FIXTURES, FIXTURES_OUTCOMES)` AND
`league_id IN (169, 235)`): 21 rows — 12 `FIXTURES` (11 dates for 169 + 1 for 235, the SAME 12 dates as the
already-folded `fixtures_schedule` cohort) + 9 `FIXTURES_OUTCOMES` (8 dates for 169 + 1 for 235 — 3 fewer
than `FIXTURES`, consistent with `entity=fixtures_outcomes` only existing for completed fixtures, not every
scheduled one).

Verified live (this script, dry-run) before any `--apply`: NO canonical sibling exists for ANY of the 21
(date, entity, league) pairs below — this is a pure MOVE, never a merge-by-af_fixture_id.

Flow per shard (identical to the fixtures_schedule precedent — hard-rule compliant):
  1. describe source (raw-id) object;
  2. describe canonical target: ABSENT -> copy; IDENTICAL -> skip-copy; DIFFERENT -> HARD-ABORT that shard only
     (never overwrite; the pure-move premise is violated for that pair, needs its own look);
  3. gcs_copy_object + re-describe both sides, verify size+crc32c equality;
  4. download the canonical object, verify it parses + non-empty;
  5. record_captured under the canonical league_id via a single shared manifest-writer per-VM shard
     (VM_NAME=league-fold-fixtures-siblings-20260724, MANIFEST_PER_VM_SHARDS=true), one write per shard,
     ONE explicit close() after the loop;
  6. verify the per-VM shard object exists + carries every row.

ONLY THEN (this bucket has NO soft-delete — retentionDurationSeconds=0, confirmed for this bucket by the
fixtures_schedule precedent — so an explicit backup, not soft-delete, is the safety net):
  7. gcs_copy_object the raw-id object to a `_purge_backups/` snapshot path;
  8. verify the backup matches (size+crc32c);
  9. gcs_delete_object the raw-id original;
  10. verify it is gone (gcs_describe_object returns None).

Post-run: verify `league=169` and `league=235` are completely empty across all 21 (date, entity) pairs.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    VM_NAME=league-fold-fixtures-siblings-20260724 MANIFEST_PER_VM_SHARDS=true \
    .venv/bin/python scripts/fold_china_russia_league_raw_id_folders_fixtures_siblings_2026_07_24.py [--apply]
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
logger = logging.getLogger("fold_china_russia_league_raw_id_folders_fixtures_siblings")

_SOURCE = "api_football"
_SHARD_INSTANCE = "league-fold-fixtures-siblings-20260724"
_PER_VM_SHARD_PATH = f"_index/per_vm/{_SHARD_INSTANCE}.parquet"
_BACKUP_PREFIX = "sports_reference/_purge_backups/2026_07_24_league_fold_fixtures_siblings"

# entity -> manifest data_type (the two names diverge; entity is the GCS partition/filename stem,
# data_type is the manifest column value).
_ENTITY_TO_DATA_TYPE = {"fixtures": "FIXTURES", "fixtures_outcomes": "FIXTURES_OUTCOMES"}


@dataclass(frozen=True)
class Shard:
    date: str
    entity: str
    raw_league_id: str
    canonical_league_id: str

    @property
    def data_type(self) -> str:
        return _ENTITY_TO_DATA_TYPE[self.entity]


_CHINA_DATES_FIXTURES = (
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
_CHINA_DATES_OUTCOMES = (
    "2026-05-05",
    "2026-05-06",
    "2026-05-19",
    "2026-05-20",
    "2026-05-29",
    "2026-05-30",
    "2026-06-26",
    "2026-06-27",
)

SHARDS: tuple[Shard, ...] = (
    *(
        Shard(date=d, entity="fixtures", raw_league_id="169", canonical_league_id="CHINA_SUPER_LEAGUE")
        for d in _CHINA_DATES_FIXTURES
    ),
    Shard(date="2026-05-20", entity="fixtures", raw_league_id="235", canonical_league_id="RUSSIA_PREMIER_LEAGUE"),
    *(
        Shard(date=d, entity="fixtures_outcomes", raw_league_id="169", canonical_league_id="CHINA_SUPER_LEAGUE")
        for d in _CHINA_DATES_OUTCOMES
    ),
    Shard(
        date="2026-05-20", entity="fixtures_outcomes", raw_league_id="235", canonical_league_id="RUSSIA_PREMIER_LEAGUE"
    ),
)
assert len(SHARDS) == 21, f"expected 21 shards per the census, got {len(SHARDS)}"


def _obj_path(date: str, entity: str, league: str) -> str:
    return f"sports_reference/by_date/day={date}/pipeline_mode=batch_api_football/entity={entity}/league={league}/{entity}.parquet"


def _fold_one(bucket: str, shard: Shard, writer: ManifestWriter | None, *, apply: bool) -> str:
    """Returns a one-word outcome token: copied|skipped_identical|aborted|dry_run."""
    src_uri = f"gs://{bucket}/{_obj_path(shard.date, shard.entity, shard.raw_league_id)}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    tgt_uri = f"gs://{bucket}/{_obj_path(shard.date, shard.entity, shard.canonical_league_id)}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name

    src_meta = gcs_describe_object(src_uri)
    if src_meta is None:
        logger.error("%s/%s: source object missing — %s", shard.date, shard.entity, src_uri)
        return "aborted"
    logger.info("%s/%s: SOURCE size=%s crc32c=%s", shard.date, shard.entity, src_meta.size, src_meta.crc32c)

    tgt_meta = gcs_describe_object(tgt_uri)
    if tgt_meta is not None:
        if (tgt_meta.size, tgt_meta.crc32c) == (src_meta.size, src_meta.crc32c):
            logger.info("%s/%s: TARGET already identical — skip-copy.", shard.date, shard.entity)
        else:
            logger.error(
                "%s/%s: HARD-ABORT — canonical target exists with DIFFERENT content "
                "(size=%s crc32c=%s vs source size=%s crc32c=%s); this pair is NOT a pure move, skipping.",
                shard.date,
                shard.entity,
                tgt_meta.size,
                tgt_meta.crc32c,
                src_meta.size,
                src_meta.crc32c,
            )
            return "aborted"
    elif apply:
        gcs_copy_object(src_uri, tgt_uri)
        logger.info("%s/%s: COPIED %s -> %s", shard.date, shard.entity, src_uri, tgt_uri)
    else:
        logger.info("%s/%s: DRY-RUN — would copy %s -> %s", shard.date, shard.entity, src_uri, tgt_uri)
        return "dry_run"

    if not apply:
        # Reached only via the "already identical" branch above — no write happened, but the
        # remaining steps (record_captured, backup, delete) are write operations and must never
        # run in dry-run mode regardless of which branch got here.
        logger.info(
            "%s/%s: DRY-RUN — target already identical, would proceed to manifest+backup+delete.",
            shard.date,
            shard.entity,
        )
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
        logger.error("%s/%s: HARD-ABORT — post-copy parity check failed.", shard.date, shard.entity)
        return "aborted"
    logger.info(
        "%s/%s: VERIFIED copy parity (size=%s crc32c=%s).", shard.date, shard.entity, tgt_meta2.size, tgt_meta2.crc32c
    )

    storage = get_storage_client()
    df = pd.read_parquet(
        io.BytesIO(storage.download_bytes(bucket, _obj_path(shard.date, shard.entity, shard.canonical_league_id)))
    )
    logger.info("%s/%s: canonical object parses — %d rows.", shard.date, shard.entity, len(df))
    if df.empty:
        logger.error("%s/%s: HARD-ABORT — canonical object parsed to 0 rows.", shard.date, shard.entity)
        return "aborted"

    assert writer is not None, "writer is only None in dry-run mode, which always returns before this point"
    writer.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": shard.date, "data_type": shard.data_type, "league_id": shard.canonical_league_id},
        df=df,
        asset_group="sports",
        instrument_type="",
        data_type=shard.data_type,
        league_id=shard.canonical_league_id,
        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        source=_SOURCE,
        service_emission_state=None,
    )

    # Backup-then-delete the raw-id original — this bucket has NO soft-delete
    # (retentionDurationSeconds=0, confirmed for this bucket by the fixtures_schedule precedent), so an
    # explicit copy is the only recovery net.
    backup_uri = f"gs://{bucket}/{_BACKUP_PREFIX}/day={shard.date}/entity={shard.entity}/league={shard.raw_league_id}/{shard.entity}.parquet"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    gcs_copy_object(src_uri, backup_uri)
    backup_meta = gcs_describe_object(backup_uri)
    if backup_meta is None or (backup_meta.size, backup_meta.crc32c) != (src_meta2.size, src_meta2.crc32c):
        logger.error(
            "%s/%s: HARD-ABORT — backup copy verification failed, NOT deleting the raw-id original.",
            shard.date,
            shard.entity,
        )
        return "aborted"
    logger.info("%s/%s: BACKED UP raw-id original to %s", shard.date, shard.entity, backup_uri)

    gcs_delete_object(src_uri)
    if gcs_describe_object(src_uri) is not None:
        logger.error("%s/%s: HARD-ABORT — raw-id original still present after delete.", shard.date, shard.entity)
        return "aborted"
    logger.info("%s/%s: DELETED raw-id original %s", shard.date, shard.entity, src_uri)
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

    aborted = [(s.date, s.entity) for s, o in zip(SHARDS, outcomes, strict=True) if o == "aborted"]
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
            & (shard_df["data_type"] == s.data_type)
            & (shard_df["capture_status"] == "captured")
        ]
        if match.empty:
            logger.error(
                "HARD-ABORT: %s/%s/%s row not present in the per-VM shard.", s.date, s.entity, s.canonical_league_id
            )
            return 3
    logger.info("Per-VM shard %s: all %d row(s) verified present.", _PER_VM_SHARD_PATH, len(SHARDS))

    # Post-run: confirm the raw-id folders are completely empty across every affected (date, entity).
    remaining = []
    for s in SHARDS:
        if gcs_describe_object(f"gs://{bucket}/{_obj_path(s.date, s.entity, s.raw_league_id)}") is not None:  # noqa: gs-uri — one-off script
            remaining.append((s.date, s.entity, s.raw_league_id))
    if remaining:
        logger.error("HARD-ABORT: raw-id objects still present: %s", remaining)
        return 3

    logger.info(
        "FOLD COMPLETE — %d/%d shard(s) copied+recorded+deleted, 0 remaining raw-id objects.", len(SHARDS), len(SHARDS)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
