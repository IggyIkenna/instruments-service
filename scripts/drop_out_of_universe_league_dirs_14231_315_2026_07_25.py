#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: --verify-only confirms 0 remaining GCS objects and 0 remaining manifest rows for
#   league_id in (14231, 315) across >=2 consolidator cycles.
"""Drop the 2 out-of-universe numeric `league=` GCS partitions (14231, 315), snapshot-first.

Context: `sports_canonical_universe_and_apifootball_reference_expansion_2026_06_24.md` flagged 2
non-canonical numeric `league=` GCS partitions written before the `_is_in_canonical_write_universe`
write-gate landed (instruments-service@0345ffc). Neither id resolves via
`get_league_by_api_football_id()` in the current UAC league registry (re-verified live below,
hard-abort guard) — they are genuinely out-of-universe, not mis-keyed leagues awaiting a fold (no
canonical twin exists or could exist for either id).

`315` was separately de-registered from the manifest by `dereg_purge_24_leagues_2026_07_13.py`, but
that script explicitly left GCS objects untouched ("GCS data objects are NOT touched by this
script"). `14231` was never covered by any prior script. This script is the first to touch the raw
GCS objects for either id.

Scope, verified live (2026-07-25): a bounded listing over `sports_reference/by_date/day=2025-` and
`day=2026-` (NOT a whole-corpus walk — the 2026-06-24 audit already verified zero in-universe-numeric
partitions across 2015-2024; these two year-prefixes are where the 2 ids were actually found,
including 2026 dates the 2026-06-24 audit predates) found 197 objects with a genuine
`/league=14231/` or `/league=315/` path segment:
  - 175 objects across 2025 (mostly `footystats_matches` under league=14231, `injuries` under
    league=315)
  - 22 objects across 2026 (injuries + the fixtures/fixtures_outcomes/fixtures_schedule split,
    league=315 only)
A separate candidate-path cross-check against the manifest's 166 captured rows for these league_ids
confirmed every manifest row is backed by a real object, and deliberately EXCLUDED 2 bare
`entity=injuries/injuries.parquet` fallback objects (2026-04-04, 2026-04-11) that also matched a
candidate-path guess for league_id=315 — those are shared bare/legacy-layout files that can carry
OTHER leagues' data too (a separate "eliminate the bare/legacy dual-layout" todo), never in scope
here; only genuine `/league=<id>/` sub-partitioned objects are touched.

Flow (mirrors `fold_china_russia_league_raw_id_folders_2026_07_24.py`'s backup idiom, minus the
fold/manifest-record step since this is a pure drop with no canonical twin):
  1. Re-verify live that neither id resolves via the UAC registry (hard-abort if either now does —
     that would make this a fold candidate, not a drop).
  2. Per GCS object: describe -> copy to `_purge_backups/2026_07_25_drop_14231_315/<path>` -> verify
     backup parity (size+crc32c) -> delete -> verify gone. This bucket has NO soft-delete
     (retentionDurationSeconds=0, per the fold-script precedent) — the backup copy is the only
     recovery net.
  3. Manifest: CAS-safe (generation-preconditioned) backup-then-rewrite of
     `_index/availability_index.parquet` dropping rows where `league_id` in (14231, 315), retrying
     on a lost CAS race against the live consolidator cron.
  4. `--verify-only`: re-run the bounded listing + manifest check, confirm both are zero. Run at
     least twice, >=1 consolidator cycle apart.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    .venv/bin/python scripts/drop_out_of_universe_league_dirs_14231_315_2026_07_25.py --dry-run
  ... --apply
  ... --verify-only   # run at least twice, >=1 consolidator cycle apart
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import get_league_by_api_football_id
from unified_trading_library import (
    StorageClient,
    gcs_copy_object,
    gcs_delete_object,
    gcs_describe_object,
    get_storage_client,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("drop_out_of_universe_league_dirs_14231_315")

INDEX_BLOB = "_index/availability_index.parquet"
BACKUP_PREFIX = "sports_reference/_purge_backups/2026_07_25_drop_14231_315"

#: Bounded prefix scan — NOT a whole-corpus walk. The 2026-06-24 audit already verified zero
#: in-universe-numeric `league=` partitions across 2015-2024; these are the only two year-prefixes
#: where 14231/315 were actually found live (see module docstring).
SCAN_PREFIXES = ("sports_reference/by_date/day=2025-", "sports_reference/by_date/day=2026-")

#: Raw numeric league ids confirmed out-of-universe — no `api_football_id` registry entry.
TARGET_LEAGUE_IDS = ("14231", "315")

MAX_CAS_ATTEMPTS = 5


def _league_segment(lid: str) -> str:
    return f"/league={lid}/"


def _assert_still_out_of_universe() -> None:
    """Hard-abort if either id has since been registered — that makes this a FOLD, not a drop."""
    for lid in TARGET_LEAGUE_IDS:
        league = get_league_by_api_football_id(int(lid))
        if league is not None:
            logger.error(
                "HARD-ABORT: api_football_id=%s now resolves to %s in the UAC registry — this is a "
                "FOLD candidate now (mirror fold_china_russia_league_raw_id_folders_2026_07_24.py), "
                "not an out-of-universe drop. Refusing to run.",
                lid,
                league.league_id,
            )
            raise SystemExit(3)
        logger.info("Confirmed still out-of-universe: api_football_id=%s has no UAC registry entry.", lid)


def _find_objects(bucket: str, storage: StorageClient) -> list[str]:
    """Bounded listing over SCAN_PREFIXES, filtered to genuine `league=<id>` sub-partitions only —
    excludes bare/shared fallback objects that could carry other leagues' data too."""
    found: set[str] = set()
    for prefix in SCAN_PREFIXES:
        for blob in storage.list_blobs(bucket=bucket, prefix=prefix):
            name = str(blob.name)
            if any(_league_segment(lid) in name for lid in TARGET_LEAGUE_IDS):
                found.add(name)
    return sorted(found)


def _purge_gcs_objects(bucket: str, objects: list[str], *, apply: bool) -> int:
    if not apply:
        logger.info("DRY-RUN — would back up + delete %d GCS object(s).", len(objects))
        return 0

    n_deleted = 0
    for name in objects:
        src_uri = f"gs://{bucket}/{name}"  # noqa: gs-uri — one-off script, bucket via resolve_bucket_name
        src_meta = gcs_describe_object(src_uri)
        if src_meta is None:
            logger.warning("%s: already gone — skipping.", name)
            continue

        backup_uri = f"gs://{bucket}/{BACKUP_PREFIX}/{name}"  # noqa: gs-uri — one-off script
        gcs_copy_object(src_uri, backup_uri)
        backup_meta = gcs_describe_object(backup_uri)
        if backup_meta is None or (backup_meta.size, backup_meta.crc32c) != (src_meta.size, src_meta.crc32c):
            logger.error("%s: HARD-ABORT — backup verification failed, NOT deleting.", name)
            raise SystemExit(3)

        gcs_delete_object(src_uri)
        if gcs_describe_object(src_uri) is not None:
            logger.error("%s: HARD-ABORT — object still present after delete.", name)
            raise SystemExit(3)

        n_deleted += 1
        logger.info("%s: backed up -> %s, deleted (%d/%d).", name, backup_uri, n_deleted, len(objects))

    logger.info("GCS purge complete: %d/%d object(s) deleted.", n_deleted, len(objects))
    return n_deleted


def _purge_manifest_rows(bucket: str, storage: StorageClient, *, apply: bool) -> int:
    for attempt in range(1, MAX_CAS_ATTEMPTS + 1):
        raw, generation = storage.download_bytes_with_generation(bucket, INDEX_BLOB)
        if raw is None:
            logger.error("HARD-ABORT: canonical index absent at %s/%s", bucket, INDEX_BLOB)
            raise SystemExit(3)

        df = pd.read_parquet(io.BytesIO(raw))
        mask = df["league_id"].fillna("").astype(str).isin(TARGET_LEAGUE_IDS)
        n_drop = int(mask.sum())
        logger.info(
            "[attempt %d] index generation=%d: %d row(s) for league_id in %s (of %d total).",
            attempt,
            generation,
            n_drop,
            TARGET_LEAGUE_IDS,
            len(df),
        )
        if n_drop == 0:
            logger.info("No manifest rows remain for %s — nothing to purge.", TARGET_LEAGUE_IDS)
            return 0

        kept = df.loc[~mask].copy()

        if not apply:
            logger.info("DRY-RUN — would drop %d row(s), rewrite %d row(s). Pass --apply to write.", n_drop, len(kept))
            return n_drop

        run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        backup_blob = f"{INDEX_BLOB[:-8]}.{run_ts}.drop_14231_315.bak.parquet"
        backup_gen = storage.conditional_upload_bytes(bucket, backup_blob, raw, if_generation_match=0)
        if backup_gen is None:
            logger.error("HARD-ABORT: backup object %s already exists — refusing to overwrite.", backup_blob)
            raise SystemExit(3)
        logger.info("Backed up pre-purge index (generation=%d) -> %s/%s", generation, bucket, backup_blob)

        out = io.BytesIO()
        kept.to_parquet(out, index=False)
        new_gen = storage.conditional_upload_bytes(bucket, INDEX_BLOB, out.getvalue(), if_generation_match=generation)
        if new_gen is None:
            logger.warning("[attempt %d] CAS lost (concurrent consolidator write) — re-reading and retrying.", attempt)
            time.sleep(10)
            continue

        logger.info(
            "REWROTE index: %d row(s) (was %d, dropped %d) generation %d -> %d.",
            len(kept),
            len(df),
            n_drop,
            generation,
            new_gen,
        )
        return n_drop

    logger.error(
        "HARD-ABORT: CAS write lost %d consecutive attempts — investigate consolidator cadence.", MAX_CAS_ATTEMPTS
    )
    raise SystemExit(3)


def _verify(bucket: str, storage: StorageClient) -> int:
    objects = _find_objects(bucket, storage)
    raw, generation = storage.download_bytes_with_generation(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw)) if raw is not None else pd.DataFrame()
    remaining_rows = int(df["league_id"].fillna("").astype(str).isin(TARGET_LEAGUE_IDS).sum()) if not df.empty else 0
    logger.info(
        "VERIFY: %d remaining GCS object(s), %d remaining manifest row(s) (index generation=%s).",
        len(objects),
        remaining_rows,
        generation,
    )
    if objects:
        logger.error("Remaining objects: %s", objects[:20])
    return 0 if not objects and remaining_rows == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    _assert_still_out_of_universe()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    storage = get_storage_client()
    logger.info("Sports instruments bucket: %s", bucket)

    if args.verify_only:
        return _verify(bucket, storage)

    objects = _find_objects(bucket, storage)
    logger.info("Found %d GCS object(s) under league_id in %s.", len(objects), TARGET_LEAGUE_IDS)

    apply = bool(args.apply)
    _purge_gcs_objects(bucket, objects, apply=apply)
    _purge_manifest_rows(bucket, storage, apply=apply)

    if not apply:
        logger.info("DRY-RUN complete — pass --apply to execute.")
        return 0

    return _verify(bucket, storage)


if __name__ == "__main__":
    sys.exit(main())
