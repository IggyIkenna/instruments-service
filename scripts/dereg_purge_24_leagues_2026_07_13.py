#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is verified in prod across >=2 consolidator cycles (--verify-only)
"""Purge the 24 de-registered league_ids from the sports availability index.

2026-07-13 operator ruling: de-register 24 league_ids from the sports universe
(110 119 122 15066 235 236 239 244 253 254 283 315 32 357 358 362 365 408 493 71
850 LA_LIGA_2 RFPL SCOTTISH_LEAGUE_CUP_185) so the index carries EXACTLY the
94-league api_football trading universe
(``get_expected_leagues_for_source("api_football")``).

Mirrors the backup-then-rewrite pattern of
``backfill_remove_unknown_league_phantom_2026_07_09.py`` with two deliberate
deviations:

1. **Captured-row deletability gate** (replaces that script's
   instrument_count==0 assertion, which would rightly hard-abort here — the
   1,652 captured rows under the 24 carry real counts): a captured row may be
   dropped IFF (a) its atom was RE-KEYED — content-verified copy at the
   canonical ``league=`` path + a captured row under the canonical league_id
   present in the SAME index being rewritten (the LA_LIGA_2 2019-06-04 MATCHES
   atom, see ``dereg_rekey_la_liga_2_2026_07_13.py``), or (b) the exact raw row
   is present in the parked-rows export
   ``_audits/parked_league_rows_20260713.parquet`` (verified re-readable at
   export time, re-downloaded and matched here row-for-row on
   (league_id, date, data_type, source, written_at) multiset counts — see
   ``dereg_park_captured_rows_2026_07_13.py``). Anything else → SystemExit(3),
   no write. GCS data objects are NOT touched by this script.

2. **Consolidator-concurrency**: the precedent's plain read-modify-write races
   the ``*/1`` instruments-sports consolidator cron (live prune-race issue:
   ``plans/active/issues/manifest_consolidator_prune_race_overlapping_executions_2026_07_13.md``).
   This script uses generation-preconditioned CAS
   (``gcs_read_object_with_generation`` + ``conditional_upload_bytes(if_generation_match=...)``)
   in a bounded retry loop, and immediately before each write attempt verifies
   ``_index/per_vm/`` carries ZERO rows under the 24 (nothing for the
   consolidator to resurrect). Post-purge persistence across >=2 consolidator
   cycles is verified via ``--verify-only`` runs.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    python scripts/dereg_purge_24_leagues_2026_07_13.py --dry-run
  ... --apply
  ... --verify-only   # run at least twice, >=1 consolidator cycle apart
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from collections import Counter
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import StorageClient, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dereg_purge_24_leagues")

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
PARKED_BLOB = "_audits/parked_league_rows_20260713.parquet"

#: Operator ruling 2026-07-13 — the 24 de-registered league_ids.
DEREG_LEAGUE_IDS = frozenset(
    {
        "110",
        "119",
        "122",
        "15066",
        "235",
        "236",
        "239",
        "244",
        "253",
        "254",
        "283",
        "315",
        "32",
        "357",
        "358",
        "362",
        "365",
        "408",
        "493",
        "71",
        "850",
        "LA_LIGA_2",
        "RFPL",
        "SCOTTISH_LEAGUE_CUP_185",
    }
)

#: Re-key map (dereg source league -> canonical league). A dropped captured row
#: under a source league is re-key-justified only when the SAME index retains a
#: captured row under the canonical league for the identical
#: (date, data_type, source) atom.
REKEY_CANONICAL = {"LA_LIGA_2": "SEGUNDA_DIVISION", "SCOTTISH_LEAGUE_CUP_185": "SCOTTISH_LEAGUE_CUP"}

#: Post-purge invariant: the index's distinct league_id set is exactly the
#: 94-league api_football universe.
EXPECTED_POST_PURGE_LEAGUES = 94

#: CAS retry bound — each retry re-reads, re-filters, re-asserts.
MAX_CAS_ATTEMPTS = 5


def _row_keys(df: pd.DataFrame) -> list[tuple[str, str, str, str, str]]:
    """Raw-row identity multiset key: (league_id, date, data_type, source, written_at)."""
    return list(
        zip(
            df["league_id"].fillna("").astype(str),
            df["date"].astype(str),
            df["data_type"].fillna("").astype(str),
            df["source"].fillna("").astype(str),
            df["written_at"].fillna("").astype(str),
            strict=True,
        )
    )


def _assert_dropped_captured_rows_accounted(dropped: pd.DataFrame, kept: pd.DataFrame, parked: pd.DataFrame) -> None:
    """Hard-abort (SystemExit(3)) unless EVERY dropped captured row is parked or re-keyed."""
    cap = dropped[dropped["capture_status"] == "captured"]
    if cap.empty:
        logger.info("No captured rows among the dropped set — nothing to justify.")
        return

    parked_counts = Counter(_row_keys(parked))
    kept_cap = kept[kept["capture_status"] == "captured"]
    kept_atoms = {(lid, d, dt, src) for lid, d, dt, src, _w in _row_keys(kept_cap)}

    unaccounted: list[tuple[str, str, str, str, str]] = []
    n_parked = 0
    n_rekeyed = 0
    for key in _row_keys(cap):
        if parked_counts.get(key, 0) > 0:
            parked_counts[key] -= 1
            n_parked += 1
            continue
        lid, d, dt, src, _w = key
        canonical = REKEY_CANONICAL.get(lid)
        if canonical is not None and (canonical, d, dt, src) in kept_atoms:
            n_rekeyed += 1
            continue
        unaccounted.append(key)

    logger.info(
        "Dropped captured rows: %d total = %d parked-justified + %d rekey-justified + %d UNACCOUNTED.",
        len(cap),
        n_parked,
        n_rekeyed,
        len(unaccounted),
    )
    if unaccounted:
        logger.error(
            "HARD-ABORT: %d captured row(s) to be dropped are neither in the parked export nor "
            "re-keyed-with-captured-canonical-twin. First 20:\n%s",
            len(unaccounted),
            "\n".join(map(str, unaccounted[:20])),
        )
        raise SystemExit(3)


def _per_vm_dereg_rows(storage: StorageClient, bucket: str) -> int:
    """Count rows under the 24 across live per-VM shards (stale-listing-race tolerant)."""
    total = 0
    for blob in storage.list_blobs(bucket, prefix=PER_VM_PREFIX):
        name = str(getattr(blob, "name", ""))
        if not name.endswith(".parquet") or ".bak" in name:
            continue
        if not storage.blob_exists(bucket, name):
            logger.warning("per-vm %s: listed but no longer exists (consolidator pruned it) — skipping.", name)
            continue
        shard = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, name)))
        if shard.empty or "league_id" not in shard.columns:
            continue
        n = int(shard["league_id"].fillna("").astype(str).isin(DEREG_LEAGUE_IDS).sum())
        if n:
            logger.warning("per-vm %s: %d row(s) under the 24.", name, n)
        total += n
    return total


def _read_index_with_generation(storage: StorageClient, bucket: str) -> tuple[pd.DataFrame, bytes, int]:
    raw, generation = storage.download_bytes_with_generation(bucket, INDEX_BLOB)
    if raw is None:
        logger.error("HARD-ABORT: canonical index absent at gs-path %s/%s", bucket, INDEX_BLOB)
        raise SystemExit(3)
    return pd.read_parquet(io.BytesIO(raw)), raw, generation


def _distinct_leagues(df: pd.DataFrame) -> set[str]:
    values: list[str] = df["league_id"].fillna("").astype(str).tolist()
    return {v for v in values if v}


def _verify(storage: StorageClient, bucket: str) -> int:
    """Confirm 0 rows under the 24 remain (canonical + per-VM) and 94 distinct leagues."""
    df, _raw, generation = _read_index_with_generation(storage, bucket)
    lid = df["league_id"].fillna("").astype(str)
    remaining = int(lid.isin(DEREG_LEAGUE_IDS).sum())
    leagues = _distinct_leagues(df)
    logger.info(
        "VERIFY canonical (generation=%d): %d rows under the 24 remaining; %d distinct league_ids (expected %d).",
        generation,
        remaining,
        len(leagues),
        EXPECTED_POST_PURGE_LEAGUES,
    )
    shard_rows = _per_vm_dereg_rows(storage, bucket)
    logger.info("VERIFY per-vm shards: %d row(s) under the 24.", shard_rows)
    ok = remaining == 0 and shard_rows == 0 and len(leagues) == EXPECTED_POST_PURGE_LEAGUES
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    verify_only = bool(args.verify_only)
    dry_run = bool(args.dry_run)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    logger.info("Sports instruments bucket: %s", bucket)
    storage = get_storage_client()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    if verify_only:
        return _verify(storage, bucket)

    if not storage.blob_exists(bucket, PARKED_BLOB):
        logger.error("HARD-ABORT: parked export missing at %s/%s — run the park step first.", bucket, PARKED_BLOB)
        return 3
    parked = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, PARKED_BLOB)))
    logger.info("Parked export: %d rows (re-downloaded + parsed OK).", len(parked))

    for attempt in range(1, MAX_CAS_ATTEMPTS + 1):
        # Immediately-pre-write drain check: nothing under the 24 may be pending
        # in per-VM shards, or the next consolidator merge would resurrect rows.
        pending = _per_vm_dereg_rows(storage, bucket)
        if pending:
            logger.error("HARD-ABORT: %d per-VM shard row(s) under the 24 — consolidate first.", pending)
            return 3

        df, raw, generation = _read_index_with_generation(storage, bucket)
        mask = df["league_id"].fillna("").astype(str).isin(DEREG_LEAGUE_IDS)
        n_drop = int(mask.sum())
        logger.info(
            "[attempt %d] index generation=%d: %d / %d rows under the 24 (distinct leagues before: %d).",
            attempt,
            generation,
            n_drop,
            len(df),
            len(_distinct_leagues(df)),
        )
        dropped, kept = df.loc[mask], df.loc[~mask]
        post_leagues = _distinct_leagues(kept)
        if len(post_leagues) != EXPECTED_POST_PURGE_LEAGUES:
            logger.error(
                "HARD-ABORT: post-purge distinct league count would be %d, expected %d: %s",
                len(post_leagues),
                EXPECTED_POST_PURGE_LEAGUES,
                sorted(post_leagues),
            )
            return 3

        _assert_dropped_captured_rows_accounted(dropped, kept, parked)

        if dry_run:
            logger.info("DRY-RUN — would delete %d rows and rewrite %d rows. Pass --apply to write.", n_drop, len(kept))
            return 0

        backup_blob = f"{INDEX_BLOB[:-8]}.{run_ts}.dereg_24_leagues.bak.parquet"
        backup_gen = storage.conditional_upload_bytes(bucket, backup_blob, raw, if_generation_match=0)
        if backup_gen is None and attempt == 1:
            logger.error("HARD-ABORT: backup object %s already exists — refusing to overwrite.", backup_blob)
            return 3
        logger.info("Backed up pre-deletion index (generation=%d) -> %s/%s", generation, bucket, backup_blob)

        out = io.BytesIO()
        kept.to_parquet(out, index=False)
        new_gen = storage.conditional_upload_bytes(bucket, INDEX_BLOB, out.getvalue(), if_generation_match=generation)
        if new_gen is None:
            logger.warning("[attempt %d] CAS lost (concurrent consolidator write) — re-reading and retrying.", attempt)
            time.sleep(10)
            continue
        logger.info(
            "REWROTE index: %d rows (was %d, dropped %d) generation %d -> %d.",
            len(kept),
            len(df),
            n_drop,
            generation,
            new_gen,
        )
        return _verify(storage, bucket)

    logger.error(
        "HARD-ABORT: CAS write lost %d consecutive attempts — investigate consolidator cadence.", MAX_CAS_ATTEMPTS
    )
    return 3


if __name__ == "__main__":
    sys.exit(main())
