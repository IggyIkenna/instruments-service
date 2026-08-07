#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after this purge is verified in prod across >=2 consolidator cycles (--verify-only)
"""Purge footystats manifest rows for leagues dropped from its subscription scope.

2026-08-07: the operator's FootyStats subscription was widened to 50 leagues,
which dropped CHINA_SUPER_LEAGUE and RUSSIA_PREMIER_LEAGUE from the footystats
write-gate (``get_expected_leagues_for_source("footystats", ...)`` —
unified-api-contracts@7810dad61503f). Both leagues remain canonical/in-universe
for OTHER sources; only their footystats-specific manifest rows are stale.

Live census (2026-08-07) confirmed ZERO ``captured`` rows among the 4,458
affected rows (all ``empty_confirmed``/``expected_unattempted``) — no real data
content is at risk, only manifest bookkeeping. This script still hard-aborts if
a captured row is found in the drop set (defense in depth against manifest
drift between the census and this run).

Mirrors the CAS backup-then-rewrite pattern of
``dereg_purge_24_leagues_2026_07_13.py``, simplified because there is no
captured-row deletability question to resolve (no parked/rekey export needed).

Bucket history note: instruments-store-sports-prd-central-element-323112 had
its GCS Soft Delete disabled until 2026-07-27 (fixed after a 2026-07-17
incident destroyed 7,185 manifest rows with zero recoverable versions) — this
script queries the retention window fresh, same-run, and hard-aborts if it is
below the 7-day reversibility threshold (delete-safety-protocol §3a).

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    python scripts/footystats_purge_out_of_scope_leagues_2026_08_07.py --dry-run
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
from unified_trading_library import (
    StorageClient,
    gcs_bucket_soft_delete_retention_seconds,
    get_storage_client,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("footystats_purge_out_of_scope")

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

#: 2026-08-07 footystats subscription-scope removal — no Prediction-tier
#: sibling in-country, so neither made the operator's 50-league subscription.
OUT_OF_SCOPE_LEAGUE_IDS = frozenset({"CHINA_SUPER_LEAGUE", "RUSSIA_PREMIER_LEAGUE"})
OUT_OF_SCOPE_DATA_TYPES = frozenset({"MATCHES", "ODDS", "PREDICTIONS"})
OUT_OF_SCOPE_SOURCE = "footystats"

#: Reversibility carve-out threshold (delete-safety-protocol §3a) — 7 days.
MIN_SOFT_DELETE_RETENTION_SECONDS = 604_800

#: CAS retry bound — each retry re-reads, re-filters, re-asserts.
MAX_CAS_ATTEMPTS = 5


def _drop_mask(df: pd.DataFrame) -> pd.Series:
    return (
        (df["source"].fillna("").astype(str) == OUT_OF_SCOPE_SOURCE)
        & (df["league_id"].fillna("").astype(str).isin(OUT_OF_SCOPE_LEAGUE_IDS))
        & (df["data_type"].fillna("").astype(str).isin(OUT_OF_SCOPE_DATA_TYPES))
    )


def _assert_no_captured_rows_in_drop_set(dropped: pd.DataFrame) -> None:
    cap = dropped[dropped["capture_status"] == "captured"]
    if not cap.empty:
        logger.error(
            "HARD-ABORT: %d captured row(s) found in the drop set — manifest state has drifted "
            "since the 2026-08-07 census (which found zero). Not safe to purge blind. First 20:\n%s",
            len(cap),
            cap[["date", "league_id", "data_type"]].head(20).to_string(),
        )
        raise SystemExit(3)
    logger.info("Confirmed: 0 captured rows among %d rows to drop.", len(dropped))


def _per_vm_scoped_rows(storage: StorageClient, bucket: str) -> int:
    """Count out-of-scope rows across live per-VM shards (stale-listing-race tolerant)."""
    total = 0
    for blob in storage.list_blobs(bucket, prefix=PER_VM_PREFIX):
        name = str(getattr(blob, "name", ""))
        if not name.endswith(".parquet") or ".bak" in name:
            continue
        if not storage.blob_exists(bucket, name):
            logger.warning("per-vm %s: listed but no longer exists (consolidator pruned it) — skipping.", name)
            continue
        shard = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, name)))
        if shard.empty or "league_id" not in shard.columns or "source" not in shard.columns:
            continue
        n = int(_drop_mask(shard).sum())
        if n:
            logger.warning("per-vm %s: %d out-of-scope row(s).", name, n)
        total += n
    return total


def _read_index_with_generation(storage: StorageClient, bucket: str) -> tuple[pd.DataFrame, bytes, int]:
    raw, generation = storage.download_bytes_with_generation(bucket, INDEX_BLOB)
    if raw is None:
        logger.error("HARD-ABORT: canonical index absent at gs-path %s/%s", bucket, INDEX_BLOB)
        raise SystemExit(3)
    return pd.read_parquet(io.BytesIO(raw)), raw, generation


def _verify(storage: StorageClient, bucket: str) -> int:
    """Confirm 0 out-of-scope rows remain (canonical + per-VM)."""
    df, _raw, generation = _read_index_with_generation(storage, bucket)
    remaining = int(_drop_mask(df).sum())
    logger.info("VERIFY canonical (generation=%d): %d out-of-scope row(s) remaining.", generation, remaining)
    shard_rows = _per_vm_scoped_rows(storage, bucket)
    logger.info("VERIFY per-vm shards: %d out-of-scope row(s).", shard_rows)
    ok = remaining == 0 and shard_rows == 0
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

    if not dry_run:
        retention = gcs_bucket_soft_delete_retention_seconds(bucket)
        logger.info("Fresh soft-delete retention for %s: %ds.", bucket, retention)
        if retention < MIN_SOFT_DELETE_RETENTION_SECONDS:
            logger.error(
                "HARD-ABORT: soft-delete retention %ds < required %ds — this overwrite would not be "
                "reversibility-qualified (delete-safety-protocol §3a). Refusing to write.",
                retention,
                MIN_SOFT_DELETE_RETENTION_SECONDS,
            )
            return 3

    for attempt in range(1, MAX_CAS_ATTEMPTS + 1):
        pending = _per_vm_scoped_rows(storage, bucket)
        if pending:
            logger.error("HARD-ABORT: %d per-VM shard row(s) out-of-scope — consolidate first.", pending)
            return 3

        df, raw, generation = _read_index_with_generation(storage, bucket)
        mask = _drop_mask(df)
        n_drop = int(mask.sum())
        logger.info(
            "[attempt %d] index generation=%d: %d / %d rows out-of-scope (footystats + China/Russia + MATCHES/ODDS/PREDICTIONS).",
            attempt,
            generation,
            n_drop,
            len(df),
        )
        dropped, kept = df.loc[mask], df.loc[~mask]
        _assert_no_captured_rows_in_drop_set(dropped)

        if dry_run:
            logger.info("DRY-RUN — would delete %d rows and rewrite %d rows. Pass --apply to write.", n_drop, len(kept))
            return 0

        backup_blob = f"{INDEX_BLOB[:-8]}.{run_ts}.footystats_purge.bak.parquet"
        backup_gen = storage.conditional_upload_bytes(bucket, backup_blob, raw, if_generation_match=0)
        if backup_gen is None and attempt == 1:
            logger.error("HARD-ABORT: backup object %s already exists — refusing to overwrite.", backup_blob)
            return 3
        logger.info("Backed up pre-purge index (generation=%d) -> %s/%s", generation, bucket, backup_blob)

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
