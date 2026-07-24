# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: purge confirmed in the live consolidated _index (zero rows for these 4 data_types remain).
#!/usr/bin/env python3
"""Purge frozen 2018-2020 sports `markets`/`outcomes`/`settlements`/`arbitrage_opportunity` manifest rows.

`sports_closeout_batch1_ao_ready_2026_07_24.md` todo 18 (CLEANUP): these 4 data_types have 6,588
manifest rows each (26,352 total), ALL dated 2018-01-01..2020-06-05, ALL `capture_status=empty_confirmed`
(source returned zero rows every attempt — no real GCS object was ever written for any of them; confirmed
via a bounded listing under several sample dates in this exact range, zero objects). No live writer touches
these dates any more (zero rows exist for these same 4 data_types on any date after 2020-06-05, confirmed
against the full manifest), so this is a dead-manifest-row purge (not a GCS object delete — none exist).

NOTE: `markets`/`outcomes`/`settlements`/`arbitrage_opportunity` remain live vocabulary in UAC's
`VENUE_DATA_TYPE_CAPABILITIES` for ODDS_API/PINNACLE/BETFAIR starting 2024-01-01 — this purge only removes
the dead pre-floor historical rows, it does not touch (or imply anything about) that live capability
declaration. A zero-rows-since-2024 gap for those (venue, data_type) tuples is a separate, live coverage
question filed on its own: `issues/sports_odds_markets_outcomes_settlements_arbitrage_expected_since_2024_zero_captured_2026_07_24.md`.

Sweeps both the consolidated ``_index/availability_index.parquet`` and every per-VM shard under
``_index/per_vm/`` (else the consolidator re-merges the stale cells back on its next cycle).

Idempotent. ``--dry-run`` (default) reports counts without writing.

Usage::

    cd instruments-service
    .venv/bin/python scripts/purge_frozen_2018_2020_sports_odds_scaffolding_2026_07_24.py --dry-run
    .venv/bin/python scripts/purge_frozen_2018_2020_sports_odds_scaffolding_2026_07_24.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_copy_object,
    gcs_describe_object,
)
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — get_storage_client factory for list_blobs / download / upload
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"instruments-store-sports-prd-{PROJECT_ID}"
INDEX_PATH = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
# Bucket has soft_delete_policy.retention_duration_seconds=0 (disabled) — an explicit snapshot is the
# ONLY recovery net, not GCS's own soft-delete window. Snapshots land under a dedicated purge_backups/
# prefix, deliberately OUTSIDE `_index/per_vm/` — a backup copy sitting inside that prefix would be
# misread as a live shard by the consolidator on its next merge cycle (same lesson already codified in
# `features-service/scripts/sports/purge_dead_dimension_groups_2026_07_24.py`).
BACKUP_PREFIX = "_index/purge_backups/"

# The frozen pre-floor data_types being purged. All-time manifest census (2026-07-24) confirmed these are
# the ONLY 4 data_types matching; no other row shares these values.
PURGE_DATA_TYPES: frozenset[str] = frozenset({"markets", "outcomes", "settlements", "arbitrage_opportunity"})


def _purge_mask(df: pd.DataFrame) -> pd.Series:
    if "data_type" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df["data_type"].astype(str).isin(PURGE_DATA_TYPES)


def _process_blob(storage_client: object, object_key: str, *, apply: bool) -> int:
    raw = storage_client.download_bytes(BUCKET, object_key)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    mask = _purge_mask(df)
    n = int(mask.sum())
    if n == 0:
        return 0
    by_type = df[mask].groupby("data_type").size().to_dict()
    logger.info("%s: %d rows to purge %s", object_key, n, by_type)
    if not apply:
        return n
    backup_key = f"{BACKUP_PREFIX}{object_key}.pre_purge_2026_07_24.bak.parquet"
    gcs_copy_object(f"gs://{BUCKET}/{object_key}", f"gs://{BUCKET}/{backup_key}")
    logger.info("%s: snapshotted original to gs://%s/%s", object_key, BUCKET, backup_key)
    keep = df[~mask]
    out = io.BytesIO()
    keep.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, object_key, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("%s: wrote filtered manifest (%d -> %d rows)", object_key, len(df), len(keep))
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the filtered manifest (default: dry-run).")
    args = parser.parse_args()
    apply = bool(args.apply)

    storage_client = get_storage_client()

    if gcs_describe_object(f"gs://{BUCKET}/{INDEX_PATH}") is None:
        logger.error("Consolidated index not found: gs://%s/%s", BUCKET, INDEX_PATH)
        return 1

    total = 0
    total += _process_blob(storage_client, INDEX_PATH, apply=apply)

    raw_blobs = storage_client.list_blobs(BUCKET, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
    shard_keys = [b.name for b in raw_blobs if str(b.name).endswith(".parquet")]  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Sweeping %d per-VM shard(s) under %s", len(shard_keys), PER_VM_PREFIX)
    for key in shard_keys:
        total += _process_blob(storage_client, key, apply=apply)

    mode = "APPLIED" if apply else "DRY-RUN (no writes)"
    logger.info("%s - total rows %s: %d", mode, "purged" if apply else "to purge", total)
    if not apply:
        logger.info("Re-run with --apply to delete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
