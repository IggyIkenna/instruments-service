# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 EXPECTED_INSTRUMENT_DELISTED rows on catalogue-LIVE pools (Gate-2/4 clean)
"""Delete STALE ``EXPECTED_INSTRUMENT_DELISTED`` seeds that the rebuilt catalogue contradicts.

Gate-2/Gate-4 finding (2026-06-24): the defi ``_index`` carries 132,374
``EXPECTED_INSTRUMENT_DELISTED`` empty_confirmed rows (2,196 distinct pools) that were emitted
by the enumerator run ``enum-universe-defi-20260624-013038`` at 01:30Z — BEFORE the 10:17Z
catalogue rebuild (spelling-collapse fix). At 01:30 those pools carried a premature
``available_to`` (the 2026-05-08 cliff bug), so the enumerator correctly-per-its-inputs marked
them DELISTED. The 10:17 rebuild collapsed the venue-spelling lifecycle → ``available_to=None``
(LIVE) for these pools, and the 10:24 re-seed (``enum-universe-defi-20260624-102449``, the
authoritative post-rebuild run) emits only the corrected seeds. But last-write-wins consolidation
never overwrote the stale DELISTED cells (the re-seed emits a different status for the same cell
or doesn't cover the exact date), so the stale DELISTED rows persist and CONTRADICT the catalogue
(a LIVE pool cannot be delisted). They are also proven-live: 553 of the 2,196 pools have a
``captured`` row on 2026-06-15+ (the prod subgraph/RPC path pulled real on-chain data).

This deletes ONLY rows that are ALL of:
  * ``error_reason == EXPECTED_INSTRUMENT_DELISTED``
  * ``capture_status != captured`` (never drop a real capture — defensive; measured 0 captured)
  * ``instrument_type == pool``
  * the pool is LIVE in the current catalogue (``available_to`` is null/blank)

A DELISTED row on a genuinely-dead pool (``available_to`` set, in the past) is KEPT — it is a
correct honest-absence reason. The deleted (date, pool) cells re-seed as ``expected_unattempted``
on the next enumerator run. Backup-then-write, dry-run→apply, idempotent (re-running finds 0).

Usage:
  python scripts/delete_stale_delisted_on_live_pools_2026_06_24.py --dry-run
  python scripts/delete_stale_delisted_on_live_pools_2026_06_24.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"


def _md_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _is_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.delisted-clean.bak.parquet"
    return f"{blob_name}.{run_ts}.delisted-clean.bak"


def _live_pool_ids(storage: object) -> set[str]:  # pyright: ignore[reportAny]
    """Catalogue pool ids whose ``available_to`` is null/blank (LIVE)."""
    raw = storage.download_bytes(_is_bucket(), "prod/catalog.parquet")  # pyright: ignore[reportAny]
    cat = pd.read_parquet(io.BytesIO(raw))
    pool = cat[cat["instrument_type"].astype(str).str.lower() == "pool"].copy()
    pool["iid_low"] = pool["instrument_id"].astype(str).str.lower()
    at = pool["available_to"]
    live_mask = at.isna() | at.astype(str).str.strip().isin(["", "NaT", "None", "nan"])
    return set(pool.loc[live_mask, "iid_low"].unique())


def clean_frame(df: pd.DataFrame, live_pools: set[str]) -> tuple[pd.DataFrame, int]:
    """Drop stale DELISTED rows on catalogue-LIVE pools. Pure — unit-testable."""
    if df.empty or "error_reason" not in df.columns or "instrument_id" not in df.columns:
        return df, 0
    iid_low = df["instrument_id"].astype(str).str.lower()
    is_pool = df["instrument_type"].astype(str).str.lower() == "pool"
    is_delisted = df["error_reason"].astype(str) == "EXPECTED_INSTRUMENT_DELISTED"
    not_captured = df["capture_status"].astype(str) != "captured"
    on_live = iid_low.isin(live_pools)
    drop_mask = is_pool & is_delisted & not_captured & on_live
    n = int(drop_mask.sum())
    if n == 0:
        return df, 0
    return df.loc[~drop_mask].copy(), n


def _process_blob(
    storage: object,  # pyright: ignore[reportAny]
    bucket: str,
    blob_name: str,
    live_pools: set[str],
    run_ts: str,
    *,
    apply: bool,
) -> int:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAny]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return 0
    df = pd.read_parquet(io.BytesIO(raw))
    cleaned, n = clean_frame(df, live_pools)
    if n == 0:
        return 0
    logger.info("  %s: drop %d stale-DELISTED-on-live rows (%d → %d)", blob_name, n, len(df), len(cleaned))
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAny]
        out = io.BytesIO()
        cleaned.to_parquet(out, index=False, engine="pyarrow")
        out.seek(0)
        storage.upload_bytes(bucket, blob_name, out.read())  # pyright: ignore[reportAny]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(cleaned))
    return n


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bucket = _md_bucket()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    storage = get_storage_client()

    live_pools = _live_pool_ids(storage)
    logger.info("Catalogue LIVE pools (available_to null): %d", len(live_pools))

    total = 0
    logger.info("=== Canonical: gs://%s/%s ===", bucket, _INDEX_PATH)
    total += _process_blob(storage, bucket, _INDEX_PATH, live_pools, run_ts, apply=args.apply)

    logger.info("=== Per-VM shards: gs://%s/%s* ===", bucket, _PER_VM_PREFIX)
    shard_blobs = [
        b.name  # pyright: ignore[reportAny]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAny]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAny]
    ]
    for blob_name in shard_blobs:
        total += _process_blob(storage, bucket, blob_name, live_pools, run_ts, apply=args.apply)

    logger.info("=" * 60)
    logger.info("Summary: dropped_stale_delisted_on_live=%d", total)
    logger.info("=" * 60)
    if args.dry_run:
        logger.info("DRY RUN — no writes performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
