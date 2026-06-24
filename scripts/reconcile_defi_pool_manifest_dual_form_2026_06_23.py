# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 glued_pair POOL rows + honest_cov re-measured post-reconcile in live consolidated index
"""Reconcile the DeFi POOL manifest to the canonical dual-form (operator Refinement 1 + 2).

The defi `_index` carries THREE POOL instrument_id namespaces (measured 2026-06-23):
  * ``canonical_0x``         (``0x…``)                       — captured, the canonical form.
  * ``glued_venuechain_0x``  (``UNISWAP_V3-ETH:POOL:0x…``)   — captured, a parallel namespace
                                                                from a rebuild path that read the
                                                                data-file ``instrument_id`` column
                                                                (``build_instrument_id``).
  * ``glued_pair``           (``UNISWAPV3-OPT:POOL:WSTETH-RSETH:500``, UPPERCASE ``POOL``) — the
                                                                OLD pre-fix enumerator seeds
                                                                (empty_confirmed / expected_unattempted),
                                                                incl. the ~408k EXPECTED_INSTRUMENT_DELISTED.

The canonical manifest pool id is ``pool_address.lower()`` (matches the MTDS writer +
``_canonical_defi_id`` + the IS catalogue/seeder fixes). This reconcile converges every POOL row
onto it, per the operator's two refinements:

  REFINEMENT 2 — MIGRATE where the data's already complete; DELETE+REDO only if re-download yields MORE:
    * ``glued_venuechain_0x`` captured  → RE-KEY ``instrument_id`` to the bare ``0x…`` (extract the
      address from the glued composite) + lowercase ``instrument_type`` → MIGRATE (no re-download;
      the captured row's data already exists, only the key was wrong). Where a bare-0x row for the
      SAME (date,venue,chain,data_type,pool) already exists, DROP the glued duplicate (dedup).
    * ``glued_pair`` empty/expected_unattempted POOL seeds → these are SUPERSEDED by the canonical
      re-seed (the rebuilt catalogue + ``enumerate_expected_universe`` now emit canonical-0x seeds).
      DELETE the non-captured seeds (incl. the 408k DELISTED) — the canonical re-seed re-materialises
      the live ones as expected_unattempted/captured; a glued_pair seed can never reconcile against a
      canonical capture and is pure phantom noise. A *captured* glued_pair row (none measured today)
      is real data → KEPT (never silently drop a capture).

  The decision is data-driven per row, idempotent, backup-then-write — mirrors
  ``delete_phantom_rows_from_shards.py``. Run ORDER: (1) rebuild catalogue + re-seed enumerator
  (canonical seeds) FIRST, (2) then this reconcile.

Usage:
  python scripts/reconcile_defi_pool_manifest_dual_form_2026_06_23.py --dry-run
  python scripts/reconcile_defi_pool_manifest_dual_form_2026_06_23.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import parse_glued_pool_id
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"

# The captured-row shard key (excluding instrument_id) used to detect whether a
# glued row's pool is ALREADY credited canonically on the same cell.
_CELL_KEY_COLS: tuple[str, ...] = ("date", "venue", "chain", "data_type")


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.dualform.bak.parquet"
    return f"{blob_name}.{run_ts}.dualform.bak"


def _is_pool_row(row: pd.Series) -> bool:  # type: ignore[type-arg]
    return str(row.get("instrument_type", "")).strip().lower() == "pool"


def _classify_pool_id(instrument_id: str) -> str:
    """Classify a POOL instrument_id into its namespace form."""
    iid = instrument_id.strip()
    low = iid.lower()
    if low.startswith("0x"):
        return "canonical_0x"
    # ``…:POOL:0x…`` (address-as-symbol, any case)
    if ":pool:" in low and low.rsplit(":pool:", 1)[1].startswith("0x"):
        return "glued_venuechain_0x"
    if ":pool:" in low:
        return "glued_pair"
    return "other"


def _canonical_address_from_glued_0x(instrument_id: str) -> str | None:
    """Extract the bare ``0x…`` pool address from a ``VENUE-CHAIN:POOL:0x…`` composite."""
    parsed = parse_glued_pool_id(instrument_id)
    if parsed is not None and parsed.pool_address:
        return parsed.pool_address.lower()
    return None


def reconcile_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Reconcile one manifest frame onto canonical pool ids. Pure — unit-tested.

    Returns ``(reconciled_df, stats)``. Idempotent: a frame with only canonical_0x
    POOL rows is returned unchanged.
    """
    stats = {"rekeyed_glued_0x": 0, "deleted_glued_pair": 0, "dropped_dup_after_rekey": 0, "untouched": 0}
    if df.empty or "instrument_type" not in df.columns:
        return df, stats

    pool_mask = df.apply(_is_pool_row, axis=1)
    if not bool(pool_mask.any()):
        stats["untouched"] = len(df)
        return df, stats

    keep_rows: list[dict[str, object]] = []
    # Track (cell, pool_address) already emitted as canonical captured to dedup re-keyed glued rows.
    seen_canonical: set[tuple[str, ...]] = set()
    for _, r in df.iterrows():
        rec = r.to_dict()
        if not _is_pool_row(r):
            keep_rows.append(rec)
            continue
        iid = str(rec.get("instrument_id", ""))
        form = _classify_pool_id(iid)
        cell = tuple(str(rec.get(c, "")) for c in _CELL_KEY_COLS)
        if form == "canonical_0x":
            rec["instrument_type"] = "pool"  # normalise case
            seen_canonical.add((*cell, iid.lower()))
            keep_rows.append(rec)
        elif form == "glued_venuechain_0x":
            addr = _canonical_address_from_glued_0x(iid)
            if addr is None:
                keep_rows.append(rec)  # unparseable — keep as-is (never silently drop a capture)
                continue
            dedup_key = (*cell, addr)
            if dedup_key in seen_canonical:
                stats["dropped_dup_after_rekey"] += 1
                continue  # a bare-0x row for this exact cell+pool already exists → drop the dup
            rec["instrument_id"] = addr
            rec["instrument_type"] = "pool"
            seen_canonical.add(dedup_key)
            stats["rekeyed_glued_0x"] += 1
            keep_rows.append(rec)
        elif form == "glued_pair":
            # OLD pre-fix SEED (empty_confirmed / expected_unattempted) — superseded by the canonical
            # re-seed; the canonical captured already credits the cell. Drop ONLY non-captured seeds.
            # A *captured* glued_pair row (none measured today, but defensive) is real data → KEEP it
            # (never silently drop a capture); it carries no recoverable 0x address from the pair form,
            # so it stays as-is for a follow-up rather than being lost.
            if str(rec.get("capture_status", "")) == "captured":
                keep_rows.append(rec)
            else:
                stats["deleted_glued_pair"] += 1
            continue
        else:
            keep_rows.append(rec)

    out = pd.DataFrame(keep_rows, columns=list(df.columns)) if keep_rows else df.iloc[0:0].copy()
    return out, stats


def _process_blob(storage: object, bucket: str, blob_name: str, run_ts: str, *, apply: bool) -> dict[str, int]:  # pyright: ignore[reportAny]
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAny]
    except Exception as exc:  # pragma: no cover — best-effort per-blob
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    reconciled, stats = reconcile_frame(df)
    changed = stats["rekeyed_glued_0x"] + stats["deleted_glued_pair"] + stats["dropped_dup_after_rekey"]
    if changed == 0:
        return stats
    logger.info(
        "  %s: rekey_glued_0x=%d delete_glued_pair=%d drop_dup=%d (rows %d → %d)",
        blob_name,
        stats["rekeyed_glued_0x"],
        stats["deleted_glued_pair"],
        stats["dropped_dup_after_rekey"],
        len(df),
        len(reconciled),
    )
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAny]
        out = io.BytesIO()
        reconciled.to_parquet(out, index=False, engine="pyarrow")
        out.seek(0)
        storage.upload_bytes(bucket, blob_name, out.read())  # pyright: ignore[reportAny]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(reconciled))
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bucket = _defi_bucket()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    storage = get_storage_client()

    totals = {"rekeyed_glued_0x": 0, "deleted_glued_pair": 0, "dropped_dup_after_rekey": 0}

    logger.info("=== Canonical: gs://%s/%s ===", bucket, _INDEX_PATH)
    canon_stats = _process_blob(storage, bucket, _INDEX_PATH, run_ts, apply=args.apply)
    for k in totals:
        totals[k] += canon_stats.get(k, 0)

    logger.info("=== Per-VM shards: gs://%s/%s* ===", bucket, _PER_VM_PREFIX)
    shard_blobs = [
        b.name  # pyright: ignore[reportAny]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAny]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAny]
    ]
    logger.info("Per-VM shards to scan: %d", len(shard_blobs))
    for blob_name in shard_blobs:
        s = _process_blob(storage, bucket, blob_name, run_ts, apply=args.apply)
        for k in totals:
            totals[k] += s.get(k, 0)

    logger.info("=" * 60)
    logger.info(
        "Summary: rekeyed_glued_0x=%d deleted_glued_pair=%d dropped_dup=%d",
        totals["rekeyed_glued_0x"],
        totals["deleted_glued_pair"],
        totals["dropped_dup_after_rekey"],
    )
    logger.info("=" * 60)
    if args.dry_run:
        logger.info("DRY RUN — no writes performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
