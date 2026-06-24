#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index lending-family glued EU rows are re-keyed to canonical 0x + honest_cov re-measured post-reconcile in the live consolidated index
"""Re-key the DeFi lending-family manifest EU/empty/captured rows onto canonical 0x ids.

The lending-family data_types (lending_indices / liquidations / liquidation_events /
flash_loan_events / risk_params) were SEEDED by the IS enumerator in the LEGACY GLUED
composite form (``AAVE_V3-ARBITRUM:A_TOKEN:AGHO``), while the MTDS per-instrument writers
(the 6-handler grain fix, mtds@02e50cb2) record_captured at the canonical on-chain ADDRESS
(``raw_symbol.lower()`` = the underlying-asset / market / reserve address). So the glued
``expected_unattempted`` cells never reconcile against the address-keyed captures
(~1.04M glued lending EU stuck flat).

This is the BOUNDED in-place re-key (NOT a full-history re-seed — that would 5x-expand EU
from the catalogue's 1970 available_from). It mirrors ``reconcile_defi_pool_manifest_dual_form``
(the proven POOL dual-form reconcile, a1aacd): backup-then-write, dry-run → apply, idempotent,
per-row data-driven, NEVER silently drops a capture. For each glued lending row it maps the
glued ``instrument_id`` → the catalogue's canonical ``raw_symbol`` (0x/base58) — spelling-
normalised (``AAVE_V3`` ↔ ``AAVEV3``) — and re-keys in place; rows that already have a bare-0x
twin in the same cell are dropped as dups. Glued rows with NO catalogue mapping (e.g. the
Kamino VAULT residual) are KEPT as-is (a follow-up handles them — never silently dropped).

Usage:
  python scripts/reconcile_defi_lending_manifest_canonical_2026_06_24.py --dry-run
  python scripts/reconcile_defi_lending_manifest_canonical_2026_06_24.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_LENDING_DATA_TYPES = frozenset(
    {"lending_indices", "liquidations", "liquidation_events", "flash_loan_events", "risk_params"}
)
_LENDING_ITYPES = frozenset({"lending", "a_token", "debt_token", "lending_market", "solana_lending", "liquidation"})
# Cell key = the (date, venue, chain, data_type) shard minus instrument_id — the dedup grain.
_CELL_KEY_COLS = ("date", "venue", "chain", "data_type")


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.lendingcanon.bak.parquet"
    return f"{blob_name}.{run_ts}.lendingcanon.bak"


def _norm_glued(s: str) -> str:
    """Normalise a glued instrument_id for spelling-agnostic catalogue lookup (AAVE_V3 ↔ AAVEV3)."""
    return str(s).replace("_", "").upper()


def _is_onchain_addr(s: str) -> bool:
    s = s.strip()
    return s.startswith("0x") or (32 <= len(s) <= 44 and s.isalnum())


def build_glued_to_canonical_map(catalogue: pd.DataFrame) -> dict[str, str]:
    """``{normalised_glued_instrument_id: canonical_0x_lower}`` from ALL catalogue rows.

    Data_type-agnostic: any catalogue row (pool / lending / vault / perp / spot …) whose
    ``raw_symbol`` OR ``pool_address`` is an on-chain address maps its glued instrument_id →
    that address. The MTDS per-instrument writers all record_captured at the address, so the
    glued EU seed for ANY data_type re-keys onto the capture once mapped. Rows with no
    address (e.g. Kamino vaults the catalogue lacks) are simply absent from the map → the
    reconcile keeps them as-is (the genuine orphan residual, handled separately).
    """
    gmap: dict[str, str] = {}
    if catalogue.empty or "instrument_id" not in catalogue.columns:
        return gmap
    for _, r in catalogue.iterrows():
        iid = str(r.get("instrument_id") or "").strip()
        if not iid:
            continue
        for col in ("raw_symbol", "pool_address"):
            v = str(r.get(col) or "").strip().lower()
            if _is_onchain_addr(v):
                gmap[_norm_glued(iid)] = v
                break
    return gmap


def reconcile_frame(df: pd.DataFrame, gmap: dict[str, str]) -> tuple[pd.DataFrame, dict[str, int]]:
    """Re-key ANY glued defi instrument_id → canonical 0x via *gmap*. Pure + idempotent.

    Data_type-agnostic: a glued instrument_id in ANY data_type whose normed form maps to a
    catalogue address re-keys; unmapped glued (the catalogue genuinely lacks it, e.g. Kamino
    vaults) is kept as-is (never dropped). A frame with only canonical/unmapped rows returns
    unchanged.
    """
    stats = {"rekeyed_glued": 0, "dropped_dup_after_rekey": 0, "unmapped_kept": 0, "untouched": 0}
    if df.empty or "instrument_id" not in df.columns:
        stats["untouched"] = len(df)
        return df, stats

    iid = df["instrument_id"].astype(str)
    # VECTORISED (the canonical _index is ~7.5M rows — iterrows is O(20min)).
    # A GLUED row = has ":" and not a bare 0x (any data_type).
    is_glued = iid.str.contains(":", regex=False) & ~iid.str.startswith("0x")
    if not bool(is_glued.any()):
        stats["untouched"] = len(df)
        return df, stats
    norm = iid.where(is_glued, "").map(lambda s: _norm_glued(s) if s else "")  # type: ignore[arg-type]
    mapped_addr = norm.map(lambda k: gmap.get(k, "") if k else "")  # canonical 0x or "" when unmapped
    rekey = is_glued & (mapped_addr != "")
    stats["unmapped_kept"] = int((is_glued & (mapped_addr == "")).sum())

    work = df.copy()
    # Re-key the mapped glued rows in place → canonical 0x.
    work.loc[rekey, "instrument_id"] = mapped_addr[rekey]
    stats["rekeyed_glued"] = int(rekey.sum())

    # Dedup: after re-key, a (cell, instrument_id) that now has >1 row — drop the NON-captured
    # duplicates (a superseded seed/empty re-keyed onto an existing canonical twin). NEVER drop a
    # capture (the captured-preserve invariant is asserted by the caller's verify + the dedup script).
    cap = work.get("capture_status")
    is_captured = (cap.astype(str) == "captured") if cap is not None else pd.Series(False, index=work.index)
    key_cols = [c for c in _CELL_KEY_COLS if c in work.columns] + ["instrument_id"]
    dup = work.duplicated(subset=key_cols, keep=False)
    # within a dup group: keep all captured + the FIRST non-captured; drop the rest of the non-captured
    drop_mask = dup & ~is_captured & work.duplicated(subset=key_cols, keep="first")
    stats["dropped_dup_after_rekey"] = int(drop_mask.sum())
    out = work.loc[~drop_mask].reset_index(drop=True)
    return out, stats


def _process_blob(
    storage: object, bucket: str, blob_name: str, run_ts: str, gmap: dict[str, str], *, apply: bool
) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover — best-effort per-blob
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    reconciled, stats = reconcile_frame(df, gmap)
    changed = stats["rekeyed_glued"] + stats["dropped_dup_after_rekey"]
    if changed == 0:
        return stats
    logger.info(
        "  %s: rekeyed_glued=%d drop_dup=%d unmapped_kept=%d (rows %d → %d)",
        blob_name,
        stats["rekeyed_glued"],
        stats["dropped_dup_after_rekey"],
        stats["unmapped_kept"],
        len(df),
        len(reconciled),
    )
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAttributeAccessIssue]
        out = io.BytesIO()
        reconciled.to_parquet(out, index=False, engine="pyarrow")
        out.seek(0)
        storage.upload_bytes(bucket, blob_name, out.read())  # pyright: ignore[reportAttributeAccessIssue]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(reconciled))
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply = bool(args.apply)

    bucket = _defi_bucket()
    cat_bucket = _catalogue_bucket()
    storage = get_storage_client(project_id=None)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    cat_raw = storage.download_bytes(cat_bucket, "prod/catalog.parquet")  # pyright: ignore[reportAttributeAccessIssue]
    gmap = build_glued_to_canonical_map(pd.read_parquet(io.BytesIO(cat_raw)))
    logger.info("Catalogue glued→canonical lending map: %d entries", len(gmap))

    # ONLY the MANIFEST files: the consolidated canonical index + the active per-VM shards.
    # NOT the whole _index/ prefix — that also holds drift_v2_sig_index_parts (~7k feature-index
    # parts) + snapshots + .bak, which are NOT manifest rows and must never be rewritten here.
    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    logger.info(
        "Mode=%s; %d manifest parquet(s) to scan (canonical + per_vm shards)",
        "APPLY" if apply else "DRY-RUN",
        len(blobs),
    )

    totals = {"rekeyed_glued": 0, "dropped_dup_after_rekey": 0, "unmapped_kept": 0}
    for blob_name in blobs:
        s = _process_blob(storage, bucket, blob_name, run_ts, gmap, apply=apply)
        for k in totals:
            totals[k] += s.get(k, 0)

    logger.info(
        "TOTAL: rekeyed_glued=%d dropped_dup=%d unmapped_kept=%d (mode=%s)",
        totals["rekeyed_glued"],
        totals["dropped_dup_after_rekey"],
        totals["unmapped_kept"],
        "APPLY" if apply else "DRY-RUN",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
