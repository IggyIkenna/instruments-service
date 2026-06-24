#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 EU rows past their catalogue available_to + honest_cov re-measured
"""Reclassify defi EU rows past their catalogue available_to → empty_confirmed DELISTED.

After the glued→canonical reconcile, some EU rows are canonical-keyed instruments whose
catalogue lifecycle says they were DELISTED before that date (``date > available_to``). An EU
cell after the instrument's delist date is a phantom — the catalogue (SSOT) says the
instrument is gone, so it is a GENUINE ``EXPECTED_INSTRUMENT_DELISTED`` empty, not a fetchable
gap. This flips ONLY EU rows whose canonical instrument_id maps (by instrument_id OR
raw_symbol address) to a catalogue row with a non-null ``available_to`` strictly before the
row's ``date``. captured/empty/failed untouched. Vectorised, backup-then-write, idempotent.

Usage:
  python scripts/reclassify_defi_postdelist_eu_2026_06_24.py --dry-run
  python scripts/reclassify_defi_postdelist_eu_2026_06_24.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import EmptyConfirmedReason
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_DELISTED = EmptyConfirmedReason.EXPECTED_INSTRUMENT_DELISTED.value


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.postdelist.bak.parquet"
    return f"{blob_name}.{run_ts}.postdelist.bak"


def build_available_to_map(catalogue: pd.DataFrame) -> dict[str, pd.Timestamp]:
    """``{canonical_instrument_id_lower: available_to_ts}`` for rows with a closed lifecycle.

    Keyed by BOTH the catalogue ``instrument_id`` AND its ``raw_symbol`` address (the manifest is
    now 0x-keyed), so a canonical-0x EU row resolves its delist date either way. Open (NaT)
    available_to is skipped (a live instrument never post-delists).
    """
    amap: dict[str, pd.Timestamp] = {}
    if catalogue.empty or "available_to" not in catalogue.columns:
        return amap
    at = pd.to_datetime(catalogue["available_to"], errors="coerce")
    for i, ts in at.items():
        if pd.isna(ts):
            continue
        for col in ("instrument_id", "raw_symbol"):
            v = str(catalogue.at[i, col] if col in catalogue.columns else "").strip().lower()
            # keep the LATEST available_to per key (most recent delist wins → conservative)
            if v and (v not in amap or ts > amap[v]):
                amap[v] = ts
    return amap


def reclassify_frame(df: pd.DataFrame, amap: dict[str, pd.Timestamp]) -> tuple[pd.DataFrame, dict[str, int]]:
    """Flip EU rows past their catalogue available_to → empty DELISTED. Pure + idempotent."""
    stats = {"reclassified": 0, "untouched": 0}
    if df.empty or not all(c in df.columns for c in ("capture_status", "instrument_id", "date")):
        stats["untouched"] = len(df)
        return df, stats
    is_eu = df["capture_status"].astype(str) == "expected_unattempted"
    iid = df["instrument_id"].astype(str).str.lower()
    at = iid.map(amap)  # NaT when no closed-lifecycle catalogue match
    d = pd.to_datetime(df["date"], errors="coerce")
    flip = is_eu & at.notna() & (d > at)
    n = int(flip.sum())
    if n == 0:
        stats["untouched"] = len(df)
        return df, stats
    work = df.copy()
    work.loc[flip, "capture_status"] = "empty_confirmed"
    if "error_reason" in work.columns:
        work.loc[flip, "error_reason"] = _DELISTED
    if "expected" in work.columns:
        work.loc[flip, "expected"] = False
    stats["reclassified"] = n
    return work, stats


def _process_blob(
    storage: object, bucket: str, blob_name: str, run_ts: str, amap: dict[str, pd.Timestamp], *, apply: bool
) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = reclassify_frame(df, amap)
    if stats["reclassified"] == 0:
        return stats
    logger.info("  %s: reclassified %d post-delist EU → empty DELISTED", blob_name, stats["reclassified"])
    if apply:
        backup = _backup_path(blob_name, run_ts)
        storage.upload_bytes(bucket, backup, raw)  # pyright: ignore[reportAttributeAccessIssue]
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        storage.upload_bytes(bucket, blob_name, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
        logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, len(out))
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
    storage = get_storage_client(project_id=None)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    cat_raw = storage.download_bytes(_catalogue_bucket(), "prod/catalog.parquet")  # pyright: ignore[reportAttributeAccessIssue]
    amap = build_available_to_map(pd.read_parquet(io.BytesIO(cat_raw)))
    logger.info(
        "Catalogue available_to map: %d closed-lifecycle entries; mode=%s", len(amap), "APPLY" if apply else "DRY-RUN"
    )

    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    total = 0
    for blob_name in blobs:
        total += _process_blob(storage, bucket, blob_name, run_ts, amap, apply=apply).get("reclassified", 0)
    logger.info("TOTAL reclassified %d post-delist EU (mode=%s)", total, "APPLY" if apply else "DRY-RUN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
