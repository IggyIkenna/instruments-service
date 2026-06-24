#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 glued-not-in-catalogue EU rows + honest_cov re-measured
"""Reclassify defi GLUED-not-in-catalogue expected_unattempted → empty_confirmed NOT_LISTED.

After the generalized glued→canonical reconcile, the residual glued EU is ~63k
``KAMINO-SOLANA:VAULT:…`` rows: 112 distinct Kamino vaults that are NOT in the IS catalogue
(which lists Kamino POOLs, not these vaults) and are NOT in the DeFi MVP archetype matrix
(MVP = Aave/Compound lending + DEX pools). The catalogue is the SSOT universe; an instrument
NOT in it is genuinely not-listed → its EU is a phantom that should be `empty_confirmed` with
`EXPECTED_INSTRUMENT_NOT_LISTED` (a denominator-excluded honest absence), not a fetchable gap.

This flips ONLY EU rows whose instrument_id is GLUED (has ":", not a bare 0x) AND whose normed
form is ABSENT from the catalogue map (so a mapped/canonical/fetchable cell is never touched).
captured/empty/failed rows untouched. Vectorised, backup-then-write, idempotent.

Usage:
  python scripts/reclassify_defi_orphan_eu_notlisted_2026_06_24.py --dry-run
  python scripts/reclassify_defi_orphan_eu_notlisted_2026_06_24.py --apply
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import logging
import os.path
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import EmptyConfirmedReason
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_NOT_LISTED = EmptyConfirmedReason.EXPECTED_INSTRUMENT_NOT_LISTED.value


def _load_reconcile_module() -> object:
    """Reuse build_glued_to_canonical_map + _norm_glued from the sibling reconcile script."""
    path = os.path.join(os.path.dirname(__file__), "reconcile_defi_lending_manifest_canonical_2026_06_24.py")
    spec = importlib.util.spec_from_file_location("_recon", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load reconcile module at {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.orphannotlisted.bak.parquet"
    return f"{blob_name}.{run_ts}.orphannotlisted.bak"


def reclassify_frame(df: pd.DataFrame, gmap: dict[str, str], norm_fn: object) -> tuple[pd.DataFrame, dict[str, int]]:
    """Flip glued-not-in-catalogue EU → empty_confirmed NOT_LISTED. Pure + idempotent."""
    stats = {"reclassified": 0, "untouched": 0}
    if df.empty or "instrument_id" not in df.columns or "capture_status" not in df.columns:
        stats["untouched"] = len(df)
        return df, stats
    iid = df["instrument_id"].astype(str)
    is_eu = df["capture_status"].astype(str) == "expected_unattempted"
    is_glued = iid.str.contains(":", regex=False) & ~iid.str.startswith("0x")
    normed = iid.where(is_glued, "").map(lambda s: norm_fn(s) if s else "")  # type: ignore[operator]
    unmapped = normed.map(lambda k: bool(k) and k not in gmap)
    flip = is_eu & is_glued & unmapped
    n = int(flip.sum())
    if n == 0:
        stats["untouched"] = len(df)
        return df, stats
    work = df.copy()
    work.loc[flip, "capture_status"] = "empty_confirmed"
    if "error_reason" in work.columns:
        work.loc[flip, "error_reason"] = _NOT_LISTED
    if "expected" in work.columns:
        work.loc[flip, "expected"] = False
    stats["reclassified"] = n
    return work, stats


def _process_blob(
    storage: object, bucket: str, blob_name: str, run_ts: str, gmap: dict[str, str], norm_fn: object, *, apply: bool
) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = reclassify_frame(df, gmap, norm_fn)
    if stats["reclassified"] == 0:
        return stats
    logger.info("  %s: reclassified %d glued-not-in-catalogue EU → empty NOT_LISTED", blob_name, stats["reclassified"])
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

    recon = _load_reconcile_module()
    bucket = _defi_bucket()
    storage = get_storage_client(project_id=None)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    cat_raw = storage.download_bytes(recon._catalogue_bucket(), "prod/catalog.parquet")  # pyright: ignore[reportAttributeAccessIssue]
    gmap = recon.build_glued_to_canonical_map(pd.read_parquet(io.BytesIO(cat_raw)))  # pyright: ignore[reportAttributeAccessIssue]
    norm_fn = recon._norm_glued  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Catalogue map: %d entries; mode=%s", len(gmap), "APPLY" if apply else "DRY-RUN")

    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    total = 0
    for blob_name in blobs:
        total += _process_blob(storage, bucket, blob_name, run_ts, gmap, norm_fn, apply=apply).get("reclassified", 0)
    logger.info("TOTAL reclassified %d (mode=%s)", total, "APPLY" if apply else "DRY-RUN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
