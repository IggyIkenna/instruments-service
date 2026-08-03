#!/usr/bin/env python3
# Epic: defi_consolidated_closeout
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 expected_unattempted rows on SPOT_ASSET/A_TOKEN/DEBT_TOKEN + honest_cov re-measured
"""Reclassify dangling defi EU rows on reference-only instrument_types → typed empty_confirmed.

SPOT_ASSET/A_TOKEN/DEBT_TOKEN rows are derived siblings of a parent POOL/LENDING row's on-chain
token leg with NO per-day market-data capture path, by construction (see EmptyConfirmedReason.
EXPECTED_REFERENCE_ONLY_NO_CAPTURE_PATH, unified_api_contracts). ``_enumerate_v2_defi`` now seeds
this typed reason directly going forward (instruments-service@a516bd01), but the 215,864-cell
backlog created BEFORE that fix (the ``available_to`` false-delisting fix reset these instruments
to catalogue-LIVE, and the enumerator re-seeded them as dangling ``expected_unattempted`` — a cell
that can never resolve to ``captured``) is still sitting in the manifest. This retroactively flips
that backlog to the typed reason.

Selection is PURE manifest-row classification (no catalogue cross-reference needed, unlike the
sibling undelist/reclassify_postdelist scripts): a row is reclassified iff
``capture_status == 'expected_unattempted'`` AND its own ``instrument_type`` (case-insensitive) is
in ``{spot_asset, a_token, debt_token}``. The row's mere existence already proves it survived the
enumerator's window-bound lifecycle check when seeded, so no catalogue lookup is needed. Mirrors
the sibling scripts' blob set (consolidated ``_index/availability_index.parquet`` +
``_index/per_vm/*.parquet``), backup-then-write, idempotent, ``--dry-run``/``--apply``.

Usage:
  python scripts/reclassify_defi_reference_only_eu_2026_07_21.py --dry-run
  python scripts/reclassify_defi_reference_only_eu_2026_07_21.py --apply

Order (load-bearing): (1) instruments-service@a516bd01 (enumerator fix) FIRST, so newly-seeded
cells never re-create this backlog; (2) THIS retroactive reconciliation.
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
_REFERENCE_ONLY = EmptyConfirmedReason.EXPECTED_REFERENCE_ONLY_NO_CAPTURE_PATH.value
_REFERENCE_ONLY_ITYPES = frozenset({"spot_asset", "a_token", "debt_token"})


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.refonly.bak.parquet"
    return f"{blob_name}.{run_ts}.refonly.bak"


def _expected_false_value(df: pd.DataFrame) -> object:
    """The frame's OWN encoding of a falsy ``expected`` (mirrors ``_expected_true_value`` in
    ``undelist_defi_false_postdelist_eu_2026_07_20.py``). An empty_confirmed row is no longer
    "should fetch", so ``expected`` must carry whatever falsy literal this frame's OTHER
    empty_confirmed rows already use — deriving it (rather than hard-coding a Python ``False``)
    avoids the exact ``ArrowTypeError`` hit on 2026-07-20 when the column turned out to be a
    parquet STRING type ('true'/'false'), not a bool."""
    if "expected" not in df.columns:
        return "false"
    col = df["expected"]
    if "capture_status" in df.columns:
        ec = col[df["capture_status"].astype(str) == "empty_confirmed"].dropna()
        if not ec.empty:
            return ec.iloc[0]
    nn = col.dropna()
    for v in nn.head(200):
        if str(v).strip().lower() in {"false", "0", "f", "no"}:
            return v
    return "false"


def reclassify_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Flip EU rows on reference-only instrument_types → typed empty_confirmed. Pure + idempotent."""
    stats = {"reclassified": 0, "untouched": 0}
    needed = ("capture_status", "instrument_type")
    if df.empty or not all(c in df.columns for c in needed):
        stats["untouched"] = len(df)
        return df, stats
    is_eu = df["capture_status"].astype(str) == "expected_unattempted"
    itype = df["instrument_type"].astype(str).str.strip().str.lower()
    flip = is_eu & itype.isin(_REFERENCE_ONLY_ITYPES)
    n = int(flip.sum())
    if n == 0:
        stats["untouched"] = len(df)
        return df, stats
    work = df.copy()
    work.loc[flip, "capture_status"] = "empty_confirmed"
    if "error_reason" in work.columns:
        work.loc[flip, "error_reason"] = _REFERENCE_ONLY
    if "expected" in work.columns:
        work.loc[flip, "expected"] = _expected_false_value(df)
    stats["reclassified"] = n
    return work, stats


def _process_blob(
    storage: object,
    bucket: str,
    blob_name: str,
    run_ts: str,
    *,
    apply: bool,
) -> dict[str, int]:
    try:
        raw = storage.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = reclassify_frame(df)
    if stats["reclassified"] == 0:
        return stats
    logger.info(
        "  %s: reclassified %d reference-only EU → empty_confirmed[%s]",
        blob_name,
        stats["reclassified"],
        _REFERENCE_ONLY,
    )
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
    logger.info(
        "Reference-only instrument_types: %s; mode=%s", sorted(_REFERENCE_ONLY_ITYPES), "APPLY" if apply else "DRY-RUN"
    )

    blobs = [_INDEX_BLOB]
    blobs += [
        b.name  # pyright: ignore[reportAttributeAccessIssue]
        for b in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if b.name.endswith(".parquet") and ".bak" not in b.name  # pyright: ignore[reportAttributeAccessIssue]
    ]
    total = 0
    for blob_name in blobs:
        total += _process_blob(storage, bucket, blob_name, run_ts, apply=apply).get("reclassified", 0)
    logger.info(
        "TOTAL reclassified %d reference-only EU → empty_confirmed[%s] (mode=%s)",
        total,
        _REFERENCE_ONLY,
        "APPLY" if apply else "DRY-RUN",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
