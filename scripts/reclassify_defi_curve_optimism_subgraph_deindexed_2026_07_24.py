#!/usr/bin/env python3
# Epic: defi_consolidated_closeout
# Lifecycle: oneoff
# Delete-when: after the defi _index has 0 CURVE/OPTIMISM dex_pool_swaps attempted_failed rows
#   matching the dead-subgraph cascade error + honest_cov re-measured
"""Reclassify CURVE/OPTIMISM ``dex_pool_swaps`` false ``attempted_failed`` rows → typed empty_confirmed.

CURVE's OPTIMISM subgraph (``CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX``, from UAC
``registry.capability_declarations._defi.SUBGRAPH_IDS["curve"]["OPTIMISM"]``) has ZERO indexer
allocations on The Graph's decentralized network — live-probed 2026-07-15:
``{"errors":[{"message":"subgraph not found: no allocations"}]}`` (HTTP 200). This is a
PERMANENT indexer-economics condition, not a transient/schema-drift issue: every backfill attempt
against this (venue, chain) pair fails identically and burns all 5 cascade query schemas before
``dex_swaps_handler.py._run_cascade`` (market-tick-data-service) raises a generic RuntimeError
("All 5 cascade schemas returned GraphQL errors for curve/OPTIMISM ... add a matching query
schema or update the existing one") — misleading, since no schema change can fix a de-indexed
subgraph. 952 of the ~1,038-row ``dex_pool_swaps`` long tail (92%) are this exact cause, spanning
``date`` 2021-01-01..2026-06-25 with ``attempted_at`` as recent as 2026-07-10 — a live, ongoing
condition, not a one-time blip. See ``EmptyConfirmedReason.EXPECTED_SUBGRAPH_DEINDEXED``
(unified-api-contracts) for the full taxonomy entry and
``plans/active/issues/defi_curve_optimism_subgraph_no_allocations_2026_07_15.md`` for the root
cause + live verification.

Selection is PURE manifest-row classification (no catalogue cross-reference needed, mirrors
``reclassify_defi_reference_only_eu_2026_07_21.py``): a row is reclassified iff
``capture_status == 'attempted_failed'`` AND ``venue`` (case-insensitive) == ``CURVE`` AND
``chain`` (case-insensitive) == ``OPTIMISM`` (when the ``chain`` column is present) AND
``data_type == 'dex_pool_swaps'`` (when present) AND ``error_reason`` contains the untruncated
message prefix ``"All 5 cascade schemas returned GraphQL errors for curve/OPTIMISM"`` — this
phrase, not the full subgraph id, is the match target because live inspection of prod
``_index/availability_index.parquet`` (2026-07-24) proved the ``error_reason`` column is
TRUNCATED AT 80 CHARS on write: every real matching row's stored value ends exactly at
``"...(subgraph=CXDZP"`` (only 5 of the dead subgraph id's 45 characters survive). An earlier
version of this script matched on the full 45-char subgraph id and silently found 0 rows against
a corpus independently confirmed (via direct pandas inspection) to hold 144 genuine matches —
this prefix is the fix. Combined with the exact venue/chain/data_type/capture_status filters,
still fully unambiguous: this phrase can only be produced by ``dex_swaps_handler.py._run_cascade``
for curve/OPTIMISM specifically (the other ~86 long-tail rows in the same data_type are
UNISWAP_V3 timeouts / schema drift / etc., explicitly NOT this condition per the issue doc's
live spot-checks).

Mirrors the sibling scripts' blob set (consolidated ``_index/availability_index.parquet`` +
``_index/per_vm/*.parquet``), backup-then-write, idempotent, ``--dry-run``/``--apply``.

Usage:
  python scripts/reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py --dry-run
  python scripts/reclassify_defi_curve_optimism_subgraph_deindexed_2026_07_24.py --apply

**``--apply`` MUST run on a VM in-region, never the operator's local machine** — this script reads
+ rewrites the WHOLE ~985 MB consolidated ``_index/availability_index.parquet`` (23.9M rows) to
flip 144 rows, which is exactly the "manifest-index read-transform-write over the whole `_index`"
case the heavy-I/O hard rule (``/codex/05-infrastructure/vm-launcher-runbook.md``) targets. Launch
via the generic ``canonical-migration`` ``VM_TASK``/``VM_MIGRATION_CMD`` dispatch in
``deployment-service/scripts/vm/setup-data-pipeline-vm.sh`` (fits scripts with no dedicated
launcher yet) with ``VM_SERVICE=instruments_service``, or extend
``launch-canonical-migration-vm.sh``'s category table with a new one-off entry — do not run
``--apply`` from an interactive per-tab session.

Follow-up (out of scope for this script, tracked in the issue doc): the market-tick-data-service
writer (``dex_swaps_handler.py._run_cascade``) does not yet recognize this condition at fetch
time, so a future backfill re-run against CURVE/OPTIMISM will re-create ``attempted_failed`` rows
until that writer-side fix ships — this script only reconciles the EXISTING historical backlog.
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
_DEINDEXED = EmptyConfirmedReason.EXPECTED_SUBGRAPH_DEINDEXED.value
_VENUE = "CURVE"
_CHAIN = "OPTIMISM"
_DATA_TYPE = "dex_pool_swaps"
_DEAD_SUBGRAPH_ID = "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX"
# LIVE-VERIFIED 2026-07-24: the manifest's `error_reason` column is truncated at 80 chars on
# write (confirmed by direct inspection of prod `_index/availability_index.parquet` — every
# matching row's stored value ends exactly at "...(subgraph=CXDZP", i.e. only the first 5 of the
# 45-char subgraph id survive). Matching on the FULL `_DEAD_SUBGRAPH_ID` string therefore NEVER
# hits (verified: an earlier version of this script matching the full id found 0 rows against a
# live corpus independently confirmed to hold 144 genuine matches). Match on this untruncated
# PREFIX instead — it comfortably fits within the 80-char cap and, combined with the exact
# venue/chain/data_type/capture_status filters below, is still fully unambiguous (this exact
# phrase can only be produced by `dex_swaps_handler.py._run_cascade`'s cascade-exhaustion
# RuntimeError for curve/OPTIMISM).
_ERROR_REASON_PREFIX = "All 5 cascade schemas returned GraphQL errors for curve/OPTIMISM"


def _defi_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.deindexed.bak.parquet"
    return f"{blob_name}.{run_ts}.deindexed.bak"


def _expected_false_value(df: pd.DataFrame) -> object:
    """The frame's OWN encoding of a falsy ``expected`` (mirrors the sibling reclassify scripts —
    deriving it rather than hard-coding a Python ``False`` avoids the ``ArrowTypeError`` hit
    2026-07-20 when the column turned out to be a parquet STRING type ('true'/'false'), not bool."""
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


def _select_flip_mask(df: pd.DataFrame) -> pd.Series[bool]:
    """True for rows matching the exact CURVE/OPTIMISM dead-subgraph cascade-failure signature."""
    needed = ("capture_status", "venue", "error_reason")
    if not all(c in df.columns for c in needed):
        return pd.Series(False, index=df.index)
    is_failed = df["capture_status"].astype(str) == "attempted_failed"
    venue_match = df["venue"].astype(str).str.strip().str.upper() == _VENUE
    subgraph_match = df["error_reason"].astype(str).str.contains(_ERROR_REASON_PREFIX, regex=False, na=False)
    mask = is_failed & venue_match & subgraph_match
    if "chain" in df.columns:
        chain_match = df["chain"].astype(str).str.strip().str.upper() == _CHAIN
        mask = mask & chain_match
    if "data_type" in df.columns:
        dt_match = df["data_type"].astype(str).str.strip() == _DATA_TYPE
        mask = mask & dt_match
    return mask


def reclassify_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Flip CURVE/OPTIMISM dead-subgraph attempted_failed rows → typed empty_confirmed. Pure + idempotent."""
    stats = {"reclassified": 0, "untouched": 0}
    if df.empty:
        return df, stats
    flip = _select_flip_mask(df)
    n = int(flip.sum())
    if n == 0:
        stats["untouched"] = len(df)
        return df, stats
    work = df.copy()
    work.loc[flip, "capture_status"] = "empty_confirmed"
    work.loc[flip, "error_reason"] = _DEINDEXED
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
        "  %s: reclassified %d CURVE/OPTIMISM dead-subgraph attempted_failed → empty_confirmed[%s]",
        blob_name,
        stats["reclassified"],
        _DEINDEXED,
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
        "Target: venue=%s chain=%s data_type=%s subgraph_id=%s; mode=%s",
        _VENUE,
        _CHAIN,
        _DATA_TYPE,
        _DEAD_SUBGRAPH_ID,
        "APPLY" if apply else "DRY-RUN",
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
        "TOTAL reclassified %d CURVE/OPTIMISM dead-subgraph attempted_failed → empty_confirmed[%s] (mode=%s)",
        total,
        _DEINDEXED,
        "APPLY" if apply else "DRY-RUN",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
