#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   canonical_instrument_id_cefi_defi_backfill_2026_07_14.md Progress Log)
"""Backfill canonical_instrument_id + fix glued_pair_id's venue prefix, in-place on the live DeFi catalog.

A `--mode full` rebuild is NOT currently safe for the DeFi catalog (see
defi_lending_atoken_debttoken_instrument_split_2026_07_07.md's 2026-07-14
entry — it silently reverts a separate, unrelated catalog-only migration).
This script performs the SAME two fixes a full rebuild would have applied,
directly on the live `prod/catalog.parquet`, without touching `by_date` or
re-deriving any row from scratch:

1. ``canonical_instrument_id`` backfill — blank -> ``instrument_id``. This is
   always correct by construction, not an approximation: for non-pool rows
   the catalog's own ``instrument_id`` already IS the resolved instrument_key
   form (canonical_instrument_id_cefi_defi_backfill_2026_07_14.md policy:
   canonical_instrument_id := instrument_key); for POOL rows, the catalog's
   ``instrument_id`` is BY DEFINITION ``pool_address.lower()``
   (build_instrument_catalogue.py::_defi_pool_dual_form's docstring), which is
   also exactly what canonical_instrument_id is defined to be for a pool row.
   So ``catalog.instrument_id == canonical_instrument_id`` holds for every
   row, pool or not — no recomputation needed, just a copy.

2. ``glued_pair_id`` venue-prefix fix — for POOL rows, the venue-chain prefix
   (everything before the first ``:``) gets the SAME idempotent regex
   ``unified_api_contracts.canonical.crosscutting.defi._insert_version_underscore``
   applies (``UNISWAPV3`` -> ``UNISWAP_V3``). Only the prefix segment is
   touched — the rest of the string (``:POOL:<pair>:<fee>``) is untouched, so
   this does NOT require quote_asset/fee/base_asset (columns the catalog
   doesn't carry) to recompute the full glued_pair_id from scratch.

Idempotent: a row whose canonical_instrument_id is already set, or whose
glued_pair_id prefix already has the underscore, is left alone.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_defi_canonical_id_and_glued_prefix_2026_07_14.py --dry-run
    .venv/bin/python scripts/backfill_defi_canonical_id_and_glued_prefix_2026_07_14.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"

# Mirrors unified_api_contracts.canonical.crosscutting.defi._insert_version_underscore
# exactly (same regex) — applied here only to the already-existing glued_pair_id
# prefix segment, never to a freshly-derived string.
_VERSION_UNDERSCORE_RE = re.compile(r"([A-Za-z])V(\d)")


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(run_ts: str) -> str:
    return f"prod/catalog.{run_ts}.canonicalidgluedprefix.bak.parquet"


def _fix_glued_prefix(glued_pair_id: str) -> str:
    """Insert the version underscore into a glued_pair_id's venue-chain prefix only."""
    if ":" not in glued_pair_id:
        return glued_pair_id
    prefix, rest = glued_pair_id.split(":", 1)
    return f"{_VERSION_UNDERSCORE_RE.sub(r'\1_V\2', prefix)}:{rest}"


def migrate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return the patched dataframe + a summary of rows touched."""
    out = df.copy()

    canonical_blank = out["canonical_instrument_id"].fillna("") == ""
    out.loc[canonical_blank, "canonical_instrument_id"] = out.loc[canonical_blank, "instrument_id"]

    glued_nonblank = out["glued_pair_id"].fillna("") != ""
    fixed_prefix = out.loc[glued_nonblank, "glued_pair_id"].map(_fix_glued_prefix)
    prefix_changed_count = int((fixed_prefix != out.loc[glued_nonblank, "glued_pair_id"]).sum())
    out.loc[glued_nonblank, "glued_pair_id"] = fixed_prefix

    summary = {
        "canonical_instrument_id_backfilled": int(canonical_blank.sum()),
        "glued_pair_id_prefix_fixed": prefix_changed_count,
        "rows_total": len(out),
    }
    return out, summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    bucket = _catalogue_bucket()
    client = get_storage_client(project_id=None)
    logger.info("Reading gs://%s/%s", bucket, _CATALOG_BLOB)
    raw = client.download_bytes(bucket, _CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d catalogue rows", len(df))

    patched, summary = migrate(df)
    for k, v in summary.items():
        logger.info("  %s = %d", k, v)

    if args.dry_run:
        logger.info("[dry-run] no cloud writes performed")
        return 0

    if summary["canonical_instrument_id_backfilled"] == 0 and summary["glued_pair_id_prefix_fixed"] == 0:
        logger.info("Nothing to fix; catalogue already fully backfilled/prefixed.")
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_blob = _backup_path(run_ts)
    logger.info("Backing up current catalogue to gs://%s/%s", bucket, backup_blob)
    client.upload_bytes(bucket, backup_blob, raw)  # pyright: ignore[reportAttributeAccessIssue]

    buf = io.BytesIO()
    patched.to_parquet(buf, index=False)
    buf.seek(0)
    logger.info("Writing patched catalogue (%d rows) to gs://%s/%s", len(patched), bucket, _CATALOG_BLOB)
    client.upload_bytes(bucket, _CATALOG_BLOB, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
