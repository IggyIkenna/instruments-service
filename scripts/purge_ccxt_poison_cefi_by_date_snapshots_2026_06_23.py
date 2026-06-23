#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: no cefi by_date snapshot carries a pre-2015 available_from + the catalogue rebuilt clean (verified 2026-06-23)
"""Scan-and-purge cefi by_date instrument snapshots with an IMPOSSIBLE pre-2015 ``available_from`` (incident 2026-06-23).

The cumulative cefi catalogue (``prod/catalog.parquet``) MINs ``available_from``
over all per-day snapshots, so a SINGLE poisoned snapshot pins a venue's
historical year-counts to an impossible date. Crypto venues genesis >= 2017;
``2010-01-01`` is the legacy CCXT-adapter placeholder (the adapter stamped a
2010 default for instruments lacking a per-instrument ``availableSince``).

PRIOR approach (now superseded): a HARDCODED list of 6 known-poison blobs. That
MISSED ``day=2026-04-02/venue={OKX-SPOT,OKX-FUTURES}`` (also full-2010 ccxt
dumps) -> OKX-SPOT stayed pinned to 2010 (inflated year-counts 2918->3584).
**Generalised 2026-06-23**: this now SCANS every
``instrument_availability/by_date/day=*/venue=*/instruments.parquet`` for an
``available_from_datetime`` (or legacy ``available_from``) earlier than
``--cutoff`` (default 2015-01-01) and purges every matching blob -- so a new
ccxt-era poison day cannot recur silently behind a stale hardcoded list.

A poisoned ccxt snapshot is ENTIRELY 2010 (the catch-all dump uses CCXT-format
ids ``BASE/QUOTE`` + a uniform 2010 placeholder), while every OTHER day for the
same venue is the clean Tardis snapshot (canonical ``VENUE:TYPE:BASE-QUOTE`` ids
+ real dates). Deleting the whole poisoned-day blob is therefore safe -- the
catalogue re-derives that venue's lifecycle from the clean snapshots on rebuild.
The source ccxt placeholder is already fixed (is@2217756); this removes the data
polluted before that fix.

Uses UTL ``gcs_delete_object`` (REST, never gsutil per-object) -- a manifest-row
DELETE, NOT a masking empty write. ``--apply`` deletes; default is dry-run.

SSOT: plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md.
"""

from __future__ import annotations

import argparse
import io
import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from unified_trading_library import (
    gcs_delete_object,
    get_storage_client,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_BY_DATE_PREFIX = "instrument_availability/by_date/"
_SNAPSHOT_SUFFIX = "instruments.parquet"
# Either column name has appeared in the snapshot schema over time.
_AVAILABLE_FROM_COLS: tuple[str, ...] = ("available_from_datetime", "available_from")
_SCAN_WORKERS = 32


def _earliest_available_from(df: pd.DataFrame) -> pd.Timestamp | None:
    """Return the minimum available-from across whichever column the snapshot carries (UTC)."""
    for col in _AVAILABLE_FROM_COLS:
        if col not in df.columns:
            continue
        series = pd.to_datetime(df[col], utc=True, errors="coerce")
        minimum = series.min()
        if pd.notna(minimum):
            return minimum
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry-run).")
    parser.add_argument(
        "--cutoff",
        default="2015-01-01",
        help="Snapshots whose earliest available_from is < this date are poison (default 2015-01-01; crypto genesis >= 2017).",
    )
    args = parser.parse_args()

    cutoff = pd.Timestamp(args.cutoff, tz="UTC")
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    logger.info("Bucket: %s | cutoff: %s", bucket, cutoff.date())

    client = get_storage_client()
    blob_names: list[str] = [
        blob.name for blob in client.list_blobs(bucket, prefix=_BY_DATE_PREFIX) if blob.name.endswith(_SNAPSHOT_SUFFIX)
    ]
    logger.info("Scanning %d by_date snapshots for available_from < cutoff ...", len(blob_names))

    def _scan(name: str) -> tuple[str, str] | None:
        blob = client.bucket(bucket).blob(name)
        df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        minimum = _earliest_available_from(df)
        if minimum is not None and minimum < cutoff:
            return (name, str(minimum.date()))
        return None

    poison: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=_SCAN_WORKERS) as executor:
        for result in executor.map(_scan, blob_names):
            if result is not None:
                poison.append(result)

    poison.sort()
    logger.info("Found %d poisoned snapshot(s):", len(poison))
    for name, earliest in poison:
        logger.info("  POISON earliest=%s  %s", earliest, name)

    deleted = 0
    for name, _earliest in poison:
        uri = f"gs://{bucket}/{name}"  # noqa: gs-uri — one-off purge; bucket already resolved via resolve_bucket_name SSOT
        if args.apply:
            gcs_delete_object(uri)
            logger.info("DELETED: %s", name)
            deleted += 1
        else:
            logger.info("[dry-run] would delete: %s", name)
            deleted += 1

    logger.info("%s %d poisoned snapshot(s).", "Deleted" if args.apply else "[dry-run] would delete", deleted)
    if not args.apply and poison:
        logger.info(
            "Re-run with --apply to delete, then rebuild: build_instrument_catalogue.py --asset-group cefi --allow-catalogue-shrink"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
