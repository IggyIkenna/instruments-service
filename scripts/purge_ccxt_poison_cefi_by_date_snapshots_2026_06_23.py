#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: the 6 stale ccxt-era cefi by_date snapshots are purged from prod + the catalogue rebuilt clean (verified 2026-06-23)
"""Purge the stale ccxt-era cefi by_date instrument snapshots (incident 2026-06-23).

The cumulative cefi catalogue (``prod/catalog.parquet``) carried an impossible
``available_from = 2010-01-01`` for COINBASE-SPOT + OKX-SWAP, plus two
non-canonical bare ``COINBASE`` / ``OKX`` venues. Root cause: SIX stale
``instrument_availability/by_date/`` snapshots written by the CCXT adapter
(NOT the canonical Tardis recon path):

* ``day=2026-04-02/venue={COINBASE-SPOT,OKX-SWAP}`` — CCXT-format ids
  (``00/USD`` / ``0G/USDC:USDC``), ``available_from_datetime=2010-01-01``
  (the now-fixed ccxt_adapter placeholder), PERPETUALs misfiled under
  COINBASE-SPOT. The catalogue rollup MINs ``available_from`` over these →
  pins COINBASE-SPOT/OKX-SWAP to 2010.
* ``day=2026-03-01,2026-03-02/venue={COINBASE,OKX}`` — bare non-canonical
  venue names (canonical is COINBASE-SPOT / OKX-SPOT) written by the same
  CCXT-era run.

Every OTHER by_date day for these venues is the clean Tardis snapshot
(canonical ``VENUE:TYPE:BASE-QUOTE`` ids + real ``available_from_datetime``),
so deleting only these 6 stale snapshots + rebuilding the catalogue removes the
2010 artifact AND the bare-venue pollution. The fix for the SOURCE (ccxt
adapter no longer stamps 2010) shipped in
``instruments_service/reference_data/adapters/cefi/ccxt_adapter.py``; this
script removes the data already polluted before that fix.

Uses UTL ``gcs_delete_object`` (REST, never gsutil per-object) — a manifest-row
DELETE, NOT a masking empty write. ``--apply`` deletes; default is dry-run.

SSOT: plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md.
"""

from __future__ import annotations

import argparse
import logging

from unified_trading_library import resolve_bucket_name
from unified_trading_library.cloud_interface import gcs_delete_object, gcs_describe_object

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# The exact stale blobs (enumerated against prod 2026-06-23). Closed set —
# deleting a whole-day snapshot for an OTHERWISE-valid venue (COINBASE-SPOT /
# OKX-SWAP on 2026-04-02) is intentional: that one day is entirely CCXT poison
# and every other day carries the clean Tardis snapshot.
_POISON_BLOBS: tuple[str, ...] = (
    "instrument_availability/by_date/day=2026-03-01/venue=COINBASE/instruments.parquet",
    "instrument_availability/by_date/day=2026-03-01/venue=OKX/instruments.parquet",
    "instrument_availability/by_date/day=2026-03-02/venue=COINBASE/instruments.parquet",
    "instrument_availability/by_date/day=2026-03-02/venue=OKX/instruments.parquet",
    "instrument_availability/by_date/day=2026-04-02/venue=COINBASE-SPOT/instruments.parquet",
    "instrument_availability/by_date/day=2026-04-02/venue=OKX-SWAP/instruments.parquet",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry-run).")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    logger.info("Bucket: gs://%s", bucket)

    deleted = 0
    for blob in _POISON_BLOBS:
        exists = gcs_describe_object(bucket=bucket, object_name=blob) is not None
        if not exists:
            logger.info("SKIP (absent): %s", blob)
            continue
        if args.apply:
            gcs_delete_object(bucket=bucket, object_name=blob)
            logger.info("DELETED: %s", blob)
            deleted += 1
        else:
            logger.info("[dry-run] would delete: %s", blob)
            deleted += 1

    logger.info("%s %d stale ccxt-poison snapshot(s).", "Deleted" if args.apply else "[dry-run] would delete", deleted)
    if not args.apply:
        logger.info("Re-run with --apply to delete, then rebuild: build_instrument_catalogue.py --asset-group cefi")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
