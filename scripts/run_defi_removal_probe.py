#!/usr/bin/env python3
# Epic: defi_consolidated_closeout
# Lifecycle: recurring
# Delete-when: never (recurring daily Cloud Run job — the DeFi removal truth-gate)
"""Daily DeFi on-chain removal probe (Option B) — the Cloud Run job entrypoint.

Reads the defi ``prod/catalog.parquet``, probes each currently-LIVE EVM-addressed
instrument on-chain (``eth_getCode`` at latest block), and writes/merges the removal
side-artifact ``_cache/defi_removals.json`` that ``build_instrument_catalogue`` reads
to set ``delisted_at`` (Option A carve-out's preserved truth-gate). Only POSITIVELY
confirmed-gone contracts are recorded — see ``instruments_service.oracle.defi_removal_probe``.

Usage:
  python scripts/run_defi_removal_probe.py --dry-run                # probe + report, NO write
  python scripts/run_defi_removal_probe.py --apply                  # probe + merge-write the artifact
  python scripts/run_defi_removal_probe.py --apply --limit 200      # cap targets (smoke)

Order in the close-out: this ADDS delistings for genuinely-gone contracts; it never
re-creates the false-delistings Option A removed (uncertainty → stays live).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

from instruments_service.oracle.defi_removal_probe import probe_catalogue_removals, write_removals

logger = logging.getLogger(__name__)


def _load_catalogue() -> pd.DataFrame:
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    raw = get_storage_client().download_bytes(bucket, "prod/catalog.parquet")
    return pd.read_parquet(io.BytesIO(raw))


async def _run(*, apply: bool, limit: int | None, concurrency: int) -> int:
    catalogue = _load_catalogue()
    logger.info("removal probe: loaded %d catalogue rows", len(catalogue))
    removals = await probe_catalogue_removals(catalogue, as_of=datetime.now(UTC), concurrency=concurrency, limit=limit)
    if not removals:
        logger.info("removal probe: 0 confirmed on-chain removals (all live) — nothing to write")
        return 0
    for r in removals[:50]:
        logger.info(
            "  CONFIRMED GONE: %s (%s %s) delisted_at=%s block=%d",
            r.canonical_id,
            r.chain,
            r.address[:12],
            r.delisted_at,
            r.probe_block,
        )
    if apply:
        total = write_removals(removals)
        logger.info("removal probe: merged %d new → %d total removals written", len(removals), total)
    else:
        logger.info("[dry-run] would write %d removals (merged into the existing artifact)", len(removals))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="cap probe targets (smoke/testing)")
    parser.add_argument("--concurrency", type=int, default=4, help="max concurrent RPC probes")
    args = parser.parse_args(argv)
    return asyncio.run(_run(apply=bool(args.apply), limit=args.limit, concurrency=int(args.concurrency)))


if __name__ == "__main__":
    raise SystemExit(main())
