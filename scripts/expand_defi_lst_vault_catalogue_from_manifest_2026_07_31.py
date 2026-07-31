#!/usr/bin/env python3
"""One-off: register the 6 pipeline-phase LST/vault venues
(ANKR/STADER/STAKEWISE/SWELL/MANTLE/MAKER-ETHEREUM) in the DeFi instruments catalogue
(``prod/catalog.parquet``) — todo 4 of
plans/active/defi_venue_pipeline_to_live_ao_build_2026_07_30.md.

**Why a manifest-driven script, not ``build_instrument_catalogue.py --mode incremental``**:
that script only ever reads ``instrument_availability/by_date/`` rows, which the daily
orchestrator only ever writes for venues already in ``_STATIC_DEFI_VENUES`` — these 6
venues are deliberately NOT wired there yet (that wiring is todo 5's `DEFI_VENUE_PHASE`
flip, done together in one commit per the invariant `phase=="live" <=> IS-producible`
in unified_api_contracts/registry/defi_venues.py). This script registers the catalogue
entries from the now-complete 90-day MTDS manifest capture (todo 3) WITHOUT touching
`_STATIC_DEFI_VENUES`/`DEFI_VENUE_PHASE`, mirroring
``expand_defi_pool_catalogue_from_manifest_2026_07_31.py``'s pattern of reusing
``build_instrument_catalogue.py``'s own tested merge/promote machinery for an
off-cycle registration.

**Instrument identity is NOT derived from the manifest's own instrument_id column**:
`vault_share_price_handler.py`'s `record_captured()` call never stamps `instrument_id`
(a separate, smaller gap than this todo — filed as a followup, not fixed inline here),
so MAKER's manifest rows carry a null instrument_id. Every field below (symbol,
on-chain contract address, underlying/quote asset) was instead read directly from this
session's own real, freshly-backfilled GCS objects (ground truth — not guessed),
matching the catalogue's existing ``{VENUE}-{CHAIN}:{TYPE}:{SYMBOL}`` convention
(confirmed against the live LIDO/ROCKETPOOL/PUFFER catalogue rows).

**Single-walk discipline, bounded read**: uses ``read_availability_index(...,
filters=[date range])`` (row-group predicate pushdown) rather than a raw full-manifest
download — the exact fix that closed the sibling incident
(``issues/expand_defi_pool_catalogue_script_unbounded_memory_2026_07_31.md`` /
``defi_venue_pipeline_to_live_ao_build_2026_07_30.md``'s Progress Log) applied
proactively here instead of repeating it. Only used to confirm each venue's
[available_from, available_to] window from genuinely captured rows — never to
fabricate a window with no manifest evidence.

**Merge safety**: reuses ``build_instrument_catalogue.py``'s own tested
``_merge_incremental(..., close_absent=False)`` (append/widen only, never
delist/shrink) + ``promote_catalogue``'s monotonic-guard second safety net.

# Epic: infrastructure_master
# Lifecycle: one-off -- 6-venue LST/vault catalogue registration (defi_venue_pipeline_to_live_ao_build_2026_07_30.md todo 4)
# Delete-when: defi_venue_pipeline_to_live_ao_build_2026_07_30.md is archived
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from unified_trading_library import StorageClient, get_config, get_storage_client, resolve_bucket_name
from unified_trading_library.manifest_writer._read_index import read_availability_index

sys.path.insert(0, str(Path(__file__).parent))
from build_instrument_catalogue import (
    CATALOG_COLUMNS,
    _add_equity_tags,
    _add_force_include,
    _add_instrument_name,
    _add_mvp_column,
    _catalogue_object_paths,
    _load_previous_catalogue,
    _merge_incremental,
    promote_catalogue,
)

logger = logging.getLogger(__name__)

_BACKFILL_WINDOW = ("2026-05-02", "2026-07-30")  # matches todo 3's completed 90-day backfill

#: (venue, data_type, instrument_type, symbol, contract_address, underlying) — every
#: field ground-truthed by reading this session's own real, freshly-written GCS
#: objects (see module docstring), not guessed.
_TARGETS: tuple[tuple[str, str, str, str, str, str], ...] = (
    ("ANKR", "lst_rates", "LST", "ankrETH", "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb", "ETH"),
    ("STADER", "lst_rates", "LST", "ETHx", "0xcf5EA1b38380f6aF39068375516Daf40Ed70D299", "ETH"),
    ("STAKEWISE", "lst_rates", "LST", "osETH", "0x2A261e60FB14586B474C208b1B7AC6D0f5000306", "ETH"),
    ("SWELL", "lst_rates", "LST", "swETH", "0xf951E335afb289353dc249e82926178EaC7DEd78", "ETH"),
    ("MANTLE", "lst_rates", "LST", "mETH", "0xe3cBd06D7dadB3F4e6557bAb7EdD924CD1489E8f", "ETH"),
    ("MAKER", "vault_share_price", "YIELD_BEARING", "sDAI", "0x83F20F44975D03b1b09e64809B757c47f942BEeA", "DAI"),
)

_CHAIN = "ETHEREUM"


def confirm_captured_windows(bucket: str) -> dict[str, tuple[str, str]]:
    """Per-target-venue [available_from, available_to] from GENUINELY captured manifest
    rows in the backfill window — never fabricated for a venue with zero evidence."""
    cols = ["date", "venue", "data_type", "capture_status"]
    filters = [("date", ">=", _BACKFILL_WINDOW[0]), ("date", "<=", _BACKFILL_WINDOW[1])]
    df = read_availability_index(bucket, columns=cols, filters=filters)
    windows: dict[str, tuple[str, str]] = {}
    for venue, data_type, *_rest in _TARGETS:
        sub = df[
            (df["venue"].astype(str).str.upper() == venue)
            & (df["data_type"] == data_type)
            & (df["capture_status"] == "captured")
        ]
        if sub.empty:
            continue
        dates = sub["date"].astype(str).str[:10]
        windows[venue] = (dates.min(), dates.max())
    return windows


def build_window_df(windows: dict[str, tuple[str, str]]) -> pd.DataFrame:
    """One catalogue row per target instrument with a confirmed captured window."""
    rows = []
    for venue, _data_type, instrument_type, symbol, contract, underlying in _TARGETS:
        if venue not in windows:
            logger.warning("  %s: NO captured rows in the backfill window — skipping (no fabricated entry)", venue)
            continue
        available_from, _last_seen = windows[venue]
        row = dict.fromkeys(CATALOG_COLUMNS, "")
        instrument_id = f"{venue}-{_CHAIN}:{instrument_type}:{symbol.upper()}"
        row["instrument_id"] = instrument_id
        row["instrument_type"] = instrument_type
        row["venue"] = venue
        row["chain"] = _CHAIN
        row["available_from"] = available_from
        row["available_to"] = None  # still live / no delisting evidence
        row["underlying"] = underlying
        row["raw_symbol"] = contract
        row["base_asset"] = underlying
        row["canonical_instrument_id"] = instrument_id
        row["base_asset_contract_address"] = contract
        rows.append(row)
    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    ap.add_argument("--project-id", default="central-element-323112")
    ap.add_argument("--apply", action="store_true", help="Actually promote the merged catalogue. Default DRY-RUN.")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    manifest_bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")
    windows = confirm_captured_windows(manifest_bucket)
    logger.info("Confirmed captured windows for %d/%d target venues: %s", len(windows), len(_TARGETS), windows)

    window_df = build_window_df(windows)
    if window_df.empty:
        logger.info("No target venues have confirmed captured data — nothing to register.")
        return 0
    logger.info("Built %d new catalogue row(s): %s", len(window_df), window_df["instrument_id"].tolist())

    storage: StorageClient = get_storage_client(project_id=args.project_id)
    env = get_config("DEPLOYMENT_ENV", "prod")
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    canonical_blob, _temp_blob = _catalogue_object_paths(env)
    prev = _load_previous_catalogue(storage, bucket, canonical_blob)
    prev_df = prev[0] if prev is not None else pd.DataFrame(columns=CATALOG_COLUMNS)
    logger.info("Current catalogue: %d rows at gs://%s/%s", len(prev_df), bucket, canonical_blob)

    merged = _merge_incremental(prev_df, window_df, window_start=None, asset_group="defi", close_absent=False)
    merged = _add_mvp_column(merged, "defi")
    merged = _add_equity_tags(merged, "defi")
    merged = _add_instrument_name(merged, "defi")
    merged = _add_force_include(merged, "defi")
    logger.info(
        "Merged catalogue: %d rows (prev %d + %d new rows, net of overlap)", len(merged), len(prev_df), len(window_df)
    )

    code = promote_catalogue(
        storage, bucket, env, merged, asset_group="defi", allow_shrink=False, dry_run=not args.apply
    )
    if not args.apply:
        logger.info("DRY-RUN complete — nothing written. Re-run with --apply to promote.")
    return code


if __name__ == "__main__":
    _code = main()
    # os._exit(), not sys.exit(): mirrors expand_defi_pool_catalogue_from_manifest_2026_07_31.py's
    # defensive fix for a lingering non-daemon storage-client thread that kept a prior sibling
    # script's process alive past main()'s own return. Safe here — every write (promote_catalogue)
    # has already completed and returned by this point.
    os._exit(_code)
