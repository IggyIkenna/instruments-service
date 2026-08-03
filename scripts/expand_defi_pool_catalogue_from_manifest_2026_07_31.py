#!/usr/bin/env python3
"""One-off: expand the DeFi pool catalogue (``prod/catalog.parquet``) with every
ever-captured pool address for every default DEX protocol, per the operator-ruled
full-completion mandate (2026-07-28) —
issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md.

**Root cause this closes**: ``_catalogue_filter.py``'s catalogue-as-filter only lists
"currently-active" pools (a forward-looking discovery snapshot), so any pool that was
ever captured under the OLD, catalogue-agnostic address-keyed path but never resolved
into a symbol-named record is invisible to the catalogue's ``[available_from,
available_to]`` window and never expected again. A prior investigation measured this
gap at ~74% of historical capture for 4 sampled protocols
(``issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md``).

**Single-walk discipline**: this reads the MTDS availability manifest ONCE (the
authoritative per-(date,venue,chain,instrument_id) capture record — every address-keyed
capture already stamps its raw address as ``instrument_id``) — no fresh GCS directory
walk. The manifest's address-shaped ``instrument_id`` rows for ``dex_pool_state``/
``dex_pool_swaps`` ARE the exact same "no catalogue-covered replacement" population the
prior investigation's GCS-listing purge tool discovered by a slower, per-directory scan;
reading the manifest gets the identical answer for free.

**Merge safety**: reuses ``build_instrument_catalogue.py``'s own tested
``_merge_incremental(..., close_absent=False)`` — the SAME frozen-tail merge a
``--mode full`` rebuild uses (see its call site) — so this script can ONLY add/extend
rows (a pool the catalogue already knows about gets its ``available_from`` widened to
the earlier date now evidenced; a pool it never knew about is appended), never
delist/shrink anything. ``promote_catalogue``'s monotonic guard is the second
independent safety net (refuses any accidental shrink).

**Invocation (bounded-by-construction, required for remaining runs)**: run this script via
``scripts/run_expand_defi_pool_catalogue_bounded.sh`` — it wraps this exact command under
``unified-trading-pm``'s ``scripts/dev/run-bounded-analysis.sh`` memory-cap wrapper (a
documented ~9.5GiB peak, see
``issues/expand_defi_pool_catalogue_script_unbounded_memory_2026_07_31.md``), so a future
run against one of the other 11 default DEX protocols dies cleanly at the cap instead of
threatening the shared host if the column-pruning fix's bound is ever exceeded. Do not
invoke this script directly (``python3 expand_defi_pool_catalogue_from_manifest_2026_07_31.py``)
on the shared planning-vm.

**Scope**: all 12 EVM default DEX protocols (from UAC's own ``SUBGRAPH_IDS`` SSOT,
address-shaped ``instrument_id`` filtered via the SAME ``0x[hex]`` regex the reference
purge tool uses) + ORCA/RAYDIUM/PHOENIX/KAMINO (Solana, base58-pubkey ``instrument_id``,
confirmed address-shaped by direct manifest sampling).

**KAMINO correction (2026-08-03,
issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md, the
KAMINO follow-up todo)**: an earlier pass of this script DELIBERATELY EXCLUDED kamino,
believing its ``dex_pool_state`` ``instrument_id`` values were UUID-shaped vault ids, not
a recognizable pool address. Direct manifest sampling disproves that: KAMINO's
``dex_pool_state`` rows carry 44-char base58 Solana addresses (e.g.
``BLP7UHUg1yNry94Qk3sM8pAfEyDhTZirwFghw9DoBjn7``, confirmed via
``_solana_defi_fetch.py::fetch_kamino_vault``'s ``vault_addr = s["address"]`` — the same
on-chain vault-strategy address instruments-service's own ``kamino.py`` adapter already
catalogues as ``pool_address``/``raw_symbol``). The UUID-shaped ids the earlier pass
actually saw belong to KAMINO's ``lending_indices`` rows (a DIFFERENT data_type this
script never reads — see ``_DEX_DATA_TYPES`` below) — the two data_types were conflated.
KAMINO is therefore included in ``_SOLANA_PROTOCOLS`` like its Solana DEX siblings; no new
discovery technique was needed.

# Epic: infrastructure_master
# Lifecycle: one-off -- DeFi pool catalogue historical-discovery expansion
# Delete-when: issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md
#   is resolved and every default DEX protocol's catalogue population has been verified
#   against the historical capture corpus.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
from unified_api_contracts.registry.capability_declarations._defi import SUBGRAPH_IDS
from unified_trading_library import StorageClient, get_config, get_storage_client, resolve_bucket_name

# build_instrument_catalogue.py lives in this same repo's scripts/ dir — reuse its
# tested merge/tag/promote machinery rather than re-implementing catalogue writes.
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

_MANIFEST_BUCKET = "market-data-tick-defi-prd-central-element-323112"
_MANIFEST_BLOB = "_index/availability_index.parquet"
_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{20,}$")

_DEX_DATA_TYPES = ("dex_pool_state", "dex_pool_swaps")

#: Solana protocols confirmed address-shaped (base58 pubkey) instrument_id via direct
#: manifest sampling for their dex_pool_state rows (see module docstring's KAMINO
#: correction, 2026-08-03 — KAMINO's UUID-shaped ids live in lending_indices, a
#: different data_type this script never reads).
_SOLANA_PROTOCOLS: tuple[str, ...] = ("ORCA", "RAYDIUM", "PHOENIX", "KAMINO")


def _evm_protocol_chain_pairs() -> list[tuple[str, str]]:
    """Every (VENUE, CHAIN) pair from UAC's SUBGRAPH_IDS SSOT — the default EVM DEX
    protocol universe (the authoritative "which protocols are in scope" driver, not
    just whatever happens to already appear in the manifest)."""
    pairs: list[tuple[str, str]] = []
    for protocol, chains in SUBGRAPH_IDS.items():
        for chain in chains:
            pairs.append((protocol.upper(), chain.upper()))
    return pairs


def discover_gap_addresses(manifest: pd.DataFrame) -> pd.DataFrame:
    """Every distinct address-shaped ``instrument_id`` ever captured for a default DEX
    protocol/chain, with its manifest-evidenced ``[available_from, available_to]``.

    Returns a DataFrame with columns ``venue, chain, pool_address, available_from,
    available_to`` — one row per distinct (venue, chain, pool_address).
    """
    sub = manifest[manifest["data_type"].isin(_DEX_DATA_TYPES) & (manifest["capture_status"] == "captured")].copy()
    sub["venue"] = sub["venue"].astype(str).str.upper()
    sub["chain"] = sub["chain"].astype(str).str.upper()
    sub["instrument_id"] = sub["instrument_id"].astype(str)

    # Vectorised (venue,chain)-pair membership — a row-wise .apply() over the full
    # multi-million-row manifest is orders of magnitude slower than a string-key isin().
    evm_pair_keys = {f"{v}|{c}" for v, c in _evm_protocol_chain_pairs()}
    is_evm_pair = (sub["venue"] + "|" + sub["chain"]).isin(evm_pair_keys)
    is_evm_addr = sub["instrument_id"].str.match(_ADDR_RE)
    evm_rows = sub[is_evm_pair & is_evm_addr]

    is_solana = sub["venue"].isin(_SOLANA_PROTOCOLS) & (sub["chain"] == "SOLANA")
    solana_rows = sub[is_solana]

    gap = pd.concat([evm_rows, solana_rows], ignore_index=True)
    if gap.empty:
        return pd.DataFrame(columns=["venue", "chain", "pool_address", "available_from", "available_to"])

    grp = gap.groupby(["venue", "chain", "instrument_id"])["date"].agg(["min", "max"]).reset_index()
    grp = grp.rename(columns={"instrument_id": "pool_address", "min": "available_from", "max": "available_to"})
    grp["pool_address"] = grp["pool_address"].str.lower()
    return grp[["venue", "chain", "pool_address", "available_from", "available_to"]]


def build_window_df(gap_addresses: pd.DataFrame) -> pd.DataFrame:
    """Shape ``gap_addresses`` into a CATALOG_COLUMNS-compatible frame for
    ``_merge_incremental`` — a genuinely new "pool" row per discovered address, every
    non-pool-identity column blank (no symbol/token metadata is knowable from the
    manifest alone; downstream row-level symbol resolution is unaffected — see
    ``_catalogue_filter.catalogue_symbol_map_for_shard``'s own documented fallback)."""
    rows = []
    for rec in gap_addresses.to_dict("records"):
        row = dict.fromkeys(CATALOG_COLUMNS, "")
        row["instrument_id"] = f"{rec['venue']}-{rec['chain']}:POOL:{rec['pool_address']}"
        row["instrument_type"] = "pool"
        row["venue"] = rec["venue"]
        row["chain"] = rec["chain"]
        row["data_type"] = "dex_pool_state"
        row["pool_address"] = rec["pool_address"]
        row["available_from"] = str(rec["available_from"])
        row["available_to"] = str(rec["available_to"])
        rows.append(row)
    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    ap.add_argument("--project-id", default="central-element-323112")
    ap.add_argument("--apply", action="store_true", help="Actually promote the merged catalogue. Default DRY-RUN.")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    storage: StorageClient = get_storage_client(project_id=args.project_id)
    manifest_bytes = storage.download_bytes(_MANIFEST_BUCKET, _MANIFEST_BLOB)
    # Column-pruned read: the manifest carries ~50 columns (schema v9) but this script only
    # ever touches these 6 — loading the rest for all 29M+ rows was the dominant memory cost
    # (confirmed incident 2026-07-31: an unbounded-looking RSS climb on a run that only ever
    # produces a ~60K-row delta — see this script's Progress Log / the followup todo).
    manifest = pd.read_parquet(
        io.BytesIO(manifest_bytes),
        columns=["date", "venue", "chain", "data_type", "instrument_id", "capture_status"],
    )
    del manifest_bytes
    logger.info("Loaded manifest: %d total rows from gs://%s/%s", len(manifest), _MANIFEST_BUCKET, _MANIFEST_BLOB)

    gap_addresses = discover_gap_addresses(manifest)
    del manifest  # the 29M+ row frame is not needed past this point (see column-pruning note above)
    logger.info(
        "Discovered %d distinct (venue,chain,pool_address) gap rows across %d (venue,chain) pairs",
        len(gap_addresses),
        gap_addresses[["venue", "chain"]].drop_duplicates().shape[0] if not gap_addresses.empty else 0,
    )
    per_pair = gap_addresses.groupby(["venue", "chain"]).size().sort_values(ascending=False)
    for (venue, chain), count in per_pair.items():
        logger.info("  %s/%s: %d gap addresses", venue, chain, count)

    if gap_addresses.empty:
        logger.info("No gap addresses found — nothing to merge.")
        return 0

    window_df = build_window_df(gap_addresses)

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
        "Merged catalogue: %d rows (prev %d + %d gap rows, net of overlap)", len(merged), len(prev_df), len(window_df)
    )

    code = promote_catalogue(
        storage, bucket, env, merged, asset_group="defi", allow_shrink=False, dry_run=not args.apply
    )
    if not args.apply:
        logger.info("DRY-RUN complete — nothing written. Re-run with --apply to promote.")
    return code


if __name__ == "__main__":
    _code = main()
    # Force-terminate rather than sys.exit(): a non-daemon thread or connection-pool worker
    # left open by a storage-client dependency (unconfirmed which — see this script's
    # followup todo) kept a prior run's process alive and growing well past main()'s own
    # return, long enough to exhaust host memory (2026-07-31 incident). os._exit() skips
    # interpreter teardown/atexit entirely so no lingering non-daemon thread can hold the
    # process open — safe here since every write this script performs (promote_catalogue)
    # has already completed and returned by this point.
    os._exit(_code)
