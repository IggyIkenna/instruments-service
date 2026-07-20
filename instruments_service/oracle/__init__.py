"""DeFi on-chain oracle — truth-gate probes over the reference catalogue.

Currently houses the removal probe (Option B of the ``available_to`` false-delisting
close-out, ``defi_catalogue_available_to_false_delisting_2026_07_20``): a per-day job
that confirms whether a DeFi instrument's on-chain CONTRACT is gone (``eth_getCode``
empty / absent Solana account) and emits a removal side-artifact the lifecycle roll-up
reads to set ``delisted_at`` — the truth-gate seam the Option A carve-out preserves.
"""

from instruments_service.oracle.defi_removal_probe import (
    RemovalRecord,
    load_removal_delisted_at_map,
    probe_catalogue_removals,
)

__all__ = ["RemovalRecord", "load_removal_delisted_at_map", "probe_catalogue_removals"]
