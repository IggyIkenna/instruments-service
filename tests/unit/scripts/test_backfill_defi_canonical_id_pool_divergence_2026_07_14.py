"""Authoritative pinning test — DeFi POOL rows DIVERGE (Option A, two-id model).

Operator ruling 2026-07-18 (defi_consolidated_closeout_2026_07_18.md, "the two-id
model" / "POOL-id policy = Option A (test wins)"): a DeFi POOL row carries TWO ids
that INTENTIONALLY diverge —

  * ``instrument_id``            = the pool ADDRESS (``pool_address.lower()``) — the
                                   machine/operational key (manifest + MTDS content-join).
  * ``canonical_instrument_id``  = the SYMBOLIC glued 3-segment key
                                   ``VENUE-CHAIN:POOL:BASE-QUOTE[-FEE_BPS]`` (the
                                   ``glued_pair_id`` column), NOT the address.

For a SPOT_ASSET (single token) the two CONVERGE by construction; for a POOL they
DIVERGE. This test pins both layers of that contract:

  1. the UAC SSOT ``build_pool_identity`` — its ``canonical_instrument_id`` (machine
     address) and ``glued_pair_id`` (symbolic 3-seg) diverge for a pool; and
  2. the in-place backfill ``migrate()`` code path
     (``scripts/backfill_defi_canonical_id_and_glued_prefix_2026_07_14.py``) does NOT
     enforce convergence on POOL rows — it never overwrites a POOL row's symbolic
     ``canonical_instrument_id`` with the address-form ``instrument_id`` (whether the
     canonical is already set OR blank), while a NON-POOL blank row DOES converge.

Plan: defi_consolidated_closeout_2026_07_18.md (Wave B — POOL-id Option A pinning test).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
from unified_api_contracts import build_pool_identity


def _load_backfill_module() -> ModuleType:
    """Load the one-off backfill script as a module by path (no GCS import)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "backfill_defi_canonical_id_and_glued_prefix_2026_07_14.py"
    spec = importlib.util.spec_from_file_location("_backfill_defi_canonical_id_pool_divergence_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# The address-form machine id and the symbolic 3-seg canonical id for one real pool.
_POOL_ADDRESS = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
_POOL_SYMBOLIC = "UNISWAP_V3-ARBITRUM:POOL:USDC-WETH-500"


def test_pool_rows_diverge_option_a_and_backfill_does_not_enforce_convergence() -> None:
    """POOL rows diverge (address vs symbolic) — and the backfill never converges them.

    ONE authoritative pin for the two-id model + the code path that must respect it.
    """
    # ── Layer 1: the UAC SSOT two-id producer DIVERGES for a pool ──────────────
    identity = build_pool_identity(
        venue="UNISWAP_V3",
        chain="ARBITRUM",
        pool_address="0x88E6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        base_asset="USDC",
        quote_asset="WETH",
        fee=500,
    )
    # Machine id = pool address (lowercased); symbolic canonical = glued 3-seg key.
    assert identity.canonical_instrument_id == _POOL_ADDRESS
    assert identity.glued_pair_id == _POOL_SYMBOLIC
    # The 3-seg grammar: exactly 2 colons (fee glued into the symbol with a hyphen).
    assert identity.glued_pair_id.count(":") == 2
    # DIVERGE — the operational address is NOT the symbolic canonical id.
    assert identity.canonical_instrument_id != identity.glued_pair_id

    # ── Layer 2: the backfill migrate() does NOT enforce convergence on POOL rows ─
    backfill = _load_backfill_module()

    df = pd.DataFrame(
        [
            # (a) POOL row already carrying BOTH diverged ids → must stay diverged
            #     (migrate must NOT overwrite the symbolic canonical with the address).
            {
                "instrument_id": _POOL_ADDRESS,
                "canonical_instrument_id": _POOL_SYMBOLIC,
                "glued_pair_id": _POOL_SYMBOLIC,
            },
            # (b) POOL row with a BLANK canonical (non-blank glued_pair_id marks it a
            #     pool) → must NOT be converged to the address; canonical stays blank
            #     (the correct symbolic id is filled by the Option-A rollup / Wave-D).
            {
                "instrument_id": _POOL_ADDRESS,
                "canonical_instrument_id": "",
                "glued_pair_id": _POOL_SYMBOLIC,
            },
            # (c) CONTROL — a NON-POOL row (blank glued_pair_id) with a blank canonical
            #     DOES converge: canonical := instrument_id (correct by construction).
            {
                "instrument_id": "BINANCE:SPOT_PAIR:BTCUSDT",
                "canonical_instrument_id": "",
                "glued_pair_id": "",
            },
        ]
    )

    patched, summary = backfill.migrate(df)
    rows = patched.to_dict("records")

    # (a) diverged POOL row is untouched — the two ids STILL diverge.
    assert rows[0]["instrument_id"] == _POOL_ADDRESS
    assert rows[0]["canonical_instrument_id"] == _POOL_SYMBOLIC
    assert rows[0]["canonical_instrument_id"] != rows[0]["instrument_id"]

    # (b) blank-canonical POOL row is NOT converged to its pool address.
    assert rows[1]["canonical_instrument_id"] != _POOL_ADDRESS
    assert rows[1]["canonical_instrument_id"] == ""

    # (c) blank-canonical NON-POOL row DID converge to instrument_id.
    assert rows[2]["canonical_instrument_id"] == "BINANCE:SPOT_PAIR:BTCUSDT"

    # Only the one non-pool row was blank-filled; neither POOL row was converged.
    assert summary["canonical_instrument_id_backfilled"] == 1
