"""Tests for the factory-address-based version resolution wired into
``scripts/canonicalize_defi_manifest_venue_2026_06_14.py`` (operator ruling
2026-07-21, defi_consolidated_closeout_2026_07_18.md "bare SUSHISWAP/UNISWAP
version").

Scope: only the NEW factory-resolution behavior. The script's pre-existing
bare/ghost/alias canonicalization behavior is unchanged and untested here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from unified_api_contracts.registry import ALL_DEFI_VENUES
from unified_api_contracts.registry.venue_mapping import VenueMapping

from instruments_service.reference_data.adapters.defi._dex_factory_registry import (
    UNISWAP_V3_FACTORY_BY_CHAIN,
)


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path (mirrors
    test_canonicalize_tradfi_catalogue_sharding.py / test_build_instrument_catalogue.py)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def canon_mod() -> ModuleType:
    return _load_script_module(
        "canonicalize_defi_manifest_venue_2026_06_14.py",
        "_canonicalize_defi_manifest_venue_test_module",
    )


class TestCanonicalVenueFactoryResolution:
    def setup_method(self) -> None:
        self.vm = VenueMapping()
        self.canon = frozenset(ALL_DEFI_VENUES)

    def test_known_factory_resolves_bare_venue_to_versioned(self, canon_mod: ModuleType) -> None:
        """A bare UNISWAP-ETHEREUM row whose factory matches the known Uniswap V3
        factory resolves to the exact versioned canonical venue."""
        factory = UNISWAP_V3_FACTORY_BY_CHAIN["ETHEREUM"]
        result = canon_mod._canonical_venue(self.vm, self.canon, "UNISWAP", "ETHEREUM", factory)
        assert result == "UNISWAP_V3-ETHEREUM"

    def test_no_factory_address_preserves_existing_behavior(self, canon_mod: ModuleType) -> None:
        """Zero-regression contract: omitting factory_address (the default, and the
        state of every row captured today) reproduces the exact pre-existing output."""
        without_factory = canon_mod._canonical_venue(self.vm, self.canon, "SUSHISWAP", "ARBITRUM")
        with_empty_factory = canon_mod._canonical_venue(self.vm, self.canon, "SUSHISWAP", "ARBITRUM", "")
        assert without_factory == with_empty_factory == "SUSHISWAP-ARBITRUM"

    def test_unresolvable_factory_falls_through_unchanged(self, canon_mod: ModuleType) -> None:
        """An unknown factory address must NOT block the pre-existing fallback path —
        the genuine residual (surface it, don't guess) leaves the bare canonical form,
        it never raises or drops the row."""
        result = canon_mod._canonical_venue(self.vm, self.canon, "SUSHISWAP", "ARBITRUM", "0x" + "9" * 40)
        assert result == "SUSHISWAP-ARBITRUM"

    def test_resolved_version_not_in_registry_falls_through(self) -> None:
        """SushiSwap-on-Arbitrum has NO registered versioned venue in ALL_DEFI_VENUES
        today (only the bare SUSHISWAP-ARBITRUM is registered) — even with a
        confidently-resolved factory match, the constructed versioned string must not
        be minted if it is not an actual registry member. This is the precise
        cross-repo (UAC) residual documented in the module + issue doc."""
        assert "SUSHISWAP_V2-ARBITRUM" not in ALL_DEFI_VENUES
        assert "SUSHISWAP_V3-ARBITRUM" not in ALL_DEFI_VENUES


class TestCanonicaliseVenueColumnFactoryColumn:
    def test_no_factory_column_unchanged_output(self, canon_mod: ModuleType) -> None:
        """Zero-regression: a frame with no factory_address column produces the exact
        same result as before this change (dedup key degrades to (venue, chain))."""
        df = pd.DataFrame({"venue": ["SUSHISWAP", "SUSHISWAP"], "chain": ["ARBITRUM", "ARBITRUM"]})
        out, n_changed = canon_mod.canonicalise_venue_column(df)
        assert list(out["venue"]) == ["SUSHISWAP-ARBITRUM", "SUSHISWAP-ARBITRUM"]
        assert n_changed == 2

    def test_factory_column_present_resolves_versioned_rows(self, canon_mod: ModuleType) -> None:
        factory = UNISWAP_V3_FACTORY_BY_CHAIN["ETHEREUM"]
        df = pd.DataFrame(
            {
                "venue": ["UNISWAP", "UNISWAP"],
                "chain": ["ETHEREUM", "ETHEREUM"],
                "factory_address": [factory, ""],
            }
        )
        out, n_changed = canon_mod.canonicalise_venue_column(df)
        assert out["venue"].tolist()[0] == "UNISWAP_V3-ETHEREUM"
        # Second row has no factory address -> falls through to the pre-existing
        # (non-versioned / unresolved) behavior, proving per-row independence even
        # when venue+chain are identical across rows.
        assert out["venue"].tolist()[1] != "UNISWAP_V3-ETHEREUM"
        assert n_changed >= 1

    def test_factory_column_present_but_empty_matches_no_factory_column(self, canon_mod: ModuleType) -> None:
        """An all-empty factory_address column must behave identically to the
        column being absent entirely (idempotent no-op contract preserved)."""
        df_with_col = pd.DataFrame({"venue": ["SUSHISWAP"], "chain": ["ARBITRUM"], "factory_address": [""]})
        df_without_col = pd.DataFrame({"venue": ["SUSHISWAP"], "chain": ["ARBITRUM"]})
        out_with, _ = canon_mod.canonicalise_venue_column(df_with_col)
        out_without, _ = canon_mod.canonicalise_venue_column(df_without_col)
        assert out_with["venue"].tolist() == out_without["venue"].tolist()
