"""Unit tests — the pure discovery/window-building functions of
``expand_defi_pool_catalogue_from_manifest_2026_07_31.py``.

Why this is worth a test: the script's core claim is "every address-shaped
``instrument_id`` for a default DEX protocol/chain that the catalogue never covered
gets discovered and merged" — a wrong filter (e.g. missing the EVM address regex, or
scoping to the wrong (venue,chain) pairs) would either miss real gap addresses (the
under-coverage this script exists to close) or write garbage rows into a PROD reference
catalogue. No GCS — pure function, module-by-path load (mirrors
test_build_instrument_catalogue.py / test_canonicalize_tradfi_catalogue_sharding.py).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def expand_mod() -> ModuleType:
    return _load_script_module(
        "expand_defi_pool_catalogue_from_manifest_2026_07_31.py",
        "_expand_defi_pool_catalogue_test_module",
    )


_ADDR_A = "0x1353fe67fff8f376762b7034dc9066f0be15a723"
_ADDR_B = "0x4585fe77225b41b697c938b018e2ac67ac5a20c0"
_SOLANA_ADDR = "AQR3psjDnnNPqxqrQA7zFLqC86EoWSzWNYvTGhkENCs2"


def _manifest_row(
    *,
    date: str,
    venue: str,
    chain: str,
    data_type: str,
    instrument_id: str,
    capture_status: str = "captured",
) -> dict[str, object]:
    return {
        "date": date,
        "venue": venue,
        "chain": chain,
        "data_type": data_type,
        "instrument_id": instrument_id,
        "capture_status": capture_status,
    }


def test_discover_gap_addresses_finds_evm_address_shaped_rows(expand_mod: ModuleType) -> None:
    """Address-shaped instrument_ids for an in-scope EVM (venue,chain) pair are discovered,
    with available_from/available_to derived from the min/max captured date."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2021-01-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="dex_pool_state",
                instrument_id=_ADDR_A,
            ),
            _manifest_row(
                date="2021-06-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="dex_pool_state",
                instrument_id=_ADDR_A,
            ),
            _manifest_row(
                date="2021-03-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="dex_pool_swaps",
                instrument_id=_ADDR_B,
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert set(gap["pool_address"]) == {_ADDR_A.lower(), _ADDR_B.lower()}
    row_a = gap[gap["pool_address"] == _ADDR_A.lower()].iloc[0]
    assert row_a["available_from"] == "2021-01-01"
    assert row_a["available_to"] == "2021-06-01"
    assert row_a["venue"] == "UNISWAP_V3"
    assert row_a["chain"] == "ETHEREUM"


def test_discover_gap_addresses_excludes_symbol_named_evm_rows(expand_mod: ModuleType) -> None:
    """A symbol-shaped instrument_id (already catalogue-resolved) is NOT treated as a gap —
    only bare 0x-address rows are the "no catalogue-covered replacement" population."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2021-01-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="dex_pool_state",
                instrument_id="WETH-USDC-30",
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty


def test_discover_gap_addresses_excludes_out_of_scope_pair(expand_mod: ModuleType) -> None:
    """A (venue,chain) pair not in UAC's SUBGRAPH_IDS / the Solana allowlist is never
    discovered, even if it carries an address-shaped instrument_id — this script's scope
    is exactly the default DEX protocol universe, not "any defi venue"."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2021-01-01",
                venue="SOME_OTHER_VENUE",
                chain="ETHEREUM",
                data_type="dex_pool_state",
                instrument_id=_ADDR_A,
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty


def test_discover_gap_addresses_includes_kamino(expand_mod: ModuleType) -> None:
    """KAMINO's dex_pool_state instrument_id is a base58 Solana vault-strategy address
    (see module docstring's 2026-08-03 correction), not a UUID — an earlier pass wrongly
    excluded it after conflating dex_pool_state with the UUID-shaped lending_indices
    rows KAMINO also writes under the same venue. It is included like its Solana DEX
    siblings."""
    kamino_addr = "BLP7UHUg1yNry94Qk3sM8pAfEyDhTZirwFghw9DoBjn7"
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2023-01-01",
                venue="KAMINO",
                chain="SOLANA",
                data_type="dex_pool_state",
                instrument_id=kamino_addr,
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert list(gap["pool_address"]) == [kamino_addr.lower()]


def test_discover_gap_addresses_excludes_kamino_lending_indices(expand_mod: ModuleType) -> None:
    """KAMINO's UUID-shaped lending_indices rows are never discovered — the data_type
    filter (_DEX_DATA_TYPES) excludes them regardless of venue, so the actual
    UUID-shaped ids the earlier conflation was worried about never reach this scope."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2023-01-01",
                venue="KAMINO",
                chain="SOLANA",
                data_type="lending_indices",
                instrument_id="7d954b46-a2e9-461c-b1e3-f1e0cdd05c65",
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty


def test_discover_gap_addresses_includes_solana_allowlisted_venue(expand_mod: ModuleType) -> None:
    """ORCA/RAYDIUM/PHOENIX/KAMINO (Solana) instrument_ids are discovered without an
    address-shape regex filter — they are base58 pubkeys, not 0x-hex, so the EVM regex
    would wrongly exclude them."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2022-11-01", venue="ORCA", chain="SOLANA", data_type="dex_pool_state", instrument_id=_SOLANA_ADDR
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert list(gap["pool_address"]) == [_SOLANA_ADDR.lower()]


def test_discover_gap_addresses_excludes_non_captured_status(expand_mod: ModuleType) -> None:
    """An attempted_failed/expected_unattempted row is never mistaken for a genuine
    historical capture."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2021-01-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="dex_pool_state",
                instrument_id=_ADDR_A,
                capture_status="attempted_failed",
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty


def test_discover_gap_addresses_excludes_non_dex_data_type(expand_mod: ModuleType) -> None:
    """A non-dex_pool_state/dex_pool_swaps data_type row (e.g. lending_indices, which also
    lives under DeFi venues) is never scanned for pool-address discovery."""
    manifest = pd.DataFrame(
        [
            _manifest_row(
                date="2021-01-01",
                venue="UNISWAP_V3",
                chain="ETHEREUM",
                data_type="lending_indices",
                instrument_id=_ADDR_A,
            ),
        ]
    )
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty


def test_discover_gap_addresses_empty_manifest_returns_empty_frame(expand_mod: ModuleType) -> None:
    manifest = pd.DataFrame(columns=["date", "venue", "chain", "data_type", "instrument_id", "capture_status"])
    gap = expand_mod.discover_gap_addresses(manifest)
    assert gap.empty
    assert list(gap.columns) == ["venue", "chain", "pool_address", "available_from", "available_to"]


def test_build_window_df_shapes_catalog_columns_row(expand_mod: ModuleType) -> None:
    """Every discovered gap address becomes one CATALOG_COLUMNS-shaped 'pool' row with the
    canonical instrument_id form, and no other row carries a stray non-empty field."""
    gap = pd.DataFrame(
        [
            {
                "venue": "UNISWAP_V3",
                "chain": "ETHEREUM",
                "pool_address": _ADDR_A,
                "available_from": "2021-01-01",
                "available_to": "2021-06-01",
            }
        ]
    )
    window = expand_mod.build_window_df(gap)
    assert list(window.columns) == list(expand_mod.CATALOG_COLUMNS)
    row = window.iloc[0]
    assert row["instrument_type"] == "pool"
    assert row["venue"] == "UNISWAP_V3"
    assert row["chain"] == "ETHEREUM"
    assert row["pool_address"] == _ADDR_A
    assert row["data_type"] == "dex_pool_state"
    assert row["available_from"] == "2021-01-01"
    assert row["available_to"] == "2021-06-01"
    assert row["instrument_id"] == f"UNISWAP_V3-ETHEREUM:POOL:{_ADDR_A}"
    # every non-populated column stays blank, never NaN/None (parquet-write safety)
    assert row["glued_pair_id"] == ""
    assert row["base_asset"] == ""


def test_build_window_df_empty_input_returns_empty_frame(expand_mod: ModuleType) -> None:
    gap = pd.DataFrame(columns=["venue", "chain", "pool_address", "available_from", "available_to"])
    window = expand_mod.build_window_df(gap)
    assert window.empty
    assert list(window.columns) == list(expand_mod.CATALOG_COLUMNS)


def test_evm_protocol_chain_pairs_covers_measured_gap_venues(expand_mod: ModuleType) -> None:
    """Sanity check: the UAC-derived pair list actually includes the venues this whole
    fix exists for (curve/sushiswap/velodrome_v2/trader_joe_v2, the 4 originally-sampled
    protocols from the source issue doc)."""
    pairs = set(expand_mod._evm_protocol_chain_pairs())
    for venue, chain in [
        ("CURVE", "ETHEREUM"),
        ("SUSHISWAP", "ARBITRUM"),
        ("VELODROME_V2", "OPTIMISM"),
        ("TRADER_JOE_V2", "AVALANCHE"),
    ]:
        assert (venue, chain) in pairs, f"{venue}/{chain} missing from the default DEX protocol universe"
