"""Unit tests — Phase 2.5 verify_defi_metadata_forward migration script.

Plan: instruments_service_metadata_refactor_2026_04_29 Phase 2.5.
"""

from __future__ import annotations

import importlib.util
import io
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pandas as pd
import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _load_verify_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrations" / "verify_defi_metadata_forward.py"
    spec = importlib.util.spec_from_file_location("_verify_defi_metadata_test_module", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verify = _load_verify_module()


def _make_parquet(rows: list[dict[str, object]]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


@pytest.mark.unit
def test_verify_one_parquet_passes_when_pool_address_populated() -> None:
    payload = _make_parquet(
        [
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30",
                "pool_address": "0xpool1",
                "pool_fee_tier": 30,
                "base_asset_contract_address": "0xusdc",
                "base_asset_decimals": 6,
                "base_asset_symbol_onchain": "USDC",
                "quote_asset_contract_address": "0xweth",
                "quote_asset_decimals": 18,
                "quote_asset_symbol_onchain": "WETH",
                "atoken_address": None,
                "debt_token_address": None,
            },
        ]
    )
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    passed, counts = verify._verify_one_parquet(storage, "instruments-store-defi-test", "any/path")

    assert passed is True
    assert counts["pool_address"] == 1
    assert counts["base_asset_contract_address"] == 1


@pytest.mark.unit
def test_verify_one_parquet_passes_with_atoken_only() -> None:
    """Aave-style parquet — only atoken_address populated, but that's enough to PASS."""
    payload = _make_parquet(
        [
            {
                "instrument_key": "AAVE_V3-ETHEREUM:A_TOKEN:AUSDC",
                "pool_address": None,
                "pool_fee_tier": None,
                "base_asset_contract_address": None,
                "base_asset_decimals": None,
                "base_asset_symbol_onchain": None,
                "quote_asset_contract_address": None,
                "quote_asset_decimals": None,
                "quote_asset_symbol_onchain": None,
                "atoken_address": "0xatoken1",
                "debt_token_address": None,
            },
        ]
    )
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    passed, counts = verify._verify_one_parquet(storage, "instruments-store-defi-test", "any/path")

    assert passed is True
    assert counts["atoken_address"] == 1


@pytest.mark.unit
def test_verify_one_parquet_fails_when_all_high_signal_columns_null() -> None:
    """No pool_address / base_asset_contract_address / atoken_address → FAIL."""
    payload = _make_parquet(
        [
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30",
                "pool_address": None,
                "pool_fee_tier": None,
                "base_asset_contract_address": None,
                "base_asset_decimals": None,
                "base_asset_symbol_onchain": None,
                "quote_asset_contract_address": None,
                "quote_asset_decimals": None,
                "quote_asset_symbol_onchain": None,
                "atoken_address": None,
                "debt_token_address": None,
            },
        ]
    )
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    passed, _counts = verify._verify_one_parquet(storage, "instruments-store-defi-test", "any/path")

    assert passed is False


@pytest.mark.unit
def test_list_dates_for_venue_extracts_and_sorts() -> None:
    """`_list_dates_for_venue` parses day=YYYY-MM-DD from blob names."""
    blobs = [
        MagicMock(name="b1"),
        MagicMock(name="b2"),
        MagicMock(name="b3"),
        MagicMock(name="b4"),
    ]
    blobs[0].name = "instrument_availability/by_date/day=2024-02-01/venue=UNISWAP_V3-ETHEREUM/instruments.parquet"
    blobs[1].name = "instrument_availability/by_date/day=2024-01-15/venue=UNISWAP_V3-ETHEREUM/instruments.parquet"
    blobs[2].name = "instrument_availability/by_date/day=2024-03-01/venue=UNISWAP_V3-ETHEREUM/instruments.parquet"
    blobs[3].name = "instrument_availability/by_date/day=2024-02-01/venue=AAVE_V3-ETHEREUM/instruments.parquet"

    bucket_handle = MagicMock()
    bucket_handle.list_blobs.return_value = blobs
    storage = MagicMock()
    storage.bucket.return_value = bucket_handle

    dates = verify._list_dates_for_venue(storage, "instruments-store-defi-test", "UNISWAP_V3-ETHEREUM")

    assert dates == ["2024-01-15", "2024-02-01", "2024-03-01"]
