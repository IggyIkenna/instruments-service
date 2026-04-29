"""Unit tests — Phase 2.5 backfill_defi_metadata_2026_04_29 migration script.

These tests are credential-free and exercise the pure helpers in the migration
script (merge logic, idempotent skip, dry-run gate) by mocking the GCS client
+ subgraph adapter. They do NOT exercise UAC capability bootstrap or hit any
network endpoint.

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


# --- Module loader ----------------------------------------------------------


def _load_migration_module() -> ModuleType:
    """Load the migration script as a module by path (script lives outside the package)."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrations" / "backfill_defi_metadata_2026_04_29.py"
    spec = importlib.util.spec_from_file_location("_backfill_defi_metadata_test_module", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


# --- Fixtures ---------------------------------------------------------------


def _historical_parquet_bytes(stamped: bool = False) -> bytes:
    """Build a parquet payload that mirrors the historical (Phase 2a/2b-pre) DeFi schema."""
    rows: list[dict[str, object]] = [
        {
            "instrument_key": "UNISWAPV3-ETHEREUM:POOL:USDC-WETH:30",
            "venue": "UNISWAPV3-ETHEREUM",
            "instrument_type": "POOL",
            "base_asset": "USDC",
            "quote_asset": "WETH",
            "raw_symbol": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            # New metadata columns: NULL on historical parquets.
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
        {
            "instrument_key": "UNISWAPV3-ETHEREUM:POOL:DAI-WETH:30",
            "venue": "UNISWAPV3-ETHEREUM",
            "instrument_type": "POOL",
            "base_asset": "DAI",
            "quote_asset": "WETH",
            "raw_symbol": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
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
    if stamped:
        rows[0]["pool_address"] = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        rows[0]["pool_fee_tier"] = 30
        rows[0]["base_asset_contract_address"] = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        rows[0]["base_asset_decimals"] = 6
        rows[0]["base_asset_symbol_onchain"] = "USDC"
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _subgraph_lookup_for_uniswap() -> dict[str, dict[str, object]]:
    """Mirror what `_fetch_metadata_lookup_for_venue` would return."""
    return {
        "UNISWAPV3-ETHEREUM:POOL:USDC-WETH:30": {
            "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "pool_fee_tier": 30,
            "base_asset_contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "base_asset_decimals": 6,
            "base_asset_symbol_onchain": "USDC",
            "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "quote_asset_decimals": 18,
            "quote_asset_symbol_onchain": "WETH",
            "atoken_address": None,
            "debt_token_address": None,
        },
        "UNISWAPV3-ETHEREUM:POOL:DAI-WETH:30": {
            "pool_address": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
            "pool_fee_tier": 30,
            "base_asset_contract_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "base_asset_decimals": 18,
            "base_asset_symbol_onchain": "DAI",
            "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "quote_asset_decimals": 18,
            "quote_asset_symbol_onchain": "WETH",
            "atoken_address": None,
            "debt_token_address": None,
        },
    }


# --- Tests ------------------------------------------------------------------


@pytest.mark.unit
def test_merge_metadata_fills_new_columns_only() -> None:
    """Left-merge fills NEW metadata columns and leaves existing data intact."""
    df = pd.read_parquet(io.BytesIO(_historical_parquet_bytes()))
    lookup = _subgraph_lookup_for_uniswap()

    merged, n_filled = migration._merge_metadata(df, lookup)

    assert n_filled == 2  # both rows had at least one non-null column added
    # Existing columns untouched
    assert merged.loc[0, "venue"] == "UNISWAPV3-ETHEREUM"
    assert merged.loc[0, "instrument_type"] == "POOL"
    assert merged.loc[0, "base_asset"] == "USDC"
    # New columns populated from lookup
    assert merged.loc[0, "pool_address"] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    assert merged.loc[0, "pool_fee_tier"] == 30
    assert merged.loc[0, "base_asset_decimals"] == 6
    # Row 1 (DAI/WETH) also stamped
    assert merged.loc[1, "pool_address"] == "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"
    assert merged.loc[1, "base_asset_symbol_onchain"] == "DAI"


@pytest.mark.unit
def test_merge_metadata_preserves_pre_stamped_values() -> None:
    """Already-populated rows are NEVER overwritten; only NULL slots get filled."""
    df = pd.read_parquet(io.BytesIO(_historical_parquet_bytes(stamped=True)))
    # Lookup says a different pool address — but the parquet's value should win.
    lookup = {
        "UNISWAPV3-ETHEREUM:POOL:USDC-WETH:30": {
            "pool_address": "0xDIFFERENT",  # Should NOT overwrite
            "pool_fee_tier": 9999,
            "base_asset_contract_address": "0xOTHER",
            "base_asset_decimals": 99,
            "base_asset_symbol_onchain": "OTHER",
            "quote_asset_contract_address": "0xWETHX",
            "quote_asset_decimals": 18,
            "quote_asset_symbol_onchain": "WETH",
            "atoken_address": None,
            "debt_token_address": None,
        },
    }
    merged, _ = migration._merge_metadata(df, lookup)

    # Row 0 was pre-stamped — values must NOT change for the columns that had data.
    assert merged.loc[0, "pool_address"] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    assert merged.loc[0, "pool_fee_tier"] == 30
    assert merged.loc[0, "base_asset_decimals"] == 6
    # But its NULL columns DID get filled from the lookup
    assert merged.loc[0, "quote_asset_contract_address"] == "0xWETHX"


@pytest.mark.unit
def test_idempotent_skip_when_pool_address_populated() -> None:
    """`_is_already_stamped` returns True when first row's pool_address is non-null."""
    stamped_df = pd.read_parquet(io.BytesIO(_historical_parquet_bytes(stamped=True)))
    historical_df = pd.read_parquet(io.BytesIO(_historical_parquet_bytes(stamped=False)))

    assert migration._is_already_stamped(stamped_df) is True
    assert migration._is_already_stamped(historical_df) is False


@pytest.mark.unit
def test_idempotent_skip_when_column_missing() -> None:
    """No `pool_address` column at all → not stamped (Phase 1 schema-pre case)."""
    df = pd.DataFrame({"instrument_key": ["UNISWAPV3-ETHEREUM:POOL:USDC-WETH:30"], "venue": ["UNISWAPV3-ETHEREUM"]})
    assert migration._is_already_stamped(df) is False


@pytest.mark.unit
def test_process_one_parquet_skipped_on_stamped() -> None:
    """A pre-stamped parquet is detected & skipped before any merge work."""
    payload = _historical_parquet_bytes(stamped=True)
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    status, n_rows, n_filled = migration._process_one_parquet(
        storage,
        bucket="instruments-store-defi-test",
        blob_path="instrument_availability/by_date/day=2024-01-01/venue=UNISWAPV3-ETHEREUM/instruments.parquet",
        lookup=_subgraph_lookup_for_uniswap(),
        dry_run=False,
    )

    assert status == "skipped"
    assert n_rows == 2
    assert n_filled == 0
    # No upload triggered
    storage.upload_bytes.assert_not_called()


@pytest.mark.unit
def test_process_one_parquet_dry_run_emits_diff_no_write() -> None:
    """Dry-run reports `would-rewrite` and never calls `upload_bytes`."""
    payload = _historical_parquet_bytes(stamped=False)
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    status, n_rows, n_filled = migration._process_one_parquet(
        storage,
        bucket="instruments-store-defi-test",
        blob_path="instrument_availability/by_date/day=2024-01-01/venue=UNISWAPV3-ETHEREUM/instruments.parquet",
        lookup=_subgraph_lookup_for_uniswap(),
        dry_run=True,
    )

    assert status == "would-rewrite"
    assert n_rows == 2
    assert n_filled == 2
    storage.upload_bytes.assert_not_called()


@pytest.mark.unit
def test_process_one_parquet_rewrites_when_not_dry_run() -> None:
    """Real run uploads merged parquet bytes back to the same blob_path."""
    payload = _historical_parquet_bytes(stamped=False)
    storage = MagicMock()
    storage.download_bytes.return_value = payload

    blob_path = "instrument_availability/by_date/day=2024-01-01/venue=UNISWAPV3-ETHEREUM/instruments.parquet"
    status, n_rows, n_filled = migration._process_one_parquet(
        storage,
        bucket="instruments-store-defi-test",
        blob_path=blob_path,
        lookup=_subgraph_lookup_for_uniswap(),
        dry_run=False,
    )

    assert status == "rewritten"
    assert n_rows == 2
    assert n_filled == 2
    storage.upload_bytes.assert_called_once()
    call = storage.upload_bytes.call_args
    assert call.kwargs["bucket"] == "instruments-store-defi-test"
    assert call.kwargs["blob_path"] == blob_path
    # The uploaded bytes deserialise back to a DataFrame with the new columns populated.
    uploaded_df = pd.read_parquet(io.BytesIO(call.kwargs["data"]))
    assert "pool_address" in uploaded_df.columns
    assert uploaded_df.loc[0, "pool_address"] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    assert uploaded_df.loc[1, "base_asset_symbol_onchain"] == "DAI"


@pytest.mark.unit
def test_list_historical_parquets_filters_to_venue() -> None:
    """`_list_historical_parquets` only returns paths under the requested venue segment."""
    blob_a = MagicMock()
    blob_a.name = "instrument_availability/by_date/day=2024-01-01/venue=UNISWAPV3-ETHEREUM/instruments.parquet"
    blob_b = MagicMock()
    blob_b.name = "instrument_availability/by_date/day=2024-01-02/venue=UNISWAPV3-ETHEREUM/instruments.parquet"
    blob_c = MagicMock()
    blob_c.name = "instrument_availability/by_date/day=2024-01-01/venue=AAVEV3-ETHEREUM/instruments.parquet"
    blob_d = MagicMock()
    blob_d.name = "_index/availability_index.parquet"  # Should be filtered out

    bucket_handle = MagicMock()
    bucket_handle.list_blobs.return_value = [blob_a, blob_b, blob_c, blob_d]
    storage = MagicMock()
    storage.bucket.return_value = bucket_handle

    paths = migration._list_historical_parquets(
        storage,
        bucket="instruments-store-defi-test",
        venue_tag="UNISWAPV3-ETHEREUM",
    )

    assert paths == [
        "instrument_availability/by_date/day=2024-01-01/venue=UNISWAPV3-ETHEREUM/instruments.parquet",
        "instrument_availability/by_date/day=2024-01-02/venue=UNISWAPV3-ETHEREUM/instruments.parquet",
    ]


@pytest.mark.unit
def test_parse_venues_filter() -> None:
    assert migration._parse_venues_filter(None) is None
    assert migration._parse_venues_filter("") is None
    assert migration._parse_venues_filter("UNISWAPV3-ETHEREUM") == {"UNISWAPV3-ETHEREUM"}
    assert migration._parse_venues_filter("UniswapV3-Ethereum,aavev3-arbitrum") == {
        "UNISWAPV3-ETHEREUM",
        "AAVEV3-ARBITRUM",
    }


@pytest.mark.unit
def test_merge_metadata_handles_empty_dataframe() -> None:
    """Empty input → empty output, n_filled=0 (no crash)."""
    df = pd.DataFrame()
    merged, n_filled = migration._merge_metadata(df, _subgraph_lookup_for_uniswap())
    assert merged.empty
    assert n_filled == 0


@pytest.mark.unit
def test_merge_metadata_unmatched_instruments_remain_null() -> None:
    """If a parquet row has an instrument_key NOT in the snapshot, its metadata stays NULL."""
    df = pd.DataFrame(
        [
            {
                "instrument_key": "UNISWAPV3-ETHEREUM:POOL:DELISTED-WETH:30",
                "venue": "UNISWAPV3-ETHEREUM",
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
    merged, n_filled = migration._merge_metadata(df, _subgraph_lookup_for_uniswap())
    assert n_filled == 0
    assert merged.loc[0, "pool_address"] is None
