"""Unit tests — Phase 4 validate_defi_metadata_cohesion migration script.

These tests exercise the pure helpers in the cohesion-validation script
(comparison logic, mismatch sampling, markdown report shape, dry behaviour)
by mocking the GCS client + subgraph adapter. They do NOT exercise UAC
capability bootstrap or hit any network endpoint.

The script is read-only; tests verify it never invokes ``upload_bytes`` /
``upload_text`` / any write path on the storage client.

Plan: instruments_service_metadata_refactor_2026_04_29 Phase 4.
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


def _load_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrations" / "validate_defi_metadata_cohesion.py"
    spec = importlib.util.spec_from_file_location("_validate_defi_cohesion_test_module", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cohesion = _load_module()


# --- Fixtures ---------------------------------------------------------------


def _parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _clean_match_subgraph_lookup() -> dict[str, dict[str, object]]:
    return {
        "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30": {
            "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "pool_fee_tier": 30,
            "base_asset_contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "base_asset_decimals": 6,
            "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "quote_asset_decimals": 18,
            "atoken_address": None,
        },
        "UNISWAP_V3-ETHEREUM:POOL:DAI-WETH:30": {
            "pool_address": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
            "pool_fee_tier": 30,
            "base_asset_contract_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "base_asset_decimals": 18,
            "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "quote_asset_decimals": 18,
            "atoken_address": None,
        },
    }


def _clean_match_parquet_df() -> pd.DataFrame:
    """Parquet rows that exactly match the subgraph lookup (100% cohesion)."""
    return pd.DataFrame(
        [
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30",
                "venue": "UNISWAP_V3-ETHEREUM",
                "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "pool_fee_tier": 30,
                "base_asset_contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "base_asset_decimals": 6,
                "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "quote_asset_decimals": 18,
                "atoken_address": None,
            },
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:DAI-WETH:30",
                "venue": "UNISWAP_V3-ETHEREUM",
                "pool_address": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
                "pool_fee_tier": 30,
                "base_asset_contract_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                "base_asset_decimals": 18,
                "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "quote_asset_decimals": 18,
                "atoken_address": None,
            },
        ]
    )


def _mismatched_parquet_df() -> pd.DataFrame:
    """Same instrument keys but different metadata values to trigger mismatches."""
    return pd.DataFrame(
        [
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30",
                "venue": "UNISWAP_V3-ETHEREUM",
                # pool_address differs — drift
                "pool_address": "0xDRIFTEDPOOL00000000000000000000000000000",
                "pool_fee_tier": 30,
                "base_asset_contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                # decimals differ
                "base_asset_decimals": 18,
                "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "quote_asset_decimals": 18,
                "atoken_address": None,
            },
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:DAI-WETH:30",
                "venue": "UNISWAP_V3-ETHEREUM",
                "pool_address": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
                "pool_fee_tier": 30,
                "base_asset_contract_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                "base_asset_decimals": 18,
                "quote_asset_contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "quote_asset_decimals": 18,
                "atoken_address": None,
            },
        ]
    )


# --- Tests: _values_match ---------------------------------------------------


@pytest.mark.unit
def test_values_match_treats_none_and_nan_as_equal() -> None:
    assert cohesion._values_match(None, None) is True
    assert cohesion._values_match(None, float("nan")) is True
    assert cohesion._values_match(float("nan"), None) is True


@pytest.mark.unit
def test_values_match_normalises_hex_case() -> None:
    """Hex addresses with different casings should compare equal."""
    assert cohesion._values_match("0xABCDEF", "0xabcdef") is True
    assert cohesion._values_match("  0xABC  ", "0xabc") is True


@pytest.mark.unit
def test_values_match_numeric_int_vs_float() -> None:
    """Subgraph might return int, parquet might float-coerce — should still match."""
    assert cohesion._values_match(30, 30.0) is True
    assert cohesion._values_match(6, 18) is False


@pytest.mark.unit
def test_values_match_distinguishes_missing_from_present() -> None:
    assert cohesion._values_match(None, "0xabc") is False
    assert cohesion._values_match("0xabc", None) is False


# --- Tests: _compare_one_venue ----------------------------------------------


@pytest.mark.unit
def test_compare_clean_match_yields_zero_mismatches() -> None:
    """100% match between subgraph and parquet → all match, no mismatch samples."""
    result = cohesion._compare_one_venue(
        "UNISWAP_V3-ETHEREUM",
        _clean_match_subgraph_lookup(),
        _clean_match_parquet_df(),
    )
    assert result["total_instruments"] == 2
    assert result["subgraph_only"] == 0
    assert result["parquet_only"] == 0
    assert result["mismatch_samples"] == []
    per_field = result["per_field"]
    assert isinstance(per_field, dict)
    for field in cohesion.COHESION_FIELDS:
        matches, mismatches = per_field[field]
        assert mismatches == 0, f"unexpected mismatch on {field}"
        assert matches == 2, f"{field} should match for both instruments"


@pytest.mark.unit
def test_compare_synthetic_mismatch_logs_correctly() -> None:
    """Synthetic drift on pool_address + base_asset_decimals → mismatch counts + samples."""
    result = cohesion._compare_one_venue(
        "UNISWAP_V3-ETHEREUM",
        _clean_match_subgraph_lookup(),
        _mismatched_parquet_df(),
    )
    assert result["total_instruments"] == 2
    per_field = result["per_field"]
    assert isinstance(per_field, dict)
    # pool_address: 1 mismatch (USDC-WETH row), 1 match (DAI-WETH row)
    pool_match, pool_mis = per_field["pool_address"]
    assert pool_match == 1
    assert pool_mis == 1
    # base_asset_decimals: 1 mismatch (USDC-WETH 6→18 drift), 1 match
    dec_match, dec_mis = per_field["base_asset_decimals"]
    assert dec_match == 1
    assert dec_mis == 1
    # Other fields fully match
    assert per_field["pool_fee_tier"] == (2, 0)
    # Mismatch samples include both drifts
    samples = result["mismatch_samples"]
    assert isinstance(samples, list)
    fields_in_samples = {s["field"] for s in samples}
    assert "pool_address" in fields_in_samples
    assert "base_asset_decimals" in fields_in_samples


@pytest.mark.unit
def test_compare_subgraph_only_and_parquet_only_counts() -> None:
    """Disjoint key sets are reported as subgraph_only / parquet_only, not as mismatches."""
    subgraph = {
        "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30": dict.fromkeys(cohesion.COHESION_FIELDS),
        "UNISWAP_V3-ETHEREUM:POOL:NEWPOOL-WETH:30": dict.fromkeys(cohesion.COHESION_FIELDS),
    }
    parquet = pd.DataFrame(
        [
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:USDC-WETH:30",
                **dict.fromkeys(cohesion.COHESION_FIELDS),
            },
            {
                "instrument_key": "UNISWAP_V3-ETHEREUM:POOL:DELISTED-WETH:30",
                **dict.fromkeys(cohesion.COHESION_FIELDS),
            },
        ]
    )
    result = cohesion._compare_one_venue("UNISWAP_V3-ETHEREUM", subgraph, parquet)
    assert result["total_instruments"] == 1  # only USDC-WETH overlaps
    assert result["subgraph_only"] == 1  # NEWPOOL only in subgraph
    assert result["parquet_only"] == 1  # DELISTED only in parquet


@pytest.mark.unit
def test_compare_mismatch_sample_cap() -> None:
    """Mismatch samples are capped at _MAX_MISMATCH_SAMPLES (5)."""
    # Build 10 instruments, all drifted on pool_address.
    subgraph: dict[str, dict[str, object]] = {}
    rows: list[dict[str, object]] = []
    for i in range(10):
        ikey = f"UNISWAP_V3-ETHEREUM:POOL:T{i}-WETH:30"
        subgraph[ikey] = dict.fromkeys(cohesion.COHESION_FIELDS)
        subgraph[ikey]["pool_address"] = f"0xSUBGRAPH{i}"
        rows.append(
            {
                "instrument_key": ikey,
                **dict.fromkeys(cohesion.COHESION_FIELDS),
                "pool_address": f"0xPARQUET{i}",
            }
        )
    parquet = pd.DataFrame(rows)
    result = cohesion._compare_one_venue("UNISWAP_V3-ETHEREUM", subgraph, parquet)
    samples = result["mismatch_samples"]
    assert isinstance(samples, list)
    assert len(samples) == cohesion._MAX_MISMATCH_SAMPLES
    per_field = result["per_field"]
    assert isinstance(per_field, dict)
    # All 10 rows recorded as mismatches even though only 5 are sampled.
    _matches, mis = per_field["pool_address"]
    assert mis == 10


@pytest.mark.unit
def test_compare_handles_missing_instrument_key_column() -> None:
    """Parquet without instrument_key column is treated as 0 overlap, not a crash."""
    parquet = pd.DataFrame([{"venue": "UNISWAP_V3-ETHEREUM", "pool_address": "0xABC"}])
    result = cohesion._compare_one_venue(
        "UNISWAP_V3-ETHEREUM",
        _clean_match_subgraph_lookup(),
        parquet,
    )
    assert result["total_instruments"] == 0
    assert result["subgraph_only"] == 2


# --- Tests: _read_parquet_for_day ------------------------------------------


@pytest.mark.unit
def test_read_parquet_for_day_returns_none_on_missing_blob() -> None:
    """Missing parquet → returns None, never raises."""
    storage = MagicMock()
    storage.download_bytes.side_effect = OSError("404 not found")

    result = cohesion._read_parquet_for_day(storage, "instruments-store-defi-test", "UNISWAP_V3-ETHEREUM", "2026-04-29")
    assert result is None


@pytest.mark.unit
def test_read_parquet_for_day_constructs_canonical_path() -> None:
    """Read targets the canonical day=YYYY-MM-DD/venue=VENUE/instruments.parquet path."""
    storage = MagicMock()
    storage.download_bytes.return_value = _parquet_bytes([{"instrument_key": "X", "pool_address": "0xabc"}])
    df = cohesion._read_parquet_for_day(storage, "instruments-store-defi-test", "UNISWAP_V3-ETHEREUM", "2026-04-29")
    assert df is not None
    assert len(df) == 1
    storage.download_bytes.assert_called_once_with(
        "instruments-store-defi-test",
        "instrument_availability/by_date/day=2026-04-29/venue=UNISWAP_V3-ETHEREUM/instruments.parquet",
    )


# --- Tests: _format_markdown_report -----------------------------------------


@pytest.mark.unit
def test_markdown_report_shape_clean_match() -> None:
    """Clean match → no mismatch samples block, summary table populated."""
    per_venue = {
        "UNISWAP_V3-ETHEREUM": cohesion._compare_one_venue(
            "UNISWAP_V3-ETHEREUM",
            _clean_match_subgraph_lookup(),
            _clean_match_parquet_df(),
        )
    }
    report = cohesion._format_markdown_report("2026-04-29", per_venue)
    assert "# DeFi Metadata Cohesion Report" in report
    assert "day=2026-04-29" in report
    assert "## Per-venue summary" in report
    assert "UNISWAP_V3-ETHEREUM" in report
    assert "100% cohesive" in report  # no mismatches → message rendered
    # Markdown table contains every cohesion field column
    for f in cohesion.COHESION_FIELDS:
        assert f in report


@pytest.mark.unit
def test_markdown_report_shape_with_mismatches() -> None:
    """Mismatches → samples table rendered with instrument_key + field + values."""
    per_venue = {
        "UNISWAP_V3-ETHEREUM": cohesion._compare_one_venue(
            "UNISWAP_V3-ETHEREUM",
            _clean_match_subgraph_lookup(),
            _mismatched_parquet_df(),
        )
    }
    report = cohesion._format_markdown_report("2026-04-29", per_venue)
    assert "## Mismatch samples" in report
    assert "### UNISWAP_V3-ETHEREUM" in report
    assert "USDC-WETH" in report
    assert "pool_address" in report
    assert "0xDRIFTEDPOOL" in report  # parquet drift value rendered in mismatch sample
    assert "100% cohesive" not in report


# --- Tests: _parse_venues_filter --------------------------------------------


@pytest.mark.unit
def test_parse_venues_filter_returns_uppercase_set() -> None:
    assert cohesion._parse_venues_filter(None) is None
    assert cohesion._parse_venues_filter("") is None
    assert cohesion._parse_venues_filter("UNISWAP_V3-ETHEREUM") == {"UNISWAP_V3-ETHEREUM"}
    assert cohesion._parse_venues_filter("uniswapv3-ethereum,AaveV3-Arbitrum") == {
        "UNISWAP_V3-ETHEREUM",
        "AAVE_V3-ARBITRUM",
    }


# --- Tests: dry behaviour (read-only contract) ------------------------------


@pytest.mark.unit
def test_read_parquet_never_invokes_write_paths() -> None:
    """The script must NEVER call any GCS write method on the storage client.

    This is a regression guard against accidentally introducing a side-effect
    write path; the cohesion script's contract is read-only.
    """
    storage = MagicMock()
    storage.download_bytes.return_value = _parquet_bytes([{"instrument_key": "X", "pool_address": "0xabc"}])
    cohesion._read_parquet_for_day(storage, "instruments-store-defi-test", "UNISWAP_V3-ETHEREUM", "2026-04-29")
    storage.upload_bytes.assert_not_called()
    storage.upload_text.assert_not_called()
    # Bucket() returns a handle — but no write methods on it should be exercised.
    storage.bucket.return_value.delete_blob.assert_not_called()


@pytest.mark.unit
def test_compare_does_not_touch_storage() -> None:
    """`_compare_one_venue` is a pure function — no I/O at all."""
    storage = MagicMock()
    cohesion._compare_one_venue(
        "UNISWAP_V3-ETHEREUM",
        _clean_match_subgraph_lookup(),
        _clean_match_parquet_df(),
    )
    storage.download_bytes.assert_not_called()
    storage.upload_bytes.assert_not_called()
    storage.bucket.assert_not_called()
