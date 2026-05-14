"""Unit tests — Phase 3 migrate_tradfi_expiry_schema migration script.

Credential-free tests that exercise the pure helpers (dry-run, apply, idempotency,
CAS guard, OCC expiry parsing, null-expiration detection) by mocking the GCS client.
No network calls; no Databento API calls.

Plan: tradfi_canonical_futures_contract_hard_required_fields_2026_05_13.md Phase 3.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pandas as pd
import pytest

if TYPE_CHECKING:
    from types import ModuleType


# ---------------------------------------------------------------------------
# Module loader (script lives outside the package)
# ---------------------------------------------------------------------------


def _load_migration_module() -> ModuleType:
    """Load migrate_tradfi_expiry_schema.py as a module by path.

    Note: module must be registered in sys.modules BEFORE exec_module so that
    NamedTuple and @dataclass decorators resolve ``sys.modules[cls.__module__]``
    correctly when the module is loaded dynamically.
    """
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_tradfi_expiry_schema.py"
    module_name = "_migrate_tradfi_expiry_schema"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # register BEFORE exec so NamedTuple works
    spec.loader.exec_module(module)
    return module


migration = _load_migration_module()


# ---------------------------------------------------------------------------
# Helpers — fixture builders
# ---------------------------------------------------------------------------


def _make_parquet_with_expiration(nullable: bool = False) -> bytes:
    """Build an options-chain parquet.

    nullable=True → expiration column present but all None.
    nullable=False → expiration populated with valid dates.
    """
    rows = [
        {
            "symbol": "AAPL  241220C00200000",
            "option_type": "C",
            "strike": 200.0,
            "expiration": None if nullable else datetime(2024, 12, 20, 15, 0, 0, tzinfo=UTC),
        },
        {
            "symbol": "AAPL  241220P00180000",
            "option_type": "P",
            "strike": 180.0,
            "expiration": None if nullable else datetime(2024, 12, 20, 15, 0, 0, tzinfo=UTC),
        },
    ]
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _make_parquet_no_expiration_col() -> bytes:
    """Build an options parquet with no expiration column at all."""
    rows = [
        {"symbol": "AAPL  241220C00200000", "option_type": "C", "strike": 200.0},
        {"symbol": "AAPL  241220P00180000", "option_type": "P", "strike": 180.0},
    ]
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _make_parquet_unknown_symbol() -> bytes:
    """Options parquet with null expiration and a non-OCC symbol (CME futures option)."""
    rows = [
        {"symbol": "ESH26C5000", "option_type": "C", "strike": 5000.0, "expiration": None},
    ]
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _make_mock_blob(payload: bytes, generation: int = 100) -> MagicMock:
    blob = MagicMock()
    blob.generation = generation
    blob.download_as_bytes.return_value = payload
    return blob


def _make_mock_bucket(blob: MagicMock) -> MagicMock:
    bucket = MagicMock()
    bucket.blob.return_value = blob
    return bucket


# ---------------------------------------------------------------------------
# Tests for _parse_occ_expiry
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_occ_expiry_valid_call() -> None:
    """Standard 21-char OCC symbol parses to the correct expiry datetime."""
    result = migration._parse_occ_expiry("AAPL  241220C00200000")
    assert result is not None
    assert result.year == 2024
    assert result.month == 12
    assert result.day == 20
    assert result.tzinfo is not None


@pytest.mark.unit
def test_parse_occ_expiry_empty_string_returns_none() -> None:
    """Empty symbol → None (no crash)."""
    assert migration._parse_occ_expiry("") is None


@pytest.mark.unit
def test_parse_occ_expiry_non_occ_symbol_returns_none() -> None:
    """A CME futures symbol that does not match OCC format → None."""
    assert migration._parse_occ_expiry("ESH26") is None
    assert migration._parse_occ_expiry("CLZ26") is None


@pytest.mark.unit
def test_parse_occ_expiry_short_symbol_right_padded() -> None:
    """Symbol shorter than 21 chars is right-padded before matching."""
    # "AAPL241220C00200000" — ticker not padded
    result = migration._parse_occ_expiry("AAPL  241220C00200000")
    assert result is not None


# ---------------------------------------------------------------------------
# Tests for _has_null_expiration
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_has_null_expiration_returns_false_when_all_populated() -> None:
    """No null expiration cells → False."""
    payload = _make_parquet_with_expiration(nullable=False)
    df = pd.read_parquet(io.BytesIO(payload))
    assert migration._has_null_expiration(df) is False


@pytest.mark.unit
def test_has_null_expiration_returns_true_when_nulls_present() -> None:
    """Null expiration cells → True."""
    payload = _make_parquet_with_expiration(nullable=True)
    df = pd.read_parquet(io.BytesIO(payload))
    assert migration._has_null_expiration(df) is True


@pytest.mark.unit
def test_has_null_expiration_returns_false_when_column_absent() -> None:
    """No expiration column at all → False (column absent is not a null)."""
    payload = _make_parquet_no_expiration_col()
    df = pd.read_parquet(io.BytesIO(payload))
    assert migration._has_null_expiration(df) is False


# ---------------------------------------------------------------------------
# Tests for _is_options_chain_blob
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_is_options_chain_blob_matches_correct_path() -> None:
    """Path with data_type=options_chain + asset_group=tradfi + .parquet → True."""
    path = "raw_tick_data/by_date/day=2026-01-15/asset_group=tradfi/venue=CBOE/data_type=options_chain/file.parquet"
    assert migration._is_options_chain_blob(path) is True


@pytest.mark.unit
def test_is_options_chain_blob_rejects_non_options_path() -> None:
    """Futures chain path → False."""
    path = "raw_tick_data/by_date/day=2026-01-15/asset_group=tradfi/venue=CME/data_type=futures_chain/file.parquet"
    assert migration._is_options_chain_blob(path) is False


@pytest.mark.unit
def test_is_options_chain_blob_rejects_non_parquet() -> None:
    """Non-parquet file → False."""
    path = "raw_tick_data/by_date/day=2026-01-15/asset_group=tradfi/venue=CBOE/data_type=options_chain/file.csv"
    assert migration._is_options_chain_blob(path) is False


# ---------------------------------------------------------------------------
# Tests for _process_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_process_parquet_dry_run_no_write() -> None:
    """Dry-run on a null-expiration parquet reports rows but writes nothing."""
    payload = _make_parquet_with_expiration(nullable=True)
    blob = _make_mock_blob(payload, generation=42)
    bucket = _make_mock_bucket(blob)

    result = migration._process_parquet(bucket, "some/path.parquet", apply=False)

    assert result.null_rows == 2
    assert result.repaired == 2
    assert result.error == ""
    blob.upload_from_file.assert_not_called()


@pytest.mark.unit
def test_process_parquet_apply_writes_with_cas_guard() -> None:
    """Apply mode writes repaired parquet with if_generation_match=generation."""
    payload = _make_parquet_with_expiration(nullable=True)
    blob = _make_mock_blob(payload, generation=99)
    bucket = _make_mock_bucket(blob)

    result = migration._process_parquet(bucket, "some/path.parquet", apply=True)

    assert result.repaired == 2
    assert result.error == ""
    blob.upload_from_file.assert_called_once()
    call_kwargs = blob.upload_from_file.call_args.kwargs
    assert call_kwargs["if_generation_match"] == 99


@pytest.mark.unit
def test_process_parquet_idempotent_skip_when_all_populated() -> None:
    """Already-populated parquet is skipped — no GCS write, null_rows=0."""
    payload = _make_parquet_with_expiration(nullable=False)
    blob = _make_mock_blob(payload)
    bucket = _make_mock_bucket(blob)

    result = migration._process_parquet(bucket, "some/path.parquet", apply=True)

    assert result.null_rows == 0
    assert result.repaired == 0
    blob.upload_from_file.assert_not_called()


@pytest.mark.unit
def test_process_parquet_unresolvable_non_occ_symbol() -> None:
    """Non-OCC symbol → unresolvable=1, no write (nothing repaired)."""
    payload = _make_parquet_unknown_symbol()
    blob = _make_mock_blob(payload)
    bucket = _make_mock_bucket(blob)

    result = migration._process_parquet(bucket, "some/path.parquet", apply=True)

    assert result.null_rows == 1
    assert result.repaired == 0
    assert result.unresolvable == 1
    blob.upload_from_file.assert_not_called()


@pytest.mark.unit
def test_process_parquet_download_error_returns_error_result() -> None:
    """Download failure returns error in result without crashing."""
    blob = MagicMock()
    blob.download_as_bytes.side_effect = Exception("connection reset")
    bucket = MagicMock()
    bucket.blob.return_value = blob

    result = migration._process_parquet(bucket, "some/path.parquet", apply=True)

    assert result.error != ""
    assert "download failed" in result.error
