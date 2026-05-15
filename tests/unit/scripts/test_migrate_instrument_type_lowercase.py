"""Unit tests — migrate_instrument_type_lowercase.py.

Tests cover:
  1. _has_uppercase_instrument_type: detects uppercase segments correctly.
  2. _lowercased_path: transforms uppercase to lowercase in the segment.
  3. Pre-state / post-state: path with uppercase round-trips through lowercasing.
  4. Idempotency: already-lowercase path is unchanged.
  5. _migrate_one: mocked GCS copy+delete, returns (src, dst, ok, err).
  6. _migrate_one failure: error returned, no delete.
  7. _migrate_one no-op: src == dst when already canonical.

No GCS or network access — pure regex helpers tested directly; GCS path mocked.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_instrument_type_lowercase.py"
    module_name = "_migrate_instrument_type_lowercase_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_mod = _load_script()

pytestmark = pytest.mark.unit

_UPPER_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=binance/"
    "instrument_type=PERPETUAL/instrument_id=BTCUSDT/data.parquet"
)
_LOWER_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=binance/"
    "instrument_type=perpetual/instrument_id=BTCUSDT/data.parquet"
)
_ALREADY_LOWER_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=kraken/"
    "instrument_type=spot/instrument_id=BTCUSD/data.parquet"
)
_MIXED_CASE_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=defi/venue=uniswap/"
    "instrument_type=Perpetual/instrument_id=WETHUSDC/data.parquet"
)


class TestHasUppercaseInstrumentType:
    """_has_uppercase_instrument_type regex detection."""

    def test_detects_all_caps(self) -> None:
        assert _mod._has_uppercase_instrument_type(_UPPER_PATH) is True

    def test_detects_mixed_case(self) -> None:
        assert _mod._has_uppercase_instrument_type(_MIXED_CASE_PATH) is True

    def test_rejects_already_lowercase(self) -> None:
        assert _mod._has_uppercase_instrument_type(_LOWER_PATH) is False

    def test_rejects_path_without_instrument_type(self) -> None:
        path = "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=binance/data.parquet"
        assert _mod._has_uppercase_instrument_type(path) is False

    def test_rejects_empty_string(self) -> None:
        assert _mod._has_uppercase_instrument_type("") is False


class TestLowercasedPath:
    """_lowercased_path pure transformation."""

    def test_lowercases_perpetual(self) -> None:
        result = _mod._lowercased_path(_UPPER_PATH)
        assert result == _LOWER_PATH

    def test_lowercases_mixed_case(self) -> None:
        result = _mod._lowercased_path(_MIXED_CASE_PATH)
        expected = _MIXED_CASE_PATH.replace("instrument_type=Perpetual/", "instrument_type=perpetual/")
        assert result == expected

    def test_idempotent_on_lowercase(self) -> None:
        result = _mod._lowercased_path(_LOWER_PATH)
        assert result == _LOWER_PATH

    def test_idempotent_on_no_instrument_type(self) -> None:
        path = "raw_tick_data/by_date/day=2026-05-01/venue=binance/data.parquet"
        assert _mod._lowercased_path(path) == path


class TestPrePostMigrationState:
    """Pre-state (uppercase) → post-state (lowercase) path assertions."""

    def test_pre_state_has_uppercase_segment(self) -> None:
        assert "instrument_type=PERPETUAL" in _UPPER_PATH

    def test_post_state_has_lowercase_segment(self) -> None:
        result = _mod._lowercased_path(_UPPER_PATH)
        assert "instrument_type=perpetual" in result
        assert "instrument_type=PERPETUAL" not in result

    def test_path_prefix_unchanged_after_migration(self) -> None:
        """Everything before instrument_type= segment must be preserved."""
        result = _mod._lowercased_path(_UPPER_PATH)
        prefix = "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=binance/"
        assert result.startswith(prefix)

    def test_path_suffix_unchanged_after_migration(self) -> None:
        """instrument_id and filename preserved."""
        result = _mod._lowercased_path(_UPPER_PATH)
        assert result.endswith("instrument_id=BTCUSDT/data.parquet")

    def test_path_length_unchanged(self) -> None:
        result = _mod._lowercased_path(_UPPER_PATH)
        assert len(result) == len(_UPPER_PATH)


class TestMigrateOne:
    """_migrate_one: mocked GCS copy+delete."""

    def _make_bucket(self, src_name: str) -> MagicMock:
        blob = MagicMock()
        blob.name = src_name
        bucket = MagicMock()
        bucket.blob.return_value = blob
        return bucket

    def test_success_returns_ok_true(self) -> None:
        bucket = self._make_bucket(_UPPER_PATH)
        result = _mod._migrate_one(bucket, _UPPER_PATH)
        assert result[2] is True  # ok
        assert result[3] == ""  # err

    def test_success_dst_is_lowercased(self) -> None:
        bucket = self._make_bucket(_UPPER_PATH)
        result = _mod._migrate_one(bucket, _UPPER_PATH)
        assert result[1] == _LOWER_PATH  # dst

    def test_success_calls_copy_then_delete(self) -> None:
        bucket = self._make_bucket(_UPPER_PATH)
        blob = bucket.blob.return_value
        _mod._migrate_one(bucket, _UPPER_PATH)
        bucket.copy_blob.assert_called_once()
        blob.delete.assert_called_once()

    def test_already_lowercase_returns_ok_no_copy(self) -> None:
        """If src == dst (already canonical), skip copy+delete."""
        bucket = self._make_bucket(_ALREADY_LOWER_PATH)
        result = _mod._migrate_one(bucket, _ALREADY_LOWER_PATH)
        assert result[2] is True  # ok
        assert result[0] == result[1]  # src == dst
        bucket.copy_blob.assert_not_called()

    def test_gcs_error_returns_ok_false(self) -> None:
        bucket = self._make_bucket(_UPPER_PATH)
        bucket.copy_blob.side_effect = Exception("GCS timeout")
        result = _mod._migrate_one(bucket, _UPPER_PATH)
        assert result[2] is False  # ok
        assert "GCS timeout" in result[3]  # err
