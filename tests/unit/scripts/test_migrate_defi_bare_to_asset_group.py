"""Unit tests — migrate_defi_bare_to_asset_group.py.

Tests cover:
  1. _is_bare_defi_path: detects paths missing asset_group/category hive segment.
  2. _canonical_path: inserts asset_group=defi/ after day= segment.
  3. Pre-state / post-state: bare path → canonical path.
  4. Idempotency: canonical path not re-migrated.
  5. _migrate_one: mocked GCS copy+delete, returns (src, dst, ok, err).
  6. _migrate_one failure: error returned, no delete.

No GCS or network access — pure helpers tested directly; GCS mocked.
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
    script_path = repo_root / "scripts" / "migrate_defi_bare_to_asset_group.py"
    module_name = "_migrate_defi_bare_to_asset_group_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_mod = _load_script()

pytestmark = pytest.mark.unit

_BARE_PATH = "raw_tick_data/by_date/day=2026-05-01/venue=uniswap_v3/chain=ethereum/instrument_id=WETHUSDC/data.parquet"
_CANONICAL_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=defi/venue=uniswap_v3/chain=ethereum/"
    "instrument_id=WETHUSDC/data.parquet"
)
_CATEGORY_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/category=defi/venue=uniswap_v3/instrument_id=WETHUSDC/data.parquet"
)
_CEFI_PATH = (
    "raw_tick_data/by_date/day=2026-05-01/asset_group=cefi/venue=binance/"
    "instrument_type=spot/instrument_id=BTCUSDT/data.parquet"
)


class TestIsBareDefiPath:
    """_is_bare_defi_path classification."""

    def test_detects_bare_path(self) -> None:
        assert _mod._is_bare_defi_path(_BARE_PATH) is True

    def test_rejects_canonical_with_asset_group(self) -> None:
        assert _mod._is_bare_defi_path(_CANONICAL_PATH) is False

    def test_rejects_legacy_category_segment(self) -> None:
        assert _mod._is_bare_defi_path(_CATEGORY_PATH) is False

    def test_rejects_cefi_canonical(self) -> None:
        assert _mod._is_bare_defi_path(_CEFI_PATH) is False

    def test_rejects_wrong_prefix(self) -> None:
        path = "other_prefix/by_date/day=2026-05-01/venue=foo/data.parquet"
        assert _mod._is_bare_defi_path(path) is False

    def test_rejects_too_short(self) -> None:
        assert _mod._is_bare_defi_path("raw_tick_data/by_date/day=2026-05-01/") is False


class TestCanonicalPath:
    """_canonical_path path transformation."""

    def test_inserts_asset_group_after_day(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert result == _CANONICAL_PATH

    def test_asset_group_segment_present(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert "asset_group=defi" in result

    def test_venue_follows_asset_group(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        parts = result.split("/")
        idx_ag = parts.index("asset_group=defi")
        assert parts[idx_ag + 1] == "venue=uniswap_v3"

    def test_day_segment_preserved(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert "day=2026-05-01" in result


class TestPrePostMigrationState:
    """Pre-state (bare) → post-state (canonical with asset_group=defi/) assertions."""

    def test_pre_state_lacks_asset_group(self) -> None:
        assert "asset_group=" not in _BARE_PATH
        assert "category=" not in _BARE_PATH

    def test_post_state_has_asset_group_defi(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert "asset_group=defi" in result

    def test_post_state_still_has_venue(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert "venue=uniswap_v3" in result

    def test_post_state_still_has_instrument_id(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        assert "instrument_id=WETHUSDC" in result

    def test_post_state_longer_by_one_segment(self) -> None:
        result = _mod._canonical_path(_BARE_PATH)
        pre_segments = _BARE_PATH.split("/")
        post_segments = result.split("/")
        assert len(post_segments) == len(pre_segments) + 1


class TestMigrateOne:
    """_migrate_one: mocked GCS copy+delete."""

    def _make_buckets(self) -> tuple[MagicMock, MagicMock]:
        src_blob = MagicMock()
        src_bucket = MagicMock()
        src_bucket.blob.return_value = src_blob
        dst_bucket = MagicMock()
        return src_bucket, dst_bucket

    def test_success_returns_ok_true(self) -> None:
        src_bucket, dst_bucket = self._make_buckets()
        result = _mod._migrate_one(src_bucket, dst_bucket, _BARE_PATH)
        assert result[2] is True  # ok
        assert result[3] == ""  # err

    def test_success_dst_is_canonical(self) -> None:
        src_bucket, dst_bucket = self._make_buckets()
        result = _mod._migrate_one(src_bucket, dst_bucket, _BARE_PATH)
        assert result[1] == _CANONICAL_PATH  # dst

    def test_success_calls_copy_then_delete(self) -> None:
        src_bucket, dst_bucket = self._make_buckets()
        src_blob = src_bucket.blob.return_value
        _mod._migrate_one(src_bucket, dst_bucket, _BARE_PATH)
        src_bucket.copy_blob.assert_called_once()
        src_blob.delete.assert_called_once()

    def test_gcs_error_returns_ok_false_no_delete(self) -> None:
        src_bucket, dst_bucket = self._make_buckets()
        src_bucket.copy_blob.side_effect = Exception("Network error")
        src_blob = src_bucket.blob.return_value
        result = _mod._migrate_one(src_bucket, dst_bucket, _BARE_PATH)
        assert result[2] is False  # ok
        assert "Network error" in result[3]  # err
        src_blob.delete.assert_not_called()
