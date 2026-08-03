"""Unit tests — purge_flat_instrument_availability_hive_2026_08_03.py.

Tests cover:
  1. hive_target_for: same flat -> full-hive mapping as the sibling copy script (kept in lockstep).
  2. scan: bounded prefix listing classification (mocked client.list_blobs).
  3. _process_one_object: fresh-verify-then-(maybe)-delete outcomes — eligible / deleted / no_twin /
     content_mismatch / source_vanished / race_lost / failed (mocked gcs_describe_object /
     gcs_conditional_delete).
  4. main(): the §3a retention-gate abort path.

No GCS or network access — pure helpers tested directly; GCS mocked.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "purge_flat_instrument_availability_hive_2026_08_03.py"
    module_name = "_purge_flat_instrument_availability_hive_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_script()

pytestmark = pytest.mark.unit

_CEFI_FLAT = "instrument_availability/by_date/day=2026-07-26/venue=HYPERLIQUID/instruments.parquet"
_CEFI_HIVE = (
    "instrument_availability/by_date/day=2026-07-26/pipeline_mode=batch_instruments_service/"
    "asset_group=cefi/venue=HYPERLIQUID/instruments.parquet"
)
_ALREADY_HIVE = _CEFI_HIVE
_UNRECOGNIZED = "instrument_availability/by_date/day=2026-07-26/some_other_shape.parquet"


class TestHiveTargetFor:
    def test_cefi_flat_maps_to_batch_instruments_service(self) -> None:
        assert _mod.hive_target_for(_CEFI_FLAT, "cefi") == _CEFI_HIVE

    def test_already_hive_path_returns_none(self) -> None:
        assert _mod.hive_target_for(_ALREADY_HIVE, "cefi") is None

    def test_unrecognized_tree_returns_none(self) -> None:
        assert _mod.hive_target_for(_UNRECOGNIZED, "cefi") is None


class TestScan:
    def _mock_client(self, names: list[str]) -> MagicMock:
        blobs = []
        for n in names:
            b = MagicMock()
            b.name = n
            blobs.append(b)
        client = MagicMock()
        client.list_blobs.side_effect = lambda bucket, prefix: [b for b in blobs if b.name.startswith(prefix)]
        return client

    def test_classifies_candidates_already_hive_and_unrecognized(self) -> None:
        client = self._mock_client([_CEFI_FLAT, _ALREADY_HIVE, _UNRECOGNIZED])
        result = _mod.scan(client, "instruments-store-cefi-prd", "cefi")
        assert result.candidate_count == 1
        assert result.candidates[0] == (_CEFI_FLAT, _CEFI_HIVE)
        assert result.already_hive == 1
        assert result.unrecognized == 1


class TestProcessOneObject:
    def _meta(self, crc32c: str, size: int, generation: int | None = 100) -> MagicMock:
        m = MagicMock()
        m.crc32c = crc32c
        m.size = size
        m.generation = generation
        return m

    def test_eligible_when_twin_matches_and_apply_false(self) -> None:
        meta = self._meta("AAAA==", 100)
        with patch.object(_mod, "gcs_describe_object", side_effect=[meta, meta]) as mock_describe:
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=False)
        assert outcome == "eligible"
        assert detail is None
        assert mock_describe.call_count == 2

    def test_deleted_when_twin_matches_and_apply_true(self) -> None:
        meta = self._meta("AAAA==", 100, generation=42)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[meta, meta]),
            patch.object(_mod, "gcs_conditional_delete", return_value=True) as mock_delete,
        ):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "deleted"
        assert detail is None
        mock_delete.assert_called_once_with(f"gs://bkt/{_CEFI_FLAT}", if_generation_match=42)

    def test_race_lost_when_conditional_delete_fails_precondition(self) -> None:
        meta = self._meta("AAAA==", 100, generation=42)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[meta, meta]),
            patch.object(_mod, "gcs_conditional_delete", return_value=False),
        ):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "race_lost"
        assert detail is not None

    def test_no_twin_when_target_absent(self) -> None:
        src_meta = self._meta("AAAA==", 100)
        with patch.object(_mod, "gcs_describe_object", side_effect=[src_meta, None]):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "no_twin"
        assert "no-migrate-first" in (detail or "")

    def test_content_mismatch_never_deletes_even_when_apply_true(self) -> None:
        src_meta = self._meta("AAAA==", 100)
        dst_meta = self._meta("BBBB==", 101)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[src_meta, dst_meta]),
            patch.object(_mod, "gcs_conditional_delete") as mock_delete,
        ):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "content_mismatch"
        assert detail is not None
        mock_delete.assert_not_called()

    def test_source_vanished_when_source_missing(self) -> None:
        with patch.object(_mod, "gcs_describe_object", side_effect=[None]):
            _src, outcome, _detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "source_vanished"

    def test_failed_when_generation_missing(self) -> None:
        meta = self._meta("AAAA==", 100, generation=None)
        with patch.object(_mod, "gcs_describe_object", side_effect=[meta, meta]):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "failed"
        assert "generation" in (detail or "")

    def test_failed_on_unexpected_exception(self) -> None:
        with patch.object(_mod, "gcs_describe_object", side_effect=RuntimeError("boom")):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE, apply=True)
        assert outcome == "failed"
        assert "boom" in (detail or "")


class TestRetentionGate:
    def test_apply_prod_aborts_below_threshold(self) -> None:
        with (
            patch.object(_mod, "_bucket_for", return_value="instruments-store-cefi-prd"),
            patch.object(_mod, "gcs_bucket_soft_delete_retention_seconds", return_value=86400),
            patch.object(sys, "argv", ["prog", "--asset-group", "cefi", "--apply-prod"]),
        ):
            rc = _mod.main()
        assert rc == 1

    def test_apply_prod_proceeds_at_or_above_threshold(self) -> None:
        with (
            patch.object(_mod, "_bucket_for", return_value="instruments-store-cefi-prd"),
            patch.object(_mod, "gcs_bucket_soft_delete_retention_seconds", return_value=604800),
            patch.object(_mod, "purge_asset_group", return_value=_mod.PurgeResult(asset_group="cefi")) as mock_purge,
            patch.object(sys, "argv", ["prog", "--asset-group", "cefi", "--apply-prod"]),
        ):
            rc = _mod.main()
        assert rc == 0
        mock_purge.assert_called_once()
        assert mock_purge.call_args.kwargs["apply"] is False  # no --confirm-prod-write => plan only
