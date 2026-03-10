"""
Unit tests for catalogue_updater module.

Tests _load_catalogue, _save_catalogue, _patch_flat_entry,
_patch_nested_entry, and update_catalogue_entry with mocked filesystem.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import yaml

from instruments_service.catalogue_updater import (
    _load_catalogue,
    _patch_flat_entry,
    _patch_nested_entry,
    _save_catalogue,
    update_catalogue_entry,
)

# ─── _load_catalogue ──────────────────────────────────────────────────────────

class TestLoadCatalogue:
    def test_returns_none_on_os_error(self, tmp_path):
        missing_path = tmp_path / "nonexistent.yaml"
        result = _load_catalogue(missing_path, "test_dataset")
        assert result is None

    def test_returns_parsed_yaml(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue_path.write_text("datasets:\n  - dataset_id: test_ds\n", encoding="utf-8")
        result = _load_catalogue(catalogue_path, "test_ds")
        assert result is not None
        assert "datasets" in result

    def test_returns_empty_dict_for_empty_file(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue_path.write_text("", encoding="utf-8")
        result = _load_catalogue(catalogue_path, "test_ds")
        assert result == {}

    def test_returns_none_on_yaml_error(self, tmp_path):
        catalogue_path = tmp_path / "bad.yaml"
        catalogue_path.write_text("{{invalid: yaml: }", encoding="utf-8")
        result = _load_catalogue(catalogue_path, "test_ds")
        assert result is None


# ─── _save_catalogue ──────────────────────────────────────────────────────────

class TestSaveCatalogue:
    def test_writes_yaml_to_disk(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue = {"datasets": [{"dataset_id": "ds1", "row_count_last_batch": 100}]}
        result = _save_catalogue(catalogue_path, catalogue)
        assert result is True
        assert catalogue_path.exists()
        loaded = yaml.safe_load(catalogue_path.read_text())
        assert loaded["datasets"][0]["dataset_id"] == "ds1"

    def test_returns_false_on_os_error(self, tmp_path):
        catalogue_path = tmp_path / "subdir" / "catalogue.yaml"
        # Parent dir doesn't exist -> write fails
        result = _save_catalogue(catalogue_path, {"datasets": []})
        assert result is False


# ─── _patch_flat_entry ────────────────────────────────────────────────────────

class TestPatchFlatEntry:
    def test_patches_matching_dataset(self):
        dt = datetime(2025, 3, 28, 12, 0, 0, tzinfo=UTC)
        catalogue = {
            "datasets": [
                {"dataset_id": "instruments_cefi_binance", "row_count_last_batch": 0},
                {"dataset_id": "instruments_tradfi_cme", "row_count_last_batch": 0},
            ]
        }
        result = _patch_flat_entry(catalogue, "instruments_cefi_binance", dt, 3200)
        assert result is True
        assert catalogue["datasets"][0]["row_count_last_batch"] == 3200
        assert "2025" in catalogue["datasets"][0]["last_updated"]

    def test_returns_false_for_missing_dataset(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {"datasets": [{"dataset_id": "other_ds", "row_count_last_batch": 0}]}
        result = _patch_flat_entry(catalogue, "nonexistent_dataset", dt, 100)
        assert result is False

    def test_returns_false_when_no_datasets_key(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {"other_key": "value"}
        result = _patch_flat_entry(catalogue, "ds1", dt, 100)
        assert result is False

    def test_returns_false_when_datasets_not_list(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {"datasets": "not a list"}
        result = _patch_flat_entry(catalogue, "ds1", dt, 100)
        assert result is False


# ─── _patch_nested_entry ─────────────────────────────────────────────────────

class TestPatchNestedEntry:
    def test_patches_direct_dict_child(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {
            "cefi": {"dataset_id": "instruments_cefi_binance", "row_count_last_batch": 0}
        }
        result = _patch_nested_entry(catalogue, "instruments_cefi_binance", dt, 500)
        assert result is True
        assert catalogue["cefi"]["row_count_last_batch"] == 500

    def test_patches_list_item(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {
            "categories": [
                {"dataset_id": "ds1", "row_count_last_batch": 0},
                {"dataset_id": "ds2", "row_count_last_batch": 0},
            ]
        }
        result = _patch_nested_entry(catalogue, "ds2", dt, 999)
        assert result is True
        assert catalogue["categories"][1]["row_count_last_batch"] == 999

    def test_returns_false_when_not_found(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {"category": {"dataset_id": "other_ds"}}
        result = _patch_nested_entry(catalogue, "not_present", dt, 100)
        assert result is False

    def test_patches_deeply_nested(self):
        dt = datetime(2025, 3, 28, tzinfo=UTC)
        catalogue = {
            "level1": {
                "level2": {
                    "dataset_id": "deep_dataset",
                    "row_count_last_batch": 0
                }
            }
        }
        result = _patch_nested_entry(catalogue, "deep_dataset", dt, 42)
        assert result is True


# ─── update_catalogue_entry ───────────────────────────────────────────────────

class TestUpdateCatalogueEntry:
    def test_returns_false_when_file_missing(self, tmp_path):
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = tmp_path / "nonexistent.yaml"
            result = update_catalogue_entry("test_ds", 100)
        assert result is False

    def test_returns_false_when_catalogue_load_fails(self, tmp_path):
        catalogue_path = tmp_path / "bad.yaml"
        catalogue_path.write_text("{{bad: yaml}", encoding="utf-8")
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = catalogue_path
            result = update_catalogue_entry("test_ds", 100)
        assert result is False

    def test_returns_false_when_dataset_not_found(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue = {"datasets": [{"dataset_id": "other_ds", "row_count_last_batch": 0}]}
        catalogue_path.write_text(yaml.dump(catalogue), encoding="utf-8")
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = catalogue_path
            result = update_catalogue_entry("nonexistent_ds", 100)
        assert result is False

    def test_successfully_updates_flat_entry(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue = {
            "datasets": [{"dataset_id": "instruments_cefi_binance", "row_count_last_batch": 0}]
        }
        catalogue_path.write_text(yaml.dump(catalogue), encoding="utf-8")
        dt = datetime(2025, 3, 28, 12, 0, 0, tzinfo=UTC)
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = catalogue_path
            result = update_catalogue_entry("instruments_cefi_binance", 3200, last_updated=dt)
        assert result is True
        loaded = yaml.safe_load(catalogue_path.read_text())
        assert loaded["datasets"][0]["row_count_last_batch"] == 3200

    def test_uses_current_time_when_last_updated_none(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue = {
            "datasets": [{"dataset_id": "ds1", "row_count_last_batch": 0}]
        }
        catalogue_path.write_text(yaml.dump(catalogue), encoding="utf-8")
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = catalogue_path
            result = update_catalogue_entry("ds1", 100, last_updated=None)
        assert result is True

    def test_successfully_updates_nested_entry(self, tmp_path):
        catalogue_path = tmp_path / "catalogue.yaml"
        catalogue = {
            "cefi": {
                "binance": {"dataset_id": "instruments_cefi_binance", "row_count_last_batch": 0}
            }
        }
        catalogue_path.write_text(yaml.dump(catalogue), encoding="utf-8")
        dt = datetime(2025, 3, 28, 12, 0, 0, tzinfo=UTC)
        with patch("instruments_service.catalogue_updater._resolve_catalogue_path") as mock_resolve:
            mock_resolve.return_value = catalogue_path
            result = update_catalogue_entry("instruments_cefi_binance", 500, last_updated=dt)
        assert result is True
