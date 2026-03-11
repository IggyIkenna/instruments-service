"""
Data Catalogue Auto-Updater

Updates data-catalogue-*.yaml entries after each successful instruments batch write.
Called from InstrumentsService.generate_instruments_for_date() after cloud storage succeeds.

Auto-updates two mutable fields per dataset entry:
  - last_updated: ISO 8601 datetime of the most recent batch write
  - row_count_last_batch: row count from the most recent batch write

The catalogue YAML lives at:
  unified-trading-pm/configs/data-catalogue.instruments-service.yaml

After updating, emits a CATALOGUE_UPDATED coordination event so downstream caches can refresh.

Usage (import update_catalogue_entry at module level, then call):
    update_catalogue_entry(
        dataset_id="instruments_cefi_binance",
        row_count=3200,
        last_updated=datetime.now(UTC),
    )
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import yaml
from unified_events_interface import log_event

from instruments_service.config.service_config import get_config

logger = logging.getLogger(__name__)

# Canonical path: unified-trading-pm/configs/data-catalogue.instruments-service.yaml
# Resolved relative to the workspace root (parent of instruments-service/)
_REPO_ROOT = Path(__file__).parent.parent
_WORKSPACE_ROOT = _REPO_ROOT.parent
_CATALOGUE_PATH = _WORKSPACE_ROOT / "unified-trading-pm" / "configs" / "data-catalogue.instruments-service.yaml"


def _resolve_catalogue_path() -> Path:
    """
    Resolve the catalogue YAML path.

    Priority:
    1. config.catalogue_path_override (INSTRUMENTS_CATALOGUE_PATH; allows override in tests / CI)
    2. Workspace-relative path: unified-trading-pm/configs/data-catalogue.instruments-service.yaml

    Returns:
        Path to the catalogue YAML file.
    """
    config = get_config()
    if config.catalogue_path_override:
        return Path(config.catalogue_path_override)
    return _CATALOGUE_PATH


def _load_catalogue(catalogue_path: Path, dataset_id: str) -> dict[str, object] | None:
    """Read and parse the catalogue YAML file. Returns None on any error."""
    try:
        raw_text = catalogue_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning(
            "catalogue_updater: could not read catalogue file %s: %s — skipping update for dataset_id=%s",
            catalogue_path,
            e,
            dataset_id,
        )
        return None
    try:
        return yaml.safe_load(raw_text) or {}
    except yaml.YAMLError as e:
        logger.warning(
            "catalogue_updater: could not parse catalogue YAML %s: %s — skipping update for dataset_id=%s",
            catalogue_path,
            e,
            dataset_id,
        )
        return None


def _save_catalogue(catalogue_path: Path, catalogue: dict[str, object]) -> bool:
    """Write updated catalogue YAML to disk. Returns False on error."""
    try:
        updated_text = yaml.dump(catalogue, default_flow_style=False, allow_unicode=True, sort_keys=False)
        catalogue_path.write_text(updated_text, encoding="utf-8")
        return True
    except OSError as e:
        logger.warning("catalogue_updater: could not write catalogue file %s: %s", catalogue_path, e)
        return False


def _patch_flat_entry(
    catalogue: dict[str, object],
    dataset_id: str,
    last_updated: datetime,
    row_count: int,
) -> bool:
    """Patch flat-list layout: datasets: [{dataset_id: ..., ...}, ...]."""
    datasets_raw = catalogue.get("datasets")
    if not isinstance(datasets_raw, list):
        return False
    for entry in datasets_raw:
        if isinstance(entry, dict) and entry.get("dataset_id") == dataset_id:
            entry["last_updated"] = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
            entry["row_count_last_batch"] = row_count
            return True
    return False


def update_catalogue_entry(
    dataset_id: str,
    row_count: int,
    last_updated: datetime | None = None,
) -> bool:
    """
    Update a single dataset entry in the instruments-service data catalogue YAML.

    Reads the existing YAML, locates the dataset entry by ``dataset_id``, patches
    ``last_updated`` and ``row_count_last_batch``, and writes it back in place.
    Emits a ``CATALOGUE_UPDATED`` coordination event on success.
    Returns False if the entry was not found or the catalogue file does not exist.
    """
    if last_updated is None:
        last_updated = datetime.now(UTC)

    catalogue_path = _resolve_catalogue_path()

    if not catalogue_path.exists():
        logger.warning(
            "catalogue_updater: catalogue file not found at %s — skipping update for dataset_id=%s",
            catalogue_path,
            dataset_id,
        )
        return False

    catalogue = _load_catalogue(catalogue_path, dataset_id)
    if catalogue is None:
        return False

    # Supports two catalogue layouts: flat list and nested-by-category.
    entry_found = _patch_flat_entry(catalogue, dataset_id, last_updated, row_count)
    if not entry_found:
        entry_found = _patch_nested_entry(catalogue, dataset_id, last_updated, row_count)

    if not entry_found:
        logger.warning(
            "catalogue_updater: dataset_id=%s not found in %s — skipping update",
            dataset_id,
            catalogue_path,
        )
        return False

    catalogue["last_updated"] = last_updated.strftime("%Y-%m-%d")
    if not _save_catalogue(catalogue_path, catalogue):
        return False

    logger.info(
        "catalogue_updater: updated dataset_id=%s last_updated=%s row_count=%s in %s",
        dataset_id,
        last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        row_count,
        catalogue_path.name,
    )
    log_event(
        "CATALOGUE_UPDATED",
        details={
            "dataset_id": dataset_id,
            "last_updated": last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "row_count_last_batch": row_count,
            "catalogue_file": catalogue_path.name,
        },
    )
    return True


def _patch_nested_entry(
    node: dict[str, object],
    dataset_id: str,
    last_updated: datetime,
    row_count: int,
) -> bool:
    """
    Recursively walk a nested dict, patching the first entry whose ``dataset_id``
    matches. Returns True if a match was found and patched.
    """
    for value in node.values():
        if isinstance(value, dict):
            if value.get("dataset_id") == dataset_id:
                value["last_updated"] = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                value["row_count_last_batch"] = row_count
                return True
            if _patch_nested_entry(value, dataset_id, last_updated, row_count):
                return True
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    if item.get("dataset_id") == dataset_id:
                        item["last_updated"] = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                        item["row_count_last_batch"] = row_count
                        return True
                    if _patch_nested_entry(item, dataset_id, last_updated, row_count):
                        return True
    return False
