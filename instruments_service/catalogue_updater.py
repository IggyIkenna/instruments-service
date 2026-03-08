"""
Data Catalogue Auto-Updater

Updates data-catalogue-*.yaml entries after each successful instruments batch write.
Called from InstrumentsService.generate_instruments_for_date() after cloud storage succeeds.

Auto-updates two mutable fields per dataset entry:
  - last_updated: ISO 8601 datetime of the most recent batch write
  - row_count_last_batch: row count from the most recent batch write

The catalogue YAML lives at:
  deployment-service/configs/data-catalogue.instruments-service.yaml

After updating, emits a CATALOGUE_UPDATED coordination event so downstream caches can refresh.

Usage:
    from instruments_service.catalogue_updater import update_catalogue_entry
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

# Canonical path: deployment-service/configs/data-catalogue.instruments-service.yaml
# Resolved relative to the workspace root (parent of instruments-service/)
_REPO_ROOT = Path(__file__).parent.parent
_WORKSPACE_ROOT = _REPO_ROOT.parent
_CATALOGUE_PATH = _WORKSPACE_ROOT / "deployment-service" / "configs" / "data-catalogue.instruments-service.yaml"


def _resolve_catalogue_path() -> Path:
    """
    Resolve the catalogue YAML path.

    Priority:
    1. config.catalogue_path_override (INSTRUMENTS_CATALOGUE_PATH; allows override in tests / CI)
    2. Workspace-relative path: deployment-service/configs/data-catalogue.instruments-service.yaml

    Returns:
        Path to the catalogue YAML file.
    """
    config = get_config()
    if config.catalogue_path_override:
        return Path(config.catalogue_path_override)
    return _CATALOGUE_PATH


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

    This function is intentionally narrow: it only patches the two mutable stats
    fields. All other fields (schema_ref, gcp_path, status, etc.) are managed by
    humans and must not be overwritten here.

    Args:
        dataset_id: Unique dataset identifier matching the ``dataset_id`` field in
            the catalogue YAML entry, e.g. ``"instruments_cefi_binance"``.
        row_count: Row count from the most recent batch write.
        last_updated: Datetime of the batch write. Defaults to ``datetime.now(UTC)``.

    Returns:
        True if the entry was found and updated; False if the entry was not found or
        the catalogue file does not exist (logged as a warning, not raised).
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

    try:
        raw_text = catalogue_path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning(
            "catalogue_updater: could not read catalogue file %s: %s — skipping update for dataset_id=%s",
            catalogue_path,
            e,
            dataset_id,
        )
        return False

    try:
        catalogue: dict[str, object] = yaml.safe_load(raw_text) or {}
    except yaml.YAMLError as e:
        logger.warning(
            "catalogue_updater: could not parse catalogue YAML %s: %s — skipping update for dataset_id=%s",
            catalogue_path,
            e,
            dataset_id,
        )
        return False

    # Locate the dataset entry by dataset_id.
    # Supports two catalogue layouts:
    #   (a) flat list: datasets: [{dataset_id: ..., ...}, ...]
    #   (b) nested by category: cefi: {binance: {dataset_id: ..., ...}}
    # Also patches the top-level service shard_status.last_updated field if present.

    entry_found = False
    datasets_raw = catalogue.get("datasets")
    if isinstance(datasets_raw, list):
        for entry in datasets_raw:
            if isinstance(entry, dict) and entry.get("dataset_id") == dataset_id:
                entry["last_updated"] = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
                entry["row_count_last_batch"] = row_count
                entry_found = True
                break

    if not entry_found:
        # Fallback: walk all nested dicts looking for {"dataset_id": dataset_id}
        entry_found = _patch_nested_entry(catalogue, dataset_id, last_updated, row_count)

    if not entry_found:
        logger.warning(
            "catalogue_updater: dataset_id=%s not found in %s — skipping update",
            dataset_id,
            catalogue_path,
        )
        return False

    # Patch top-level last_updated on the catalogue file itself
    catalogue["last_updated"] = last_updated.strftime("%Y-%m-%d")

    try:
        updated_text = yaml.dump(catalogue, default_flow_style=False, allow_unicode=True, sort_keys=False)
        catalogue_path.write_text(updated_text, encoding="utf-8")
    except OSError as e:
        logger.warning(
            "catalogue_updater: could not write catalogue file %s: %s",
            catalogue_path,
            e,
        )
        return False

    logger.info(
        "catalogue_updater: updated dataset_id=%s last_updated=%s row_count=%s in %s",
        dataset_id,
        last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        row_count,
        catalogue_path.name,
    )

    # Emit coordination event so downstream caches (UCI MetadataClient, instruments-service
    # in-memory snapshot) can refresh without a restart.
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
