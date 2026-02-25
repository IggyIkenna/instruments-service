"""Adapters package for instruments-service.

Thin I/O adapters that delegate to unified-cloud-services.
"""

from instruments_service.adapters.data_source_adapter import DataSourceAdapter
from instruments_service.adapters.storage_adapter import StorageAdapter

__all__ = ["DataSourceAdapter", "StorageAdapter"]
