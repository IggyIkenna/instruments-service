"""Adapters package for instruments-service.

Thin I/O adapters that delegate to unified-trading-library.
"""

from instruments_service.adapters.broadcast_sink import BroadcastSink
from instruments_service.adapters.data_source_adapter import DataSourceAdapter
from instruments_service.adapters.live_data_source import LiveDataSource
from instruments_service.adapters.storage_adapter import StorageAdapter

__all__ = ["BroadcastSink", "DataSourceAdapter", "LiveDataSource", "StorageAdapter"]
