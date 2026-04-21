"""Unit tests for ``scripts/purge_legacy_unsharded_manifest_rows.py``.

The purger itself lives in UTL (``unified_trading_library.manifest_migrations.
LegacyRowPurger``); this test covers the script's module loading, argument
surface, and integration with UTL's ``LegacyRowPurger.dry_run``.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path

import pandas as pd


def _load_purge_module():  # type: ignore[no-untyped-def]
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "scripts" / "purge_legacy_unsharded_manifest_rows.py"
    spec = importlib.util.spec_from_file_location("purge_legacy_unsharded_manifest_rows", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_purge_module_defines_default_entity_types() -> None:
    mod = _load_purge_module()
    assert set(mod._DEFAULT_ENTITY_TYPES) >= {"FIXTURES", "WEATHER", "XG"}


def test_purge_dry_run_identifies_unsharded_rows_only_when_shard_exists() -> None:
    """LegacyRowPurger.dry_run drops unsharded rows iff a sharded sibling exists."""
    from unified_trading_library import LegacyRowPurger

    # Two dates worth of rows:
    # 2024-09-15 WEATHER: unsharded (league_id="") + sharded (EPL) → UNSHARDED DROPS
    # 2024-09-16 WEATHER: unsharded (league_id="") only → KEPT (no sharded sibling)
    rows = [
        {
            "date": "2024-09-15",
            "data_type": "WEATHER",
            "service_name": "instruments-service",
            "league_id": "",
            "instrument_count": 10,
            "schema_version": 5,
            "written_at": "2024-09-15T00:00:00Z",
            "attempted_at": "2024-09-15T00:00:00Z",
            "capture_status": "captured",
        },
        {
            "date": "2024-09-15",
            "data_type": "WEATHER",
            "service_name": "instruments-service",
            "league_id": "EPL",
            "instrument_count": 5,
            "schema_version": 5,
            "written_at": "2024-09-15T00:00:00Z",
            "attempted_at": "2024-09-15T00:00:00Z",
            "capture_status": "captured",
        },
        {
            "date": "2024-09-16",
            "data_type": "WEATHER",
            "service_name": "instruments-service",
            "league_id": "",
            "instrument_count": 8,
            "schema_version": 5,
            "written_at": "2024-09-16T00:00:00Z",
            "attempted_at": "2024-09-16T00:00:00Z",
            "capture_status": "captured",
        },
    ]
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)

    class _FakeStorage:
        def __init__(self) -> None:
            self.written: bytes | None = None

        def blob_exists(self, bucket: str, path: str) -> bool:
            return True

        def download_bytes(self, bucket: str, path: str) -> bytes:
            return buf.getvalue()

        def upload_bytes(self, bucket: str, path: str, data: bytes) -> None:
            self.written = data

    storage = _FakeStorage()
    purger = LegacyRowPurger(
        "bucket",
        service_name="instruments-service",
        storage=storage,  # type: ignore[arg-type]
        index_blob="_index/availability_index.parquet",
    )

    removed = purger.dry_run(["WEATHER"])
    # Only the 2024-09-15 unsharded row should drop; 2024-09-16 has no shard.
    assert len(removed) == 1
    assert removed[0].date == "2024-09-15"
    assert removed[0].league_id == ""
