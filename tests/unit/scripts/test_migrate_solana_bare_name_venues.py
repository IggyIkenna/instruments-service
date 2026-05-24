"""Unit tests — migrate_solana_bare_name_venues.py.

Tests cover:
  1. Category A (MARINADE captured rows): manifest rows updated, parquets copied
     with venue column rewritten, old rows marked attempted_failed.
  2. Category B (DRIFT empty rows): just phantom-marked, no parquet copy.
  3. Dry-run mode: no writes performed.
  4. MANIFEST_PER_VM_SHARDS=true assertion in apply mode.
  5. No parquets found for a row: skipped gracefully, warning logged.

No GCS or network access — uses in-memory mock storage client + manifest DataFrame.
"""

from __future__ import annotations

import importlib.util
import io
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_solana_bare_name_venues.py"
    module_name = "_migrate_solana_bare_name_venues_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_script()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manifest(rows: list[dict[str, object]]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def _make_parquet_bytes(venue: str, n_rows: int = 3) -> bytes:
    df = pd.DataFrame({"venue": [venue] * n_rows, "value": list(range(n_rows))})
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def _blob(name: str) -> MagicMock:
    b = MagicMock()
    b.name = name
    return b


# ---------------------------------------------------------------------------
# Tests: dry-run
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_no_writes_in_dry_run(self, caplog: pytest.LogCaptureFixture) -> None:
        manifest_bytes = _make_manifest(
            [
                {
                    "date": "2024-01-15",
                    "venue": "MARINADE",
                    "capture_status": "captured",
                    "data_type": "lst_rates",
                    "chain": "SOLANA",
                    "instrument_type": "staking",
                    "instrument_count": 2,
                    "error_reason": "",
                    "attempted_at": "",
                }
            ]
        )
        storage = MagicMock()
        storage.download_bytes.return_value = manifest_bytes
        # list_blobs returns one parquet
        blob = _blob(
            "raw_tick_data/by_date/day=2024-01-15/asset_group=defi/venue=MARINADE"
            "/chain=SOLANA/instrument_type=staking/data_type=lst_rates/data.parquet"
        )
        storage.list_blobs.return_value = [blob]

        with (
            patch.object(_mod, "get_storage_client", return_value=storage),
            patch.object(_mod, "_resolve_bucket", return_value="market-data-tick-defi-test-project"),
        ):
            result = _mod.main.__wrapped__() if hasattr(_mod.main, "__wrapped__") else None
            # Run via calling main with sys.argv patched
            with patch("sys.argv", ["script"]):
                rc = _mod.main()

        assert rc == 0
        # No upload_bytes calls (dry-run)
        storage.upload_bytes.assert_not_called()
        storage.download_bytes.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: Category A (has data)
# ---------------------------------------------------------------------------


class TestCategoryAMigration:
    def _run_apply(
        self,
        manifest_bytes: bytes,
        storage: MagicMock,
    ) -> tuple[int, bytes]:
        uploaded: list[bytes] = []
        uploaded_paths: list[str] = []

        def capture_upload(bucket: str, path: str, data: bytes, **_: object) -> str:
            uploaded_paths.append(path)
            if path == "_index/availability_index.parquet":
                uploaded.append(data)
            return f"gs://{bucket}/{path}"

        storage.upload_bytes.side_effect = capture_upload

        env = {
            "MANIFEST_PER_VM_SHARDS": "true",
            "VM_NAME": "test-vm",
            "DEPLOYMENT_ENV": "prod",
        }
        with (
            patch.object(_mod, "get_storage_client", return_value=storage),
            patch.object(_mod, "_resolve_bucket", return_value="market-data-tick-defi-test-project"),
            patch.dict(os.environ, env),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
        ):
            rc = _mod.main()

        manifest_out = uploaded[-1] if uploaded else b""
        return rc, manifest_out

    def test_marinade_rows_migrated(self) -> None:
        manifest_bytes = _make_manifest(
            [
                {
                    "date": "2024-01-15",
                    "venue": "MARINADE",
                    "capture_status": "captured",
                    "data_type": "lst_rates",
                    "chain": "SOLANA",
                    "instrument_type": "staking",
                    "instrument_count": 2,
                    "error_reason": "",
                    "attempted_at": "",
                }
            ]
        )
        old_parquet_bytes = _make_parquet_bytes("MARINADE", 2)
        storage = MagicMock()
        storage.download_bytes.side_effect = lambda bucket, path: (
            manifest_bytes if path == "_index/availability_index.parquet" else old_parquet_bytes
        )
        old_blob_name = (
            "raw_tick_data/by_date/day=2024-01-15/asset_group=defi/venue=MARINADE"
            "/chain=SOLANA/instrument_type=staking/data_type=lst_rates/data.parquet"
        )
        storage.list_blobs.side_effect = lambda bucket, prefix="": (
            [_blob(old_blob_name)] if "venue=MARINADE/" in prefix else []
        )

        rc, manifest_out = self._run_apply(manifest_bytes, storage)
        assert rc == 0

        df_out = pd.read_parquet(io.BytesIO(manifest_out))
        # Old row marked attempted_failed
        old_rows = df_out[df_out["venue"] == "MARINADE"]
        assert len(old_rows) == 1
        assert old_rows.iloc[0]["capture_status"] == "attempted_failed"

        # New canonical row added
        new_rows = df_out[df_out["venue"] == "MARINADE-SOLANA"]
        assert len(new_rows) == 1
        assert new_rows.iloc[0]["capture_status"] == "captured"

    def test_new_parquet_has_updated_venue_column(self) -> None:
        manifest_bytes = _make_manifest(
            [
                {
                    "date": "2024-02-10",
                    "venue": "ORCA",
                    "capture_status": "captured",
                    "data_type": "dex_pools",
                    "chain": "SOLANA",
                    "instrument_type": "pool",
                    "instrument_count": 5,
                    "error_reason": "",
                    "attempted_at": "",
                }
            ]
        )
        old_parquet_bytes = _make_parquet_bytes("ORCA", 5)
        new_parquet_data: list[bytes] = []

        def upload_side_effect(bucket: str, path: str, data: bytes, **_: object) -> str:
            if path.endswith(".parquet") and "venue=ORCA-SOLANA" in path:
                new_parquet_data.append(data)
            return f"gs://{bucket}/{path}"

        storage = MagicMock()
        storage.download_bytes.side_effect = lambda bucket, path: (
            manifest_bytes if path == "_index/availability_index.parquet" else old_parquet_bytes
        )
        old_blob_name = (
            "raw_tick_data/by_date/day=2024-02-10/asset_group=defi/venue=ORCA"
            "/chain=SOLANA/instrument_type=pool/data_type=dex_pools/data.parquet"
        )
        storage.list_blobs.side_effect = lambda bucket, prefix="": (
            [_blob(old_blob_name)] if "venue=ORCA/" in prefix else []
        )
        storage.upload_bytes.side_effect = upload_side_effect

        env = {"MANIFEST_PER_VM_SHARDS": "true", "VM_NAME": "test-vm", "DEPLOYMENT_ENV": "prod"}
        with (
            patch.object(_mod, "get_storage_client", return_value=storage),
            patch.object(_mod, "_resolve_bucket", return_value="market-data-tick-defi-test-project"),
            patch.dict(os.environ, env),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
        ):
            _mod.main()

        # Script probes both asset_group= and category= hive keys, so uploads ≥ 1.
        assert len(new_parquet_data) >= 1
        for pq_bytes in new_parquet_data:
            df_new = pd.read_parquet(io.BytesIO(pq_bytes))
            assert (df_new["venue"] == "ORCA-SOLANA").all()

    def test_no_parquets_found_row_skipped(self, caplog: pytest.LogCaptureFixture) -> None:
        manifest_bytes = _make_manifest(
            [
                {
                    "date": "2024-03-01",
                    "venue": "RAYDIUM",
                    "capture_status": "captured",
                    "data_type": "dex_pools",
                    "chain": "SOLANA",
                    "instrument_type": "pool",
                    "instrument_count": 3,
                    "error_reason": "",
                    "attempted_at": "",
                }
            ]
        )
        storage = MagicMock()
        storage.download_bytes.return_value = manifest_bytes
        storage.list_blobs.return_value = []  # no blobs found

        env = {"MANIFEST_PER_VM_SHARDS": "true", "VM_NAME": "test-vm", "DEPLOYMENT_ENV": "prod"}
        with (
            patch.object(_mod, "get_storage_client", return_value=storage),
            patch.object(_mod, "_resolve_bucket", return_value="market-data-tick-defi-test-project"),
            patch.dict(os.environ, env),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
        ):
            rc = _mod.main()

        assert rc == 0
        # Even with no parquets found, the old row is still phantom-marked and manifest written.
        # upload_bytes called for: backup + manifest index.
        upload_paths = [call_args[0][1] for call_args in storage.upload_bytes.call_args_list]
        assert "_index/availability_index.parquet" in upload_paths
        # Backup also written
        assert any(".bak.parquet" in p for p in upload_paths)
        # No parquet file copies (only manifest + backup)
        parquet_copies = [p for p in upload_paths if "venue=RAYDIUM-SOLANA" in p]
        assert len(parquet_copies) == 0


# ---------------------------------------------------------------------------
# Tests: Category B (empty → phantom-mark only)
# ---------------------------------------------------------------------------


class TestCategoryBPhantomMark:
    def test_drift_rows_phantom_marked_no_parquet_copy(self) -> None:
        manifest_bytes = _make_manifest(
            [
                {
                    "date": "2024-01-15",
                    "venue": "DRIFT",
                    "capture_status": "expected_unattempted",
                    "data_type": "perp_funding",
                    "chain": "SOLANA",
                    "instrument_type": "perpetual",
                    "instrument_count": 0,
                    "error_reason": "",
                    "attempted_at": "",
                }
            ]
        )
        uploaded_manifest: list[bytes] = []

        def upload_side_effect(bucket: str, path: str, data: bytes, **_: object) -> str:
            if path == "_index/availability_index.parquet":
                uploaded_manifest.append(data)
            return f"gs://{bucket}/{path}"

        storage = MagicMock()
        storage.download_bytes.return_value = manifest_bytes
        storage.upload_bytes.side_effect = upload_side_effect

        env = {"MANIFEST_PER_VM_SHARDS": "true", "VM_NAME": "test-vm", "DEPLOYMENT_ENV": "prod"}
        with (
            patch.object(_mod, "get_storage_client", return_value=storage),
            patch.object(_mod, "_resolve_bucket", return_value="market-data-tick-defi-test-project"),
            patch.dict(os.environ, env),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
        ):
            rc = _mod.main()

        assert rc == 0
        # No parquet copy
        storage.list_blobs.assert_not_called()

        assert len(uploaded_manifest) == 1
        df_out = pd.read_parquet(io.BytesIO(uploaded_manifest[0]))
        row = df_out[df_out["venue"] == "DRIFT"].iloc[0]
        assert row["capture_status"] == "attempted_failed"


# ---------------------------------------------------------------------------
# Tests: shard isolation enforcement
# ---------------------------------------------------------------------------


class TestShardIsolation:
    def test_apply_requires_manifest_per_vm_shards(self) -> None:
        env = {k: v for k, v in os.environ.items() if k not in ("MANIFEST_PER_VM_SHARDS", "VM_NAME")}
        with (
            patch.dict(os.environ, env, clear=True),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
        ):
            rc = _mod.main()
        assert rc == 1

    def test_apply_requires_vm_name(self) -> None:
        env = {"MANIFEST_PER_VM_SHARDS": "true"}
        clean = {k: v for k, v in os.environ.items() if k != "VM_NAME"}
        with (
            patch.dict(os.environ, {**clean, **env}, clear=False),
            patch("sys.argv", ["script", "--apply", "--confirm"]),
            patch.dict(os.environ, {"VM_NAME": ""}, clear=False),
        ):
            rc = _mod.main()
        assert rc == 1
