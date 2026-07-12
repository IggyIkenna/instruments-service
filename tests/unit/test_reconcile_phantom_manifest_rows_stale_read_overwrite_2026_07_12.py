"""Regression: reconciler survives a per-VM shard write landing mid-audit.

Guards ``reconcile_phantom_manifest_rows_stale_read_overwrite_2026_07_12``
(issue: ``plans/active/issues/reconcile_phantom_manifest_rows_stale_read_
overwrite_2026_07_12.md``). The reconciler used to read the canonical
manifest ONCE via a raw ``pd.read_parquet``, run its (potentially long,
multi-worker GCS-listing) phantom audit, then blindly re-upload the FULL
dataframe from that FIRST read — silently discarding any per-VM shard write
that landed during the audit window (the 2026-07-12 sports incident:
~2.5 hours of already-completed backfill progress briefly reverted).

This test simulates that exact race: a per-VM shard write "lands" (via a
wrapped ``_audit_generic``) between the reconciler's initial canonical read
and its write-back, and asserts the final uploaded canonical still contains
that shard's row — proving the write-back path now re-reads + re-merges via
``merge_canonical_with_outstanding_shards`` immediately before uploading,
instead of re-uploading the stale first snapshot.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from types import ModuleType
from typing import NamedTuple
from unittest.mock import patch

import pandas as pd


def _load_reconciler_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_phantom_manifest_rows_all.py"
    spec = importlib.util.spec_from_file_location("reconcile_phantom_manifest_rows_all_stale_read_test", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Blob(NamedTuple):
    name: str


class _StubStorageClient:
    """In-memory storage client: canonical + per-VM shards + upload capture.

    A single ``list_blobs`` serves both callers the reconciler drives: the
    phantom audit's on-disk object-existence probe (data-object prefixes,
    which never match anything seeded here — every manifest row in this test
    is a genuine phantom) and the per-VM shard merge helper's
    ``_index/per_vm/`` directory listing.
    """

    def __init__(self) -> None:
        self._storage: dict[str, bytes] = {}

    def download_bytes(self, _bucket: str, path: str) -> bytes:
        if path not in self._storage:
            raise FileNotFoundError(path)
        return self._storage[path]

    def upload_from_file_obj(self, _bucket: str, path: str, file_obj: io.BytesIO) -> None:
        self._storage[path] = file_obj.getvalue()

    def list_blobs(self, _bucket: str, prefix: str = "") -> list[_Blob]:
        return [_Blob(name=p) for p in self._storage if p.startswith(prefix)]

    def seed_parquet(self, path: str, df: pd.DataFrame) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self._storage[path] = buf.getvalue()

    def read_parquet(self, path: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self._storage[path]))


def _canonical_row() -> pd.DataFrame:
    """One captured CeFi row with no backing on-disk object — a genuine phantom."""
    return pd.DataFrame(
        [
            {
                "date": "2026-07-12",
                "venue": "REALCEFI",
                "data_type": "trades",
                "service_name": "market-tick-data-service",
                "instrument_count": 1,
                "written_at": "2026-07-12T00:00:00+00:00",
                "schema_version": 8,
                "capture_status": "captured",
            }
        ]
    )


def _late_shard_row() -> pd.DataFrame:
    """The per-VM shard that 'lands' during the audit window."""
    return pd.DataFrame(
        [
            {
                "date": "2026-07-12",
                "venue": "LATE-SHARD-VENUE",
                "data_type": "trades",
                "service_name": "market-tick-data-service",
                "instrument_count": 1,
                "written_at": "2026-07-12T07:03:42+00:00",
                "schema_version": 8,
                "capture_status": "captured",
            }
        ]
    )


def test_write_back_preserves_shard_written_during_audit() -> None:
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    stub.seed_parquet(mod.ASSET_GROUP_CONFIG["cefi"]["index"], _canonical_row())

    real_audit_generic = mod._audit_generic

    def _audit_generic_with_late_shard_race(*args: object, **kwargs: object) -> dict[int, bool]:
        # Simulate a per-VM shard write landing DURING the reconciler's
        # (long, multi-worker) audit pass — after its initial canonical
        # read, before its write-back.
        stub.seed_parquet("_index/per_vm/vm-late.parquet", _late_shard_row())
        return real_audit_generic(*args, **kwargs)

    with (
        patch.object(mod, "_audit_generic", side_effect=_audit_generic_with_late_shard_race),
        patch.object(mod, "get_storage_client", return_value=stub),
        patch.object(mod, "resolve_bucket_name", return_value="test-cefi-bucket"),
        patch.object(sys, "argv", ["reconcile_phantom_manifest_rows_all.py", "--asset-group", "cefi", "--apply"]),
    ):
        exit_code = mod.main()

    assert exit_code == 0

    final_df = stub.read_parquet(mod.ASSET_GROUP_CONFIG["cefi"]["index"])
    venues = set(final_df["venue"])

    # The late-landing shard's row survived the write-back — the lost-update
    # bug would have silently dropped it (the write-back re-uploads only what
    # the FIRST canonical read saw).
    assert "LATE-SHARD-VENUE" in venues
    # The original phantom row was still correctly flipped — the staleness
    # guard must not break the reconciler's actual job.
    real_row = final_df.loc[final_df["venue"] == "REALCEFI"].iloc[0]
    assert real_row["capture_status"] == "attempted_failed"
    assert real_row["error_reason"] == "phantom_captured_no_parquet_at_canonical_path"
    # The late shard's own row is untouched by the flip (it was never part
    # of the audited captured set).
    late_row = final_df.loc[final_df["venue"] == "LATE-SHARD-VENUE"].iloc[0]
    assert late_row["capture_status"] == "captured"
