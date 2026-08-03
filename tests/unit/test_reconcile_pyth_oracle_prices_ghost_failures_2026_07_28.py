"""Regression: PYTH oracle_prices stale day-level ghost-failure reconciliation.

Guards the mirror-image phantom case found by the ``mvp_backfill_defi_onchain_v10``
PYTH oracle_prices re-run gate verification (issue:
``plans/active/issues/pyth_oracle_prices_stale_ghost_failure_rows_2026_07_28.md``):
pre-fix (before ``market-tick-data-service@533514c2``) the aiodns-missing-resolver
crash was recorded as a DAY-LEVEL ``attempted_failed`` row (``instrument_id`` blank)
because the whole day's fetch aborted before any per-instrument row was written. The
fixed writer succeeds at PER-INSTRUMENT granularity, so a later successful re-run
writes real ``captured`` per-instrument rows for that date WITHOUT ever
touching/superseding the old day-level failure entry (different shard-key
components). This leaves a stale ghost ``attempted_failed`` row sitting alongside 14
legitimately ``captured`` rows for the same date forever — the existing
``reconcile_phantom_manifest_rows_all.py`` reconciler explicitly skips
``attempted_failed`` rows (its own docstring), so it never covers this case.

Tests the new ``_pyth_oracle_prices_ghost_failure_mask`` predicate + the sibling
``_apply_delete_pyth_oracle_prices_ghost_failures`` write path added to cover it.
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from types import ModuleType
from typing import NamedTuple

import pandas as pd


def _load_reconciler_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_phantom_manifest_rows_all.py"
    spec = importlib.util.spec_from_file_location("reconcile_phantom_manifest_rows_all_pyth_ghost_test", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Blob(NamedTuple):
    name: str


class _StubStorageClient:
    """In-memory storage client: canonical index + per-VM shards + upload capture."""

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


def _fixture_df() -> pd.DataFrame:
    """Mirrors the real distribution found in the 2026-07-28 gate verification."""
    return pd.DataFrame(
        [
            # (1) Genuine ghost: stale day-level failure, superseded by real
            # per-instrument captures for the same date -> MUST be deleted.
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "oracle_prices",
                "capture_status": "attempted_failed",
                "error_reason": "Resolver requires aiodns library",
                "instrument_id": "",
            },
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "oracle_prices",
                "capture_status": "captured",
                "error_reason": "",
                "instrument_id": "BTC-USD",
            },
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "oracle_prices",
                "capture_status": "captured",
                "error_reason": "",
                "instrument_id": "ETH-USD",
            },
            # (2) A day-level attempted_failed with NO captured sibling — a genuine
            # live gap, NOT a ghost. Must survive.
            {
                "date": "2025-08-08",
                "venue": "PYTH",
                "data_type": "oracle_prices",
                "capture_status": "attempted_failed",
                "error_reason": "PYTH_HERMES_HISTORICAL_HTTP_520",
                "instrument_id": "",
            },
            # (3) attempted_failed with a POPULATED instrument_id — a different
            # (per-instrument) failure mode, out of scope for this predicate. Must
            # survive even though captured siblings exist for the same date.
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "oracle_prices",
                "capture_status": "attempted_failed",
                "error_reason": "some transient per-instrument error",
                "instrument_id": "SOL-USD",
            },
            # (4) Same shape as (1) but a DIFFERENT venue — must NOT be caught
            # (predicate is scoped to PYTH oracle_prices only).
            {
                "date": "2024-04-30",
                "venue": "CHAINLINK",
                "data_type": "oracle_prices",
                "capture_status": "attempted_failed",
                "error_reason": "Resolver requires aiodns library",
                "instrument_id": "",
            },
            {
                "date": "2024-04-30",
                "venue": "CHAINLINK",
                "data_type": "oracle_prices",
                "capture_status": "captured",
                "error_reason": "",
                "instrument_id": "BTC-USD",
            },
            # (5) Same shape as (1) but a DIFFERENT data_type — must NOT be caught.
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "lst_rates",
                "capture_status": "attempted_failed",
                "error_reason": "Resolver requires aiodns library",
                "instrument_id": "",
            },
            {
                "date": "2024-04-30",
                "venue": "PYTH",
                "data_type": "lst_rates",
                "capture_status": "captured",
                "error_reason": "",
                "instrument_id": "STETH-USD",
            },
        ]
    )


def test_mask_flags_only_the_superseded_daylevel_ghost_row() -> None:
    mod = _load_reconciler_module()
    df = _fixture_df()

    mask = mod._pyth_oracle_prices_ghost_failure_mask(df)

    assert int(mask.sum()) == 1
    flagged = df[mask].iloc[0]
    assert flagged["venue"] == "PYTH"
    assert flagged["data_type"] == "oracle_prices"
    assert flagged["date"] == "2024-04-30"
    assert flagged["instrument_id"] == ""
    assert flagged["error_reason"] == "Resolver requires aiodns library"


def test_mask_leaves_unsuperseded_failure_untouched() -> None:
    """A day-level attempted_failed row with no captured sibling is a genuine live
    gap (e.g. the unrelated PYTH_HERMES_HISTORICAL_HTTP_520 residual) — not a ghost."""
    mod = _load_reconciler_module()
    df = _fixture_df()
    mask = mod._pyth_oracle_prices_ghost_failure_mask(df)
    unsuperseded_idx = df.index[df["error_reason"] == "PYTH_HERMES_HISTORICAL_HTTP_520"][0]
    assert not bool(mask.loc[unsuperseded_idx])


def test_mask_leaves_per_instrument_failure_untouched() -> None:
    """A populated-instrument_id attempted_failed row is a different failure mode —
    out of scope for the day-level ghost predicate even with captured siblings."""
    mod = _load_reconciler_module()
    df = _fixture_df()
    mask = mod._pyth_oracle_prices_ghost_failure_mask(df)
    per_instrument_idx = df.index[df["instrument_id"] == "SOL-USD"][0]
    assert not bool(mask.loc[per_instrument_idx])


def test_apply_delete_removes_only_the_ghost_row_and_preserves_everything_else() -> None:
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    df = _fixture_df()
    index_blob = "_index/availability_index.parquet"
    stub.seed_parquet(index_blob, df)

    n_deleted = mod._apply_delete_pyth_oracle_prices_ghost_failures(stub, "test-defi-bucket", index_blob, df)

    assert n_deleted == 1
    final_df = stub.read_parquet(index_blob)
    assert len(final_df) == len(df) - 1
    # The deleted row's exact identity (date, venue, data_type, capture_status,
    # instrument_id) no longer appears.
    still_present = (
        (final_df["date"] == "2024-04-30")
        & (final_df["venue"] == "PYTH")
        & (final_df["data_type"] == "oracle_prices")
        & (final_df["capture_status"] == "attempted_failed")
        & (final_df["instrument_id"] == "")
    )
    assert not bool(still_present.any())
    # captured count is unchanged; attempted_failed decreased by exactly 1.
    status_before = df["capture_status"]
    status_after = final_df["capture_status"]
    assert int((status_after == "captured").sum()) == int((status_before == "captured").sum())
    assert int((status_after == "attempted_failed").sum()) == int((status_before == "attempted_failed").sum()) - 1
    # The unrelated live-gap row and the per-instrument-failure row both survive.
    assert bool((final_df["error_reason"] == "PYTH_HERMES_HISTORICAL_HTTP_520").any())
    assert bool((final_df["instrument_id"] == "SOL-USD").any())
    # Other venues/data_types are untouched.
    assert bool((final_df["venue"] == "CHAINLINK").any())
    assert bool((final_df["data_type"] == "lst_rates").any())


def test_apply_delete_is_idempotent_on_a_clean_manifest() -> None:
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    df = _fixture_df()
    df = df[
        ~(
            (df["date"] == "2024-04-30")
            & (df["venue"] == "PYTH")
            & (df["data_type"] == "oracle_prices")
            & (df["capture_status"] == "attempted_failed")
            & (df["instrument_id"] == "")
        )
    ].reset_index(drop=True)
    index_blob = "_index/availability_index.parquet"
    stub.seed_parquet(index_blob, df)

    n_deleted = mod._apply_delete_pyth_oracle_prices_ghost_failures(stub, "test-defi-bucket", index_blob, df)

    assert n_deleted == 0


def test_apply_delete_preserves_shard_landed_before_write() -> None:
    """Staleness guard (mirrors reconcile_phantom_manifest_rows_stale_read_overwrite_
    2026_07_12): a per-VM shard write landing between the caller's read and this
    function's write-back must survive, not be silently reverted."""
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    df = _fixture_df()
    index_blob = "_index/availability_index.parquet"
    stub.seed_parquet(index_blob, df)

    late_shard_row = pd.DataFrame(
        [
            {
                "date": "2026-07-28",
                "venue": "LATE-SHARD-VENUE",
                "data_type": "trades",
                "service_name": "market-tick-data-service",
                "capture_status": "captured",
                "instrument_id": "LATE-INSTR",
            }
        ]
    )
    real_list_blobs = stub.list_blobs
    seeded = False

    def _list_blobs_with_late_shard(bucket: str, prefix: str = "") -> list[_Blob]:
        nonlocal seeded
        if not seeded and prefix == "_index/per_vm/":
            seeded = True
            stub.seed_parquet("_index/per_vm/vm-late.parquet", late_shard_row)
        return real_list_blobs(bucket, prefix=prefix)

    stub.list_blobs = _list_blobs_with_late_shard  # type: ignore[method-assign]

    n_deleted = mod._apply_delete_pyth_oracle_prices_ghost_failures(stub, "test-defi-bucket", index_blob, df)

    assert n_deleted == 1
    final_df = stub.read_parquet(index_blob)
    assert "LATE-SHARD-VENUE" in set(final_df["venue"])
