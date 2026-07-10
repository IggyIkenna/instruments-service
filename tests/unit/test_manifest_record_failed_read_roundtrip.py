"""Read-path round-trip: record_failed() write -> manifest read confirms ATTEMPTED_FAILED.

CF-11 gap (honest_coverage_metrics_2026_04_19 / shard-level-failure-isolation lineage):
every existing manifest test in this repo either (a) mocks ``ManifestWriter`` entirely
(``test_orchestrator_polymarket_capture_status.py`` — despite its name, it stubs
``record_failed`` and never reads anything back), or (b) exercises the adapter-side
``_should_skip_shard`` pre-flight against a hand-built ``MagicMock`` row. None of them
prove the actual production round trip: a real ``ManifestWriter.record_failed()`` call,
flushed to storage, is later visible to a fresh ``ManifestWriter.lookup()`` (the exact
API adapters use for pre-flight retry decisions, per
``codex/02-data/availability-manifest-and-data-status.md``) with
``capture_status == CaptureStatus.ATTEMPTED_FAILED.value``.

Uses the same in-memory ``_StorageClient``-protocol stub pattern proven in
unified-trading-library's own ``test_manifest_writer_capture_status.py``
(``test_lookup_returns_existing_row``): ``_client = None`` on the stub makes
``_read_consolidated_if_fresh`` take the non-GCS-native branch (a plain
``client.download_bytes(bucket, INDEX_PATH)``), so no real GCS mocking is needed.
"""

from __future__ import annotations

import io
from unittest.mock import patch

import pandas as pd
import pytest
import unified_trading_library.manifest_writer as _mw_module
from unified_api_contracts import PipelineMode
from unified_trading_library import CaptureStatus, ManifestRow, ManifestWriter

# Real production service_name (see instruments_service/engine/orchestrator/*.py —
# every ManifestWriter(...) call site uses this literal).
_SERVICE_NAME = "instruments-service"


class _StubStorageClient:
    """In-memory stand-in for UTL's ``_StorageClient`` protocol.

    ``_client = None`` (class attribute) is the load-bearing bit: ``_read_index.py``'s
    ``_read_consolidated_if_fresh`` does ``getattr(client, "_client", None)`` and only
    takes the native-GCS staleness-check branch (``blob.reload()`` / ``.updated``) when
    that's non-None. With it None, the read path is just
    ``client.download_bytes(bucket, INDEX_PATH)`` — a plain dict lookup here.
    """

    _client: object = None

    def __init__(self) -> None:
        self._storage: dict[tuple[str, str], bytes] = {}

    def download_bytes(self, bucket: str, path: str) -> bytes:
        if (bucket, path) not in self._storage:
            raise FileNotFoundError(f"No blob at {bucket}/{path}")
        return self._storage[(bucket, path)]

    def upload_bytes(self, bucket: str, path: str, data: bytes, **_: object) -> None:
        self._storage[(bucket, path)] = data

    def read_index_df(self, bucket: str, path: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self._storage[(bucket, path)]))


@pytest.fixture(autouse=True)
def _reset_manifest_module_caches() -> None:
    """Clear UTL's process-level index caches so each test reads its own write.

    Mirrors the ``_reset_module_state`` fixture in UTL's own
    ``test_manifest_writer_capture_status.py`` — ``_INDEX_CACHE`` /
    ``_CANONICAL_CACHE`` are keyed by bucket name only, so a stale entry from a
    prior test (or a prior run in this same test) would mask the write this
    test is trying to prove is readable.
    """
    _mw_module._INDEX_CACHE.clear()
    _mw_module._CANONICAL_CACHE.clear()


def _flush(writer: ManifestWriter, stub: _StubStorageClient) -> None:
    """Force-drain the writer's buffered record(s) to the stub's in-memory store."""
    with (
        patch.object(_mw_module, "_should_flush_to_gcs", return_value=True),
        patch(
            "unified_trading_library.cloud_interface.get_storage_client",
            return_value=stub,
        ),
    ):
        writer.flush()


@pytest.mark.parametrize(
    ("asset_group_label", "venue", "data_type", "pipeline_mode", "error_reason"),
    [
        (
            "tradfi",
            "CME",
            "FUTURE",
            PipelineMode.BATCH_DATABENTO,
            "Databento fetch failed for dataset=GLBX.MDP3 (error_code=RATE_LIMIT, retry_safe=True): 429",
        ),
        (
            "prediction",
            "POLYMARKET",
            "MARKETS",
            PipelineMode.BATCH_POLYMARKET_CLOB,
            "Polymarket clob/markets fetch failed (error_code=RATE_LIMIT, retry_safe=True): 429",
        ),
    ],
)
def test_record_failed_read_roundtrip_confirms_attempted_failed(
    asset_group_label: str,
    venue: str,
    data_type: str,
    pipeline_mode: PipelineMode,
    error_reason: str,
) -> None:
    """record_failed() write -> a FRESH ManifestWriter.lookup() sees ATTEMPTED_FAILED.

    This is the missing read-path proof: not "was record_failed() called with the
    right args" (the mocked-stub style everywhere else in this repo) but "does the
    manifest actually come back attempted_failed after a real write + flush", for
    both a tradfi (Databento) and a prediction (Polymarket CLOB) shard.
    """
    stub = _StubStorageClient()
    bucket = f"instruments-store-{asset_group_label}-test"
    row_key = {"date": "2026-01-15", "venue": venue, "data_type": data_type}

    writer = ManifestWriter(service_name=_SERVICE_NAME, catalogue_bucket=bucket)
    writer.record_failed(
        row_key=row_key,
        error=error_reason,
        pipeline_mode=pipeline_mode,
    )
    _flush(writer, stub)

    # Simulate a genuinely separate read (e.g. the next backfill invocation's
    # pre-flight _should_skip_shard lookup) — a fresh ManifestWriter instance,
    # same service_name + bucket, no in-process handle to the writer above.
    _mw_module._INDEX_CACHE.clear()
    _mw_module._CANONICAL_CACHE.clear()
    reader = ManifestWriter(service_name=_SERVICE_NAME, catalogue_bucket=bucket)
    with patch(
        "unified_trading_library.cloud_interface.get_storage_client",
        return_value=stub,
    ):
        result = reader.lookup(row_key=row_key)

    assert result is not None, "lookup() must find the row this test just record_failed()+flushed"
    assert isinstance(result, ManifestRow)
    assert result.capture_status == CaptureStatus.ATTEMPTED_FAILED.value, (
        f"Expected capture_status=attempted_failed after record_failed(), got {result.capture_status!r}"
    )
    assert result.error_reason == error_reason
    assert result.date == "2026-01-15"
    assert result.venue == venue
    assert result.data_type == data_type
