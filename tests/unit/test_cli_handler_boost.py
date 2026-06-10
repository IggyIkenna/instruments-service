"""Coverage boost for instruments_service/cli/instruments_handler.py.

Targets the specific uncovered branches identified in the 85% → 90% coverage push:
- Module-level bucket helpers (_get_instruments_bucket_for_asset_group, _get_instruments_bucket)
- preflight(): --source massive adds MASSIVE pseudo-venue
- _wire_cli_filters_from_args(): venues+earliest_date, recovery_fixture_ids, source arg
- _load_recovery_fixture_ids(): GCS path, local path, exception, missing column
- process(): date normalisation when "T" present
- cleanup(): ManifestWriter flush branch + sports coordination event branch
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_runtime(mode: str = "batch") -> MagicMock:
    rt = MagicMock()
    rt.gcp_project_id = "test-project"
    rt.mode = mode
    rt.start_date = "2026-03-22"
    rt.end_date = "2026-03-22"
    rt.asset_group = ["SPORTS"]
    return rt


def _make_handler(mode: str = "batch") -> Any:
    from instruments_service.cli.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime(mode))
    handler.args = None
    return handler


# ---------------------------------------------------------------------------
# Module-level bucket helpers
# ---------------------------------------------------------------------------


def test_get_instruments_bucket_for_asset_group_delegates_to_resolve() -> None:
    """Lines 49-51: _get_instruments_bucket_for_asset_group resolves via public API."""
    from instruments_service.cli.instruments_handler import _get_instruments_bucket_for_asset_group

    with (
        patch(
            "instruments_service.cli.instruments_handler.resolve_instruments_store_kind",
            return_value=("instruments-store", "sports"),
        ) as mock_resolve_kind,
        patch(
            "instruments_service.cli.instruments_handler.resolve_bucket_name",
            return_value="test-sports-bucket",
        ) as mock_resolve_bucket,
    ):
        result = _get_instruments_bucket_for_asset_group("SPORTS")

    assert result == "test-sports-bucket"
    mock_resolve_kind.assert_called_once_with("sports")
    mock_resolve_bucket.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="sports")


def test_get_instruments_bucket_alias_calls_through() -> None:
    """Line 56: _get_instruments_bucket alias delegates to _get_instruments_bucket_for_asset_group."""
    from instruments_service.cli.instruments_handler import _get_instruments_bucket

    with (
        patch(
            "instruments_service.cli.instruments_handler.resolve_instruments_store_kind",
            return_value=("instruments-store", "sports"),
        ),
        patch(
            "instruments_service.cli.instruments_handler.resolve_bucket_name",
            return_value="alias-sports-bucket",
        ),
    ):
        result = _get_instruments_bucket("SPORTS")

    assert result == "alias-sports-bucket"


# ---------------------------------------------------------------------------
# preflight(): --source massive
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preflight_source_massive_adds_venue() -> None:
    """Line 123: when _source == 'massive', MASSIVE is appended to active_venues."""
    handler = _make_handler()
    handler._source = "massive"

    captured_venues: list[str] = []
    mock_reloader = MagicMock()
    mock_reloader.current_keys = {}

    def _capture_start_reloader(venues: list[str]) -> None:
        captured_venues.extend(venues)
        handler._key_reloader = mock_reloader
        mock_reloader.start()

    with (
        patch(
            "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
            return_value=["BINANCE"],
        ),
        patch(
            "instruments_service.cli.instruments_handler.clear_defi_universe_cache",
        ),
        patch.object(handler, "_start_key_reloader", side_effect=_capture_start_reloader),
    ):
        await handler.preflight()

    assert "MASSIVE" in captured_venues


# ---------------------------------------------------------------------------
# _wire_cli_filters_from_args()
# ---------------------------------------------------------------------------


def test_wire_cli_filters_venues_with_earliest_date() -> None:
    """Line 133: when earliest_venue_date returns a date, it is logged."""
    handler = _make_handler()
    args = MagicMock()
    args.venues = ["NYSE"]
    args.sports_entity = None
    args.sports_provider = None
    args.league = None
    args.season = None
    args.recovery_fixture_ids = None
    args.source = None
    args.trigger = None
    args.lookback_days = None
    args.lookahead_days = None
    args.force_window = False
    handler.args = args

    with patch(
        "instruments_service.cli.instruments_handler.earliest_venue_date",
        return_value="2020-01-01",
    ) as mock_earliest:
        handler._wire_cli_filters_from_args()

    mock_earliest.assert_called_once_with(["NYSE"])
    assert handler._venue_override == ["NYSE"]


def test_wire_cli_filters_recovery_fixture_ids() -> None:
    """Line 160: when recovery_fixture_ids arg is set, _load_recovery_fixture_ids is called."""
    handler = _make_handler()
    args = MagicMock()
    args.venues = None
    args.sports_entity = None
    args.sports_provider = None
    args.league = None
    args.season = None
    args.recovery_fixture_ids = "/tmp/recovery.parquet"
    args.source = None
    args.trigger = None
    args.lookback_days = None
    args.lookahead_days = None
    args.force_window = False
    handler.args = args

    expected_ids = frozenset([1, 2, 3])
    with patch.object(handler, "_load_recovery_fixture_ids", return_value=expected_ids) as mock_load:
        handler._wire_cli_filters_from_args()

    mock_load.assert_called_once_with("/tmp/recovery.parquet")
    assert handler._recovery_fixture_ids == expected_ids


def test_wire_cli_filters_source_arg() -> None:
    """Lines 164-165: when source arg is set, _source is normalised to lowercase."""
    handler = _make_handler()
    args = MagicMock()
    args.venues = None
    args.sports_entity = None
    args.sports_provider = None
    args.league = None
    args.season = None
    args.recovery_fixture_ids = None
    args.source = "MASSIVE"
    args.trigger = None
    args.lookback_days = None
    args.lookahead_days = None
    args.force_window = False
    handler.args = args

    handler._wire_cli_filters_from_args()

    assert handler._source == "massive"


# ---------------------------------------------------------------------------
# _load_recovery_fixture_ids()
# ---------------------------------------------------------------------------


def test_load_recovery_fixture_ids_gcs_path() -> None:
    """Lines 199-210: gs:// path downloads via get_storage_client and parses parquet."""
    handler = _make_handler()

    df = pd.DataFrame({"af_fixture_id": [101, 202, 303]})
    raw_bytes = df.to_parquet(index=False)

    mock_storage = MagicMock()
    mock_storage.download_bytes.return_value = raw_bytes

    with patch(
        "instruments_service.cli.instruments_handler.get_storage_client",
        return_value=mock_storage,
    ):
        result = handler._load_recovery_fixture_ids("gs://test-bucket/path/recovery.parquet")

    assert result == frozenset([101, 202, 303])
    mock_storage.download_bytes.assert_called_once_with(bucket="test-bucket", blob_path="path/recovery.parquet")


def test_load_recovery_fixture_ids_local_path(tmp_path) -> None:
    """Lines 211-212: local path reads directly via pd.read_parquet."""
    handler = _make_handler()

    parquet_path = tmp_path / "recovery.parquet"
    df = pd.DataFrame({"af_fixture_id": [500, 600]})
    df.to_parquet(parquet_path, index=False)

    result = handler._load_recovery_fixture_ids(str(parquet_path))

    assert result == frozenset([500, 600])


def test_load_recovery_fixture_ids_exception_returns_none() -> None:
    """Lines 213-215: any exception returns None (shard isolation — no raise)."""
    handler = _make_handler()

    with patch(
        "instruments_service.cli.instruments_handler.get_storage_client",
        side_effect=RuntimeError("storage error"),
    ):
        result = handler._load_recovery_fixture_ids("gs://bucket/path.parquet")

    assert result is None


def test_load_recovery_fixture_ids_missing_column_returns_none(tmp_path) -> None:
    """Lines 216-222: parquet without af_fixture_id column returns None with an error log."""
    handler = _make_handler()

    parquet_path = tmp_path / "no_column.parquet"
    df = pd.DataFrame({"some_other_col": [1, 2]})
    df.to_parquet(parquet_path, index=False)

    result = handler._load_recovery_fixture_ids(str(parquet_path))

    assert result is None


# ---------------------------------------------------------------------------
# process(): date normalisation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_date_with_t_normalised() -> None:
    """Line 266: datetime strings containing 'T' are truncated to YYYY-MM-DD."""
    from unified_trading_library import BatchPayload

    handler = _make_handler()
    handler._key_reloader = MagicMock()
    handler._key_reloader.current_keys = {}

    captured_dates: list[str] = []

    async def _fake_process_instruments(**kwargs: object) -> dict[str, int]:
        captured_dates.append(str(kwargs.get("date", "")))
        return {}

    payload = BatchPayload(date="2026-03-22T15:00:00", asset_groups=["SPORTS"])

    with patch("instruments_service.cli.instruments_handler.engine_orchestrator") as mock_orch:
        mock_orch.process_instruments = _fake_process_instruments
        await handler.process(payload)

    assert captured_dates == ["2026-03-22"]


# ---------------------------------------------------------------------------
# cleanup()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_with_flushed_buckets() -> None:
    """Lines 298-305: when ManifestWriter.flush() succeeds, flushed list is logged."""
    handler = _make_handler()

    mock_writer = MagicMock()
    mock_writer.flush.return_value = None

    with (
        patch(
            "instruments_service.cli.instruments_handler._get_instruments_bucket_for_asset_group",
            return_value="test-bucket",
        ),
        patch(
            "instruments_service.cli.instruments_handler.ManifestWriter",
            return_value=mock_writer,
        ),
        patch("instruments_service.cli.instruments_handler.publish_coordination_event"),
    ):
        await handler.cleanup()

    # flush was called once per asset group (4 groups)
    assert mock_writer.flush.call_count == 4


@pytest.mark.asyncio
async def test_cleanup_sports_asset_group_emits_event() -> None:
    """Lines 318-320: SPORTS in asset_group triggers SPORTS_LIVE_STATS coordination event."""
    handler = _make_handler()

    args = MagicMock()
    args.asset_group = ["SPORTS"]
    args.category = None
    handler.args = args

    published: list[str] = []

    def _capture_event(name: str, **kwargs: object) -> None:
        published.append(name)

    with (
        patch(
            "instruments_service.cli.instruments_handler._get_instruments_bucket_for_asset_group",
            side_effect=Exception("bucket lookup skipped"),
        ),
        patch(
            "instruments_service.cli.instruments_handler.publish_coordination_event",
            side_effect=_capture_event,
        ),
    ):
        await handler.cleanup()

    assert "SPORTS_LIVE_STATS" in published
