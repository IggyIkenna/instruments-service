"""Unit tests for ``_find_stale_fixture_leagues_for_date`` (sports_fixtures.py).

Root-cause coverage for the api_football stale-NS finding: a captured
FIXTURES row frozen at a non-terminal ``status_short`` (typically ``NS``)
must be detectable per (date, league) cell from a SINGLE date's already-
captured parquet — no whole-corpus walk — so the periodic status-refresh
trigger (``sports_fixture_status_refresh.py``) knows which cells to
re-fetch.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator.sports_fixtures import (
    TERMINAL_FIXTURE_STATUSES,
    _find_stale_fixture_leagues_for_date,
    _read_fixtures_entity_with_schedule_fallback,
)


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


def _mock_blob(league: str) -> MagicMock:
    blob = MagicMock()
    blob.name = (
        f"sports_reference/by_date/day=2026-06-24/pipeline_mode=batch_api_football/"
        f"entity=fixtures/league={league}/fixtures.parquet"
    )
    return blob


def test_terminal_statuses_set() -> None:
    """The terminal set matches the issue doc's stated set exactly."""
    assert {"FT", "AET", "PEN", "CANC", "AWD", "PST", "ABD"} == TERMINAL_FIXTURE_STATUSES


def test_all_terminal_returns_empty_set() -> None:
    """A date whose captured fixtures are all terminal has no stale leagues."""
    df = pd.DataFrame({"af_fixture_id": [1, 2], "status_short": ["FT", "AET"]})
    mock_blob = _mock_blob("EPL")
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [mock_blob]
    mock_storage.download_bytes.return_value = _parquet_bytes(df)

    with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
        result = _find_stale_fixture_leagues_for_date("bucket", "2026-06-24")

    assert result == set()


def test_non_terminal_status_flags_league_stale() -> None:
    """A league with any non-terminal (e.g. NS) row is returned as stale."""
    df = pd.DataFrame({"af_fixture_id": [1, 2, 3], "status_short": ["NS", "NS", "FT"]})
    mock_blob = _mock_blob("EPL")
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [mock_blob]
    mock_storage.download_bytes.return_value = _parquet_bytes(df)

    with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
        result = _find_stale_fixture_leagues_for_date("bucket", "2026-06-24")

    assert result == {"EPL"}


def test_mixed_leagues_only_stale_one_returned() -> None:
    """Two leagues on the same date: only the one with a non-terminal row is flagged."""
    epl_df = pd.DataFrame({"af_fixture_id": [1], "status_short": ["NS"]})
    laliga_df = pd.DataFrame({"af_fixture_id": [2], "status_short": ["FT"]})
    epl_blob = _mock_blob("EPL")
    laliga_blob = _mock_blob("LALIGA")
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [epl_blob, laliga_blob]

    def _download(*, bucket: str, blob_path: str) -> bytes:
        if "league=EPL" in blob_path:
            return _parquet_bytes(epl_df)
        return _parquet_bytes(laliga_df)

    mock_storage.download_bytes.side_effect = _download

    with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
        result = _find_stale_fixture_leagues_for_date("bucket", "2026-06-24")

    assert result == {"EPL"}


def test_no_captured_fixtures_returns_empty_set() -> None:
    """A date with zero captured FIXTURES parquets returns an empty set (not a capture gap concern here)."""
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = []

    with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
        result = _find_stale_fixture_leagues_for_date("bucket", "2026-06-24")

    assert result == set()


def test_missing_status_short_column_returns_empty_set() -> None:
    """Malformed/legacy parquet without status_short is treated as nothing-to-refresh, not a crash."""
    df = pd.DataFrame({"af_fixture_id": [1, 2]})
    mock_blob = _mock_blob("EPL")
    mock_storage = MagicMock()
    mock_storage.list_blobs.return_value = [mock_blob]
    mock_storage.download_bytes.return_value = _parquet_bytes(df)

    with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
        result = _find_stale_fixture_leagues_for_date("bucket", "2026-06-24")

    assert result == set()


def test_schedule_fallback_prefers_fixtures_schedule() -> None:
    """Regression pin for api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19:
    the 254fb843 entity-split (2026-06-24) moved status data to fixtures_schedule and
    _write_fixtures_per_league never writes the legacy fixtures entity again — the
    fallback helper must check fixtures_schedule FIRST and use it when present."""
    schedule_df = pd.DataFrame({"af_fixture_id": [1], "status_short": ["FT"]})

    def _read(bucket: str, date: str, entity_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert entity_name == "fixtures_schedule", "must try fixtures_schedule first"
        return schedule_df

    with patch("instruments_service.engine.orchestrator._read_per_league_entity_df", _read):
        result = _read_fixtures_entity_with_schedule_fallback("bucket", "2026-07-01")

    assert result is schedule_df


def test_schedule_fallback_falls_back_to_legacy_fixtures_when_schedule_absent() -> None:
    """A date never re-touched since the 2026-06-24 entity-split has no fixtures_schedule
    data at all — fall back to the legacy fixtures entity rather than returning None."""
    legacy_df = pd.DataFrame({"af_fixture_id": [1], "status_short": ["FT"]})
    calls: list[str] = []

    def _read(bucket: str, date: str, entity_name: str, **kwargs: object) -> pd.DataFrame | None:
        calls.append(entity_name)
        if entity_name == "fixtures_schedule":
            return None
        return legacy_df

    with patch("instruments_service.engine.orchestrator._read_per_league_entity_df", _read):
        result = _read_fixtures_entity_with_schedule_fallback("bucket", "2020-06-10")

    assert calls == ["fixtures_schedule", "fixtures"]
    assert result is legacy_df


def test_schedule_fallback_returns_none_when_neither_entity_exists() -> None:
    with patch(
        "instruments_service.engine.orchestrator._read_per_league_entity_df",
        return_value=None,
    ):
        result = _read_fixtures_entity_with_schedule_fallback("bucket", "2099-01-01")

    assert result is None
