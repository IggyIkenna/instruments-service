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
