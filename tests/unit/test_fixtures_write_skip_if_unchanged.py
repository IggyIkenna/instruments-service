"""Tests for the daily-repoll write-skip guard on per-league fixtures.

`_per_league_fixtures_data_unchanged` returns True only when the on-disk parquet holds
identical fixture DATA (ignoring the re-stamped `available_at`), so the daily re-poll can
skip churning an unchanged (date, league) cell. The guard is biased to write on any doubt.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine import orchestrator


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _mock_storage(existing_df: pd.DataFrame | None) -> MagicMock:
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = existing_df is not None
    client.bucket.return_value.blob.return_value = blob
    if existing_df is not None:
        client.download_bytes.return_value = _parquet_bytes(existing_df)
    return client


def _df(available_at: datetime, value: str = "Arsenal") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"af_fixture_id": 1, "home_team": value, "away_team": "Chelsea", "available_at": available_at},
            {"af_fixture_id": 2, "home_team": "Spurs", "away_team": "Wolves", "available_at": available_at},
        ]
    )


def test_unchanged_data_different_available_at_returns_true() -> None:
    """Same fixtures, different available_at → unchanged (available_at is excluded)."""
    existing = _df(datetime(2026, 6, 1, tzinfo=UTC))
    new = _df(datetime(2026, 6, 2, 12, 30, tzinfo=UTC))  # re-stamped now()
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        assert orchestrator._per_league_fixtures_data_unchanged("b", "2026-06-10", "fixtures", "PL", new) is True


def test_changed_data_returns_false() -> None:
    """A changed fixture value → must write (False)."""
    existing = _df(datetime(2026, 6, 1, tzinfo=UTC), value="Arsenal")
    new = _df(datetime(2026, 6, 2, tzinfo=UTC), value="Liverpool")  # home_team changed
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        assert orchestrator._per_league_fixtures_data_unchanged("b", "2026-06-10", "fixtures", "PL", new) is False


def test_missing_object_returns_false() -> None:
    """No existing parquet → must write (False)."""
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(None)):
        assert (
            orchestrator._per_league_fixtures_data_unchanged(
                "b", "2026-06-10", "fixtures", "PL", _df(datetime(2026, 6, 2, tzinfo=UTC))
            )
            is False
        )


def test_extra_row_returns_false() -> None:
    """Different row count → must write (False)."""
    existing = _df(datetime(2026, 6, 1, tzinfo=UTC))
    new = pd.concat(
        [
            _df(datetime(2026, 6, 2, tzinfo=UTC)),
            pd.DataFrame(
                [
                    {
                        "af_fixture_id": 3,
                        "home_team": "City",
                        "away_team": "Everton",
                        "available_at": datetime(2026, 6, 2, tzinfo=UTC),
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        assert orchestrator._per_league_fixtures_data_unchanged("b", "2026-06-10", "fixtures", "PL", new) is False


def test_writer_skips_gated_write_when_unchanged() -> None:
    """_write_fixtures_per_league skips the gated write when the guard reports unchanged."""
    fixture_df = pd.DataFrame([{"af_fixture_id": 1, "league_id": "PL", "home_team": "Arsenal", "away_team": "Chelsea"}])
    with (
        patch.object(orchestrator, "_per_league_fixtures_data_unchanged", return_value=True),
        patch.object(orchestrator, "_gated_sink_write") as mock_write,
    ):
        orchestrator._write_fixtures_per_league(
            MagicMock(), fixture_df, "2026-06-10", source_label="test", bucket="b", skip_if_unchanged=True
        )
    mock_write.assert_not_called()


def test_writer_writes_when_changed() -> None:
    """_write_fixtures_per_league writes when the guard reports changed."""
    fixture_df = pd.DataFrame([{"af_fixture_id": 1, "league_id": "PL", "home_team": "Arsenal", "away_team": "Chelsea"}])
    with (
        patch.object(orchestrator, "_per_league_fixtures_data_unchanged", return_value=False),
        patch.object(orchestrator, "_gated_sink_write") as mock_write,
    ):
        orchestrator._write_fixtures_per_league(
            MagicMock(), fixture_df, "2026-06-10", source_label="test", bucket="b", skip_if_unchanged=True
        )
    mock_write.assert_called_once()


def test_writer_writes_when_skip_disabled() -> None:
    """Default (skip_if_unchanged=False) never calls the guard — always writes."""
    fixture_df = pd.DataFrame([{"af_fixture_id": 1, "league_id": "PL", "home_team": "Arsenal", "away_team": "Chelsea"}])
    with (
        patch.object(orchestrator, "_per_league_fixtures_data_unchanged") as mock_guard,
        patch.object(orchestrator, "_gated_sink_write") as mock_write,
    ):
        orchestrator._write_fixtures_per_league(MagicMock(), fixture_df, "2026-06-10", source_label="test")
    mock_guard.assert_not_called()
    mock_write.assert_called_once()
