"""Unit tests for Transfermarkt master/ + snapshots/ GCS write-path helpers.

Covers ITEM 6a (master + snapshot write-path) and ITEM 6b (append-only merge
with dedup on natural key).

All tests are credential-free: storage client is mocked; no GCS calls hit the
network.  Set CLOUD_PROVIDER=local CLOUD_MOCK_MODE=true for CI parity.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator import (
    _master_blob_path,
    _snapshot_blob_path_player_values,
    _write_master_append,
    _write_snapshot_player_values,
)

# ---------------------------------------------------------------------------
# Path-shape tests
# ---------------------------------------------------------------------------


class TestMasterBlobPath:
    def test_teams_path(self) -> None:
        assert _master_blob_path("teams") == "sports_reference/master/entity=teams/master.parquet"

    def test_team_mapping_path(self) -> None:
        assert _master_blob_path("team_mapping") == "sports_reference/master/entity=team_mapping/master.parquet"

    def test_player_values_path(self) -> None:
        assert _master_blob_path("player_values") == "sports_reference/master/entity=player_values/master.parquet"


class TestSnapshotBlobPath:
    def test_snapshot_path_shape(self) -> None:
        assert _snapshot_blob_path_player_values(2024, "2024-09-01") == (
            "sports_reference/snapshots/entity=player_values/season=2024/trigger=2024-09-01/player_values.parquet"
        )

    def test_snapshot_path_different_season(self) -> None:
        path = _snapshot_blob_path_player_values(2022, "2022-08-05")
        assert "season=2022" in path
        assert "trigger=2022-08-05" in path
        assert path.endswith("/player_values.parquet")


# ---------------------------------------------------------------------------
# _write_master_append — append-only merge + dedup (ITEM 6b)
# ---------------------------------------------------------------------------


def _make_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class TestWriteMasterAppend:
    def test_creates_fresh_master_when_none_exists(self) -> None:
        """No existing parquet → new_df is written as-is (plus last_updated_at)."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None  # blob absent

        new_df = pd.DataFrame(
            [
                {"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024, "name": "Arsenal"},
            ]
        )

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("test-bucket", "player_values", new_df, ["canonical_league", "team_id", "season"])

        mock_storage.upload_bytes.assert_called_once()
        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        assert _bucket == "test-bucket"
        assert _path == "sports_reference/master/entity=player_values/master.parquet"
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        assert len(result_df) == 1
        assert "last_updated_at" in result_df.columns

    def test_appends_new_rows_to_existing(self) -> None:
        """Existing 1 row + new 1 different row → merged 2 rows."""
        existing_df = pd.DataFrame([{"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024}])
        new_df = pd.DataFrame([{"canonical_league": "LA_LIGA", "team_id": "BARCA", "season": 2024}])

        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = _make_parquet_bytes(existing_df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "player_values", new_df, ["canonical_league", "team_id", "season"])

        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        assert len(result_df) == 2
        assert set(result_df["team_id"]) == {"ARSENAL", "BARCA"}

    def test_dedup_keeps_last_row_on_key_collision(self) -> None:
        """Duplicate natural key: new row wins (keep='last' after concat)."""
        existing_df = pd.DataFrame(
            [{"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024, "squad_size": "25"}]
        )
        # Same key — new row has updated squad_size
        new_df = pd.DataFrame([{"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024, "squad_size": "26"}])

        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = _make_parquet_bytes(existing_df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "team_mapping", new_df, ["canonical_league", "team_id"])

        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        # Only 1 row after dedup
        assert len(result_df) == 1
        # New row wins (squad_size=26)
        assert str(result_df["squad_size"].iloc[0]) == "26"

    def test_empty_new_df_skips_write(self) -> None:
        """Empty new_df → upload_bytes never called."""
        mock_storage = MagicMock()
        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "teams", pd.DataFrame(), ["team_id", "season"])
        mock_storage.upload_bytes.assert_not_called()

    def test_write_failure_is_non_blocking(self) -> None:
        """Storage upload error must NOT raise — classified + emitted, not propagated."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None
        mock_storage.upload_bytes.side_effect = RuntimeError("GCS down")

        new_df = pd.DataFrame([{"team_id": "ARSENAL", "season": 2024}])

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error") as mock_classify,
        ):
            # Must not raise
            _write_master_append("bucket", "teams", new_df, ["team_id", "season"])

        mock_classify.assert_called_once()

    def test_missing_dedup_key_columns_skips_dedup_gracefully(self) -> None:
        """If dedup_key columns are absent from the DataFrame, still writes."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None

        new_df = pd.DataFrame([{"name": "Arsenal"}])  # no dedup key columns

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "teams", new_df, ["team_id", "season"])

        mock_storage.upload_bytes.assert_called_once()
        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        assert len(result_df) == 1

    def test_team_mapping_natural_key_dedup(self) -> None:
        """team_mapping dedup key is (canonical_league, team_id) — season not in key."""
        existing_df = pd.DataFrame(
            [
                {"canonical_league": "EPL", "team_id": "ARSENAL", "squad_size": "25"},
                {"canonical_league": "EPL", "team_id": "CHELSEA", "squad_size": "24"},
            ]
        )
        new_df = pd.DataFrame(
            [
                # Update ARSENAL, add LIVERPOOL
                {"canonical_league": "EPL", "team_id": "ARSENAL", "squad_size": "27"},
                {"canonical_league": "EPL", "team_id": "LIVERPOOL", "squad_size": "22"},
            ]
        )

        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = _make_parquet_bytes(existing_df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "team_mapping", new_df, ["canonical_league", "team_id"])

        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        # ARSENAL(updated) + CHELSEA + LIVERPOOL = 3 rows
        assert len(result_df) == 3
        arsenal_row = result_df[result_df["team_id"] == "ARSENAL"]
        assert str(arsenal_row["squad_size"].iloc[0]) == "27"

    def test_by_date_write_still_unaffected(self) -> None:
        """Master append does NOT modify the by_date/ sink — two separate writes."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None
        new_df = pd.DataFrame([{"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024}])

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_master_append("bucket", "player_values", new_df, ["canonical_league", "team_id", "season"])

        # The upload went to master/, not by_date/
        _bucket, _path, _bytes = mock_storage.upload_bytes.call_args.args
        assert "master" in _path
        assert "by_date" not in _path


# ---------------------------------------------------------------------------
# _write_snapshot_player_values
# ---------------------------------------------------------------------------


class TestWriteSnapshotPlayerValues:
    def test_snapshot_written_to_correct_path(self) -> None:
        mock_storage = MagicMock()
        df = pd.DataFrame(
            [
                {"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024},
                {"canonical_league": "EPL", "team_id": "CHELSEA", "season": 2024},
            ]
        )

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_snapshot_player_values("test-bucket", season=2024, trigger_date="2024-09-01", df=df)

        mock_storage.upload_bytes.assert_called_once()
        _bucket, _path, written_bytes = mock_storage.upload_bytes.call_args.args
        assert _bucket == "test-bucket"
        assert _path == (
            "sports_reference/snapshots/entity=player_values/season=2024/trigger=2024-09-01/player_values.parquet"
        )
        result_df = pd.read_parquet(io.BytesIO(written_bytes))
        assert len(result_df) == 2
        assert "snapshot_season" in result_df.columns
        assert "snapshot_trigger_date" in result_df.columns
        assert "snapshot_written_at" in result_df.columns
        assert result_df["snapshot_season"].iloc[0] == 2024
        assert result_df["snapshot_trigger_date"].iloc[0] == "2024-09-01"

    def test_empty_df_skips_write(self) -> None:
        mock_storage = MagicMock()
        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_snapshot_player_values("bucket", season=2024, trigger_date="2024-09-01", df=pd.DataFrame())
        mock_storage.upload_bytes.assert_not_called()

    def test_snapshot_failure_is_non_blocking(self) -> None:
        mock_storage = MagicMock()
        mock_storage.upload_bytes.side_effect = RuntimeError("GCS down")
        df = pd.DataFrame([{"team_id": "ARSENAL", "season": 2024}])

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error") as mock_classify,
        ):
            _write_snapshot_player_values("bucket", season=2024, trigger_date="2024-09-01", df=df)

        mock_classify.assert_called_once()

    def test_snapshot_does_not_mutate_source_df(self) -> None:
        """Snapshot adds metadata columns to a copy — source df must be unmodified."""
        mock_storage = MagicMock()
        df = pd.DataFrame([{"canonical_league": "EPL", "team_id": "ARSENAL", "season": 2024}])
        original_columns = set(df.columns)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            _write_snapshot_player_values("bucket", season=2024, trigger_date="2024-09-01", df=df)

        # Source df must still have only original columns
        assert set(df.columns) == original_columns

    def test_snapshot_path_encodes_trigger_date(self) -> None:
        """Different trigger dates produce distinct snapshot paths."""
        path_1 = _snapshot_blob_path_player_values(2024, "2024-09-01")
        path_2 = _snapshot_blob_path_player_values(2024, "2024-01-31")
        assert path_1 != path_2
        assert "trigger=2024-09-01" in path_1
        assert "trigger=2024-01-31" in path_2
