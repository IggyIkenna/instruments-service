"""Guard tests: a rescheduled fixture must be stored with the NEW kickoff_utc.

Reschedule correctness rule (plan: sports_fixture_completeness_oracle_2026_06_24.md §5):
  When a fixture is postponed/rescheduled the catalogue must record the FINAL
  scheduled kickoff time, not the original one.  Two mechanisms must handle this:

  1. ``_per_league_fixtures_data_unchanged`` must return False when kickoff_utc
     changes so the write path is never skipped on a reschedule.
  2. ``_merge_with_existing_per_league_parquet`` (recovery-mode merge) must drop
     the stale row and replace it with the updated kickoff_utc.

These tests assert both properties without touching the network or GCS.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine import orchestrator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _mock_storage(existing_df: pd.DataFrame | None) -> MagicMock:
    """Return a mock storage client whose blob holds ``existing_df``."""
    client = MagicMock()
    blob = MagicMock()
    blob.exists.return_value = existing_df is not None
    client.bucket.return_value.blob.return_value = blob
    if existing_df is not None:
        client.download_bytes.return_value = _parquet_bytes(existing_df)
    return client


T_ORIGINAL = datetime(2026, 3, 15, 15, 0, tzinfo=UTC)  # kickoff before reschedule
T_NEW = datetime(2026, 3, 22, 17, 0, tzinfo=UTC)  # new kickoff after postponement
T_AVAIL = datetime(2026, 3, 10, 9, 0, tzinfo=UTC)  # available_at — independent


def _fixture_df(kickoff_utc: datetime, fixture_id: int = 999) -> pd.DataFrame:
    """Minimal fixture row with a kickoff_utc column."""
    return pd.DataFrame(
        [
            {
                "af_fixture_id": fixture_id,
                "league_id": "PL",
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "kickoff_utc": kickoff_utc,
                "available_at": T_AVAIL,
            }
        ]
    )


# ---------------------------------------------------------------------------
# 1. skip-if-unchanged guard detects a kickoff_utc change → must write
# ---------------------------------------------------------------------------


def test_unchanged_guard_returns_false_on_kickoff_change() -> None:
    """A kickoff_utc change must NOT be skipped — guard must return False."""
    existing = _fixture_df(kickoff_utc=T_ORIGINAL)
    rescheduled = _fixture_df(kickoff_utc=T_NEW)
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        result = orchestrator._per_league_fixtures_data_unchanged("bucket", "2026-03-15", "fixtures", "PL", rescheduled)
    assert result is False, (
        "_per_league_fixtures_data_unchanged must return False when kickoff_utc changes "
        "so the rescheduled time is written to GCS, not skipped."
    )


def test_unchanged_guard_returns_true_when_kickoff_same() -> None:
    """Re-fetching the same kickoff_utc (no reschedule) → guard correctly skips."""
    existing = _fixture_df(kickoff_utc=T_ORIGINAL)
    # Same data, only available_at differs (re-stamped on re-fetch)
    same_data = _fixture_df(kickoff_utc=T_ORIGINAL)
    same_data["available_at"] = datetime(2026, 3, 16, 10, 0, tzinfo=UTC)
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        result = orchestrator._per_league_fixtures_data_unchanged("bucket", "2026-03-15", "fixtures", "PL", same_data)
    assert result is True, (
        "_per_league_fixtures_data_unchanged must return True when only available_at "
        "differs (no actual data change) so we don't churn GCS unnecessarily."
    )


# ---------------------------------------------------------------------------
# 2. Recovery-mode merge stores the NEW kickoff_utc (not the original)
# ---------------------------------------------------------------------------


def test_recovery_merge_replaces_stale_kickoff() -> None:
    """Recovery merge must drop the old row and keep the new kickoff_utc."""
    existing = _fixture_df(kickoff_utc=T_ORIGINAL, fixture_id=999)
    rescheduled = _fixture_df(kickoff_utc=T_NEW, fixture_id=999)

    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        merged = orchestrator._merge_with_existing_per_league_parquet(
            "bucket", "2026-03-15", "fixtures", "PL", rescheduled, fid_col="af_fixture_id"
        )

    assert len(merged) == 1, "Merged frame must have exactly one row (old dropped, new kept)."
    stored_kickoff = pd.to_datetime(merged.iloc[0]["kickoff_utc"], utc=True)
    assert stored_kickoff == T_NEW, (
        f"Stored kickoff_utc must be the NEW time ({T_NEW}), got {stored_kickoff}. "
        "A rescheduled fixture must be keyed to its updated kickoff time."
    )


def test_recovery_merge_preserves_other_fixtures() -> None:
    """Recovery merge keeps unaffected fixtures alongside the updated one."""
    other_fixture = pd.DataFrame(
        [
            {
                "af_fixture_id": 888,
                "league_id": "PL",
                "home_team": "Spurs",
                "away_team": "Wolves",
                "kickoff_utc": T_ORIGINAL,
                "available_at": T_AVAIL,
            }
        ]
    )
    existing = pd.concat([_fixture_df(kickoff_utc=T_ORIGINAL, fixture_id=999), other_fixture], ignore_index=True)
    rescheduled = _fixture_df(kickoff_utc=T_NEW, fixture_id=999)

    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(existing)):
        merged = orchestrator._merge_with_existing_per_league_parquet(
            "bucket", "2026-03-15", "fixtures", "PL", rescheduled, fid_col="af_fixture_id"
        )

    assert len(merged) == 2, "Other fixture (id=888) must survive the merge."
    updated = merged[pd.to_numeric(merged["af_fixture_id"], errors="coerce") == 999]
    assert len(updated) == 1
    stored_kickoff = pd.to_datetime(updated.iloc[0]["kickoff_utc"], utc=True)
    assert stored_kickoff == T_NEW, (
        f"Rescheduled fixture (id=999) must have new kickoff_utc={T_NEW}, got {stored_kickoff}."
    )


def test_recovery_merge_no_existing_parquet_returns_new_rows() -> None:
    """No existing parquet → merge falls through to the new rows only."""
    rescheduled = _fixture_df(kickoff_utc=T_NEW, fixture_id=999)
    with patch.object(orchestrator, "get_storage_client", return_value=_mock_storage(None)):
        merged = orchestrator._merge_with_existing_per_league_parquet(
            "bucket", "2026-03-15", "fixtures", "PL", rescheduled, fid_col="af_fixture_id"
        )
    assert len(merged) == 1
    stored_kickoff = pd.to_datetime(merged.iloc[0]["kickoff_utc"], utc=True)
    assert stored_kickoff == T_NEW
