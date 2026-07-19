"""Unit tests for ``sports.fixtures.status_refresh`` trigger.

Root-cause fix coverage for the api_football stale-NS finding
(``api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``):
a captured FIXTURES row frozen at a non-terminal ``status_short`` (usually
``NS``) blocks per-fixture enrichment forever, because
``_read_fixture_ids_from_gcs`` only treats ``FT``/``AET``/``PEN`` as
"completed" and nothing re-fetches an already-captured date to pick up its
real final status. These tests cover:

1. Window construction (bounded trailing scan, never whole-corpus).
2. A stale (date, league) cell triggers a targeted re-fetch + overwrite.
3. An all-terminal date makes zero API calls (no wasted budget).
4. Fetch/scan/record errors are shard-isolated (loop keeps going).
5. Blank api_key / unresolvable bucket fail loud at entry.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE = "instruments_service.triggers.sports_fixture_status_refresh"


def _make_fixture(*, af_id: int, league_canonical: str, kickoff: datetime) -> Any:
    """Minimal CanonicalFixture — only fields the trigger's flatten path reads."""
    from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

    home = CanonicalTeam(team_id="LIVERPOOL", name="Liverpool FC", country="England")
    away = CanonicalTeam(team_id="MAN_UTD", name="Manchester United", country="England")
    league = CanonicalLeague(league_id=league_canonical, name="Premier League", country="England")
    return CanonicalFixture(
        fixture_id=str(af_id),
        source_fixture_id=str(af_id),
        home_team=home,
        away_team=away,
        league=league,
        kickoff_utc=kickoff,
        season="2025-26",
        source="api_football",
        status="FT",
    )


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    kickoff = datetime(2026, 6, 24, 15, 0, tzinfo=UTC)
    fixtures = [_make_fixture(af_id=1234567, league_canonical="EPL", kickoff=kickoff)]
    adapter.get_fixtures_with_raw = AsyncMock(return_value=[(fx, {}) for fx in fixtures])
    return adapter


@pytest.fixture
def patch_factory(mock_adapter: MagicMock) -> Any:
    with patch(f"{MODULE}.create_sports_reference_adapter", return_value=mock_adapter) as p:
        yield p


def _mock_league(af_id: int) -> MagicMock:
    ld = MagicMock()
    ld.api_football_id = af_id
    return ld


# ---------------------------------------------------------------------------
# Window / date helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_today_none_returns_utc_date() -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import _resolve_today

    result = _resolve_today(None)
    assert result.year >= 2026


@pytest.mark.unit
def test_resolve_today_string() -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import _resolve_today

    result = _resolve_today("2026-07-19")
    assert result == date(2026, 7, 19)


@pytest.mark.unit
def test_status_refresh_window_bounded_trailing() -> None:
    """Window = [today - min_age - lookback, today - min_age], never touching recent days."""
    from instruments_service.triggers.sports_fixture_status_refresh import _status_refresh_window

    window = _status_refresh_window(date(2026, 7, 19), min_age_days=2, lookback_days=5)
    assert len(window) == 5
    assert window[0] == date(2026, 7, 17)  # today - 2 (most recent eligible day, first in list)
    assert window[-1] == date(2026, 7, 13)  # today - 6 (oldest day in the bounded window)
    # Never includes today or today-1 (too recent — daily_repoll's job).
    assert date(2026, 7, 19) not in window
    assert date(2026, 7, 18) not in window


@pytest.mark.unit
def test_trigger_name_constant() -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import (
        SPORTS_FIXTURE_STATUS_REFRESH_TRIGGER,
    )

    assert SPORTS_FIXTURE_STATUS_REFRESH_TRIGGER == "sports.fixtures.status_refresh"


# ---------------------------------------------------------------------------
# Entry-point validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_blank_api_key_raises() -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    with pytest.raises(ValueError, match="api-football API key"):
        await run_sports_fixture_status_refresh(today=date(2026, 7, 19), api_key="", bucket="test-bucket")
    with pytest.raises(ValueError, match="api-football API key"):
        await run_sports_fixture_status_refresh(today=date(2026, 7, 19), api_key="   ", bucket="test-bucket")


@pytest.mark.asyncio
@pytest.mark.unit
async def test_no_bucket_raises() -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    with (
        patch(f"{MODULE}.resolve_bucket_name", return_value=""),
        pytest.raises(ValueError, match="bucket"),
    ):
        await run_sports_fixture_status_refresh(today=date(2026, 7, 19), api_key="test-key", bucket=None)


# ---------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_all_terminal_dates_make_zero_api_calls(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """No stale leagues anywhere in the window → adapter never called (no wasted API budget)."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value=set()),
        patch(f"{MODULE}.ManifestWriter", return_value=MagicMock()),
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 19),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=5,
        )

    assert result == {}
    mock_adapter.get_fixtures_with_raw.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_stale_league_triggers_targeted_refetch(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """A stale (date, league) cell → adapter called with ONLY that league's af_id, result recorded."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    manifest_mock = MagicMock()

    def _stale_for_date(bucket: str, day_str: str) -> set[str]:
        return {"EPL"} if day_str == "2026-06-24" else set()

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", side_effect=_stale_for_date),
        patch(f"{MODULE}.get_league", return_value=_mock_league(39)),
        patch(f"{MODULE}.ManifestWriter", return_value=manifest_mock),
        patch(f"{MODULE}.write_fixtures_per_league"),
        patch(f"{MODULE}.sports_ref_sink_for", return_value=MagicMock()),
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=10,
        )

    # Exactly one day in the window had a stale league → exactly one fetch call.
    assert mock_adapter.get_fixtures_with_raw.call_count == 1
    call_args = mock_adapter.get_fixtures_with_raw.call_args
    assert call_args.args[0] == "2026-06-24"
    assert call_args.kwargs["league_ids"] == [39]
    assert result == {"2026-06-24/EPL": 1}
    assert manifest_mock.record_captured.call_count == 1
    rc_call = manifest_mock.record_captured.call_args
    assert rc_call.kwargs["row_key"] == {"date": "2026-06-24", "data_type": "FIXTURES", "league_id": "EPL"}
    assert rc_call.kwargs["source"] == "api_football"
    assert manifest_mock.flush.called


@pytest.mark.asyncio
@pytest.mark.unit
async def test_league_with_no_af_id_skipped(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """A stale canonical league that doesn't resolve to an api_football id is skipped, not fetched."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value={"UNKNOWN"}),
        patch(f"{MODULE}.get_league", return_value=None),
        patch(f"{MODULE}.ManifestWriter", return_value=MagicMock()),
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=1,
        )

    assert result == {}
    mock_adapter.get_fixtures_with_raw.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_scan_error_isolated_per_day(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """A scan exception for one day is caught + classified; the loop still checks other days."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    def _scan(bucket: str, day_str: str) -> set[str]:
        if day_str == "2026-06-25":
            raise RuntimeError("gcs boom")
        return set()

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", side_effect=_scan),
        patch(f"{MODULE}.classify_and_emit_error") as mock_classify,
        patch(f"{MODULE}.ManifestWriter", return_value=MagicMock()),
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=10,
        )

    assert result == {}
    mock_classify.assert_called_once()
    assert mock_classify.call_args.kwargs["operation"] == "sports_fixture_status_refresh_scan"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_fetch_error_records_failed_and_continues(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """Adapter raises on the stale day → record_failed for that day; other days unaffected."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    mock_adapter.get_fixtures_with_raw = AsyncMock(side_effect=RuntimeError("api down"))
    manifest_mock = MagicMock()

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value={"EPL"}),
        patch(f"{MODULE}.get_league", return_value=_mock_league(39)),
        patch(f"{MODULE}.ManifestWriter", return_value=manifest_mock),
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=1,
        )

    assert result == {}
    assert manifest_mock.record_failed.call_count == 1
    failed_call = manifest_mock.record_failed.call_args
    assert failed_call.kwargs["row_key"]["data_type"] == "FIXTURES"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_empty_fetch_result_no_write(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """A stale league that api-football now returns zero fixtures for → no write, no crash."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    mock_adapter.get_fixtures_with_raw = AsyncMock(return_value=[])
    manifest_mock = MagicMock()

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value={"EPL"}),
        patch(f"{MODULE}.get_league", return_value=_mock_league(39)),
        patch(f"{MODULE}.ManifestWriter", return_value=manifest_mock),
        patch(f"{MODULE}.write_fixtures_per_league") as mock_write,
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=1,
        )

    assert result == {}
    mock_write.assert_not_called()
    manifest_mock.record_captured.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_record_captured_exception_isolated(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """A record_captured failure is caught + classified — doesn't halt the run."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    manifest_mock = MagicMock()
    manifest_mock.record_captured.side_effect = RuntimeError("manifest down")

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value={"EPL"}),
        patch(f"{MODULE}.get_league", return_value=_mock_league(39)),
        patch(f"{MODULE}.ManifestWriter", return_value=manifest_mock),
        patch(f"{MODULE}.write_fixtures_per_league"),
        patch(f"{MODULE}.sports_ref_sink_for", return_value=MagicMock()),
        patch(f"{MODULE}.classify_and_emit_error") as mock_classify,
    ):
        result = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=1,
        )

    assert result == {}
    assert manifest_mock.flush.called
    assert any(
        c.kwargs.get("operation") == "sports_fixture_status_refresh_record" for c in mock_classify.call_args_list
    )


@pytest.mark.asyncio
@pytest.mark.unit
async def test_idempotent_rerun_same_result(mock_adapter: MagicMock, patch_factory: Any) -> None:
    """Re-running the same window twice produces the same shard keys (upsert semantics)."""
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    def _stale_for_date(bucket: str, day_str: str) -> set[str]:
        return {"EPL"} if day_str == "2026-06-24" else set()

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", side_effect=_stale_for_date),
        patch(f"{MODULE}.get_league", return_value=_mock_league(39)),
        patch(f"{MODULE}.ManifestWriter", return_value=MagicMock()),
        patch(f"{MODULE}.write_fixtures_per_league"),
        patch(f"{MODULE}.sports_ref_sink_for", return_value=MagicMock()),
    ):
        first = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=10,
        )
        second = await run_sports_fixture_status_refresh(
            today=date(2026, 7, 1),
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=10,
        )

    assert first == second


@pytest.mark.asyncio
@pytest.mark.unit
async def test_string_today_accepted(mock_adapter: MagicMock, patch_factory: Any) -> None:
    from instruments_service.triggers.sports_fixture_status_refresh import (
        run_sports_fixture_status_refresh,
    )

    with (
        patch(f"{MODULE}.find_stale_fixture_leagues_for_date", return_value=set()),
        patch(f"{MODULE}.ManifestWriter", return_value=MagicMock()),
    ):
        result = await run_sports_fixture_status_refresh(
            today="2026-07-19",
            api_key="test-key",
            bucket="test-sports-bucket",
            min_age_days=2,
            lookback_days=1,
        )

    assert result == {}
