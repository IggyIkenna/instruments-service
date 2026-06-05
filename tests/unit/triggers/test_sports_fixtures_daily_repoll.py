"""Unit + integration tests for ``sports.fixtures.daily_repoll`` trigger.

Covers Phase B.1 of ``instruments_master.md``:

1. Adapter mocked → run_sports_fixtures_daily_repoll iterates the
   9-day window, calls ``get_fixtures`` per day, flattens via the same
   helper as batch, and writes per-(day, league) parquets via the
   canonical ``_write_fixtures_per_league`` sink.
2. ``available_at`` is stamped per-row at write time as
   ``announced_at = kickoff_utc - 7d`` (UAC SPORTS FIXTURES rule).
3. Manifest writer is called with the correct row_key shape per
   (day, canonical league_id) and ``data_type="FIXTURES"``.
4. Idempotency: re-running the same trigger upserts the same shard
   keys (last-writer-wins, no duplicate rows on disk).
5. Empty source → ``record_empty`` with typed reason.
6. Raise inside per-day loop → ``record_failed`` for that day, loop
   continues for remaining days (shard isolation).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_fixture(
    *,
    fixture_id: str,
    af_id: int,
    league_canonical: str,
    af_league_id: int,
    kickoff: datetime,
    season: str = "2025-26",
) -> Any:
    """Build a CanonicalFixture for the test mock.

    Keep the shape minimal — only fields ``_flatten_canonical_fixture_for_disk``
    reads. The rest default to ``None`` per CanonicalFixture's BaseModel.
    """
    from unified_api_contracts.sports import (
        CanonicalFixture,
        CanonicalLeague,
        CanonicalTeam,
    )

    home = CanonicalTeam(
        team_id="LIVERPOOL",
        name="Liverpool FC",
        country="England",
    )
    away = CanonicalTeam(
        team_id="MAN_UTD",
        name="Manchester United",
        country="England",
    )
    league = CanonicalLeague(
        league_id=league_canonical,
        name="Premier League",
        country="England",
    )
    return CanonicalFixture(
        fixture_id=fixture_id,
        source_fixture_id=str(af_id),
        home_team=home,
        away_team=away,
        league=league,
        kickoff_utc=kickoff,
        season=season,
        source="api_football",
        status="NS",
    )


@pytest.fixture
def mock_adapter() -> MagicMock:
    """Mock ApiFootballAdapter with a default ``get_fixtures`` returning 1 EPL fixture.

    The trigger now calls ``get_fixtures_with_raw`` (fixture-schedule-split Phase
    3) to thread the raw api-football response into the Q5/Q6 lifecycle overlay.
    We configure it to pair each fixture with an EMPTY raw dict (the base
    adapter's default for sources that don't surface the AF response) so the
    flatten falls back to honest Q5/Q6 defaults — these tests assert the trigger
    plumbing, not the lifecycle columns (covered in
    ``test_fixture_lifecycle_columns.py``).
    """
    adapter = MagicMock()
    kickoff = datetime(2026, 5, 9, 15, 0, tzinfo=UTC)
    _fixtures = [
        _make_fixture(
            fixture_id="1234567",
            af_id=1234567,
            league_canonical="EPL",
            af_league_id=39,
            kickoff=kickoff,
        )
    ]
    adapter.get_fixtures = AsyncMock(return_value=_fixtures)
    adapter.get_fixtures_with_raw = AsyncMock(return_value=[(fx, {}) for fx in _fixtures])
    return adapter


@pytest.fixture
def patch_factory(mock_adapter: MagicMock) -> Any:
    """Patch the trigger's adapter factory to return our mock."""
    with patch(
        "instruments_service.triggers.sports_fixtures_daily_repoll.create_sports_reference_adapter",
        return_value=mock_adapter,
    ) as p:
        yield p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_iterates_window(
    mock_adapter: MagicMock,
    patch_factory: Any,
) -> None:
    """Trigger iterates today + 8 days (9 total) and calls get_fixtures per day."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink") as _sink,
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter") as _mw,
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
    ):
        _sink.return_value = MagicMock()
        _mw.return_value = MagicMock()
        result = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
        )
    # 9 days x 1 league = 9 calls. The trigger fetches via get_fixtures_with_raw
    # (paired raw response) for the Q5/Q6 lifecycle overlay.
    assert mock_adapter.get_fixtures_with_raw.call_count == 9
    # 9 (day, league) shards written, 1 fixture each
    assert len(result) == 9
    assert all(v == 1 for v in result.values())
    # Keys = "<day>/EPL" for the 9 days
    expected_days = [(date(2026, 5, 9) + timedelta(days=i)).isoformat() for i in range(9)]
    expected_keys = {f"{d}/EPL" for d in expected_days}
    assert set(result.keys()) == expected_keys


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_stamps_available_at(
    mock_adapter: MagicMock,
    patch_factory: Any,
) -> None:
    """Each row carries `available_at = announced_at = kickoff_utc - 7d`."""
    captured_dfs: list[Any] = []

    def _capture_write(sink, df, day, *, source_label, bucket=None, skip_if_unchanged=False) -> None:
        captured_dfs.append(df.copy())

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink") as _sink,
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter") as _mw,
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league",
            side_effect=_capture_write,
        ),
    ):
        _sink.return_value = MagicMock()
        _mw.return_value = MagicMock()
        await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
            lookahead_days=0,  # just today
        )

    assert len(captured_dfs) == 1
    df = captured_dfs[0]
    assert "available_at" in df.columns
    assert "data_available_at" not in df.columns
    # mock fixture kickoff = 2026-05-09 15:00 UTC; expected available_at = 2026-05-02 15:00 UTC
    expected = datetime(2026, 5, 2, 15, 0, tzinfo=UTC)
    actual = df["available_at"].iloc[0].to_pydatetime()
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_record_captured_shape(
    mock_adapter: MagicMock,
    patch_factory: Any,
) -> None:
    """ManifestWriter.record_captured called with canonical sports row_key."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    manifest_mock = MagicMock()
    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink",
            return_value=MagicMock(),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
    ):
        await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
            lookahead_days=0,  # just today, single shard
        )

    assert manifest_mock.record_captured.call_count == 1
    call = manifest_mock.record_captured.call_args
    # row_key shape per CLAUDE.md "Per-asset-group shard-key matrix → Sports"
    assert call.kwargs["row_key"] == {
        "date": "2026-05-09",
        "data_type": "FIXTURES",
        "league_id": "EPL",
    }
    assert call.kwargs["asset_group"] == "sports"
    assert call.kwargs["instrument_type"] == "football"
    assert call.kwargs["data_type"] == "FIXTURES"
    assert call.kwargs["league_id"] == "EPL"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_idempotent(
    mock_adapter: MagicMock,
    patch_factory: Any,
) -> None:
    """Re-running the same trigger same day → same shard keys (upsert semantics).

    The manifest writer dedupes by row_key (CAS, last-writer-wins), and the
    sink's per-partition write overwrites the existing parquet — so two
    fires of the same trigger same UTC day produce the same on-disk state.
    Test just confirms the second invocation produces the same result dict
    keys as the first (no duplicate keys, no missing keys).
    """
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink",
            return_value=MagicMock(),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=MagicMock(),
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
    ):
        first = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
        )
        second = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
        )

    assert set(first.keys()) == set(second.keys())
    assert first == second


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_empty_records_per_league_typed_reason(
    patch_factory: Any,
) -> None:
    """Adapter returns 0 fixtures for the day → manifest record_empty per league with
    oracle-derived typed reason (CF-5 write-path fix).

    When api-football returns 0 fixtures the trigger no longer emits a single
    day-grain SOURCE_RETURNED_ZERO.  Instead it resolves the oracle reason per
    league:
    - oracle says (False, reason) → that EXPECTED_* reason
    - oracle says (True, None) AND no fixture on calendar → EXPECTED_NO_FIXTURE
    - oracle says (True, None) AND calendar has a fixture → SOURCE_RETURNED_ZERO

    This test drives the EXPECTED_NO_FIXTURE branch (typical mid-week day with
    no fixture scheduled).
    """
    from unittest.mock import call as _call

    from unified_api_contracts import EmptyConfirmedReason

    empty_adapter = MagicMock()
    empty_adapter.get_fixtures = AsyncMock(return_value=[])
    empty_adapter.get_fixtures_with_raw = AsyncMock(return_value=[])
    manifest_mock = MagicMock()
    write_fn = MagicMock()

    # Mock league definition returned by get_league_by_api_football_id
    mock_league_def = MagicMock()
    mock_league_def.league_id = "EPL"

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.create_sports_reference_adapter",
            return_value=empty_adapter,
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink",
            return_value=MagicMock(),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league",
            side_effect=write_fn,
        ),
        # Patch oracle: EPL is expected (True, None)
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.is_expected_for_source",
            return_value=(True, None),
        ),
        # Patch get_league_by_api_football_id: returns EPL league def
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_league_by_api_football_id",
            return_value=mock_league_def,
        ),
        # Patch get_league_fixture_calendar: no fixture on this day → EXPECTED_NO_FIXTURE
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_league_fixture_calendar",
            return_value=[],  # empty = no fixture scheduled
        ),
    ):
        result = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
            lookahead_days=0,
        )

    assert result == {}
    write_fn.assert_not_called()
    # One per-league record_empty call (EPL)
    assert manifest_mock.record_empty.call_count == 1
    call = manifest_mock.record_empty.call_args
    assert call.kwargs["row_key"] == {"date": "2026-05-09", "data_type": "FIXTURES", "league_id": "EPL"}
    assert call.kwargs["reason"] == EmptyConfirmedReason.EXPECTED_NO_FIXTURE


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_empty_pre_coverage_start_reason(
    patch_factory: Any,
) -> None:
    """Oracle says (False, EXPECTED_PRE_SOURCE_COVERAGE_START) → that typed reason emitted per league."""
    from unified_api_contracts import EmptyConfirmedReason

    empty_adapter = MagicMock()
    empty_adapter.get_fixtures = AsyncMock(return_value=[])
    empty_adapter.get_fixtures_with_raw = AsyncMock(return_value=[])
    manifest_mock = MagicMock()

    mock_league_def = MagicMock()
    mock_league_def.league_id = "EPL"

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.create_sports_reference_adapter",
            return_value=empty_adapter,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink", return_value=MagicMock()),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
        # Oracle: date is before coverage start → not expected
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.is_expected_for_source",
            return_value=(False, "EXPECTED_PRE_SOURCE_COVERAGE_START"),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_league_by_api_football_id",
            return_value=mock_league_def,
        ),
    ):
        result = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
            lookahead_days=0,
        )

    assert result == {}
    assert manifest_mock.record_empty.call_count == 1
    call = manifest_mock.record_empty.call_args
    assert call.kwargs["row_key"]["league_id"] == "EPL"
    assert call.kwargs["reason"] == EmptyConfirmedReason.EXPECTED_PRE_SOURCE_COVERAGE_START


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_empty_source_returned_zero_when_fixture_scheduled(
    patch_factory: Any,
) -> None:
    """Oracle says (True, None) AND fixture on calendar → SOURCE_RETURNED_ZERO (real gap)."""
    from unified_api_contracts import EmptyConfirmedReason

    empty_adapter = MagicMock()
    empty_adapter.get_fixtures = AsyncMock(return_value=[])
    empty_adapter.get_fixtures_with_raw = AsyncMock(return_value=[])
    manifest_mock = MagicMock()

    mock_league_def = MagicMock()
    mock_league_def.league_id = "EPL"

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.create_sports_reference_adapter",
            return_value=empty_adapter,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink", return_value=MagicMock()),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
        # Oracle: shard IS expected
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.is_expected_for_source",
            return_value=(True, None),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_league_by_api_football_id",
            return_value=mock_league_def,
        ),
        # Calendar HAS a fixture → source really returned zero (unexpected gap)
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_league_fixture_calendar",
            return_value=["fixture1"],
        ),
    ):
        result = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
            lookahead_days=0,
        )

    assert result == {}
    assert manifest_mock.record_empty.call_count == 1
    call = manifest_mock.record_empty.call_args
    assert call.kwargs["row_key"]["league_id"] == "EPL"
    assert call.kwargs["reason"] == EmptyConfirmedReason.SOURCE_RETURNED_ZERO


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_per_day_isolation_on_fetch_error(
    patch_factory: Any,
) -> None:
    """Adapter raises on day 1 → record_failed for day 1; days 2..9 still execute."""
    erratic_adapter = MagicMock()
    kickoff = datetime(2026, 5, 9, 15, 0, tzinfo=UTC)
    good_fixture = [
        _make_fixture(
            fixture_id="1234567",
            af_id=1234567,
            league_canonical="EPL",
            af_league_id=39,
            kickoff=kickoff,
        )
    ]
    erratic_adapter.get_fixtures = AsyncMock(side_effect=[RuntimeError("boom")] + [good_fixture] * 8)
    # Trigger calls get_fixtures_with_raw — mirror the per-day error isolation:
    # day 1 raises, days 2..9 return the good fixture paired with an empty raw dict.
    _good_pairs = [(fx, {}) for fx in good_fixture]
    erratic_adapter.get_fixtures_with_raw = AsyncMock(side_effect=[RuntimeError("boom")] + [_good_pairs] * 8)
    manifest_mock = MagicMock()

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.create_sports_reference_adapter",
            return_value=erratic_adapter,
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink",
            return_value=MagicMock(),
        ),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
    ):
        result = await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="test-key",
            bucket="test-sports-bucket",
            league_filter=["EPL"],
        )

    # 8 successful (day, league) shards + 1 failed day → 8 captured shards in result.
    assert len(result) == 8
    # day 1 (2026-05-09) had failed fetch → not in result.
    assert "2026-05-09/EPL" not in result
    # record_failed called once for the failed day.
    assert manifest_mock.record_failed.call_count == 1
    failed_call = manifest_mock.record_failed.call_args
    assert failed_call.kwargs["row_key"] == {"date": "2026-05-09", "data_type": "FIXTURES"}


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_blank_api_key_raises() -> None:
    """Empty / whitespace api_key fails loud at the trigger entry point."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with pytest.raises(ValueError, match="api-football API key"):
        await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="",
            bucket="test-sports-bucket",
        )
    with pytest.raises(ValueError, match="api-football API key"):
        await run_sports_fixtures_daily_repoll(
            today=date(2026, 5, 9),
            api_key="   ",
            bucket="test-sports-bucket",
        )


@pytest.mark.unit
def test_trigger_name_constant() -> None:
    """Public trigger-name constant matches the closed-set name in CLAUDE.md."""
    from instruments_service.triggers import (
        SPORTS_FIXTURES_DAILY_REPOLL_TRIGGER,
    )

    assert SPORTS_FIXTURES_DAILY_REPOLL_TRIGGER == "sports.fixtures.daily_repoll"


@pytest.mark.unit
def test_resolve_today_none_returns_utc_date() -> None:
    """_resolve_today(None) returns today from UTC clock."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _resolve_today,
    )

    result = _resolve_today(None)
    assert result is not None
    assert result.year >= 2026


@pytest.mark.unit
def test_resolve_today_string() -> None:
    """_resolve_today accepts ISO date strings."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _resolve_today,
    )

    result = _resolve_today("2026-05-15")
    assert result.year == 2026
    assert result.month == 5
    assert result.day == 15


@pytest.mark.unit
def test_date_window_correct() -> None:
    """_date_window returns inclusive [today, today+N] list."""
    from datetime import date

    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _date_window,
    )

    window = _date_window(date(2026, 5, 9), 2)
    assert len(window) == 3
    assert window[0] == date(2026, 5, 9)
    assert window[-1] == date(2026, 5, 11)


@pytest.mark.unit
def test_get_instruments_bucket_for_asset_group() -> None:
    """get_instruments_bucket_for_asset_group normalizes the asset group name."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        get_instruments_bucket_for_asset_group,
    )

    with patch(
        "instruments_service.triggers.sports_fixtures_daily_repoll.resolve_bucket_name",
        return_value="test-bucket",
    ) as mock_resolve:
        result = get_instruments_bucket_for_asset_group("SPORTS")
    mock_resolve.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="sports")
    assert result == "test-bucket"


@pytest.mark.unit
def test_league_ids_for_repoll_with_numeric_filter() -> None:
    """Numeric string league IDs are passed through directly."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _league_ids_for_repoll,
    )

    result = _league_ids_for_repoll(["39", "140"])
    assert 39 in result
    assert 140 in result


@pytest.mark.unit
def test_league_ids_for_repoll_with_integer_filter() -> None:
    """Integer league IDs are passed through directly."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _league_ids_for_repoll,
    )

    result = _league_ids_for_repoll([39, 140])
    assert 39 in result
    assert 140 in result


@pytest.mark.unit
def test_league_ids_for_repoll_with_canonical_name_not_found() -> None:
    """Canonical league name not found → skipped."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _league_ids_for_repoll,
    )

    with patch("unified_api_contracts.sports.get_league", return_value=None):
        result = _league_ids_for_repoll(["NONEXISTENT"])
    assert result == []


@pytest.mark.unit
def test_league_ids_for_repoll_with_canonical_name_found() -> None:
    """Canonical league name resolves to AF ID when found."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        _league_ids_for_repoll,
    )

    mock_league = MagicMock()
    mock_league.api_football_id = 39
    with patch("unified_api_contracts.sports.get_league", return_value=mock_league):
        result = _league_ids_for_repoll(["EPL"])
    assert 39 in result


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_no_bucket_raises() -> None:
    """Empty bucket after resolution raises ValueError."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.get_instruments_bucket_for_asset_group",
            return_value="",
        ),
        pytest.raises(ValueError, match="bucket"),
    ):
        await run_sports_fixtures_daily_repoll(
            today="2026-05-09",
            api_key="test-key",
            bucket=None,
            league_filter=["39"],
        )


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_string_today(
    mock_adapter: MagicMock,
    patch_factory: MagicMock,
) -> None:
    """today= accepts ISO date strings."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    with (
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink") as _sink,
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter") as _mw,
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
    ):
        _sink.return_value = MagicMock()
        _mw.return_value = MagicMock()
        result = await run_sports_fixtures_daily_repoll(
            today="2026-05-09",
            api_key="test-key",
            bucket="test-bucket",
            league_filter=["EPL"],
            lookahead_days=0,
        )
    assert len(result) == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_run_sports_fixtures_daily_repoll_record_captured_exception(
    mock_adapter: MagicMock,
    patch_factory: MagicMock,
) -> None:
    """record_captured exception is caught and doesn't halt execution."""
    from instruments_service.triggers.sports_fixtures_daily_repoll import (
        run_sports_fixtures_daily_repoll,
    )

    manifest_mock = MagicMock()
    manifest_mock.record_captured.side_effect = RuntimeError("manifest error")

    with (
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.get_data_sink", return_value=MagicMock()),
        patch(
            "instruments_service.triggers.sports_fixtures_daily_repoll.ManifestWriter",
            return_value=manifest_mock,
        ),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll._write_fixtures_per_league"),
        patch("instruments_service.triggers.sports_fixtures_daily_repoll.classify_and_emit_error"),
    ):
        result = await run_sports_fixtures_daily_repoll(
            today="2026-05-09",
            api_key="test-key",
            bucket="test-bucket",
            league_filter=["EPL"],
            lookahead_days=0,
        )
    # record_captured failed so count is NOT added to result
    assert result == {}
