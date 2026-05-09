"""Unit + integration tests for ``sports.fixtures.daily_repoll`` trigger.

Covers Phase B.1 of ``instruments_live_master_2026_05_08.md``:

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
    """Mock ApiFootballAdapter with a default ``get_fixtures`` returning 1 EPL fixture."""
    adapter = MagicMock()
    kickoff = datetime(2026, 5, 9, 15, 0, tzinfo=UTC)
    adapter.get_fixtures = AsyncMock(
        return_value=[
            _make_fixture(
                fixture_id="1234567",
                af_id=1234567,
                league_canonical="EPL",
                af_league_id=39,
                kickoff=kickoff,
            )
        ]
    )
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
    # 9 days x 1 league = 9 calls
    assert mock_adapter.get_fixtures.call_count == 9
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

    def _capture_write(sink, df, day, *, source_label) -> None:
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
    assert "data_available_at" in df.columns
    # mock fixture kickoff = 2026-05-09 15:00 UTC; expected available_at = 2026-05-02 15:00 UTC
    expected = datetime(2026, 5, 2, 15, 0, tzinfo=UTC)
    actual = df["available_at"].iloc[0].to_pydatetime()
    assert actual == expected
    actual_legacy = df["data_available_at"].iloc[0].to_pydatetime()
    assert actual_legacy == expected


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
    assert call.kwargs["category"] == "sports"
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
async def test_run_sports_fixtures_daily_repoll_empty_records_typed_reason(
    patch_factory: Any,
) -> None:
    """Adapter returns 0 fixtures for the day → manifest record_empty with
    typed ``SOURCE_RETURNED_ZERO`` reason; no parquet write.
    """
    empty_adapter = MagicMock()
    empty_adapter.get_fixtures = AsyncMock(return_value=[])
    manifest_mock = MagicMock()
    write_fn = MagicMock()

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
    assert manifest_mock.record_empty.call_count == 1
    call = manifest_mock.record_empty.call_args
    assert call.kwargs["row_key"] == {"date": "2026-05-09", "data_type": "FIXTURES"}
    assert call.kwargs["reason"] == "SOURCE_RETURNED_ZERO"


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
