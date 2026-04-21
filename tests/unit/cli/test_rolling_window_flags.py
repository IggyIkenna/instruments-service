"""Unit tests for rolling-window CLI flag resolution.

Target: instruments_service.cli.rolling_window.resolve_rolling_window_args

Covers every edge case from the plan's "Edge cases to cover" section:
  1. Both explicit dates and rolling flags → RollingWindowError
  2. Only lookback (lookahead absent) → start=today-N, end=today
  3. Only lookahead (lookback absent) → start=today, end=today+M
  4. Zero values → single-date today
  5. Negative values rejected
  6. UTC today semantics (injected for determinism)
  7. --force-window without a window
  8. --force-window alongside rolling flags injects --force
  9. --force-window when --force already present: no duplicate
  10. Passthrough when no rolling flags present
  11. ``--flag=value`` syntax supported
  12. Malformed int values rejected
"""

from __future__ import annotations

from datetime import date

import pytest

from instruments_service.cli.rolling_window import (
    RollingWindowError,
    resolve_rolling_window_args,
)

_FIXED_TODAY = date(2026, 4, 21)


def test_lookback_and_lookahead_resolve_to_explicit_dates() -> None:
    argv = [
        "--operation",
        "instruments",
        "--mode",
        "batch",
        "--category",
        "SPORTS",
        "--sports-entity",
        "FIXTURES",
        "--sports-provider",
        "API_FOOTBALL",
        "--lookback-days",
        "1",
        "--lookahead-days",
        "7",
    ]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert "--start-date" in resolved
    assert "--end-date" in resolved
    start_idx = resolved.index("--start-date")
    end_idx = resolved.index("--end-date")
    assert resolved[start_idx + 1] == "2026-04-20"  # today - 1
    assert resolved[end_idx + 1] == "2026-04-28"  # today + 7


def test_only_lookback_fills_end_date_as_today() -> None:
    argv = ["--lookback-days", "3"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-18"
    assert resolved[resolved.index("--end-date") + 1] == "2026-04-21"


def test_only_lookahead_fills_start_date_as_today() -> None:
    argv = ["--lookahead-days", "5"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-21"
    assert resolved[resolved.index("--end-date") + 1] == "2026-04-26"


def test_zero_values_produce_single_date_today() -> None:
    argv = ["--lookback-days", "0", "--lookahead-days", "0"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-21"
    assert resolved[resolved.index("--end-date") + 1] == "2026-04-21"


def test_negative_lookback_rejected() -> None:
    with pytest.raises(RollingWindowError, match="--lookback-days must be >= 0"):
        resolve_rolling_window_args(["--lookback-days", "-1"], today=_FIXED_TODAY)


def test_negative_lookahead_rejected() -> None:
    with pytest.raises(RollingWindowError, match="--lookahead-days must be >= 0"):
        resolve_rolling_window_args(["--lookahead-days", "-3"], today=_FIXED_TODAY)


def test_mutual_exclusion_with_start_date() -> None:
    argv = ["--start-date", "2026-04-01", "--lookback-days", "7"]
    with pytest.raises(RollingWindowError, match="Cannot combine --start-date"):
        resolve_rolling_window_args(argv, today=_FIXED_TODAY)


def test_mutual_exclusion_with_end_date() -> None:
    argv = ["--end-date", "2026-04-28", "--lookahead-days", "7"]
    with pytest.raises(RollingWindowError, match="Cannot combine --start-date"):
        resolve_rolling_window_args(argv, today=_FIXED_TODAY)


def test_force_window_without_window_injects_force() -> None:
    argv = ["--force-window"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert "--force" in resolved
    assert "--start-date" not in resolved  # no window to expand


def test_force_window_with_rolling_injects_force_once() -> None:
    argv = ["--lookback-days", "1", "--lookahead-days", "7", "--force-window"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved.count("--force") == 1
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-20"


def test_force_window_when_force_already_present_no_duplicate() -> None:
    argv = ["--force", "--force-window"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved.count("--force") == 1


def test_passthrough_when_no_rolling_flags() -> None:
    argv = ["--operation", "instruments", "--start-date", "2026-04-01", "--end-date", "2026-04-05"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved == argv
    # Returned list should be an independent copy (not the same object).
    assert resolved is not argv


def test_equals_syntax_supported() -> None:
    argv = ["--lookback-days=2", "--lookahead-days=4"]
    resolved = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-19"
    assert resolved[resolved.index("--end-date") + 1] == "2026-04-25"


def test_malformed_int_value_rejected() -> None:
    with pytest.raises(RollingWindowError, match="expects an integer"):
        resolve_rolling_window_args(["--lookback-days", "not-a-number"], today=_FIXED_TODAY)


def test_missing_value_rejected() -> None:
    with pytest.raises(RollingWindowError, match="requires a value"):
        resolve_rolling_window_args(["--lookback-days"], today=_FIXED_TODAY)


def test_utc_today_default_is_used_when_not_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    """When today is not injected, UTC wall-clock date is used."""
    from datetime import UTC, datetime

    import instruments_service.cli.rolling_window as rw

    class _FrozenDateTime:
        @classmethod
        def now(cls, tz: object | None = None) -> datetime:
            assert tz is UTC  # we require UTC usage
            return datetime(2026, 4, 21, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(rw, "datetime", _FrozenDateTime)
    resolved = rw.resolve_rolling_window_args(["--lookback-days", "1", "--lookahead-days", "7"])
    assert resolved[resolved.index("--start-date") + 1] == "2026-04-20"
    assert resolved[resolved.index("--end-date") + 1] == "2026-04-28"


def test_resolution_does_not_mutate_input_argv() -> None:
    argv = ["--lookback-days", "1", "--lookahead-days", "7", "--force-window"]
    original = list(argv)
    _ = resolve_rolling_window_args(argv, today=_FIXED_TODAY)
    assert argv == original


# ---------------------------------------------------------------------------
# Handler-wiring test: --force-window sets self._force_window=True in preflight()
# and propagates to redo_all=True in the orchestrator call.
# ---------------------------------------------------------------------------


def test_force_window_preflight_sets_flag_and_propagates_to_orchestrator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preflight reads --force-window from args; process() ORs it into redo_all."""
    import argparse
    import asyncio
    from unittest.mock import MagicMock

    from unified_trading_library import BatchPayload

    from instruments_service.cli.instruments_handler import InstrumentsHandler

    runtime = MagicMock()
    runtime.category = []
    runtime.start_date = "2026-04-20"
    runtime.end_date = "2026-04-28"
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        category=["SPORTS"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        lookback_days=1,
        lookahead_days=7,
        force_window=True,
        start_date="2026-04-20",
        end_date="2026-04-28",
    )

    captured: dict[str, object] = {}

    async def _fake_process(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.engine_orchestrator.process_instruments",
        _fake_process,
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.get_venues_for_categories",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())
    assert handler._force_window is True  # set by preflight

    # BatchPayload with force=False ⇒ redo_all still True because of _force_window
    payload = BatchPayload(
        date="2026-04-21",
        categories=["SPORTS"],
        instruments=[],
        force=False,
    )
    asyncio.run(handler.process(payload))
    assert captured["redo_all"] is True


def test_force_window_false_does_not_set_redo_all_without_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sanity: without --force-window or payload.force, redo_all stays False."""
    import argparse
    import asyncio
    from unittest.mock import MagicMock

    from unified_trading_library import BatchPayload

    from instruments_service.cli.instruments_handler import InstrumentsHandler

    runtime = MagicMock()
    runtime.category = []
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        category=["SPORTS"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        lookback_days=None,
        lookahead_days=None,
        force_window=False,
        start_date=None,
        end_date=None,
    )

    captured: dict[str, object] = {}

    async def _fake_process(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.engine_orchestrator.process_instruments",
        _fake_process,
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.get_venues_for_categories",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())
    assert handler._force_window is False

    payload = BatchPayload(
        date="2026-04-21",
        categories=["SPORTS"],
        instruments=[],
        force=False,
    )
    asyncio.run(handler.process(payload))
    assert captured["redo_all"] is False
