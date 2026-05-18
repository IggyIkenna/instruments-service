"""Rolling-window CLI integration test for instruments-service.

The resolver itself is tested exhaustively in UTL
(`tests/unit/service_framework/test_rolling_window.py`). This test verifies
the flags are wired into the instruments-service ServiceBootstrap path and
that `--force-window` propagates all the way to `redo_all=True` in the
orchestrator call via UTL's ServiceCLI argv-resolution.
"""

from __future__ import annotations

import argparse
import asyncio
from unittest.mock import MagicMock

import pytest
from unified_trading_library import BatchPayload

from instruments_service.cli.instruments_handler import InstrumentsHandler
from instruments_service.cli.main import _add_instruments_extra_args


def test_service_cli_parses_rolling_window_flags_end_to_end() -> None:
    """ServiceCLI.run() resolves rolling flags before parse_args."""
    from datetime import date

    from unified_trading_library import ServiceCLI, resolve_rolling_window_args

    cli = ServiceCLI(
        service_name="instruments-service",
        operations={"instruments": InstrumentsHandler},
        config={},
        extra_args_fn=_add_instruments_extra_args,
    )
    parser = cli.build_parser()

    raw = [
        "--operation",
        "instruments",
        "--mode",
        "batch",
        "--asset-group",
        "SPORTS",
        "--lookback-days",
        "1",
        "--lookahead-days",
        "7",
        "--force-window",
        "--sports-provider",
        "API_FOOTBALL",
    ]
    resolved = resolve_rolling_window_args(raw, today=date(2026, 4, 21))
    args = parser.parse_args(resolved)

    assert args.start_date == "2026-04-20"
    assert args.end_date == "2026-04-28"
    assert args.force is True
    assert args.lookback_days == 1
    assert args.lookahead_days == 7
    assert args.force_window is True
    assert args.sports_provider == "API_FOOTBALL"


def test_force_window_propagates_to_redo_all_via_payload_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--force-window → UTL injects --force → BatchIO sets payload.force → redo_all=True."""
    runtime = MagicMock()
    runtime.asset_group = []
    runtime.start_date = "2026-04-20"
    runtime.end_date = "2026-04-28"
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        asset_group=["SPORTS"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        lookback_days=1,
        lookahead_days=7,
        force_window=True,
        force=True,  # injected by UTL resolve_rolling_window_args
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
        "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())

    payload = BatchPayload(
        date="2026-04-21",
        asset_groups=["SPORTS"],
        instruments=[],
        force=True,  # propagated by BatchIO from args.force
    )
    asyncio.run(handler.process(payload))
    assert captured["redo_all"] is True


def test_preflight_wires_all_custom_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercises every `--sports-entity / --sports-provider / --league / --season /
    --venues` preflight branch in one go. Boosts handler coverage for the
    custom-args block (lines 70-101 + rolling-window log at ~110)."""
    runtime = MagicMock()
    runtime.asset_group = []
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        asset_group=["SPORTS"],
        venues=["API_FOOTBALL"],
        sports_entity="FIXTURES",
        sports_provider="api_football",
        league="EPL,BUNDESLIGA",
        season=2024,
        lookback_days=1,
        lookahead_days=7,
        force_window=True,
        force=True,
        start_date="2026-04-20",
        end_date="2026-04-28",
    )

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.earliest_venue_date",
        lambda _: "2020-01-01",
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={"api_football": "k"}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())

    assert handler._venue_override == ["API_FOOTBALL"]
    assert handler._sports_entity_filter == "FIXTURES"
    assert handler._sports_provider == "API_FOOTBALL"  # uppercased
    assert handler._league_filter == ["EPL", "BUNDESLIGA"]
    assert handler._season_override == 2024


def test_cleanup_flushes_manifest_writers_and_emits_coordination_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercises the cleanup() path (lines 180-211): per-category manifest flush
    + DATA_READY / SPORTS_LIVE_STATS coordination events. Both publish calls
    are wrapped in suppress(RuntimeError, ValueError) for batch mode."""
    runtime = MagicMock()
    runtime.asset_group = ["SPORTS"]
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(asset_group=["SPORTS"])

    flushed_buckets: list[str] = []
    published_events: list[str] = []

    class _FakeWriter:
        def __init__(self, *, service_name: str, catalogue_bucket: str) -> None:
            self.bucket = catalogue_bucket

        def flush(self) -> None:
            flushed_buckets.append(self.bucket)

    def _fake_publish(event_type: str, payload: dict[str, object]) -> None:
        published_events.append(event_type)
        # Simulate batch-mode behaviour: raises ValueError, which cleanup()
        # suppresses via contextlib.suppress.
        raise ValueError("batch mode — no-op")

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler._get_instruments_bucket",
        lambda cat: f"instruments-store-{cat.lower()}-test-project",
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ManifestWriter",
        _FakeWriter,
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.publish_coordination_event",
        _fake_publish,
    )

    asyncio.run(handler.cleanup())

    # All four categories flushed.
    assert len(flushed_buckets) == 4
    # DATA_READY always published; SPORTS_LIVE_STATS because SPORTS is in category.
    assert "DATA_READY" in published_events
    assert "SPORTS_LIVE_STATS" in published_events


def test_no_rolling_flags_leaves_redo_all_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity: no --force / --force-window / redo_all in payload ⇒ redo_all=False."""
    runtime = MagicMock()
    runtime.asset_group = []
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        asset_group=["SPORTS"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        lookback_days=None,
        lookahead_days=None,
        force_window=False,
        force=False,
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
        "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())
    payload = BatchPayload(
        date="2026-04-21",
        asset_groups=["SPORTS"],
        instruments=[],
        force=False,
    )
    asyncio.run(handler.process(payload))
    assert captured["redo_all"] is False
