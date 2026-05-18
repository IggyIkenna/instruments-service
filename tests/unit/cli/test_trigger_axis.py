"""Live-mode ``--trigger`` axis CLI tests.

Phase A.7 of ``instruments_live_master_2026_05_08`` — the instruments-service
CLI gains a ``--trigger <name>`` flag that pairs with ``--mode live`` to select
which entity-type subset is refreshed + which source adapter is invoked. The
flag itself is additive (free-form string, validated downstream by the Phase
B.1+ trigger dispatcher against the UAC closed-set taxonomy).

These tests assert two seams:

  1. ``ServiceCLI.build_parser()`` registers ``--trigger`` via
     ``_add_instruments_extra_args``; ``parse_args`` accepts a trigger name
     under both ``--mode batch`` (no-op until dispatch wires) and
     ``--mode live``; default is ``None`` so existing batch invocations stay
     unaffected. Multiple distinct trigger names parse cleanly.

  2. ``InstrumentsHandler._wire_cli_filters_from_args`` propagates the parsed
     trigger onto ``handler._trigger_name`` so downstream phases can dispatch.
     Default ``None`` leaves the field unchanged. Existing batch arg-namespaces
     without a ``trigger`` attr keep working (``getattr`` default ``None``).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date
from unittest.mock import MagicMock

import pytest
from unified_trading_library import ServiceCLI, resolve_rolling_window_args

from instruments_service.cli.instruments_handler import InstrumentsHandler
from instruments_service.cli.main import _add_instruments_extra_args

# Canonical trigger-name examples per asset_group. The closed-set lives in UAC
# (Phase A.6 todo); these are the operator-direction names per the master plan.
_CANONICAL_TRIGGER_NAMES: tuple[str, ...] = (
    "cefi.instruments.daily_refresh",
    "defi.token_lists.refresh",
    "sports.fixtures.daily_repoll",
    "sports.lineups.pre_kickoff",
    "prediction.markets.discover",
)


@pytest.mark.parametrize("trigger_name", _CANONICAL_TRIGGER_NAMES)
def test_service_cli_parses_trigger_flag_under_live_mode(trigger_name: str) -> None:
    """``ServiceCLI.build_parser()`` accepts ``--trigger <name>`` under live mode.

    Closed set per-asset-group is enforced downstream (Phase B.1+ dispatcher
    against UAC taxonomy); at parser layer the flag is a free-form string so
    new trigger names can land without a parser change.
    """
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
        "live",
        "--asset-group",
        "SPORTS",
        "--trigger",
        trigger_name,
    ]
    resolved = resolve_rolling_window_args(raw, today=date(2026, 5, 8))
    args = parser.parse_args(resolved)

    assert args.operation == "instruments"
    assert args.mode == "live"
    assert args.trigger == trigger_name


def test_trigger_default_is_none_under_batch_mode() -> None:
    """Batch invocations without ``--trigger`` parse cleanly; ``args.trigger`` is
    ``None``. This is the back-compat seam: every existing batch launcher script
    keeps working unchanged. The handler's ``getattr(... "trigger", None)``
    matches this default explicitly."""
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
        "CEFI",
        "--start-date",
        "2024-01-01",
        "--end-date",
        "2024-01-02",
    ]
    args = parser.parse_args(raw)
    assert args.trigger is None
    assert args.mode == "batch"


def test_trigger_flag_dispatches_to_handler_field(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parsed ``--trigger`` lands on ``handler._trigger_name`` after preflight.

    Asserts the full wiring path: parser → args.Namespace → preflight() →
    ``_wire_cli_filters_from_args`` → ``handler._trigger_name``. This is what
    downstream Phase B.1+ trigger dispatch will read.
    """
    runtime = MagicMock()
    runtime.asset_group = []
    runtime.gcp_project_id = "test-project"
    runtime.mode = "live"

    handler = InstrumentsHandler(runtime)
    handler.args = argparse.Namespace(
        asset_group=["SPORTS"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        recovery_fixture_ids=None,
        trigger="sports.fixtures.daily_repoll",
        lookback_days=None,
        lookahead_days=None,
        force_window=False,
        force=False,
        start_date=None,
        end_date=None,
    )

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
        lambda _: ["API_FOOTBALL"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={"api_football": "k"}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())

    assert handler._trigger_name == "sports.fixtures.daily_repoll"


def test_trigger_absent_leaves_handler_field_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Batch-mode invocations with no ``--trigger`` flag leave
    ``handler._trigger_name`` at its constructor default of ``None``. This is
    the explicit back-compat assertion: every existing batch launcher path
    must NOT populate the trigger field, so downstream dispatch can switch on
    ``None`` to mean "no live trigger; treat as batch entity sweep"."""
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
        recovery_fixture_ids=None,
        trigger=None,
        lookback_days=None,
        lookahead_days=None,
        force_window=False,
        force=False,
        start_date="2026-04-20",
        end_date="2026-04-28",
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

    assert handler._trigger_name is None


def test_trigger_attr_missing_on_args_legacy_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy code paths that hand-build an ``argparse.Namespace`` without a
    ``trigger`` attribute (e.g. older test harnesses, scripts that don't use
    ServiceCLI) still work — the handler reads via ``getattr(args, "trigger",
    None)`` so the missing attr returns ``None``. This is the back-compat
    insurance against the foot-gun where the parser registers the flag but a
    test/script bypasses argparse entirely."""
    runtime = MagicMock()
    runtime.asset_group = []
    runtime.gcp_project_id = "test-project"
    runtime.mode = "batch"

    handler = InstrumentsHandler(runtime)
    # Note: no `trigger=...` in the Namespace — simulates a pre-Phase-A.7
    # caller. getattr fallback must handle this without AttributeError.
    handler.args = argparse.Namespace(
        asset_group=["CEFI"],
        venues=None,
        sports_entity=None,
        sports_provider=None,
        league=None,
        season=None,
        recovery_fixture_ids=None,
        lookback_days=None,
        lookahead_days=None,
        force_window=False,
        force=False,
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.get_venues_for_asset_groups",
        lambda _: ["BINANCE-FUTURES"],
    )
    monkeypatch.setattr(
        "instruments_service.cli.instruments_handler.ApiKeyReloader",
        lambda **_: MagicMock(current_keys={}, start=MagicMock()),
    )

    asyncio.run(handler.preflight())

    assert handler._trigger_name is None
