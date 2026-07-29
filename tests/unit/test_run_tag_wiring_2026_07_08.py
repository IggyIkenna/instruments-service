"""Regression tests for --run-tag actually reaching a real output path.

instruments_service_run_tag_flag_not_applied_2026_07_08: --run-tag's help text
promised a "GCS output prefix tag" but the parsed value never reached
apply_run_tag() anywhere in this service -- it was a pure no-op flag (the only
real consumer was a narrow raw-argv string match in cli/main.py that
self-defaults the recon date window, unrelated to output isolation).

Fix: thread run_tag from the CLI --run-tag arg -> InstrumentsHandler ->
process_instruments() -> the package-level engine_orchestrator._RUN_TAG state
-> _sports_ref_sink_for()'s apply_run_tag() call, per the documented
{run_tag}/{service_name}/{date}/ convention in
unified-trading-pm/codex/08-workflows/t1-batch-dag.md.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from instruments_service.engine import orchestrator as engine_orchestrator
from instruments_service.engine.orchestrator import _sports_ref_sink_for, process_instruments


@pytest.fixture(autouse=True)
def _reset_run_tag_state() -> Any:
    """_RUN_TAG is process-global mutable state -- never let one test's tag leak into another."""
    prior = engine_orchestrator._RUN_TAG
    yield
    engine_orchestrator._RUN_TAG = prior


def _make_runtime(mode: str = "batch") -> MagicMock:
    rt = MagicMock()
    rt.gcp_project_id = "test-project"
    rt.mode = mode
    rt.start_date = "2026-03-22"
    rt.end_date = "2026-03-22"
    rt.asset_group = ["SPORTS"]
    return rt


def _make_handler() -> Any:
    from instruments_service.cli.instruments_handler import InstrumentsHandler

    handler = InstrumentsHandler(_make_runtime())
    handler.args = None
    return handler


# ---------------------------------------------------------------------------
# _sports_ref_sink_for() -- the real GCS writer choke point for sports
# ---------------------------------------------------------------------------


def test_sports_ref_sink_for_default_run_tag_is_no_op() -> None:
    """Default '--run-tag' ('batch') must not change the canonical prefix."""
    engine_orchestrator._RUN_TAG = "batch"
    with patch("instruments_service.engine.orchestrator.get_data_sink", return_value=MagicMock()) as mock_sink:
        _sports_ref_sink_for("test-bucket", "2026-03-22", "FIXTURES")
    _, kwargs = mock_sink.call_args
    assert kwargs["prefix"] == "sports_reference/by_date/day=2026-03-22/pipeline_mode=batch_api_football"
    assert not kwargs["prefix"].startswith("batch/")


def test_sports_ref_sink_for_applies_real_run_tag_prefix() -> None:
    """A real --run-tag (e.g. t1-recon) must prefix the sink's GCS path."""
    engine_orchestrator._RUN_TAG = "t1-recon"
    with patch("instruments_service.engine.orchestrator.get_data_sink", return_value=MagicMock()) as mock_sink:
        _sports_ref_sink_for("test-bucket", "2026-03-22", "FIXTURES")
    _, kwargs = mock_sink.call_args
    assert kwargs["prefix"] == "t1-recon/sports_reference/by_date/day=2026-03-22/pipeline_mode=batch_api_football"


# ---------------------------------------------------------------------------
# process_instruments() -- sets the package-level _RUN_TAG state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_instruments_stashes_run_tag_on_orchestrator_state() -> None:
    """run_tag= must land on engine_orchestrator._RUN_TAG before any write happens."""
    assert engine_orchestrator._RUN_TAG == "batch"  # sane starting point (autouse fixture resets after)
    with (
        patch(
            "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
            return_value=["TOTALLY_NEW_VENUE"],
        ),
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=False),
    ):
        result = await process_instruments("2026-03-22", ["CEFI"], run_tag="t1-recon")
    assert result == {}
    assert engine_orchestrator._RUN_TAG == "t1-recon"


@pytest.mark.asyncio
async def test_process_instruments_defaults_run_tag_to_batch() -> None:
    """Omitting run_tag= (the overwhelming majority of call sites) must resolve to 'batch'."""
    engine_orchestrator._RUN_TAG = "some-stale-value-from-a-prior-call"
    with (
        patch(
            "instruments_service.engine.orchestrator.get_venues_for_asset_groups",
            return_value=["TOTALLY_NEW_VENUE"],
        ),
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=False),
    ):
        await process_instruments("2026-03-22", ["CEFI"])
    assert engine_orchestrator._RUN_TAG == "batch"


# ---------------------------------------------------------------------------
# InstrumentsHandler -- CLI --run-tag -> self._run_tag -> process_instruments(run_tag=...)
# ---------------------------------------------------------------------------


def test_wire_cli_filters_run_tag_arg() -> None:
    handler = _make_handler()
    args = MagicMock()
    args.venues = None
    args.sports_entity = None
    args.sports_provider = None
    args.league = None
    args.season = None
    args.recovery_fixture_ids = None
    args.source = None
    args.trigger = None
    args.lookback_days = None
    args.lookahead_days = None
    args.force_window = False
    args.run_tag = "t1-recon"
    handler.args = args

    handler._wire_cli_filters_from_args()

    assert handler._run_tag == "t1-recon"


def test_wire_cli_filters_run_tag_defaults_to_batch_when_unset() -> None:
    handler = _make_handler()
    args = MagicMock()
    args.venues = None
    args.sports_entity = None
    args.sports_provider = None
    args.league = None
    args.season = None
    args.recovery_fixture_ids = None
    args.source = None
    args.trigger = None
    args.lookback_days = None
    args.lookahead_days = None
    args.force_window = False
    args.run_tag = None
    handler.args = args

    handler._wire_cli_filters_from_args()

    assert handler._run_tag == "batch"


@pytest.mark.asyncio
async def test_process_passes_run_tag_through_to_process_instruments() -> None:
    handler = _make_handler()
    handler._run_tag = "t1-recon"
    handler._key_reloader = None

    payload = MagicMock()
    payload.date = "2026-03-22"
    payload.force = False
    payload.extra = {}
    payload.asset_groups = ["CEFI"]

    with (
        patch(
            "instruments_service.cli.instruments_handler.engine_orchestrator.process_instruments",
        ) as mock_process,
        patch.object(handler, "_emit_date_heartbeat"),
    ):
        mock_process.return_value = {}
        await handler.process(payload)

    assert mock_process.call_args.kwargs["run_tag"] == "t1-recon"
