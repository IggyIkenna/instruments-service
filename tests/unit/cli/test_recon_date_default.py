"""Unit tests for the daily-recon date self-default (incident 2026-06-23).

The scheduled cefi IS fetch job (``uts-prod-instruments-service-cefi-t1-recon``)
must NOT carry a hardcoded ``--start-date``/``--end-date`` (goes stale the next
day). ``_default_recon_dates_to_today`` injects TODAY (UTC) into ``sys.argv``
when the run is a recon run and both dates are unset — so the 06:00 scheduled
run always fetches the current day's universe.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

import pytest

from instruments_service.cli.main import (
    _arg_present,
    _arg_value,
    _default_recon_dates_to_today,
)


def test_arg_present_space_and_equals() -> None:
    assert _arg_present(["--run-tag", "t1-recon"], "--run-tag")
    assert _arg_present(["--run-tag=t1-recon"], "--run-tag")
    assert not _arg_present(["--mode", "batch"], "--run-tag")


def test_arg_value_space_and_equals() -> None:
    assert _arg_value(["--run-tag", "t1-recon"], "--run-tag") == "t1-recon"
    assert _arg_value(["--run-tag=t1-recon"], "--run-tag") == "t1-recon"
    assert _arg_value(["--mode", "batch"], "--run-tag") is None


def test_recon_run_injects_today(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--operation=instruments", "--mode=batch", "--asset-group=CEFI", "--run-tag=t1-recon"],
    )
    _default_recon_dates_to_today()
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    assert _arg_value(sys.argv[1:], "--start-date") == today
    assert _arg_value(sys.argv[1:], "--end-date") == today


def test_non_recon_run_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    argv = ["prog", "--operation=instruments", "--mode=batch", "--asset-group=CEFI", "--run-tag=batch"]
    monkeypatch.setattr(sys, "argv", list(argv))
    _default_recon_dates_to_today()
    assert sys.argv == argv  # no date injection for a non-recon run


def test_recon_run_keeps_explicit_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    argv = [
        "prog",
        "--operation=instruments",
        "--run-tag=t1-recon",
        "--start-date=2026-01-01",
        "--end-date=2026-01-05",
    ]
    monkeypatch.setattr(sys, "argv", list(argv))
    _default_recon_dates_to_today()
    assert sys.argv == argv  # manual backfill window preserved (idempotent no-op)
