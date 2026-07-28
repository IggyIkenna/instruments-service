"""Proves the T0/T1 dependency gate actually fires from REAL production call
sites, not just via a direct ``create_sports_reference_adapter(date=...)`` call.

``sports_t0_t1_dependency_gate_never_wired_2026_07_15``: the fail-loud
``DependencyError`` pre-flight existed and was unit-tested at the factory
level, but every real orchestrator call site (footystats x3, transfermarkt,
understat, sfi) omitted ``date=``, so the gate never actually fired in
production. This file exercises the real ``_fetch_*`` orchestrator functions
end-to-end (bypassing only their own skip/guard logic, never the dependency
gate itself) to prove the wiring reaches production code paths.

SSOT: ``unified-trading-pm/codex/02-data/sports-adapter-dependency-order.md``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import (
    _fetch_footystats_predictions,
    _fetch_sfi_data,
    _fetch_transfermarkt_data,
    _fetch_understat_xg,
)
from instruments_service.reference_data import sports_dependency

_DATE = "2026-01-15"
_BUCKET = "test-bucket"


class _FakeBlob:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def exists(self) -> bool:
        return self._exists


class _FakeBucket:
    def __init__(self, present_paths: set[str]) -> None:
        self._present = present_paths

    def blob(self, path: str) -> _FakeBlob:
        return _FakeBlob(path in self._present)


class _FakeStorageClient:
    """Mimics the real storage client for BOTH exact-blob and prefix probes.

    ``check_api_football_dependency`` calls ``bucket().blob().exists()`` for
    the bare date-aggregate paths and ``list_blobs(bucket, prefix=...)`` for
    the per-league prefixes — an empty ``present_paths`` set means every
    probe reports absent, forcing the DependencyError.
    """

    def __init__(self, present_paths: set[str]) -> None:
        self._present = present_paths

    def bucket(self, _name: str) -> _FakeBucket:
        return _FakeBucket(self._present)

    def list_blobs(self, _bucket: str, prefix: str = "") -> list[object]:
        return []


def _patch_dependency_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force check_api_football_dependency's two-tier check to a full miss:
    empty manifest slice (primary) + no GCS objects under any probe (fallback).
    """
    monkeypatch.setattr(
        sports_dependency,
        "read_availability_index",
        lambda *_a, **_k: pd.DataFrame(columns=["date", "data_type", "capture_status"]),
    )
    monkeypatch.setattr(
        sports_dependency,
        "get_storage_client",
        lambda *_a, **_k: _FakeStorageClient(set()),
    )


def _league(league_id: str) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(league_id=league_id)


class TestFootystatsPredictionsRealCallerWiring:
    """_fetch_footystats_predictions is a REAL production call site of
    create_sports_reference_adapter — proves date= now reaches the factory.
    """

    @pytest.mark.asyncio
    async def test_missing_api_football_dependency_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_dependency_missing(monkeypatch)

        with (
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.footystats_season_status_for_day", return_value=None),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_league("EPL")],
            ),
            pytest.raises(DependencyError),
        ):
            await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)


class TestUnderstatXgRealCallerWiring:
    """_fetch_understat_xg is a REAL production call site — no api_key arg,
    the venue that motivated this fix (pre-2018 historical understat data)."""

    @pytest.mark.asyncio
    async def test_missing_api_football_dependency_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_dependency_missing(monkeypatch)

        with (
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.footystats_season_status_for_day", return_value=None),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_league("EPL")],
            ),
            pytest.raises(DependencyError),
        ):
            await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)


class TestSfiRealCallerWiring:
    """_fetch_sfi_data is a REAL production call site — the gate fires
    unconditionally here since SFI has no early-return skip path before the
    adapter is created."""

    @pytest.mark.asyncio
    async def test_missing_api_football_dependency_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_dependency_missing(monkeypatch)

        with (
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_league("EPL")],
            ),
            pytest.raises(DependencyError),
        ):
            await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)


class TestTransfermarktRealCallerWiring:
    """_fetch_transfermarkt_data is a REAL production call site."""

    @pytest.mark.asyncio
    async def test_missing_api_football_dependency_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_dependency_missing(monkeypatch)

        with (
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator.is_transfer_window_open", return_value=True),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_league("EPL")],
            ),
            pytest.raises(DependencyError),
        ):
            await _fetch_transfermarkt_data(date=_DATE, api_key="key", bucket=_BUCKET)
