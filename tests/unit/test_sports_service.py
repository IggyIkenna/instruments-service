"""
Unit tests for sports orchestrator, service config, and package imports.

Tests cover:
- SportsOrchestrator: league determination, stub processing
- Service config: SPORTS bucket configuration
- Sports package importability and exported symbols
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pytest

from instruments_service.sports.league_registry import LEAGUE_REGISTRY

# ---------------------------------------------------------------------------
# Sports Orchestrator
# ---------------------------------------------------------------------------

# The SportsOrchestrator lives under instruments_service.engine.operations.instruments.orchestration.
# Importing that path transitively triggers __init__.py files that pull in heavy dependencies
# (unified_events_interface.ErrorWarningCounter, unified_market_interface.clients.block_resolver)
# which may not be available in every test environment.
# We use importlib.util to load the module directly, bypassing the __init__.py chain.


def _load_sports_orchestrator() -> type:
    """Load SportsOrchestrator without triggering transitive __init__ imports."""
    _root = Path(__file__).resolve().parent.parent.parent
    _sports_path = (
        _root
        / "instruments_service"
        / "engine"
        / "operations"
        / "instruments"
        / "orchestration"
        / "sports_orchestration.py"
    )
    spec = importlib.util.spec_from_file_location(
        "sports_orchestration",
        str(_sports_path),
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.SportsOrchestrator  # type: ignore[no-any-return]


_SportsOrchestrator = _load_sports_orchestrator()


class TestSportsOrchestrator:
    """Tests for the sports orchestrator."""

    @pytest.mark.asyncio
    async def test_process_sports_no_filter(self) -> None:
        """Without filter, all registered leagues are considered."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=[],
        )
        # Phase 1 stub: always returns empty dict
        assert result == {}

    @pytest.mark.asyncio
    async def test_process_sports_with_filter(self) -> None:
        """With filter, only matching leagues are processed."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=["EPL", "NBA"],
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_process_sports_no_matching_leagues(self) -> None:
        """If filter contains no valid leagues, returns empty."""
        orchestrator = _SportsOrchestrator()
        result = await orchestrator.process_sports(
            date=datetime(2026, 3, 1, tzinfo=UTC),
            venues_filter=["BINANCE-SPOT"],
        )
        assert result == {}

    def test_determine_leagues_no_filter(self) -> None:
        """No filter returns all registered leagues."""
        orchestrator = _SportsOrchestrator()
        leagues = orchestrator._determine_leagues([])
        assert sorted(leagues) == sorted(LEAGUE_REGISTRY.keys())

    def test_determine_leagues_with_filter(self) -> None:
        """Filter restricts to matching leagues."""
        orchestrator = _SportsOrchestrator()
        leagues = orchestrator._determine_leagues(["EPL", "NBA", "UNKNOWN"])
        assert sorted(leagues) == ["EPL", "NBA"]


# ---------------------------------------------------------------------------
# Service Config -- SPORTS bucket support
# ---------------------------------------------------------------------------


class TestServiceConfigSports:
    """Tests for SPORTS category in InstrumentsServiceConfig."""

    def test_get_cloud_target_sports(self) -> None:
        """get_bucket_for_category('SPORTS') returns a bucket string."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        # Config uses get_bucket_for_category instead of get_cloud_target
        bucket = config.get_bucket_for_category("SPORTS")
        assert bucket is not None
        assert isinstance(bucket, str)

    def test_get_cloud_target_invalid_category(self) -> None:
        """get_bucket_for_category with invalid category raises ValueError."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        with pytest.raises(ValueError, match="Invalid category"):
            config.get_bucket_for_category("INVALID")

    def test_get_bucket_for_category_sports(self) -> None:
        """get_bucket_for_category('SPORTS') returns a bucket name."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        # Either returns the configured bucket or falls back to default
        bucket = config.get_bucket_for_category("SPORTS")
        assert isinstance(bucket, str)

    def test_get_bucket_for_category_invalid(self) -> None:
        """get_bucket_for_category with invalid category raises ValueError."""
        from instruments_service.config.service_config import InstrumentsServiceConfig

        config = InstrumentsServiceConfig()
        with pytest.raises(ValueError, match="Invalid category"):
            config.get_bucket_for_category("INVALID")


# ---------------------------------------------------------------------------
# Sports __init__ imports
# ---------------------------------------------------------------------------


class TestSportsPackageImports:
    """Verify the sports package is importable and exports expected symbols."""

    def test_import_sports_package(self) -> None:
        import instruments_service.sports

        assert hasattr(instruments_service.sports, "FixtureParser")
        assert hasattr(instruments_service.sports, "LeagueDefinition")
        assert hasattr(instruments_service.sports, "LEAGUE_REGISTRY")
        assert hasattr(instruments_service.sports, "TeamNormalizer")

    def test_import_new_registry_functions(self) -> None:
        """New registry functions are importable from the package."""
        from instruments_service.sports import (
            get_league,
            get_league_by_api_football_id,
            get_leagues_by_classification,
            get_leagues_by_country,
            get_leagues_for_sport,
            get_prediction_leagues,
        )

        assert callable(get_league)
        assert callable(get_league_by_api_football_id)
        assert callable(get_leagues_by_classification)
        assert callable(get_leagues_by_country)
        assert callable(get_leagues_for_sport)
        assert callable(get_prediction_leagues)

    def test_import_fixture_parser(self) -> None:
        from instruments_service.sports import FixtureParser

        parser = FixtureParser()
        assert parser is not None

    def test_import_league_registry(self) -> None:
        from instruments_service.sports import LEAGUE_REGISTRY

        assert isinstance(LEAGUE_REGISTRY, dict)

    def test_import_team_normalizer(self) -> None:
        from instruments_service.sports import TeamNormalizer

        normalizer = TeamNormalizer()
        assert normalizer is not None
