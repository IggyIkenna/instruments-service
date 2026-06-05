"""Unit tests for sports adapter api-football dependency enforcement.

Phase 3 of institutional-smoke-matrix plan — the sports enrichment adapters
(footystats / understat / transfermarkt / SFI / open_meteo / betfair) MUST
fail loud with an actionable ``DependencyError`` if api-football canonical
fixtures have not been fetched yet for the target date. Today the dep is
silent and downstream adapters read empty results with no clear signal.

Also covers :func:`check_sports_manifest_v9_columns` — the gated v9
schema-column regression guard added 2026-06-02 (Task 2 of
sports_manifest_canonicalisation_2026_06_01.md §P2).

SSOT: ``unified-trading-pm/codex/02-data/sports-adapter-dependency-order.md``.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from instruments_service.reference_data import sports_dependency
from instruments_service.reference_data.adapters.sports import (
    check_api_football_dependency,
    create_sports_reference_adapter,
    venue_requires_api_football,
)
from instruments_service.reference_data.sports_dependency import (
    _API_FOOTBALL_DEPENDENT_VENUES,
    _V9_REQUIRED_COLUMNS,
    check_sports_manifest_v9_columns,
)


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
    def __init__(self, present_paths: set[str]) -> None:
        self._present = present_paths

    def bucket(self, _name: str) -> _FakeBucket:
        return _FakeBucket(self._present)


def _patch_storage(monkeypatch: pytest.MonkeyPatch, present_paths: set[str]) -> None:
    """Patch the storage client in sports_dependency to a fake with given paths."""

    def _factory(*_args: Any, **_kwargs: Any) -> _FakeStorageClient:
        return _FakeStorageClient(present_paths)

    monkeypatch.setattr(sports_dependency, "get_storage_client", _factory)


class TestVenueRequiresApiFootball:
    """venue_requires_api_football returns True only for enrichment venues."""

    def test_api_football_itself_does_not_require(self) -> None:
        assert venue_requires_api_football("api_football") is False

    def test_footystats_requires(self) -> None:
        assert venue_requires_api_football("footystats") is True

    def test_understat_requires(self) -> None:
        assert venue_requires_api_football("understat") is True

    def test_transfermarkt_requires(self) -> None:
        assert venue_requires_api_football("transfermarkt") is True

    def test_sfi_requires_via_both_aliases(self) -> None:
        assert venue_requires_api_football("soccer_football_info") is True
        assert venue_requires_api_football("soccerfootball_info") is True

    def test_open_meteo_requires(self) -> None:
        assert venue_requires_api_football("open_meteo") is True

    def test_betfair_requires(self) -> None:
        assert venue_requires_api_football("betfair") is True

    def test_case_insensitive(self) -> None:
        assert venue_requires_api_football("FOOTYSTATS") is True
        assert venue_requires_api_football("Understat") is True

    def test_unknown_venue_does_not_require(self) -> None:
        assert venue_requires_api_football("nonexistent_provider") is False

    def test_dependent_set_matches_adapter_registry(self) -> None:
        """The dependent-venue set is explicit — it must cover every
        non-api-football adapter currently registered in the factory.
        """
        expected = {
            "footystats",
            "understat",
            "transfermarkt",
            "soccer_football_info",
            "soccerfootball_info",
            "open_meteo",
            "betfair",
        }
        assert frozenset(expected) == _API_FOOTBALL_DEPENDENT_VENUES


class TestCheckApiFootballDependency:
    """check_api_football_dependency reads GCS and raises DependencyError."""

    def test_passes_when_canonical_fixtures_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        bucket = "instruments-store-sports-test-project"
        date = "2026-04-14"
        canonical_path = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
        _patch_storage(monkeypatch, {canonical_path})

        # Should not raise
        check_api_football_dependency(date=date, bucket=bucket)

    def test_passes_when_raw_api_football_entity_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        bucket = "instruments-store-sports-test-project"
        date = "2026-04-14"
        raw_path = f"sports_reference/by_date/day={date}/entity=api_football/api_football.parquet"
        _patch_storage(monkeypatch, {raw_path})

        check_api_football_dependency(date=date, bucket=bucket)

    def test_raises_dependency_error_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        bucket = "instruments-store-sports-test-project"
        date = "2026-04-14"
        _patch_storage(monkeypatch, set())  # no data

        with pytest.raises(DependencyError) as exc_info:
            check_api_football_dependency(date=date, bucket=bucket)

        message = str(exc_info.value)
        # Actionable message must reference the CLI remediation command.
        assert "api-football" in message.lower()
        assert date in message
        assert bucket in message
        assert "--sports-provider API_FOOTBALL" in message
        assert f"--start-date {date}" in message
        assert f"--end-date {date}" in message

    def test_storage_error_surfaces_as_dependency_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the storage probe blows up, we still surface a DependencyError
        — not a leaked 500 / transport exception. Callers upstream can always
        retry or run the CLI remediation.
        """
        from unified_trading_library import DependencyError

        class _ExplodingClient:
            def bucket(self, _name: str) -> Any:
                raise RuntimeError("GCS transport error")

        monkeypatch.setattr(
            sports_dependency,
            "get_storage_client",
            lambda *_a, **_k: _ExplodingClient(),
        )

        with pytest.raises(DependencyError):
            check_api_football_dependency(date="2026-04-14", bucket="instruments-store-sports-test-project")


class TestFactoryDependencyWiring:
    """create_sports_reference_adapter pre-flights the dep when date is supplied."""

    def test_api_football_skips_dep_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Requesting the api-football adapter itself never triggers the
        dep check — it IS the dep. No storage access should happen.
        """
        called: dict[str, bool] = {"probed": False}

        def _marker(*_a: Any, **_k: Any) -> None:
            called["probed"] = True
            raise AssertionError("api-football adapter triggered its own dep check")

        monkeypatch.setattr(sports_dependency, "get_storage_client", _marker)
        adapter = create_sports_reference_adapter("api_football", api_key="k", date="2026-04-14")
        assert adapter.venue == "api_football"
        assert called["probed"] is False

    def test_footystats_without_date_skips_dep_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without ``date`` the factory preserves the legacy behaviour of
        just building the adapter. The orchestrator is responsible for
        calling the dep check at its pre-flight stage when it has a date.
        """

        def _marker(*_a: Any, **_k: Any) -> None:
            raise AssertionError("dep check should not fire without date")

        monkeypatch.setattr(sports_dependency, "get_storage_client", _marker)
        adapter = create_sports_reference_adapter("footystats", api_key="k")
        assert adapter.venue == "footystats"

    def test_footystats_with_date_and_missing_dep_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_storage(monkeypatch, set())

        with pytest.raises(DependencyError):
            create_sports_reference_adapter(
                "footystats",
                api_key="k",
                date="2026-04-14",
                bucket="instruments-store-sports-test-project",
            )

    def test_footystats_with_date_and_present_dep_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        bucket = "instruments-store-sports-test-project"
        date = "2026-04-14"
        canonical_path = f"sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet"
        _patch_storage(monkeypatch, {canonical_path})

        adapter = create_sports_reference_adapter("footystats", api_key="k", date=date, bucket=bucket)
        assert adapter.venue == "footystats"

    def test_understat_with_missing_dep_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unified_trading_library import DependencyError

        _patch_storage(monkeypatch, set())

        with pytest.raises(DependencyError):
            create_sports_reference_adapter(
                "understat",
                date="2026-04-14",
                bucket="instruments-store-sports-test-project",
            )

    def test_sfi_alias_still_enforces_dep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both SFI alias forms (``soccer_football_info`` and
        ``soccerfootball_info``) must go through the dep check.
        """
        from unified_trading_library import DependencyError

        _patch_storage(monkeypatch, set())

        with pytest.raises(DependencyError):
            create_sports_reference_adapter(
                "soccer_football_info",
                api_key="k",
                date="2026-04-14",
                bucket="instruments-store-sports-test-project",
            )
        with pytest.raises(DependencyError):
            create_sports_reference_adapter(
                "soccerfootball_info",
                api_key="k",
                date="2026-04-14",
                bucket="instruments-store-sports-test-project",
            )

    def test_unsupported_venue_fails_before_dep_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unsupported venues raise ``ValueError`` from the adapter registry,
        not ``DependencyError`` — venue-not-supported is a coding bug, not a
        data-availability problem.
        """

        def _marker(*_a: Any, **_k: Any) -> None:
            raise AssertionError("dep check should not fire for unsupported venue")

        monkeypatch.setattr(sports_dependency, "get_storage_client", _marker)
        with pytest.raises(ValueError, match="Unsupported"):
            create_sports_reference_adapter("nonexistent_provider", date="2026-04-14")


# ---------------------------------------------------------------------------
# Task 2 — v9 schema-column regression guard (gated)
# sports_manifest_canonicalisation_2026_06_01.md §P2  (2026-06-02)
# ---------------------------------------------------------------------------


def _v8_df() -> pd.DataFrame:
    """Simulate a pre-migration v8 manifest _index — no v9 columns."""
    return pd.DataFrame(
        {
            "date": ["2026-01-01"],
            "data_type": ["FIXTURES"],
            "capture_status": ["captured"],
            "schema_version": [8],
            # v9 columns absent: asset_group, source, pipeline_mode, available_at
        }
    )


def _v9_df() -> pd.DataFrame:
    """Simulate a post-walk v9 manifest _index — all v9 columns present."""
    return pd.DataFrame(
        {
            "date": ["2026-01-01"],
            "data_type": ["FIXTURES"],
            "capture_status": ["captured"],
            "schema_version": [9],
            "asset_group": ["sports"],
            "source": ["api_football"],
            "pipeline_mode": ["batch_api_football"],
            "available_at": ["2026-01-01T00:00:00Z"],
        }
    )


class TestCheckSportsManifestV9Columns:
    """check_sports_manifest_v9_columns is a gated regression guard.

    Default (flag off, v8 data) → WARNING + pass.
    SPORTS_V9_ENFORCED=true or schema_version=9 but missing cols → DependencyError.
    All v9 columns present → always pass regardless of flag.
    """

    def test_v8_data_flag_off_passes_with_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Pre-migration v8 index + flag off → pass (just warn)."""
        import logging

        monkeypatch.setattr(
            sports_dependency,
            "get_config",
            lambda: _FakeConfig(sports_v9_enforced=False),
        )
        with caplog.at_level(logging.WARNING, logger="instruments_service.reference_data.sports_dependency"):
            check_sports_manifest_v9_columns(_v8_df())
        assert any("v9-column" in r.message for r in caplog.records)

    def test_v9_data_all_cols_present_always_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Full v9 index → passes even when flag is True."""
        monkeypatch.setattr(
            sports_dependency,
            "get_config",
            lambda: _FakeConfig(sports_v9_enforced=True),
        )
        check_sports_manifest_v9_columns(_v9_df())  # must not raise

    def test_flag_on_v8_data_missing_cols_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SPORTS_V9_ENFORCED=true + v8 data (missing cols) → DependencyError."""
        from unified_trading_library import DependencyError as UTLDependencyError

        monkeypatch.setattr(
            sports_dependency,
            "get_config",
            lambda: _FakeConfig(sports_v9_enforced=True),
        )
        with pytest.raises(UTLDependencyError, match="missing v9 schema columns"):
            check_sports_manifest_v9_columns(_v8_df())

    def test_schema_version_9_triggers_enforcement_even_flag_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """schema_version=9 in data activates enforcement even if flag is False
        — a post-walk regression (v9 claimed but columns missing) must fail loud.
        """
        from unified_trading_library import DependencyError as UTLDependencyError

        # DataFrame claims v9 but is missing the v9 columns.
        broken_v9_df = pd.DataFrame(
            {
                "date": ["2026-01-01"],
                "schema_version": [9],
                "capture_status": ["captured"],
                # missing: asset_group, source, pipeline_mode, available_at
            }
        )
        monkeypatch.setattr(
            sports_dependency,
            "get_config",
            lambda: _FakeConfig(sports_v9_enforced=False),
        )
        with pytest.raises(UTLDependencyError, match="missing v9 schema columns"):
            check_sports_manifest_v9_columns(broken_v9_df)

    def test_required_columns_constant(self) -> None:
        """The required column set is the canonical v9 column quartet."""
        assert {"asset_group", "source", "pipeline_mode", "available_at"} == _V9_REQUIRED_COLUMNS


class _FakeConfig:
    """Minimal stub for InstrumentsServiceConfig used in v9 guard tests."""

    def __init__(self, *, sports_v9_enforced: bool) -> None:
        self.sports_v9_enforced = sports_v9_enforced
        self.is_test_run = False
