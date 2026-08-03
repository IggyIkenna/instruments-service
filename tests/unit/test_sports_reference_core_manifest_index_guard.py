"""``emit_empty_gaps_for_entity``'s manifest-index oscillation guard — extends
the FIXTURES_SCHEDULE-only guard (``process_write._write_sports_fixture_venue``,
``instruments-service@<sha for _manifest_captured_leagues_for_data_type
generalization>``) to the "regular sports instruments" (TEAMS/STANDINGS/
INJURIES/...) emitted via ``_AfManifestHooks.emit_empty_gaps_for_entity``.

``sports_index_recency_masked_captured_atoms_2026_07_13.md`` § "ROOT CAUSE
CORRECTED": the ``enumerate_v2`` seeder's ``captured_set`` guard
(``instruments-service@ba306543``) and the FIXTURES-only manifest guard both
leave this entity path uncovered — a captured manifest row under a DIFFERENT
identity than this run's writer, with no on-disk object under this run's
per-league path, could still be masked by a later ``empty_confirmed`` stamp.

Covers:
* ``_manifest_captured_leagues_for_data_type`` generalized to a non-FIXTURES
  data_type (TEAMS) — proves the generalization, not just the rename.
* ``_AfManifestHooks._manifest_index_guarded_captured_leagues`` — unions the
  manifest-captured set in; ``bucket=""`` disables it (mirrors the presence
  guard); a read failure fails safe (``None``).
* ``emit_empty_gaps_for_entity`` end-to-end: a league with an existing
  manifest ``captured`` row (but not captured this run, and no on-disk
  presence) must NOT get ``record_empty``; a genuinely uncaptured league
  still does; a manifest-read failure skips the whole empty-emission pass.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator.sports_reference_core import (
    _AfManifestHooks,
    _manifest_captured_leagues_for_data_type,
)


class TestManifestCapturedLeaguesForDataTypeGeneralized:
    def test_non_fixtures_data_type_filters_correctly(self) -> None:
        """The generalized helper must filter on the CALLER-supplied data_type,
        not a hardcoded FIXTURES_SCHEDULE — proves this is a real generalization,
        not just a rename."""
        idx = pd.DataFrame(
            {
                "date": ["2026-07-22", "2026-07-22", "2026-07-22"],
                "data_type": ["TEAMS", "TEAMS", "FIXTURES_SCHEDULE"],
                "league_id": ["BRASILEIRAO", "EPL", "BRASILEIRAO"],
                "capture_status": ["captured", "empty_confirmed", "captured"],
            }
        )
        with patch("instruments_service.engine.orchestrator.read_availability_index", return_value=idx):
            result = _manifest_captured_leagues_for_data_type(
                bucket="test-bucket", date="2026-07-22", data_type="TEAMS"
            )
        # EPL/TEAMS is empty_confirmed (excluded); BRASILEIRAO/FIXTURES_SCHEDULE
        # is a different data_type (excluded) — only BRASILEIRAO/TEAMS survives.
        assert result == {"BRASILEIRAO"}


class TestManifestIndexGuardedCapturedLeagues:
    def test_bucket_empty_disables_guard(self) -> None:
        """``bucket=""`` (legacy/test construction) must skip the manifest
        read entirely and return the input set unchanged — mirrors the
        presence guard's disable contract."""
        hooks = _AfManifestHooks(date="2026-07-22", manifest=MagicMock(), attempt_ts=datetime.now(UTC), bucket="")
        with patch("instruments_service.engine.orchestrator._manifest_captured_leagues_for_data_type") as mock_read:
            result = hooks._manifest_index_guarded_captured_leagues("TEAMS", {"EPL"})
        mock_read.assert_not_called()
        assert result == {"EPL"}

    def test_unions_manifest_captured_leagues_in(self) -> None:
        hooks = _AfManifestHooks(
            date="2026-07-22", manifest=MagicMock(), attempt_ts=datetime.now(UTC), bucket="test-bucket"
        )
        with patch(
            "instruments_service.engine.orchestrator._manifest_captured_leagues_for_data_type",
            return_value={"BRASILEIRAO"},
        ):
            result = hooks._manifest_index_guarded_captured_leagues("TEAMS", {"EPL"})
        assert result == {"EPL", "BRASILEIRAO"}

    def test_manifest_read_failure_returns_none_fail_safe(self) -> None:
        hooks = _AfManifestHooks(
            date="2026-07-22", manifest=MagicMock(), attempt_ts=datetime.now(UTC), bucket="test-bucket"
        )
        with patch(
            "instruments_service.engine.orchestrator._manifest_captured_leagues_for_data_type",
            return_value=None,
        ):
            assert hooks._manifest_index_guarded_captured_leagues("TEAMS", {"EPL"}) is None


class TestEmitEmptyGapsForEntityManifestIndexGuardEndToEnd:
    def test_manifest_captured_league_not_this_run_is_not_re_emitted_empty(self) -> None:
        """A league with NO on-disk presence and NOT captured this run, but a
        CAPTURED manifest row from a PRIOR run under a different identity,
        must be excluded from the empty-gap loop for a "regular sports
        instrument" entity (TEAMS)."""
        mock_manifest = MagicMock()
        hooks = _AfManifestHooks(
            date="2026-07-22", manifest=mock_manifest, attempt_ts=datetime.now(UTC), bucket="test-bucket"
        )
        manifest_index = pd.DataFrame(
            {
                "date": ["2026-07-22"],
                "data_type": ["TEAMS"],
                "league_id": ["BRASILEIRAO"],
                "capture_status": ["captured"],
            }
        )
        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="BRASILEIRAO"), MagicMock(league_id="LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator.get_entity_league_coverage", return_value=None),
            patch("instruments_service.engine.orchestrator._list_present_parquet_leagues", return_value=set()),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=manifest_index),
            patch("instruments_service.engine.orchestrator.is_league_entity_covered", return_value=True),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
        ):
            hooks.emit_empty_gaps_for_entity("TEAMS", set())

        emitted_leagues = {c.kwargs["row_key"]["league_id"] for c in mock_manifest.record_empty.call_args_list}
        assert emitted_leagues == {"LA_LIGA"}, "BRASILEIRAO must be guarded (manifest already shows it captured)"

    def test_manifest_read_failure_skips_empty_emission_entirely(self) -> None:
        """FAIL-SAFE: a manifest-index read error must skip the whole
        empty-emission pass, not just fall back to the presence guard's
        result — never stamp empty_confirmed over data it could not prove
        absent."""
        mock_manifest = MagicMock()
        hooks = _AfManifestHooks(
            date="2026-07-22", manifest=mock_manifest, attempt_ts=datetime.now(UTC), bucket="test-bucket"
        )
        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator._list_present_parquet_leagues", return_value=set()),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                side_effect=RuntimeError("gcs unavailable"),
            ),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
        ):
            hooks.emit_empty_gaps_for_entity("STANDINGS", set())

        assert mock_manifest.record_empty.call_count == 0

    def test_this_run_and_presence_captured_leagues_still_excluded(self) -> None:
        """Regression: leagues already excluded by the this-run set or the
        presence guard stay excluded once the manifest-index guard is
        unioned in on top (empty manifest index this time)."""
        mock_manifest = MagicMock()
        hooks = _AfManifestHooks(
            date="2026-07-22", manifest=mock_manifest, attempt_ts=datetime.now(UTC), bucket="test-bucket"
        )
        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="EPL"), MagicMock(league_id="LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator.get_entity_league_coverage", return_value=None),
            patch("instruments_service.engine.orchestrator._list_present_parquet_leagues", return_value=set()),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.is_league_entity_covered", return_value=True),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
        ):
            hooks.emit_empty_gaps_for_entity("STANDINGS", {"EPL"})

        emitted_leagues = {c.kwargs["row_key"]["league_id"] for c in mock_manifest.record_empty.call_args_list}
        assert emitted_leagues == {"LA_LIGA"}
