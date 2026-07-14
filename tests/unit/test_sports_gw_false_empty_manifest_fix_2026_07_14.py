"""Regression tests for the 2026-07-14 GW enrichment false-empty manifest fix.

Plan: sports_p2_history_apifootball_2015_to_present_2026_06_27.md — "Fix the
enrichment manifest write path (3 legs)". Issue:
sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14.md.

Three legs:
  1. Skip-as-already-present per-fixture leagues must never be demoted to
     ``empty_confirmed`` by a no-op pre-fetch-skip run — they must be
     reaffirmed as ``captured`` from the existing parquet.
  2. The fixture league map must cover the full 94-league api_football
     universe (not the 33-league Prediction tier), and rows that cannot be
     mapped to a league must ``record_failed`` instead of being silently
     dropped.
  3. The zero-fixture-day FIXTURES marker denominator must be the full
     94-league set, not the 33-league Prediction tier.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator.process_zero_records import (
    _zero_sports_empty_fixture_markers,
)
from instruments_service.engine.orchestrator.sports_fixtures import (
    _build_fixture_league_map_from_gcs,
)
from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks
from instruments_service.engine.orchestrator.sports_reference_fixtures import (
    _write_per_fixture_entities,
)

_WIDE_LEAGUES = [
    SimpleNamespace(league_id="LA_LIGA", api_football_id=140),
    SimpleNamespace(league_id="A_LEAGUE", api_football_id=188),
]


def _hooks(manifest: MagicMock) -> _AfManifestHooks:
    return _AfManifestHooks(date="2025-11-08", manifest=manifest, attempt_ts=datetime(2026, 7, 14, tzinfo=UTC))


class TestSkipAsPresentLeaguesNeverDemoted:
    """Leg 1: a no-op pre-fetch-skip run must reaffirm captured, not emit empty."""

    def test_fully_skipped_entity_reaffirms_present_league_as_captured(self) -> None:
        """Entity yields zero NEW rows (100% skip-as-present) but the league
        has existing parquet data — must record_captured from parquet and
        exclude the league from the empty-gap loop."""
        manifest = MagicMock()
        existing_df = pd.DataFrame({"af_fixture_id": [1, 2], "value": [10, 20]})

        with (
            patch(
                "instruments_service.engine.orchestrator._read_existing_per_league_entity_df",
                return_value=existing_df,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[SimpleNamespace(league_id="LA_LIGA")],
            ),
        ):
            _write_per_fixture_entities(
                date="2025-11-08",
                bucket="test-bucket",
                hooks=_hooks(manifest),
                counts={},
                entity_names=["fixture_events"],
                entity_rows={"fixture_events": []},
                entity_failures={"fixture_events": (0, "")},
                af_fid_to_league={"111": "LA_LIGA"},
                recovery_fixture_ids=None,
                captured_per_entity_league={("fixture_events", "LA_LIGA"): frozenset({111})},
            )

        captured_calls = manifest.record_captured.call_args_list
        assert any(
            c.kwargs["row_key"] == {"date": "2025-11-08", "data_type": "FIXTURE_EVENTS", "league_id": "LA_LIGA"}
            for c in captured_calls
        ), "skip-as-present league must be reaffirmed via record_captured"

        empty_calls = manifest.record_empty.call_args_list
        assert not any(c.kwargs["row_key"].get("league_id") == "LA_LIGA" for c in empty_calls), (
            "skip-as-present league must NOT be demoted to empty_confirmed"
        )

    def test_league_with_no_existing_parquet_still_gets_honest_empty(self) -> None:
        """A genuinely never-captured league (no existing parquet, not in
        captured_per_entity_league) is unaffected — still gets the normal
        honest-absence treatment."""
        manifest = MagicMock()

        with patch(
            "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
            return_value=[SimpleNamespace(league_id="A_LEAGUE")],
        ):
            _write_per_fixture_entities(
                date="2025-11-08",
                bucket="test-bucket",
                hooks=_hooks(manifest),
                counts={},
                entity_names=["fixture_events"],
                entity_rows={"fixture_events": []},
                entity_failures={"fixture_events": (0, "")},
                af_fid_to_league={},
                recovery_fixture_ids=None,
                captured_per_entity_league={},
            )

        # No skip-as-present leagues known -> reaffirm is a no-op, normal
        # empty-gap path still runs (record_empty may or may not fire
        # depending on the coverage/calendar oracle, which isn't mocked
        # here — the key invariant is record_captured was NOT fabricated).
        assert manifest.record_captured.call_count == 0

    def test_partial_run_unions_fresh_and_reaffirmed_captures(self) -> None:
        """Some leagues get fresh rows this run, others are skip-as-present —
        both must end up captured, neither demoted."""
        manifest = MagicMock()
        existing_df = pd.DataFrame({"af_fixture_id": [5], "value": [1]})

        with (
            patch(
                "instruments_service.engine.orchestrator._read_existing_per_league_entity_df",
                return_value=existing_df,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[SimpleNamespace(league_id="LA_LIGA"), SimpleNamespace(league_id="SERIE_A")],
            ),
            patch(
                "instruments_service.engine.orchestrator._is_in_canonical_write_universe",
                return_value=True,
            ),
            patch(
                "instruments_service.engine.orchestrator._sports_ref_source",
                return_value="api_football",
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
        ):
            _write_per_fixture_entities(
                date="2025-11-08",
                bucket="test-bucket",
                hooks=_hooks(manifest),
                counts={},
                entity_names=["fixture_events"],
                # SERIE_A gets a freshly-fetched row this run.
                entity_rows={"fixture_events": [{"af_fixture_id": 999, "value": 1}]},
                entity_failures={"fixture_events": (0, "")},
                af_fid_to_league={"999": "SERIE_A"},
                recovery_fixture_ids=None,
                # LA_LIGA is skip-as-present (no new rows, existing parquet).
                captured_per_entity_league={("fixture_events", "LA_LIGA"): frozenset({5})},
            )

        captured_leagues = {
            c.kwargs["row_key"]["league_id"]
            for c in manifest.record_captured.call_args_list
            if c.kwargs["row_key"]["data_type"] == "FIXTURE_EVENTS"
        }
        assert captured_leagues == {"LA_LIGA", "SERIE_A"}
        empty_calls = manifest.record_empty.call_args_list
        assert not any(c.kwargs["row_key"].get("league_id") in {"LA_LIGA", "SERIE_A"} for c in empty_calls)


class TestBarePathDropsRecordFailed:
    """Leg 2: rows that cannot be mapped to a league must record_failed, never be silently dropped."""

    def test_unmapped_rows_record_failed(self) -> None:
        manifest = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[],
            ),
        ):
            _write_per_fixture_entities(
                date="2025-11-08",
                bucket="test-bucket",
                hooks=_hooks(manifest),
                counts={},
                entity_names=["fixture_events"],
                # af_fixture_id 42 has no entry in af_fid_to_league -> unmapped.
                entity_rows={"fixture_events": [{"af_fixture_id": 42, "value": 1}]},
                entity_failures={"fixture_events": (0, "")},
                af_fid_to_league={"1": "LA_LIGA"},
                recovery_fixture_ids=None,
                captured_per_entity_league={},
            )

        failed_calls = manifest.record_failed.call_args_list
        assert any(
            c.kwargs["row_key"] == {"date": "2025-11-08", "data_type": "FIXTURE_EVENTS"}
            and c.kwargs["error"] == "UNMAPPED_LEAGUE_ROWS_DROPPED"
            for c in failed_calls
        ), "dropped unmapped rows must record_failed, not just log a warning"

    def test_empty_league_map_record_failed(self) -> None:
        """No fixture-id column / empty af_fid map at all -> also record_failed."""
        manifest = MagicMock()

        _write_per_fixture_entities(
            date="2025-11-08",
            bucket="test-bucket",
            hooks=_hooks(manifest),
            counts={},
            entity_names=["fixture_events"],
            entity_rows={"fixture_events": [{"value": 1}]},  # no fixture-id column at all
            entity_failures={"fixture_events": (0, "")},
            af_fid_to_league={},
            recovery_fixture_ids=None,
            captured_per_entity_league={},
        )

        failed_calls = manifest.record_failed.call_args_list
        assert any(
            c.kwargs["row_key"] == {"date": "2025-11-08", "data_type": "FIXTURE_EVENTS"}
            and c.kwargs["error"] == "UNMAPPED_LEAGUE_ROWS_DROPPED"
            for c in failed_calls
        )


class TestFixtureLeagueMapUsesWideUniverse:
    """Leg 2: _build_fixture_league_map_from_gcs must cover all 94 api_football leagues, not 33."""

    def test_uses_get_expected_leagues_for_source_not_prediction_leagues(self) -> None:
        fixtures_df = pd.DataFrame({"af_fixture_id": [111], "af_league_id": [188]})

        with (
            patch(
                "instruments_service.engine.orchestrator._read_per_league_entity_df",
                return_value=fixtures_df,
            ) as mock_read,
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=_WIDE_LEAGUES,
            ) as mock_wide,
            patch("instruments_service.engine.orchestrator.get_prediction_leagues") as mock_narrow,
        ):
            result = _build_fixture_league_map_from_gcs("test-bucket", "2025-09-05")

        mock_wide.assert_called_once_with("api_football")
        mock_narrow.assert_not_called()
        # af_league_id=188 (A_LEAGUE, non-Prediction-tier) resolves via the wide map.
        assert result == {"111": "A_LEAGUE"}
        # Unbounded listing — no silent max_results=100 truncation.
        assert mock_read.call_args.kwargs.get("max_results") is None

    def test_fixture_id_column_fallback_when_af_fixture_id_absent(self) -> None:
        """Column-name drift: fall back to bare fixture_id when af_fixture_id is missing."""
        fixtures_df = pd.DataFrame({"fixture_id": [222], "league_id": ["A_LEAGUE"]})

        with (
            patch(
                "instruments_service.engine.orchestrator._read_per_league_entity_df",
                return_value=fixtures_df,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=_WIDE_LEAGUES,
            ),
        ):
            result = _build_fixture_league_map_from_gcs("test-bucket", "2025-09-05")

        assert result == {"222": "A_LEAGUE"}


class TestZeroFixtureDayMarkersUseWideUniverse:
    """Leg 3: zero-fixture-day FIXTURES markers must cover all 94 leagues, not 33."""

    def test_wide_denominator_used_when_no_league_filter(self) -> None:
        manifest = MagicMock()
        mock_cls = MagicMock(return_value=manifest)

        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_cls),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=_WIDE_LEAGUES,
            ) as mock_wide,
            patch("instruments_service.engine.orchestrator.get_all_prediction_league_ids") as mock_narrow,
        ):
            _zero_sports_empty_fixture_markers(
                date="2025-09-05",
                bucket="test-bucket",
                league_filter=None,
                fixtures_fetch_failed=False,
            )

        mock_wide.assert_called_once_with("api_football")
        mock_narrow.assert_not_called()
        empty_calls = manifest.record_empty.call_args_list
        leagues_marked = {c.kwargs["row_key"]["league_id"] for c in empty_calls}
        assert leagues_marked == {"LA_LIGA", "A_LEAGUE"}
