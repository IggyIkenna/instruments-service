"""Unit tests for ``_close_stale_enrichment_expected_unattempted_cells``.

Plan: ``unified-trading-pm/plans/active/issues/
api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``
(final todo). The function's whole reason for existing is to be SAFER than a naive mirror of
``process_write.py``'s FIXTURES ``_expected_af_lids - _captured_lids`` closer — it must never
stamp ``EXPECTED_NO_FIXTURE`` over a cell where a real fixture demonstrably existed. These
tests pin that safety contract directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator.sports_reference_core import (
    _close_stale_enrichment_expected_unattempted_cells as close_stale_cells,
)


def _stuck_df(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Build a ``stuck_cells`` frame from ``(date, data_type, league_id)`` tuples."""
    return pd.DataFrame(rows, columns=["date", "data_type", "league_id"])


class TestNoProviderCoverageBranch:
    """A (league, entity) the provider never covers closes unconditionally — this
    fact is independent of whether a fixture existed, so it never needs the
    FIXTURES cross-check."""

    def test_closes_as_expected_no_provider_coverage(self) -> None:
        manifest = MagicMock()
        stuck = _stuck_df([("2020-06-14", "FIXTURE_EVENTS", "AUSTRIAN_2_LIGA")])

        with patch(
            "instruments_service.engine.orchestrator.is_league_entity_covered",
            return_value=False,
        ):
            counts = close_stale_cells(
                manifest=manifest,
                stuck_cells=stuck,
                fixtures_empty_reason_by_date_league={},
            )

        assert counts == {"2020-06-14/FIXTURE_EVENTS": 1}
        manifest.record_empty.assert_called_once()
        kwargs = manifest.record_empty.call_args.kwargs
        assert kwargs["reason"] == "EXPECTED_NO_PROVIDER_COVERAGE"
        assert kwargs["row_key"] == {
            "date": "2020-06-14",
            "data_type": "FIXTURE_EVENTS",
            "league_id": "AUSTRIAN_2_LIGA",
        }


class TestFixturesMirrorBranch:
    """When the provider DOES cover the entity, closing is only safe when FIXTURES
    itself already proved no fixture existed for the identical (date, league)."""

    def test_mirrors_fixtures_own_empty_reason_when_covered_and_fixtures_confirms_absence(self) -> None:
        manifest = MagicMock()
        stuck = _stuck_df([("2020-06-14", "FIXTURE_STATS", "ENG_NATIONAL_LEAGUE")])

        with patch(
            "instruments_service.engine.orchestrator.is_league_entity_covered",
            return_value=True,
        ):
            counts = close_stale_cells(
                manifest=manifest,
                stuck_cells=stuck,
                fixtures_empty_reason_by_date_league={
                    ("2020-06-14", "ENG_NATIONAL_LEAGUE"): "EXPECTED_PAUSED_LEAGUE",
                },
            )

        assert counts == {"2020-06-14/FIXTURE_STATS": 1}
        kwargs = manifest.record_empty.call_args.kwargs
        assert kwargs["reason"] == "EXPECTED_PAUSED_LEAGUE"


class TestGenuinePendingFetchGapIsNeverTouched:
    """Regression pin for the disconfirmed 'off-season' hypothesis (live spot-check,
    2026-07-19): AUSTRIAN_2_LIGA/AUSTRIAN_BUNDESLIGA/GREEK_SUPER_LEAGUE FIXTURE_STATS
    on 2020-06-14 are provider-covered AND FIXTURES was captured (a real fixture
    existed) — closing these would be false-empty corruption. Must be left alone."""

    def test_covered_and_fixtures_captured_leaves_cell_untouched(self) -> None:
        manifest = MagicMock()
        stuck = _stuck_df(
            [
                ("2020-06-14", "FIXTURE_STATS", "AUSTRIAN_2_LIGA"),
                ("2020-06-14", "FIXTURE_STATS", "AUSTRIAN_BUNDESLIGA"),
                ("2020-06-14", "FIXTURE_STATS", "GREEK_SUPER_LEAGUE"),
            ]
        )

        with patch(
            "instruments_service.engine.orchestrator.is_league_entity_covered",
            return_value=True,
        ):
            # FIXTURES entity was `captured` for all 3 (not `empty_confirmed`), so
            # none of them appear in the cross-check lookup at all.
            counts = close_stale_cells(
                manifest=manifest,
                stuck_cells=stuck,
                fixtures_empty_reason_by_date_league={},
            )

        assert counts == {}
        manifest.record_empty.assert_not_called()

    def test_mixed_group_only_closes_the_provably_safe_leagues(self) -> None:
        manifest = MagicMock()
        stuck = _stuck_df(
            [
                ("2020-06-14", "FIXTURE_STATS", "GREEK_SUPER_LEAGUE"),  # captured FIXTURES — leave alone
                ("2020-06-14", "FIXTURE_STATS", "SOME_OFF_SEASON_LEAGUE"),  # FIXTURES empty_confirmed — safe to close
            ]
        )

        with patch(
            "instruments_service.engine.orchestrator.is_league_entity_covered",
            return_value=True,
        ):
            counts = close_stale_cells(
                manifest=manifest,
                stuck_cells=stuck,
                fixtures_empty_reason_by_date_league={
                    ("2020-06-14", "SOME_OFF_SEASON_LEAGUE"): "EXPECTED_NO_FIXTURE",
                },
            )

        assert counts == {"2020-06-14/FIXTURE_STATS": 1}
        manifest.record_empty.assert_called_once()
        assert manifest.record_empty.call_args.kwargs["row_key"]["league_id"] == "SOME_OFF_SEASON_LEAGUE"


class TestEmptyInput:
    def test_empty_stuck_cells_returns_empty_counts(self) -> None:
        manifest = MagicMock()
        counts = close_stale_cells(
            manifest=manifest,
            stuck_cells=_stuck_df([]),
            fixtures_empty_reason_by_date_league={},
        )
        assert counts == {}
        manifest.record_empty.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
