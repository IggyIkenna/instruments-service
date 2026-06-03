"""Unit tests for sports_reference v9 canonical path helpers.

Validates:
1. _sports_ref_pm() returns the correct pipeline_mode value per entity name.
2. _sports_ref_source() strips the batch_ prefix to return the source string.
3. _sports_ref_sink_for() creates a sink whose prefix embeds day= + pipeline_mode=
   so that DataSink's alphabetic partition sort puts entity= and league= in the
   correct canonical order:
     sports_reference/by_date/day={D}/pipeline_mode={PM}/entity={E}/league={L}/{fname}
4. _sports_ref_canonical_blob_path() and _sports_ref_legacy_blob_path() produce
   the expected path strings.
5. _resolve_sports_ref_blob() returns canonical when it exists, else legacy.
6. Object paths produced by _sports_ref_sink_for() carry pipeline_mode= matching
   the manifest row's pipeline_mode (path==manifest invariant).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from instruments_service.engine.orchestrator import (
    _ENTITY_NAME_TO_PIPELINE_MODE,
    _resolve_sports_ref_blob,
    _sports_ref_canonical_blob_path,
    _sports_ref_legacy_blob_path,
    _sports_ref_pm,
    _sports_ref_sink_for,
    _sports_ref_source,
)

# ---------------------------------------------------------------------------
# _sports_ref_pm
# ---------------------------------------------------------------------------


class TestSportsRefPm:
    """Tests for _sports_ref_pm entity → pipeline_mode mapping."""

    @pytest.mark.parametrize(
        "entity,expected_pm",
        [
            ("fixtures", "batch_api_football"),
            ("injuries", "batch_api_football"),
            ("teams", "batch_api_football"),
            ("standings", "batch_api_football"),
            ("fixture_stats", "batch_api_football"),
            ("fixture_events", "batch_api_football"),
            ("fixture_lineups", "batch_api_football"),
            ("player_stats", "batch_api_football"),
            ("footystats_predictions", "batch_footystats"),
            ("footystats_matches", "batch_footystats"),
            ("footystats_odds", "batch_odds_api"),
            ("understat_xg", "batch_understat"),
            ("understat_xg_shots", "batch_understat"),
            ("player_values", "batch_transfermarkt"),
            ("progressive_stats", "batch_soccer_football_info"),
            ("weather", "batch_open_meteo"),
        ],
    )
    def test_known_entities(self, entity: str, expected_pm: str) -> None:
        """Known entity names return the correct pipeline_mode value string."""
        assert _sports_ref_pm(entity) == expected_pm

    def test_unknown_entity_falls_back(self) -> None:
        """Unknown entity falls back to BATCH_INSTRUMENTS_SERVICE."""
        result = _sports_ref_pm("unknown_entity_xyz")
        assert result == "batch_instruments_service"


# ---------------------------------------------------------------------------
# _sports_ref_source
# ---------------------------------------------------------------------------


class TestSportsRefSource:
    """Tests for _sports_ref_source entity → source string derivation."""

    @pytest.mark.parametrize(
        "entity,expected_source",
        [
            ("fixtures", "api_football"),
            ("injuries", "api_football"),
            ("footystats_predictions", "footystats"),
            ("footystats_odds", "odds_api"),
            ("understat_xg", "understat"),
            ("player_values", "transfermarkt"),
            ("progressive_stats", "soccer_football_info"),
            ("weather", "open_meteo"),
        ],
    )
    def test_source_strips_batch_prefix(self, entity: str, expected_source: str) -> None:
        """Source is the pipeline_mode value with 'batch_' stripped."""
        assert _sports_ref_source(entity) == expected_source


# ---------------------------------------------------------------------------
# _sports_ref_canonical_blob_path / _sports_ref_legacy_blob_path
# ---------------------------------------------------------------------------


class TestBlobPathBuilders:
    """Tests for canonical and legacy blob path builders."""

    def test_canonical_with_league(self) -> None:
        """Canonical path includes pipeline_mode= between day= and entity=."""
        path = _sports_ref_canonical_blob_path("2026-06-01", "fixtures", league="EPL")
        assert path == (
            "sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_api_football"
            "/entity=fixtures/league=EPL/fixtures.parquet"
        )

    def test_canonical_without_league(self) -> None:
        """Canonical path without league omits league= segment."""
        path = _sports_ref_canonical_blob_path("2026-06-01", "fixtures")
        assert path == (
            "sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_api_football/entity=fixtures/fixtures.parquet"
        )

    def test_canonical_custom_filename(self) -> None:
        """Canonical path uses the provided filename."""
        path = _sports_ref_canonical_blob_path(
            "2026-06-01", "understat_xg", league="BUNDESLIGA", filename="understat_xg.parquet"
        )
        assert path == (
            "sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_understat"
            "/entity=understat_xg/league=BUNDESLIGA/understat_xg.parquet"
        )

    def test_legacy_with_league(self) -> None:
        """Legacy path has no pipeline_mode= segment."""
        path = _sports_ref_legacy_blob_path("2026-06-01", "fixtures", league="EPL")
        assert path == ("sports_reference/by_date/day=2026-06-01/entity=fixtures/league=EPL/fixtures.parquet")

    def test_legacy_without_league(self) -> None:
        """Legacy path without league omits league= segment."""
        path = _sports_ref_legacy_blob_path("2026-06-01", "fixtures")
        assert path == "sports_reference/by_date/day=2026-06-01/entity=fixtures/fixtures.parquet"

    def test_canonical_pm_matches_migration_ssot(self) -> None:
        """Canonical path format matches _canon_instr_reference in migrate_sports_canonical_v9.py.

        The migration replaces day={D}/ with day={D}/pipeline_mode={PM}/ in the path.
        This test asserts the IS writer produces the same layout.

        Migration target (from _canon_instr_reference docstring):
          sports_reference/by_date/day={D}/pipeline_mode={PM}/entity={E}/[league={L}/]{fname}
        """
        # api_football entities
        for entity in ("fixtures", "teams", "standings", "injuries"):
            path = _sports_ref_canonical_blob_path("2024-01-15", entity, league="EPL")
            assert "pipeline_mode=batch_api_football" in path
            assert f"/entity={entity}/" in path
            assert "/league=EPL/" in path
            # day= must come before pipeline_mode= must come before entity=
            idx_day = path.index("/day=")
            idx_pm = path.index("/pipeline_mode=")
            idx_entity = path.index("/entity=")
            assert idx_day < idx_pm < idx_entity, f"Path segment order wrong for {entity}: {path}"


# ---------------------------------------------------------------------------
# _sports_ref_sink_for — path ordering via DataSink
# ---------------------------------------------------------------------------


class TestSportsRefSinkFor:
    """Tests that _sports_ref_sink_for produces a sink with the correct prefix."""

    def test_sink_prefix_includes_day_and_pm(self) -> None:
        """Sink prefix includes day= and pipeline_mode= so partition only needs entity/league."""
        with (
            __import__("unittest.mock", fromlist=["patch"]).patch(
                "instruments_service.engine.orchestrator.get_data_sink"
            ) as mock_get_sink,
            __import__("unittest.mock", fromlist=["patch"]).patch(
                "instruments_service.engine.orchestrator.get_storage_client"
            ),
        ):
            mock_get_sink.return_value = MagicMock()
            _sports_ref_sink_for("my-bucket", "2026-06-01", "fixtures")

        mock_get_sink.assert_called_once_with(
            bucket="my-bucket",
            prefix="sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_api_football",
        )

    def test_sink_prefix_for_understat(self) -> None:
        """Understat xg sink has batch_understat pipeline_mode in prefix."""
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "instruments_service.engine.orchestrator.get_data_sink"
        ) as mock_get_sink:
            mock_get_sink.return_value = MagicMock()
            _sports_ref_sink_for("bucket", "2026-01-01", "understat_xg")

        mock_get_sink.assert_called_once_with(
            bucket="bucket",
            prefix="sports_reference/by_date/day=2026-01-01/pipeline_mode=batch_understat",
        )

    def test_path_pm_matches_manifest_row_pm(self) -> None:
        """pipeline_mode= value in the object path equals the manifest row's pipeline_mode.

        This enforces the path==manifest invariant from the plan:
        'The `pm` value MUST equal the `pipeline_mode=` already passed to the matching
        record_captured_from_counts/record_empty call for that entity.'
        """
        from unified_api_contracts.canonical.crosscutting.pipeline_mode import PipelineMode

        for entity, pm_enum in _ENTITY_NAME_TO_PIPELINE_MODE.items():
            # The sink prefix should embed the pipeline_mode value string.
            # Derive it directly from the same PipelineMode enum value.
            expected_pm_str = pm_enum.value
            actual_pm_str = _sports_ref_pm(entity)
            assert actual_pm_str == expected_pm_str, (
                f"Entity {entity!r}: _sports_ref_pm returned {actual_pm_str!r}, "
                f"expected {expected_pm_str!r} (from _ENTITY_NAME_TO_PIPELINE_MODE)"
            )

            # Canonical blob path must contain the correct pipeline_mode= segment.
            canon_path = _sports_ref_canonical_blob_path("2026-01-01", entity)
            assert f"pipeline_mode={expected_pm_str}" in canon_path, (
                f"Canonical path for {entity!r} missing pipeline_mode={expected_pm_str}: {canon_path}"
            )


# ---------------------------------------------------------------------------
# _resolve_sports_ref_blob
# ---------------------------------------------------------------------------


class TestResolveSportsRefBlob:
    """Tests for the canonical-first blob path resolution."""

    def test_returns_canonical_when_exists(self) -> None:
        """Returns canonical path when the canonical blob exists in GCS."""
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        canonical = (
            "sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_api_football/entity=fixtures/fixtures.parquet"
        )
        legacy = "sports_reference/by_date/day=2026-06-01/entity=fixtures/fixtures.parquet"

        result = _resolve_sports_ref_blob(mock_storage, "my-bucket", canonical, legacy)
        assert result == canonical

    def test_returns_legacy_when_canonical_missing(self) -> None:
        """Falls back to legacy path when canonical blob does not exist."""
        mock_storage = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_storage.bucket.return_value.blob.return_value = mock_blob

        canonical = (
            "sports_reference/by_date/day=2026-06-01/pipeline_mode=batch_api_football/entity=fixtures/fixtures.parquet"
        )
        legacy = "sports_reference/by_date/day=2026-06-01/entity=fixtures/fixtures.parquet"

        result = _resolve_sports_ref_blob(mock_storage, "my-bucket", canonical, legacy)
        assert result == legacy

    def test_returns_legacy_on_storage_error(self) -> None:
        """Falls back to legacy path if the exists() check raises an exception."""
        mock_storage = MagicMock()
        mock_storage.bucket.return_value.blob.return_value.exists.side_effect = OSError("GCS error")

        canonical = "canonical_path"
        legacy = "legacy_path"

        result = _resolve_sports_ref_blob(mock_storage, "bucket", canonical, legacy)
        assert result == legacy


# ---------------------------------------------------------------------------
# source= derivation integration
# ---------------------------------------------------------------------------


class TestSourceDerivation:
    """Tests that _sports_ref_source produces values consistent with the migration.

    The migration's _source_from_row falls back to:
      data_type → SPORTS_DATA_TYPE_TO_SOURCE[data_type]
    and the pipeline_mode prefix: batch_X → X.

    _sports_ref_source(entity) must produce the same value as would be inferred
    from the pipeline_mode by the manifest rebuild.
    """

    def test_api_football_entities_source(self) -> None:
        """api_football entities produce source='api_football'."""
        for entity in ("fixtures", "injuries", "teams", "standings", "fixture_stats"):
            assert _sports_ref_source(entity) == "api_football", entity

    def test_footystats_entities_source(self) -> None:
        """footystats_predictions and footystats_matches produce source='footystats'."""
        assert _sports_ref_source("footystats_predictions") == "footystats"
        assert _sports_ref_source("footystats_matches") == "footystats"

    def test_footystats_odds_source(self) -> None:
        """footystats_odds produce source='odds_api' (BATCH_ODDS_API)."""
        assert _sports_ref_source("footystats_odds") == "odds_api"

    def test_understat_source(self) -> None:
        """understat entities produce source='understat'."""
        assert _sports_ref_source("understat_xg") == "understat"
        assert _sports_ref_source("understat_xg_shots") == "understat"

    def test_transfermarkt_source(self) -> None:
        """player_values produces source='transfermarkt'."""
        assert _sports_ref_source("player_values") == "transfermarkt"

    def test_sfi_source(self) -> None:
        """progressive_stats produces source='soccer_football_info'."""
        assert _sports_ref_source("progressive_stats") == "soccer_football_info"

    def test_open_meteo_source(self) -> None:
        """weather produces source='open_meteo'."""
        assert _sports_ref_source("weather") == "open_meteo"
