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

import contextlib
import io
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import (
    _ENTITY_NAME_TO_PIPELINE_MODE,
    _read_per_league_entity_df,
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


# ---------------------------------------------------------------------------
# _read_per_league_entity_df
# ---------------------------------------------------------------------------


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


class TestReadPerLeagueEntityDf:
    """Tests for the shared canonical-then-legacy per-league parquet reader.

    Regression coverage for the 2026-07-08 stale-path fix: FIXTURES (and other
    sports_reference entities) are written per-league under a canonical
    ``pipeline_mode=`` hive segment
    (``entity={entity}/league={L}/{entity}.parquet``) — a single bare-blob
    probe at ``entity={entity}/{entity}.parquet`` (with or without
    ``pipeline_mode=``) never finds this data. These tests assert the reader
    lists the canonical PREFIX first (so it finds per-league objects), only
    falls back to the legacy prefix when the canonical prefix is empty, and
    does not silently swallow read/transport errors (callers that need to
    distinguish "no data" from "read failed" rely on the exception
    propagating).
    """

    def test_canonical_prefix_hit_skips_legacy_listing(self) -> None:
        """Canonical (pipeline_mode=) prefix returning blobs must short-circuit — no legacy call."""
        df = pd.DataFrame({"af_fixture_id": [1, 2], "venue_name": ["Anfield", "Anfield"]})
        mock_blob = MagicMock()
        mock_blob.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=EPL/fixtures.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = _parquet_bytes(df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is not None
        assert len(result) == 2
        # Only ONE list_blobs call — the canonical prefix already had data.
        assert mock_storage.list_blobs.call_count == 1
        called_prefix = mock_storage.list_blobs.call_args.kwargs["prefix"]
        assert "pipeline_mode=batch_api_football" in called_prefix
        assert (
            called_prefix == "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/"
        )

    def test_legacy_fallback_when_canonical_prefix_empty(self) -> None:
        """Canonical prefix returns nothing → falls back to the legacy (no pipeline_mode=) prefix."""
        df = pd.DataFrame({"af_fixture_id": [1], "venue_name": ["Anfield"]})
        mock_blob = MagicMock()
        mock_blob.name = "sports_reference/by_date/day=2026-07-04/entity=fixtures/league=EPL/fixtures.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = [[], [mock_blob]]
        mock_storage.download_bytes.return_value = _parquet_bytes(df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is not None
        assert len(result) == 1
        assert mock_storage.list_blobs.call_count == 2
        first_prefix = mock_storage.list_blobs.call_args_list[0].kwargs["prefix"]
        second_prefix = mock_storage.list_blobs.call_args_list[1].kwargs["prefix"]
        assert "pipeline_mode=" in first_prefix
        assert "pipeline_mode=" not in second_prefix
        assert second_prefix == "sports_reference/by_date/day=2026-07-04/entity=fixtures/"

    def test_returns_none_when_no_blobs_under_either_prefix(self) -> None:
        """Both prefixes empty → None (the legacy-bare-path stale-bug's original symptom)."""
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is None

    def test_non_parquet_blobs_ignored(self) -> None:
        """Blobs under the prefix that aren't .parquet are filtered out."""
        mock_marker_blob = MagicMock()
        mock_marker_blob.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=EPL/_SUCCESS"
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_marker_blob]

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is None
        mock_storage.download_bytes.assert_not_called()

    def test_concatenates_multiple_per_league_blobs(self) -> None:
        """Multiple per-league blobs under the same prefix are concatenated into one frame."""
        df_epl = pd.DataFrame({"af_fixture_id": [1], "league_id": ["EPL"]})
        df_bun = pd.DataFrame({"af_fixture_id": [2], "league_id": ["BUNDESLIGA"]})
        blob_epl = MagicMock()
        blob_epl.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=EPL/fixtures.parquet"
        blob_bun = MagicMock()
        blob_bun.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=BUNDESLIGA/fixtures.parquet"

        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [blob_epl, blob_bun]
        mock_storage.download_bytes.side_effect = [_parquet_bytes(df_epl), _parquet_bytes(df_bun)]

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is not None
        assert len(result) == 2
        assert set(result["league_id"]) == {"EPL", "BUNDESLIGA"}

    def test_league_id_injected_from_path_when_requested(self) -> None:
        """inject_league_id=True populates a missing league_id column from the blob's league= segment."""
        df = pd.DataFrame({"af_fixture_id": [1], "venue_name": ["Anfield"]})
        mock_blob = MagicMock()
        mock_blob.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=EPL/fixtures.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = _parquet_bytes(df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures", inject_league_id=True)

        assert result is not None
        assert "league_id" in result.columns
        assert result["league_id"].iloc[0] == "EPL"

    def test_league_id_not_injected_by_default(self) -> None:
        """inject_league_id defaults to False — no league_id column added unless requested."""
        df = pd.DataFrame({"af_fixture_id": [1], "venue_name": ["Anfield"]})
        mock_blob = MagicMock()
        mock_blob.name = "sports_reference/by_date/day=2026-07-04/pipeline_mode=batch_api_football/entity=fixtures/league=EPL/fixtures.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = _parquet_bytes(df)

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            result = _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")

        assert result is not None
        assert "league_id" not in result.columns

    def test_transport_error_propagates_not_swallowed(self) -> None:
        """Unlike most read-helpers in this module, errors are NOT swallowed here.

        Callers that must distinguish "no data" (record_empty) from "read
        failed" (record_failed) — e.g. ``_fetch_weather_data`` — rely on this
        so a real GCS outage isn't silently mis-recorded as honest absence.
        """
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = RuntimeError("GCS unreachable")

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            pytest.raises(RuntimeError, match="GCS unreachable"),
        ):
            _read_per_league_entity_df("bucket", "2026-07-04", "fixtures")


# ---------------------------------------------------------------------------
# _ensure_canonical_fixtures_for_override
# ---------------------------------------------------------------------------


class TestEnsureCanonicalFixturesForOverride:
    """Regression for the 2026-07-08 stale-bare-path cost bug.

    ``_ensure_canonical_fixtures_for_override`` decides whether canonical
    fixtures still need writing by checking for existing data. Before the
    fix, that check probed a single bare
    ``entity=fixtures/fixtures.parquet`` blob that no writer has populated
    since the per-league migration — so the check always found nothing and
    this function ALWAYS fell through to the old-path/API-fetch branch
    (wasting 33 api-football calls per date) even when real per-league
    fixtures were already captured. It now uses
    ``_read_per_league_entity_df`` (canonical-then-legacy per-league prefix
    listing) instead of a single bare-blob ``.exists()`` probe.
    """

    @pytest.mark.asyncio
    async def test_skips_refetch_when_per_league_fixtures_already_exist(self) -> None:
        """Real per-league data present → no old-path read, no API adapter call."""
        from instruments_service.engine.orchestrator.sports_reference_fixtures import (
            _ensure_canonical_fixtures_for_override,
        )

        existing_df = pd.DataFrame({"af_fixture_id": [1, 2], "timestamp": [1700000000, 1700003600]})
        mock_storage = MagicMock()
        mock_create_adapter = MagicMock()

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage)
            )
            stack.enter_context(
                patch("instruments_service.engine.orchestrator._read_per_league_entity_df", return_value=existing_df)
            )
            stack.enter_context(
                patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", mock_create_adapter)
            )
            stack.enter_context(patch("instruments_service.engine.orchestrator._write_fixtures_per_league"))
            await _ensure_canonical_fixtures_for_override(date="2026-07-04", bucket="bucket", api_key="key")

        # Real per-league data already exists — must NOT re-fetch from the API
        # (the pre-fix bug re-fetched every time regardless).
        mock_create_adapter.assert_not_called()
        # And must not probe the old (pre-by_date-restructure) legacy path either.
        mock_storage.bucket.return_value.blob.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_from_api_when_no_per_league_fixtures_exist(self) -> None:
        """No real per-league data (genuine gap) → old-path fallback still runs, still fetches."""
        from instruments_service.engine.orchestrator.sports_reference_fixtures import (
            _ensure_canonical_fixtures_for_override,
        )

        mock_old_blob = MagicMock()
        mock_old_blob.exists.return_value = False
        mock_storage = MagicMock()
        mock_storage.bucket.return_value.blob.return_value = mock_old_blob

        mock_adapter = MagicMock()
        mock_adapter.get_fixtures_with_raw = AsyncMock(return_value=[])
        mock_create_adapter = MagicMock(return_value=mock_adapter)

        with contextlib.ExitStack() as stack:
            stack.enter_context(
                patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage)
            )
            stack.enter_context(
                patch("instruments_service.engine.orchestrator._read_per_league_entity_df", return_value=None)
            )
            stack.enter_context(
                patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", mock_create_adapter)
            )
            await _ensure_canonical_fixtures_for_override(date="2026-07-04", bucket="bucket", api_key="key")

        # Genuinely no per-league data → the old-path probe + API fallback still engage.
        mock_storage.bucket.return_value.blob.assert_called_once()
        mock_create_adapter.assert_called_once()
        mock_adapter.get_fixtures_with_raw.assert_awaited_once_with("2026-07-04")
