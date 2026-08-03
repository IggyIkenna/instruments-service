"""``process_write``'s FIXTURES oscillation guard (extends `ba306543` to the
batch-capture emission path).

``sports_index_recency_masked_captured_atoms_2026_07_13.md`` § "ROOT CAUSE
CORRECTED": the ``enumerate_v2`` seeder's ``captured_set`` guard
(``instruments-service@ba306543``) never covers ``uts-prod-instruments-
service-sports-fixtures`` (``--operation=instruments --mode=batch
--asset-group=SPORTS``), a SEPARATE emission site in
``process_write._write_sports_fixture_venue``. That loop previously only
consulted THIS RUN's captured leagues (``_captured_lids``) — a run that
legitimately returned zero fixtures for an already-captured league would
stamp ``empty_confirmed`` over it, masking the prior capture on read (no
consolidator dedup collapse across differing service_name/source identity).

Covers:
* ``_manifest_captured_leagues_for_data_type`` — the manifest-read guard
  helper (filters to the requested date + data_type + capture_status ==
  captured; empty index → empty set; read failure → ``None`` fail-safe).
  Generalized from FIXTURES_SCHEDULE-only (formerly
  ``_manifest_captured_fixture_leagues``) so ``emit_empty_gaps_for_entity``
  can reuse it for TEAMS/STANDINGS/INJURIES — see
  ``test_sports_reference_core_manifest_index_guard.py``.
* ``_write_sports_fixture_venue`` — a league already captured in the
  manifest (but not this run) must NOT get ``record_empty``; a genuinely
  absent league still does; a manifest-read failure skips the whole
  empty-emission pass rather than risk masking.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator.process_write import _write_sports_fixture_venue
from instruments_service.engine.orchestrator.sports_reference_core import _manifest_captured_leagues_for_data_type


class TestManifestCapturedFixtureLeagues:
    def test_returns_canonical_captured_league_ids_for_the_date(self) -> None:
        idx = pd.DataFrame(
            {
                "date": ["2026-07-22", "2026-07-22", "2026-07-22", "2026-07-21"],
                "data_type": ["FIXTURES_SCHEDULE", "FIXTURES_SCHEDULE", "FIXTURE_STATS", "FIXTURES_SCHEDULE"],
                "league_id": ["BRASILEIRAO", "EPL", "BRASILEIRAO", "BRASILEIRAO"],
                "capture_status": ["captured", "empty_confirmed", "captured", "captured"],
            }
        )
        with patch("instruments_service.engine.orchestrator.read_availability_index", return_value=idx):
            result = _manifest_captured_leagues_for_data_type(
                bucket="test-bucket", date="2026-07-22", data_type="FIXTURES_SCHEDULE"
            )
        # EPL is empty_confirmed (excluded); the FIXTURE_STATS + prior-date rows
        # are a different data_type / date (excluded) — only the one qualifying
        # captured FIXTURES_SCHEDULE/2026-07-22 row survives.
        assert result == {"BRASILEIRAO"}

    def test_empty_index_returns_empty_set(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=pd.DataFrame(),
        ):
            assert (
                _manifest_captured_leagues_for_data_type(
                    bucket="test-bucket", date="2026-07-22", data_type="FIXTURES_SCHEDULE"
                )
                == set()
            )

    def test_read_failure_returns_none_fail_safe(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            side_effect=RuntimeError("gcs unavailable"),
        ):
            assert (
                _manifest_captured_leagues_for_data_type(
                    bucket="test-bucket", date="2026-07-22", data_type="FIXTURES_SCHEDULE"
                )
                is None
            )


class TestWriteSportsFixtureVenueOscillationGuard:
    def test_manifest_captured_league_is_not_re_emitted_empty(self) -> None:
        """A league with NO fixtures captured this run, but a captured manifest
        row from a PRIOR run, must be excluded from the empty-gap loop."""
        _date = "2026-07-22"
        venue_df = pd.DataFrame({"instrument_key": pd.Series([], dtype=str), "venue": pd.Series([], dtype=str)})
        manifest_index = pd.DataFrame(
            {
                "date": [_date],
                "data_type": ["FIXTURES_SCHEDULE"],
                "league_id": ["BRASILEIRAO"],
                "capture_status": ["captured"],
            }
        )
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="BRASILEIRAO"), MagicMock(league_id="LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=manifest_index),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date=_date,
                league_filter=None,
                bucket="test-bucket",
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        emitted_leagues = {c.kwargs["row_key"]["league_id"] for c in mock_manifest.record_empty.call_args_list}
        assert emitted_leagues == {"LA_LIGA"}, "BRASILEIRAO must be guarded (manifest already shows it captured)"

    def test_this_run_captured_league_still_excluded(self) -> None:
        """Regression: a league captured THIS run (pre-existing _captured_lids
        guard) stays excluded once the manifest guard is unioned in."""
        _date = "2026-07-22"
        venue_df = pd.DataFrame(
            {
                "instrument_key": ["EPL:ARSENAL_v_CHELSEA:20260722"],
                "venue": ["API_FOOTBALL"],
            }
        )
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="EPL"), MagicMock(league_id="LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
            # _write_sports_fixture_venue now builds its own hive sink internally
            # (instruments-service@ba87cc32) rather than taking an injected sink=
            # param — without this mock the captured-league branch below constructs
            # a REAL GCSStorageClient, which reaches out to the GCE metadata server
            # and only fails in CI (blocked by pytest-socket; passes locally where
            # ambient GOOGLE_APPLICATION_CREDENTIALS masks the gap).
            patch(
                "instruments_service.engine.orchestrator._instrument_availability_sink_for", return_value=MagicMock()
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, when: df,
            ),
            # Avoid polluting the real _is_in_canonical_write_universe's
            # module-level lru_cache with this test's fake league mocks —
            # that cache is process-global and would leak into unrelated
            # tests run later in the same session.
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date=_date,
                league_filter=None,
                bucket="test-bucket",
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        emitted_leagues = {c.kwargs["row_key"]["league_id"] for c in mock_manifest.record_empty.call_args_list}
        assert emitted_leagues == {"LA_LIGA"}
        assert mock_manifest.record_captured.call_count == 1

    def test_captured_league_writes_full_hive_sink_with_trailing_league(self) -> None:
        """Regression (todo 2 of instrument_availability_hive_migration_unrecognized_shapes_and_
        content_mismatch_2026_08_03.md): the writer must build its sink via
        ``_instrument_availability_sink_for`` (full canonical hive prefix — day/pipeline_mode/
        asset_group/venue baked in) and pass ONLY ``league=`` through the write() partition dict,
        never the old flat ``{day, venue, league}`` partition (which sorted alphabetically to the
        non-canonical ``day=/league=/venue=`` shape confirmed live 2026-08-02)."""
        _date = "2026-08-02"
        venue_df = pd.DataFrame(
            {
                "instrument_key": ["ARGENTINA_PRIMERA_NACIONAL:BOCA_v_RIVER:20260802"],
                "venue": ["API_FOOTBALL"],
            }
        )
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"
        mock_hive_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="ARGENTINA_PRIMERA_NACIONAL")],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
            patch(
                "instruments_service.engine.orchestrator._instrument_availability_sink_for",
                return_value=mock_hive_sink,
            ) as mock_sink_for,
            patch("instruments_service.engine.orchestrator._gated_sink_write") as mock_write,
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, when: df,
            ),
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date=_date,
                league_filter=None,
                bucket="instruments-store-sports-prd",
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        mock_sink_for.assert_called_once_with(
            "instruments-store-sports-prd",
            date=_date,
            pipeline_mode="batch_api_football",
            asset_group="sports",
            venue="API_FOOTBALL",
        )
        mock_write.assert_called_once()
        assert mock_write.call_args.args[0] is mock_hive_sink
        assert mock_write.call_args.kwargs["partition"] == {"league": "ARGENTINA_PRIMERA_NACIONAL"}
        assert mock_write.call_args.kwargs["filename"] == "instruments.parquet"

    def test_manifest_read_failure_skips_empty_emission_entirely(self) -> None:
        """Fail-safe: a manifest-read error must not risk masking — no
        record_empty calls at all, rather than treating absence-of-proof as
        proof-of-absence."""
        _date = "2026-07-22"
        venue_df = pd.DataFrame({"instrument_key": pd.Series([], dtype=str), "venue": pd.Series([], dtype=str)})
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="LA_LIGA")],
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                side_effect=RuntimeError("gcs unavailable"),
            ),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date=_date,
                league_filter=None,
                bucket="test-bucket",
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        assert mock_manifest.record_empty.call_count == 0
