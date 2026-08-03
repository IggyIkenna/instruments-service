"""``_write_sports_fixture_venue`` full-hive shape fix (2026-08-03).

Sports's ``instrument_availability`` writer was never migrated to the R2
(2026-07-21) full-hive shape — it kept writing via the flat ``sink`` + a
``{day, venue, league}`` partition dict, which the UTL sink's alphabetical key
sort turns into ``day=/league=/venue=/...`` (missing ``pipeline_mode=``/
``asset_group=`` entirely, and invisible to the migration tool's flat-shape
regex). Full incident writeup:
``unified-trading-pm/plans/active/issues/instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md``.

The fix bakes the ordered hive prefix via ``_instrument_availability_sink_for``
(mirrors ``_write_venue``/``_write_prediction_venue``) and writes ONE combined
``instruments.parquet`` per (day, venue) — the MTDS reader
(``instrument_availability_paths.match_instruments_blob``) only knows how to
resolve exactly one such file per (day, venue), with no trailing league
segment. League identity is NOT dropped: each row's own ``instrument_key``
(``{LEAGUE}:{HOME}_v_{AWAY}:{DATE}``) already carries it, so consolidating
multiple leagues' rows into one file loses no information — only the on-disk
per-league fan-out is removed. Manifest honest-coverage recording
(``record_captured``) stays per-league (unaffected by this fix — a separate
mechanism from the physical file layout).

``_instrument_availability_sink_for`` is called as a bare (module-local) name
inside ``writers.py`` (not via the ``_orch`` package namespace), so it must be
patched at ``instruments_service.engine.orchestrator.writers.<name>`` — patching
the package-level re-export (``instruments_service.engine.orchestrator.<name>``)
does not intercept this intra-module call. ``_gated_sink_write`` IS called via
``_orch.`` and is patched at the package level as usual.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator.writers import _write_sports_fixture_venue


def _venue_df(rows: list[tuple[str, str]]) -> pd.DataFrame:
    """Build a minimal venue_df: rows of (instrument_key, venue)."""
    return pd.DataFrame(
        {
            "instrument_key": [r[0] for r in rows],
            "venue": [r[1] for r in rows],
        }
    )


class TestWriteSportsFixtureVenueFullHiveShape:
    def test_single_league_writes_via_hive_sink_not_flat_sink(self) -> None:
        """The write must go through `_instrument_availability_sink_for`'s
        per-shard sink, not the legacy flat `sink` argument, and must NOT carry
        `day`/`venue`/`league` in the partition dict (the alphabetical-sort trap)."""
        venue_df = _venue_df([("EPL:ARSENAL_v_CHELSEA:20260803", "API_FOOTBALL")])
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"
        flat_sink = MagicMock()
        hive_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[MagicMock(league_id="EPL")],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["x"]),
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
            patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda x: x),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, when: df,
            ),
            patch(
                "instruments_service.engine.orchestrator.writers._instrument_availability_sink_for",
                return_value=hive_sink,
            ) as mock_sink_for,
            patch("instruments_service.engine.orchestrator._gated_sink_write") as mock_gated_write,
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date="2026-08-03",
                bucket="instruments-store-sports-prd",
                league_filter=None,
                sink=flat_sink,
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        mock_sink_for.assert_called_once_with(
            "instruments-store-sports-prd",
            date="2026-08-03",
            pipeline_mode="batch_api_football",
            asset_group="sports",
            venue="API_FOOTBALL",
        )
        assert mock_gated_write.call_count == 1
        args, kwargs = mock_gated_write.call_args
        assert args[0] is hive_sink
        assert kwargs["partition"] == {}
        assert kwargs["filename"] == "instruments.parquet"
        assert len(kwargs["data"]) == 1

    def test_multiple_leagues_are_combined_into_one_write_call(self) -> None:
        """Two in-universe leagues on the same (day, venue) must land in ONE
        combined parquet write, not two separate per-league writes — the ruled
        grain is (day, pipeline_mode, asset_group, venue), no finer split."""
        venue_df = _venue_df(
            [
                ("EPL:ARSENAL_v_CHELSEA:20260803", "API_FOOTBALL"),
                ("LA_LIGA:REAL_MADRID_v_BARCELONA:20260803", "API_FOOTBALL"),
            ]
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
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
            patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda x: x),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, when: df,
            ),
            patch("instruments_service.engine.orchestrator.writers._instrument_availability_sink_for"),
            patch("instruments_service.engine.orchestrator._gated_sink_write") as mock_gated_write,
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date="2026-08-03",
                bucket="instruments-store-sports-prd",
                league_filter=None,
                sink=MagicMock(),
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        # ONE parquet write covering both leagues' rows (league survives in
        # each row's own instrument_key — no data lost by not path-splitting).
        assert mock_gated_write.call_count == 1
        _, kwargs = mock_gated_write.call_args
        written_df = kwargs["data"]
        assert len(written_df) == 2
        assert set(written_df["instrument_key"].str.split(":").str[0]) == {"EPL", "LA_LIGA"}
        # Manifest honest-coverage recording stays per-league — unaffected by
        # the physical-file consolidation.
        assert mock_manifest.record_captured.call_count == 2

    def test_no_in_universe_rows_skips_the_write_entirely(self) -> None:
        """All-out-of-universe leagues (or an empty venue_df) must not call
        the hive sink at all — matches the pre-fix behaviour of writing
        nothing when there's nothing to write."""
        venue_df = _venue_df([("UNKNOWN_LEAGUE:A_v_B:20260803", "API_FOOTBALL")])
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=False),
            patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda x: x),
            patch(
                "instruments_service.engine.orchestrator.writers._instrument_availability_sink_for"
            ) as mock_sink_for,
            patch("instruments_service.engine.orchestrator._gated_sink_write") as mock_gated_write,
        ):
            _write_sports_fixture_venue(
                venue_str="API_FOOTBALL",
                venue_df=venue_df,
                date="2026-08-03",
                bucket="instruments-store-sports-prd",
                league_filter=None,
                sink=MagicMock(),
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        mock_sink_for.assert_not_called()
        mock_gated_write.assert_not_called()
