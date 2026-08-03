"""``_write_sports_fixture_venue`` full-hive shape fix (2026-08-03).

Sports's ``instrument_availability`` writer was never migrated to the R2
(2026-07-21) full-hive shape — it kept writing via the flat ``sink`` + a
``{day, venue, league}`` partition dict, which the UTL sink's alphabetical key
sort turns into ``day=/league=/venue=/...`` (missing ``pipeline_mode=``/
``asset_group=`` entirely, and the ``league=`` segment landing BEFORE
``venue=`` instead of trailing after it). Full incident writeup:
``unified-trading-pm/plans/active/issues/instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md``.

Operator ruled (2026-08-03, todo 1 of that doc) option (a): ``league=`` IS a
legitimate trailing key for sports — the writer keeps its per-league split
(no rollup to one venue-level file). Ruled target shape
(``cross-asset-canonical-target-ssot.md`` §8 sports-exception banner):
``instrument_availability/by_date/day={D}/pipeline_mode={mode}_{src}/asset_group=sports/venue={V}/league={L}/instruments.parquet``.

The fix bakes the ordered 4-key hive prefix via
``_instrument_availability_sink_for`` (mirrors ``_write_venue``) ONCE per
venue call, then passes ``league`` as a SINGLE trailing partition key per
per-league write (mirrors ``_write_prediction_venue``'s
``canonical_question_group`` handling — a single-key partition dict can't hit
the alphabetical-sort trap that a multi-key dict would).

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
    def test_single_league_writes_via_hive_sink_with_trailing_league_partition(self) -> None:
        """The write must go through `_instrument_availability_sink_for`'s
        per-shard (day/pipeline_mode/asset_group/venue) sink, not the legacy
        flat `sink` argument, with `league` as the ONLY partition key (trailing,
        after venue) — not `day`/`venue` (those are baked into the sink prefix)."""
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
        assert kwargs["partition"] == {"league": "EPL"}
        assert kwargs["filename"] == "instruments.parquet"
        assert len(kwargs["data"]) == 1

    def test_multiple_leagues_get_separate_writes_same_hive_sink(self) -> None:
        """Two in-universe leagues on the same (day, venue) each get their OWN
        write (per-league split preserved, per the operator ruling), but share
        the SAME hive sink (built once per venue, not once per league)."""
        venue_df = _venue_df(
            [
                ("EPL:ARSENAL_v_CHELSEA:20260803", "API_FOOTBALL"),
                ("LA_LIGA:REAL_MADRID_v_BARCELONA:20260803", "API_FOOTBALL"),
            ]
        )
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"
        hive_sink = MagicMock()

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
                sink=MagicMock(),
                manifest=mock_manifest,
                counts={},
                sampler=MagicMock(enable_sampling=False),
            )

        # ONE hive-sink construction (per venue call), TWO writes (per league).
        mock_sink_for.assert_called_once()
        assert mock_gated_write.call_count == 2
        written_leagues = {c.kwargs["partition"]["league"] for c in mock_gated_write.call_args_list}
        assert written_leagues == {"EPL", "LA_LIGA"}
        for call in mock_gated_write.call_args_list:
            assert call.args[0] is hive_sink
            assert len(call.kwargs["data"]) == 1
        # Manifest honest-coverage recording stays per-league — unchanged.
        assert mock_manifest.record_captured.call_count == 2

    def test_no_in_universe_rows_skips_the_write_entirely(self) -> None:
        """All-out-of-universe leagues (or an empty venue_df) must not call
        `_gated_sink_write` at all — matches the pre-fix behaviour of writing
        nothing when there's nothing to write. The hive sink is still built
        once (it no longer depends on there being any in-universe rows)."""
        venue_df = _venue_df([("UNKNOWN_LEAGUE:A_v_B:20260803", "API_FOOTBALL")])
        mock_manifest = MagicMock()
        mock_manifest.catalogue_bucket = "test-bucket"
        hive_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[],
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=False),
            patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda x: x),
            patch(
                "instruments_service.engine.orchestrator.writers._instrument_availability_sink_for",
                return_value=hive_sink,
            ),
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

        mock_gated_write.assert_not_called()
