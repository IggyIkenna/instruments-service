"""Per-fixture entity WRITE stage: DataFrame prep + per-league parquet/manifest writes.

Cohesion module of the ``engine.orchestrator`` package. Carries the write-side
of the per-fixture enrichment pipeline — split out of
``sports_reference_fixtures.py`` (which regrew past the 900-line
``MAX_FILE_LINES`` ratchet a second time; see
``unified-trading-pm/plans/active/issues/qg_size_gate_sentinel_skip_root_cause_2026_07_25.md``)
to keep both files under the size gate. Pure behaviour-preserving extraction —
same ``codex_violations_ratchet_to_five_2026_06_10.md`` decomposition plan as
the rest of the ``engine.orchestrator`` package split.

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
    from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_entity_dt_for_short",
    "_write_per_fixture_entities",
]

# Per-fixture entity short-name → canonical manifest data_type. SSOT for the
# entity-axis key used by both the observed-coverage skip (in
# ``sports_reference_fixtures._gather_per_fixture_rows``) and the per-league
# manifest writes below, so the two never drift.
_ENTITY_DT_BY_SHORT: dict[str, str] = {
    "fixture_stats": "FIXTURE_STATS",
    "fixture_events": "FIXTURE_EVENTS",
    "fixture_lineups": "FIXTURE_LINEUPS",
    "player_stats": "PLAYER_STATS",
}


def _entity_dt_for_short(entity_name: str) -> str:
    """Canonical manifest data_type for a per-fixture entity short-name (``""`` if unknown)."""
    return _ENTITY_DT_BY_SHORT.get(entity_name, "")


def _prepare_fixture_entity_df(
    entity_name: str, all_rows: list[dict[str, object]], date: str, counts: dict[str, int]
) -> _orch.pd.DataFrame:
    """Build + clean the per-fixture entity DataFrame: drop unparquetable nested
    columns, stamp the PIT ``available_at`` approximation, and record the row count."""
    df = _orch.pd.DataFrame(all_rows)

    # Drop columns containing nested structures (lists/dicts) that
    # cannot be serialised to Parquet.  API Football player_stats
    # responses may carry a raw "statistics" column with nested
    # dicts even after normalisation.
    _nested_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, (dict, list))).any()]
    if _nested_cols:
        _orch.logger.info(
            "Dropping %d nested columns from %s: %s",
            len(_nested_cols),
            entity_name,
            _nested_cols,
        )
        df = df.drop(columns=_nested_cols)
    if entity_name == "player_stats":
        df = _orch._dedupe_player_stats_df(df)
    # PIT safety: per-fixture stats/events/lineups/player_stats available ~2h after kickoff.
    # No per-row kickoff here — approximate using date + 17:00 UTC (15:00 typical KO + 2h).
    df["available_at"] = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=17)

    counts[entity_name] = len(df)
    return df


def _write_fixture_entity_per_league(
    df: _orch.pd.DataFrame,
    *,
    entity_name: str,
    af_entity_dt: str,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    af_fid_to_league: dict[str, str],
    pre_captured_leagues: dict[str, set[str]],
    recovery_fixture_ids: frozenset[int] | None,
) -> None:
    """Write per-league partitioned files for a non-empty per-fixture entity using
    the AF fixture_id -> league mapping, then emit empty-gap rows for any expected
    league that captured nothing this run."""
    manifest = hooks.manifest
    # Write per-league partitioned files using AF fixture_id -> league mapping.
    # Column name is "af_fixture_id" (not "fixture_id") in per-fixture entity data.
    _fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
    if _fid_col in df.columns and af_fid_to_league:
        # Ensure string type for map lookup (map keys are str(int(af_id)))
        df["_league_id"] = df[_fid_col].astype(str).str.split(".").str[0].map(af_fid_to_league)
        _has_league = df["_league_id"].notna()
        _with_league = df[_has_league]
        _without_league = df[~_has_league]
        _pf_captured: set[str] = set()

        for _pf_lid, _pf_league_df in _with_league.groupby("_league_id"):
            _pf_lid_str = str(_pf_lid)
            _pf_canon = _orch._canonical_league_id(_pf_lid_str)
            # WRITE-UNIVERSE gate: fixtures roll-up spans the whole api_football
            # universe; only write per-fixture captures for tracked leagues so
            # out-of-universe (numeric-keyed) leagues don't pollute the manifest
            # with a second schema (incident 2026-06-24).
            if not _orch._is_in_canonical_write_universe(_pf_canon):
                continue
            _pf_captured.add(_pf_canon)
            _pf_clean = _pf_league_df.drop(columns=["_league_id"])

            # Recovery mode: read existing per-league parquet (if
            # any) and merge our newly-fetched fixture rows so we
            # don't lose previously-captured fixtures' data. The
            # standard write path is overwrite-on-write, which is
            # safe when we always fetch ALL fixtures for the
            # (date, league) cell — but in recovery mode we only
            # fetched a subset, so a plain overwrite would drop
            # the rest of the cell.
            if recovery_fixture_ids is not None and _fid_col in _pf_clean.columns:
                _pf_clean = _orch._merge_with_existing_per_league_parquet(
                    bucket=bucket,
                    date=date,
                    entity_name=entity_name,
                    canonical_league_id=_orch._canonical_league_id(_pf_lid_str),
                    new_rows=_pf_clean,
                    fid_col=_fid_col,
                )
            if entity_name == "fixture_events":
                _pf_clean = _orch._normalize_fixture_events_schema(_pf_clean)
            # C.6: available_at = date + 17h already set on df above (KO + 2h
            # approximation). Preserve it; fillna wall-clock for any NaT rows (defensive).
            _pf_copy = _pf_clean.copy()
            _pf_copy["available_at"] = _pf_copy["available_at"].fillna(
                _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
            )
            _stamped_pf_df = _pf_copy
            _orch._gated_sink_write(
                _orch._sports_ref_sink_for(bucket, date, entity_name),
                data=_stamped_pf_df,
                partition={"entity": entity_name, "league": _orch._canonical_league_id(_pf_lid_str)},
                filename=f"{entity_name}.parquet",
                venue="api_football",
                entity=entity_name,
            )
            if manifest is not None:
                manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                    row_key={
                        "date": date,
                        "data_type": af_entity_dt,
                        "league_id": _orch._canonical_league_id(_pf_lid_str),
                    },
                    df=_stamped_pf_df,
                    asset_group="sports",
                    instrument_type="",
                    data_type=af_entity_dt,
                    league_id=_orch._canonical_league_id(_pf_lid_str),
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    source=_orch._sports_ref_source(entity_name),
                    service_emission_state=None,
                )

        # Out-of-universe drop (NOT a capture gap) — unmapped
        # per-fixture rows belong to fixtures OUTSIDE the canonical
        # write universe, so they are skipped silently, mirroring the
        # mapped-branch ``_is_in_canonical_write_universe`` ``continue``
        # above. Why they occur: the enrichment TARGET (URDI's
        # ``get_fixtures(date=...)`` — no league filter, see
        # ``api_football_reference.py::get_instruments``) spans the
        # ENTIRE api_football league universe (~1000+ leagues), while
        # ``af_fid_to_league`` is built ONLY from the canonical-94 GCS
        # fixtures map (``_build_fixture_league_map_from_gcs`` over
        # ``get_expected_leagues_for_source``). A completed fixture in
        # one of the 1,438 non-canonical leagues purged by
        # ``delete_noncanonical_sports_leagues_2026_06_25.py`` therefore
        # gets enriched but can never map.
        #
        # Recording these as ``record_failed(LEAGUE_MAP_INCOMPLETE)``
        # was wrong twice over: (1) it mis-classified deliberately-
        # untracked leagues as ``attempted_failed`` — a perpetual
        # backfill target that can never resolve (the league is out of
        # universe by design); and (2) the blank-league
        # ``row_key={date,data_type}`` aggregate can NEVER be superseded
        # by any per-league capture (the same unsupersedable-row defect
        # already fixed for the zero-rows branch below —
        # ``api_football_per_fixture_blank_league_orphan_2026_07_15``),
        # so it sat ``attempted_failed`` forever. The 2026-07-14 GW
        # ``record_failed`` made sense only while the map was too NARROW
        # (built from the 33-league ``get_prediction_leagues()``, so
        # ~65% of IN-universe fixtures fell through); once the map was
        # widened to the full 94 (see ``_build_fixture_league_map_from_gcs``
        # addendum) the residual unmapped rows are exclusively
        # out-of-universe. Determination:
        # ``sports_data_sources_canonical_completion`` (2026-07-16) — the
        # 362 residual ``LEAGUE_MAP_INCOMPLETE`` rows were ALL
        # out-of-universe fixtures, not a registry gap.
        #
        # Honest-absence guard preserved: a genuine IN-universe capture
        # gap (map build fails / fixtures parquet incomplete) surfaces
        # on the FIXTURES shard itself (tracked separately); the WARNING
        # below keeps any such regression visible in logs without
        # minting an unsupersedable failure row here.
        if not _without_league.empty:
            _orch.logger.warning(
                "%s date=%s: %d per-fixture rows are out-of-universe "
                "(fixture league not in the canonical write universe) — skipping. "
                "Not a capture gap; genuine in-universe gaps surface on the FIXTURES shard.",
                af_entity_dt,
                date,
                len(_without_league),
            )
        if manifest is not None:
            hooks.emit_empty_gaps_for_entity(af_entity_dt, _pf_captured | pre_captured_leagues.get(entity_name, set()))
    else:
        # Single-SSOT: bare manifest row + bare parquet write are
        # both suppressed; writing one would create a phantom
        # captured shard with no parquet on disk.  Surface the
        # upstream regression in logs so it can be diagnosed.
        _orch.logger.warning(
            "%s bare-path fallback triggered for date=%s — data shape regression: "
            "no fixture-id column or empty af_fid->league map (rows=%d). "
            "Skipping bare write + manifest row to keep manifest honest.",
            af_entity_dt,
            date,
            len(df),
        )

    _orch.logger.info("Sports reference: %d %s rows written", len(df), entity_name)


def _handle_empty_fixture_entity(
    *,
    entity_name: str,
    af_entity_dt: str,
    date: str,
    hooks: _AfManifestHooks,
    entity_failures: dict[str, tuple[int, str]],
    pre_captured_leagues: dict[str, set[str]],
    af_fid_to_league: dict[str, str],
) -> None:
    """Handle a per-fixture entity that produced zero rows this run — distinguish
    fetch failure (record_failed -> attempted_failed) from a legitimate empty
    (record_empty -> empty_confirmed).

    CF-11 fix (2026-06-02): the original guard only routed to record_failed when
    EVERY fixture call raised. A partial failure fell through to
    emit_empty_gaps_for_entity → empty_confirmed(EXPECTED_NO_FIXTURE), falsely
    claiming "nothing to capture" and freezing the gap forever. Correct rule: ANY
    failure → record_failed so the shard is flagged for backfill.

    Root-cause fix (api_football_per_fixture_blank_league_orphan_2026_07_15, see
    plans/active/sports_data_sources_canonical_completion_2026_07_13.md): this
    branch previously wrote ONE blank-league_id date-aggregate ``record_failed``
    row — a row_key no per-league success path can ever supersede, so it sat
    ``attempted_failed`` forever even after a clean re-fetch (live-verified
    2026-07-15: 15 blank rows across
    PLAYER_STATS/FIXTURE_STATS/FIXTURE_EVENTS/FIXTURE_LINEUPS). Write per-league
    instead, using ``af_fid_to_league``.
    """
    manifest = hooks.manifest
    _fail_count, _err_code = entity_failures.get(entity_name, (0, ""))
    if _fail_count > 0 and _err_code:
        # At least one fixture call raised → treat the entity as
        # attempted_failed so it is backfilled.  The error code
        # from the first failure is representative; per-fixture
        # errors are already emitted individually by _fetch_one.
        if manifest is not None:
            _affected_leagues = {_orch._canonical_league_id(str(_lid)) for _lid in af_fid_to_league.values() if _lid}
            if _affected_leagues:
                for _lid in sorted(_affected_leagues):
                    manifest.record_failed(
                        row_key={"date": date, "data_type": af_entity_dt, "league_id": _lid},
                        error=_err_code,
                        attempted_at=hooks.attempt_ts,
                        pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    )
            else:
                # No league mapping at all this date (implies FIXTURES
                # resolution itself failed) — extremely rare, since
                # _fail_count>0 requires at least one queued per-fixture
                # task, which requires a non-empty af_fid_to_league.
                # Fall back to the date-aggregate row rather than lose
                # the failure signal entirely.
                manifest.record_failed(
                    row_key={"date": date, "data_type": af_entity_dt},
                    error=_err_code,
                    attempted_at=hooks.attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                )
    else:
        # All calls succeeded but returned zero rows (e.g.
        # post-match stats not yet published, lineups not
        # disclosed for low-profile fixture) — legitimate empty.
        # OR every fixture for this entity was skip-as-already-
        # captured (zero tasks queued at all) — union in the
        # pre-captured leagues so a no-op run cannot demote them
        # to empty (same false-ENF bug as the ``if all_rows``
        # branch above, but hit when literally nothing was
        # fetched this run for the entity).
        hooks.emit_empty_gaps_for_entity(af_entity_dt, pre_captured_leagues.get(entity_name, set()))


def _write_per_fixture_entities(
    *,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
    entity_names: list[str],
    entity_rows: dict[str, list[dict[str, object]]],
    entity_failures: dict[str, tuple[int, str]],
    pre_captured_leagues: dict[str, set[str]],
    af_fid_to_league: dict[str, str],
    recovery_fixture_ids: frozenset[int] | None,
) -> None:
    """Write per-league partitioned per-fixture entity files + manifest rows."""
    for entity_name in entity_names:
        _af_entity_dt = _entity_dt_for_short(entity_name)
        all_rows = entity_rows[entity_name]
        if all_rows:
            df = _prepare_fixture_entity_df(entity_name, all_rows, date, counts)
            _write_fixture_entity_per_league(
                df,
                entity_name=entity_name,
                af_entity_dt=_af_entity_dt,
                date=date,
                bucket=bucket,
                hooks=hooks,
                af_fid_to_league=af_fid_to_league,
                pre_captured_leagues=pre_captured_leagues,
                recovery_fixture_ids=recovery_fixture_ids,
            )
        else:
            _handle_empty_fixture_entity(
                entity_name=entity_name,
                af_entity_dt=_af_entity_dt,
                date=date,
                hooks=hooks,
                entity_failures=entity_failures,
                pre_captured_leagues=pre_captured_leagues,
                af_fid_to_league=af_fid_to_league,
            )
