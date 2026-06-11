"""_fetch_sports_reference_data — sports reference entity fetch orchestration.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

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
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_fetch_sports_reference_data",
]


async def _fetch_sports_reference_data(
    date: str,
    api_key: str,
    bucket: str,
    entities_to_fetch: list[str] | None = None,
    enrichment_only: bool = False,
    fixture_ids_override: list[int] | None = None,
    manifest: _orch.ManifestWriter | None = None,
    recovery_fixture_ids: frozenset[int] | None = None,
    redo_all: bool = False,
) -> dict[str, int]:
    """Fetch sports reference data (teams, leagues, standings, injuries, etc.).

    Calls USRI api_football adapter for enrichment data that describes the
    instruments (fixtures). Written to the same bucket as instruments under
    separate hive-partitioned prefixes:
        sports_reference/by_date/day={date}/entity={type}/{type}.parquet
        sports_reference/mappings/team_mapping.parquet
        sports_reference/mappings/fixture_mapping.parquet

    This data is slow-moving (leagues/teams change per season, not per day)
    but we re-fetch on each run to capture mid-season transfers, promotions,
    and new referee assignments.

    Args:
        entities_to_fetch: Specific manifest entity names to fetch (e.g.
            ["FIXTURE_LINEUPS", "PLAYER_STATS"]).
            When provided, only these entities are fetched — all others skipped.
            None = fetch everything (legacy behaviour).
        enrichment_only: If True, skip core entities. Superseded by entities_to_fetch.
        fixture_ids_override: Pre-computed list of completed fixture IDs from the
            URDI instruments fetch or GCS lookup. When provided, skips the expensive
            33-league re-fetch (saves 33 API calls per date).
        recovery_fixture_ids: af_fixture_id allowlist for targeted per-fixture
            recovery. When set, the per-fixture entity loop (PLAYER_STATS /
            FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS) intersects
            ``fixture_ids`` with this set, and the per-league parquet writes
            do read-modify-write merges so existing fixtures' rows are
            preserved.
    """
    # Convert entities_to_fetch to a set of short entity names for easy lookup.
    # E.g. "FIXTURE_LINEUPS" → "fixture_lineups"
    _fetch_set: set[str] | None = None
    if entities_to_fetch:
        _fetch_set = set()
        for e in entities_to_fetch:
            short = e.replace("API_FOOTBALL_", "").lower()
            _fetch_set.add(short)
        # If only per-fixture entities requested, set enrichment_only
        core_shorts = {"leagues", "teams", "standings", "injuries"}
        if not (_fetch_set & core_shorts):
            enrichment_only = True
    adapter = _orch.create_sports_reference_adapter("api_football", api_key=api_key)
    # v9 canonical: entity-specific sinks embed pipeline_mode= in prefix.
    # _sports_ref_sink_for() creates the right sink per entity_name so
    # DataSink's alphabetic partition sort produces the correct path order.
    counts: dict[str, int] = {}

    # Honest-coverage helper: only record when an external manifest is wired
    # in by the caller (existing call-sites always pass one, but the default
    # signature keeps it optional for legacy use).
    _af_attempt_ts = _orch.datetime.now(_orch.UTC)

    def _af_record_failed(data_type: str, exc: Exception, league_id: str = "") -> None:
        if manifest is None:
            return
        _err_code = _orch._classify_adapter_failure(exc, "api_football")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "api_football",
                "endpoint": data_type.lower(),
                "date": date,
                "league_id": league_id,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        _row_key: dict[str, str] = {"date": date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=_af_attempt_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
        )

    def _af_record_empty(data_type: str, league_id: str = "", reason: str = "") -> None:
        if manifest is None:
            return
        _row_key: dict[str, str] = {"date": date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        manifest.record_empty(
            row_key=_row_key,
            attempted_at=_af_attempt_ts,
            reason=reason,
            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
        )

    def _af_emit_empty_gaps_for_entity(data_type: str, captured_league_ids: set[str]) -> None:
        """Emit empty_confirmed per expected league with no captured rows (same contract as FIXTURES)."""
        if manifest is None:
            return
        _expected = {lg.league_id for lg in _orch.get_expected_leagues_for_source("api_football")}
        for _exp_lid in sorted(_expected - captured_league_ids):
            if not _orch.get_league_fixture_calendar(_exp_lid, date, date):
                continue
            manifest.record_empty(
                row_key={"date": date, "data_type": data_type, "league_id": _exp_lid},
                attempted_at=_af_attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
            )

    def _should_fetch(entity_short: str) -> bool:
        """Check if this entity should be fetched (not in _fetch_set or _fetch_set is None)."""
        if _fetch_set is None:
            return True
        return entity_short in _fetch_set

    if enrichment_only:
        _orch.logger.info("Enrichment-only mode: skipping leagues/teams/standings/injuries for date=%s", date)

    # LEAGUES write path retired 2026-05-07 (C.1 audit, manifest_migration_master).
    # Replaced by UAC ``LeagueDefinition`` + ``provider_league_ids`` (FOOTYSTATS_SEASON_IDS,
    # FOOTYSTATS_HISTORICAL_SEASON_IDS) which canonicalise the league refdata via code
    # commits — no daily-cadence GCS dump needed. Downstream consumers were 100%
    # schema-only declarations (features-sports LEAGUES_COLUMNS) — no actual feature
    # consumed `logo_url` or other fields beyond what UAC already provides.
    # The api_football `/leagues` endpoint is no longer called from the daily
    # orchestrator path; teams fetch (below) reads `get_prediction_leagues()` from UAC
    # instead of the freshly-fetched leagues_df.

    # Teams — for each prediction league (cached across dates)
    if not enrichment_only and _should_fetch("leagues"):
        teams_df = _orch._cached_teams_df
        prediction_league_ids: list[int] = []
        if teams_df is None:
            all_teams: list[dict[str, object]] = []
            try:
                for league_def in _orch.get_prediction_leagues():
                    if league_def.api_football_id is None:
                        continue
                    prediction_league_ids.append(league_def.api_football_id)
                    try:
                        teams = await adapter.get_teams(league_def.api_football_id)
                        for t in teams:
                            row = _orch._coerce_adapter_output(t)
                            # Tag each team row with the league_id for per-league partitioning
                            row["league_id"] = league_def.league_id
                            all_teams.append(row)
                    except Exception as exc:
                        _orch.classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sports_reference_teams_fetch",
                            shard=str(league_def.league_id),
                        )
                        _af_record_failed("TEAMS", exc, league_id=league_def.league_id)
                if all_teams:
                    teams_df = _orch.pd.DataFrame(all_teams)
                    _orch._set_cached_teams(teams_df, prediction_league_ids)
                    _orch.logger.info("Sports reference: %d teams fetched (API calls — will cache)", len(teams_df))
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sports_reference_teams_batch",
                )
                _af_record_failed("TEAMS", exc)
        else:
            prediction_league_ids = _orch._cached_prediction_league_ids
            _orch.logger.info("Sports reference: %d teams from cache (0 API calls)", len(teams_df))
        if teams_df is not None:
            # Write per-league partitioned team files. The bare-path fallback
            # was retired in sports_manifest_single_ssot_2026_04_30 — TEAMS is
            # a league-axis data type and MUST always carry league_id.
            if "league_id" in teams_df.columns:
                for _t_lid, _t_league_df in teams_df.groupby("league_id"):
                    _t_lid_str = str(_t_lid)
                    _t_stamped = _orch.stamp_available_at_explicit(_t_league_df, when=_orch.datetime.now(_orch.UTC))
                    _orch._gated_sink_write(
                        _orch._sports_ref_sink_for(bucket, date, "teams"),
                        data=_t_stamped,
                        partition={"entity": "teams", "league": _orch._canonical_league_id(_t_lid_str)},
                        filename="teams.parquet",
                        venue="api_football",
                        entity="teams",
                    )
            else:
                _orch.logger.warning(
                    "TEAMS bare-path fallback triggered for date=%s — data shape regression: "
                    "teams_df missing league_id column (rows=%d). Skipping write to keep manifest honest.",
                    date,
                    len(teams_df),
                )
            counts["teams"] = len(teams_df)

            # Extract venues from teams and write a global venues.parquet.
            # The features-sports-service reads this at:
            #   sports_reference/venues/venues.parquet
            # Venue coordinates come from the UAC static registry (enriched
            # by _parse_team_item when get_teams() is called).
            _orch._write_venues_from_teams(teams_df, bucket)

        # Standings — for each prediction league (cached across dates)
        standings_df = _orch._cached_standings_df
        if standings_df is None:
            all_standings: list[dict[str, object]] = []
            for lid in prediction_league_ids:
                try:
                    standings = await adapter.get_standings(lid)
                    for row in standings:
                        d = row.model_dump() if hasattr(row, "model_dump") else row
                        all_standings.append(d)
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sports_reference_standings_fetch",
                        shard=str(lid),
                    )
                    _af_record_failed("STANDINGS", exc, league_id=str(lid))
            if all_standings:
                standings_df = _orch.pd.DataFrame(all_standings)
                _orch._set_cached_standings(standings_df)
                _orch.logger.info(
                    "Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df)
                )
        else:
            _orch.logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
        if standings_df is not None:
            # Write per-league partitioned standings files + per-league manifest rows.
            if "league_id" in standings_df.columns:
                _std_captured: set[str] = set()
                for _s_lid, _s_league_df in standings_df.groupby("league_id"):
                    _s_lid_str = str(_s_lid)
                    _std_captured.add(_s_lid_str)
                    _stamped_std_df = _orch.stamp_available_at_explicit(
                        _s_league_df, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        _orch._sports_ref_sink_for(bucket, date, "standings"),
                        data=_stamped_std_df,
                        partition={"entity": "standings", "league": _orch._canonical_league_id(_s_lid_str)},
                        filename="standings.parquet",
                        venue="api_football",
                        entity="standings",
                    )
                    if manifest is not None:
                        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                            row_key={
                                "date": date,
                                "data_type": "STANDINGS",
                                "league_id": _orch._canonical_league_id(_s_lid_str),
                            },
                            df=_stamped_std_df,
                            asset_group="sports",
                            instrument_type="",
                            data_type="STANDINGS",
                            league_id=_orch._canonical_league_id(_s_lid_str),
                            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                            source=_orch._sports_ref_source("standings"),
                            service_emission_state=None,
                        )
                if manifest is not None:
                    _af_emit_empty_gaps_for_entity("STANDINGS", _std_captured)
            else:
                _orch.logger.warning(
                    "STANDINGS bare-path fallback triggered for date=%s — data shape regression: "
                    "standings_df missing league_id column (rows=%d). Skipping write to keep manifest honest.",
                    date,
                    len(standings_df),
                )
            counts["standings"] = len(standings_df)

    # Injuries — date-specific, always fetched fresh.
    # IMPORTANT: outside the leagues/teams/standings block so it runs even when
    # only injuries is requested (entities_to_fetch=["API_FOOTBALL_INJURIES"]).
    if not enrichment_only and _should_fetch("injuries"):
        try:
            injuries = await adapter.get_injuries(date)
            if injuries:
                df = _orch.pd.DataFrame([_orch._coerce_adapter_output(inj) for inj in injuries])
                # PIT safety: daily injuries published morning-of (date + 12:00 UTC)
                df["available_at"] = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=12)
                counts["injuries"] = len(df)

                # Determine league column — prefer league_id, fallback to fixture_id prefix
                _inj_league_col: str | None = None
                if "league_id" in df.columns and df["league_id"].notna().any():
                    _inj_league_col = "league_id"
                elif "fixture_id" in df.columns and df["fixture_id"].notna().any():
                    # Try canonical fixture_id format (LEAGUE:HOME_v_AWAY:DATE)
                    _sample = df["fixture_id"].dropna().iloc[0] if not df["fixture_id"].dropna().empty else ""
                    if ":" in str(_sample):
                        df["_inj_league"] = df["fixture_id"].str.split(":").str[0]
                        _inj_league_col = "_inj_league"

                if _inj_league_col is not None:
                    _has_league = df[_inj_league_col].notna() & (df[_inj_league_col] != "")
                    _with_league = df[_has_league]
                    _without_league = df[~_has_league]
                    _inj_captured: set[str] = set()

                    for _inj_lid, _inj_league_df in _with_league.groupby(_inj_league_col):
                        _inj_lid_str = str(_inj_lid)
                        _inj_captured.add(_inj_lid_str)
                        _inj_clean = _inj_league_df.drop(columns=["_inj_league"], errors="ignore")
                        _stamped_inj_df = _orch.stamp_available_at_explicit(
                            _inj_clean, when=_orch.datetime.now(_orch.UTC)
                        )
                        _orch._gated_sink_write(
                            _orch._sports_ref_sink_for(bucket, date, "injuries"),
                            data=_stamped_inj_df,
                            partition={"entity": "injuries", "league": _orch._canonical_league_id(_inj_lid_str)},
                            filename="injuries.parquet",
                            venue="api_football",
                            entity="injuries",
                        )
                        if manifest is not None:
                            manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                                row_key={
                                    "date": date,
                                    "data_type": "INJURIES",
                                    "league_id": _orch._canonical_league_id(_inj_lid_str),
                                },
                                df=_stamped_inj_df,
                                asset_group="sports",
                                instrument_type="",
                                data_type="INJURIES",
                                league_id=_orch._canonical_league_id(_inj_lid_str),
                                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                                source=_orch._sports_ref_source("injuries"),
                                service_emission_state=None,
                            )

                    if not _without_league.empty:
                        _orch.logger.warning(
                            "INJURIES bare-path fallback triggered for date=%s — data shape regression: "
                            "%d rows missing league_id (could not derive from fixture_id prefix). "
                            "Skipping bare write to keep manifest honest.",
                            date,
                            len(_without_league),
                        )
                    if manifest is not None:
                        _af_emit_empty_gaps_for_entity("INJURIES", _inj_captured)
                else:
                    _orch.logger.warning(
                        "INJURIES bare-path fallback triggered for date=%s — data shape regression: "
                        "no league_id column AND no fixture_id-prefix-derivable league (rows=%d). "
                        "Skipping bare write to keep manifest honest.",
                        date,
                        len(df),
                    )

                _orch.logger.info("Sports reference: %d injuries written", len(df))
            else:
                # Honest-coverage: legitimate zero-injuries day for this date
                # (no players on the season-wide injuries list have a reported
                # status — common on off-season days).  Per
                # ``sports_manifest_single_ssot_2026_04_30`` we no longer write
                # an empty bare parquet — record_empty per league (handled by
                # the _af_emit_empty_gaps_for_entity call below) is sufficient.
                counts["injuries"] = 0
                _orch.logger.info("Sports reference: 0 injuries returned by API")
                # Honest-coverage: legitimate zero-injuries day for this date
                # (no players on the season-wide injuries list have a reported
                # status — common on off-season days).  Emit empty_confirmed
                # instead of captured(0) so the data-status page distinguishes
                # "source said zero" from "we wrote zero rows".
                _af_emit_empty_gaps_for_entity("INJURIES", set())
        except Exception as exc:
            _orch.classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_injuries_fetch",
                shard=date,
            )
            _af_record_failed("INJURIES", exc)

    # Per-fixture enrichment: stats, events, lineups, player stats.
    # Only for completed fixtures (status in FT/AET/PEN — stats unavailable for future/live).
    # Use fixture_ids_override when available (from URDI fetch) to avoid redundant
    # 33-league re-fetch (saves 33 API calls per date).
    fixture_ids: list[int] = []
    # Mapping from AF fixture ID (str) to canonical league_id for per-league writes.
    _af_fid_to_league: dict[str, str] = {}
    if fixture_ids_override is not None:
        fixture_ids = fixture_ids_override
        _orch.logger.info(
            "Sports reference: %d completed fixture IDs passed from URDI (0 extra API calls)", len(fixture_ids)
        )
        # Build AF fixture_id -> league mapping from GCS fixtures parquet
        _af_fid_to_league = _orch._build_fixture_league_map_from_gcs(bucket, date)

        # Ensure canonical fixtures exist at sports_reference/by_date/entity=fixtures/.
        # The URDI phase writes instrument records, but features-sports needs the
        # canonical fixture format (af_fixture_id, timestamp, home/away names, etc.).
        # Read from the old path (sports_reference/fixtures/day=) or fetch from API.
        # v9: probe canonical path (pipeline_mode= in prefix) first, then legacy.
        _new_fixtures_canonical = _orch._sports_ref_canonical_blob_path(date, "fixtures", filename="fixtures.parquet")
        _new_fixtures_legacy = _orch._sports_ref_legacy_blob_path(date, "fixtures", filename="fixtures.parquet")
        try:
            _storage = _orch.get_storage_client()
            _new_fixtures_path = _orch._resolve_sports_ref_blob(
                _storage, bucket, _new_fixtures_canonical, _new_fixtures_legacy
            )
            _new_blob = _storage.bucket(bucket).blob(_new_fixtures_path)
            # Check if path already has canonical data (not instrument records)
            _needs_write = True
            if _new_blob.exists():
                _existing = _orch.pd.read_parquet(
                    _orch.io.BytesIO(_storage.download_bytes(bucket=bucket, blob_path=_new_fixtures_path))
                )
                if "af_fixture_id" in _existing.columns or "timestamp" in _existing.columns:
                    _needs_write = False  # Already canonical format

            if _needs_write:
                # Try old path first (zero API calls)
                _old_path = f"sports_reference/fixtures/day={date}/fixtures.parquet"
                _old_blob = _storage.bucket(bucket).blob(_old_path)
                if _old_blob.exists():
                    _old_data = _storage.download_bytes(bucket=bucket, blob_path=_old_path)
                    _old_df = _orch.pd.read_parquet(_orch.io.BytesIO(_old_data))
                    # v9: _write_fixtures_per_league creates entity-specific sink internally
                    _orch._write_fixtures_per_league(
                        _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                        _old_df,
                        date,
                        source_label="old-path-copy",
                        bucket=bucket,
                    )
                    _orch.logger.info(
                        "Canonical fixtures copied from old path to entity=fixtures/ (%d rows)",
                        len(_old_df),
                    )
                else:
                    # No old path — fetch from API Football (costs 33 API calls).
                    # Paired with raw so Q5/Q6 lifecycle columns populate (live=batch).
                    _adapter = _orch.create_sports_reference_adapter("api_football", api_key=api_key)
                    _fx_pairs = await _adapter.get_fixtures_with_raw(date)
                    if _fx_pairs:
                        _fx_dicts = [
                            _orch._flatten_canonical_fixture_for_disk(fx, date, af_response=raw)
                            for fx, raw in _fx_pairs
                        ]
                        _fx_df = _orch.pd.DataFrame(_fx_dicts)
                        # PIT safety: scheduled fixtures published ~1 week before kickoff
                        if "timestamp" in _fx_df.columns:
                            _fx_df["available_at"] = _orch.pd.to_datetime(
                                _fx_df["timestamp"], utc=True, errors="coerce"
                            ) - _orch.pd.Timedelta(days=7)
                        _orch._write_fixtures_per_league(
                            _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                            _fx_df,
                            date,
                            source_label="api-fetch-override",
                            bucket=bucket,
                        )
                        _orch.logger.info(
                            "Canonical fixtures fetched from API and written to entity=fixtures/ (%d fixtures)",
                            len(_fx_df),
                        )
        except Exception as _fx_exc:
            _orch.logger.warning("Could not ensure canonical fixtures at entity=fixtures/: %s", _fx_exc)
    else:
        # Fallback: fetch fixtures from API (33 calls for 33 leagues).
        # Only used when called from the zero-fixture early-return path
        # where URDI returned 0 instruments.
        completed_statuses = {"FT", "AET", "PEN"}
        fallback_league_ids: list[int] = []
        _af_id_to_canonical_league: dict[int, str] = {}
        # Fetch fixtures for ALL football leagues (prediction + features + reference).
        # Reference leagues (cups, continental) provide team workload context for
        # fatigue/distance calculations. Features leagues (lower divisions) provide
        # additional fixture data for cross-division team tracking.
        from unified_api_contracts.sports import get_leagues_by_classification

        for cls in ("Prediction", "Features", "Reference"):
            for league_def in get_leagues_by_classification(cls):
                if league_def.api_football_id is not None:
                    fallback_league_ids.append(league_def.api_football_id)
                    _af_id_to_canonical_league[league_def.api_football_id] = league_def.league_id
        try:
            # Paired fetch so the flatten below can populate Q5/Q6 lifecycle
            # columns from the raw api-football response (live=batch parity).
            _fixture_pairs = await adapter.get_fixtures_with_raw(date, league_ids=fallback_league_ids)
            fixtures = [_fx for _fx, _raw in _fixture_pairs]
            for fx in fixtures:
                if fx.status in completed_statuses:
                    raw_id = fx.source_fixture_id or fx.fixture_id
                    with _orch.contextlib.suppress(ValueError, TypeError):
                        fid_int = int(raw_id)
                        fixture_ids.append(fid_int)
                        # Map AF ID -> league from the fixture's league object
                        if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                            _af_fid_to_league[str(fid_int)] = str(fx.league.league_id)
                        elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                            af_lid = fx.league.api_football_id
                            if af_lid in _af_id_to_canonical_league:
                                _af_fid_to_league[str(fid_int)] = _af_id_to_canonical_league[af_lid]
                elif fx.status in {"PST", "CANC"}:
                    _reason = (
                        _orch.EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED
                        if fx.status == "PST"
                        else _orch.EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED
                    )
                    _lid: str = ""
                    if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                        _lid = str(fx.league.league_id)
                    elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                        af_lid = fx.league.api_football_id
                        _lid = _af_id_to_canonical_league.get(af_lid, "")
                    _af_record_empty("FIXTURES", league_id=_lid, reason=str(_reason))
            _orch.logger.info(
                "Sports reference: %d completed fixtures found for enrichment (API fetch)", len(fixture_ids)
            )

            # Write canonical fixtures to sports_reference/by_date/entity=fixtures/
            # so features-sports-service and trigger scheduler can read them.
            if fixtures:
                try:
                    fixture_dicts = [
                        _orch._flatten_canonical_fixture_for_disk(fx, date, af_response=raw)
                        for fx, raw in _fixture_pairs
                    ]
                    fixture_df = _orch.pd.DataFrame(fixture_dicts)
                    # PIT safety: scheduled fixtures published ~1 week before kickoff
                    if "timestamp" in fixture_df.columns:
                        fixture_df["available_at"] = _orch.pd.to_datetime(
                            fixture_df["timestamp"], utc=True, errors="coerce"
                        ) - _orch.pd.Timedelta(days=7)
                    _orch._write_fixtures_per_league(
                        _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                        fixture_df,
                        date,
                        source_label="api-fetch-fallback",
                        bucket=bucket,
                    )
                    _orch.logger.info(
                        "Canonical fixtures written to sports_reference/by_date/entity=fixtures/ (%d fixtures)",
                        len(fixture_df),
                    )
                except Exception as _fx_write_exc:
                    _orch.logger.warning("Failed to write canonical fixtures to reference path: %s", _fx_write_exc)

        except Exception as exc:
            _orch.classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sports_reference_fixtures_fetch",
                shard=date,
            )
            _af_record_failed("FIXTURES", exc)

    # Recovery-mode fixture-id allowlist filter — runs BEFORE the per-fixture
    # entity loop so we only call api_football for the targeted set. Lifts
    # the per-fixture work from O(all_fixtures_on_day x 5 entities) to
    # O(allowlist_intersection_with_day x N_requested_entities). Used for
    # targeted recovery (e.g. Phase 2's truth-set audit produced a 39k
    # fixture-id list; we feed it here so we don't re-burn ~560k api_football
    # calls re-fetching already-captured fixtures' per-fixture entities).
    if recovery_fixture_ids is not None and fixture_ids:
        _pre_filter = len(fixture_ids)
        fixture_ids = [fid for fid in fixture_ids if fid in recovery_fixture_ids]
        _orch.logger.info(
            "Recovery fixture-id filter applied for date=%s: %d → %d fixtures (%d skipped — not in allowlist)",
            date,
            _pre_filter,
            len(fixture_ids),
            _pre_filter - len(fixture_ids),
        )
        if not fixture_ids:
            # Allowlist intersected to zero on this date — no per-fixture work
            # to do. Return early so we don't write phantom empty manifest rows
            # for entities we never attempted to fetch on this date.
            _orch.logger.info(
                "Recovery fixture-id filter: no targeted fixtures on date=%s — skipping per-fixture loop",
                date,
            )
            # Cross-provider mapping tables can still be useful here, but skip
            # them in recovery mode to keep the run cheap.
            return counts

    if fixture_ids:
        _per_fixture_entities = [
            ("fixture_stats", adapter.get_fixture_statistics),
            ("fixture_events", adapter.get_fixture_events),
            ("fixture_lineups", adapter.get_fixture_lineups),
            ("player_stats", adapter.get_fixture_player_stats),
        ]

        # Filter to only fetch entities that are actually missing
        if _fetch_set is not None:
            _per_fixture_entities = [(name, fn) for name, fn in _per_fixture_entities if _should_fetch(name)]
            skipped = 4 - len(_per_fixture_entities)
            if skipped:
                _orch.logger.info(
                    "Per-fixture: skipping %d entities already in manifest, fetching %s",
                    skipped,
                    [n for n, _ in _per_fixture_entities],
                )

        # Concurrent per-fixture fetching with rate-limit semaphore.
        # API Football Mega plan: 900 req/min. With multiple processes sharing
        # the key, cap per-process concurrency at 50 to leave headroom.
        # The adapter's _get_with_retry reads X-RateLimit-Remaining and
        # preemptively sleeps when near-exhausted.
        concurrency = 50
        sem = _orch.asyncio.Semaphore(concurrency)
        entity_rows: dict[str, list[dict[str, object]]] = {name: [] for name, _ in _per_fixture_entities}
        # Per-entity failure tracking for honest-coverage: map entity → (failed_count, sample_error_code).
        entity_failures: dict[str, tuple[int, str]] = {name: (0, "") for name, _ in _per_fixture_entities}

        async def _fetch_one(
            entity_name: str,
            fetch_fn: _orch.Callable[[int], _orch.Awaitable[_orch.Sequence[object]]],
            fid: int,
        ) -> None:
            async with sem:
                try:
                    rows = await fetch_fn(fid)
                    for row in rows:
                        # Adapters return a mix of Pydantic models and plain dicts
                        # depending on whether the normalizer produces a typed model.
                        if hasattr(row, "model_dump"):
                            d: dict[str, object] = _orch.cast(_orch._ModelDumpable, row).model_dump()
                        else:
                            d = _orch.cast(dict[str, object], row)
                        entity_rows[entity_name].append(d)
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation=f"sports_reference_{entity_name}_fetch",
                        shard=str(fid),
                    )
                    # Shard-level failure isolation: count the failure so that
                    # honest-coverage can record_failed at entity-level if
                    # EVERY fixture call for this entity raised.
                    _prev_count, _prev_code = entity_failures[entity_name]
                    entity_failures[entity_name] = (
                        _prev_count + 1,
                        _prev_code or _orch._classify_adapter_failure(exc, "api_football"),
                    )
                # Throttle handled by adapter's _get_with_retry + rate limit headers

        # Pre-fetch skip: read existing per-league parquet for each (entity, league)
        # cell on this date, build the set of af_fixture_ids already captured, and
        # skip api_football calls for those fixtures. Bypassed when ``redo_all`` is
        # True (i.e. the operator passed --force, explicitly asking to re-fetch
        # everything).
        #
        # Why this exists: today's manifest is keyed on (date, data_type, league_id)
        # — it tracks "the cell is captured" but NOT which fixtures within the cell
        # are captured. So the cell-level pre-flight (at orchestrator entry) can't
        # tell "5 of 10 fixtures already done" from "all 10 already done." The fix:
        # at fetch-time, read the per-league parquet (which IS keyed at
        # af_fixture_id row granularity) and skip api calls for fixtures already
        # represented. Generalises to any future per-fixture entity recovery —
        # e.g. when downstream of recovered FIXTURES, only the genuinely-missing
        # fixtures get re-fetched, not the entire cell.
        captured_per_entity_league: dict[tuple[str, str], frozenset[int]] = {}
        if not redo_all and _af_fid_to_league:
            for entity_name, _ in _per_fixture_entities:
                _entity_leagues_seen: set[str] = set()
                for fid in fixture_ids:
                    canonical_league = _af_fid_to_league.get(str(fid))
                    if not canonical_league:
                        continue
                    canonical_league = _orch._canonical_league_id(canonical_league)
                    if canonical_league in _entity_leagues_seen:
                        continue
                    _entity_leagues_seen.add(canonical_league)
                    captured_set = _orch._read_existing_per_league_fixture_ids(
                        bucket=bucket,
                        date=date,
                        entity_name=entity_name,
                        canonical_league_id=canonical_league,
                    )
                    captured_per_entity_league[(entity_name, canonical_league)] = captured_set

        # Build all tasks: N entities x M fixtures (only missing entities)
        tasks: list[_orch.asyncio.Task[None]] = []
        skipped_already_captured = 0
        for entity_name, fetch_fn in _per_fixture_entities:
            for fid in fixture_ids:
                if not redo_all and captured_per_entity_league:
                    canonical_league = _af_fid_to_league.get(str(fid))
                    if canonical_league:
                        canonical_league = _orch._canonical_league_id(canonical_league)
                        captured_set = captured_per_entity_league.get((entity_name, canonical_league), frozenset())
                        if int(fid) in captured_set:
                            skipped_already_captured += 1
                            continue
                tasks.append(_orch.asyncio.ensure_future(_fetch_one(entity_name, fetch_fn, fid)))

        if skipped_already_captured:
            _orch.logger.info(
                "Per-fixture pre-fetch skip: %d (entity, fixture_id) pairs already in existing per-league "
                "parquets — skipping api_football calls (pass --force to re-fetch regardless)",
                skipped_already_captured,
            )
        _orch.logger.info(
            "Per-fixture enrichment: %d fixtures x %d entities = %d calls queued (concurrency=%d, "
            "skipped_already_captured=%d)",
            len(fixture_ids),
            len(_per_fixture_entities),
            len(tasks),
            concurrency,
            skipped_already_captured,
        )
        await _orch.asyncio.gather(*tasks)

        _entity_dt_by_short = {
            "fixture_stats": "FIXTURE_STATS",
            "fixture_events": "FIXTURE_EVENTS",
            "fixture_lineups": "FIXTURE_LINEUPS",
            "player_stats": "PLAYER_STATS",
        }

        for entity_name, _ in _per_fixture_entities:
            _af_entity_dt = _entity_dt_by_short[entity_name]
            all_rows = entity_rows[entity_name]
            if all_rows:
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

                # PIT safety: per-fixture stats/events/lineups/player_stats available ~2h after kickoff.
                # No per-row kickoff here — approximate using date + 17:00 UTC (15:00 typical KO + 2h).
                df["available_at"] = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=17)

                counts[entity_name] = len(df)

                # Write per-league partitioned files using AF fixture_id -> league mapping.
                # Column name is "af_fixture_id" (not "fixture_id") in per-fixture entity data.
                _fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
                if _fid_col in df.columns and _af_fid_to_league:
                    # Ensure string type for map lookup (map keys are str(int(af_id)))
                    df["_league_id"] = df[_fid_col].astype(str).str.split(".").str[0].map(_af_fid_to_league)
                    _has_league = df["_league_id"].notna()
                    _with_league = df[_has_league]
                    _without_league = df[~_has_league]
                    _pf_captured: set[str] = set()

                    for _pf_lid, _pf_league_df in _with_league.groupby("_league_id"):
                        _pf_lid_str = str(_pf_lid)
                        _pf_captured.add(_pf_lid_str)
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

                        # C.6: available_at = date + 17h already set on df at line ~4444 (KO + 2h
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
                                    "data_type": _af_entity_dt,
                                    "league_id": _orch._canonical_league_id(_pf_lid_str),
                                },
                                df=_stamped_pf_df,
                                asset_group="sports",
                                instrument_type="",
                                data_type=_af_entity_dt,
                                league_id=_orch._canonical_league_id(_pf_lid_str),
                                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                                source=_orch._sports_ref_source(entity_name),
                                service_emission_state=None,
                            )

                    # Drop unmapped rows — single-SSOT means bare writes are
                    # forbidden for league-axis data types. Surface as a
                    # warning so we can spot upstream league-mapping
                    # regressions in logs.
                    if not _without_league.empty:
                        _orch.logger.warning(
                            "%s bare-path fallback triggered for date=%s — data shape regression: "
                            "%d rows could not be mapped to a league. Skipping bare write to keep manifest honest.",
                            _af_entity_dt,
                            date,
                            len(_without_league),
                        )
                    if manifest is not None:
                        _af_emit_empty_gaps_for_entity(_af_entity_dt, _pf_captured)
                else:
                    # Single-SSOT: bare manifest row + bare parquet write are
                    # both suppressed; writing one would create a phantom
                    # captured shard with no parquet on disk.  Surface the
                    # upstream regression in logs so it can be diagnosed.
                    _orch.logger.warning(
                        "%s bare-path fallback triggered for date=%s — data shape regression: "
                        "no fixture-id column or empty af_fid->league map (rows=%d). "
                        "Skipping bare write + manifest row to keep manifest honest.",
                        _af_entity_dt,
                        date,
                        len(df),
                    )

                _orch.logger.info("Sports reference: %d %s rows written", len(df), entity_name)
            else:
                # Honest-coverage: entity produced zero rows.  Distinguish
                # fetch failure (record_failed → attempted_failed) from legit
                # empty (record_empty → empty_confirmed).
                #
                # CF-11 fix (2026-06-02): the original guard only routed to
                # record_failed when EVERY fixture call raised
                # (_fail_count == len(fixture_ids)).  A partial failure
                # (_fail_count > 0 but < len(fixture_ids)) fell through to
                # _af_emit_empty_gaps_for_entity → empty_confirmed(EXPECTED_NO_FIXTURE),
                # falsely claiming "we know there's nothing" and freezing the
                # gap forever.  Correct rule: ANY failure → record_failed so
                # the shard is flagged for backfill, not silently confirmed-empty.
                _fail_count, _err_code = entity_failures.get(entity_name, (0, ""))
                if _fail_count > 0 and _err_code:
                    # At least one fixture call raised → treat the entity as
                    # attempted_failed so it is backfilled.  The error code
                    # from the first failure is representative; per-fixture
                    # errors are already emitted individually by _fetch_one.
                    if manifest is not None:
                        manifest.record_failed(
                            row_key={"date": date, "data_type": _af_entity_dt},
                            error=_err_code,
                            attempted_at=_af_attempt_ts,
                            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                        )
                else:
                    # All calls succeeded but returned zero rows
                    # (e.g. post-match stats not yet published, lineups not
                    # disclosed for low-profile fixture) — legitimate empty.
                    _af_emit_empty_gaps_for_entity(_af_entity_dt, set())

    # Cross-provider mapping tables
    _orch._write_team_mapping(bucket)
    _orch._write_fixture_mapping(bucket, date)

    return counts
