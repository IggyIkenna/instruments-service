"""Sports reference core-entity fetch helpers (teams / standings / injuries).

Cohesion module of the ``engine.orchestrator`` package. Carries the
core-entity stages decomposed out of the legacy ~882-line
``_fetch_sports_reference_data`` body (pure behaviour-preserving extraction;
plan: ``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``),
plus the honest-coverage manifest hooks shared by every sports-reference stage.

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

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
    from instruments_service.reference_data.adapters.sports.adapters.base import BaseSportsReferenceAdapter
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_AfManifestHooks",
    "_fetch_injuries",
    "_fetch_teams_and_standings",
]


@dataclass
class _AfManifestHooks:
    """Honest-coverage manifest hooks for the api_football reference fetch.

    Only record when an external manifest is wired in by the caller (existing
    call-sites always pass one, but the orchestration signature keeps it
    optional for legacy use). Carries the shared attempt timestamp so every
    row from one fetch run stamps consistently.
    """

    date: str
    manifest: _orch.ManifestWriter | None
    attempt_ts: _orch.datetime

    def record_failed(self, data_type: str, exc: Exception, league_id: str = "") -> None:
        if self.manifest is None:
            return
        _err_code = _orch._classify_adapter_failure(exc, "api_football")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "api_football",
                "endpoint": data_type.lower(),
                "date": self.date,
                "league_id": league_id,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        _row_key: dict[str, str] = {"date": self.date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        self.manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=self.attempt_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
        )

    def record_empty(self, data_type: str, league_id: str = "", reason: str = "") -> None:
        if self.manifest is None:
            return
        _row_key: dict[str, str] = {"date": self.date, "data_type": data_type}
        if league_id:
            _row_key["league_id"] = league_id
        self.manifest.record_empty(
            row_key=_row_key,
            attempted_at=self.attempt_ts,
            reason=reason,
            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
        )

    def emit_empty_gaps_for_entity(self, data_type: str, captured_league_ids: set[str]) -> None:
        """Emit empty_confirmed per expected league with no captured rows (same contract as FIXTURES)."""
        if self.manifest is None:
            return
        _expected = {lg.league_id for lg in _orch.get_expected_leagues_for_source("api_football")}
        for _exp_lid in sorted(_expected - captured_league_ids):
            if not _orch.get_league_fixture_calendar(_exp_lid, self.date, self.date):
                continue
            self.manifest.record_empty(
                row_key={"date": self.date, "data_type": data_type, "league_id": _exp_lid},
                attempted_at=self.attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
            )


async def _fetch_teams_and_standings(
    *,
    adapter: BaseSportsReferenceAdapter,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
) -> None:
    """Teams + standings — for each prediction league (cached across dates)."""
    manifest = hooks.manifest
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
                    hooks.record_failed("TEAMS", exc, league_id=league_def.league_id)
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
            hooks.record_failed("TEAMS", exc)
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
                hooks.record_failed("STANDINGS", exc, league_id=str(lid))
        if all_standings:
            standings_df = _orch.pd.DataFrame(all_standings)
            _orch._set_cached_standings(standings_df)
            _orch.logger.info("Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df))
    else:
        _orch.logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
    if standings_df is not None:
        # Write per-league partitioned standings files + per-league manifest rows.
        if "league_id" in standings_df.columns:
            _std_captured: set[str] = set()
            for _s_lid, _s_league_df in standings_df.groupby("league_id"):
                _s_lid_str = str(_s_lid)
                _std_captured.add(_s_lid_str)
                _stamped_std_df = _orch.stamp_available_at_explicit(_s_league_df, when=_orch.datetime.now(_orch.UTC))
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
                hooks.emit_empty_gaps_for_entity("STANDINGS", _std_captured)
        else:
            _orch.logger.warning(
                "STANDINGS bare-path fallback triggered for date=%s — data shape regression: "
                "standings_df missing league_id column (rows=%d). Skipping write to keep manifest honest.",
                date,
                len(standings_df),
            )
        counts["standings"] = len(standings_df)


async def _fetch_injuries(
    *,
    adapter: BaseSportsReferenceAdapter,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
) -> None:
    """Injuries — date-specific, always fetched fresh.

    IMPORTANT: separate from the leagues/teams/standings stage so it runs even
    when only injuries is requested (entities_to_fetch=["API_FOOTBALL_INJURIES"]).
    """
    manifest = hooks.manifest
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
                    _stamped_inj_df = _orch.stamp_available_at_explicit(_inj_clean, when=_orch.datetime.now(_orch.UTC))
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
                    hooks.emit_empty_gaps_for_entity("INJURIES", _inj_captured)
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
            # the emit_empty_gaps_for_entity call below) is sufficient.
            counts["injuries"] = 0
            _orch.logger.info("Sports reference: 0 injuries returned by API")
            # Honest-coverage: legitimate zero-injuries day for this date
            # (no players on the season-wide injuries list have a reported
            # status — common on off-season days).  Emit empty_confirmed
            # instead of captured(0) so the data-status page distinguishes
            # "source said zero" from "we wrote zero rows".
            hooks.emit_empty_gaps_for_entity("INJURIES", set())
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sports_reference_injuries_fetch",
            shard=date,
        )
        hooks.record_failed("INJURIES", exc)
