"""Footystats data: predictions, matches, odds fetches + null-rate validation.

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
    "_fetch_footystats_matches",
    "_fetch_footystats_odds",
    "_fetch_footystats_predictions",
    "_validate_predictions_null_rates",
]

# Hard per-date ceiling for a single FootyStats entity fetch. The base session
# bounds each individual HTTP request (total=120s) + retries, but a
# connector/DNS/executor-level stall inside aiohttp can leave the awaited
# coroutine blocked WITHOUT ever surfacing the ClientTimeout (the 2026-06-22 FS
# backfill froze with python alive, no traceback, no progress). ``asyncio
# .wait_for`` cancels the coroutine from the event loop regardless of where it
# is stuck and raises ``asyncio.TimeoutError`` (caught by the per-date handler
# → record_failed → the date loop continues), so one wedged fetch can never
# hang the whole backfill VM. Each FootyStats entity is a single /todays-matches
# call, so 300s is far above any legitimate completion (a few retries).
_FS_PER_DATE_TIMEOUT_SECS: float = 300.0


async def _fetch_footystats_predictions(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats predictive data and write to GCS as a separate entity.

    Predictive fields (btts_potential, o25_potential, xg_prematch, etc.) are
    FootyStats-proprietary pre-match signals. Written separately from factual
    fixture data so FSS can consume them as third-party signal input.

    GCS path (snapshots preserved per fetch for prediction-evolution tracking):
      sports_reference/by_date/day={date}/entity=footystats_predictions/
        fetched_at_hour={YYYY-MM-DDTHH}/league={league_id}/footystats_predictions.parquet
    """
    adapter = _orch.create_sports_reference_adapter("footystats", api_key=api_key)
    # v9: entity-specific sink so pipeline_mode= lands in the GCS path prefix.
    sink = _orch._sports_ref_sink_for(bucket, date, "footystats_predictions")
    counts: dict[str, int] = {}
    fetched_at_ts = _orch.pd.Timestamp.now(tz="UTC")
    fetched_at_hour = fetched_at_ts.strftime("%Y-%m-%dT%H")

    # Honest-coverage pre-flight + attempt-stamp.  See module-level helpers.
    pred_manifest = _orch.ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )
    _row_key: dict[str, str] = {"date": date, "data_type": "PREDICTIONS"}
    # Per-league skip: only skip the date when every expected canonical
    # footystats league has a (captured | empty_confirmed) row for it.
    # See ``_should_skip_date_for_per_league`` for the bug this fixes.
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _orch._should_skip_date_for_per_league(
        pred_manifest,
        date=date,
        data_type="PREDICTIONS",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        _orch.logger.info("FootyStats predictions: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = _orch.datetime.now(_orch.UTC)

    try:
        from unified_api_contracts.sports import build_fixture_id, resolve_footystats_team

        predictions = await _orch.asyncio.wait_for(
            _orch.cast(_orch.FootystatsAdapter, adapter).get_fixture_predictions(date),
            timeout=_FS_PER_DATE_TIMEOUT_SECS,
        )
        if predictions:
            df = _orch.pd.DataFrame([_orch._coerce_adapter_output(p) for p in predictions])
            # PIT safety: FootyStats predictions publish alongside odds ~3 days before kickoff
            # (empirically verified 2026-04-17: 98% coverage at T-24h, 100% at T-72h).
            if "kickoff_utc" in df.columns:
                df["available_at"] = _orch.pd.to_datetime(df["kickoff_utc"], utc=True) - _orch.pd.Timedelta(hours=72)
            df["fetched_at"] = fetched_at_ts
            # Build canonical fixture_id for downstream join.
            # FootyStats fixture_id format: "{competition_id}:{HOME}_v_{AWAY}:{DATE}"
            # Use historical season ID map (covers ALL seasons, not just current).
            _ft_id_to_league = _orch.FOOTYSTATS_HISTORICAL_SEASON_IDS

            def _ft_canonical(row: _orch.pd.Series) -> str:
                home = str(row.get("home_team", "") or "")
                away = str(row.get("away_team", "") or "")
                if not home or not away:
                    return ""
                # Extract competition_id from fixture_id if present
                fid = str(row.get("fixture_id", "") or "")
                league = ""
                if ":" in fid:
                    comp_str = fid.split(":")[0]
                    if comp_str.isdigit():
                        league = _ft_id_to_league.get(int(comp_str), "")
                return build_fixture_id(
                    league_id=league,
                    home_team_id=resolve_footystats_team(home),
                    away_team_id=resolve_footystats_team(away),
                    date_str=date,
                )

            if "home_team" in df.columns and "away_team" in df.columns:
                df["canonical_fixture_id"] = df.apply(_ft_canonical, axis=1)
            violations = _orch._validate_predictions_null_rates(df, date)
            if violations:
                _orch.logger.warning(
                    "FootyStats predictions shard %s has null-rate warnings (writing anyway): %s",
                    date,
                    "; ".join(violations),
                )
            counts["footystats_predictions"] = len(df)

            # Write per-league partitioned files when canonical_fixture_id is available.
            if "canonical_fixture_id" in df.columns:
                df["_pred_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_pred_league"].notna() & (df["_pred_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _pred_lid, _pred_league_df in _with_league.groupby("_pred_league"):
                    _pred_lid_str = str(_pred_lid)
                    _pred_clean = _pred_league_df.drop(columns=["_pred_league"])
                    _stamped_pred_clean = _orch.stamp_available_at_explicit(
                        _pred_clean, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_pred_clean,
                        partition={
                            "entity": "footystats_predictions",
                            "fetched_at_hour": fetched_at_hour,
                            "league": _pred_lid_str,
                        },
                        venue="footystats",
                        entity="footystats_predictions",
                        filename="footystats_predictions.parquet",
                    )
                    pred_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={
                            "date": date,
                            "data_type": "PREDICTIONS",
                            "league_id": _orch._canonical_league_id(_pred_lid_str),
                        },
                        df=_stamped_pred_clean,
                        asset_group="sports",
                        instrument_type="",
                        data_type="PREDICTIONS",
                        league_id=_orch._canonical_league_id(_pred_lid_str),
                        pipeline_mode=_orch.PipelineMode.BATCH_FOOTYSTATS,
                        source=_orch._sports_ref_source("footystats_predictions"),
                        service_emission_state=None,
                    )

                if not _without_league.empty:
                    _pred_unmapped = _without_league.drop(columns=["_pred_league"])
                    _stamped_pred_unmapped = _orch.stamp_available_at_explicit(
                        _pred_unmapped, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_pred_unmapped,
                        partition={
                            "entity": "footystats_predictions",
                            "fetched_at_hour": fetched_at_hour,
                        },
                        venue="footystats",
                        entity="footystats_predictions",
                        filename="footystats_predictions.parquet",
                    )
                    pred_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": "PREDICTIONS"},
                        df=_stamped_pred_unmapped,
                        asset_group="sports",
                        instrument_type="",
                        data_type="PREDICTIONS",
                        pipeline_mode=_orch.PipelineMode.BATCH_FOOTYSTATS,
                        source=_orch._sports_ref_source("footystats_predictions"),
                        service_emission_state=None,
                    )
            else:
                _stamped_pred_df = _orch.stamp_available_at_explicit(df, when=_orch.datetime.now(_orch.UTC))
                _orch._gated_sink_write(
                    sink,
                    data=_stamped_pred_df,
                    partition={
                        "entity": "footystats_predictions",
                        "fetched_at_hour": fetched_at_hour,
                    },
                    venue="footystats",
                    entity="footystats_predictions",
                    filename="footystats_predictions.parquet",
                )
                pred_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                    row_key={"date": date, "data_type": "PREDICTIONS"},
                    df=_stamped_pred_df,
                    asset_group="sports",
                    instrument_type="",
                    data_type="PREDICTIONS",
                    pipeline_mode=_orch.PipelineMode.BATCH_FOOTYSTATS,
                    source=_orch._sports_ref_source("footystats_predictions"),
                    service_emission_state=None,
                )
            pred_manifest.write()

            _orch.logger.info(
                "FootyStats predictions: %d rows written for date=%s",
                len(df),
                date,
            )
        else:
            _orch.logger.info("FootyStats predictions: no predictive data for date=%s", date)
            # Honest-coverage: legitimate empty (no predictions for this date).
            # footystats catalog refresh tagged canonical BATCH_FOOTYSTATS
            # (Q2=(A) flip 2026-05-12; resolves
            # footystats_pipeline_mode_gap_2026_05_12.md workaround).
            pred_manifest.record_empty(
                row_key=_row_key,
                attempted_at=attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch._pipeline_mode_for_sports_data_type("PREDICTIONS"),
            )
            pred_manifest.write()
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_predictions_fetch",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "footystats")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixture_predictions",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        pred_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_orch._pipeline_mode_for_sports_data_type("PREDICTIONS"),
        )
        with _orch.contextlib.suppress(Exception):
            pred_manifest.write()

    return counts


def _validate_predictions_null_rates(
    df: _orch.pd.DataFrame,
    date: str,
) -> list[str]:
    """Validate FootyStats predictions null rates against expectations.

    Returns a list of violation messages. Empty list = valid shard.

    Column tiers:
      REQUIRED (max 5% null): core identifiers + core potentials
      SPARSE (allowed >80% null): corners/cards/offsides/avg potentials
      VARIABLE (no constraint): everything else
    """
    # Core identifiers must be present (5% null max).
    core_cols = [
        "fixture_id",
        "source",
        "kickoff_utc",
        "home_team",
        "away_team",
    ]
    # Potentials are coverage-dependent — lower leagues often lack them.
    # Relaxed to 20% null max.
    potential_cols = [
        "btts_potential",
        "o25_potential",
        "o35_potential",
        "o45_potential",
        "xg_prematch_home",
        "xg_prematch_away",
    ]
    violations: list[str] = []
    n = len(df)
    if n == 0:
        return violations
    for col in core_cols:
        if col not in df.columns:
            violations.append(f"{col} missing from schema")
            continue
        null_pct = df[col].isnull().mean() * 100
        if null_pct > 5.0:
            violations.append(f"{col} null rate {null_pct:.1f}% exceeds 5% max (date={date}, n={n})")
    for col in potential_cols:
        if col not in df.columns:
            continue  # Optional — not all dates have all potentials
        null_pct = df[col].isnull().mean() * 100
        if null_pct > 20.0:
            violations.append(f"{col} null rate {null_pct:.1f}% exceeds 20% max (date={date}, n={n})")
    return violations


async def _fetch_footystats_matches(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats match data and write to GCS.

    FootyStats provides detailed match statistics (possession, shots, corners,
    xG) from a different source than API Football. Written as a separate entity
    so downstream consumers can cross-validate or merge with API Football data.

    GCS path: sports_reference/by_date/day={date}/entity=footystats_matches/
              footystats_matches.parquet
    """
    adapter = _orch.create_sports_reference_adapter("footystats", api_key=api_key)
    sink = _orch._sports_ref_sink_for(bucket, date, "footystats_matches")
    counts: dict[str, int] = {}

    # Honest-coverage pre-flight + attempt-stamp.
    _ft_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    _row_key: dict[str, str] = {"date": date, "data_type": "MATCHES"}
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _orch._should_skip_date_for_per_league(
        _ft_manifest,
        date=date,
        data_type="MATCHES",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        _orch.logger.info("FootyStats matches: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = _orch.datetime.now(_orch.UTC)

    try:
        from unified_api_contracts.sports import build_fixture_id, resolve_footystats_team

        # FootyStats league IDs are seasonal — use HISTORICAL map which covers
        # all seasons 2019-2026 (not just current, so old backfill dates match).
        league_ids = list(_orch.FOOTYSTATS_HISTORICAL_SEASON_IDS.keys())
        fixtures = await _orch.asyncio.wait_for(
            adapter.get_fixtures(date, league_ids=league_ids),
            timeout=_FS_PER_DATE_TIMEOUT_SECS,
        )
        if fixtures:
            rows = [_orch._coerce_adapter_output(fx) for fx in fixtures]
            # Flatten nested models for parquet compatibility
            flat_rows: list[dict[str, str | None]] = []
            for row in rows:
                flat: dict[str, str | None] = {}
                for k, v in row.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            flat[f"{k}_{sub_k}"] = str(sub_v) if sub_v is not None else None
                    else:
                        flat[k] = str(v) if v is not None else None
                # Build canonical fixture_id from team names + date.
                # Use the SAME resolution path as _fetch_footystats_odds: reverse-map
                # competition_id (numeric FootyStats league ID) via FOOTYSTATS_HISTORICAL_SEASON_IDS
                # so we get EPL, BUNDESLIGA, etc. (same keys as odds — essential for joins).
                home_name = flat.get("home_team_name") or flat.get("home_team") or ""
                away_name = flat.get("away_team_name") or flat.get("away_team") or ""
                league = ""
                # 1. Try league_league_id (numeric from the flattened league sub-dict)
                raw_league = flat.get("league_league_id") or flat.get("competition_id") or ""
                if raw_league and str(raw_league).isdigit():
                    league = _orch.FOOTYSTATS_HISTORICAL_SEASON_IDS.get(int(raw_league), "")
                # 2. Fallback: parse from fixture_id prefix (same as odds path)
                if not league:
                    fid = flat.get("fixture_id") or flat.get("source_fixture_id") or ""
                    if ":" in fid:
                        comp_str = fid.split(":")[0]
                        if comp_str.isdigit():
                            league = _orch.FOOTYSTATS_HISTORICAL_SEASON_IDS.get(int(comp_str), "")
                if home_name and away_name:
                    canonical_home = resolve_footystats_team(home_name)
                    canonical_away = resolve_footystats_team(away_name)
                    flat["canonical_fixture_id"] = build_fixture_id(
                        league_id=league,
                        home_team_id=canonical_home,
                        away_team_id=canonical_away,
                        date_str=date,
                    )
                flat_rows.append(flat)
            df = _orch.pd.DataFrame(flat_rows)
            # PIT safety: post-match stats available ~3h after kickoff when match complete
            if "kickoff_utc" in df.columns:
                df["available_at"] = _orch.pd.to_datetime(
                    df["kickoff_utc"], utc=True, errors="coerce"
                ) + _orch.pd.Timedelta(hours=3)
            counts["footystats_matches"] = len(df)

            # Write per-league partitioned files using canonical_fixture_id.
            _captured_leagues: set[str] = set()
            if "canonical_fixture_id" in df.columns:
                df["_ft_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_ft_league"].notna() & (df["_ft_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _ft_lid, _ft_league_df in _with_league.groupby("_ft_league"):
                    _ft_lid_str = str(_ft_lid)
                    _ft_canonical = _orch._canonical_league_id(_ft_lid_str)
                    _ft_clean = _ft_league_df.drop(columns=["_ft_league"])
                    _stamped_ft_df = _orch.stamp_available_at_explicit(_ft_clean, when=_orch.datetime.now(_orch.UTC))
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_ft_df,
                        partition={
                            "entity": "footystats_matches",
                            "league": _ft_canonical,
                        },
                        venue="footystats",
                        entity="footystats_matches",
                        filename="footystats_matches.parquet",
                    )
                    _ft_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": "MATCHES", "league_id": _ft_canonical},
                        df=_stamped_ft_df,
                        asset_group="sports",
                        instrument_type="",
                        data_type="MATCHES",
                        league_id=_ft_canonical,
                        pipeline_mode=_orch.PipelineMode.BATCH_FOOTYSTATS,
                        source=_orch._sports_ref_source("footystats_matches"),
                        service_emission_state=None,
                    )
                    _captured_leagues.add(_ft_canonical)

                if not _without_league.empty:
                    _orch.logger.warning(
                        "MATCHES bare-path fallback triggered for date=%s — data shape regression: "
                        "%d footystats rows could not derive a league from canonical_fixture_id. "
                        "Skipping bare write + manifest row to keep manifest honest.",
                        date,
                        len(_without_league),
                    )
            else:
                _orch.logger.warning(
                    "MATCHES bare-path fallback triggered for date=%s — data shape regression: "
                    "footystats df missing canonical_fixture_id column (rows=%d). "
                    "Skipping bare write + manifest row to keep manifest honest.",
                    date,
                    len(df),
                )

            # Honest-coverage per-league: record_empty for expected footystats
            # leagues with no matches on this date (off-season / no fixtures).
            # Mirrors the XG adapter pattern at the understat block below.
            # footystats-served MATCHES tagged canonical BATCH_FOOTYSTATS
            # (Q2=(A) flip 2026-05-12).
            for _exp_lid in sorted(set(_ft_expected) - _captured_leagues):
                _ft_manifest.record_empty(
                    row_key={"date": date, "data_type": "MATCHES", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch._pipeline_mode_for_sports_data_type("MATCHES"),
                )
            _ft_manifest.write()
            _orch.logger.info("FootyStats matches: %d rows written for date=%s", len(df), date)
        else:
            _orch.logger.info("FootyStats matches: no fixtures for date=%s", date)
            # Honest-coverage: emit per-league record_empty for ALL expected
            # leagues — date-aggregate rows were retired in Phase 2 of the
            # sports_manifest_shard_migration_cleanup, mirroring the XG pattern.
            for _exp_lid in sorted(set(_ft_expected)):
                _ft_manifest.record_empty(
                    row_key={"date": date, "data_type": "MATCHES", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch._pipeline_mode_for_sports_data_type("MATCHES"),
                )
            _ft_manifest.write()
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_matches_fetch",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "footystats")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixtures",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        _ft_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_orch._pipeline_mode_for_sports_data_type("MATCHES"),
        )
        with _orch.contextlib.suppress(Exception):
            _ft_manifest.write()

    return counts


async def _fetch_footystats_odds(
    date: str,
    api_key: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch FootyStats pre-match odds (68 markets) and write to GCS.

    FootyStats provides comprehensive pre-match odds from aggregated
    bookmakers: full-time 1X2, O/U 0.5-4.5, 1st/2nd half results and
    O/U, BTTS (full/per-half), corners (result + O/U), clean sheet,
    team to score first, win to nil, double chance, draw no bet.

    Same ``/todays-matches`` endpoint as predictions and matches — no
    extra API calls needed.

    GCS path (snapshots preserved per fetch for odds-evolution tracking):
      sports_reference/by_date/day={date}/entity=footystats_odds/
        fetched_at_hour={YYYY-MM-DDTHH}/league={league_id}/footystats_odds.parquet

    Each fetch lands in its own `fetched_at_hour` partition so repeated polls
    of the same future date accumulate snapshots instead of overwriting.
    """
    adapter = _orch.create_sports_reference_adapter("footystats", api_key=api_key)
    sink = _orch._sports_ref_sink_for(bucket, date, "footystats_odds")
    counts: dict[str, int] = {}
    fetched_at_ts = _orch.pd.Timestamp.now(tz="UTC")
    fetched_at_hour = fetched_at_ts.strftime("%Y-%m-%dT%H")

    # Honest-coverage pre-flight + attempt-stamp.
    odds_manifest = _orch.ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )
    _row_key: dict[str, str] = {"date": date, "data_type": "ODDS"}
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    _ft_expected = [
        lg.league_id for lg in get_expected_leagues_for_source("footystats", classifications=["Prediction", "Features"])
    ]
    if _orch._should_skip_date_for_per_league(
        odds_manifest,
        date=date,
        data_type="ODDS",
        expected_canonical_leagues=_ft_expected,
        force=force,
    ):
        _orch.logger.info("FootyStats odds: skipping date=%s (all canonical leagues captured)", date)
        return counts
    attempt_ts = _orch.datetime.now(_orch.UTC)

    try:
        from unified_api_contracts.sports import build_fixture_id, resolve_footystats_team

        odds_rows = await _orch.asyncio.wait_for(
            _orch.cast(_orch.FootystatsAdapter, adapter).get_fixture_odds_snapshot(date),
            timeout=_FS_PER_DATE_TIMEOUT_SECS,
        )
        if odds_rows:
            df = _orch.pd.DataFrame(odds_rows)
            # PIT safety: FootyStats publishes odds ~3 days before kickoff (empirically verified
            # 2026-04-17: 98% of matches have odds at T-24h, 100% at T-72h, ~8% at T-168h).
            # Conservative: assume odds available from T-72h.
            if "kickoff_utc" in df.columns:
                df["available_at"] = _orch.pd.to_datetime(df["kickoff_utc"], utc=True) - _orch.pd.Timedelta(hours=72)
            # fetched_at = when we actually captured this snapshot (for odds movement tracking)
            df["fetched_at"] = fetched_at_ts
            _ft_id_to_league = _orch.FOOTYSTATS_HISTORICAL_SEASON_IDS

            def _odds_canonical(row: _orch.pd.Series) -> str:
                home = str(row.get("home_team", "") or "")
                away = str(row.get("away_team", "") or "")
                if not home or not away:
                    return ""
                fid = str(row.get("fixture_id", "") or "")
                league = ""
                if ":" in fid:
                    comp_str = fid.split(":")[0]
                    if comp_str.isdigit():
                        league = _ft_id_to_league.get(int(comp_str), "")
                return build_fixture_id(
                    league_id=league,
                    home_team_id=resolve_footystats_team(home),
                    away_team_id=resolve_footystats_team(away),
                    date_str=date,
                )

            if "home_team" in df.columns and "away_team" in df.columns:
                df["canonical_fixture_id"] = df.apply(_odds_canonical, axis=1)
            counts["footystats_odds"] = len(df)

            if "canonical_fixture_id" in df.columns:
                df["_odds_league"] = df["canonical_fixture_id"].str.split(":").str[0]
                _has_league = df["_odds_league"].notna() & (df["_odds_league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _odds_lid, _odds_league_df in _with_league.groupby("_odds_league"):
                    _odds_lid_str = str(_odds_lid)
                    _odds_clean = _odds_league_df.drop(columns=["_odds_league"])
                    _stamped_odds_clean = _orch.stamp_available_at_explicit(
                        _odds_clean, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_odds_clean,
                        partition={
                            "entity": "footystats_odds",
                            "fetched_at_hour": fetched_at_hour,
                            "league": _odds_lid_str,
                        },
                        venue="footystats",
                        entity="footystats_odds",
                        filename="footystats_odds.parquet",
                    )
                    odds_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={
                            "date": date,
                            "data_type": "ODDS",
                            "league_id": _orch._canonical_league_id(_odds_lid_str),
                        },
                        df=_stamped_odds_clean,
                        asset_group="sports",
                        instrument_type="",
                        data_type="ODDS",
                        league_id=_orch._canonical_league_id(_odds_lid_str),
                        pipeline_mode=_orch.PipelineMode.BATCH_ODDS_API,
                        source=_orch._sports_ref_source("footystats_odds"),
                        service_emission_state=None,
                        # Phase 1A cluster gate: per-fixture ≥1 row floor (bridge until
                        # EXPECTED_BOOKMAKER_MARKET_SETS ships as item 1 of this plan
                        # and the min-count can be set to len(expected_set[tier])).
                        expected_root_clusters={
                            str(fid): 1
                            for fid in _stamped_odds_clean["canonical_fixture_id"].dropna().unique()
                            if fid
                        },
                        cluster_extractor=lambda sym: sym,
                        cluster_symbol_column="canonical_fixture_id",
                    )

                if not _without_league.empty:
                    _odds_unmapped = _without_league.drop(columns=["_odds_league"])
                    _stamped_odds_unmapped = _orch.stamp_available_at_explicit(
                        _odds_unmapped, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_odds_unmapped,
                        partition={
                            "entity": "footystats_odds",
                            "fetched_at_hour": fetched_at_hour,
                        },
                        venue="footystats",
                        entity="footystats_odds",
                        filename="footystats_odds.parquet",
                    )
                    odds_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": "ODDS"},
                        df=_stamped_odds_unmapped,
                        asset_group="sports",
                        instrument_type="",
                        data_type="ODDS",
                        pipeline_mode=_orch.PipelineMode.BATCH_ODDS_API,
                        source=_orch._sports_ref_source("footystats_odds"),
                        service_emission_state=None,
                        # Unmapped-league rows still carry canonical_fixture_id; assert
                        # ≥1 row per fixture to catch silent drops.
                        expected_root_clusters={
                            str(fid): 1
                            for fid in _stamped_odds_unmapped["canonical_fixture_id"].dropna().unique()
                            if fid
                        },
                        cluster_extractor=lambda sym: sym,
                        cluster_symbol_column="canonical_fixture_id",
                    )
            else:
                _stamped_odds_df = _orch.stamp_available_at_explicit(df, when=_orch.datetime.now(_orch.UTC))
                _orch._gated_sink_write(
                    sink,
                    data=_stamped_odds_df,
                    partition={
                        "entity": "footystats_odds",
                        "fetched_at_hour": fetched_at_hour,
                    },
                    venue="footystats",
                    entity="footystats_odds",
                    filename="footystats_odds.parquet",
                )
                odds_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                    row_key={"date": date, "data_type": "ODDS"},
                    df=_stamped_odds_df,
                    asset_group="sports",
                    instrument_type="",
                    data_type="ODDS",
                    pipeline_mode=_orch.PipelineMode.BATCH_ODDS_API,
                    source=_orch._sports_ref_source("footystats_odds"),
                    service_emission_state=None,
                    # No canonical_fixture_id column (home_team/away_team absent in source
                    # response) — NO-OP cluster gate per SP-10 contract.
                    expected_root_clusters={},
                    cluster_extractor=lambda sym: sym,
                )
            odds_manifest.write()
            _orch.logger.info("FootyStats odds: %d rows written for date=%s", len(df), date)
        else:
            _orch.logger.info("FootyStats odds: no odds data for date=%s", date)
            # Honest-coverage: legitimate empty (no odds for this date).
            # ODDS slice tagged BATCH_ODDS_API per UAC SOURCE_PRIORITY
            # (footystats odds adapter; see footystats_pipeline_mode_gap_2026_05_12.md).
            odds_manifest.record_empty(
                row_key=_row_key,
                attempted_at=attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch._pipeline_mode_for_sports_data_type("ODDS"),
            )
            odds_manifest.write()
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="footystats_odds_fetch",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "footystats")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "footystats",
                "endpoint": "get_fixture_odds_snapshot",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt.
        odds_manifest.record_failed(
            row_key=_row_key,
            error=_err_code,
            attempted_at=attempt_ts,
            pipeline_mode=_orch._pipeline_mode_for_sports_data_type("ODDS"),
        )
        with _orch.contextlib.suppress(Exception):
            odds_manifest.write()

    return counts
