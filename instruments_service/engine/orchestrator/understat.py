"""Understat data: xG and shots fetches.

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
    "_fetch_understat_xg",
    "_run_understat_shots_date",
]


async def _fetch_understat_xg(
    date: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch Understat xG data and write to GCS.

    Understat provides expected goals (xG), shot data, and advanced stats
    scraped from public pages. No API key required. Covers 6 leagues:
    EPL, La Liga, Bundesliga, Serie A, Ligue 1, RFPL.

    GCS path: sports_reference/by_date/day={date}/entity=understat_xg/
              understat_xg.parquet
    """
    adapter = _orch.create_sports_reference_adapter("understat")
    sink = _orch._sports_ref_sink_for(bucket, date, "understat_xg")
    counts: dict[str, int] = {}

    # Expected-league denominator (Understat covers 5 PREDICTION leagues: EPL,
    # LA_LIGA, BUNDESLIGA, SERIE_A, LIGUE_1). SSOT:
    # ``codex/02-data/sports-data-source-coverage-matrix.md``.
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    xg_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    _expected_understat_leagues = {
        lg.league_id for lg in get_expected_leagues_for_source("understat", classifications=["Prediction"])
    }

    # Honest-coverage per-league pre-flight: only short-circuit if EVERY
    # expected league already has its own captured/empty_confirmed row. The
    # legacy date-aggregate row (row_key without league_id) is ignored so
    # pre-sharding-era shards get back-filled per-league on the next run.
    # ``attempted_failed`` per-league rows fall through and are retried.
    # ``force=True`` bypasses the skip entirely.
    _all_per_league_captured = bool(_expected_understat_leagues) and all(
        _orch._should_skip_shard(
            xg_manifest,
            row_key={"date": date, "data_type": "XG", "league_id": lid},
            force=force,
        )
        for lid in _expected_understat_leagues
    )
    if _all_per_league_captured:
        _orch.logger.info(
            "Understat xG: skipping date=%s — all %d expected leagues per-league captured",
            date,
            len(_expected_understat_leagues),
        )
        return counts

    # Stamp attempt-start before the network call so record_empty / record_failed
    # reflect the attempt time, not the manifest write time.
    attempt_ts = _orch.datetime.now(_orch.UTC)

    try:
        from unified_api_contracts.sports import build_fixture_id, resolve_understat_team

        fixtures = await adapter.get_fixtures(date)
        if fixtures:
            rows = [_orch._coerce_adapter_output(fx) for fx in fixtures]
            flat_rows: list[dict[str, str | None]] = []
            for row in rows:
                flat: dict[str, str | None] = {}
                for k, v in row.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            flat[f"{k}_{sub_k}"] = str(sub_v) if sub_v is not None else None
                    else:
                        flat[k] = str(v) if v is not None else None
                # Build canonical fixture_id from team names + date
                home_name = flat.get("h_title") or ""
                away_name = flat.get("a_title") or ""
                league = flat.get("league") or ""
                if home_name and away_name:
                    canonical_home = resolve_understat_team(home_name)
                    canonical_away = resolve_understat_team(away_name)
                    flat["canonical_fixture_id"] = build_fixture_id(
                        league_id=league,
                        home_team_id=canonical_home,
                        away_team_id=canonical_away,
                        date_str=date,
                    )
                flat_rows.append(flat)
            df = _orch.pd.DataFrame(flat_rows)
            # PIT safety: Understat xG scraped day after match
            if "kickoff_utc" in df.columns:
                df["available_at"] = _orch.pd.to_datetime(
                    df["kickoff_utc"], utc=True, errors="coerce"
                ) + _orch.pd.Timedelta(hours=24)
            counts["understat_xg"] = len(df)

            _captured_leagues: set[str] = set()
            # Write per-league partitioned files if league column exists
            if "league" in df.columns:
                _has_league = df["league"].notna() & (df["league"] != "")
                _with_league = df[_has_league]
                _without_league = df[~_has_league]

                for _xg_lid, _xg_league_df in _with_league.groupby("league"):
                    _xg_lid_str = str(_xg_lid)
                    _captured_leagues.add(_xg_lid_str)
                    # C.6: available_at = kickoff + 24h already set on df at line ~5688 (understat
                    # data scraped the day after). Preserve it; fill NaT rows (missing kickoff_utc)
                    # with wall-clock as fallback. Do NOT override with stamp_available_at_explicit.
                    _xg_copy = _xg_league_df.copy()
                    _xg_copy["available_at"] = _xg_copy["available_at"].fillna(
                        _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
                    )
                    _stamped_xg_df = _xg_copy
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_xg_df,
                        partition={"entity": "understat_xg", "league": _orch._canonical_league_id(_xg_lid_str)},
                        filename="understat_xg.parquet",
                        venue="understat",
                        entity="understat_xg",
                    )
                    xg_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": "XG", "league_id": _orch._canonical_league_id(_xg_lid_str)},
                        df=_stamped_xg_df,
                        asset_group="sports",
                        instrument_type="",
                        data_type="XG",
                        league_id=_orch._canonical_league_id(_xg_lid_str),
                        pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                        source=_orch._sports_ref_source("understat_xg"),
                        service_emission_state=None,
                    )

                if not _without_league.empty:
                    _orch.logger.warning(
                        "XG bare-path fallback triggered for date=%s — data shape regression: "
                        "%d understat rows missing league label. Skipping bare write to keep manifest honest.",
                        date,
                        len(_without_league),
                    )
            else:
                _orch.logger.warning(
                    "XG bare-path fallback triggered for date=%s — data shape regression: "
                    "understat df missing league column (rows=%d). "
                    "Skipping bare write + manifest row to keep manifest honest.",
                    date,
                    len(df),
                )

            # Honest-coverage per-league: record_empty for expected PREDICTION
            # leagues with no rows on this date (off-season / no fixtures).
            for _exp_lid in sorted(_expected_understat_leagues - _captured_leagues):
                xg_manifest.record_empty(
                    row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                )
            xg_manifest.write()
            _orch.logger.info("Understat xG: %d rows written for date=%s", len(df), date)
        else:
            # Honest-coverage: distinguish genuine off-season/no-fixture empty
            # from fetch errors (e.g. HTTP 404 when the season is not yet indexed
            # in Understat — observed for 2019 backfills). adapter._fetch_error_count
            # is set by _fetch_league_fixtures on each per-league error.
            _xg_fetch_errors: int = getattr(adapter, "_fetch_error_count", 0)
            if _xg_fetch_errors > 0:
                _orch.logger.info(
                    "Understat xG: no fixtures for date=%s — %d league fetch(es) errored"
                    " (not honest-absence); recording attempted_failed",
                    date,
                    _xg_fetch_errors,
                )
                for _exp_lid in sorted(_expected_understat_leagues):
                    xg_manifest.record_failed(
                        row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                        error="HTTP_NOT_FOUND",
                        attempted_at=attempt_ts,
                        pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                    )
            else:
                _orch.logger.info("Understat xG: no fixtures for date=%s", date)
                # Emit per-league record_empty ONLY — the date-aggregate row was
                # deleted in Phase 2 of sports_manifest_shard_migration_cleanup.
                for _exp_lid in sorted(_expected_understat_leagues):
                    xg_manifest.record_empty(
                        row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                        attempted_at=attempt_ts,
                        reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                        pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                    )
            xg_manifest.write()
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="understat_xg_fetch",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "understat")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "understat",
                "endpoint": "get_fixtures",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        # Shard isolation: do not raise; record the failed attempt so the
        # manifest reflects honest attempt-vs-capture coverage and the next
        # run can decide to retry. Emit a per-league failure row for each
        # expected league — the date-aggregate row was deleted in Phase 2 of
        # sports_manifest_shard_migration_cleanup.
        for _exp_lid in sorted(_expected_understat_leagues):
            xg_manifest.record_failed(
                row_key={"date": date, "data_type": "XG", "league_id": _exp_lid},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
            )
        with _orch.contextlib.suppress(Exception):
            xg_manifest.write()

    return counts


async def _run_understat_shots_date(
    date: str,
    bucket: str,
    *,
    force: bool = False,
) -> dict[str, int]:
    """Fetch Understat per-shot xG data and write to GCS.

    Calls ``GET /getMatch/{match_id}`` for each match on ``date`` identified
    from the league-data feed. Normalises via
    ``normalize_understat_shot`` and writes per-league partitioned parquets.

    GCS path:
        sports_reference/by_date/day={date}/entity=understat_xg_shots/
            league={league}/understat_xg_shots.parquet

    data_type key in manifest: ``XG_SHOTS``.
    """
    from unified_api_contracts.external.understat import UnderstatShot
    from unified_api_contracts.external.understat.normalize import normalize_understat_shot
    from unified_api_contracts.sports import get_expected_leagues_for_source

    from instruments_service.reference_data.adapters.sports.adapters.understat import (
        UnderstatAdapter,
    )

    adapter = UnderstatAdapter()
    sink = _orch._sports_ref_sink_for(bucket, date, "understat_xg_shots")
    counts: dict[str, int] = {}

    shots_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    _expected_leagues = {
        lg.league_id for lg in get_expected_leagues_for_source("understat", classifications=["Prediction"])
    }

    _all_per_league_captured = bool(_expected_leagues) and all(
        _orch._should_skip_shard(
            shots_manifest,
            row_key={"date": date, "data_type": "XG_SHOTS", "league_id": lid},
            force=force,
        )
        for lid in _expected_leagues
    )
    if _all_per_league_captured:
        _orch.logger.info(
            "Understat XG_SHOTS: skipping date=%s — all %d expected leagues per-league captured",
            date,
            len(_expected_leagues),
        )
        return counts

    attempt_ts = _orch.datetime.now(_orch.UTC)

    try:
        match_ids = await adapter.get_match_ids_for_date(date)

        league_shots: dict[str, list[dict[str, object]]] = {}
        for match_id, league_name in match_ids:
            shots: list[UnderstatShot] = await adapter.get_match_shots(match_id)
            normalized = [normalize_understat_shot(s) for s in shots]
            canonical_lid = _orch._canonical_league_id(league_name)
            if canonical_lid not in league_shots:
                league_shots[canonical_lid] = []
            league_shots[canonical_lid].extend(normalized)

        _captured_leagues: set[str] = set()

        for lid, shot_rows in league_shots.items():
            if not shot_rows:
                continue
            _captured_leagues.add(lid)
            df = _orch.pd.DataFrame(shot_rows)
            df["available_at"] = _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
            _orch._gated_sink_write(
                sink,
                data=df,
                partition={"entity": "understat_xg_shots", "league": lid},
                filename="understat_xg_shots.parquet",
                venue="understat",
                entity="understat_xg_shots",
            )
            shots_manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                row_key={"date": date, "data_type": "XG_SHOTS", "league_id": lid},
                df=df,
                asset_group="sports",
                instrument_type="shot",
                data_type="XG_SHOTS",
                league_id=lid,
                pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                source=_orch._sports_ref_source("understat_xg_shots"),
                service_emission_state=None,
            )
            counts[f"understat_xg_shots_{lid}"] = len(shot_rows)

        # Honest-coverage: use record_failed for leagues with no captured shots when
        # any fetch error occurred (getLeagueData 404 or getMatch 404). adapter._fetch_error_count
        # accumulates across get_match_ids_for_date() and get_match_shots() calls.
        _had_errors = adapter._fetch_error_count > 0
        for _exp_lid in sorted(_expected_leagues - _captured_leagues):
            if _had_errors:
                shots_manifest.record_failed(
                    row_key={"date": date, "data_type": "XG_SHOTS", "league_id": _exp_lid},
                    error="HTTP_NOT_FOUND",
                    attempted_at=attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                )
            else:
                shots_manifest.record_empty(
                    row_key={"date": date, "data_type": "XG_SHOTS", "league_id": _exp_lid},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
                )
        shots_manifest.write()
        _orch.logger.info(
            "Understat XG_SHOTS: %d matches, %d total shot rows for date=%s",
            len(match_ids),
            sum(counts.values()),
            date,
        )
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="understat_xg_shots_fetch",
            shard=date,
        )
        _err_code = _orch._classify_adapter_failure(exc, "understat")
        _orch.log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "understat",
                "endpoint": "get_match_shots",
                "date": date,
                "error": str(exc),
                "error_code": _err_code,
            },
        )
        for _exp_lid in sorted(_expected_leagues):
            shots_manifest.record_failed(
                row_key={"date": date, "data_type": "XG_SHOTS", "league_id": _exp_lid},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_UNDERSTAT,
            )
        with _orch.contextlib.suppress(Exception):
            shots_manifest.write()

    return counts
