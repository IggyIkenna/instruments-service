"""Transfermarkt data: team-mapping cache, master append + player-value snapshots, drift anomaly, fetch.

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
    "_cache_is_fresh",
    "_fetch_transfermarkt_data",
    "_master_blob_path",
    "_maybe_emit_drift_anomaly",
    "_read_transfermarkt_team_mapping",
    "_snapshot_blob_path_player_values",
    "_transfermarkt_mapping_blob_path",
    "_write_master_append",
    "_write_snapshot_player_values",
    "_write_transfermarkt_team_mapping",
]


def _transfermarkt_mapping_blob_path(season: int) -> str:
    return f"sports_reference/mappings/transfermarkt_league_teams/season={season}/teams.parquet"


def _write_transfermarkt_team_mapping(
    bucket: str,
    teams: list[dict[str, str | None]],
    season: int,
) -> None:
    """Persist per-season Transfermarkt league → team roster to GCS.

    Path: ``sports_reference/mappings/transfermarkt_league_teams/season={YYYY}/teams.parquet``

    Columns: ``league_id, canonical_league, team_id, name, squad_size,
    player_count, last_fetched_at``. Idempotent — overwrites on every
    orchestrator run.
    """
    if not teams:
        return
    try:
        df = _orch.pd.DataFrame(teams)
        df["last_fetched_at"] = _orch.datetime.now(_orch.UTC).isoformat()
        mapping_sink = _orch.get_data_sink(
            bucket=bucket,
            prefix="sports_reference/mappings",
        )
        mapping_sink.write(
            data=df,
            partition={"transfermarkt_league_teams": "", "season": str(season)},
            format="parquet",
            filename="teams.parquet",
        )
        _orch.logger.info(
            "Transfermarkt team mapping cache: %d rows written for season=%d",
            len(df),
            season,
        )
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="transfermarkt_team_mapping_write",
        )


def _read_transfermarkt_team_mapping(
    bucket: str,
    season: int,
) -> _orch.pd.DataFrame | None:
    """Return cached Transfermarkt roster parquet, or ``None`` when absent.

    Returns ``None`` on 404 or any read error; callers MUST fall back to a
    live fetch in that case. Cache-hit freshness is judged by the caller
    using ``last_fetched_at``.
    """
    blob_path = _orch._transfermarkt_mapping_blob_path(season)
    try:
        storage = _orch.get_storage_client()
        raw = storage.download_bytes(bucket, blob_path)
        if raw is None:
            return None
        return _orch.pd.read_parquet(_orch.io.BytesIO(raw))
    except Exception as exc:
        _orch.logger.debug(
            "Transfermarkt team mapping cache miss for season=%d: %s",
            season,
            exc,
        )
        return None


def _master_blob_path(entity: str) -> str:
    """Return the GCS blob path for the accumulating master parquet.

    Path shape: ``sports_reference/master/entity={entity}/master.parquet``
    """
    return f"sports_reference/master/entity={entity}/master.parquet"


def _snapshot_blob_path_player_values(season: int, trigger_date: str) -> str:
    """Return the GCS blob path for a point-in-time player_values snapshot.

    Path shape:
        ``sports_reference/snapshots/entity=player_values/season={Y}/trigger={T}/player_values.parquet``

    Args:
        season: Season year (e.g. 2024).
        trigger_date: UTC trigger date as ISO string ``YYYY-MM-DD``.
    """
    return (
        f"sports_reference/snapshots/entity=player_values/season={season}/trigger={trigger_date}/player_values.parquet"
    )


def _write_master_append(
    bucket: str,
    entity: str,
    new_df: _orch.pd.DataFrame,
    dedup_key: list[str],
) -> None:
    """Append new rows into the accumulating master parquet, dedup on *dedup_key*.

    Semantics (ITEM 6b):
      1. Read existing master parquet (if present).
      2. Concat existing + new_df.
      3. Deduplicate: for duplicate *dedup_key* rows keep the newest row
         (i.e. the one from new_df — achieved by dropping duplicates while
         keeping the **last** occurrence after concat).
      4. Write back as a single parquet.

    Errors are non-blocking: classify + emit and return so a master-write
    failure never kills the main by_date/ write.

    Args:
        bucket: GCS bucket name (already resolved via resolve_bucket_name).
        entity: Entity label used in the path (``teams``, ``team_mapping``,
            ``player_values``).
        new_df: DataFrame of new rows to merge in.
        dedup_key: Column(s) forming the natural dedup key.
    """
    if new_df.empty:
        return
    blob_path = _orch._master_blob_path(entity)
    try:
        storage = _orch.get_storage_client()
        existing_df: _orch.pd.DataFrame | None = None
        try:
            raw = storage.download_bytes(bucket, blob_path)
            if raw is not None:
                existing_df = _orch.pd.read_parquet(_orch.io.BytesIO(raw))
        except Exception as read_exc:
            _orch.logger.debug(
                "master/%s: no existing parquet (will create fresh): %s",
                entity,
                read_exc,
            )

        if existing_df is not None and not existing_df.empty:
            # Align columns: only keep columns present in both to avoid dtype
            # explosions when new_df introduces new columns mid-season.
            combined = _orch.pd.concat([existing_df, new_df], ignore_index=True, sort=False)
        else:
            combined = new_df.copy()

        # Dedup: keep last (new_df wins) for duplicate natural-key combos.
        valid_dedup_key = [k for k in dedup_key if k in combined.columns]
        if valid_dedup_key:
            combined = combined.drop_duplicates(subset=valid_dedup_key, keep="last")

        combined["last_updated_at"] = _orch.datetime.now(_orch.UTC).isoformat()

        buf = _orch.io.BytesIO()
        combined.to_parquet(buf, index=False)
        storage.upload_bytes(bucket, blob_path, buf.getvalue())
        _orch.logger.info(
            "Transfermarkt master/%s: %d rows written (dedup_key=%s)",
            entity,
            len(combined),
            valid_dedup_key,
        )
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation=f"transfermarkt_master_{entity}_write",
        )


def _write_snapshot_player_values(
    bucket: str,
    season: int,
    trigger_date: str,
    df: _orch.pd.DataFrame,
) -> None:
    """Write a point-in-time snapshot of player_values for (season, trigger_date).

    Path: ``sports_reference/snapshots/entity=player_values/season={Y}/trigger={T}/player_values.parquet``

    Idempotent — overwrites if the same (season, trigger_date) key is re-run.
    Errors are non-blocking.

    Args:
        bucket: GCS bucket name (already resolved).
        season: Season year (e.g. 2024).
        trigger_date: UTC trigger date ISO string ``YYYY-MM-DD``.
        df: Full player_values DataFrame for this trigger date.
    """
    if df.empty:
        return
    blob_path = _orch._snapshot_blob_path_player_values(season, trigger_date)
    try:
        storage = _orch.get_storage_client()
        snapshot_df = df.copy()
        snapshot_df["snapshot_season"] = season
        snapshot_df["snapshot_trigger_date"] = trigger_date
        snapshot_df["snapshot_written_at"] = _orch.datetime.now(_orch.UTC).isoformat()

        buf = _orch.io.BytesIO()
        snapshot_df.to_parquet(buf, index=False)
        storage.upload_bytes(bucket, blob_path, buf.getvalue())
        _orch.logger.info(
            "Transfermarkt snapshot player_values: %d rows → season=%d trigger=%s",
            len(snapshot_df),
            season,
            trigger_date,
        )
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="transfermarkt_snapshot_player_values_write",
        )


def _maybe_emit_drift_anomaly(
    *,
    venue: str,
    endpoint: str,
    league_id: str,
    date: str,
    season: int | None,
    got_count: int,
    expected_count: int | None,
    threshold_pct: float = 10.0,
    high_severity_pct: float = 25.0,
) -> float | None:
    """Emit ``ADAPTER_FETCH_ANOMALY`` when got vs expected deviates beyond
    ``threshold_pct``.  Returns the deviation percentage on emit, else ``None``.

    Shared by TM per-league drift + SFI league-denominator drift.  ``None``
    expected count means "no seed" → silent skip (never emit).
    """
    if expected_count is None or expected_count <= 0:
        return None
    deviation_pct = abs(got_count - expected_count) / expected_count * 100.0
    if deviation_pct <= threshold_pct:
        return None
    severity = "HIGH" if deviation_pct > high_severity_pct else "MEDIUM"
    details: dict[str, object] = {
        "venue": venue,
        "endpoint": endpoint,
        "league_id": league_id,
        "date": date,
        "expected_count": expected_count,
        "got_count": got_count,
        "deviation_pct": round(deviation_pct, 1),
        "severity": severity,
    }
    if season is not None:
        details["season"] = season
    _orch.log_event("ADAPTER_FETCH_ANOMALY", details=details)
    return deviation_pct


def _cache_is_fresh(df: _orch.pd.DataFrame, ttl: _orch.timedelta) -> bool:
    """Return True when every row in ``df`` was fetched within ``ttl``."""
    if df.empty or "last_fetched_at" not in df.columns:
        return False
    try:
        timestamps = _orch.pd.to_datetime(df["last_fetched_at"], utc=True, errors="coerce")
        if timestamps.isna().any():
            return False
        now = _orch.datetime.now(_orch.UTC)
        oldest = timestamps.min().to_pydatetime()
        return (now - oldest) < ttl
    except Exception:
        return False


async def _fetch_transfermarkt_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
    season: int | None = None,
    league_filter: list[str] | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Fetch Transfermarkt leagues and teams (with player values) to GCS.

    entity_filter: when set to "PLAYER_VALUES", only that entity is fetched
        and written (entity-scoped VM mode). TRANSFERMARKT_LEAGUES retired 2026-05-05.
    season: override season year for historical backfill (e.g. 2019).
        When None, the adapter defaults to the current year.

    Transfermarkt provides squad composition, player market values, and
    transfer history. Data is slow-moving (changes at trigger dates:
    season start, transfer window open/close) and fetched only then.

    Honest-coverage: per-league PLAYER_VALUES shards are emitted as
    ``captured`` / ``empty_confirmed`` / ``attempted_failed`` so the
    data-status page can distinguish "league had no squad data" from
    "API call failed" from "league never attempted".

    GCS paths:
        sports_reference/by_date/day={date}/entity=transfermarkt_leagues/
        sports_reference/by_date/day={date}/entity=transfermarkt_teams/
    """
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    adapter = _orch.create_sports_reference_adapter("transfermarkt", api_key=api_key)
    sink = _orch._sports_ref_sink_for(bucket, date, "player_values")
    counts: dict[str, int] = {}

    # TRANSFERMARKT_LEAGUES retired 2026-05-05 — was a static provider-catalog
    # mapping (provider_id -> canonical_name + country). Mappings now live in
    # UAC (TRANSFERMARKT_IDS) as versioned config. Don't fetch + write to GCS.
    _want_teams = entity_filter is None or entity_filter == "PLAYER_VALUES"

    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = _orch.datetime.now(_orch.UTC)

    # --- PLAYER_VALUES shards (per expected league) ---
    if _want_teams:
        # Denominator = expected leagues with a Transfermarkt mapping.
        # SSOT: ``codex/02-data/sports-data-source-coverage-matrix.md`` — 55
        # leagues (Prediction + Features).  The adapter also needs a
        # provider-league-id mapping; leagues without a mapping are skipped
        # with a record_empty entry (we have no way to attempt them).
        _expected_tm_leagues = get_expected_leagues_for_source(
            "transfermarkt",
            classifications=["Prediction", "Features"],
        )
        _league_filter_set = set(league_filter) if league_filter else None
        # Merge legacy ``get_prediction_leagues()`` with the canonical expected
        # set so we keep bundled prediction leagues even if the data_sources
        # registry ever drifts.
        _merged_leagues = {lg.league_id: lg for lg in _expected_tm_leagues}
        for _p_lg in _orch.get_prediction_leagues():
            _merged_leagues.setdefault(_p_lg.league_id, _p_lg)

        # Per-league skip: only skip the date when EVERY expected canonical
        # league has a captured/empty_confirmed PLAYER_VALUES row. Coarse
        # `_should_skip_shard` at (date, data_type) — the previous shape — is
        # the 2026-05-05 MATCHES 18%-coverage bug pattern: writer emits per-
        # league rows (e.g. lines 4946-4951) but skip is at the bundle level,
        # so any one captured league locks the whole date out from per-league
        # re-fetch.
        _expected_pv_league_ids = sorted(
            lg_id for lg_id in _merged_leagues if _league_filter_set is None or lg_id in _league_filter_set
        )
        if _orch._should_skip_date_for_per_league(
            manifest,
            date=date,
            data_type="PLAYER_VALUES",
            expected_canonical_leagues=_expected_pv_league_ids,
            force=force,
        ):
            _orch.logger.info("PLAYER_VALUES: skipping date=%s (all canonical leagues captured)", date)
            return counts

        effective_season = season if season is not None else _orch.datetime.now(_orch.UTC).year

        # Cache short-circuit: skip API calls on non-trigger dates when we
        # already have a fresh roster for this season.  ``cached_rows`` survives
        # the short-circuit so per-league manifest rows are emitted from it.
        _cache_hit = False
        _cached_df = _orch._read_transfermarkt_team_mapping(bucket, effective_season)
        if _cached_df is not None and _orch._cache_is_fresh(
            _cached_df, _orch.timedelta(days=_orch._TRANSFERMARKT_CACHE_STALENESS_DAYS)
        ):
            try:
                _triggers_today = _orch.get_leagues_needing_refresh(_orch.date_type.fromisoformat(date))
            except Exception:
                _triggers_today = ["__fallback__"]
            if not _triggers_today:
                _cache_hit = True
                _orch.logger.info(
                    "Transfermarkt cache hit for season=%d date=%s — skipping API loop",
                    effective_season,
                    date,
                )

        all_teams: list[dict[str, str | None]] = []
        _captured_league_counts: dict[str, int] = {}
        _failed_leagues: dict[str, str] = {}
        _empty_leagues: set[str] = set()
        _unmapped_leagues: set[str] = set()

        if _cache_hit and _cached_df is not None:
            # Populate ``_captured_league_counts`` from the cache so honest-
            # coverage manifest rows + UPSTREAM_FETCH_COMPLETED events fire
            # exactly as they would on a live fetch — minus the paid API calls.
            if "canonical_league" in _cached_df.columns:
                for _canon_league, _group_df in _cached_df.groupby("canonical_league"):
                    _captured_league_counts[str(_canon_league)] = len(_group_df)
            # Honest-coverage: mark expected leagues missing from the cache as
            # empty so the data-status denominator aligns with the orchestrator's
            # attempt set. Without this the cache-hit branch only emits captured
            # rows and leaves the (cadence-window x non-cached-league) cells as
            # data-status "missing" even though the prior live fetch decided
            # there was nothing to capture.
            _expected_canonical = {lg.league_id for lg in _merged_leagues.values()}
            if _league_filter_set is not None:
                _expected_canonical &= _league_filter_set
            _empty_leagues |= _expected_canonical - set(_captured_league_counts)
            _orch.log_event(
                "UPSTREAM_FETCH_COMPLETED",
                details={
                    "venue": "transfermarkt",
                    "endpoint": "get_teams",
                    "date": date,
                    "season": effective_season,
                    "cached": True,
                    "league_count": len(_captured_league_counts),
                },
            )
        else:
            for _lid_key, league_def in sorted(_merged_leagues.items()):
                if _league_filter_set is not None and league_def.league_id not in _league_filter_set:
                    continue
                tm_code = _orch.get_provider_league_id(league_def.league_id, "transfermarkt")
                if tm_code is None:
                    # No provider mapping — we cannot attempt this league.
                    # Record empty so the denominator counts it as
                    # confirmed-unavailable (operator may add mapping later).
                    _unmapped_leagues.add(league_def.league_id)
                    continue
                try:
                    teams = await adapter.get_teams(tm_code, season=season)
                    if not teams:
                        _empty_leagues.add(league_def.league_id)
                        continue
                    _league_count = 0
                    for t in teams:
                        row = _orch._coerce_adapter_output(t)
                        flat: dict[str, str | None] = {k: str(v) if v is not None else None for k, v in row.items()}
                        flat["league_id"] = str(tm_code)
                        flat["canonical_league"] = league_def.league_id
                        # Derive player_count for FSS normalizer
                        players = row.get("players")
                        flat["player_count"] = (
                            str(len(players)) if isinstance(players, list) else flat.get("squad_size")
                        )
                        # Drop nested players list (serializes as unhelpful string)
                        flat.pop("players", None)
                        all_teams.append(flat)
                        _league_count += 1
                    # Drift detection — emit ADAPTER_FETCH_ANOMALY before the
                    # manifest write so observability catches silent partial
                    # responses (e.g. EPL returning 17 teams instead of 20).
                    _orch._maybe_emit_drift_anomaly(
                        venue="transfermarkt",
                        endpoint="get_teams",
                        league_id=league_def.league_id,
                        date=date,
                        season=effective_season,
                        got_count=_league_count,
                        expected_count=_orch.get_expected_team_count_for_league(league_def.league_id, effective_season),
                    )
                    _captured_league_counts[league_def.league_id] = _league_count
                except Exception as exc:
                    # Shard-level failure isolation — per-league failure MUST
                    # NOT kill the batch.  Record_failed + continue.
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="transfermarkt_teams_fetch",
                        shard=str(league_def.league_id),
                    )
                    _err_code = _orch._classify_adapter_failure(exc, "transfermarkt")
                    _orch.log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "transfermarkt",
                            "endpoint": "get_teams",
                            "league_id": league_def.league_id,
                            "date": date,
                            "error": str(exc),
                            "error_code": _err_code,
                        },
                    )
                    _failed_leagues[league_def.league_id] = _err_code

            if all_teams:
                df = _orch.pd.DataFrame(all_teams)
                # Add season column for provenance
                df["season"] = effective_season
                # Write as player_values entity — partition by season when
                # doing historical backfill so seasons don't overwrite each other.
                # v9: day= is embedded in the sink prefix; don't include in partition.
                pv_partition: dict[str, str] = {"entity": "player_values"}
                if season is not None:
                    pv_partition["season"] = str(season)
                _orch._gated_sink_write(
                    sink,
                    data=_orch.stamp_available_at_explicit(df, when=_orch.datetime.now(_orch.UTC)),
                    partition=pv_partition,
                    filename="player_values.parquet",
                    venue="transfermarkt",
                    entity="player_values",
                )
                counts["transfermarkt_teams"] = len(df)
                _orch.logger.info(
                    "Transfermarkt teams → player_values: %d rows written",
                    len(df),
                )

                # Persist per-season cache for the next backfill iteration.
                _cache_rows: list[dict[str, str | None]] = [
                    {
                        "league_id": str(_r.get("league_id", "")),
                        "canonical_league": str(_r.get("canonical_league", "")),
                        "team_id": str(_r.get("team_id", "") or _r.get("id", "")),
                        "name": str(_r.get("name", "")),
                        "squad_size": str(_r.get("squad_size", "") or ""),
                        "player_count": str(_r.get("player_count", "") or ""),
                    }
                    for _r in all_teams
                ]
                _orch._write_transfermarkt_team_mapping(bucket, _cache_rows, effective_season)

                # --- ITEM 6a: master/ + snapshots/ write-path ---
                # master/entity=player_values/ — accumulating append-only union.
                # master/entity=teams/ — team metadata accumulation.
                # master/entity=team_mapping/ — league→team mapping accumulation.
                # snapshots/entity=player_values/ — point-in-time snapshot keyed
                # by (season, trigger date) for lookahead-safe ML training.
                #
                # Natural dedup keys (ITEM 6b):
                #   player_values → (canonical_league, team_id, season)
                #   teams         → (team_id, season)
                #   team_mapping  → (canonical_league, team_id)
                _orch._write_master_append(
                    bucket,
                    entity="player_values",
                    new_df=df,
                    dedup_key=["canonical_league", "team_id", "season"],
                )
                _orch._write_master_append(
                    bucket,
                    entity="teams",
                    new_df=_orch.pd.DataFrame(_cache_rows).assign(season=effective_season),
                    dedup_key=["team_id", "season"],
                )
                _orch._write_master_append(
                    bucket,
                    entity="team_mapping",
                    new_df=_orch.pd.DataFrame(_cache_rows),
                    dedup_key=["canonical_league", "team_id"],
                )
                _orch._write_snapshot_player_values(
                    bucket,
                    season=effective_season,
                    trigger_date=date,
                    df=df,
                )

        # Per-league honest-coverage manifest rows — identical between the
        # cache-hit and live-fetch branches.  ``cached=True`` is passed as a
        # kwarg for future schema evolution (ManifestWriter v8 tolerates extra
        # kwargs); the cache-hit path also emits an UPSTREAM_FETCH_COMPLETED
        # event with ``cached=True`` so current observability can filter on it.
        for _cap_lid, _cap_count in _captured_league_counts.items():
            manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _orch._canonical_league_id(_cap_lid)},
                total_rows=_cap_count,
                # SP-10: per-league PLAYER_VALUES shard — the shard atom is (date, data_type,
                # league_id), so a "league" is already the row key, not a sub-cluster within the
                # row. No finer per-root-cluster contract exists, so the gate is a deliberate
                # no-op; an unfounded expectation would false-fail genuinely-captured data.
                expected_root_clusters={},
                observed_clusters={"": _cap_count},
                available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                pipeline_mode=_orch.PipelineMode.BATCH_TRANSFERMARKT,
                service_emission_state=None,
            )
        for _emp_lid in sorted(_empty_leagues):
            manifest.record_empty(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _emp_lid},
                attempted_at=attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch.PipelineMode.BATCH_TRANSFERMARKT,
            )
        for _unm_lid in sorted(_unmapped_leagues):
            manifest.record_empty(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _unm_lid},
                attempted_at=attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_MAPPING,
                pipeline_mode=_orch.PipelineMode.BATCH_TRANSFERMARKT,
            )
        for _f_lid, _f_err in sorted(_failed_leagues.items()):
            manifest.record_failed(
                row_key={"date": date, "data_type": "PLAYER_VALUES", "league_id": _f_lid},
                error=_f_err,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_TRANSFERMARKT,
            )

    manifest.write()

    return counts
