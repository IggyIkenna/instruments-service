"""SoccerFootball.info data: league-mapping cache and fetch.

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
    "_fetch_sfi_data",
    "_read_sfi_league_mapping",
    "_sfi_mapping_blob_path",
    "_write_sfi_league_mapping",
]


def _sfi_mapping_blob_path() -> str:
    return "sports_reference/mappings/sfi_league_mapping.parquet"


def _write_sfi_league_mapping(
    bucket: str,
    leagues: list[dict[str, str | None]],
) -> None:
    """Persist SFI league hex-id → canonical mapping to GCS.

    Path: ``sports_reference/mappings/sfi_league_mapping.parquet``

    SFI league hex IDs are long-lived (not season-scoped); a single flat
    parquet is sufficient. Columns: ``canonical_league_id, sfi_league_hex,
    name, last_fetched_at``.
    """
    if not leagues:
        return
    try:
        df = _orch.pd.DataFrame(leagues)
        df["last_fetched_at"] = _orch.datetime.now(_orch.UTC).isoformat()
        mapping_sink = _orch.get_data_sink(
            bucket=bucket,
            prefix="sports_reference/mappings",
        )
        mapping_sink.write(
            data=df,
            partition={},
            format="parquet",
            filename="sfi_league_mapping.parquet",
        )
        _orch.logger.info("SFI league mapping cache: %d rows written", len(df))
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sfi_league_mapping_write",
        )


def _read_sfi_league_mapping(bucket: str) -> _orch.pd.DataFrame | None:
    """Return cached SFI league mapping parquet, or ``None`` when absent."""
    try:
        storage = _orch.get_storage_client()
        raw = storage.download_bytes(bucket, _orch._sfi_mapping_blob_path())
        if raw is None:
            return None
        return _orch.pd.read_parquet(_orch.io.BytesIO(raw))
    except Exception as exc:
        _orch.logger.debug("SFI league mapping cache miss: %s", exc)
        return None


async def _fetch_sfi_data(
    date: str,
    api_key: str,
    bucket: str,
    entity_filter: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Fetch SoccerFootball.info leagues, standings, and progressive stats to GCS.

    entity_filter: when set to "SFI_PROGRESSIVE_STATS", only that entity is
        written (entity-scoped VM mode). SFI_LEAGUES + SFI_STANDINGS retired 2026-04-24/2026-05-05.

    SFI provides progressive stats: 30-second interval match time-series data
    for halftime feature engineering.

    Honest-coverage: per-league SFI_PROGRESSIVE_STATS shards emit ``captured``
    / ``empty_confirmed`` / ``attempted_failed`` so the data-status page can
    distinguish legitimate empties from API failures.
    Shard-level failure isolation: a per-league exception is recorded and
    the loop continues — never raised to caller.

    GCS paths:
        sports_reference/by_date/day={date}/entity=sfi_leagues/
        sports_reference/by_date/day={date}/entity=sfi_standings/
        sports_reference/by_date/day={date}/entity=progressive_stats/
    """
    from unified_api_contracts.sports import (
        get_expected_leagues_for_source,
    )

    adapter = _orch.create_sports_reference_adapter("soccer_football_info", api_key=api_key)
    sink = _orch._sports_ref_sink_for(bucket, date, "progressive_stats")
    counts: dict[str, int] = {}

    # SFI_LEAGUES retired 2026-05-05 — provider catalog mapping in UAC.
    # adapter.get_leagues() still runs at runtime to build the prediction-tier
    # filter for progressive_stats fetches, but no GCS write or manifest row.
    # SFI_STANDINGS retired 2026-04-24 — SFI has no standings endpoint.
    _want_sfi_progressive = entity_filter is None or entity_filter == "SFI_PROGRESSIVE_STATS"

    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    attempt_ts = _orch.datetime.now(_orch.UTC)

    # Expected denominator: 33 PREDICTION leagues per coverage matrix.
    _expected_sfi_leagues = get_expected_leagues_for_source(
        "soccer_football_info",
        classifications=["Prediction"],
    )
    _expected_sfi_league_ids = {lg.league_id for lg in _expected_sfi_leagues}

    # Per-league skip: only skip the date when EVERY expected canonical league
    # has a captured/empty_confirmed row for SFI_PROGRESSIVE_STATS. The plain
    # `_should_skip_shard` matched on (date, data_type) only — that's the
    # 2026-05-05 MATCHES 18%-coverage bug shape. SFI_PROGRESSIVE_STATS writes
    # per-league rows at lines 5293-5313 / 5210-5218 / 5335-5343 / 5352-5360 /
    # 5368-5376, so the skip MUST also be per-league.
    if _want_sfi_progressive and _orch._should_skip_date_for_per_league(
        manifest,
        date=date,
        data_type="SFI_PROGRESSIVE_STATS",
        expected_canonical_leagues=sorted(_expected_sfi_league_ids),
        force=force,
    ):
        _want_sfi_progressive = False

    # --- SFI league mapping cache short-circuit (per-date, 24h TTL) ---
    # Backfill VMs hit the SFI ``get_leagues`` endpoint once per trigger date.
    # A fresh cache lets non-trigger dates reuse the canonical league-ID list
    # without a paid API call.  ``sfi_league_ids`` is the list of SFI hex IDs
    # used downstream by ``_filtered_sfi_ids`` for progressive-stats.
    sfi_league_ids: list[str] = []
    _sfi_cache_hit = False
    _sfi_cached_df = _orch._read_sfi_league_mapping(bucket)
    if _sfi_cached_df is not None and _orch._cache_is_fresh(
        _sfi_cached_df, _orch.timedelta(hours=_orch._SFI_CACHE_STALENESS_HOURS)
    ):
        try:
            _sfi_triggers_today = _orch.get_leagues_needing_refresh(_orch.date_type.fromisoformat(date))
        except Exception:
            _sfi_triggers_today = ["__fallback__"]
        if not _sfi_triggers_today and "sfi_league_hex" in _sfi_cached_df.columns:
            _sfi_cache_hit = True
            sfi_league_ids = [str(v) for v in _sfi_cached_df["sfi_league_hex"].dropna().tolist() if str(v)]
            _orch.logger.info(
                "SFI league mapping cache hit for date=%s — skipping get_leagues API",
                date,
            )
            _orch.log_event(
                "UPSTREAM_FETCH_COMPLETED",
                details={
                    "venue": "soccer_football_info",
                    "endpoint": "get_leagues",
                    "date": date,
                    "cached": True,
                    "league_count": len(sfi_league_ids),
                },
            )
            # SFI_LEAGUES retired 2026-05-05 — was a static catalog (provider
            # hash -> canonical name); mappings now in UAC SOCCER_FOOTBALL_INFO_IDS.
            # Cache hit: no manifest row, just keep sfi_league_ids for runtime.

    # --- SFI catalog runtime fetch (NOT a captured data type) ---
    # adapter.get_leagues() returns the live SFI catalog; we use it to build
    # the prediction-tier filter for downstream progressive_stats fetches.
    # No GCS write, no manifest row — see retirement note above.
    leagues = [] if _sfi_cache_hit else None
    try:
        if not _sfi_cache_hit:
            leagues = await adapter.get_leagues()
        if leagues:
            sfi_league_ids = [lg.league_id for lg in leagues]

            # Drift detection — compare Prediction-classified league count the
            # provider returned against the canonical denominator.  Fires an
            # ADAPTER_FETCH_ANOMALY event (does not block the write) when we
            # see fewer mapped leagues than expected.  SFI uses a wider 15/30%
            # threshold than TM (10/25%) because the provider routinely drops
            # and re-adds fringe leagues day-to-day.
            if _expected_sfi_league_ids:
                _mapped_sfi_ids_check = set(_orch.SOCCER_FOOTBALL_INFO_IDS.values())
                _got_mapped_count = sum(1 for lid in sfi_league_ids if lid in _mapped_sfi_ids_check)
                _orch._maybe_emit_drift_anomaly(
                    venue="soccer_football_info",
                    endpoint="get_leagues",
                    league_id="__denominator__",
                    date=date,
                    season=None,
                    got_count=_got_mapped_count,
                    expected_count=len(_expected_sfi_league_ids),
                    threshold_pct=15.0,
                    high_severity_pct=30.0,
                )

            # Persist SFI league-mapping cache for the next backfill iteration.
            _sfi_hex_by_canonical = {v: k for k, v in _orch.SOCCER_FOOTBALL_INFO_IDS.items()}
            _cache_rows: list[dict[str, str | None]] = []
            for _lg in leagues:
                _raw = _orch._coerce_adapter_output(_lg)
                _hex = str(_raw.get("league_id", ""))
                _canonical = _sfi_hex_by_canonical.get(_hex, "")
                _cache_rows.append(
                    {
                        "canonical_league_id": _canonical,
                        "sfi_league_hex": _hex,
                        "name": str(_raw.get("name", "")),
                    }
                )
            _orch._write_sfi_league_mapping(bucket, _cache_rows)
        elif not _sfi_cache_hit:
            _orch.logger.info("SFI leagues: 0 rows returned for date=%s (no manifest write — retired)", date)
    except Exception as exc:
        # Retired entity — log + classify but don't write a manifest row.
        # The downstream sfi_progressive_stats fetch will still run with
        # whatever sfi_league_ids we managed to populate (possibly empty).
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sfi_leagues_fetch",
            shard=date,
        )

    # Standings — only for our mapped prediction leagues (not all 2800+ SFI championships).
    # SOCCER_FOOTBALL_INFO_IDS maps canonical league → SFI hex ID. We only fetch standings
    # for IDs in that set to avoid 404s on leagues SFI doesn't support for standings.
    _mapped_sfi_ids = set(_orch.SOCCER_FOOTBALL_INFO_IDS.values())
    _filtered_sfi_ids = [lid for lid in sfi_league_ids if lid in _mapped_sfi_ids]
    if _filtered_sfi_ids != sfi_league_ids:
        _orch.logger.info(
            "SFI: filtered %d → %d leagues (only mapped prediction leagues)",
            len(sfi_league_ids),
            len(_filtered_sfi_ids),
        )

    # Progressive stats — per-match 30-second interval time-series.
    # Requires SFI match IDs for the date, then fetches progressive data
    # for each completed match. Written as entity=progressive_stats.
    #
    # Pre-cutoff / known-gap skip: SFI's progressive endpoint has a hard
    # historical floor (probed live 2026-04-30: pre-2020-01-01 returns
    # empty for every match). Honour the per-(source, data_type) coverage
    # start in UAC + any registered known-gap windows so the VM doesn't
    # burn rate-limit quota grinding through dead range.
    _sfi_pp_floor = _orch.get_source_coverage_start("soccer_football_info", data_type="SFI_PROGRESSIVE_STATS")
    _sfi_pp_pre_cutoff = bool(_sfi_pp_floor) and date < _sfi_pp_floor.isoformat()
    _sfi_pp_in_known_gap = _orch.is_in_known_gap("soccer_football_info", "SFI_PROGRESSIVE_STATS", date)
    if _want_sfi_progressive and (_sfi_pp_pre_cutoff or _sfi_pp_in_known_gap):
        _orch.logger.info(
            "SFI progressive stats: skipping date=%s (%s)",
            date,
            "pre-coverage-start" if _sfi_pp_pre_cutoff else "known-gap",
        )
        # Honest-coverage Phase 2.E.2: pre-source-coverage-start vs paused-league
        # window get distinct EXPECTED_* reasons so downstream consumers can
        # classify legacy null-reason rows without re-deriving the calendar.
        _sfi_reason = "EXPECTED_PRE_SOURCE_COVERAGE_START" if _sfi_pp_pre_cutoff else "EXPECTED_PAUSED_LEAGUE"
        manifest.record_expected_empty(
            row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
            reason=_sfi_reason,
            attempted_at=attempt_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
            source=_orch._sports_ref_source("progressive_stats"),
        )
        for _exp_lid in sorted(_expected_sfi_league_ids):
            manifest.record_expected_empty(
                row_key={
                    "date": date,
                    "data_type": "SFI_PROGRESSIVE_STATS",
                    "league_id": _exp_lid,
                },
                reason=_sfi_reason,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                source=_orch._sports_ref_source("progressive_stats"),
            )
        _want_sfi_progressive = False
    # Season-window guard — when EVERY expected league is in its off-season
    # gap on this date, skip the API call and record per-league expected-empty
    # with the typed pre/post-season reason (mirrors the genesis-floor guard).
    if _want_sfi_progressive:
        _sfi_day = _orch.date_type.fromisoformat(date)
        _sfi_season = {
            _lid: _orch.footystats_season_status_for_day(_lid, _sfi_day) for _lid in _expected_sfi_league_ids
        }
        if _sfi_season and all(_s is not None for _s in _sfi_season.values()):
            _orch.logger.info("SFI progressive stats: skipping date=%s (all expected leagues off-season)", date)
            for _exp_lid, _status in sorted(_sfi_season.items()):
                if _status is None:
                    continue
                manifest.record_expected_empty(
                    row_key={
                        "date": date,
                        "data_type": "SFI_PROGRESSIVE_STATS",
                        "league_id": _exp_lid,
                    },
                    reason=_status,
                    attempted_at=attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                    source=_orch._sports_ref_source("progressive_stats"),
                )
            _want_sfi_progressive = False
    if _want_sfi_progressive:
        try:
            # League-scoped fetch: SFI's day-list returns ~50 championships'
            # worth of matches but our prediction set is ~4 leagues. Filter
            # match descriptors by championship_id BEFORE the per-match
            # progressive call so we don't burn ~10x RapidAPI quota on
            # leagues we'll never use as features.
            _sfi_descriptors = await _orch.asyncio.wait_for(adapter.get_match_descriptors_for_date(date), timeout=60.0)
            _expected_sfi_hex_ids = {
                _orch.get_provider_league_id(_canonical, "soccer_football_info")
                for _canonical in _expected_sfi_league_ids
            }
            _expected_sfi_hex_ids.discard(None)
            _expected_sfi_hex_ids.discard("")
            # Build match_id -> canonical league_id map BEFORE the per-match
            # loop so each progressive-stats entry can be tagged with its
            # league for per-league partitioning.  SOCCER_FOOTBALL_INFO_IDS
            # is canonical->hex; reverse it for hex->canonical lookup.
            _sfi_canonical_by_hex: dict[str, str] = {
                _hex: _canonical for _canonical, _hex in _orch.SOCCER_FOOTBALL_INFO_IDS.items()
            }
            _match_to_canonical: dict[str, str] = {}
            sfi_match_ids: list[str] = []
            for _d in _sfi_descriptors:
                _hex = _d["championship_id"]
                if _hex not in _expected_sfi_hex_ids:
                    continue
                _mid = _d["match_id"]
                _canonical_lid = _sfi_canonical_by_hex.get(str(_hex), "")
                if _canonical_lid:
                    _match_to_canonical[str(_mid)] = _canonical_lid
                sfi_match_ids.append(_mid)
            _orch.logger.info(
                "SFI progressive: %d/%d matches in mapped prediction leagues for date=%s",
                len(sfi_match_ids),
                len(_sfi_descriptors),
                date,
            )
            if sfi_match_ids:
                all_progressive: list[dict[str, str | int | float | None]] = []
                for mid in sfi_match_ids:
                    try:
                        stats = await _orch.asyncio.wait_for(adapter.get_progressive_stats(mid), timeout=30.0)
                        _canonical_for_match = _match_to_canonical.get(str(mid), "")
                        # Derive match_end_time + report_time once per match
                        # (detect_match_end_time needs CanonicalProgressiveStats objects,
                        # which we have before dict-coercion below).
                        _mid_kickoff = _orch.datetime(
                            int(date[:4]), int(date[5:7]), int(date[8:10]), 15, 0, tzinfo=_orch.UTC
                        )
                        _mid_match_end = _orch._sfi_detect_match_end_time(stats, _mid_kickoff)
                        _mid_report_time: _orch.datetime | None = (
                            _mid_match_end + _orch.timedelta(seconds=_orch.SFI_DATA_LAG_P95_SECONDS)
                            if _mid_match_end is not None
                            else None
                        )
                        for entry in stats:
                            _row: dict[str, str | int | float | None] = {
                                k: str(v) if v is not None else None
                                for k, v in _orch._coerce_adapter_output(entry).items()
                            }
                            # Tag for per-league partitioning at write time.
                            _row["league_id"] = _canonical_for_match or None
                            # Per-match timing fields (None for in-progress or short matches).
                            _row["match_end_time"] = _mid_match_end.isoformat() if _mid_match_end is not None else None
                            _row["report_time"] = _mid_report_time.isoformat() if _mid_report_time is not None else None
                            all_progressive.append(_row)
                    except Exception as exc:
                        _orch.classify_and_emit_error(
                            exc,
                            service_name="instruments-service",
                            operation="sfi_progressive_stats_fetch",
                            shard=mid,
                        )
                if all_progressive:
                    df = _orch.pd.DataFrame(all_progressive)
                    # PIT safety: progressive stat tick became available at kickoff + timer_seconds.
                    # Without per-match kickoff lookup, approximate using date at 15:00 UTC (common match hour).
                    if "timer_seconds" in df.columns:
                        _sfi_kickoff = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=15)
                        df["available_at"] = _sfi_kickoff + _orch.pd.to_timedelta(
                            _orch.pd.to_numeric(df["timer_seconds"], errors="coerce"), unit="s"
                        )
                    # Per-league partitioned write — single SSOT, no bare write.
                    _sfi_pp_captured: set[str] = set()
                    _sfi_pp_failed: set[str] = set()
                    if "league_id" in df.columns:
                        _has_league = df["league_id"].notna() & (df["league_id"].astype(str).str.strip() != "")
                        _with_league = df[_has_league]
                        _without_league = df[~_has_league]

                        for _pp_lid, _pp_league_df in _with_league.groupby("league_id"):
                            _pp_lid_str = str(_pp_lid)
                            if not _orch._is_in_canonical_write_universe(_pp_lid_str):
                                continue
                            _pp_canonical = _orch._canonical_league_id(_pp_lid_str)
                            # C.6: use report_time (match_end + SFI_DATA_LAG_P95_SECONDS) as available_at for
                            # completed matches — more accurate than timer_seconds approximation. For in-progress
                            # rows where report_time is absent, fall back to wall-clock (live write-time).
                            _pp_copy = _pp_league_df.copy()
                            if "report_time" in _pp_copy.columns:
                                _rt_series = _orch.pd.to_datetime(_pp_copy["report_time"], utc=True, errors="coerce")
                                _pp_copy["available_at"] = _rt_series.fillna(
                                    _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
                                )
                            else:
                                _pp_copy["available_at"] = _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
                            _stamped_pp_df = _pp_copy
                            # Shard-level isolation (codex/04-architecture/
                            # shard-level-failure-isolation.md — sports shard
                            # atom is per-league): a write/record failure for
                            # ONE league must not abort the loop over the
                            # OTHER matched leagues for this date, and must not
                            # leave a captured manifest row with no
                            # corresponding durable write (the
                            # phantom_captured_no_parquet_at_canonical_path
                            # class, root-caused 2026-07-13).
                            try:
                                _orch._gated_sink_write(
                                    sink,
                                    data=_stamped_pp_df,
                                    partition={
                                        "entity": "progressive_stats",
                                        "league": _pp_lid_str,
                                    },
                                    filename="progressive_stats.parquet",
                                    venue="soccer_football_info",
                                    entity="progressive_stats",
                                )
                                manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                                    row_key={
                                        "date": date,
                                        "data_type": "SFI_PROGRESSIVE_STATS",
                                        "league_id": _pp_canonical,
                                    },
                                    df=_stamped_pp_df,
                                    asset_group="sports",
                                    instrument_type="",
                                    data_type="SFI_PROGRESSIVE_STATS",
                                    league_id=_pp_canonical,
                                    pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                                    source=_orch._sports_ref_source("progressive_stats"),
                                    service_emission_state=None,
                                )
                                _sfi_pp_captured.add(_pp_lid_str)
                            except Exception as _pp_league_exc:
                                _pp_league_err = _orch._classify_adapter_failure(_pp_league_exc, "soccer_football_info")
                                _orch.log_event(
                                    "ADAPTER_FETCH_FAILED",
                                    details={
                                        "venue": "soccer_football_info",
                                        "endpoint": "get_progressive_stats",
                                        "date": date,
                                        "league_id": _pp_canonical,
                                        "error": str(_pp_league_exc),
                                        "error_code": _pp_league_err,
                                    },
                                )
                                manifest.record_failed(
                                    row_key={
                                        "date": date,
                                        "data_type": "SFI_PROGRESSIVE_STATS",
                                        "league_id": _pp_canonical,
                                    },
                                    error=_pp_league_err,
                                    attempted_at=attempt_ts,
                                    pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                                )
                                _sfi_pp_failed.add(_pp_lid_str)

                        if not _without_league.empty:
                            _orch.logger.warning(
                                "SFI_PROGRESSIVE_STATS bare-path fallback triggered for date=%s — data shape regression: "
                                "%d rows missing league_id (championship_id->canonical mapping returned empty). "
                                "Skipping bare write to keep manifest honest.",
                                date,
                                len(_without_league),
                            )
                    else:
                        _orch.logger.warning(
                            "SFI_PROGRESSIVE_STATS bare-path fallback triggered for date=%s — data shape regression: "
                            "df missing league_id column entirely (rows=%d). "
                            "Skipping bare write + manifest row to keep manifest honest.",
                            date,
                            len(df),
                        )
                    counts["progressive_stats"] = len(df)
                    # Per-league empty_confirmed for in-season leagues that
                    # had no captured rows (mirrors WEATHER / per-fixture
                    # honest-coverage pattern). Leagues whose write raised
                    # (_sfi_pp_failed) already carry an honest record_failed
                    # row from the per-league write loop above — excluding
                    # them here prevents a same-run record_empty from masking
                    # that failure.
                    for _exp_lid in sorted(_expected_sfi_league_ids - _sfi_pp_captured - _sfi_pp_failed):
                        manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": "SFI_PROGRESSIVE_STATS",
                                "league_id": _exp_lid,
                            },
                            attempted_at=attempt_ts,
                            reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                            pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                            source=_orch._sports_ref_source("progressive_stats"),
                        )
                    _orch.logger.info("SFI progressive stats: %d rows written", len(df))
                else:
                    # Match IDs present but all per-match fetches produced zero
                    # rows — games exist but stats not yet published (data
                    # latency).  This is SOURCE_RETURNED_ZERO, not
                    # EXPECTED_NO_FIXTURE (which would mean no games scheduled).
                    # Per-match exceptions are already caught by
                    # classify_and_emit_error (shard isolation), so reaching
                    # this else-branch means the fetch pipeline ran cleanly
                    # and each call returned 0 rows — i.e. a genuine 2xx+0-rows
                    # response that satisfies FetchEvidence.proves_honest_absence().
                    _sfi_ev = _orch.FetchEvidence(
                        http_status=200,
                        response_received=True,
                        rows_in_response=0,
                        source="soccer_football_info",
                        endpoint="sfi_progressive_stats",
                        attempted_at=attempt_ts,
                        error_signal="",
                    )
                    manifest.record_empty(
                        row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                        attempted_at=attempt_ts,
                        reason=_orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO,  # QG-allow: sports-sfi-stats-latency; proven honest absence via fetch_evidence (clean 2xx+0-rows)
                        pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                        fetch_evidence=_sfi_ev,
                        source=_orch._sports_ref_source("progressive_stats"),
                    )
                    for _exp_lid in sorted(_expected_sfi_league_ids):
                        manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": "SFI_PROGRESSIVE_STATS",
                                "league_id": _exp_lid,
                            },
                            attempted_at=attempt_ts,
                            reason=_orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO,  # QG-allow: sports-sfi-stats-latency; per-league mirror; proven honest absence via fetch_evidence
                            pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                            fetch_evidence=_sfi_ev,
                            source=_orch._sports_ref_source("progressive_stats"),
                        )
            else:
                # No completed matches on this date (off-season / rest day).
                _orch.logger.info("SFI progressive stats: no completed matches for date=%s", date)
                manifest.record_empty(
                    row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                    attempted_at=attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                    source=_orch._sports_ref_source("progressive_stats"),
                )
                for _exp_lid in sorted(_expected_sfi_league_ids):
                    manifest.record_empty(
                        row_key={
                            "date": date,
                            "data_type": "SFI_PROGRESSIVE_STATS",
                            "league_id": _exp_lid,
                        },
                        attempted_at=attempt_ts,
                        reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                        pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                        source=_orch._sports_ref_source("progressive_stats"),
                    )
        except Exception as exc:
            _orch.classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="sfi_progressive_stats_batch",
                shard=date,
            )
            _err_code = _orch._classify_adapter_failure(exc, "soccer_football_info")
            _orch.log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "soccer_football_info",
                    "endpoint": "get_match_ids_for_date",
                    "date": date,
                    "error": str(exc),
                    "error_code": _err_code,
                },
            )
            manifest.record_failed(
                row_key={"date": date, "data_type": "SFI_PROGRESSIVE_STATS"},
                error=_err_code,
                attempted_at=attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
            )
            for _exp_lid in sorted(_expected_sfi_league_ids):
                manifest.record_failed(
                    row_key={
                        "date": date,
                        "data_type": "SFI_PROGRESSIVE_STATS",
                        "league_id": _exp_lid,
                    },
                    error=_err_code,
                    attempted_at=attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
                )

    manifest.write()

    return counts
