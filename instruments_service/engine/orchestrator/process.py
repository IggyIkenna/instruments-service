"""process_instruments — the per-date / per-venue orchestration entrypoint.

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
    "process_instruments",
]


async def process_instruments(
    date: str | _orch.datetime,
    asset_groups: list[str],
    redo_all: bool = False,
    api_keys: dict[str, str] | None = None,
    venue_override: list[str] | None = None,
    mode: str = "batch",
    sports_entity_filter: str | None = None,
    sports_provider: str | None = None,
    league_filter: list[str] | None = None,
    season_override: int | None = None,
    recovery_fixture_ids: frozenset[int] | None = None,
    source: str | None = None,
) -> dict[str, int]:
    """Process instruments for a single date and set of asset groups.

    Args:
        sports_provider: When set, only run this data provider (e.g. OPEN_METEO,
            API_FOOTBALL, TRANSFERMARKT). Maps to venue filter + entity scope.
        league_filter: When set, only process these canonical league IDs
            (e.g. ["EPL", "BUNDESLIGA"]). Default None = all prediction leagues.
        recovery_fixture_ids: af_fixture_id allowlist for targeted per-fixture
            recovery. When set, the per-fixture entity handlers
            (PLAYER_STATS / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS)
            filter fixture_ids to this set BEFORE calling api_football, and
            the per-league parquet writes do read-modify-write merges so
            existing fixtures' rows are preserved. Bypasses date-level
            pre-flight skip — already-captured (date, league) cells are
            still drilled into for these specific fixture_ids.

    Returns:
        Dict mapping venue → record count written.

    Raises:
        RuntimeError: If URDI returns zero total records (fail the shard).
    """
    _ = _orch.get_config()  # ensure config is initialized

    # Normalise date: BatchIO passes datetime objects from get_date_range(),
    # but all downstream code (URDI, date filter, partition keys) needs str YYYY-MM-DD.
    if isinstance(date, _orch.datetime):
        date = date.strftime("%Y-%m-%d")

    # venue_override bypasses category lookup when --venues filter is active (sharding)
    venues = venue_override if venue_override is not None else _orch.get_venues_for_asset_groups(asset_groups)

    # Track which sports entities are missing (set in skip-if-exists check).
    # Empty = fetch everything; non-empty = only fetch these specific entities.
    _sports_missing_entities: list[str] = []

    # Recovery-mode hint: when --recovery-fixture-ids is set, the per-provider
    # fetches (footystats / understat / sfi / open_meteo) need to bypass their
    # per-day/per-league pre-flight skip, because empty_confirmed phantom rows
    # would otherwise mask the dates we're trying to recover. Each provider's
    # fetch already has a ``force=...`` parameter that bypasses its skip; we
    # promote redo_all when recovery is active so the existing dispatch
    # ``force=redo_all`` propagates correctly.
    #
    # For api_football the orchestrator's per-fixture loop has its own
    # explicit allowlist filter (further down in _fetch_sports_reference_data),
    # so the redo_all promotion here is harmless — the allowlist is the
    # finer-grained scope.
    if recovery_fixture_ids is not None:
        if not redo_all:
            _orch.logger.info(
                "Recovery mode: promoting redo_all=True so per-provider per-day skip "
                "is bypassed (recovery_fixture_ids has %d af_fixture_ids)",
                len(recovery_fixture_ids),
            )
        redo_all = True

    # 1. Skip venues not yet launched
    active_venues = [v for v in venues if _orch.is_venue_available(v, date)]

    # --sports-provider: restrict to only this provider's venues
    if sports_provider:
        provider_venues = _orch._SPORTS_PROVIDER_VENUES.get(sports_provider)
        if provider_venues is None:
            _orch.logger.error(
                "Unknown --sports-provider: %s. Valid: %s", sports_provider, list(_orch._SPORTS_PROVIDER_VENUES)
            )
            return {}
        active_venues = [v for v in active_venues if v in provider_venues]
        _orch.logger.info("Sports provider filter: %s → venues %s", sports_provider, active_venues)

        # Enrichment provider short-circuits: skip ALL orchestrator logic
        # (URDI, API Football fixture fetch, etc.) and go straight to the
        # specific provider's fetch function. Each reads fixtures from GCS
        # (already fetched by API_FOOTBALL runs) and calls only its own API.
        _enrichment_providers = {"OPEN_METEO", "UNDERSTAT", "FOOTYSTATS", "TRANSFERMARKT", "SOCCER_FOOTBALL_INFO"}
        if sports_provider in _enrichment_providers:
            _orch.logger.info("%s short-circuit: skipping orchestrator for date=%s", sports_provider, date)
            primary_asset_group = asset_groups[0] if asset_groups else "SPORTS"
            bucket = _orch._get_instruments_bucket(primary_asset_group)
            if not bucket:
                _orch.logger.error("No bucket resolved for asset_group=%s", primary_asset_group)
                return {}
            _keys = api_keys or {}

            result: dict[str, int] = {}
            if sports_provider == "OPEN_METEO":
                result = await _orch._fetch_weather_data(date=date, bucket=bucket, api_key=_keys.get("open_meteo"))
            elif sports_provider == "UNDERSTAT":
                result = await _orch._fetch_understat_xg(date=date, bucket=bucket, force=redo_all)
                shots_result = await _orch._run_understat_shots_date(date=date, bucket=bucket, force=redo_all)
                result.update(shots_result)
            elif sports_provider == "FOOTYSTATS":
                fs_key = _keys.get("footystats")
                if not fs_key:
                    _orch.logger.warning("No footystats API key — skipping date=%s", date)
                    return {}
                _ef = sports_entity_filter
                if not _ef or _ef == "PREDICTIONS":
                    pred_result = await _orch._fetch_footystats_predictions(
                        date=date, api_key=fs_key, bucket=bucket, force=redo_all
                    )
                    result.update(pred_result)
                if not _ef or _ef == "MATCHES":
                    match_result = await _orch._fetch_footystats_matches(
                        date=date, api_key=fs_key, bucket=bucket, force=redo_all
                    )
                    result.update(match_result)
                if not _ef or _ef == "ODDS":
                    odds_result = await _orch._fetch_footystats_odds(
                        date=date, api_key=fs_key, bucket=bucket, force=redo_all
                    )
                    result.update(odds_result)
            elif sports_provider == "TRANSFERMARKT":
                tm_key = _keys.get("transfermarkt")
                if not tm_key:
                    _orch.logger.warning("No transfermarkt API key — skipping date=%s", date)
                    return {}
                # Transfermarkt teams are slow (33 leagues x 90s rate limit = ~50 min).
                # Per-league triggers: only fetch teams for leagues whose trigger dates
                # match TODAY (transfer windows, season boundaries) — not all 33 on
                # every trigger. Leagues list (metadata) is fast (1 API call), always fetched.
                _batch_dt = _orch.date_type.fromisoformat(date) if isinstance(date, str) else date
                _leagues_today = _orch.get_leagues_needing_refresh(_batch_dt)
                # CLI entity filter takes precedence; TRANSFERMARKT_LEAGUES retired 2026-05-05
                _tm_entity = sports_entity_filter
                if not _tm_entity:
                    _tm_entity = None
                if not _leagues_today and not sports_entity_filter:
                    _orch.logger.info(
                        "Transfermarkt: date=%s has no league triggers — skipping PLAYER_VALUES refresh", date
                    )
                elif _leagues_today and not sports_entity_filter:
                    _orch.logger.info(
                        "Transfermarkt: date=%s triggers %d leagues: %s", date, len(_leagues_today), _leagues_today
                    )
                # Derive the European-football season from the batch date: a league
                # season spans Aug-May by convention, so `season_year = d.year` when
                # d.month >= 8 else `d.year - 1`. CLI `--season` (season_override)
                # wins if explicitly set; otherwise we MUST derive here — passing
                # `season=None` to the adapter defaults to `datetime.now(UTC).year`
                # (= current year), which is a §5 data-crime for any historical
                # backfill (writes today's roster onto a 2023 date partition).
                _tm_season = (
                    season_override
                    if season_override is not None
                    else (_batch_dt.year if _batch_dt.month >= 8 else _batch_dt.year - 1)
                )
                result = await _orch._fetch_transfermarkt_data(
                    date=date,
                    api_key=tm_key,
                    bucket=bucket,
                    entity_filter=_tm_entity,
                    league_filter=_leagues_today if _leagues_today else None,
                    season=_tm_season,
                    force=redo_all,
                )
            elif sports_provider == "SOCCER_FOOTBALL_INFO":
                sfi_key = _keys.get("soccer_football_info")
                if not sfi_key:
                    _orch.logger.warning("No soccer_football_info API key — skipping date=%s", date)
                    return {}
                result = await _orch._fetch_sfi_data(
                    date=date,
                    api_key=sfi_key,
                    bucket=bucket,
                    entity_filter=sports_entity_filter,
                    force=redo_all,
                )
            else:
                result = {}

            _orch.logger.info("%s DONE for date=%s: %s", sports_provider, date, result)
            return result

    if not active_venues:
        _orch.logger.info("No active venues for date=%s asset_groups=%s", date, asset_groups)
        return {}

    # Sports entity lists — used by freshness check AND later fast-path logic,
    # so they must be defined unconditionally (not inside redo_all gate).
    is_sports_run = any(c.upper() in ("SPORTS", "ALL") for c in asset_groups)
    _sports_core_entities = [
        # LEAGUES retired 2026-05-07 (C.1 audit, manifest_migration_SUPERSEDED_2026_05_21).
        # UAC ``LeagueDefinition`` + ``provider_league_ids`` (FOOTYSTATS_SEASON_IDS,
        # FOOTYSTATS_HISTORICAL_SEASON_IDS, etc.) canonicalise the league refdata via
        # code commits — daily-cadence GCS dump was 3046 daily shards of identical
        # static data. Existing manifest rows flipped to empty_confirmed with
        # reason=EXPECTED_DEPRECATED_DATA_TYPE via the migration script in
        # instruments-service/scripts/migrate_leagues_kill_2026_05_07.py.
        "TEAMS",
        "STANDINGS",
        "INJURIES",
    ]
    _sports_per_fixture_entities = [
        "FIXTURE_STATS",
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "PLAYER_STATS",
    ]

    # Tracks which sports entities the manifest says are missing.
    # Populated by the freshness check; stays empty when --force is set.
    _sports_missing_entities: list[str] = []

    # 1b. Skip-if-exists: check manifest for fresh data (unless --force)
    if not redo_all:
        primary_asset_group = asset_groups[0] if asset_groups else None
        bucket = _orch._get_instruments_bucket(primary_asset_group)

        # For SPORTS, require both core AND per-fixture reference entities.
        # Core: leagues/teams/standings/injuries (slow-moving, fetched every run).
        # Per-fixture: fixture_stats/events/lineups/player_stats (one API call per
        # completed fixture, rate-limited to 1 req/sec — expensive to re-fetch).
        # Remap venue names to match manifest data_type entries (API_FOOTBALL → FIXTURES).
        expected = ["FIXTURES" if v == "API_FOOTBALL" else v for v in active_venues]
        _active_venues_set_freshness = set(active_venues)
        # Enrichment entity → venue that produces it.
        # Only include in expected[] when that venue is in active_venues
        # (respects --venues filter so API_FOOTBALL-only runs don't wait on
        # SFI/Transfermarkt/Understat/Weather manifest entries).
        _enrichment_entity_venues: list[tuple[str, str]] = [
            ("MATCHES", "FOOTYSTATS"),
            ("PREDICTIONS", "FOOTYSTATS"),
            ("XG", "UNDERSTAT"),
            ("PLAYER_VALUES", "TRANSFERMARKT"),
            ("SFI_PROGRESSIVE_STATS", "SOCCER_FOOTBALL_INFO"),
            ("WEATHER", "OPEN_METEO"),
        ]
        if is_sports_run:
            expected.extend(_sports_core_entities)
            expected.extend(_sports_per_fixture_entities)

            # League-aware enrichment: only expect an enrichment entity if
            # the leagues it covers have fixtures on this date.  Read the
            # manifest index once to get leagues with FIXTURES on this date.
            #
            # Performance: the in-process index cache is invalidated after every
            # ``manifest.write()`` call (manifest_writer.py:_invalidate_index_cache).
            # In the per-date BatchIO loop the orchestrator writes manifest at
            # the END of each date's work, so the next date's read_availability_index
            # call here misses the cache → re-reads the full 25MB / 2.6M-row
            # canonical → ~27s GCS pull. That dominates wall-clock for ALL
            # multi-date sports backfills.
            #
            # Skip this read entirely when scope is already explicit:
            #   * ``sports_entity_filter`` set → entity-scoped run, ``expected``
            #     gets restricted to that one entity later anyway (line 1221)
            #   * ``recovery_fixture_ids`` set → targeted recovery, the allowlist
            #     IS the date-aware scope; no need to introspect the manifest.
            # Both signals mean the league-aware enrichment expectations don't
            # change the orchestration outcome — we already know what to fetch.
            _date_fixture_leagues: set[str] = set()
            _scope_is_explicit = bool(sports_entity_filter) or recovery_fixture_ids is not None
            if not _scope_is_explicit:
                _index_df = _orch.read_availability_index(bucket)
                if not _index_df.empty and "league_id" in _index_df.columns:
                    _fix_mask = (_index_df["date"] == date) & (_index_df["data_type"] == "FIXTURES")
                    _lid_series = _index_df.loc[_fix_mask, "league_id"].dropna()
                    _date_fixture_leagues = {str(lid).upper() for lid in _lid_series.unique() if str(lid).strip()}
            else:
                _orch.logger.debug(
                    "date=%s: skipping per-date read_availability_index — scope is explicit "
                    "(sports_entity_filter=%s, recovery_fixture_ids=%s)",
                    date,
                    sports_entity_filter,
                    "set" if recovery_fixture_ids is not None else "unset",
                )

            for entity, venue in _enrichment_entity_venues:
                if venue not in _active_venues_set_freshness:
                    continue
                # Check league coverage — skip entity if its covered leagues
                # have no fixtures on this date. With explicit scope, we skip
                # the league check (no fixture data loaded) and let the
                # downstream sports_entity_filter restriction below scope us.
                coverage = _orch.get_entity_league_coverage(entity)
                if coverage is not None and _date_fixture_leagues and not coverage & _date_fixture_leagues:
                    _orch.logger.debug(
                        "date=%s: skipping %s from expected — no fixture from covered leagues %s",
                        date,
                        entity,
                        sorted(coverage),
                    )
                    continue
                expected.append(entity)

        # Entity-scoped VM: when --sports-entity is set, restrict expected[] to just
        # that one entity. This makes the freshness check and all fetches single-entity,
        # allowing 17 parallel VMs (one per manifest entity type) instead of 8 year VMs.
        if sports_entity_filter and is_sports_run:
            expected = [sports_entity_filter]
            _sports_core_entities = [e for e in _sports_core_entities if e == sports_entity_filter]
            _sports_per_fixture_entities = [e for e in _sports_per_fixture_entities if e == sports_entity_filter]
            _orch.logger.info("Entity-scoped mode: restricting to %s only", sports_entity_filter)

        # Historical dates (>7 days ago) have immutable data — completed fixtures
        # and reference data don't change retroactively. Use max_age_hours=0 so the
        # freshness check only fails on schema version mismatch, not on timestamp.
        # The 24h default is correct for live/today runs where data updates daily.
        _date_cutoff = (_orch.datetime.now(_orch.UTC) - _orch.timedelta(days=7)).strftime("%Y-%m-%d")
        _freshness_max_age = 0.0 if date < _date_cutoff else 24.0

        # Sports per-league entities (FIXTURES + PREDICTIONS + MATCHES + ODDS +
        # 5 per-fixture downstreams + ...) write one manifest row per
        # (date, data_type, league_id). The coarse `check_shard_freshness`
        # only checks "is data_type present for this date" — once any league
        # has e.g. FIXTURES for date X, the whole date is "fresh" and
        # skipped, so other-league missing rows never get re-fetched.
        # Per-league freshness lives in the entity handlers themselves
        # (`_should_skip_date_for_per_league`); skip the coarse pre-flight
        # for these so the per-entity handlers run. Reference incident
        # 2026-05-06: phantom-recovery DELETE of 100k per-(date, league)
        # FIXTURES rows still got skipped because legitimate captures for
        # OTHER leagues kept the date "fresh" at the coarse level.
        _sports_per_league_entities: frozenset[str] = frozenset(
            {
                "FIXTURES",
                "PREDICTIONS",
                "MATCHES",
                "ODDS",
                "STANDINGS",
                "TEAMS",
                "INJURIES",
                "FIXTURE_STATS",
                "FIXTURE_EVENTS",
                "FIXTURE_LINEUPS",
                "PLAYER_STATS",
                "XG",
                "PLAYER_VALUES",
                "SFI_PROGRESSIVE_STATS",
                "WEATHER",
                "ODDS_HORIZON_BUCKET",
            }
        )
        _has_sports_per_league_in_scope = bool(set(expected) & _sports_per_league_entities)

        if is_sports_run and _has_sports_per_league_in_scope:
            # Defer to per-league checks in the entity handlers. Treat all
            # expected entities as "missing" at the date level so the
            # downstream per-entity dispatch fires; each handler does its
            # own per-league `_should_skip_date_for_per_league`.
            is_fresh = False
            stale = []
            missing = list(expected)
            _orch.logger.info(
                "date=%s: deferring pre-flight to per-league entity handlers (sports per-league mode; expected=%s)",
                date,
                expected,
            )
        else:
            is_fresh, stale, missing = _orch.check_shard_freshness(
                bucket=bucket,
                date=date,
                service_name="instruments-service",
                expected_venues=expected,
                max_age_hours=_freshness_max_age,
            )
        if is_fresh:
            _orch.logger.info(
                "SKIP date=%s: all %d venues/entities already fresh in manifest (use --force to re-fetch)",
                date,
                len(expected),
            )
            return {}

        # Per-entity skip: pass the exact missing list so _fetch_sports_reference_data
        # only fetches entities that are actually absent from the manifest.
        if is_sports_run and missing:
            _sports_missing_entities = list(missing)
            missing_set = set(missing)
            core_missing = missing_set & set(_sports_core_entities)
            pf_missing = [e for e in _sports_per_fixture_entities if e in missing_set]
            instruments_missing = missing_set - set(_sports_core_entities) - set(_sports_per_fixture_entities)
            _orch.logger.info(
                "date=%s: per-entity breakdown — %d core missing (%s), %d per-fixture missing (%s), %d instruments missing",
                date,
                len(core_missing),
                sorted(core_missing),
                len(pf_missing),
                pf_missing,
                len(instruments_missing),
            )
            # If only per-fixture entities are missing (core + instruments done),
            # skip the expensive URDI fetch and jump to enrichment.
            if not core_missing and not instruments_missing and pf_missing:
                _orch.logger.info(
                    "date=%s: core entities fresh — enrichment-only mode for %s",
                    date,
                    pf_missing,
                )

        if stale or missing:
            _orch.logger.info(
                "date=%s: %d stale + %d missing venues/entities — will re-fetch (stale=%s, missing=%s)",
                date,
                len(stale),
                len(missing),
                stale[:5],
                missing[:5],
            )

    # Fast path: if only specific sports entities are missing (instruments done),
    # skip URDI fetch and jump to targeted sports enrichment.
    # Two sub-cases:
    #   A) Only per-fixture entities missing (core + instruments done)
    #   B) Only core entities missing (e.g. injuries only — instruments done)
    # Both skip the expensive URDI fetch and go straight to _fetch_sports_reference_data.
    if _sports_missing_entities and api_keys:
        missing_set = set(_sports_missing_entities)
        # Enrichment entities (XG, Transfermarkt, FootyStats, SFI, Weather) can
        # read existing fixtures from GCS — they don't need a URDI fetch.
        _enrichment_entity_names = {e for e, _ in _enrichment_entity_venues}
        instruments_missing = (
            missing_set - set(_sports_core_entities) - set(_sports_per_fixture_entities) - _enrichment_entity_names
        )
        # Fast path fires when only core/per-fixture/enrichment entities are missing
        # (no actual instrument records to fetch from URDI)
        if not instruments_missing:
            api_football_key = api_keys.get("api_football")
            if api_football_key:
                primary_asset_group = asset_groups[0] if asset_groups else None
                bucket = _orch._get_instruments_bucket(primary_asset_group)
                # Resolve fixture IDs from existing GCS fixtures parquet (0 API calls)
                gcs_fixture_ids = _orch._read_fixture_ids_from_gcs(bucket, date)
                _orch.logger.info(
                    "ENRICHMENT-ONLY date=%s: %d fixture IDs from GCS, fetching %s",
                    date,
                    len(gcs_fixture_ids),
                    _sports_missing_entities,
                )
                # Create manifest writer so _fetch_sports_reference_data can write
                # per-league manifest entries for injuries and per-fixture entities.
                sports_manifest = _orch.ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                sports_ref_counts = await _orch._fetch_sports_reference_data(
                    date=date,
                    api_key=api_football_key,
                    bucket=bucket,
                    entities_to_fetch=_sports_missing_entities,
                    fixture_ids_override=gcs_fixture_ids,
                    manifest=sports_manifest,
                    recovery_fixture_ids=recovery_fixture_ids,
                    redo_all=redo_all,
                )
                # Write manifest for entities that did NOT write their own
                # manifest entries inside _fetch_sports_reference_data.
                _self_manifested_enr = {
                    "injuries",
                    "fixture_stats",
                    "fixture_events",
                    "fixture_lineups",
                    "player_stats",
                }
                for entity_name, row_count in sports_ref_counts.items():
                    if entity_name not in _self_manifested_enr:
                        sports_manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                            row_key={"date": date, "data_type": entity_name.upper()},
                            total_rows=row_count,
                            # SP-10: single-entity sports-reference shard (one (date, data_type)
                            # row per core entity) — no per-root-cluster contract exists for this
                            # data_type, so the cluster gate is intentionally a no-op here. Adding
                            # an expectation with no authoritative source would false-fail real data.
                            expected_root_clusters={},
                            observed_clusters={"": row_count},
                            available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                            pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                            service_emission_state=None,
                        )
                # Honest-coverage: per-fixture entities on a 0-fixture date are
                # legitimately empty — record_empty so attempt_coverage_pct lifts
                # while capture_coverage_pct stays accurate.
                _enr_attempt_ts = _orch.datetime.now(_orch.UTC)
                if not gcs_fixture_ids:
                    for pf_entity in _sports_per_fixture_entities:
                        entity_short = pf_entity.replace("API_FOOTBALL_", "").lower()
                        if entity_short not in sports_ref_counts:
                            sports_manifest.record_empty(
                                row_key={
                                    "date": date,
                                    "data_type": pf_entity.replace("API_FOOTBALL_", "").upper(),
                                },
                                attempted_at=_enr_attempt_ts,
                                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                            )
                sports_manifest.write()
                _orch.logger.info(
                    "Enrichment-only manifest: %d entities for %s",
                    len(sports_ref_counts),
                    date,
                )
                return sports_ref_counts

    _orch.log_event(
        "PROCESSING_STARTED",
        details={"date": date, "asset_groups": asset_groups, "venue_count": len(active_venues)},
    )

    # Enrichment-only entities don't need URDI at all — they fetch by date,
    # not by fixture ID.  Skip the expensive URDI bootstrap entirely.
    _enrichment_only_entities = frozenset(
        {
            "XG",
            "MATCHES",
            "PREDICTIONS",
            "PLAYER_VALUES",
        }
    )
    # Per-fixture entities need fixture IDs but can read them from GCS
    # instead of making expensive URDI calls to API Football.
    _per_fixture_entities = frozenset(
        {
            "FIXTURE_EVENTS",
            "FIXTURE_LINEUPS",
            "FIXTURE_STATS",
            "PLAYER_STATS",
        }
    )
    _skip_urdi = sports_entity_filter in (_enrichment_only_entities | _per_fixture_entities)
    if sports_entity_filter in _enrichment_only_entities:
        _orch.logger.info(
            "Skipping URDI fetch — %s is an enrichment-only entity (fetches by date, not fixture ID)",
            sports_entity_filter,
        )
    elif sports_entity_filter in _per_fixture_entities:
        _orch.logger.info(
            "Skipping URDI fetch — %s will read fixture IDs from existing GCS fixtures",
            sports_entity_filter,
        )

    # 2. Fetch from URDI — sole external API path
    # api_keys injected from preflight() → validate_api_keys_for_venues() → Secret Manager
    # date passed so date-aware adapters (e.g. API-Football) can filter server-side
    #
    # DeFi batch optimisation: DeFi instruments are monotonically growing
    # (immutable contracts, never deleted). In batch mode, the universe is
    # fetched ONCE and cached — subsequent dates in the range just filter
    # by available_from_datetime. Non-DeFi venues are fetched fresh per date.
    records: list[_orch.InstrumentRecord] = []
    _retryable_venues: list[str] = []
    # Track venues where the adapter ran without error (even if 0 records returned).
    # Used by the completeness check to distinguish "adapter returned nothing for this
    # date range" (OK) from "adapter failed to respond" (completeness failure).
    _non_error_venues: set[str] = set()

    defi_venue_names = frozenset(_orch._DEFI_VENUES)
    if _skip_urdi:
        # Enrichment-only: empty the venue lists so URDI fetch loops are no-ops.
        defi_active: list[str] = []
        non_defi_active: list[str] = []
    else:
        defi_active = [v for v in active_venues if v in defi_venue_names]
        non_defi_active = [v for v in active_venues if v not in defi_venue_names]

    # DeFi: use cached universe (one API call for entire batch run)
    if defi_active and mode == "batch":  # noqa: L2-mode-seam — DeFi caching decision; design call pending per batch_live_symmetry Q3
        defi_records, defi_retryable = await _orch._get_or_fetch_defi_universe(
            defi_active, api_keys=api_keys, mode=mode
        )
        records.extend(defi_records)
        _retryable_venues.extend(defi_retryable)
        # All DeFi venues that aren't retryable ran OK (even if 0 records after date filter)
        _non_error_venues.update(v for v in defi_active if v not in defi_retryable)
    elif defi_active:
        # Live mode: always fetch fresh DeFi data (with monotonicity check)
        with _orch.SolanaCacheSession(), _orch.EvmCacheSession():
            defi_result = await _orch.fetch_instruments_for_all_venues(
                defi_active, api_keys=api_keys, date=date, mode=mode
            )
        defi_live_records = list(defi_result.records)
        _non_error_venues.update(v for v in defi_active if v not in {e.venue for e in defi_result.failed_venues})

        # Monotonicity check: retry regressed venues, then block any still below HWM
        hwm = _orch._get_defi_manifest_high_watermarks()
        if hwm:
            live_counts = _orch._count_per_venue(defi_live_records)
            regressed = [v for v, mx in hwm.items() if live_counts.get(v, 0) < mx]
            if regressed:
                retry_records = await _orch._retry_regressed_venues(regressed, api_keys, mode)
                retry_counts = _orch._count_per_venue(retry_records)
                for venue in regressed:
                    if retry_counts.get(venue, 0) > live_counts.get(venue, 0):
                        defi_live_records = [r for r in defi_live_records if r.venue != venue]
                        defi_live_records.extend(r for r in retry_records if r.venue == venue)
            # Final enforcement: block venues still below HWM from being written
            defi_live_records, blocked = _orch._enforce_defi_monotonicity(defi_live_records, hwm)
            if blocked:
                _orch.logger.error(
                    "DeFi live monotonicity: %d venue(s) BLOCKED: %s",
                    len(blocked),
                    sorted(blocked),
                )

        records.extend(defi_live_records)
        _retryable_venues.extend(defi_result.retryable_venues)

    # Non-DeFi: always fetch fresh (CeFi instruments change daily, TradFi has expiries)
    if non_defi_active:
        with _orch.SolanaCacheSession():
            non_defi_result = await _orch.fetch_instruments_for_all_venues(
                non_defi_active, api_keys=api_keys, date=date, mode=mode, source=source
            )
        records.extend(non_defi_result.records)
        _retryable_venues.extend(non_defi_result.retryable_venues)
        _non_error_venues.update(
            v for v in non_defi_active if v not in {e.venue for e in non_defi_result.failed_venues}
        )

    # Per-fixture URDI skip: read fixture IDs from GCS and jump to enrichment.
    # This avoids the URDI fetch + date filter which returns 0 for historical dates.
    if _skip_urdi and sports_entity_filter in _per_fixture_entities:
        primary_asset_group = asset_groups[0] if asset_groups else None
        _pf_bucket = _orch._get_instruments_bucket(primary_asset_group)
        gcs_fixture_ids = _orch._read_fixture_ids_from_gcs(_pf_bucket, date)
        if not gcs_fixture_ids and not recovery_fixture_ids:
            _orch.logger.info("Per-fixture GCS skip: no fixtures in GCS for date=%s, no recovery IDs provided", date)
            return {}
        if not gcs_fixture_ids and recovery_fixture_ids:
            _orch.logger.info(
                "Per-fixture GCS skip: no GCS fixtures for date=%s — using %d recovery IDs directly",
                date,
                len(recovery_fixture_ids),
            )
            gcs_fixture_ids = list(recovery_fixture_ids)
        api_football_key = api_keys.get("api_football") if api_keys else None
        if not api_football_key:
            _orch.logger.warning("Per-fixture backfill: no API Football key for date=%s", date)
            return {}
        _orch.logger.info(
            "Per-fixture GCS-based enrichment date=%s: %d fixture IDs from GCS, entity=%s",
            date,
            len(gcs_fixture_ids),
            sports_entity_filter,
        )
        pf_counts = await _orch._fetch_sports_reference_data(
            date=date,
            api_key=api_football_key,
            bucket=_pf_bucket,
            entities_to_fetch=[sports_entity_filter],
            fixture_ids_override=gcs_fixture_ids,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
        )
        pf_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=_pf_bucket)
        for entity_name, row_count in pf_counts.items():
            pf_manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                row_key={"date": date, "data_type": entity_name.upper()},
                total_rows=row_count,
                # SP-10: single-entity per-fixture sports shard (one (date, data_type) row per
                # entity count) — no per-root-cluster contract exists for this data_type, so the
                # cluster gate is intentionally a no-op. The genuinely-bundled per-fixture entities
                # (FIXTURE_STATS/EVENTS/LINEUPS/PLAYER_STATS) write per-league via record_captured,
                # not this counts path. Inventing an expectation here would false-fail real data.
                expected_root_clusters={},
                observed_clusters={"": row_count},
                available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                service_emission_state=None,
            )
        pf_manifest.write()
        return pf_counts

    # 3. Filter to instruments active on the requested date.
    # URDI adapters return the full historical instrument universe; this reduces
    # it to only instruments tradeable on the requested day.
    # Pass the DeFi venue set so the filter can warn on missing available_from_datetime.
    is_defi_run = any(c.upper() in ("DEFI", "ALL") for c in asset_groups)
    defi_venue_set: frozenset[str] | None = frozenset(_orch._DEFI_VENUES) if is_defi_run else None
    date_dt = _orch.datetime.fromisoformat(date).replace(tzinfo=_orch.UTC)
    records = _orch.filter_instruments_by_date(records, date_dt, defi_venues=defi_venue_set)
    _orch.logger.info(
        "Date filter %s: %d instruments active (from URDI fetch)",
        date,
        len(records),
    )

    # 3b. Enrich CeFi/DeFi instruments with timezone=UTC (24/7 markets).
    # TradFi instruments get timezone from the databento adapter's session metadata.
    _tradfi_set = frozenset(_orch._TRADFI_VENUES)
    for r in records:
        if r.timezone is None and r.venue not in _tradfi_set:
            r.timezone = "UTC"

    # Per-venue breakdown after date filter
    venue_counts: dict[str, int] = {}
    for r in records:
        v = getattr(r, "venue", "UNKNOWN") or "UNKNOWN"
        venue_counts[v] = venue_counts.get(v, 0) + 1
    for v in sorted(venue_counts):
        _orch.logger.info("  %s: %d instruments after date filter", v, venue_counts[v])

    # 3a. DeFi available_from_datetime coverage summary.
    # Counts how many DeFi instruments in the date-filtered set have a populated
    # available_from_datetime vs None. Low coverage indicates URDI adapters are not
    # returning on-chain creation timestamps and the date filter is permissive
    # (treating None as "always available").
    if is_defi_run and records:
        defi_records = [r for r in records if (getattr(r, "venue", "") or "").upper() in _orch._DEFI_VENUES]
        if defi_records:
            populated = sum(1 for r in defi_records if getattr(r, "available_from_datetime", None) is not None)
            total_defi = len(defi_records)
            pct = int(populated * 100 / total_defi)
            _orch.logger.info(
                "Date accuracy: %d/%d DeFi instruments have available_from_datetime populated (%d%% coverage)",
                populated,
                total_defi,
                pct,
            )

    # 3b. DEFI relevance filter: keep only instruments involving major liquid assets.
    # Whitelist is from config_reloaders.get_defi_major_assets() — defaults to
    # ETH/BTC/USDT/USDC and known derivatives; can be overridden via ConfigStore.
    if any(c.upper() in ("DEFI", "ALL") for c in asset_groups):
        before = len(records)
        records = _orch.filter_defi_instruments_by_relevance(records)
        _orch.logger.info(
            "DEFI relevance filter: %d → %d instruments (removed %d long-tail)",
            before,
            len(records),
            before - len(records),
        )

    # 4. Handle zero records.
    # For SPORTS: zero fixtures on a given day is normal (no matches scheduled).
    # Write an empty marker parquet so the manifest knows the day was processed
    # successfully and won't re-fetch without --force.
    # For DeFi in batch mode: zero records after date filter is expected for dates
    # before the first pool was created — skip silently (no GCS write, no error).
    # For CeFi/TradFi: zero records = something is broken → fail the shard.
    if not records:
        is_sports_only = all(c.upper() == "SPORTS" for c in asset_groups)
        if is_sports_only:
            primary_asset_group = asset_groups[0] if asset_groups else None
            bucket = _orch._get_instruments_bucket(primary_asset_group)
            sink = _orch.get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
            # Write one empty marker per prediction league so downstream
            # consumers see each league as "processed with 0 fixtures".
            #
            # Honest-coverage (CLAUDE.md "4 pillars" #1): we use
            # ``record_empty`` here, NOT ``add(row_count=0)``. Marking
            # zero-fixture days as ``captured`` with row_count=0 is the
            # exact anti-pattern the rule was added for — it inflates the
            # captured count and masks honest absence. Reference incident
            # 2026-05-06: AUSTRIAN_BUNDESLIGA, GREEK_SUPER_LEAGUE et al.
            # showed 3041 captured FIXTURES rows that were ALL phantoms
            # (instrument_count=0, no parquet on disk) before this fix.
            #
            # We also DROP the empty placeholder parquet write — empty
            # placeholders that look populated are worse than missing data
            # because they evade detection. If a date has no fixtures, no
            # parquet should exist; the manifest's ``empty_confirmed`` row
            # is the single honest marker.
            _empty_league_ids = league_filter if league_filter else _orch.get_all_prediction_league_ids()
            _empty_attempt_ts = _orch.datetime.now(_orch.UTC)
            _empty_manifest = _orch.ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            for _league_id in _empty_league_ids:
                _empty_manifest.record_empty(
                    row_key={
                        "date": date,
                        "data_type": "FIXTURES",
                        "league_id": _orch._canonical_league_id(_league_id),
                    },
                    attempted_at=_empty_attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                )
            _empty_manifest.write()
            _orch.logger.info(
                "SPORTS: No fixtures for date=%s — wrote empty_confirmed markers for %d leagues",
                date,
                len(_empty_league_ids),
            )
            # Still fetch sports reference data (leagues/teams/standings/injuries)
            # even when no fixtures exist. These are date-independent slow-moving
            # entities needed for downstream feature computation.
            if api_keys:
                api_football_key = api_keys.get("api_football")
                if api_football_key:
                    # Only fetch entities that are actually missing from the manifest.
                    # Create manifest writer so _fetch_sports_reference_data can write
                    # per-league manifest entries for injuries and per-fixture entities.
                    sports_manifest = _orch.ManifestWriter(
                        service_name="instruments-service",
                        catalogue_bucket=bucket,
                    )
                    sports_ref_counts = await _orch._fetch_sports_reference_data(
                        date=date,
                        api_key=api_football_key,
                        bucket=bucket,
                        entities_to_fetch=_sports_missing_entities if _sports_missing_entities else None,
                        fixture_ids_override=list(recovery_fixture_ids) if recovery_fixture_ids else [],
                        manifest=sports_manifest,
                        recovery_fixture_ids=recovery_fixture_ids,
                        redo_all=redo_all,
                    )
                    if sports_ref_counts:
                        _self_manifested_zf = {
                            "injuries",
                            "fixture_stats",
                            "fixture_events",
                            "fixture_lineups",
                            "player_stats",
                        }
                        for entity_name, row_count in sports_ref_counts.items():
                            if entity_name not in _self_manifested_zf:
                                if row_count > 0:
                                    sports_manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                                        row_key={"date": date, "data_type": entity_name.upper()},
                                        total_rows=row_count,
                                        # SP-10: single-entity sports-reference shard (one
                                        # (date, data_type) row per core entity) — no
                                        # per-root-cluster contract for this data_type, so the
                                        # cluster gate is a deliberate no-op; an unfounded
                                        # expectation would false-fail genuinely-captured data.
                                        expected_root_clusters={},
                                        observed_clusters={"": row_count},
                                        available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                                        pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                                        service_emission_state=None,
                                    )
                                else:
                                    # Honest-coverage: api returned 0 rows
                                    # → empty_confirmed, not captured-with-0.
                                    sports_manifest.record_empty(
                                        row_key={
                                            "date": date,
                                            "data_type": entity_name.upper(),
                                        },
                                        attempted_at=_orch.datetime.now(_orch.UTC),
                                        reason=_orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO,  # QG-allow: sports-entity-no-fixture-oracle; oracle=sports-fixture-lookup not available per entity_name grain; A10c-fleet followup required
                                        pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                                    )
                        # Per-fixture entities on zero-fixture dates: nothing
                        # to fetch (no fixtures = no per-fixture data). Write
                        # ``empty_confirmed`` markers so the orchestrator
                        # knows we attempted and skip-on-rerun without
                        # inflating the captured count (CLAUDE.md "4 pillars"
                        # #1: row_count > 0 OR record_empty, never
                        # ``captured`` with row_count=0).
                        for pf_entity in _sports_per_fixture_entities:
                            entity_short = pf_entity.lower()
                            if entity_short not in sports_ref_counts:
                                sports_manifest.record_empty(
                                    row_key={
                                        "date": date,
                                        "data_type": pf_entity,
                                    },
                                    attempted_at=_orch.datetime.now(_orch.UTC),
                                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                                )
                        sports_manifest.write()
                # Zero-fixture fast path: fixture-dependent enrichment entities get
                # 0-count manifest entries immediately (no API calls / rate limits).
                # Fixture-INDEPENDENT entities (Transfermarkt, SFI) are excluded —
                # they provide reference data (team values, standings) regardless
                # of whether matches are played on this date.
                _active_venues_set = set(active_venues)
                _enrichment_zero_entities: list[str] = []
                if "FOOTYSTATS" in _active_venues_set:
                    _enrichment_zero_entities += ["PREDICTIONS", "MATCHES"]
                if "UNDERSTAT" in _active_venues_set:
                    _enrichment_zero_entities += ["XG"]
                # NOTE: TRANSFERMARKT and SFI are NOT zero-gated — they provide
                # fixture-independent reference data (team values, standings).
                if "OPEN_METEO" in _active_venues_set:
                    _enrichment_zero_entities += ["WEATHER"]
                if _enrichment_zero_entities:
                    _enr_manifest = _orch.ManifestWriter(
                        service_name="instruments-service",
                        catalogue_bucket=bucket,
                    )
                    _enr_attempt_ts = _orch.datetime.now(_orch.UTC)
                    for _enr_entity in _enrichment_zero_entities:
                        # Honest-coverage: zero-fixture day → record_empty,
                        # NOT add(row_count=0). See CLAUDE.md "4 pillars" #1
                        # and AUSTRIAN_BUNDESLIGA phantom-row incident
                        # 2026-05-06.
                        _enr_manifest.record_empty(
                            row_key={
                                "date": date,
                                "data_type": _enr_entity,
                            },
                            attempted_at=_enr_attempt_ts,
                            reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                            pipeline_mode=_orch._pipeline_mode_for_sports_data_type(_enr_entity),
                        )
                    _enr_manifest.write()
                    _orch.logger.info(
                        "Zero-fixture fast path: wrote empty_confirmed for %d fixture-dependent entities on date=%s",
                        len(_enrichment_zero_entities),
                        date,
                    )

            # Fixture-independent reference data: fetch even on zero-fixture dates,
            # but ONLY on trigger dates (season start, transfer window open/close).
            # This avoids re-fetching identical squad data every day.
            counts: dict[str, int] = {}
            _active_venues_set = set(active_venues)
            _ef = sports_entity_filter

            def _entity_wanted_zf(ent: str) -> bool:
                return _ef is None or _ef == ent

            # Check if today is a reference refresh trigger for any league.
            _batch_date = _orch.date_type.fromisoformat(date)
            _is_trigger = _orch.is_any_league_refresh_date(_batch_date) or redo_all
            _trigger_leagues = _orch.get_leagues_needing_refresh(_batch_date) if _is_trigger else []
            if not _is_trigger:
                _orch.logger.info(
                    "date=%s: not a reference refresh trigger — skipping Transfermarkt/SFI team fetches",
                    date,
                )

            transfermarkt_key = (
                api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in _active_venues_set) else None
            )
            if not transfermarkt_key and "TRANSFERMARKT" in _active_venues_set:
                _orch.logger.warning(
                    "TRANSFERMARKT is active but no API key found — skipping for date=%s.",
                    date,
                )
            if transfermarkt_key and _is_trigger and _entity_wanted_zf("PLAYER_VALUES"):
                _orch.logger.info(
                    "Trigger-based Transfermarkt refresh for date=%s (leagues: %s)",
                    date,
                    _trigger_leagues[:5] if _trigger_leagues else "all (--force)",
                )
                try:
                    tm_counts = await _orch._fetch_transfermarkt_data(
                        date=date,
                        api_key=transfermarkt_key,
                        bucket=bucket,
                        entity_filter=_ef,
                        season=season_override,
                        force=redo_all,
                    )
                    for k, v in tm_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="transfermarkt_data_fetch",
                        shard=date,
                    )

            sfi_key = (
                api_keys.get("soccer_football_info")
                if (api_keys and "SOCCER_FOOTBALL_INFO" in _active_venues_set)
                else None
            )
            if not sfi_key and "SOCCER_FOOTBALL_INFO" in _active_venues_set:
                _orch.logger.warning(
                    "SOCCER_FOOTBALL_INFO is active but no API key found — skipping for date=%s.",
                    date,
                )
            if sfi_key and _entity_wanted_zf("SFI_PROGRESSIVE_STATS"):
                try:
                    sfi_counts = await _orch._fetch_sfi_data(
                        date=date,
                        api_key=sfi_key,
                        bucket=bucket,
                        entity_filter=_ef,
                        force=redo_all,
                    )
                    for k, v in sfi_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="sfi_data_fetch",
                        shard=date,
                    )

            _orch.log_event("PROCESSING_COMPLETED", details={"date": date, "asset_groups": asset_groups, "fixtures": 0})
            return counts
        # DeFi batch: zero records after date filter is normal for early dates
        # (venue exists in UAC but no pools created yet on-chain). Skip without error.
        is_defi_only = all(c.upper() in ("DEFI",) for c in asset_groups)
        if is_defi_only and mode == "batch":  # noqa: L2-mode-seam — DeFi pre-genesis early-exit; design call pending per batch_live_symmetry Q3
            _orch.logger.debug(
                "DeFi batch: zero instruments after date filter for date=%s — "
                "all venues pre-date their first pool creation. Skipping.",
                date,
            )
            return {}

        # TradFi non-trading day: zero instruments on weekends/holidays is expected.
        # Write 0-count manifest entries per venue so the manifest marks the day as
        # processed and won't re-fetch without --force. This prevents permanent gaps
        # in instrument data for every weekend and exchange holiday.
        tradfi_active = [v for v in active_venues if v in _orch._TRADFI_VENUES]
        if tradfi_active:
            target_dt = _orch.date_type.fromisoformat(date)
            non_trading_venues = [v for v in tradfi_active if _orch.is_non_trading_day(v, target_dt)]
            if non_trading_venues and len(non_trading_venues) == len(tradfi_active):
                primary_asset_group = asset_groups[0] if asset_groups else None
                bucket = _orch._get_instruments_bucket(primary_asset_group)
                manifest = _orch.ManifestWriter(
                    service_name="instruments-service",
                    catalogue_bucket=bucket,
                )
                _nt_attempt_ts = _orch.datetime.now(_orch.UTC)
                for venue in non_trading_venues:
                    # Honest-coverage Phase 2.E.2: discriminate weekend vs
                    # holiday so the manifest carries an EXPECTED_* row per
                    # (shard_key, day) instead of a bare empty_confirmed.
                    # instruments-service emits the TradFi non-trading-day
                    # marker on behalf of its own catalog refresh — the
                    # underlying tick source for TradFi venues (CME/NQ) is
                    # databento per UAC SOURCE_PRIORITY, but the manifest
                    # row here represents the instruments-service catalog's
                    # statement that no instruments exist for the day, so
                    # tag with BATCH_INSTRUMENTS_SERVICE.
                    _reason = _orch.non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
                    manifest.record_expected_empty(
                        row_key={"date": date, "venue": venue},
                        reason=_reason,
                        attempted_at=_nt_attempt_ts,
                        pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                    )
                manifest.write()
                _orch.logger.info(
                    "TRADFI non-trading day: date=%s venues=%s — wrote empty_confirmed manifest entries",
                    date,
                    sorted(non_trading_venues),
                )
                _orch.log_event(
                    "PROCESSING_COMPLETED",
                    details={
                        "date": date,
                        "asset_groups": asset_groups,
                        "non_trading_venues": sorted(non_trading_venues),
                    },
                )
                return dict.fromkeys(non_trading_venues, 0)

        # Active-day-zero diagnostic: TradFi venues that are trading today but
        # returned 0 records indicate a likely upstream data source issue (not a
        # calendar gap). Emit a structured WARN so oncall can distinguish adapter
        # failure from expected absence without inspecting raw adapter logs.
        if tradfi_active:
            target_dt = _orch.date_type.fromisoformat(date)
            _active_tradfi_zero = [v for v in tradfi_active if not _orch.is_non_trading_day(v, target_dt)]
            if _active_tradfi_zero:
                _orch.logger.warning(
                    "TRADFI active-day-zero: date=%s venues=%s returned 0 instruments on "
                    "a trading day — potential upstream adapter / connectivity issue",
                    date,
                    sorted(_active_tradfi_zero),
                )
                _orch.log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "date": date,
                        "venues": sorted(_active_tradfi_zero),
                        "reason": "active_day_zero_instruments",
                    },
                )
        msg = (
            f"URDI returned zero records for date={date} asset_groups={asset_groups}. "
            f"Venues attempted: {active_venues}. "
            "Check URDI adapter coverage and network connectivity."
        )
        _orch.logger.error("%s", msg)
        _orch.log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)

    # 5. Schema validation — per-record failure isolation (hard_schema_enforcement Phase 2).
    #    Invalid records route to SCHEMA_VALIDATION_FAILED event; valid records from the
    #    same venue continue to record_captured. A venue is added to validation_failed_venues
    #    only when ALL its records fail — per CLAUDE.md shard-level failure isolation rule
    #    (no raise inside per-record loop; bad row must not kill the whole shard).
    valid_records, rejected = _orch.validate_instrument_records(records, as_of_date=_orch.date_type.fromisoformat(date))
    validation_failed_venues: set[str] = set()
    if rejected:
        rejected_by_venue: dict[str, int] = {}
        for rec, reason in rejected:
            rejected_by_venue[rec.venue] = rejected_by_venue.get(rec.venue, 0) + 1
            _orch.logger.warning(
                "SCHEMA_VALIDATION_FAILED date=%s venue=%s instrument_key=%s reason=%s",
                date,
                rec.venue,
                rec.instrument_key,
                reason,
            )
            _orch.log_event(
                "SCHEMA_VALIDATION_FAILED",
                details={
                    "date": date,
                    "venue": rec.venue,
                    "instrument_key": rec.instrument_key,
                    "reason": reason,
                },
            )
        # Mark venue fully-failed only when zero valid records survive for it.
        valid_by_venue: dict[str, int] = {}
        for rec in valid_records:
            valid_by_venue[rec.venue] = valid_by_venue.get(rec.venue, 0) + 1
        for venue, failed_count in rejected_by_venue.items():
            if venue not in valid_by_venue:
                validation_failed_venues.add(venue)
                _orch.logger.error(
                    "SHARD FAILED date=%s venue=%s: all %d instruments failed validation",
                    date,
                    venue,
                    failed_count,
                )
    records = valid_records
    if not records:
        msg = f"All records rejected by schema validation for date={date}"
        _orch.logger.error("%s", msg)
        _orch.log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)

    rows = []
    for r in records:
        d = r.model_dump()
        # Serialize legs list[InstrumentLeg] → JSON string for parquet storage
        if d.get("legs") is not None:
            d["legs"] = _orch.json.dumps(d["legs"])
        rows.append(d)
    df = _orch.pd.DataFrame(rows)

    # 6. Domain validation — logs anomalies, doesn't raise for instruments domain
    _orch.DomainValidationService("instruments").validate_for_domain(df)

    # 6. Write per-venue parquet + catalogue + CSV sample
    # Pass config explicitly — _uc is read at call time so sampling honours
    # ENABLE_CSV_SAMPLING even when set after the singleton initialised.
    counts: dict[str, int] = {}
    sampler = _orch.create_sampling_service(
        {
            "enable_sampling": _orch._uc.enable_csv_sampling,
            "sample_size": _orch._uc.csv_sample_size,
            "sample_dir": _orch._uc.csv_sample_dir,
        }
    )
    # Use the first (primary) category to route to the correct category-specific bucket.
    # UCI naming: instruments-store-{category.lower()}-{project}
    # e.g. DEFI → instruments-store-defi-{gcp_project_id}
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _orch._get_instruments_bucket(primary_asset_group)
    # prefix ensures writes land at instrument_availability/by_date/{day=X}/{venue=Y}/
    sink = _orch.get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
    # Separate sink for prediction market lifecycle parquet (per predictions_master Phase 3 L618).
    # Path: market_lifecycle/by_canonical_group/group={g}/day={d}/market_lifecycle.parquet
    lifecycle_sink = _orch.get_data_sink(bucket=bucket, prefix="market_lifecycle/by_canonical_group")

    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            venue_str = str(venue_name)
            if venue_str == "API_FOOTBALL":
                # League-based sharding: partition sports fixtures by league_id.
                # instrument_key format: {LEAGUE}:{HOME}_v_{AWAY}:{DATE}
                # Extract league_id as the part before the first colon.
                _sports_df = venue_df.copy()
                _sports_df["_league_id"] = _sports_df["instrument_key"].str.split(":").str[0]
                # Apply league filter if set (--league CLI arg)
                if league_filter:
                    _sports_df = _sports_df[_sports_df["_league_id"].isin(league_filter)]
                _captured_lids: set[str] = set()
                for _lid, _league_df in _sports_df.groupby("_league_id"):
                    _league_id_str = str(_lid)
                    _captured_lids.add(_league_id_str)
                    _league_df_clean = _league_df.drop(columns=["_league_id"])
                    _stamped_fixture_df = _orch.stamp_available_at_explicit(
                        _league_df_clean, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_fixture_df,
                        partition={
                            "day": date,
                            "venue": venue_str,
                            "league": _orch._canonical_league_id(_league_id_str),
                        },
                        filename="instruments.parquet",
                        venue=venue_str,
                        entity="instruments",
                    )
                    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={
                            "date": date,
                            "data_type": "FIXTURES",
                            "league_id": _orch._canonical_league_id(_league_id_str),
                        },
                        df=_stamped_fixture_df,
                        asset_group="sports",
                        instrument_type="",
                        data_type="FIXTURES",
                        league_id=_orch._canonical_league_id(_league_id_str),
                        pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                        # FIXTURES is multi-source (api_football + footystats) →
                        # explicit source required (data_source_provenance Phase 4).
                        # This branch is the API_FOOTBALL venue (venue_str filter).
                        source="api_football",
                        service_emission_state=None,
                    )
                    counts[f"FIXTURES/{_league_id_str}"] = len(_league_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _league_df_clean,
                            filename_prefix=f"instruments_API_FOOTBALL_{_league_id_str}_{date}",
                        )

                # Honest-coverage: every league that is in-season on this date
                # but had zero fixtures gets a record_empty row. Without this,
                # mid-week gaps render as red "missing" in the data-status
                # drilldown even though the adapter ran and the API legitimately
                # returned zero for that league. Season window comes from UAC
                # get_league_fixture_calendar — only leagues whose season
                # actually covers this date are claimed empty.
                _fx_attempt_ts = _orch.datetime.now(_orch.UTC)
                _expected_af_lids = {
                    league.league_id for league in _orch.get_expected_leagues_for_source("api_football")
                }
                if league_filter:
                    _expected_af_lids &= set(league_filter)
                for _exp_lid in sorted(_expected_af_lids - _captured_lids):
                    if not _orch.get_league_fixture_calendar(_exp_lid, date, date):
                        continue
                    manifest.record_empty(
                        row_key={
                            "date": date,
                            "data_type": "FIXTURES",
                            "league_id": _exp_lid,
                        },
                        attempted_at=_fx_attempt_ts,
                        reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                        pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    )

            elif venue_str.upper() in ("POLYMARKET", "KALSHI") and "base_asset" in venue_df.columns:
                # PREDICTION: bundle by canonical_question_group per the UAC
                # SSOT (``BTC_UP_DOWN_HOURLY`` / ``BTC_UP_DOWN_DAILY`` /
                # ``SPX_UP_DOWN_DAILY`` / ``ELECTION_PRESIDENT_2028`` /
                # ``OTHER``, etc.). Recurring canonical groups cycle through
                # multiple condition_ids over time — HOURLY = ~24/day,
                # DAILY = 1/day — so the shard atom is per-(canonical_group,
                # day), with all market_ids active on that day bundled into
                # one parquet (analogous to options-chain bundling). Per
                # ``predictions_master.plan.md`` Phase 1
                # critical-path + CLAUDE.md "Per-asset-group shard-key
                # matrix → Prediction". Polymarket + Kalshi share this
                # path: both prediction venues classify per the UAC
                # ``classify_*_to_canonical_group`` SSOT and bundle on
                # the same axis so MTDS reads + features compute apply
                # identically.
                _pred_df = venue_df.copy()
                _pred_df["_canonical_group"] = _pred_df.apply(
                    _orch._extract_prediction_canonical_group,
                    axis=1,
                )
                _manifest_venue = venue_str.upper()
                for _group_raw, _group_df in _pred_df.groupby("_canonical_group"):
                    _group_str = str(_group_raw)
                    _group_df_clean = _group_df.drop(columns=["_canonical_group"])
                    # Manifest row: data_type=prediction_canonical_question_group
                    # (the bundled data_type per UAC BUNDLED_DATA_TYPES SSOT),
                    # underlying=<canonical_group> (the per-bundle cluster
                    # identity, mirroring options_chain root-bucketing).
                    _stamped_group_df = _orch.stamp_available_at_explicit(
                        _group_df_clean, when=_orch.datetime.now(_orch.UTC)
                    )
                    _orch._gated_sink_write(
                        sink,
                        data=_stamped_group_df,
                        partition={
                            "day": date,
                            "venue": venue_str,
                            "canonical_question_group": _group_str,
                        },
                        filename="instruments.parquet",
                        venue=venue_str,
                        entity="instruments",
                    )
                    _pred_pm = (
                        _orch.PipelineMode.BATCH_POLYMARKET_GAMMA_API
                        if _manifest_venue == "POLYMARKET"
                        else _orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE
                    )
                    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={
                            "date": date,
                            "data_type": "prediction_canonical_question_group",
                            "venue": _manifest_venue,
                            # Canonical v9 bundled-atom identity: the cqg lives in `instrument_id`
                            # (matches the MTDS record_captured_from_counts row_key + what
                            # deployment-api `_prediction_venue_detail` reads first). `underlying`
                            # is kept as the migration-window fallback deployment-api still reads.
                            "instrument_id": _group_str,
                            "underlying": _group_str,
                        },
                        df=_stamped_group_df,
                        asset_group="prediction",
                        instrument_type="",
                        data_type="prediction_canonical_question_group",
                        venue=_manifest_venue,
                        instrument_id=_group_str,
                        underlying=_group_str,
                        # SP-10: prediction canonical-question-group IS a genuine per-market
                        # bundle, and UAC expected_market_ids_for_canonical_group(group, day,
                        # lifecycles) is the authoritative expected-cluster source. Wiring it
                        # here requires threading the MARKET_LIFECYCLE rows for this
                        # (group, day) cell to the callsite (not currently loaded here), and
                        # this is the `prediction` asset_group — outside the SP-10 sports scope.
                        # Left {} for now; tracked as a P2 follow-up (see predictions_master.md).
                        expected_root_clusters={},
                        cluster_extractor=lambda s: s,
                        pipeline_mode=_pred_pm,
                        service_emission_state=None,
                    )
                    counts[f"{_manifest_venue}/{_group_str}"] = len(_group_df_clean)
                    if sampler.enable_sampling:
                        sampler.generate_csv_sample(
                            _group_df_clean,
                            filename_prefix=f"instruments_{_manifest_venue}_{_group_str}_{date}",
                        )
                    _orch._write_market_lifecycle(
                        sink=lifecycle_sink,
                        group_df=_group_df_clean,
                        canonical_group_str=_group_str,
                        date=date,
                        manifest_venue=_manifest_venue,
                        manifest=manifest,
                        pipeline_mode=_pred_pm,
                    )
            else:
                _orch._write_venue(venue_str, venue_df, date, bucket, sink, counts, sampler, manifest)
                # Phase 4.2 (tradfi_canonical_futures_contract_hard_required_fields_2026_05_13):
                # For TradFi futures venues (CME, ICE), also write CanonicalFuturesContract
                # records alongside the InstrumentRecord instruments.parquet.
                # build_futures_contracts() derives all 5 hard-required lifecycle dates
                # from InstrumentRecord.expiry using physical/cash-settled conventions.
                # Shard-level isolation: a write failure here does NOT abort the instruments
                # write — the futures_contracts.parquet is best-effort on the same date.
                if venue_str in frozenset(_orch._TRADFI_VENUES):
                    _venue_instrument_records = [r for r in records if r.venue == venue_str]
                    _orch._write_futures_contracts(
                        venue_str=venue_str,
                        instrument_records=_venue_instrument_records,
                        date=date,
                        bucket=bucket,
                        sink=sink,
                    )
    else:
        _orch._write_venue("all", df, date, bucket, sink, counts, sampler, manifest)

    # Write 0-count manifest entries for TRADFI venues that returned 0 instruments
    # because the date is a non-trading day (weekend/holiday). Without this, those
    # venues have no manifest entry and appear as permanent gaps in the data status.
    _tradfi_set_for_manifest = frozenset(_orch._TRADFI_VENUES)
    tradfi_empty = _non_error_venues - set(counts.keys())
    tradfi_empty = {v for v in tradfi_empty if v in _tradfi_set_for_manifest}
    if tradfi_empty:
        target_dt = _orch.date_type.fromisoformat(date)
        non_trading = {v for v in tradfi_empty if _orch.is_non_trading_day(v, target_dt)}
        if non_trading:
            _nt_attempt_ts = _orch.datetime.now(_orch.UTC)
            for venue in sorted(non_trading):
                # Honest-coverage Phase 2.E.2: discriminate weekend vs holiday
                # so the manifest carries an EXPECTED_* row per (shard_key, day).
                # See header note on TradFi non-trading day pipeline_mode: this
                # is the instruments-service catalog asserting absence; tag
                # with BATCH_INSTRUMENTS_SERVICE.
                _reason = _orch.non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
                manifest.record_expected_empty(
                    row_key={"date": date, "venue": venue},
                    reason=_reason,
                    attempted_at=_nt_attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                )
                counts[venue] = 0
            _orch.logger.info(
                "TRADFI non-trading day manifest: date=%s venues=%s — wrote empty_confirmed entries",
                date,
                sorted(non_trading),
            )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    manifest.close()

    # 7. SPORTS enrichment: fetch and write reference data (teams, leagues, etc.)
    # alongside fixtures. These are slow-moving entities that don't change per-date
    # but are re-fetched to capture transfers, promotions, new seasons.
    is_sports = any(c.upper() in ("SPORTS", "ALL") for c in asset_groups)
    # OPEN_METEO doesn't need API keys — allow sports enrichment even with empty api_keys
    # OPEN_METEO and UNDERSTAT don't need API keys (free, no auth)
    _needs_api_keys = sports_provider not in ("OPEN_METEO", "UNDERSTAT") if sports_provider else True
    if is_sports and (api_keys or not _needs_api_keys):
        _keys = api_keys or {}
        api_football_key = _keys.get("api_football")
        if not api_football_key:
            _orch.logger.warning("api_football key missing from api_keys — skipping sports reference data")
        else:
            # Pass completed fixture IDs from URDI fetch to avoid 33-league re-fetch
            # (saves 33 API calls per date). _urdi_completed_fixture_ids is populated
            # during the URDI instruments fetch above.
            # Only fetch entities that are actually missing from the manifest.
            # Create manifest writer so _fetch_sports_reference_data can write
            # per-league manifest entries for injuries and per-fixture entities.
            sports_manifest = _orch.ManifestWriter(
                service_name="instruments-service",
                catalogue_bucket=bucket,
            )
            sports_ref_counts = await _orch._fetch_sports_reference_data(
                date=date,
                api_key=api_football_key,
                bucket=bucket,
                entities_to_fetch=_sports_missing_entities if _sports_missing_entities else None,
                fixture_ids_override=list(_orch._urdi_completed_fixture_ids),
                manifest=sports_manifest,
                recovery_fixture_ids=recovery_fixture_ids,
                redo_all=redo_all,
            )
            for k, v in sports_ref_counts.items():
                counts[k] = counts.get(k, 0) + v

            # Write manifest for sports reference entities that did NOT write
            # their own manifest entries inside _fetch_sports_reference_data
            # (injuries and per-fixture entities write per-league entries directly).
            _self_manifested = {"injuries", "fixture_stats", "fixture_events", "fixture_lineups", "player_stats"}
            for entity_name, row_count in sports_ref_counts.items():
                if entity_name not in _self_manifested:
                    sports_manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": entity_name.upper()},
                        total_rows=row_count,
                        # SP-10: single-entity sports-reference shard (one (date, data_type) row
                        # per core entity) — no per-root-cluster contract for this data_type, so
                        # the cluster gate is a deliberate no-op; an unfounded expectation would
                        # false-fail genuinely-captured data.
                        expected_root_clusters={},
                        observed_clusters={"": row_count},
                        available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
                        pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                        service_emission_state=None,
                    )
            sports_manifest.write()
            _orch.logger.info(
                "Sports reference manifest: %d entities for %s",
                len(sports_ref_counts),
                date,
            )

        # FootyStats predictive data: proprietary potentials (btts_potential,
        # o25_potential, xg_prematch, etc.) written as a separate entity so FSS
        # can consume them as third-party signal input alongside odds.
        # Only call each enrichment provider if it's in active_venues (respects --venues filter).
        # When sports_entity_filter is set (entity-scoped VM), also guard individual
        # enrichment calls so only the requested entity is fetched.
        _active_venues_set = set(active_venues)
        _ef = sports_entity_filter  # short alias for entity filter checks

        def _entity_wanted(manifest_name: str) -> bool:
            """Return True if this entity should be fetched in the current run."""
            return _ef is None or _ef == manifest_name

        footystats_key = api_keys.get("footystats") if (api_keys and "FOOTYSTATS" in _active_venues_set) else None
        if footystats_key:
            if _entity_wanted("PREDICTIONS"):
                try:
                    pred_counts = await _orch._fetch_footystats_predictions(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in pred_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_predictions_fetch",
                        shard=date,
                    )

            if _entity_wanted("MATCHES"):
                try:
                    match_counts = await _orch._fetch_footystats_matches(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in match_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_matches_fetch",
                        shard=date,
                    )

            if _entity_wanted("ODDS"):
                try:
                    odds_counts = await _orch._fetch_footystats_odds(
                        date=date,
                        api_key=footystats_key,
                        bucket=bucket,
                        force=redo_all,
                    )
                    for k, v in odds_counts.items():
                        counts[k] = counts.get(k, 0) + v
                except Exception as exc:
                    _orch.classify_and_emit_error(
                        exc,
                        service_name="instruments-service",
                        operation="footystats_odds_fetch",
                        shard=date,
                    )

        if "UNDERSTAT" in _active_venues_set and _entity_wanted("XG"):
            try:
                xg_counts = await _orch._fetch_understat_xg(date=date, bucket=bucket, force=redo_all)
                for k, v in xg_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="understat_xg_fetch",
                    shard=date,
                )

        if "UNDERSTAT" in _active_venues_set and _entity_wanted("XG_SHOTS"):
            try:
                xg_shots_counts = await _orch._run_understat_shots_date(date=date, bucket=bucket, force=redo_all)
                for k, v in xg_shots_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="understat_xg_shots_fetch",
                    shard=date,
                )

        transfermarkt_key = (
            api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in _active_venues_set) else None
        )
        if not transfermarkt_key and "TRANSFERMARKT" in _active_venues_set:
            _orch.logger.warning(
                "TRANSFERMARKT is active but no API key found — skipping Transfermarkt fetch for date=%s. "
                "Ensure 'transfermarkt' key exists in Secret Manager and is passed via api_keys.",
                date,
            )
        # Trigger-based: only fetch Transfermarkt on reference refresh dates
        # (season start, transfer window open/close) or when --force is set.
        _batch_dt = _orch.date_type.fromisoformat(date)
        _tm_trigger = _orch.is_any_league_refresh_date(_batch_dt) or redo_all
        if not _tm_trigger and transfermarkt_key:
            _orch.logger.info(
                "date=%s: not a reference refresh trigger — skipping Transfermarkt",
                date,
            )
        if transfermarkt_key and _tm_trigger and _entity_wanted("PLAYER_VALUES"):
            try:
                tm_counts = await _orch._fetch_transfermarkt_data(
                    date=date,
                    api_key=transfermarkt_key,
                    bucket=bucket,
                    entity_filter=_ef,
                    season=season_override,
                    force=redo_all,
                )
                for k, v in tm_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="transfermarkt_data_fetch",
                    shard=date,
                )

        sfi_key = (
            api_keys.get("soccer_football_info")
            if (api_keys and "SOCCER_FOOTBALL_INFO" in _active_venues_set)
            else None
        )
        if not sfi_key and "SOCCER_FOOTBALL_INFO" in _active_venues_set:
            _orch.logger.warning(
                "SOCCER_FOOTBALL_INFO is active but no API key found — skipping SFI fetch for date=%s.",
                date,
            )
        if sfi_key and _entity_wanted("SFI_PROGRESSIVE_STATS"):
            try:
                sfi_counts = await _orch._fetch_sfi_data(
                    date=date,
                    api_key=sfi_key,
                    bucket=bucket,
                    entity_filter=_ef,
                    force=redo_all,
                )
                for k, v in sfi_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="sfi_data_fetch",
                    shard=date,
                )

        if _entity_wanted("WEATHER") and (
            "OPEN_METEO" in _active_venues_set or sports_entity_filter == "WEATHER" or sports_provider == "OPEN_METEO"
        ):
            try:
                weather_counts = await _orch._fetch_weather_data(date=date, bucket=bucket)
                for k, v in weather_counts.items():
                    counts[k] = counts.get(k, 0) + v
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation="weather_data_fetch",
                    shard=date,
                )

    # Weather: runs independently of API Football — no API key needed.
    # Must be outside the api_football_key gate so --sports-provider OPEN_METEO works.
    if is_sports and sports_provider == "OPEN_METEO":
        try:
            weather_counts = await _orch._fetch_weather_data(date=date, bucket=bucket)
            for k, v in weather_counts.items():
                counts[k] = counts.get(k, 0) + v
        except Exception as exc:
            _orch.classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="weather_data_fetch",
                shard=date,
            )

    total = sum(counts.values())

    # 8. Shard completeness check + automatic retry for missing venues.
    # Expected = configured active_venues (from category config + launch date filter),
    # NOT what was fetched. If a venue returns 0 instruments (adapter error, network
    # failure), it must show up as missing — never silently pass.
    #
    # HOWEVER, venues are excluded from expected if:
    #  - Adapter ran OK (in _non_error_venues) but returned 0 records after date
    #    filtering — the data source simply has no data for that date (e.g. NASDAQ
    #    before DBEQ.BASIC dataset starts, or CME on a holiday).
    #  - Validation rejected all records (validation_failed_venues) — data quality
    #    issue, not a missing-data issue. Per-record SCHEMA_VALIDATION_FAILED events logged above.
    #  - [SPORTS] The venue doesn't cover any leagues with fixtures on this date.
    #    Each league declares its data_sources in UAC LeagueDefinition. A venue
    #    is only expected if at least one league with fixtures lists it.
    #
    # When venues are missing (typically due to API rate limits or transient errors),
    # retry just the missing venues with exponential backoff before failing.
    expected_venues = set(active_venues)
    written_venues = set(counts.keys())

    # Sports: scope expected venues by league coverage.
    # Understat covers ~6 leagues, FootyStats ~50, SFI varies.
    # Only expect a venue if it covers leagues that had fixtures today.
    #
    # Performance: same 25MB / 2.6M-row manifest read as the upstream
    # _date_fixture_leagues read (line ~1213) — invalidated by every
    # manifest.write() so it misses cache on every date in BatchIO. Skip
    # this read when scope is already explicit (sports_entity_filter or
    # recovery_fixture_ids set) — the per-fixture entity loop has already
    # decided what to fetch from the explicit scope; the venue-scoping
    # is only needed for full-spectrum runs that haven't pre-decided.
    if is_sports_run and not (sports_entity_filter or recovery_fixture_ids is not None):
        try:
            from unified_api_contracts.sports import get_league

            # Get leagues with fixtures on this date from written data
            _fixture_leagues: set[str] = set()
            _sports_bucket = _orch._get_instruments_bucket("SPORTS")
            if _sports_bucket:
                _idx = _orch.read_availability_index(_sports_bucket)
                if not _idx.empty and "league_id" in _idx.columns:
                    _fix_rows = _idx[(_idx["date"] == date) & (_idx["data_type"] == "FIXTURES")]
                    _fixture_leagues = {
                        str(lid).upper() for lid in _fix_rows["league_id"].dropna().unique() if str(lid).strip()
                    }

            if _fixture_leagues:
                # Build set of data_sources that cover at least one fixture league
                _active_sources: set[str] = set()
                for lid in _fixture_leagues:
                    league_def = get_league(lid)
                    if league_def is not None:
                        _active_sources |= league_def.data_sources
                    else:
                        # Unknown league — assume all sources needed
                        _active_sources |= {
                            "api_football",
                            "footystats",
                            "understat",
                            "transfermarkt",
                            "soccer_football_info",
                            "open_meteo",
                        }

                # Map data_source names to venue names and remove uncovered venues
                _sports_venues = {
                    "API_FOOTBALL",
                    "FOOTYSTATS",
                    "UNDERSTAT",
                    "TRANSFERMARKT",
                    "SOCCER_FOOTBALL_INFO",
                    "OPEN_METEO",
                }
                _uncovered = set()
                for venue in expected_venues & _sports_venues:
                    source_name = venue.lower()
                    if source_name not in _active_sources:
                        _uncovered.add(venue)

                if _uncovered:
                    _orch.logger.info(
                        "Sports league scoping: removing %d venue(s) not covering any fixture leagues: %s",
                        len(_uncovered),
                        sorted(_uncovered),
                    )
                    expected_venues -= _uncovered
        except Exception as _scope_exc:
            _orch.logger.debug("Sports league scoping skipped: %s", _scope_exc)

    # Venues where the adapter succeeded but no records survived date/relevance filtering
    # are not "missing" — the data source simply had nothing for this date.
    empty_ok_venues = (_non_error_venues - written_venues) - validation_failed_venues
    if empty_ok_venues:
        _orch.logger.info(
            "Shard completeness: %d venue(s) fetched OK but 0 records after filtering (excluded from expected): %s",
            len(empty_ok_venues),
            sorted(empty_ok_venues),
        )
    expected_venues -= empty_ok_venues
    expected_venues -= validation_failed_venues

    missing_shards = expected_venues - written_venues

    # Retry ONLY venues that failed with retryable errors (RATE_LIMIT, NETWORK, TIMEOUT,
    # SERVER_ERROR). Permanent failures (UNSUPPORTED, ADAPTER_ERROR, PARSE_ERROR) are not
    # retried — they'll fail the same way again.
    # Exponential backoff: 10s, 30s. Enough for rate limits to clear.
    retry_delays = [10, 30]
    retryable_set = set(_retryable_venues)
    for retry_idx, delay in enumerate(retry_delays):
        # Only retry venues that are both missing AND had retryable errors
        retry_candidates = missing_shards & retryable_set
        if not retry_candidates or not written_venues:
            break  # Nothing retryable, or total failure (retrying won't help)

        _orch.logger.warning(
            "Shard incomplete: %d/%d venues missing, %d retryable — retrying in %ds (attempt %d/%d): %s",
            len(missing_shards),
            len(expected_venues),
            len(retry_candidates),
            delay,
            retry_idx + 1,
            len(retry_delays),
            sorted(retry_candidates),
        )
        await _orch.asyncio.sleep(delay)

        # Re-fetch just the retryable venues
        retry_venues = sorted(retry_candidates)
        with _orch.SolanaCacheSession():
            retry_result = await _orch.fetch_instruments_for_all_venues(
                retry_venues,
                api_keys=api_keys,
                date=date,
                mode=mode,
                source=source,
            )
        retry_records = retry_result.records
        # Update retryable set from this attempt's failures
        retryable_set = (retryable_set - set(retry_venues)) | set(retry_result.retryable_venues)

        if not retry_records:
            _orch.logger.warning(
                "Retry %d/%d: still 0 records for %d venues",
                retry_idx + 1,
                len(retry_delays),
                len(retry_venues),
            )
            continue

        # Apply same pipeline: date filter → relevance filter → validation → write
        retry_records = _orch.filter_instruments_by_date(retry_records, date_dt, defi_venues=defi_venue_set)
        if any(c.upper() in ("DEFI", "ALL") for c in asset_groups):
            retry_records = _orch.filter_defi_instruments_by_relevance(retry_records)
        if retry_records:
            valid_retry, _ = _orch.validate_instrument_records(
                retry_records, as_of_date=_orch.date_type.fromisoformat(date)
            )
            if valid_retry:
                retry_rows = []
                for r in valid_retry:
                    d = r.model_dump()
                    if d.get("legs") is not None:
                        d["legs"] = _orch.json.dumps(d["legs"])
                    retry_rows.append(d)
                retry_df = _orch.pd.DataFrame(retry_rows)
                if "venue" in retry_df.columns:
                    retry_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
                    for venue_name, venue_df in retry_df.groupby("venue"):
                        _orch._write_venue(
                            str(venue_name), venue_df, date, bucket, sink, counts, sampler, retry_manifest
                        )
                    retry_manifest.close()

        # Recalculate missing
        written_venues = set(counts.keys())
        missing_shards = expected_venues - written_venues
        recovered = len(retry_venues) - len(missing_shards & set(retry_venues))
        if recovered:
            _orch.logger.info(
                "Retry %d/%d: recovered %d/%d venues",
                retry_idx + 1,
                len(retry_delays),
                recovered,
                len(retry_venues),
            )
        total = sum(counts.values())

    # Final completeness assessment
    completeness_pct = int(len(written_venues) * 100 / len(expected_venues)) if expected_venues else 0

    if missing_shards:
        _orch.logger.error(
            "SHARD COMPLETENESS FAILURE date=%s: %d/%d venues written (%d%% complete), %d missing — %s",
            date,
            len(written_venues),
            len(expected_venues),
            completeness_pct,
            len(missing_shards),
            sorted(missing_shards),
        )
        _orch.log_event(
            "SHARD_INCOMPLETE",
            details={
                "date": date,
                "expected": len(expected_venues),
                "written": len(written_venues),
                "missing": sorted(missing_shards),
                "completeness_pct": completeness_pct,
            },
        )
        # Below 50% completeness = catastrophic failure (network outage, API down).
        # Fail the shard — the data is unusable and should not be treated as success.
        if completeness_pct < 50:
            msg = (
                f"SHARD CATASTROPHIC FAILURE date={date}: only {len(written_venues)}/{len(expected_venues)} "
                f"venues written ({completeness_pct}%). "
                f"Missing: {sorted(missing_shards)[:10]}{'...' if len(missing_shards) > 10 else ''}"
            )
            _orch.log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
            raise RuntimeError(msg)
    else:
        _orch.logger.info(
            "Shard completeness OK: %d/%d venues written for date=%s",
            len(written_venues),
            len(expected_venues),
            date,
        )

    # Honest-coverage: venues still missing after all retries are permanently-failed
    # shards.  Write attempted_failed rows so the manifest gap is explicit rather
    # than silently absent.  Shard isolation preserved — no raise, just records.
    if missing_shards:
        _failed_attempt_ts = _orch.datetime.now(_orch.UTC)
        _failed_manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
        for _failed_venue in sorted(missing_shards):
            _failed_manifest.record_failed(
                row_key={"date": date, "venue": _failed_venue},
                error=_orch.RecordFailedReason.UNCLASSIFIED_ADAPTER_ERROR,
                attempted_at=_failed_attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
            )
        _failed_manifest.close()
        _orch.logger.info(
            "Honest-coverage: wrote attempted_failed manifest rows for %d permanently-missing venues: %s",
            len(missing_shards),
            sorted(missing_shards),
        )

    # Emission policy check — PARTIAL_OK: emits PUBLISHED_DEGRADED when completeness < 1.0
    # but always allows write through. Per UAC seed Phase 6.8 PART B.
    _emission = _orch._check_emission_policy(
        date=date,
        completeness_fraction=len(written_venues) / len(expected_venues) if expected_venues else 1.0,
    )
    _orch.logger.debug(
        "catalog_snapshot emission decision date=%s: %s (completeness=%.3f)",
        date,
        _emission.service_emission_state,
        _emission.completeness_fraction,
    )

    _orch.log_event(
        "PROCESSING_COMPLETED",
        details={"date": date, "total_records": total, "venues": len(counts)},
    )
    _orch.logger.info("instruments: date=%s wrote %d records across %d venues", date, total, len(counts))
    return counts
