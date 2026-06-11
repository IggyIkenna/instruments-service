"""process_instruments pre-flight stages — provider short-circuit, freshness, fast path.

Cohesion module of the ``engine.orchestrator`` package. Carries the pre-flight
stages decomposed out of the legacy ~1,931-line ``process_instruments`` body
(pure behaviour-preserving extraction; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``):

* the ``--sports-provider`` enrichment short-circuit,
* the manifest skip-if-exists freshness pre-flight (stage 1b), and
* the enrichment-only fast path (instruments done, only sports entities missing).

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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_ENRICHMENT_ENTITY_VENUES",
    "_ENRICHMENT_PROVIDERS",
    "_SPORTS_CORE_ENTITIES",
    "_SPORTS_PER_FIXTURE_ENTITY_NAMES",
    "_FreshnessOutcome",
    "_apply_sports_provider_filter",
    "_enrichment_only_fast_path",
    "_freshness_preflight",
    "_promote_redo_all_for_recovery",
    "_sports_provider_short_circuit",
]

# Sports entity lists — used by the freshness check AND later fast-path logic,
# so they are defined unconditionally (not inside the redo_all gate).
_SPORTS_CORE_ENTITIES: tuple[str, ...] = (
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
)
_SPORTS_PER_FIXTURE_ENTITY_NAMES: tuple[str, ...] = (
    "FIXTURE_STATS",
    "FIXTURE_EVENTS",
    "FIXTURE_LINEUPS",
    "PLAYER_STATS",
)


# Enrichment provider short-circuits: skip ALL orchestrator logic
# (URDI, API Football fixture fetch, etc.) and go straight to the
# specific provider's fetch function. Each reads fixtures from GCS
# (already fetched by API_FOOTBALL runs) and calls only its own API.
_ENRICHMENT_PROVIDERS: frozenset[str] = frozenset(
    {"OPEN_METEO", "UNDERSTAT", "FOOTYSTATS", "TRANSFERMARKT", "SOCCER_FOOTBALL_INFO"}
)

# Enrichment entity → venue that produces it.
# Only include in expected[] when that venue is in active_venues
# (respects --venues filter so API_FOOTBALL-only runs don't wait on
# SFI/Transfermarkt/Understat/Weather manifest entries).
_ENRICHMENT_ENTITY_VENUES: tuple[tuple[str, str], ...] = (
    ("MATCHES", "FOOTYSTATS"),
    ("PREDICTIONS", "FOOTYSTATS"),
    ("XG", "UNDERSTAT"),
    ("PLAYER_VALUES", "TRANSFERMARKT"),
    ("SFI_PROGRESSIVE_STATS", "SOCCER_FOOTBALL_INFO"),
    ("WEATHER", "OPEN_METEO"),
)

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
_SPORTS_PER_LEAGUE_ENTITIES: frozenset[str] = frozenset(
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


@dataclass
class _FreshnessOutcome:
    """Result of the manifest freshness pre-flight (stage 1b).

    ``skip`` means every expected venue/entity is already fresh in the
    manifest — the caller returns ``{}`` immediately. The entity lists carry
    the (possibly entity-scoped) core / per-fixture entity sets so downstream
    stages see exactly what the pre-flight decided.
    """

    skip: bool
    missing_entities: list[str] = field(default_factory=list)
    core_entities: list[str] = field(default_factory=list)
    per_fixture_entities: list[str] = field(default_factory=list)


def _promote_redo_all_for_recovery(
    *,
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> bool:
    """Recovery-mode hint: promote ``redo_all=True`` when recovery is active.

    When --recovery-fixture-ids is set, the per-provider fetches (footystats /
    understat / sfi / open_meteo) need to bypass their per-day/per-league
    pre-flight skip, because empty_confirmed phantom rows would otherwise mask
    the dates we're trying to recover. Each provider's fetch already has a
    ``force=...`` parameter that bypasses its skip; we promote redo_all when
    recovery is active so the existing dispatch ``force=redo_all`` propagates
    correctly.

    For api_football the orchestrator's per-fixture loop has its own explicit
    allowlist filter (in _fetch_sports_reference_data), so the redo_all
    promotion here is harmless — the allowlist is the finer-grained scope.
    """
    if recovery_fixture_ids is not None:
        if not redo_all:
            _orch.logger.info(
                "Recovery mode: promoting redo_all=True so per-provider per-day skip "
                "is bypassed (recovery_fixture_ids has %d af_fixture_ids)",
                len(recovery_fixture_ids),
            )
        redo_all = True
    return redo_all


async def _apply_sports_provider_filter(
    *,
    date: str,
    asset_groups: list[str],
    redo_all: bool,
    api_keys: dict[str, str] | None,
    active_venues: list[str],
    sports_provider: str,
    sports_entity_filter: str | None,
    season_override: int | None,
) -> tuple[list[str], dict[str, int] | None]:
    """--sports-provider: restrict to only this provider's venues.

    Returns ``(active_venues, early_result)``: a non-None ``early_result``
    means the caller returns it immediately (unknown provider → ``{}``, or the
    enrichment provider short-circuit ran to completion).
    """
    provider_venues = _orch._SPORTS_PROVIDER_VENUES.get(sports_provider)
    if provider_venues is None:
        _orch.logger.error(
            "Unknown --sports-provider: %s. Valid: %s", sports_provider, list(_orch._SPORTS_PROVIDER_VENUES)
        )
        return active_venues, {}
    active_venues = [v for v in active_venues if v in provider_venues]
    _orch.logger.info("Sports provider filter: %s → venues %s", sports_provider, active_venues)

    if sports_provider in _ENRICHMENT_PROVIDERS:
        return active_venues, await _sports_provider_short_circuit(
            date=date,
            asset_groups=asset_groups,
            redo_all=redo_all,
            api_keys=api_keys,
            sports_provider=sports_provider,
            sports_entity_filter=sports_entity_filter,
            season_override=season_override,
        )
    return active_venues, None


async def _sports_provider_short_circuit(
    *,
    date: str,
    asset_groups: list[str],
    redo_all: bool,
    api_keys: dict[str, str] | None,
    sports_provider: str,
    sports_entity_filter: str | None,
    season_override: int | None,
) -> dict[str, int]:
    """Run an enrichment provider's fetch directly, skipping the orchestrator.

    Each provider reads fixtures from GCS (already fetched by API_FOOTBALL
    runs) and calls only its own API.
    """
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
            odds_result = await _orch._fetch_footystats_odds(date=date, api_key=fs_key, bucket=bucket, force=redo_all)
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
            _orch.logger.info("Transfermarkt: date=%s has no league triggers — skipping PLAYER_VALUES refresh", date)
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


def _build_expected_entities(
    *,
    date: str,
    bucket: str,
    active_venues: list[str],
    is_sports_run: bool,
    sports_entity_filter: str | None,
    recovery_fixture_ids: frozenset[int] | None,
    core_entities: list[str],
    per_fixture_entities: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """Build the expected venue/entity list for the freshness pre-flight.

    For SPORTS, require both core AND per-fixture reference entities.
    Core: leagues/teams/standings/injuries (slow-moving, fetched every run).
    Per-fixture: fixture_stats/events/lineups/player_stats (one API call per
    completed fixture, rate-limited to 1 req/sec — expensive to re-fetch).
    Remap venue names to match manifest data_type entries (API_FOOTBALL → FIXTURES).
    """
    expected = ["FIXTURES" if v == "API_FOOTBALL" else v for v in active_venues]
    _active_venues_set_freshness = set(active_venues)
    if is_sports_run:
        expected.extend(core_entities)
        expected.extend(per_fixture_entities)

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
        #     gets restricted to that one entity later anyway
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

        for entity, venue in _ENRICHMENT_ENTITY_VENUES:
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
        core_entities = [e for e in core_entities if e == sports_entity_filter]
        per_fixture_entities = [e for e in per_fixture_entities if e == sports_entity_filter]
        _orch.logger.info("Entity-scoped mode: restricting to %s only", sports_entity_filter)

    return expected, core_entities, per_fixture_entities


def _freshness_preflight(
    *,
    date: str,
    asset_groups: list[str],
    active_venues: list[str],
    is_sports_run: bool,
    sports_entity_filter: str | None,
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> _FreshnessOutcome:
    """Stage 1b — skip-if-exists: check manifest for fresh data (unless --force).

    With ``redo_all`` (--force) the manifest check is skipped entirely and the
    outcome carries the unfiltered default entity lists with no missing
    entities (matching the legacy in-line behaviour).
    """
    core_entities = list(_SPORTS_CORE_ENTITIES)
    per_fixture_entities = list(_SPORTS_PER_FIXTURE_ENTITY_NAMES)
    if redo_all:
        return _FreshnessOutcome(
            skip=False,
            missing_entities=[],
            core_entities=core_entities,
            per_fixture_entities=per_fixture_entities,
        )
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _orch._get_instruments_bucket(primary_asset_group)

    expected, core_entities, per_fixture_entities = _build_expected_entities(
        date=date,
        bucket=bucket,
        active_venues=active_venues,
        is_sports_run=is_sports_run,
        sports_entity_filter=sports_entity_filter,
        recovery_fixture_ids=recovery_fixture_ids,
        core_entities=core_entities,
        per_fixture_entities=per_fixture_entities,
    )

    # Historical dates (>7 days ago) have immutable data — completed fixtures
    # and reference data don't change retroactively. Use max_age_hours=0 so the
    # freshness check only fails on schema version mismatch, not on timestamp.
    # The 24h default is correct for live/today runs where data updates daily.
    _date_cutoff = (_orch.datetime.now(_orch.UTC) - _orch.timedelta(days=7)).strftime("%Y-%m-%d")
    _freshness_max_age = 0.0 if date < _date_cutoff else 24.0

    _has_sports_per_league_in_scope = bool(set(expected) & _SPORTS_PER_LEAGUE_ENTITIES)

    if is_sports_run and _has_sports_per_league_in_scope:
        # Defer to per-league checks in the entity handlers. Treat all
        # expected entities as "missing" at the date level so the
        # downstream per-entity dispatch fires; each handler does its
        # own per-league `_should_skip_date_for_per_league`.
        is_fresh = False
        stale: list[str] = []
        missing: list[str] = list(expected)
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
        return _FreshnessOutcome(
            skip=True,
            missing_entities=[],
            core_entities=core_entities,
            per_fixture_entities=per_fixture_entities,
        )

    # Per-entity skip: pass the exact missing list so _fetch_sports_reference_data
    # only fetches entities that are actually absent from the manifest.
    missing_entities: list[str] = []
    if is_sports_run and missing:
        missing_entities = list(missing)
        missing_set = set(missing)
        core_missing = missing_set & set(core_entities)
        pf_missing = [e for e in per_fixture_entities if e in missing_set]
        instruments_missing = missing_set - set(core_entities) - set(per_fixture_entities)
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

    return _FreshnessOutcome(
        skip=False,
        missing_entities=missing_entities,
        core_entities=core_entities,
        per_fixture_entities=per_fixture_entities,
    )


async def _enrichment_only_fast_path(
    *,
    date: str,
    asset_groups: list[str],
    api_keys: dict[str, str],
    missing_entities: list[str],
    core_entities: list[str],
    per_fixture_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> dict[str, int] | None:
    """Fast path: only specific sports entities are missing (instruments done).

    Skips URDI fetch and jumps to targeted sports enrichment. Two sub-cases:
      A) Only per-fixture entities missing (core + instruments done)
      B) Only core entities missing (e.g. injuries only — instruments done)
    Both skip the expensive URDI fetch and go straight to
    ``_fetch_sports_reference_data``. Returns ``None`` when the fast path does
    not apply (caller continues with the full pipeline).
    """
    missing_set = set(missing_entities)
    # Enrichment entities (XG, Transfermarkt, FootyStats, SFI, Weather) can
    # read existing fixtures from GCS — they don't need a URDI fetch.
    _enrichment_entity_names = {e for e, _ in _ENRICHMENT_ENTITY_VENUES}
    instruments_missing = missing_set - set(core_entities) - set(per_fixture_entities) - _enrichment_entity_names
    # Fast path fires when only core/per-fixture/enrichment entities are missing
    # (no actual instrument records to fetch from URDI)
    if instruments_missing:
        return None
    api_football_key = api_keys.get("api_football")
    if not api_football_key:
        return None
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _orch._get_instruments_bucket(primary_asset_group)
    # Resolve fixture IDs from existing GCS fixtures parquet (0 API calls)
    gcs_fixture_ids = _orch._read_fixture_ids_from_gcs(bucket, date)
    _orch.logger.info(
        "ENRICHMENT-ONLY date=%s: %d fixture IDs from GCS, fetching %s",
        date,
        len(gcs_fixture_ids),
        missing_entities,
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
        entities_to_fetch=missing_entities,
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
        for pf_entity in per_fixture_entities:
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
