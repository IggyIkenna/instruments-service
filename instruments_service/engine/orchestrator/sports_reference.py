"""_fetch_sports_reference_data — sports reference entity fetch orchestration.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).
The legacy ~882-line function body is decomposed by stage into sibling modules
(pure behaviour-preserving extraction, same plan):

* ``sports_reference_core``     — manifest hooks + teams/standings/injuries
* ``sports_reference_fixtures`` — fixture-id resolution + per-fixture enrichment

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

from instruments_service.engine.orchestrator.sports_reference_core import (
    _AfManifestHooks,
    _fetch_injuries,
    _fetch_teams_and_standings,
)
from instruments_service.engine.orchestrator.sports_reference_filters import (
    _filter_fixtures_for_enrichment,
)
from instruments_service.engine.orchestrator.sports_reference_fixtures import (
    _resolve_fixture_ids,
    _run_per_fixture_enrichment,
)

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

    Per-fixture ENRICHMENT scope varies by entity (``SPORTS_ENTITY_LEAGUE_COVERAGE``,
    ``unified_api_contracts``) — operator ruling 2026-07-28: FIXTURE_STATS (game
    results) and FIXTURE_LINEUPS follow the full curated universe (383 leagues,
    same as FIXTURES), while FIXTURE_EVENTS and PLAYER_STATS stay restricted to
    the MVP/prediction-scope league set (``get_mvp_football_league_ids()``, 96
    leagues) regardless of how many leagues ``fixture_ids``/``fixture_ids_override``
    span — per-event/per-player enrichment fan-out for leagues we don't predict
    on is pure API-Football quota burn for data nobody consumes there. The check
    runs per-entity inside ``_gather_per_fixture_rows``, not as a shared
    pre-filter (see ``_filter_fixtures_for_enrichment``'s docstring).

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

    # Honest-coverage hooks: only record when an external manifest is wired
    # in by the caller (existing call-sites always pass one, but the default
    # signature keeps it optional for legacy use).
    hooks = _AfManifestHooks(date=date, manifest=manifest, attempt_ts=_orch.datetime.now(_orch.UTC), bucket=bucket)

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
    # orchestrator path; teams fetch reads
    # `get_expected_leagues_for_source("api_football")` from UAC instead of the
    # freshly-fetched leagues_df (2026-07-13: widened from the narrower
    # `get_prediction_leagues()`, which silently starved 61 of 94 expected
    # leagues of any per-league TEAMS/STANDINGS capture — see
    # sports_reference_core.py::_fetch_teams_and_standings docstring).

    # Teams + standings — for every api_football-covered league (cached across dates).
    # Guard accepts "leagues" (legacy umbrella), "teams", or "standings" so that
    # entity-scoped VM runs (e.g. --entity STANDINGS) still invoke the combined
    # fetcher.  DP-VM-002 root-cause: only "leagues" was checked, so a
    # STANDINGS-only VM skipped _fetch_teams_and_standings entirely → 0 rows.
    if not enrichment_only and (_should_fetch("leagues") or _should_fetch("teams") or _should_fetch("standings")):
        await _fetch_teams_and_standings(
            adapter=adapter,
            date=date,
            bucket=bucket,
            hooks=hooks,
            counts=counts,
        )

    # Injuries — date-specific, always fetched fresh.
    # IMPORTANT: outside the leagues/teams/standings block so it runs even when
    # only injuries is requested (entities_to_fetch=["API_FOOTBALL_INJURIES"]).
    if not enrichment_only and _should_fetch("injuries"):
        await _fetch_injuries(
            adapter=adapter,
            date=date,
            bucket=bucket,
            hooks=hooks,
            counts=counts,
        )

    # Per-fixture enrichment: stats, events, lineups, player stats.
    # Only for completed fixtures (status in FT/AET/PEN — stats unavailable for future/live).
    # Use fixture_ids_override when available (from URDI fetch) to avoid redundant
    # 33-league re-fetch (saves 33 API calls per date).
    fixture_ids, _af_fid_to_league = await _resolve_fixture_ids(
        adapter=adapter,
        api_key=api_key,
        date=date,
        bucket=bucket,
        fixture_ids_override=fixture_ids_override,
        hooks=hooks,
        redo_all=redo_all,
    )

    # MVP-league + recovery-allowlist filters — extracted to
    # ``_filter_fixtures_for_enrichment`` (function-size ratchet; see its
    # docstring for the MVP-scope and recovery-mode rationale). ``None``
    # signals the recovery allowlist intersected to zero: no per-fixture work
    # to do, so we return early (also skipping the cross-provider mapping
    # writes below, same as the original inline behaviour).
    fixture_ids = _filter_fixtures_for_enrichment(
        fixture_ids=fixture_ids,
        af_fid_to_league=_af_fid_to_league,
        recovery_fixture_ids=recovery_fixture_ids,
        date=date,
    )
    if fixture_ids is None:
        return counts

    if fixture_ids:
        await _run_per_fixture_enrichment(
            adapter=adapter,
            date=date,
            bucket=bucket,
            hooks=hooks,
            counts=counts,
            fixture_ids=fixture_ids,
            af_fid_to_league=_af_fid_to_league,
            fetch_set=_fetch_set,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
        )

    # Cross-provider mapping tables
    _orch._write_team_mapping(bucket)
    _orch._write_fixture_mapping(bucket, date)

    return counts
