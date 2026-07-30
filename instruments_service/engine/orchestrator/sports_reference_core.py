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

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
    from instruments_service.reference_data.adapters.sports.adapters.base import BaseSportsReferenceAdapter
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_AfManifestHooks",
    "_close_stale_enrichment_expected_unattempted_cells",
    "_fetch_injuries",
    "_fetch_teams_and_standings",
    "_list_present_parquet_leagues",
    "_manifest_captured_leagues_for_data_type",
]


def _list_present_parquet_leagues(bucket: str, date: str, entity_name: str) -> set[str]:
    """League ids with an existing per-league parquet for ``(date, entity)``.

    List-only presence probe (one GCS prefix list, no object downloads) —
    mirrors ``_read_per_league_entity_df``'s canonical-then-legacy prefix
    pattern. Used by the honest-absence emitters as a hard PRESENCE guard:
    a cell whose per-league parquet EXISTS at the canonical path must NEVER
    be stamped ``empty_confirmed``, no matter how the caller's captured-league
    set was built (this-run rows, the pre-fetch skip tracker, or nothing at
    all on the zero-fixture-day / ``redo_all`` paths where the pre-fetch probe
    is bypassed). 2026-07-14 GW verification RED: 3,720 false-empty cells were
    stamped over existing parquets by a this-run-only captured set. Issue:
    ``sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14``.

    GCS transport errors propagate to the caller (same contract as
    ``_read_per_league_entity_df``) — a failed presence probe must not be
    silently treated as "nothing present".
    """
    pm = _orch._sports_ref_pm(entity_name)
    canonical_prefix = f"sports_reference/by_date/day={date}/pipeline_mode={pm}/entity={entity_name}/"
    legacy_prefix = f"sports_reference/by_date/day={date}/entity={entity_name}/"
    storage_client = _orch.get_storage_client()
    blobs = list(storage_client.list_blobs(bucket=bucket, prefix=canonical_prefix, max_results=None))
    if not blobs:
        blobs = list(storage_client.list_blobs(bucket=bucket, prefix=legacy_prefix, max_results=None))
    present: set[str] = set()
    for blob_meta in blobs:
        if not blob_meta.name.endswith(".parquet"):
            continue
        league_match = re.search(r"/league=([^/]+)/", blob_meta.name)
        if league_match:
            present.add(_orch._canonical_league_id(league_match.group(1)))
    return present


def _manifest_captured_leagues_for_data_type(*, bucket: str, date: str, data_type: str) -> set[str] | None:
    """Leagues already CAPTURED for ``data_type`` on ``date``, per the manifest.

    Oscillation guard mirroring ``enumerate_expected_universe.py``'s
    ``captured_set`` (instruments-service@ba306543,
    ``sports_index_recency_masked_captured_atoms_2026_07_13.md``): the
    ``ba306543`` guard only covers the ``enumerate_v2`` seeder. Two SEPARATE
    batch-capture emission sites must never stamp ``empty_confirmed`` over an
    atom a PRIOR run already captured (that loop previously only consulted
    THIS run's captured leagues, so a run that legitimately returned zero
    results for an already-captured league would mask it):
    ``process_write._write_sports_fixture_venue`` (FIXTURES_SCHEDULE) and
    ``_AfManifestHooks.emit_empty_gaps_for_entity`` (the "regular sports
    instruments" — TEAMS/STANDINGS/INJURIES/etc. — via
    :meth:`_AfManifestHooks._manifest_index_guarded_captured_leagues`).
    Originally FIXTURES_SCHEDULE-only (as ``_manifest_captured_fixture_leagues``);
    generalized to any ``data_type`` so both emission sites share one guard.

    Single filtered index read (row-group pushdown on ``date``, slim
    columns) — not a corpus walk; same pattern as
    ``close_stale_enrichment_expected_unattempted_cells_2026_07_19.py``.
    Returns ``None`` on read failure — the caller must then skip the whole
    empty-emission pass rather than risk masking (same fail-safe contract as
    :meth:`_AfManifestHooks._presence_guarded_captured_leagues`).
    """
    try:
        _idx = _orch.read_availability_index(
            bucket,
            columns=["date", "data_type", "league_id", "capture_status"],
            filters=[("date", "==", date)],
        )
    except Exception as exc:
        _orch.logger.warning(
            "%s captured-set guard: manifest read failed for date=%s (%s) — skipping "
            "empty-gap emission (cannot prove captured status without a read)",
            data_type,
            date,
            exc,
        )
        return None
    if _idx.empty:
        return set()
    _dt = _idx[(_idx["data_type"] == data_type) & (_idx["capture_status"] == "captured")]
    return {_orch._canonical_league_id(str(lid)) for lid in _dt["league_id"].dropna().unique() if str(lid).strip()}


@dataclass
class _AfManifestHooks:
    """Honest-coverage manifest hooks for the api_football reference fetch.

    Only record when an external manifest is wired in by the caller (existing
    call-sites always pass one, but the orchestration signature keeps it
    optional for legacy use). Carries the shared attempt timestamp so every
    row from one fetch run stamps consistently.

    Method names are deliberately ``note_*`` (not ``record_*``): they inject
    ``pipeline_mode=PipelineMode.BATCH_API_FOOTBALL`` into the real
    ``ManifestWriter.record_*`` call below, so they must NOT shadow the writer
    method names (QG STEP 5.70's AST check matches ``record_*`` by name and
    would otherwise flag these wrapper call-sites as missing the kwarg).
    """

    date: str
    manifest: _orch.ManifestWriter | None
    attempt_ts: _orch.datetime
    # Sports instruments-store bucket for the PRESENCE guard in
    # ``emit_empty_gaps_for_entity``. Empty string = guard disabled (legacy
    # constructions that predate the guard; production call sites wire it).
    bucket: str = ""

    def note_failed(self, data_type: str, exc: Exception, league_id: str = "") -> None:
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

    def note_empty(self, data_type: str, league_id: str = "", reason: str = "") -> None:
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
            source=_orch._sports_ref_source(data_type.lower()),
        )

    def _presence_guarded_captured_leagues(self, data_type: str, captured_league_ids: set[str]) -> set[str] | None:
        """Union any league with an EXISTING per-league parquet for ``(date, entity)``
        into ``captured_league_ids`` — absence classification is presence-based,
        never this-run-only (protects the zero-fixture-day and ``redo_all`` paths
        where the pre-fetch skip tracker is bypassed). Returns ``None`` when the
        presence probe itself fails (caller must abort emission — see the
        FAIL-SAFE note below), else the unioned set.

        Issue: ``sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14``.
        """
        if not self.bucket:
            return captured_league_ids
        try:
            _present = _orch._list_present_parquet_leagues(self.bucket, self.date, data_type.lower())
        except Exception as exc:
            # FAIL-SAFE: if presence cannot be verified, absence cannot be
            # honestly stamped — skip the empty emission entirely (cells
            # stay pending and are re-attempted later) rather than risk
            # stamping empty_confirmed over data we could not see.
            _orch.logger.warning(
                "%s presence probe failed for date=%s (%s) — skipping empty-gap emission "
                "(cannot prove absence without presence)",
                data_type,
                self.date,
                exc,
            )
            return None
        _rescued = _present - captured_league_ids
        if _rescued:
            _orch.logger.warning(
                "%s presence-guard: %d league(s) have an existing per-league parquet for "
                "date=%s but no captured rows this run (%s%s) — NOT stamping empty_confirmed "
                "over present data",
                data_type,
                len(_rescued),
                self.date,
                ", ".join(sorted(_rescued)[:10]),
                ", ..." if len(_rescued) > 10 else "",
            )
        return captured_league_ids | _present

    def _manifest_index_guarded_captured_leagues(
        self, data_type: str, captured_league_ids: set[str]
    ) -> set[str] | None:
        """Union any league with an EXISTING manifest ``captured`` row for
        ``(date, data_type)`` into ``captured_league_ids`` — the same
        cross-identity oscillation guard ``process_write._write_sports_fixture_venue``
        already applies to FIXTURES_SCHEDULE
        (:func:`_manifest_captured_leagues_for_data_type`), extended here to the
        "regular sports instruments" entities (TEAMS/STANDINGS/INJURIES/...)
        emitted via :meth:`emit_empty_gaps_for_entity`.

        Guards the SAME bug class as the presence guard above, but for the case
        the presence guard cannot catch: a captured manifest row written under a
        DIFFERENT identity (service_name/venue/instrument dims) than this run's
        writer, with no on-disk object under THIS run's expected per-league path
        (e.g. a legacy/bare-path capture) — the recency-masking class documented
        in ``sports_index_recency_masked_captured_atoms_2026_07_13.md`` §
        "ROOT CAUSE CORRECTED". Returns ``None`` when the manifest read itself
        fails (fail-safe — caller must abort emission, mirroring
        :meth:`_presence_guarded_captured_leagues`). Gated on ``self.bucket``
        (empty string = guard disabled, same as the presence guard).
        """
        if not self.bucket:
            return captured_league_ids
        _mlids = _orch._manifest_captured_leagues_for_data_type(bucket=self.bucket, date=self.date, data_type=data_type)
        if _mlids is None:
            return None
        return captured_league_ids | _mlids

    def _emit_empty_gap_for_league(self, data_type: str, exp_lid: str) -> None:
        """Classify + stamp one expected league's absence (the per-league body of
        :meth:`emit_empty_gaps_for_entity`'s loop). See that method's docstring for
        the 3-way classification contract."""
        _canon_lid = _orch._canonical_league_id(exp_lid)
        if not _orch.is_league_entity_covered(_canon_lid, data_type):
            # Provider has no coverage for this (league, entity) — honest,
            # uncollectable absence (excluded from the gap denominator).
            self.manifest.record_empty(
                row_key={"date": self.date, "data_type": data_type, "league_id": exp_lid},
                attempted_at=self.attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_PROVIDER_COVERAGE,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                source=_orch._sports_ref_source(data_type.lower()),
            )
            return
        if not _orch.get_league_fixture_calendar(exp_lid, self.date, self.date):
            # Off-season for this league (SEASON_BY_COUNTRY) — a real,
            # typed absence, NOT "nothing to say". Previously this
            # silently skipped the cell (no manifest write at all),
            # leaving it permanently blank-reason expected_unattempted
            # (2026-07-14 GW verification: A_LEAGUE's Oct-Apr season
            # left every September date off-season, producing 30
            # blank INJURIES cells that never resolved on re-fetch).
            self.manifest.record_empty(
                row_key={"date": self.date, "data_type": data_type, "league_id": exp_lid},
                attempted_at=self.attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_PAUSED_LEAGUE,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                source=_orch._sports_ref_source(data_type.lower()),
            )
            return
        self.manifest.record_empty(
            row_key={"date": self.date, "data_type": data_type, "league_id": exp_lid},
            attempted_at=self.attempt_ts,
            reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
            source=_orch._sports_ref_source(data_type.lower()),
        )

    def emit_empty_gaps_for_entity(self, data_type: str, captured_league_ids: set[str]) -> None:
        """Emit empty_confirmed per expected league with no captured rows (same contract as FIXTURES).

        For per-fixture / enrichment entities the absence is classified per the
        OBSERVED (league x entity) coverage map: a league that has NEVER yielded
        this entity in API-Football is honest ``EXPECTED_NO_PROVIDER_COVERAGE``
        (out-of-coverage — provider has no data for this (league, entity)), NOT
        ``EXPECTED_NO_FIXTURE`` (which means a fixture genuinely wasn't scheduled
        and would falsely imply the gap is restorable by a re-fetch). An
        out-of-coverage league is emitted even with a fixture on the calendar
        (the fixture exists but the entity is uncollectable there). SSOT:
        ``unified_api_contracts.registry.sports_league_entity_coverage``.

        The gap DENOMINATOR is also entity-scope-aware (``SPORTS_ENTITY_LEAGUE_
        COVERAGE`` via ``get_entity_league_coverage`` — operator ruling
        2026-07-28): an entity restricted to a subset of leagues (e.g.
        FIXTURE_EVENTS/PLAYER_STATS, MVP-only) is intersected against that
        subset rather than the full curated universe, so a non-MVP league is
        simply excluded from "expected" for that entity — not flagged as any
        kind of gap. Previously this hardcoded the full curated-universe
        denominator for every entity, so coverage views showed ~287 non-MVP
        leagues as a permanent gap for those entities when they were never in
        scope by policy.

        PRESENCE guard (2026-07-14 GW verification RED): see
        :meth:`_presence_guarded_captured_leagues`.

        MANIFEST-INDEX oscillation guard (2026-07-23, extends instruments-service@
        ba306543/the FIXTURES_SCHEDULE guard to this entity path): see
        :meth:`_manifest_index_guarded_captured_leagues`.
        """
        if self.manifest is None:
            return
        guarded = self._presence_guarded_captured_leagues(data_type, captured_league_ids)
        if guarded is None:
            return
        captured_league_ids = guarded
        guarded = self._manifest_index_guarded_captured_leagues(data_type, captured_league_ids)
        if guarded is None:
            return
        captured_league_ids = guarded
        _expected = {lg.league_id for lg in _orch.get_expected_leagues_for_source("api_football")}
        _entity_scope = _orch.get_entity_league_coverage(data_type)
        if _entity_scope is not None:
            _expected &= _entity_scope
        for _exp_lid in sorted(_expected - captured_league_ids):
            self._emit_empty_gap_for_league(data_type, _exp_lid)


def _close_stale_enrichment_expected_unattempted_cells(
    *,
    manifest: _orch.ManifestWriter,
    stuck_cells: _orch.pd.DataFrame,
    fixtures_empty_reason_by_date_league: dict[tuple[str, str], str],
) -> dict[str, int]:
    """Close residual ``expected_unattempted`` cells for the 4 per-fixture ENRICHMENT
    entities (FIXTURE_EVENTS/FIXTURE_LINEUPS/FIXTURE_STATS/PLAYER_STATS) — but ONLY the
    subset that is provably an honest off-season/no-fixture/no-coverage absence, never a
    genuine pending-fetch gap.

    Live spot-check (this issue doc, 2026-07-19): a naive mirror of ``process_write.py``'s
    ``_expected_af_lids - _captured_lids`` FIXTURES closer is UNSAFE here. Sampled 3 stuck
    FIXTURE_STATS cells (date=2020-06-14, AUSTRIAN_2_LIGA/AUSTRIAN_BUNDESLIGA/
    GREEK_SUPER_LEAGUE) — for ALL three, ``is_league_entity_covered(league, "FIXTURE_STATS")``
    is ``True`` (so a naive closer would fall through to ``get_league_fixture_calendar`` and
    stamp ``EXPECTED_NO_FIXTURE``), yet the manifest shows FIXTURES ``captured`` for the SAME
    (date, league) on 2 of the 3, and for GREEK_SUPER_LEAGUE the SIBLING FIXTURE_EVENTS/
    FIXTURE_LINEUPS entities are ALSO ``captured`` for the identical cell — i.e. a real
    fixture demonstrably existed and was even successfully enriched by sibling entities;
    FIXTURE_STATS specifically was simply never attempted (a genuine pending-fetch gap, most
    likely from a historical backfill run whose ``entities_to_fetch`` scope excluded
    FIXTURE_STATS). Stamping ``EXPECTED_NO_FIXTURE`` there would be false-empty corruption —
    the exact failure mode this codebase has already paid to fix elsewhere (see
    ``sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14``).

    Safety design: a cell is closed ONLY when the classification is independently provable
    without assuming absence:
      1. ``is_league_entity_covered(league, entity)`` is ``False`` → ``EXPECTED_NO_PROVIDER_
         COVERAGE``. This is a provider-capability fact, independent of whether a fixture
         existed that date, so it is always safe.
      2. Else, the FIXTURES entity's OWN manifest row for the SAME (date, league) is
         ``empty_confirmed`` with an ``EXPECTED_*`` reason (i.e. FIXTURES *itself* already
         proved — via a calendar/coverage fact, not a live fetch — no fixture existed there
         that date) → mirror that SAME reason (``EXPECTED_PAUSED_LEAGUE``/``EXPECTED_NO_
         FIXTURE``/etc.) onto the enrichment cell. This never guesses; it reuses an
         already-proven fact from the corpus. ``SOURCE_RETURNED_ZERO`` is explicitly EXCLUDED
         from this mirror: per
         ``unified_api_contracts.canonical.crosscutting._honest_coverage_empty_reasons``'s own
         documented design, ``SOURCE_RETURNED_ZERO`` is data-type-aware — "resolved" only for a
         schedule-DEFINING data_type (FIXTURES itself); for an ENRICHMENT data_type it "stays a
         within-window gap (its zero may be real)". Mirroring it here would also violate the
         manifest writer's Phase-1 KEYSTONE honest-absence gate
         (``record_empty(reason=SOURCE_RETURNED_ZERO)`` requires real ``FetchEvidence`` proving
         a fresh 200+empty fetch, which this classification-only closer never has — confirmed
         live 2026-07-19: a first `--apply` attempt hard-crashed on
         ``UnprovenHonestAbsenceError`` for exactly this case, `date=2024-12-24,
         data_type=FIXTURE_EVENTS, league_id=DANISH_1ST_DIVISION`; 36 of ~5,300 mirror
         candidates carry this reason).
      3. Otherwise (coverage says yes AND FIXTURES shows ``captured``/``SOURCE_RETURNED_ZERO``,
         or is itself still pending) → **do not touch the cell**. It is left
         ``expected_unattempted`` exactly as it is today — a genuine pending-fetch gap that
         needs a real re-fetch, not a classification.

    Args:
        manifest: Writer for the ``record_empty`` calls emitted per closeable league.
        stuck_cells: Slim manifest rows (``date``, ``data_type``, ``league_id`` columns)
            already filtered by the caller to genuinely-stuck cells for the 4 enrichment
            entities (``capture_status == "expected_unattempted"``, blank ``error_reason`` —
            same pending_fetch definition as ``query_api_football_pending_clusters_2026_07_18.py``).
        fixtures_empty_reason_by_date_league: ``{(date, league_id): error_reason}`` for every
            FIXTURES row that is ``empty_confirmed`` — the caller's single manifest read
            already covers this alongside ``stuck_cells`` (no extra corpus walk).

    Returns:
        ``{f"{date}/{data_type}": n_leagues_closed}`` per (date, entity) group that had at
        least one league closed. Leagues left untouched (genuine pending-fetch gaps) are
        NOT counted — they remain visible to the standard pending_fetch gate.
    """
    counts: dict[str, int] = {}
    for (date, data_type), group in stuck_cells.groupby(["date", "data_type"]):
        _date_str = str(date)
        _dt_str = str(data_type)
        _attempt_ts = _orch.datetime.now(_orch.UTC)
        _closed = 0
        for _lid in sorted({str(lid) for lid in group["league_id"].dropna().unique() if str(lid)}):
            if not _orch.is_league_entity_covered(_lid, _dt_str):
                manifest.record_empty(
                    row_key={"date": _date_str, "data_type": _dt_str, "league_id": _lid},
                    attempted_at=_attempt_ts,
                    reason=_orch.EmptyConfirmedReason.EXPECTED_NO_PROVIDER_COVERAGE,
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    source=_orch._sports_ref_source(_dt_str.lower()),
                )
                _closed += 1
                continue
            _fixtures_reason = fixtures_empty_reason_by_date_league.get((_date_str, _lid))
            # SOURCE_RETURNED_ZERO is data-type-aware (see docstring above) — mirroring it onto
            # an ENRICHMENT entity is neither semantically safe nor writer-permitted (no
            # FetchEvidence exists for this classification-only closer). Every OTHER
            # empty_confirmed reason on FIXTURES is an EXPECTED_* calendar/coverage fact, safe
            # to mirror unconditionally.
            if _fixtures_reason and _fixtures_reason != _orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value:
                manifest.record_empty(
                    row_key={"date": _date_str, "data_type": _dt_str, "league_id": _lid},
                    attempted_at=_attempt_ts,
                    reason=_fixtures_reason,
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    source=_orch._sports_ref_source(_dt_str.lower()),
                )
                _closed += 1
            # else: FIXTURES shows captured, is itself still pending, or is empty_confirmed via
            # SOURCE_RETURNED_ZERO (no provable evidence for THIS entity) — a genuine
            # pending-fetch gap. Leave untouched.
        if _closed:
            counts[f"{_date_str}/{_dt_str}"] = _closed
    return counts


async def _fetch_and_cache_teams(
    *, adapter: BaseSportsReferenceAdapter, hooks: _AfManifestHooks
) -> tuple[_orch.pd.DataFrame | None, list[int]]:
    """Fetch (or reuse cached) TEAMS rows for every api_football-covered league.

    Returns ``(teams_df, prediction_league_ids)`` — the latter feeds the STANDINGS
    fetch (:func:`_fetch_and_cache_standings`): only leagues teams were fetched
    for get standings looked up."""
    teams_df = _orch._cached_teams_df
    prediction_league_ids: list[int] = []
    if teams_df is None:
        all_teams: list[dict[str, object]] = []
        try:
            for league_def in _orch.get_expected_leagues_for_source("api_football"):
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
                    hooks.note_failed("TEAMS", exc, league_id=league_def.league_id)
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
            hooks.note_failed("TEAMS", exc)
    else:
        prediction_league_ids = _orch._cached_prediction_league_ids
        _orch.logger.info("Sports reference: %d teams from cache (0 API calls)", len(teams_df))
    return teams_df, prediction_league_ids


def _write_teams_and_venues(
    *, teams_df: _orch.pd.DataFrame, bucket: str, date: str, hooks: _AfManifestHooks, counts: dict[str, int]
) -> None:
    """Write per-league partitioned TEAMS files + per-league manifest rows, then
    extract + write the global venues.parquet from the fetched teams.

    The bare-path fallback was retired in sports_manifest_single_ssot_2026_04_30
    — TEAMS is a league-axis data type and MUST always carry league_id.

    2026-07-13 root-cause addendum: this loop always wrote the correct
    per-league GCS parquet, but until now NEVER called
    ``manifest.record_captured`` itself — the only manifest bookkeeping
    for TEAMS came from ``process_enrichment.py``'s blanket
    ``record_captured_from_counts(row_key={"date": date, "data_type":
    "TEAMS"})`` fallback (fires because "teams" was absent from that
    call's ``_self_manifested`` exclusion set), which writes ONE
    blank-league_id "captured" row per date summing all leagues. That
    blanket call is the CONFIRMED LIVE source of the blank-league_id
    bulk bundle (verified still growing through 2026-07-13, not legacy
    residue as an earlier pass guessed) — mirrored by an identical bug
    for STANDINGS (which already self-manifests per-league below but
    was never excluded from the blanket call either). Fixed here (own
    per-league record_captured, matching STANDINGS) + in
    ``process_enrichment.py`` (added "teams"/"standings" to
    ``_self_manifested`` so the blanket call stops firing for both) —
    together these make per-league TEAMS/STANDINGS manifest rows the
    sole, canonical source of truth going forward.
    """
    manifest = hooks.manifest
    if "league_id" in teams_df.columns:
        _teams_captured: set[str] = set()
        for _t_lid, _t_league_df in teams_df.groupby("league_id"):
            _t_lid_str = str(_t_lid)
            _t_canon = _orch._canonical_league_id(_t_lid_str)
            # WRITE-UNIVERSE gate (incident 2026-06-24, mirrored from the
            # STANDINGS write below): never write a captured teams row for
            # a league outside our tracked api_football set.
            if not _orch._is_in_canonical_write_universe(_t_canon):
                continue
            _teams_captured.add(_t_canon)
            _t_stamped = _orch.stamp_available_at_explicit(_t_league_df, when=_orch.datetime.now(_orch.UTC))
            _orch._gated_sink_write(
                _orch._sports_ref_sink_for(bucket, date, "teams"),
                data=_t_stamped,
                partition={"entity": "teams", "league": _t_canon},
                filename="teams.parquet",
                venue="api_football",
                entity="teams",
            )
            if manifest is not None:
                manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                    row_key={"date": date, "data_type": "TEAMS", "league_id": _t_canon},
                    df=_t_stamped,
                    asset_group="sports",
                    instrument_type="",
                    data_type="TEAMS",
                    league_id=_t_canon,
                    pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    source=_orch._sports_ref_source("teams"),
                    service_emission_state=None,
                )
        if manifest is not None:
            hooks.emit_empty_gaps_for_entity("TEAMS", _teams_captured)
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


async def _fetch_and_cache_standings(
    *, adapter: BaseSportsReferenceAdapter, hooks: _AfManifestHooks, prediction_league_ids: list[int]
) -> _orch.pd.DataFrame | None:
    """Fetch (or reuse cached) STANDINGS rows for every league TEAMS was fetched for."""
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
                hooks.note_failed("STANDINGS", exc, league_id=str(lid))
        if all_standings:
            standings_df = _orch.pd.DataFrame(all_standings)
            _orch._set_cached_standings(standings_df)
            _orch.logger.info("Sports reference: %d standing rows fetched (API calls — will cache)", len(standings_df))
    else:
        _orch.logger.info("Sports reference: %d standings from cache (0 API calls)", len(standings_df))
    return standings_df


def _write_standings_per_league(
    *, standings_df: _orch.pd.DataFrame, bucket: str, date: str, hooks: _AfManifestHooks, counts: dict[str, int]
) -> None:
    """Write per-league partitioned STANDINGS files + per-league manifest rows."""
    manifest = hooks.manifest
    if "league_id" in standings_df.columns:
        _std_captured: set[str] = set()
        for _s_lid, _s_league_df in standings_df.groupby("league_id"):
            _s_lid_str = str(_s_lid)
            _s_canon = _orch._canonical_league_id(_s_lid_str)
            # WRITE-UNIVERSE gate (incident 2026-06-24): never write a captured
            # standings row for a league outside our tracked api_football set —
            # un-canonicalisable leagues stay numeric-keyed and split the schema.
            if not _orch._is_in_canonical_write_universe(_s_canon):
                continue
            _std_captured.add(_s_canon)
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


async def _fetch_teams_and_standings(
    *,
    adapter: BaseSportsReferenceAdapter,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
) -> None:
    """Teams + standings — for every api_football-covered league (cached across dates).

    UAC ``SPORTS_ENTITY_LEAGUE_COVERAGE["TEAMS"]`` / ``["STANDINGS"]`` are both
    ``None`` ("expected on all fixture dates" — i.e. all leagues, not just
    Prediction-tier). The league source here MUST match the enumerator's
    denominator (``get_expected_leagues_for_source("api_football")``, the
    same call ``hooks.emit_empty_gaps_for_entity`` already uses for the
    STANDINGS honest-absence emission below) — looping the narrower
    Prediction-tier ``get_prediction_leagues()`` here silently starved 61 of
    94 expected leagues of any per-league TEAMS/STANDINGS capture ever
    (2026-07-13 root-cause: a classification-filter mismatch between this
    writer and the enumerator, not a real per-league data gap).
    """
    teams_df, prediction_league_ids = await _fetch_and_cache_teams(adapter=adapter, hooks=hooks)
    if teams_df is not None:
        _write_teams_and_venues(teams_df=teams_df, bucket=bucket, date=date, hooks=hooks, counts=counts)
    standings_df = await _fetch_and_cache_standings(
        adapter=adapter, hooks=hooks, prediction_league_ids=prediction_league_ids
    )
    if standings_df is not None:
        _write_standings_per_league(standings_df=standings_df, bucket=bucket, date=date, hooks=hooks, counts=counts)


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
                    _inj_canon = _orch._canonical_league_id(_inj_lid_str)
                    # WRITE-UNIVERSE gate: get_injuries(date) is date-wide and returns
                    # injuries for the ENTIRE api_football universe (~1200 leagues). Only
                    # write captured rows for leagues we actually track — otherwise the
                    # ~1100 out-of-universe leagues (un-canonicalisable → numeric-keyed)
                    # pollute the manifest with a second schema (incident 2026-06-24).
                    if not _orch._is_in_canonical_write_universe(_inj_canon):
                        continue
                    _inj_captured.add(_inj_canon)
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
        # Root-cause fix (api_football_injuries_blank_league_orphan_2026_07_15,
        # see plans/active/sports_data_sources_canonical_completion_2026_07_13.md):
        # ``get_injuries(date)`` is a single DATE-WIDE call — a top-level
        # exception here previously fell through to ``hooks.note_failed`` with
        # NO ``league_id``, writing one blank-league_id date-aggregate
        # ``attempted_failed`` row. That row_key can never be superseded by
        # this function's per-league ``record_captured``/``emit_empty_gaps_
        # for_entity`` success paths (they always key on a real canonical
        # ``league_id``), so it sat stuck forever even after a LATER retry
        # genuinely captured every league for that date — live-verified
        # 2026-07-15: 1,923 such rows, 100% blank league_id, frozen across a
        # 12+-hour active re-attempt window. Mirrors the footystats
        # ``_fetch_footystats_predictions``/``_matches``/``_odds`` fix
        # (instruments-service@ed3e75b8) — write per-league instead.
        for _exp_lid in sorted({lg.league_id for lg in _orch.get_expected_leagues_for_source("api_football")}):
            hooks.note_failed("INJURIES", exc, league_id=_exp_lid)
