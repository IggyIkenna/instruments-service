"""process_instruments zero-record handling — stage 4 of the orchestration flow.

Cohesion module of the ``engine.orchestrator`` package. Carries the
zero-records-after-filter handling decomposed out of the legacy ~1,931-line
``process_instruments`` body (pure behaviour-preserving extraction; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

For SPORTS: zero fixtures on a given day is normal (no matches scheduled) —
write honest ``empty_confirmed`` markers and still run the reference fetches.
For DeFi in batch mode: zero records before the first pool creation is
expected — skip silently. For CeFi/TradFi: zero records on a trading day means
something is broken → fail the shard (non-trading days get honest manifest
markers instead).

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

from unified_api_contracts.registry.market_data_categories import VENUE_TO_ASSET_GROUP

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_handle_zero_records",
]


async def _handle_zero_records(
    *,
    date: str,
    asset_groups: list[str],
    active_venues: list[str],
    mode: str,
    api_keys: dict[str, str] | None,
    league_filter: list[str] | None,
    missing_entities: list[str],
    per_fixture_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
    sports_entity_filter: str | None,
    season_override: int | None,
    fixtures_fetch_failed: bool = False,
) -> dict[str, int]:
    """Stage 4 — handle zero records after the date filter.

    ``fixtures_fetch_failed`` is True when the sports fixtures venue's adapter
    actually ERRORED (raised → ``failed_venues``), as opposed to running clean
    and returning zero fixtures. A failed fetch must record ``attempted_failed``
    (not ``empty_confirmed``) so the gap stays visible.

    Raises:
        RuntimeError: when zero records is NOT an expected absence
            (CeFi/TradFi active trading day → fail the shard).
    """
    is_sports_only = all(c.upper() == "SPORTS" for c in asset_groups)
    if is_sports_only:
        return await _zero_records_sports(
            date=date,
            asset_groups=asset_groups,
            active_venues=active_venues,
            api_keys=api_keys,
            league_filter=league_filter,
            missing_entities=missing_entities,
            per_fixture_entities=per_fixture_entities,
            recovery_fixture_ids=recovery_fixture_ids,
            redo_all=redo_all,
            sports_entity_filter=sports_entity_filter,
            season_override=season_override,
            fixtures_fetch_failed=fixtures_fetch_failed,
        )
    return _zero_records_non_sports(
        date=date,
        asset_groups=asset_groups,
        active_venues=active_venues,
        mode=mode,
    )


async def _zero_records_sports(
    *,
    date: str,
    asset_groups: list[str],
    active_venues: list[str],
    api_keys: dict[str, str] | None,
    league_filter: list[str] | None,
    missing_entities: list[str],
    per_fixture_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
    sports_entity_filter: str | None,
    season_override: int | None,
    fixtures_fetch_failed: bool = False,
) -> dict[str, int]:
    """SPORTS zero-fixture day: honest empty markers + reference fetches.

    ``fixtures_fetch_failed`` → the fixtures venue errored, so write
    ``attempted_failed`` markers instead of ``empty_confirmed``.
    """
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _orch._get_instruments_bucket(primary_asset_group)

    _zero_sports_empty_fixture_markers(
        date=date,
        bucket=bucket,
        league_filter=league_filter,
        fixtures_fetch_failed=fixtures_fetch_failed,
    )

    # Still fetch sports reference data (leagues/teams/standings/injuries)
    # even when no fixtures exist. These are date-independent slow-moving
    # entities needed for downstream feature computation.
    if api_keys:
        api_football_key = api_keys.get("api_football")
        if api_football_key:
            await _zero_sports_reference_fetch(
                date=date,
                bucket=bucket,
                api_football_key=api_football_key,
                missing_entities=missing_entities,
                per_fixture_entities=per_fixture_entities,
                recovery_fixture_ids=recovery_fixture_ids,
                redo_all=redo_all,
            )
        _zero_sports_enrichment_markers(date=date, bucket=bucket, active_venues=active_venues)

    counts = await _zero_sports_fixture_independent(
        date=date,
        bucket=bucket,
        active_venues=active_venues,
        api_keys=api_keys,
        sports_entity_filter=sports_entity_filter,
        season_override=season_override,
        redo_all=redo_all,
    )

    _orch.log_event("PROCESSING_COMPLETED", details={"date": date, "asset_groups": asset_groups, "fixtures": 0})
    return counts


def _zero_sports_empty_fixture_markers(
    *,
    date: str,
    bucket: str,
    league_filter: list[str] | None,
    fixtures_fetch_failed: bool = False,
) -> None:
    """Write one marker per prediction league for a zero-fixture day.

    When ``fixtures_fetch_failed`` is True the fixtures venue's adapter ERRORED
    (e.g. API-Football plan/quota/auth error → ``response: []``); the day's
    "zero fixtures" is NOT honest absence, so write ``attempted_failed`` via
    ``record_failed`` so the gap stays visible + is backfilled. Only a clean
    fetch that genuinely returned zero fixtures writes ``empty_confirmed``.

    Honest-coverage (CLAUDE.md "4 pillars" #1): we use ``record_empty`` here,
    NOT ``add(row_count=0)``. Marking zero-fixture days as ``captured`` with
    row_count=0 is the exact anti-pattern the rule was added for — it inflates
    the captured count and masks honest absence. Reference incident 2026-05-06:
    AUSTRIAN_BUNDESLIGA, GREEK_SUPER_LEAGUE et al. showed 3041 captured
    FIXTURES rows that were ALL phantoms (instrument_count=0, no parquet on
    disk) before this fix.

    We also DROP the empty placeholder parquet write — empty placeholders that
    look populated are worse than missing data because they evade detection.
    If a date has no fixtures, no parquet should exist; the manifest's
    ``empty_confirmed`` row is the single honest marker.
    """
    _empty_league_ids = league_filter if league_filter else _orch.get_all_prediction_league_ids()
    _empty_attempt_ts = _orch.datetime.now(_orch.UTC)
    _empty_manifest = _orch.ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
    )
    for _league_id in _empty_league_ids:
        _row_key = {
            "date": date,
            "data_type": "FIXTURES",
            "league_id": _orch._canonical_league_id(_league_id),
        }
        if fixtures_fetch_failed:
            # Fetch ERRORED (plan/quota/auth/etc.) — NOT honest absence.
            _empty_manifest.record_failed(
                row_key=_row_key,
                error="FIXTURES_FETCH_FAILED",
                attempted_at=_empty_attempt_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
            )
        else:
            _empty_manifest.record_empty(
                row_key=_row_key,
                attempted_at=_empty_attempt_ts,
                reason=_orch.EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
                pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
            )
    _empty_manifest.write()
    if fixtures_fetch_failed:
        _orch.logger.warning(
            "SPORTS: fixtures FETCH FAILED for date=%s — wrote attempted_failed markers for %d leagues",
            date,
            len(_empty_league_ids),
        )
    else:
        _orch.logger.info(
            "SPORTS: No fixtures for date=%s — wrote empty_confirmed markers for %d leagues",
            date,
            len(_empty_league_ids),
        )


async def _zero_sports_reference_fetch(
    *,
    date: str,
    bucket: str,
    api_football_key: str,
    missing_entities: list[str],
    per_fixture_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> None:
    """Fetch sports reference entities on a zero-fixture day + write manifest."""
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
        entities_to_fetch=missing_entities if missing_entities else None,
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
                    # The fetch succeeded (no exception escaped from
                    # _fetch_sports_reference_data for this entity) and
                    # returned 0 rows — a genuine 2xx+0-rows response that
                    # satisfies FetchEvidence.proves_honest_absence().
                    _entity_attempt_ts = _orch.datetime.now(_orch.UTC)
                    _entity_ev = _orch.FetchEvidence(
                        http_status=200,
                        response_received=True,
                        rows_in_response=0,
                        source="api_football",
                        endpoint=f"api_football_{entity_name.lower()}",
                        attempted_at=_entity_attempt_ts,
                        error_signal="",
                    )
                    sports_manifest.record_empty(
                        row_key={
                            "date": date,
                            "data_type": entity_name.upper(),
                        },
                        attempted_at=_entity_attempt_ts,
                        reason=_orch.EmptyConfirmedReason.SOURCE_RETURNED_ZERO,  # QG-allow: sports-entity-no-fixture-oracle; proven honest absence via fetch_evidence (clean 2xx+0-rows)
                        pipeline_mode=_orch._pipeline_mode_for_sports_data_type(entity_name.upper()),
                        fetch_evidence=_entity_ev,
                    )
        # Per-fixture entities on zero-fixture dates: nothing
        # to fetch (no fixtures = no per-fixture data). Write
        # ``empty_confirmed`` markers so the orchestrator
        # knows we attempted and skip-on-rerun without
        # inflating the captured count (CLAUDE.md "4 pillars"
        # #1: row_count > 0 OR record_empty, never
        # ``captured`` with row_count=0).
        for pf_entity in per_fixture_entities:
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


def _zero_sports_enrichment_markers(
    *,
    date: str,
    bucket: str,
    active_venues: list[str],
) -> None:
    """Zero-fixture fast path: fixture-dependent enrichment entities get
    0-count manifest entries immediately (no API calls / rate limits).

    Fixture-INDEPENDENT entities (Transfermarkt, SFI) are excluded — they
    provide reference data (team values, standings) regardless of whether
    matches are played on this date.
    """
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


async def _zero_sports_fixture_independent(
    *,
    date: str,
    bucket: str,
    active_venues: list[str],
    api_keys: dict[str, str] | None,
    sports_entity_filter: str | None,
    season_override: int | None,
    redo_all: bool,
) -> dict[str, int]:
    """Fixture-independent reference data: fetch even on zero-fixture dates.

    Only fires on trigger dates (season start, transfer window open/close) to
    avoid re-fetching identical squad data every day.
    """
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

    transfermarkt_key = api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in _active_venues_set) else None
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
        api_keys.get("soccer_football_info") if (api_keys and "SOCCER_FOOTBALL_INFO" in _active_venues_set) else None
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

    return counts


def _zero_records_non_sports(
    *,
    date: str,
    asset_groups: list[str],
    active_venues: list[str],
    mode: str,
) -> dict[str, int]:
    """Zero-record handling for DeFi batch / TradFi non-trading days.

    Raises:
        RuntimeError: when the zero is not an expected absence.
    """
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
    tradfi_active = [v for v in active_venues if VENUE_TO_ASSET_GROUP.get(v) == "tradfi"]
    if tradfi_active:
        target_dt = _orch.date_type.fromisoformat(date)
        non_trading_venues = [v for v in tradfi_active if _orch.is_non_trading_day(v, target_dt)]
        # Stamp each non-trading venue individually — do NOT require ALL tradfi venues
        # to be non-trading.  The old ``len(non_trading_venues) == len(tradfi_active)``
        # guard blocked the stamp whenever FX (declared 24/7; never non-trading) was
        # in the active set, which is always the case for a TRADFI backfill run.
        # Result: every weekend/holiday the entire tradfi non-trading-day path was
        # unreachable, leaving CME/NASDAQ/NYSE/CBOE cells silently absent.
        if non_trading_venues:
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
            # FX is 24/7 — if it is in tradfi_active it returns records normally
            # (not suppressed here).  Return only the non-trading venues as 0-count.
            if len(non_trading_venues) == len(tradfi_active):
                # All tradfi venues are non-trading (FX absent or a pure-non-trading
                # subset): zero records is fully accounted for — signal clean exit.
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
