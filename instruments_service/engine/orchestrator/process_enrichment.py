"""process_instruments sports enrichment stage (7) — reference data + providers.

Cohesion module of the ``engine.orchestrator`` package. Carries the sports
enrichment stage decomposed out of the legacy ~1,931-line
``process_instruments`` body (pure behaviour-preserving extraction; plan:
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
    from collections.abc import Callable

    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_run_sports_enrichment",
]


async def _run_sports_enrichment(
    *,
    date: str,
    bucket: str,
    counts: dict[str, int],
    asset_groups: list[str],
    api_keys: dict[str, str] | None,
    active_venues: list[str],
    sports_entity_filter: str | None,
    sports_provider: str | None,
    missing_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
    season_override: int | None,
) -> None:
    """Stage 7 — SPORTS enrichment: fetch and write reference data.

    (teams, leagues, etc.) alongside fixtures. These are slow-moving entities
    that don't change per-date but are re-fetched to capture transfers,
    promotions, new seasons. Mutates ``counts`` in place.
    """
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
            await _fetch_sports_reference_block(
                date=date,
                bucket=bucket,
                counts=counts,
                api_football_key=api_football_key,
                missing_entities=missing_entities,
                recovery_fixture_ids=recovery_fixture_ids,
                redo_all=redo_all,
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
            await _run_footystats_enrichment(
                date=date,
                bucket=bucket,
                counts=counts,
                footystats_key=footystats_key,
                entity_wanted=_entity_wanted,
                redo_all=redo_all,
            )

        await _run_remaining_enrichment(
            date=date,
            bucket=bucket,
            counts=counts,
            api_keys=api_keys,
            active_venues_set=_active_venues_set,
            ef=_ef,
            entity_wanted=_entity_wanted,
            season_override=season_override,
            redo_all=redo_all,
            sports_entity_filter=sports_entity_filter,
            sports_provider=sports_provider,
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


async def _fetch_sports_reference_block(
    *,
    date: str,
    bucket: str,
    counts: dict[str, int],
    api_football_key: str,
    missing_entities: list[str],
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> None:
    """Fetch sports reference entities + write their manifest rows."""
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
        entities_to_fetch=missing_entities if missing_entities else None,
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


async def _run_footystats_enrichment(
    *,
    date: str,
    bucket: str,
    counts: dict[str, int],
    footystats_key: str,
    entity_wanted: Callable[[str], bool],
    redo_all: bool,
) -> None:
    """FootyStats predictions / matches / odds enrichment fetches."""
    if entity_wanted("PREDICTIONS"):
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

    if entity_wanted("MATCHES"):
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

    if entity_wanted("ODDS"):
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


async def _run_remaining_enrichment(
    *,
    date: str,
    bucket: str,
    counts: dict[str, int],
    api_keys: dict[str, str] | None,
    active_venues_set: set[str],
    ef: str | None,
    entity_wanted: Callable[[str], bool],
    season_override: int | None,
    redo_all: bool,
    sports_entity_filter: str | None,
    sports_provider: str | None,
) -> None:
    """Understat / Transfermarkt / SFI / Weather enrichment fetches."""
    if "UNDERSTAT" in active_venues_set and entity_wanted("XG"):
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

    if "UNDERSTAT" in active_venues_set and entity_wanted("XG_SHOTS"):
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

    transfermarkt_key = api_keys.get("transfermarkt") if (api_keys and "TRANSFERMARKT" in active_venues_set) else None
    if not transfermarkt_key and "TRANSFERMARKT" in active_venues_set:
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
    if transfermarkt_key and _tm_trigger and entity_wanted("PLAYER_VALUES"):
        try:
            tm_counts = await _orch._fetch_transfermarkt_data(
                date=date,
                api_key=transfermarkt_key,
                bucket=bucket,
                entity_filter=ef,
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
        api_keys.get("soccer_football_info") if (api_keys and "SOCCER_FOOTBALL_INFO" in active_venues_set) else None
    )
    if not sfi_key and "SOCCER_FOOTBALL_INFO" in active_venues_set:
        _orch.logger.warning(
            "SOCCER_FOOTBALL_INFO is active but no API key found — skipping SFI fetch for date=%s.",
            date,
        )
    if sfi_key and entity_wanted("SFI_PROGRESSIVE_STATS"):
        try:
            sfi_counts = await _orch._fetch_sfi_data(
                date=date,
                api_key=sfi_key,
                bucket=bucket,
                entity_filter=ef,
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

    if entity_wanted("WEATHER") and (
        "OPEN_METEO" in active_venues_set or sports_entity_filter == "WEATHER" or sports_provider == "OPEN_METEO"
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
