"""Sports reference fixture-id resolution + per-fixture enrichment helpers.

Cohesion module of the ``engine.orchestrator`` package. Carries the
fixture-id resolution and per-fixture enrichment stages decomposed out of the
legacy ~882-line ``_fetch_sports_reference_data`` body (pure
behaviour-preserving extraction; plan:
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

from unified_api_contracts.sports import get_leagues_by_classification

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from instruments_service.engine import orchestrator as _orch
    from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks
    from instruments_service.reference_data.adapters.sports.adapters.base import BaseSportsReferenceAdapter
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_resolve_fixture_ids",
    "_run_per_fixture_enrichment",
]

# Per-fixture entity short-name → canonical manifest data_type. SSOT for the
# entity-axis key used by both the observed-coverage skip (in
# ``_gather_per_fixture_rows``) and the per-league manifest writes (in
# ``_write_per_fixture_entities``) so the two never drift.
_ENTITY_DT_BY_SHORT: dict[str, str] = {
    "fixture_stats": "FIXTURE_STATS",
    "fixture_events": "FIXTURE_EVENTS",
    "fixture_lineups": "FIXTURE_LINEUPS",
    "player_stats": "PLAYER_STATS",
}


def _entity_dt_for_short(entity_name: str) -> str:
    """Canonical manifest data_type for a per-fixture entity short-name (``""`` if unknown)."""
    return _ENTITY_DT_BY_SHORT.get(entity_name, "")


async def _resolve_fixture_ids(
    *,
    adapter: BaseSportsReferenceAdapter,
    api_key: str,
    date: str,
    bucket: str,
    fixture_ids_override: list[int] | None,
    hooks: _AfManifestHooks,
) -> tuple[list[int], dict[str, str]]:
    """Resolve completed fixture IDs + the AF fixture_id → league mapping.

    Per-fixture enrichment (stats, events, lineups, player stats) only applies
    to completed fixtures (status in FT/AET/PEN — stats unavailable for
    future/live). Uses ``fixture_ids_override`` when available (from the URDI
    instruments fetch or GCS lookup) to avoid the redundant 33-league re-fetch
    (saves 33 API calls per date).
    """
    if fixture_ids_override is not None:
        fixture_ids = fixture_ids_override
        _orch.logger.info(
            "Sports reference: %d completed fixture IDs passed from URDI (0 extra API calls)", len(fixture_ids)
        )
        # Build AF fixture_id -> league mapping from GCS fixtures parquet
        _af_fid_to_league = _orch._build_fixture_league_map_from_gcs(bucket, date)
        await _ensure_canonical_fixtures_for_override(date=date, bucket=bucket, api_key=api_key)
        return fixture_ids, _af_fid_to_league
    return await _fetch_fixture_ids_via_api(adapter=adapter, date=date, bucket=bucket, hooks=hooks)


async def _ensure_canonical_fixtures_for_override(
    *,
    date: str,
    bucket: str,
    api_key: str,
) -> None:
    """Ensure canonical fixtures exist at sports_reference/by_date/entity=fixtures/.

    The URDI phase writes instrument records, but features-sports needs the
    canonical fixture format (af_fixture_id, timestamp, home/away names, etc.).
    Read from the old path (sports_reference/fixtures/day=) or fetch from API.
    v9: probe canonical path (pipeline_mode= in prefix) first, then legacy.
    """
    _new_fixtures_canonical = _orch._sports_ref_canonical_blob_path(date, "fixtures", filename="fixtures.parquet")
    _new_fixtures_legacy = _orch._sports_ref_legacy_blob_path(date, "fixtures", filename="fixtures.parquet")
    try:
        _storage = _orch.get_storage_client()
        _new_fixtures_path = _orch._resolve_sports_ref_blob(
            _storage, bucket, _new_fixtures_canonical, _new_fixtures_legacy
        )
        _new_blob = _storage.bucket(bucket).blob(_new_fixtures_path)
        # Check if path already has canonical data (not instrument records)
        _needs_write = True
        if _new_blob.exists():
            _existing = _orch.pd.read_parquet(
                _orch.io.BytesIO(_storage.download_bytes(bucket=bucket, blob_path=_new_fixtures_path))
            )
            if "af_fixture_id" in _existing.columns or "timestamp" in _existing.columns:
                _needs_write = False  # Already canonical format

        if _needs_write:
            # Try old path first (zero API calls)
            _old_path = f"sports_reference/fixtures/day={date}/fixtures.parquet"
            _old_blob = _storage.bucket(bucket).blob(_old_path)
            if _old_blob.exists():
                _old_data = _storage.download_bytes(bucket=bucket, blob_path=_old_path)
                _old_df = _orch.pd.read_parquet(_orch.io.BytesIO(_old_data))
                # v9: _write_fixtures_per_league creates entity-specific sink internally
                _orch._write_fixtures_per_league(
                    _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                    _old_df,
                    date,
                    source_label="old-path-copy",
                    bucket=bucket,
                )
                _orch.logger.info(
                    "Canonical fixtures copied from old path to entity=fixtures/ (%d rows)",
                    len(_old_df),
                )
            else:
                # No old path — fetch from API Football (costs 33 API calls).
                # Paired with raw so Q5/Q6 lifecycle columns populate (live=batch).
                _adapter = _orch.create_sports_reference_adapter("api_football", api_key=api_key)
                _fx_pairs = await _adapter.get_fixtures_with_raw(date)
                if _fx_pairs:
                    _fx_dicts = [
                        _orch._flatten_canonical_fixture_for_disk(fx, date, af_response=raw) for fx, raw in _fx_pairs
                    ]
                    _fx_df = _orch.pd.DataFrame(_fx_dicts)
                    # PIT safety: scheduled fixtures published ~1 week before kickoff
                    if "timestamp" in _fx_df.columns:
                        _fx_df["available_at"] = _orch.pd.to_datetime(
                            _fx_df["timestamp"], utc=True, errors="coerce"
                        ) - _orch.pd.Timedelta(days=7)
                    _orch._write_fixtures_per_league(
                        _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                        _fx_df,
                        date,
                        source_label="api-fetch-override",
                        bucket=bucket,
                    )
                    _orch.logger.info(
                        "Canonical fixtures fetched from API and written to entity=fixtures/ (%d fixtures)",
                        len(_fx_df),
                    )
    except Exception as _fx_exc:
        _orch.logger.warning("Could not ensure canonical fixtures at entity=fixtures/: %s", _fx_exc)


async def _fetch_fixture_ids_via_api(
    *,
    adapter: BaseSportsReferenceAdapter,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
) -> tuple[list[int], dict[str, str]]:
    """Fallback: fetch fixtures from API (33 calls for 33 leagues).

    Only used when called from the zero-fixture early-return path where URDI
    returned 0 instruments.
    """
    fixture_ids: list[int] = []
    _af_fid_to_league: dict[str, str] = {}
    completed_statuses = {"FT", "AET", "PEN"}
    fallback_league_ids: list[int] = []
    _af_id_to_canonical_league: dict[int, str] = {}
    # Fetch fixtures for ALL football leagues (prediction + features + reference).
    # Reference leagues (cups, continental) provide team workload context for
    # fatigue/distance calculations. Features leagues (lower divisions) provide
    # additional fixture data for cross-division team tracking.
    for cls in ("Prediction", "Features", "Reference"):
        for league_def in get_leagues_by_classification(cls):
            if league_def.api_football_id is not None:
                fallback_league_ids.append(league_def.api_football_id)
                _af_id_to_canonical_league[league_def.api_football_id] = league_def.league_id
    try:
        # Paired fetch so the flatten below can populate Q5/Q6 lifecycle
        # columns from the raw api-football response (live=batch parity).
        _fixture_pairs = await adapter.get_fixtures_with_raw(date, league_ids=fallback_league_ids)
        fixtures = [_fx for _fx, _raw in _fixture_pairs]
        for fx in fixtures:
            if fx.status in completed_statuses:
                raw_id = fx.source_fixture_id or fx.fixture_id
                with _orch.contextlib.suppress(ValueError, TypeError):
                    fid_int = int(raw_id)
                    fixture_ids.append(fid_int)
                    # Map AF ID -> league from the fixture's league object
                    if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                        _af_fid_to_league[str(fid_int)] = str(fx.league.league_id)
                    elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                        af_lid = fx.league.api_football_id
                        if af_lid in _af_id_to_canonical_league:
                            _af_fid_to_league[str(fid_int)] = _af_id_to_canonical_league[af_lid]
            elif fx.status in {"PST", "CANC"}:
                _reason = (
                    _orch.EmptyConfirmedReason.EXPECTED_FIXTURE_POSTPONED
                    if fx.status == "PST"
                    else _orch.EmptyConfirmedReason.EXPECTED_FIXTURE_CANCELLED
                )
                _lid: str = ""
                if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
                    _lid = str(fx.league.league_id)
                elif hasattr(fx, "league") and hasattr(fx.league, "api_football_id"):
                    af_lid = fx.league.api_football_id
                    _lid = _af_id_to_canonical_league.get(af_lid, "")
                hooks.note_empty("FIXTURES", league_id=_lid, reason=str(_reason))
        _orch.logger.info("Sports reference: %d completed fixtures found for enrichment (API fetch)", len(fixture_ids))

        # Write canonical fixtures to sports_reference/by_date/entity=fixtures/
        # so features-sports-service and trigger scheduler can read them.
        if fixtures:
            try:
                fixture_dicts = [
                    _orch._flatten_canonical_fixture_for_disk(fx, date, af_response=raw) for fx, raw in _fixture_pairs
                ]
                fixture_df = _orch.pd.DataFrame(fixture_dicts)
                # PIT safety: scheduled fixtures published ~1 week before kickoff
                if "timestamp" in fixture_df.columns:
                    fixture_df["available_at"] = _orch.pd.to_datetime(
                        fixture_df["timestamp"], utc=True, errors="coerce"
                    ) - _orch.pd.Timedelta(days=7)
                _orch._write_fixtures_per_league(
                    _orch._sports_ref_sink_for(bucket, date, "fixtures"),
                    fixture_df,
                    date,
                    source_label="api-fetch-fallback",
                    bucket=bucket,
                )
                _orch.logger.info(
                    "Canonical fixtures written to sports_reference/by_date/entity=fixtures/ (%d fixtures)",
                    len(fixture_df),
                )
            except Exception as _fx_write_exc:
                _orch.logger.warning("Failed to write canonical fixtures to reference path: %s", _fx_write_exc)

    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="sports_reference_fixtures_fetch",
            shard=date,
        )
        hooks.note_failed("FIXTURES", exc)
    return fixture_ids, _af_fid_to_league


async def _run_per_fixture_enrichment(
    *,
    adapter: BaseSportsReferenceAdapter,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    fetch_set: set[str] | None,
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> None:
    """Per-fixture enrichment: stats, events, lineups, player stats."""
    _per_fixture_entities: list[tuple[str, Callable[[int], Awaitable[Sequence[object]]]]] = [
        ("fixture_stats", adapter.get_fixture_statistics),
        ("fixture_events", adapter.get_fixture_events),
        ("fixture_lineups", adapter.get_fixture_lineups),
        ("player_stats", adapter.get_fixture_player_stats),
    ]

    # Filter to only fetch entities that are actually missing
    if fetch_set is not None:
        _per_fixture_entities = [(name, fn) for name, fn in _per_fixture_entities if name in fetch_set]
        skipped = 4 - len(_per_fixture_entities)
        if skipped:
            _orch.logger.info(
                "Per-fixture: skipping %d entities already in manifest, fetching %s",
                skipped,
                [n for n, _ in _per_fixture_entities],
            )

    entity_rows, entity_failures = await _gather_per_fixture_rows(
        per_fixture_entities=_per_fixture_entities,
        date=date,
        bucket=bucket,
        fixture_ids=fixture_ids,
        af_fid_to_league=af_fid_to_league,
        redo_all=redo_all,
    )

    _write_per_fixture_entities(
        date=date,
        bucket=bucket,
        hooks=hooks,
        counts=counts,
        entity_names=[name for name, _ in _per_fixture_entities],
        entity_rows=entity_rows,
        entity_failures=entity_failures,
        af_fid_to_league=af_fid_to_league,
        recovery_fixture_ids=recovery_fixture_ids,
    )


async def _gather_per_fixture_rows(
    *,
    per_fixture_entities: list[tuple[str, Callable[[int], Awaitable[Sequence[object]]]]],
    date: str,
    bucket: str,
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    redo_all: bool,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, tuple[int, str]]]:
    """Concurrent per-fixture fetching with rate-limit semaphore.

    API Football Mega plan: 900 req/min shared across the fleet. The
    adapter's ``_throttle()`` serialises per-process requests at
    ``_min_request_interval`` (0.067s = 15 req/sec per VM), so with
    ~20 concurrent backfill VMs the fleet can burst to 300 req/sec =
    18 000 req/min — far over the 900/min cap.

    ``concurrency = 10`` limits in-flight tasks per-process. Combined
    with the per-class token-bucket throttle (0.067s/req), this yields
    at most 10 tasks x 1/0.067 ≈ 150 req/sec per VM — but the lock
    in ``_throttle`` serialises ALL tasks onto the one token-bucket
    slot, so the actual throughput is still capped at 15 req/sec per VM.
    The semaphore's role is to cap the queue depth (avoids spawning
    thousands of coroutines that all immediately block on the lock),
    not to control the send rate (that's the lock). 10 gives the
    throttle lock enough in-flight coroutines to keep the pipe full
    without overwhelming the VM's async loop.

    The ``_fetch_and_extract`` helper in ``ApiFootballAdapter`` handles
    JSON-envelope ``rateLimit`` responses (HTTP 200 +
    ``{"errors": {"rateLimit": "..."}}```) with minute-boundary back-off
    + retry, so transient fleet-level quota exhaustion no longer records
    every fixture as ``attempted_failed``.
    """
    concurrency = 10
    sem = _orch.asyncio.Semaphore(concurrency)
    entity_rows: dict[str, list[dict[str, object]]] = {name: [] for name, _ in per_fixture_entities}
    # Per-entity failure tracking for honest-coverage: map entity → (failed_count, sample_error_code).
    entity_failures: dict[str, tuple[int, str]] = {name: (0, "") for name, _ in per_fixture_entities}

    async def _fetch_one(
        entity_name: str,
        fetch_fn: Callable[[int], Awaitable[Sequence[object]]],
        fid: int,
    ) -> None:
        async with sem:
            try:
                rows = await fetch_fn(fid)
                for row in rows:
                    # Adapters return a mix of Pydantic models and plain dicts
                    # depending on whether the normalizer produces a typed model.
                    if hasattr(row, "model_dump"):
                        d: dict[str, object] = _orch.cast(_orch._ModelDumpable, row).model_dump()
                    else:
                        d = _orch.cast(dict[str, object], row)
                    entity_rows[entity_name].append(d)
            except Exception as exc:
                _orch.classify_and_emit_error(
                    exc,
                    service_name="instruments-service",
                    operation=f"sports_reference_{entity_name}_fetch",
                    shard=str(fid),
                )
                # Shard-level failure isolation: count the failure so that
                # honest-coverage can record_failed at entity-level if
                # EVERY fixture call for this entity raised.
                _prev_count, _prev_code = entity_failures[entity_name]
                entity_failures[entity_name] = (
                    _prev_count + 1,
                    _prev_code or _orch._classify_adapter_failure(exc, "api_football"),
                )
            # Throttle handled by adapter's _get_with_retry + rate limit headers

    # Pre-fetch skip: read existing per-league parquet for each (entity, league)
    # cell on this date, build the set of af_fixture_ids already captured, and
    # skip api_football calls for those fixtures. Bypassed when ``redo_all`` is
    # True (i.e. the operator passed --force, explicitly asking to re-fetch
    # everything).
    #
    # Why this exists: today's manifest is keyed on (date, data_type, league_id)
    # — it tracks "the cell is captured" but NOT which fixtures within the cell
    # are captured. So the cell-level pre-flight (at orchestrator entry) can't
    # tell "5 of 10 fixtures already done" from "all 10 already done." The fix:
    # at fetch-time, read the per-league parquet (which IS keyed at
    # af_fixture_id row granularity) and skip api calls for fixtures already
    # represented. Generalises to any future per-fixture entity recovery —
    # e.g. when downstream of recovered FIXTURES, only the genuinely-missing
    # fixtures get re-fetched, not the entire cell.
    captured_per_entity_league: dict[tuple[str, str], frozenset[int]] = {}
    if not redo_all and af_fid_to_league:
        for entity_name, _ in per_fixture_entities:
            _entity_leagues_seen: set[str] = set()
            for fid in fixture_ids:
                canonical_league = af_fid_to_league.get(str(fid))
                if not canonical_league:
                    continue
                canonical_league = _orch._canonical_league_id(canonical_league)
                if canonical_league in _entity_leagues_seen:
                    continue
                _entity_leagues_seen.add(canonical_league)
                captured_set = _orch._read_existing_per_league_fixture_ids(
                    bucket=bucket,
                    date=date,
                    entity_name=entity_name,
                    canonical_league_id=canonical_league,
                )
                captured_per_entity_league[(entity_name, canonical_league)] = captured_set

    # Build all tasks: N entities x M fixtures (only missing entities)
    #
    # Out-of-coverage skip (observed (league x entity) map): API-Football only
    # provides PLAYER_STATS / FIXTURE_LINEUPS / FIXTURE_EVENTS / FIXTURE_STATS
    # for SOME leagues (measured ~57% of /fixtures/players calls return 0 —
    # 729 of 790 leagues never yield PLAYER_STATS). For a (league, entity) that
    # the captured corpus shows is observed-out-of-coverage we DON'T call the
    # API at all (kills the wasted fan-out); the zero-row cell is recorded as
    # ``EXPECTED_NO_PROVIDER_COVERAGE`` by the emit_empty path below rather than
    # forced to ``attempted_failed`` by the live-instrument guard. SSOT:
    # ``unified_api_contracts.registry.sports_league_entity_coverage``.
    tasks: list[_orch.asyncio.Task[None]] = []
    skipped_already_captured = 0
    skipped_no_provider_coverage = 0
    for entity_name, fetch_fn in per_fixture_entities:
        _af_entity_dt = _entity_dt_for_short(entity_name)
        for fid in fixture_ids:
            canonical_league = af_fid_to_league.get(str(fid))
            canonical_league = _orch._canonical_league_id(canonical_league) if canonical_league else ""
            if (
                canonical_league
                and _af_entity_dt
                and not _orch.is_league_entity_covered(canonical_league, _af_entity_dt)
            ):
                # League never yields this entity in API-Football — skip the call.
                skipped_no_provider_coverage += 1
                continue
            if not redo_all and captured_per_entity_league and canonical_league:
                captured_set = captured_per_entity_league.get((entity_name, canonical_league), frozenset())
                if int(fid) in captured_set:
                    skipped_already_captured += 1
                    continue
            tasks.append(_orch.asyncio.ensure_future(_fetch_one(entity_name, fetch_fn, fid)))

    if skipped_no_provider_coverage:
        _orch.logger.info(
            "Per-fixture observed-coverage skip: %d (entity, fixture_id) pairs whose (league, entity) is "
            "observed-out-of-coverage in API-Football — skipping api calls (honest EXPECTED_NO_PROVIDER_COVERAGE)",
            skipped_no_provider_coverage,
        )
    if skipped_already_captured:
        _orch.logger.info(
            "Per-fixture pre-fetch skip: %d (entity, fixture_id) pairs already in existing per-league "
            "parquets — skipping api_football calls (pass --force to re-fetch regardless)",
            skipped_already_captured,
        )
    _orch.logger.info(
        "Per-fixture enrichment: %d fixtures x %d entities = %d calls queued (concurrency=%d, "
        "skipped_already_captured=%d)",
        len(fixture_ids),
        len(per_fixture_entities),
        len(tasks),
        concurrency,
        skipped_already_captured,
    )
    await _orch.asyncio.gather(*tasks)

    return entity_rows, entity_failures


def _write_per_fixture_entities(
    *,
    date: str,
    bucket: str,
    hooks: _AfManifestHooks,
    counts: dict[str, int],
    entity_names: list[str],
    entity_rows: dict[str, list[dict[str, object]]],
    entity_failures: dict[str, tuple[int, str]],
    af_fid_to_league: dict[str, str],
    recovery_fixture_ids: frozenset[int] | None,
) -> None:
    """Write per-league partitioned per-fixture entity files + manifest rows."""
    manifest = hooks.manifest

    for entity_name in entity_names:
        _af_entity_dt = _entity_dt_for_short(entity_name)
        all_rows = entity_rows[entity_name]
        if all_rows:
            df = _orch.pd.DataFrame(all_rows)

            # Drop columns containing nested structures (lists/dicts) that
            # cannot be serialised to Parquet.  API Football player_stats
            # responses may carry a raw "statistics" column with nested
            # dicts even after normalisation.
            _nested_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, (dict, list))).any()]
            if _nested_cols:
                _orch.logger.info(
                    "Dropping %d nested columns from %s: %s",
                    len(_nested_cols),
                    entity_name,
                    _nested_cols,
                )
                df = df.drop(columns=_nested_cols)

            # PIT safety: per-fixture stats/events/lineups/player_stats available ~2h after kickoff.
            # No per-row kickoff here — approximate using date + 17:00 UTC (15:00 typical KO + 2h).
            df["available_at"] = _orch.pd.Timestamp(date, tz="UTC") + _orch.pd.Timedelta(hours=17)

            counts[entity_name] = len(df)

            # Write per-league partitioned files using AF fixture_id -> league mapping.
            # Column name is "af_fixture_id" (not "fixture_id") in per-fixture entity data.
            _fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
            if _fid_col in df.columns and af_fid_to_league:
                # Ensure string type for map lookup (map keys are str(int(af_id)))
                df["_league_id"] = df[_fid_col].astype(str).str.split(".").str[0].map(af_fid_to_league)
                _has_league = df["_league_id"].notna()
                _with_league = df[_has_league]
                _without_league = df[~_has_league]
                _pf_captured: set[str] = set()

                for _pf_lid, _pf_league_df in _with_league.groupby("_league_id"):
                    _pf_lid_str = str(_pf_lid)
                    _pf_canon = _orch._canonical_league_id(_pf_lid_str)
                    # WRITE-UNIVERSE gate: fixtures roll-up spans the whole api_football
                    # universe; only write per-fixture captures for tracked leagues so
                    # out-of-universe (numeric-keyed) leagues don't pollute the manifest
                    # with a second schema (incident 2026-06-24).
                    if not _orch._is_in_canonical_write_universe(_pf_canon):
                        continue
                    _pf_captured.add(_pf_canon)
                    _pf_clean = _pf_league_df.drop(columns=["_league_id"])

                    # Recovery mode: read existing per-league parquet (if
                    # any) and merge our newly-fetched fixture rows so we
                    # don't lose previously-captured fixtures' data. The
                    # standard write path is overwrite-on-write, which is
                    # safe when we always fetch ALL fixtures for the
                    # (date, league) cell — but in recovery mode we only
                    # fetched a subset, so a plain overwrite would drop
                    # the rest of the cell.
                    if recovery_fixture_ids is not None and _fid_col in _pf_clean.columns:
                        _pf_clean = _orch._merge_with_existing_per_league_parquet(
                            bucket=bucket,
                            date=date,
                            entity_name=entity_name,
                            canonical_league_id=_orch._canonical_league_id(_pf_lid_str),
                            new_rows=_pf_clean,
                            fid_col=_fid_col,
                        )

                    # C.6: available_at = date + 17h already set on df above (KO + 2h
                    # approximation). Preserve it; fillna wall-clock for any NaT rows (defensive).
                    _pf_copy = _pf_clean.copy()
                    _pf_copy["available_at"] = _pf_copy["available_at"].fillna(
                        _orch.pd.Timestamp(_orch.datetime.now(_orch.UTC))
                    )
                    _stamped_pf_df = _pf_copy
                    _orch._gated_sink_write(
                        _orch._sports_ref_sink_for(bucket, date, entity_name),
                        data=_stamped_pf_df,
                        partition={"entity": entity_name, "league": _orch._canonical_league_id(_pf_lid_str)},
                        filename=f"{entity_name}.parquet",
                        venue="api_football",
                        entity=entity_name,
                    )
                    if manifest is not None:
                        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                            row_key={
                                "date": date,
                                "data_type": _af_entity_dt,
                                "league_id": _orch._canonical_league_id(_pf_lid_str),
                            },
                            df=_stamped_pf_df,
                            asset_group="sports",
                            instrument_type="",
                            data_type=_af_entity_dt,
                            league_id=_orch._canonical_league_id(_pf_lid_str),
                            pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                            source=_orch._sports_ref_source(entity_name),
                            service_emission_state=None,
                        )

                # Drop unmapped rows — single-SSOT means bare writes are
                # forbidden for league-axis data types. Surface as a
                # warning so we can spot upstream league-mapping
                # regressions in logs.
                if not _without_league.empty:
                    _orch.logger.warning(
                        "%s bare-path fallback triggered for date=%s — data shape regression: "
                        "%d rows could not be mapped to a league. Skipping bare write to keep manifest honest.",
                        _af_entity_dt,
                        date,
                        len(_without_league),
                    )
                if manifest is not None:
                    hooks.emit_empty_gaps_for_entity(_af_entity_dt, _pf_captured)
            else:
                # Single-SSOT: bare manifest row + bare parquet write are
                # both suppressed; writing one would create a phantom
                # captured shard with no parquet on disk.  Surface the
                # upstream regression in logs so it can be diagnosed.
                _orch.logger.warning(
                    "%s bare-path fallback triggered for date=%s — data shape regression: "
                    "no fixture-id column or empty af_fid->league map (rows=%d). "
                    "Skipping bare write + manifest row to keep manifest honest.",
                    _af_entity_dt,
                    date,
                    len(df),
                )

            _orch.logger.info("Sports reference: %d %s rows written", len(df), entity_name)
        else:
            # Honest-coverage: entity produced zero rows.  Distinguish
            # fetch failure (record_failed → attempted_failed) from legit
            # empty (record_empty → empty_confirmed).
            #
            # CF-11 fix (2026-06-02): the original guard only routed to
            # record_failed when EVERY fixture call raised
            # (_fail_count == len(fixture_ids)).  A partial failure
            # (_fail_count > 0 but < len(fixture_ids)) fell through to
            # emit_empty_gaps_for_entity → empty_confirmed(EXPECTED_NO_FIXTURE),
            # falsely claiming "we know there's nothing" and freezing the
            # gap forever.  Correct rule: ANY failure → record_failed so
            # the shard is flagged for backfill, not silently confirmed-empty.
            _fail_count, _err_code = entity_failures.get(entity_name, (0, ""))
            if _fail_count > 0 and _err_code:
                # At least one fixture call raised → treat the entity as
                # attempted_failed so it is backfilled.  The error code
                # from the first failure is representative; per-fixture
                # errors are already emitted individually by _fetch_one.
                if manifest is not None:
                    manifest.record_failed(
                        row_key={"date": date, "data_type": _af_entity_dt},
                        error=_err_code,
                        attempted_at=hooks.attempt_ts,
                        pipeline_mode=_orch.PipelineMode.BATCH_API_FOOTBALL,
                    )
            else:
                # All calls succeeded but returned zero rows
                # (e.g. post-match stats not yet published, lineups not
                # disclosed for low-profile fixture) — legitimate empty.
                hooks.emit_empty_gaps_for_entity(_af_entity_dt, set())
