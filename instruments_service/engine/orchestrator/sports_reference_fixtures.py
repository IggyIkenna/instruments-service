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

from unified_api_contracts.sports import FIXTURES_SCHEDULE, get_leagues_by_classification

from instruments_service.engine.orchestrator.sports_reference_filters import _entity_league_scope
from instruments_service.engine.orchestrator.sports_reference_fixtures_write import (
    _entity_dt_for_short,
    _write_per_fixture_entities,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from instruments_service.engine import orchestrator as _orch
    from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks
    from instruments_service.reference_data.adapters.sports.adapters.base import BaseSportsReferenceAdapter
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_resolve_fixture_ids",
    "_resolve_fixture_league_slug",
    "_run_per_fixture_enrichment",
]


async def _resolve_fixture_ids(
    *,
    adapter: BaseSportsReferenceAdapter,
    api_key: str,
    date: str,
    bucket: str,
    fixture_ids_override: list[int] | None,
    hooks: _AfManifestHooks,
    redo_all: bool = False,
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
        await _ensure_canonical_fixtures_for_override(date=date, bucket=bucket, api_key=api_key, redo_all=redo_all)
        return fixture_ids, _af_fid_to_league
    return await _fetch_fixture_ids_via_api(adapter=adapter, date=date, bucket=bucket, hooks=hooks)


async def _ensure_canonical_fixtures_for_override(
    *,
    date: str,
    bucket: str,
    api_key: str,
    redo_all: bool = False,
) -> None:
    """Ensure canonical fixtures exist at sports_reference/by_date/entity=fixtures/.

    The URDI phase writes instrument records, but features-sports needs the
    canonical fixture format (af_fixture_id, timestamp, home/away names, etc.).
    Read from the old path (sports_reference/fixtures/day=) or fetch from API.

    Bug fix (2026-07-08): the "already have canonical data" check previously
    probed a single bare ``entity=fixtures/fixtures.parquet`` blob — FIXTURES
    are written per-league under a ``pipeline_mode=`` hive segment
    (``entity=fixtures/league={L}/fixtures.parquet``), so that bare blob is
    never populated post-migration and the check always found nothing. That
    meant this function always fell through to the old-path/API-fetch branch
    even when real per-league fixtures were already captured — wasting 33
    api-football calls per date (a real cost bug, not a data-loss one, since
    the write path below is unaffected). Uses the canonical-then-legacy
    per-league prefix listing (``_read_per_league_entity_df``) instead.
    """
    try:
        _storage = _orch.get_storage_client()
        # Check if per-league canonical data already exists (not instrument records)
        _needs_write = True
        _existing = _orch._read_per_league_entity_df(bucket, date, "fixtures")
        if _existing is not None and ("af_fixture_id" in _existing.columns or "timestamp" in _existing.columns):
            _needs_write = False  # Already canonical format

        # ``--force`` (redo_all) MUST override the existence check. Without this the
        # gate is existence-ONLY, so an already-captured date can never be re-written
        # and a WRITER fix can never reach historical data. Measured 2026-07-18: two
        # full backfill launches of `--entity FIXTURES 2019-01-01..2026-07-17` (the
        # second WITH --force) wrote ZERO entity=fixtures objects, because redo_all
        # was plumbed to the per-fixture enrichment entities but never to here — the
        # enrichment shards re-wrote fine while `round` stayed blank on every fixture
        # row. See sports_features_layer_findings_sweep_2026_07_18 § G.
        if redo_all:
            _needs_write = True

        if _needs_write:
            # Try old path first (zero API calls) — but NOT under ``redo_all``. The
            # old-path parquet is pre-migration data written by the OLD writer, so
            # copying it forward would re-materialise exactly the stale rows the
            # operator passed ``--force`` to replace (e.g. blank ``round``). A forced
            # re-capture must go to the API branch below, which flattens through the
            # CURRENT writer (``_flatten_canonical_fixture_for_disk``) and therefore
            # picks up writer fixes such as instruments-service@19ae5890.
            _old_path = f"sports_reference/fixtures/day={date}/fixtures.parquet"
            _old_blob = _storage.bucket(bucket).blob(_old_path)
            if _old_blob.exists() and not redo_all:
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
                _orch.logger.warning("LEGACY_FLAT_PATH_HIT sports_ref_fixtures date=%s (%d rows)", date, len(_old_df))
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


def _resolve_fixture_league_slug(fx: object, af_id_to_canonical_league: dict[int, str]) -> str | None:
    """Resolve a fixture's league to its canonical LEAGUE_REGISTRY slug.

    Numeric ``api_football_id`` resolution wins: ``fx.league.league_id`` is a raw
    provider display name (ambiguous for 6 leagues, e.g. CHAMPIONSHIP = English +
    Scottish), so only the numeric id can disambiguate. Falls back to the raw
    value for an unregistered league (honest absence). None if no league at all.
    See plans/active/issues/sports_league_id_namespace_migration_2026_07_20.md.
    """
    _af_lid = getattr(getattr(fx, "league", None), "api_football_id", None)
    _canon = af_id_to_canonical_league.get(_af_lid) if isinstance(_af_lid, int) else None
    if _canon is not None:
        return _canon
    if hasattr(fx, "league") and hasattr(fx.league, "league_id"):
        return str(fx.league.league_id)
    return None


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
                    _slug = _resolve_fixture_league_slug(fx, _af_id_to_canonical_league)
                    if _slug is not None:
                        _af_fid_to_league[str(fid_int)] = _slug
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
                hooks.note_empty(FIXTURES_SCHEDULE, league_id=_lid, reason=str(_reason))
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
        hooks.note_failed(FIXTURES_SCHEDULE, exc)
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

    entity_rows, entity_failures, pre_captured_leagues = await _gather_per_fixture_rows(
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
        pre_captured_leagues=pre_captured_leagues,
        af_fid_to_league=af_fid_to_league,
        recovery_fixture_ids=recovery_fixture_ids,
    )


async def _read_captured_per_entity_league(
    *,
    bucket: str,
    date: str,
    per_fixture_entities: list[tuple[str, Callable[[int], Awaitable[Sequence[object]]]]],
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    redo_all: bool,
) -> dict[tuple[str, str], frozenset[int]]:
    """Pre-fetch skip: read existing per-league parquet for each (entity, league)
    cell on this date, build the set of af_fixture_ids already captured, so the
    caller can skip api_football calls for those fixtures. Returns ``{}`` when
    ``redo_all`` or there's no league mapping to work from.

    Why this exists: today's manifest is keyed on (date, data_type, league_id)
    — it tracks "the cell is captured" but NOT which fixtures within the cell
    are captured. So the cell-level pre-flight (at orchestrator entry) can't
    tell "5 of 10 fixtures already done" from "all 10 already done." The fix:
    at fetch-time, read the per-league parquet (which IS keyed at
    af_fixture_id row granularity) and skip api calls for fixtures already
    represented. Generalises to any future per-fixture entity recovery —
    e.g. when downstream of recovered FIXTURES, only the genuinely-missing
    fixtures get re-fetched, not the entire cell.

    Batched per entity, not per (entity, league) pair
    (sports_dependency_check_manifest_vs_gcs_path_2026_07_08.md,
    ``sports_fixtures.py:356`` todo — see
    ``sports_fixture_prefetch_skip._read_captured_league_fixture_ids_for_entity``
    for why per-entity, not per-corpus, batching is the real ceiling here).
    Remaining (small, entity-count-bounded) calls fan out concurrently via
    ``asyncio.to_thread`` + ``asyncio.gather``.
    """
    if redo_all or not af_fid_to_league:
        return {}

    entity_names: list[str] = []
    lookup_keys: list[tuple[str, str]] = []
    for entity_name, _ in per_fixture_entities:
        _entity_scope = _entity_league_scope(_entity_dt_for_short(entity_name))
        entity_leagues_seen: set[str] = set()
        for fid in fixture_ids:
            canonical_league = af_fid_to_league.get(str(fid))
            if not canonical_league:
                continue
            canonical_league = _orch._canonical_league_id(canonical_league)
            if _entity_scope is not None and canonical_league not in _entity_scope:
                continue  # out of policy scope — queueing loop skips it too, no prefetch needed
            if canonical_league in entity_leagues_seen:
                continue
            entity_leagues_seen.add(canonical_league)
            lookup_keys.append((entity_name, canonical_league))
        if entity_leagues_seen:
            entity_names.append(entity_name)

    if not lookup_keys:
        return {}

    per_entity_results = await _orch.asyncio.gather(
        *[
            _orch.asyncio.to_thread(
                _orch._read_captured_league_fixture_ids_for_entity,
                bucket,
                date,
                entity_name,
            )
            for entity_name in entity_names
        ]
    )
    captured_by_entity = dict(zip(entity_names, per_entity_results, strict=True))
    return {
        (entity_name, canonical_league): captured_by_entity.get(entity_name, {}).get(canonical_league, frozenset())
        for entity_name, canonical_league in lookup_keys
    }


async def _gather_per_fixture_rows(
    *,
    per_fixture_entities: list[tuple[str, Callable[[int], Awaitable[Sequence[object]]]]],
    date: str,
    bucket: str,
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    redo_all: bool,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, tuple[int, str]], dict[str, set[str]]]:
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

    # Pre-fetch skip (see _read_captured_per_entity_league docstring): read
    # existing per-league parquets CONCURRENTLY and skip api_football calls
    # for fixtures already represented. Bypassed when ``redo_all`` is True.
    captured_per_entity_league = await _read_captured_per_entity_league(
        bucket=bucket,
        date=date,
        per_fixture_entities=per_fixture_entities,
        fixture_ids=fixture_ids,
        af_fid_to_league=af_fid_to_league,
        redo_all=redo_all,
    )

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
    skipped_out_of_entity_scope = 0
    # Track, per (entity, league), whether ANY task was actually queued this
    # run and whether at least one of its fixtures was provider-covered
    # (i.e. reached the already-captured check rather than being skipped as
    # observed-out-of-coverage). A league with zero queued tasks that DID
    # reach that check was skipped purely because every one of its fixtures
    # was already present on disk — see ``pre_captured_leagues`` below.
    _queued_leagues: set[tuple[str, str]] = set()
    _provider_covered_leagues: set[tuple[str, str]] = set()
    for entity_name, fetch_fn in per_fixture_entities:
        _af_entity_dt = _entity_dt_for_short(entity_name)
        _entity_scope = _entity_league_scope(_af_entity_dt)
        for fid in fixture_ids:
            canonical_league = af_fid_to_league.get(str(fid))
            canonical_league = _orch._canonical_league_id(canonical_league) if canonical_league else ""
            if canonical_league and _entity_scope is not None and canonical_league not in _entity_scope:
                # Policy skip, not a provider gap (see _entity_league_scope docstring).
                skipped_out_of_entity_scope += 1
                continue
            if (
                canonical_league
                and _af_entity_dt
                and not _orch.is_league_entity_covered(canonical_league, _af_entity_dt)
            ):
                # League never yields this entity in API-Football — skip the call.
                skipped_no_provider_coverage += 1
                continue
            if canonical_league:
                _provider_covered_leagues.add((entity_name, canonical_league))
            if not redo_all and captured_per_entity_league and canonical_league:
                captured_set = captured_per_entity_league.get((entity_name, canonical_league), frozenset())
                if int(fid) in captured_set:
                    skipped_already_captured += 1
                    continue
            if canonical_league:
                _queued_leagues.add((entity_name, canonical_league))
            tasks.append(_orch.asyncio.ensure_future(_fetch_one(entity_name, fetch_fn, fid)))

    if skipped_out_of_entity_scope:
        _orch.logger.info(
            "Per-fixture entity-scope skip: %d (entity, fixture_id) pairs out of "
            "SPORTS_ENTITY_LEAGUE_COVERAGE policy scope",
            skipped_out_of_entity_scope,
        )
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

    # A no-op run must never demote a present cell to empty_confirmed: a
    # league with pre-existing captured data (non-empty captured_set) whose
    # every fixture on this date was skip-as-already-present (no task
    # queued, and it wasn't excluded as observed-out-of-provider-coverage)
    # already has real data on disk — exclude it from the empty-gap
    # emission below instead of letting an incomplete this-run
    # captured_league_ids set stamp it EXPECTED_NO_FIXTURE (2026-07-14 GW
    # verification: 3,720 false-empty cells on the top/prediction-tier
    # leagues whose enrichment was entirely skip-as-present).
    pre_captured_leagues: dict[str, set[str]] = {name: set() for name, _ in per_fixture_entities}
    for (entity_name, canonical_league), captured_set in captured_per_entity_league.items():
        if not captured_set:
            continue
        if (entity_name, canonical_league) in _queued_leagues:
            continue
        if (entity_name, canonical_league) not in _provider_covered_leagues:
            continue
        pre_captured_leagues[entity_name].add(canonical_league)

    return entity_rows, entity_failures, pre_captured_leagues
