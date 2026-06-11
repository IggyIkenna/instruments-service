"""process_instruments shard-completeness stage (8) — retry + honest coverage.

Cohesion module of the ``engine.orchestrator`` package. Carries the shard
completeness check + automatic retry decomposed out of the legacy ~1,931-line
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

from unified_api_contracts.sports import get_league

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_completeness_and_retry",
]


async def _completeness_and_retry(
    *,
    counts: dict[str, int],
    date: str,
    date_dt: _orch.datetime,
    defi_venue_set: frozenset[str] | None,
    asset_groups: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
    source: str | None,
    bucket: str,
    sink: _orch.DataSink,
    sampler: _orch.SamplingService,
    active_venues: list[str],
    non_error_venues: set[str],
    validation_failed_venues: set[str],
    retryable_venues: list[str],
    is_sports_run: bool,
    sports_entity_filter: str | None,
    recovery_fixture_ids: frozenset[int] | None,
) -> dict[str, int]:
    """Stage 8 — shard completeness check + automatic retry for missing venues.

    Expected = configured active_venues (from category config + launch date
    filter), NOT what was fetched. If a venue returns 0 instruments (adapter
    error, network failure), it must show up as missing — never silently pass.

    HOWEVER, venues are excluded from expected if:
     - Adapter ran OK (in non_error_venues) but returned 0 records after date
       filtering — the data source simply has no data for that date (e.g. NASDAQ
       before DBEQ.BASIC dataset starts, or CME on a holiday).
     - Validation rejected all records (validation_failed_venues) — data quality
       issue, not a missing-data issue. Per-record SCHEMA_VALIDATION_FAILED
       events logged upstream.
     - [SPORTS] The venue doesn't cover any leagues with fixtures on this date.
       Each league declares its data_sources in UAC LeagueDefinition. A venue
       is only expected if at least one league with fixtures lists it.

    When venues are missing (typically due to API rate limits or transient
    errors), retry just the missing venues with exponential backoff before
    failing.
    """
    expected_venues = set(active_venues)
    written_venues = set(counts.keys())

    # Sports: scope expected venues by league coverage.
    # Understat covers ~6 leagues, FootyStats ~50, SFI varies.
    # Only expect a venue if it covers leagues that had fixtures today.
    #
    # Performance: same 25MB / 2.6M-row manifest read as the upstream
    # _date_fixture_leagues read (freshness pre-flight) — invalidated by every
    # manifest.write() so it misses cache on every date in BatchIO. Skip
    # this read when scope is already explicit (sports_entity_filter or
    # recovery_fixture_ids set) — the per-fixture entity loop has already
    # decided what to fetch from the explicit scope; the venue-scoping
    # is only needed for full-spectrum runs that haven't pre-decided.
    if is_sports_run and not (sports_entity_filter or recovery_fixture_ids is not None):
        expected_venues = _scope_sports_expected_venues(expected_venues=expected_venues, date=date)

    # Venues where the adapter succeeded but no records survived date/relevance filtering
    # are not "missing" — the data source simply had nothing for this date.
    empty_ok_venues = (non_error_venues - written_venues) - validation_failed_venues
    if empty_ok_venues:
        _orch.logger.info(
            "Shard completeness: %d venue(s) fetched OK but 0 records after filtering (excluded from expected): %s",
            len(empty_ok_venues),
            sorted(empty_ok_venues),
        )
    expected_venues -= empty_ok_venues
    expected_venues -= validation_failed_venues

    missing_shards = expected_venues - written_venues

    written_venues, missing_shards = await _retry_missing_venues(
        counts=counts,
        missing_shards=missing_shards,
        expected_venues=expected_venues,
        written_venues=written_venues,
        retryable_venues=retryable_venues,
        date=date,
        date_dt=date_dt,
        defi_venue_set=defi_venue_set,
        asset_groups=asset_groups,
        api_keys=api_keys,
        mode=mode,
        source=source,
        bucket=bucket,
        sink=sink,
        sampler=sampler,
    )

    return _finalize_completeness(
        counts=counts,
        date=date,
        expected_venues=expected_venues,
        written_venues=written_venues,
        missing_shards=missing_shards,
        bucket=bucket,
    )


def _scope_sports_expected_venues(
    *,
    expected_venues: set[str],
    date: str,
) -> set[str]:
    """Remove sports venues whose covered leagues have no fixtures today."""
    try:
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
    return expected_venues


async def _retry_missing_venues(
    *,
    counts: dict[str, int],
    missing_shards: set[str],
    expected_venues: set[str],
    written_venues: set[str],
    retryable_venues: list[str],
    date: str,
    date_dt: _orch.datetime,
    defi_venue_set: frozenset[str] | None,
    asset_groups: list[str],
    api_keys: dict[str, str] | None,
    mode: str,
    source: str | None,
    bucket: str,
    sink: _orch.DataSink,
    sampler: _orch.SamplingService,
) -> tuple[set[str], set[str]]:
    """Retry ONLY venues that failed with retryable errors.

    (RATE_LIMIT, NETWORK, TIMEOUT, SERVER_ERROR). Permanent failures
    (UNSUPPORTED, ADAPTER_ERROR, PARSE_ERROR) are not retried — they'll fail
    the same way again. Exponential backoff: 10s, 30s. Enough for rate limits
    to clear. Returns the updated ``(written_venues, missing_shards)``.
    """
    retry_delays = [10, 30]
    retryable_set = set(retryable_venues)
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

    return written_venues, missing_shards


def _finalize_completeness(
    *,
    counts: dict[str, int],
    date: str,
    expected_venues: set[str],
    written_venues: set[str],
    missing_shards: set[str],
    bucket: str,
) -> dict[str, int]:
    """Final completeness assessment + honest-coverage manifest rows.

    Raises:
        RuntimeError: below 50% completeness = catastrophic failure (network
            outage, API down) — the data is unusable and should not be treated
            as success.
    """
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

    total = sum(counts.values())
    _orch.log_event(
        "PROCESSING_COMPLETED",
        details={"date": date, "total_records": total, "venues": len(counts)},
    )
    _orch.logger.info("instruments: date=%s wrote %d records across %d venues", date, total, len(counts))
    return counts
