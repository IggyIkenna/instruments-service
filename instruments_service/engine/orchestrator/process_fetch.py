"""process_instruments fetch stages — URDI fetch, date filtering, per-fixture fast path.

Cohesion module of the ``engine.orchestrator`` package. Carries the fetch /
filter stages decomposed out of the legacy ~1,931-line ``process_instruments``
body (pure behaviour-preserving extraction; plan:
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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_PER_FIXTURE_ENTITIES",
    "_UrdiFetchOutcome",
    "_fetch_urdi_records",
    "_filter_and_enrich_records",
    "_per_fixture_gcs_fast_path",
    "_resolve_skip_urdi",
]


# Enrichment-only entities don't need URDI at all — they fetch by date,
# not by fixture ID.  Skip the expensive URDI bootstrap entirely.
_ENRICHMENT_ONLY_ENTITIES: frozenset[str] = frozenset(
    {
        "XG",
        "MATCHES",
        "PREDICTIONS",
        "PLAYER_VALUES",
    }
)

# Per-fixture entities need fixture IDs but can read them from GCS
# instead of making expensive URDI calls to API Football.
_PER_FIXTURE_ENTITIES: frozenset[str] = frozenset(
    {
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "FIXTURE_STATS",
        "PLAYER_STATS",
    }
)


@dataclass
class _UrdiFetchOutcome:
    """Result of the URDI fetch stage (stage 2)."""

    records: list[_orch.InstrumentRecord] = field(default_factory=list)
    retryable_venues: list[str] = field(default_factory=list)
    # Venues where the adapter ran without error (even if 0 records returned).
    # Used by the completeness check to distinguish "adapter returned nothing
    # for this date range" (OK) from "adapter failed to respond" (completeness
    # failure).
    non_error_venues: set[str] = field(default_factory=set)


def _resolve_skip_urdi(sports_entity_filter: str | None) -> bool:
    """Return True when the entity-scoped run can skip the URDI bootstrap."""
    _skip_urdi = sports_entity_filter in (_ENRICHMENT_ONLY_ENTITIES | _PER_FIXTURE_ENTITIES)
    if sports_entity_filter in _ENRICHMENT_ONLY_ENTITIES:
        _orch.logger.info(
            "Skipping URDI fetch — %s is an enrichment-only entity (fetches by date, not fixture ID)",
            sports_entity_filter,
        )
    elif sports_entity_filter in _PER_FIXTURE_ENTITIES:
        _orch.logger.info(
            "Skipping URDI fetch — %s will read fixture IDs from existing GCS fixtures",
            sports_entity_filter,
        )
    return _skip_urdi


async def _fetch_urdi_records(
    *,
    active_venues: list[str],
    api_keys: dict[str, str] | None,
    date: str,
    mode: str,
    source: str | None,
    skip_urdi: bool,
) -> _UrdiFetchOutcome:
    """Stage 2 — fetch from URDI, the sole external API path.

    api_keys injected from preflight() → validate_api_keys_for_venues() → Secret Manager.
    date passed so date-aware adapters (e.g. API-Football) can filter server-side.

    DeFi batch optimisation: DeFi instruments are monotonically growing
    (immutable contracts, never deleted). In batch mode, the universe is
    fetched ONCE and cached — subsequent dates in the range just filter
    by available_from_datetime. Non-DeFi venues are fetched fresh per date.
    """
    out = _UrdiFetchOutcome()
    records = out.records
    _retryable_venues = out.retryable_venues
    _non_error_venues = out.non_error_venues

    defi_venue_names = frozenset(_orch._DEFI_VENUES)
    if skip_urdi:
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

    return out


async def _per_fixture_gcs_fast_path(
    *,
    date: str,
    asset_groups: list[str],
    api_keys: dict[str, str] | None,
    sports_entity_filter: str | None,
    recovery_fixture_ids: frozenset[int] | None,
    redo_all: bool,
) -> dict[str, int]:
    """Per-fixture URDI skip: read fixture IDs from GCS and jump to enrichment.

    This avoids the URDI fetch + date filter which returns 0 for historical dates.
    """
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
        entities_to_fetch=[sports_entity_filter] if sports_entity_filter else None,
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


def _filter_and_enrich_records(
    *,
    records: list[_orch.InstrumentRecord],
    date: str,
    asset_groups: list[str],
) -> tuple[list[_orch.InstrumentRecord], _orch.datetime, frozenset[str] | None]:
    """Stage 3 — filter to instruments active on the requested date + enrich.

    URDI adapters return the full historical instrument universe; this reduces
    it to only instruments tradeable on the requested day. Passes the DeFi
    venue set so the filter can warn on missing available_from_datetime.
    Returns ``(records, date_dt, defi_venue_set)`` for downstream stages.
    """
    is_defi_run = any(c.upper() in ("DEFI", "ALL") for c in asset_groups)
    defi_venue_set: frozenset[str] | None = frozenset(_orch._DEFI_VENUES) if is_defi_run else None
    date_dt = _orch.datetime.fromisoformat(date).replace(tzinfo=_orch.UTC)
    records = _orch.filter_instruments_by_date(records, date_dt, defi_venues=defi_venue_set)
    _orch.logger.info(
        "Date filter %s: %d instruments active (from URDI fetch)",
        date,
        len(records),
    )

    # §1.5/G1.4 noise guard — reject junk/test/non-ASCII instruments at CAPTURE time
    # (every AG), so CJK/meme test bases (龙虾/币安人生/我踏马来了) never enter by_date/ →
    # never reach the catalogue roll-up / coverage / MTDS.
    _before_junk = len(records)
    records = _orch.reject_junk_instruments(records)
    if len(records) != _before_junk:
        _orch.logger.info(
            "Junk-symbol guard %s: %d → %d instruments (rejected %d)",
            date,
            _before_junk,
            len(records),
            _before_junk - len(records),
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

    return records, date_dt, defi_venue_set
