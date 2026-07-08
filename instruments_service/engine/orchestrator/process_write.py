"""process_instruments validation + per-venue write stages (5/6).

Cohesion module of the ``engine.orchestrator`` package. Carries the schema
validation and per-venue parquet/manifest write stages decomposed out of the
legacy ~1,931-line ``process_instruments`` body (pure behaviour-preserving
extraction; plan:
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

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from unified_api_contracts import source_string_for
from unified_api_contracts.registry.market_data_categories import VENUE_TO_ASSET_GROUP

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_WriteOutcome",
    "_asset_group_for_venue",
    "_records_to_dataframe",
    "_validate_records",
    "_write_all_venues",
    "_write_tradfi_non_trading_day_entries",
]

# Asset-group tokens whose IS instruments-manifest atom is (date, venue[, chain]) —
# the venue-grain AGs that get EU seeded. Sports/prediction use a (date, data_type)
# grain (materialised elsewhere) so they are excluded. Mirrors the cefi/defi/tradfi
# branches of get_venues_for_asset_groups (venue_core.py). Defined as a frozenset
# constant (not an inline list) so it is a single source of truth + token-checkable.
_VENUE_GRAIN_ASSET_GROUP_TOKENS: frozenset[str] = frozenset({"CEFI", "DEFI", "TRADFI", "ALL"})

# Sports/prediction venue NAMES that an "ALL" run pulls into the venue list but which
# are NOT venue-grain (their honest-absence is materialised on the data_type grain by
# the zero-fixture / empty paths) — excluded from venue-grain EU seeding.
_NON_VENUE_GRAIN_VENUE_NAMES: frozenset[str] = frozenset(
    {
        "API_FOOTBALL",
        "FOOTYSTATS",
        "UNDERSTAT",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "OPEN_METEO",
        "POLYMARKET",
        "KALSHI",
    }
)


def _asset_group_for_venue(venue_str: str) -> str:
    """Return the lowercase asset-group ('cefi'/'defi'/'tradfi'/'sports'/'prediction') for a venue.

    Used by ``_write_all_venues`` when ``asset_groups == ["ALL"]`` so each venue's
    instruments parquet and manifest row land in the correct per-group bucket.

    Lookup order (fast-path first):
    1. Sports/prediction venue names known at module-load time (frozensets, O(1)).
    2. CEFI and TradFi venue lists (O(n), typically small; both imported from _orch).
    3. DeFi: any remaining venue with a "-" in the name (PROTOCOL-CHAIN format).
    4. Fallback → "sports" (conservative; sports bucket is the default primary for
       ALL runs and is the correct target for un-categorised API_FOOTBALL sub-venues).
    """
    upper = venue_str.upper()
    if upper in ("POLYMARKET", "KALSHI"):
        return "prediction"
    if upper in (
        "API_FOOTBALL",
        "FOOTYSTATS",
        "UNDERSTAT",
        "TRANSFERMARKT",
        "SOCCER_FOOTBALL_INFO",
        "OPEN_METEO",
    ):
        return "sports"
    # Use UAC reverse-lookup for cefi/tradfi membership (VENUE_TO_ASSET_GROUP covers the
    # full canonical universe including KALSHI-PERP/POLYMARKET-PERP and YAHOO_FINANCE).
    ag = VENUE_TO_ASSET_GROUP.get(venue_str)
    if ag == "cefi":
        return "cefi"
    if ag == "tradfi":
        return "tradfi"
    # DeFi venues use PROTOCOL-CHAIN format; the "-" is the discriminator.
    if "-" in venue_str:
        return "defi"
    return "sports"


@dataclass
class _WriteOutcome:
    """Result of the per-venue write stage — inputs for retry/completeness."""

    counts: dict[str, int]
    bucket: str
    sink: _orch.DataSink
    sampler: _orch.SamplingService


def _validate_records(
    *,
    records: list[_orch.InstrumentRecord],
    date: str,
) -> tuple[list[_orch.InstrumentRecord], set[str]]:
    """Stage 5 — schema validation with per-record failure isolation.

    (hard_schema_enforcement Phase 2.) Invalid records route to
    SCHEMA_VALIDATION_FAILED event; valid records from the same venue continue
    to record_captured. A venue is added to validation_failed_venues only when
    ALL its records fail — per CLAUDE.md shard-level failure isolation rule
    (no raise inside per-record loop; bad row must not kill the whole shard).

    Raises:
        RuntimeError: when ALL records are rejected by schema validation.
    """
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
    if not valid_records:
        msg = f"All records rejected by schema validation for date={date}"
        _orch.logger.error("%s", msg)
        _orch.log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)
    return valid_records, validation_failed_venues


def _records_to_dataframe(records: list[_orch.InstrumentRecord]) -> _orch.pd.DataFrame:
    """Serialize InstrumentRecord list to a flat DataFrame for parquet writes."""
    from instruments_service.reference_data.adapters.prediction.polymarket import (  # noqa: qg-inside-import
        _clob_token_ids_for_condition_id,
    )

    rows: list[dict[str, object]] = []
    for r in records:
        d = r.model_dump()
        # Serialize legs list[InstrumentLeg] → JSON string for parquet storage
        if d.get("legs") is not None:
            d["legs"] = _orch.json.dumps(d["legs"])
        # Polymarket: InstrumentRecord (UAC) has no clob_token_ids field. Join the
        # per-outcome decimal CLOB token-ids from the package side-table (keyed by
        # condition_id == instrument_key, registered in parsing._parse_market) so the
        # availability parquet carries the column the Polymarket CLOB WS subscribes by
        # (live + batch). No-op for non-Polymarket rows (lookup returns None).
        _ik = d.get("instrument_key")
        if isinstance(_ik, str):
            _tids = _clob_token_ids_for_condition_id(_ik)
            if _tids:
                d["clob_token_ids"] = _tids
        rows.append(d)
    return _orch.pd.DataFrame(rows)


def _write_sports_fixture_venue(
    *,
    venue_str: str,
    venue_df: _orch.pd.DataFrame,
    date: str,
    league_filter: list[str] | None,
    sink: _orch.DataSink,
    manifest: _orch.ManifestWriter,
    counts: dict[str, int],
    sampler: _orch.SamplingService,
) -> None:
    """League-based sharding: partition sports fixtures by league_id.

    instrument_key format: {LEAGUE}:{HOME}_v_{AWAY}:{DATE} — extract league_id
    as the part before the first colon.
    """
    _sports_df = venue_df.copy()
    _sports_df["_league_id"] = _sports_df["instrument_key"].str.split(":").str[0]
    # Apply league filter if set (--league CLI arg)
    if league_filter:
        _sports_df = _sports_df[_sports_df["_league_id"].isin(league_filter)]
    _captured_lids: set[str] = set()
    for _lid, _league_df in _sports_df.groupby("_league_id"):
        _league_id_str = str(_lid)
        _canonical_lid_str = _orch._canonical_league_id(_league_id_str)
        # WRITE-UNIVERSE gate: skip non-canonical leagues to keep the IS index clean.
        if not _orch._is_in_canonical_write_universe(_canonical_lid_str):
            continue
        _captured_lids.add(_league_id_str)
        _league_df_clean = _league_df.drop(columns=["_league_id"])
        _stamped_fixture_df = _orch.stamp_available_at_explicit(_league_df_clean, when=_orch.datetime.now(_orch.UTC))
        _orch._gated_sink_write(
            sink,
            data=_stamped_fixture_df,
            partition={
                "day": date,
                "venue": venue_str,
                "league": _canonical_lid_str,
            },
            filename="instruments.parquet",
            venue=venue_str,
            entity="instruments",
        )
        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
            row_key={
                "date": date,
                "data_type": "FIXTURES",
                "league_id": _canonical_lid_str,
            },
            df=_stamped_fixture_df,
            asset_group="sports",
            instrument_type="",
            data_type="FIXTURES",
            league_id=_canonical_lid_str,
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
    _expected_af_lids = {league.league_id for league in _orch.get_expected_leagues_for_source("api_football")}
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


def _write_prediction_venue(
    *,
    venue_str: str,
    venue_df: _orch.pd.DataFrame,
    date: str,
    sink: _orch.DataSink,
    lifecycle_sink: _orch.DataSink,
    manifest: _orch.ManifestWriter,
    counts: dict[str, int],
    sampler: _orch.SamplingService,
) -> None:
    """PREDICTION: bundle by canonical_question_group per the UAC SSOT.

    (``BTC_UP_DOWN_HOURLY`` / ``BTC_UP_DOWN_DAILY`` / ``SPX_UP_DOWN_DAILY`` /
    ``ELECTION_PRESIDENT_2028`` / ``OTHER``, etc.). Recurring canonical groups
    cycle through multiple condition_ids over time — HOURLY = ~24/day,
    DAILY = 1/day — so the shard atom is per-(canonical_group, day), with all
    market_ids active on that day bundled into one parquet (analogous to
    options-chain bundling). Per ``predictions_master.plan.md`` Phase 1
    critical-path + CLAUDE.md "Per-asset-group shard-key matrix → Prediction".
    Polymarket + Kalshi share this path: both prediction venues classify per
    the UAC ``classify_*_to_canonical_group`` SSOT and bundle on the same axis
    so MTDS reads + features compute apply identically.
    """
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
        _stamped_group_df = _orch.stamp_available_at_explicit(_group_df_clean, when=_orch.datetime.now(_orch.UTC))
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
        # The cqg manifest data_type (prediction_canonical_question_group) is MULTI-source
        # in UAC SOURCE_PRIORITY (polymarket_clob + kalshi) → record_captured REQUIRES an
        # explicit venue-derived source=. Resolve a cqg-specific pipeline_mode whose source
        # is in that closed set (POLYMARKET→polymarket_clob, KALSHI→kalshi). The lifecycle
        # write below keeps _pred_pm (prediction_market_lifecycle is single/unregistered).
        _pred_pm = (
            _orch.PipelineMode.BATCH_POLYMARKET_GAMMA_API
            if _manifest_venue == "POLYMARKET"
            else _orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE
        )
        _cqg_pm = (
            _orch.PipelineMode.BATCH_POLYMARKET_CLOB
            if _manifest_venue == "POLYMARKET"
            else _orch.PipelineMode.BATCH_KALSHI
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
            pipeline_mode=_cqg_pm,
            source=source_string_for(_cqg_pm),
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


def _write_tradfi_non_trading_day_entries(
    *,
    date: str,
    non_error_venues: set[str],
    counts: dict[str, int],
    manifest_for_venue: Callable[[str], _orch.ManifestWriter],
) -> set[str]:
    """Write 0-count manifest entries for TRADFI venues that returned 0 instruments
    because the date is a non-trading day (weekend/holiday).

    Without this, those venues have no manifest entry and appear as permanent gaps
    in the data status. ``manifest_for_venue`` allows the caller to route each
    venue to the correct per-group manifest (required for ALL-group runs).

    Returns the set of non-trading venue names that were stamped so the caller can
    suppress regular parquet writes for them (see the per-venue loop in
    ``_write_all_venues``).
    """
    tradfi_empty = {v for v in (non_error_venues - set(counts.keys())) if VENUE_TO_ASSET_GROUP.get(v) == "tradfi"}
    if not tradfi_empty:
        return set()
    target_dt = _orch.date_type.fromisoformat(date)
    non_trading = {v for v in tradfi_empty if _orch.is_non_trading_day(v, target_dt)}
    if not non_trading:
        return set()
    _nt_attempt_ts = _orch.datetime.now(_orch.UTC)
    for venue in sorted(non_trading):
        # Honest-coverage Phase 2.E.2: discriminate weekend vs holiday so the
        # manifest carries an EXPECTED_* row per (shard_key, day).
        _reason = _orch.non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
        manifest_for_venue(venue).record_expected_empty(
            row_key={"date": date, "venue": venue},
            reason=_reason,
            attempted_at=_nt_attempt_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
            # C-#6 contract: an explicit BATCH source must equal
            # source_string_for(pipeline_mode) — see
            # plans/active/issues/manifest_record_expected_empty_blank_source_2026_07_08.md.
            source=source_string_for(_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE),
        )
        counts[venue] = 0
    _orch.logger.info(
        "TRADFI non-trading day manifest: date=%s venues=%s — wrote empty_confirmed entries",
        date,
        sorted(non_trading),
    )
    return non_trading


def _pre_stamp_non_trading_tradfi(
    *,
    date: str,
    records: list[_orch.InstrumentRecord],
    non_error_venues: set[str],
    manifest_for_venue: Callable[[str], _orch.ManifestWriter],
    counts: dict[str, int],
) -> set[str]:
    """Identify tradfi venues that are non-trading on ``date`` and pre-stamp
    ``empty_confirmed`` for them before the per-venue write loop runs.

    Scoped to venues that *actually ran* this call (present in ``non_error_venues``
    or appearing in ``records``) so a CEFI-only call never stamps CME/NASDAQ.

    FX is declared 24/7 — ``is_non_trading_day("FX", …)`` always returns ``False``
    and FX will never appear in the returned set.

    Returns the set of non-trading tradfi venues that were stamped (callers suppress
    parquet writes for these venues to avoid writing look-back artefacts as captured).
    """
    _attempted = {v for v in (non_error_venues | {r.venue for r in records}) if VENUE_TO_ASSET_GROUP.get(v) == "tradfi"}
    target_dt = _orch.date_type.fromisoformat(date)
    non_trading: set[str] = {v for v in _attempted if _orch.is_non_trading_day(v, target_dt)}
    if not non_trading:
        return set()
    _nt_attempt_ts = _orch.datetime.now(_orch.UTC)
    for _ntv in sorted(non_trading):
        _nt_reason = _orch.non_trading_day_reason(_ntv, target_dt) or "EXPECTED_WEEKEND"
        manifest_for_venue(_ntv).record_expected_empty(
            row_key={"date": date, "venue": _ntv},
            reason=_nt_reason,
            attempted_at=_nt_attempt_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
            source=source_string_for(_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE),
        )
        counts[_ntv] = 0
    _orch.logger.info(
        "TRADFI non-trading day (pre-stamp): date=%s venues=%s — wrote empty_confirmed entries",
        date,
        sorted(non_trading),
    )
    return non_trading


def _write_all_venues(
    *,
    records: list[_orch.InstrumentRecord],
    date: str,
    asset_groups: list[str],
    league_filter: list[str] | None,
    non_error_venues: set[str],
) -> _WriteOutcome:
    """Stage 6 — write per-venue parquet + catalogue + CSV sample + manifest."""
    df = _records_to_dataframe(records)

    # Domain validation — logs anomalies, doesn't raise for instruments domain
    _orch.DomainValidationService("instruments").validate_for_domain(df)

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
    # ALL runs use per-venue bucket routing; single-AG runs share one bucket.
    # _is_all_run is the discriminator — "ALL" is not a valid bucket key.
    _raw_primary = asset_groups[0] if asset_groups else None
    _is_all_run = _raw_primary is None or _raw_primary.upper() == "ALL"
    # Primary bucket: "sports" for ALL runs (downstream sports stages need one bucket).
    primary_asset_group: str | None = "sports" if _is_all_run else _raw_primary
    bucket = _orch._get_instruments_bucket(primary_asset_group)
    sink = _orch.get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
    # Prediction market lifecycle parquet (predictions_master Phase 3 L618).
    lifecycle_sink = _orch.get_data_sink(bucket=bucket, prefix="market_lifecycle/by_canonical_group")
    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)

    # Per-bucket helpers for ALL runs: lazily created, pre-seeded with primary bucket.
    _extra_sinks: dict[str, _orch.DataSink] = {bucket: sink}
    _extra_lc_sinks: dict[str, _orch.DataSink] = {bucket: lifecycle_sink}
    _extra_manifests: dict[str, _orch.ManifestWriter] = {bucket: manifest}

    def _sink_for_bucket(b: str) -> _orch.DataSink:
        if b not in _extra_sinks:
            _extra_sinks[b] = _orch.get_data_sink(bucket=b, prefix="instrument_availability/by_date")
        return _extra_sinks[b]

    def _lc_sink_for_bucket(b: str) -> _orch.DataSink:
        if b not in _extra_lc_sinks:
            _extra_lc_sinks[b] = _orch.get_data_sink(bucket=b, prefix="market_lifecycle/by_canonical_group")
        return _extra_lc_sinks[b]

    def _manifest_for_bucket(b: str) -> _orch.ManifestWriter:
        if b not in _extra_manifests:
            _extra_manifests[b] = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=b)
        return _extra_manifests[b]

    def _get_venue_bucket(venue_str: str) -> str:
        """For ALL runs, resolve the correct per-group bucket; otherwise use primary."""
        if not _is_all_run:
            return bucket
        return _orch._get_instruments_bucket(_asset_group_for_venue(venue_str))

    # Identify tradfi venues that are non-trading on this date and pre-stamp
    # empty_confirmed for them.  Scoped to venues that actually ran (union of
    # non_error_venues and record venues) so a CEFI-only run never stamps CME/NASDAQ.
    # FX is 24/7 and is never in the returned set.  The write loop skips parquet
    # writes for these venues (look-back artefact suppression — see helper docstring).
    _non_trading_tradfi = _pre_stamp_non_trading_tradfi(
        date=date,
        records=records,
        non_error_venues=non_error_venues,
        manifest_for_venue=lambda v: _manifest_for_bucket(_get_venue_bucket(v)),
        counts=counts,
    )

    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            venue_str = str(venue_name)
            _v_bucket = _get_venue_bucket(venue_str)
            # For ALL runs use the per-venue bucket objects; for single-AG runs
            # all three vars alias the shared objects (identical to pre-fix behaviour).
            _v_sink = _sink_for_bucket(_v_bucket) if _is_all_run else sink
            _v_lc_sink = _lc_sink_for_bucket(_v_bucket) if _is_all_run else lifecycle_sink
            _v_manifest = _manifest_for_bucket(_v_bucket) if _is_all_run else manifest
            if venue_str == "API_FOOTBALL":
                _write_sports_fixture_venue(
                    venue_str=venue_str,
                    venue_df=venue_df,
                    date=date,
                    league_filter=league_filter,
                    sink=_v_sink,
                    manifest=_v_manifest,
                    counts=counts,
                    sampler=sampler,
                )
            elif venue_str.upper() in ("POLYMARKET", "KALSHI") and "base_asset" in venue_df.columns:
                _write_prediction_venue(
                    venue_str=venue_str,
                    venue_df=venue_df,
                    date=date,
                    sink=_v_sink,
                    lifecycle_sink=_v_lc_sink,
                    manifest=_v_manifest,
                    counts=counts,
                    sampler=sampler,
                )
            elif venue_str in _non_trading_tradfi:
                # Non-trading day for this TradFi venue: the adapter returned records
                # via its look-back window (e.g. DBEQ.BASIC 5-day equity look-back
                # surfaces Friday's definitions on a Saturday query).  These records
                # must NOT be written as captured for the non-trading date — the
                # empty_confirmed stamp was already written in the pre-stamp block above.
                # Shard-level isolation: log + skip, never raise.
                _orch.logger.info(
                    "TRADFI non-trading day: suppressing %d records for venue=%s date=%s "
                    "(look-back artefact — empty_confirmed already stamped)",
                    len(venue_df),
                    venue_str,
                    date,
                )
            else:
                _orch._write_venue(venue_str, venue_df, date, _v_bucket, _v_sink, counts, sampler, _v_manifest)
                # Phase 4.2 (tradfi_canonical_futures_contract_hard_required_fields_2026_05_13):
                # For TradFi futures venues (CME, ICE), also write CanonicalFuturesContract
                # records alongside the InstrumentRecord instruments.parquet.
                # build_futures_contracts() derives all 5 hard-required lifecycle dates
                # from InstrumentRecord.expiry using physical/cash-settled conventions.
                # Shard-level isolation: a write failure here does NOT abort the instruments
                # write — the futures_contracts.parquet is best-effort on the same date.
                if VENUE_TO_ASSET_GROUP.get(venue_str) == "tradfi":
                    _venue_instrument_records = [r for r in records if r.venue == venue_str]
                    _orch._write_futures_contracts(
                        venue_str=venue_str,
                        instrument_records=_venue_instrument_records,
                        date=date,
                        bucket=_v_bucket,
                        sink=_v_sink,
                    )
    else:
        _orch._write_venue("all", df, date, bucket, sink, counts, sampler, manifest)

    # Write 0-count manifest entries for TRADFI non-trading days where the adapter
    # returned zero records (i.e. venues in non_error_venues but not yet in counts).
    # Non-trading venues that DID surface records (look-back artefacts) were already
    # pre-stamped above and are excluded here via set subtraction.
    _write_tradfi_non_trading_day_entries(
        date=date,
        non_error_venues=non_error_venues - _non_trading_tradfi,
        counts=counts,
        manifest_for_venue=lambda v: _manifest_for_bucket(_get_venue_bucket(v)) if _is_all_run else manifest,
    )

    # EU seeding (HARD RULE — the WRITER materialises expected_unattempted, never
    # re-derived downstream): for every TARGET-universe (venue x this-day) cell that
    # was NOT captured this run and has no prior captured/empty/EU row, write an
    # expected_unattempted marker so a missing day reads 0% (honest gap) instead of
    # being silently ABSENT. Out-of-universe (pre-venue-launch) venues are NOT seeded
    # — they stay honestly absent. Rides the same per-run manifest (no extra GCS walk).
    #
    # For ALL runs, _seed_expected_unattempted_for_target_universe must run once per
    # bucket (CEFI/DEFI/TRADFI each have their own manifest and write to their own
    # bucket; sports/prediction EU seeding is handled by their own data paths and is
    # explicitly excluded from venue-grain EU seeding in the helper's docstring).
    if _is_all_run:
        # Expand "ALL" to the explicit venue-grain asset groups so each group seeds
        # its EU markers into the correct per-group manifest.
        for _ag in ("CEFI", "DEFI", "TRADFI"):
            _ag_bucket = _orch._get_instruments_bucket(_ag.lower())
            _seed_expected_unattempted_for_target_universe(
                manifest=_manifest_for_bucket(_ag_bucket),
                date=date,
                asset_groups=[_ag],
                captured_venues=set(counts.keys()),
            )
    else:
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,
            date=date,
            asset_groups=asset_groups,
            captured_venues=set(counts.keys()),
        )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    # For ALL runs _extra_manifests already contains the primary manifest (pre-seeded
    # above), so we flush all per-group manifests together and skip the duplicate
    # manifest.close() that would otherwise double-flush the primary bucket.
    if _is_all_run:
        for _extra_manifest in _extra_manifests.values():
            _extra_manifest.close()
    else:
        manifest.close()

    return _WriteOutcome(counts=counts, bucket=bucket, sink=sink, sampler=sampler)


def _pre_launch_empty_reason(venue_str: str, manifest_chain: str, date: str) -> str:
    """Return the typed EXPECTED_* reason for a pre-launch (venue, date) cell.

    For DeFi venues (manifest_chain non-empty), compare ``date`` against the chain
    genesis date first. A date before chain genesis stamps
    ``EXPECTED_PRE_GENESIS_CHAIN``; a date between chain genesis and the protocol
    discovery start stamps ``EXPECTED_PRE_VENUE_LAUNCH`` (chain is live but the
    specific protocol/adapter is not yet).

    For CeFi/TradFi venues (no chain) the venue's discovery API is simply not live
    yet: ``EXPECTED_PRE_VENUE_LAUNCH``.
    """
    if manifest_chain:
        # Lazy import to avoid circular imports at module load time.
        from unified_api_contracts.registry.chain_env import (  # noqa: imports-inside-functions
            get_chain_genesis_date,
        )

        chain_genesis = get_chain_genesis_date(manifest_chain)
        if chain_genesis and date < chain_genesis:
            return _orch.EmptyConfirmedReason.EXPECTED_PRE_GENESIS_CHAIN.value
    return _orch.EmptyConfirmedReason.EXPECTED_PRE_VENUE_LAUNCH.value


def _seed_expected_unattempted_for_target_universe(
    *,
    manifest: _orch.ManifestWriter,
    date: str,
    asset_groups: list[str],
    captured_venues: set[str],
) -> None:
    """Seed ``expected_unattempted`` for target-universe (venue x day) cells with no row.

    The IS instruments-capture manifest grain is ``(date, venue)`` (+ ``chain`` for
    DeFi). Coverage is dishonest when a missing day is silently ABSENT rather than a
    0% cell — so the producer materialises the expected universe for the dates it
    runs: every configured venue for ``asset_groups`` whose discovery API is live on
    ``date`` (``is_venue_available``) gets an ``expected_unattempted`` row UNLESS it
    was captured this run or already carries a captured / empty_confirmed / EXPECTED_*
    row (``_should_skip_shard``). A later ``record_captured`` / ``record_empty`` /
    ``record_failed`` for the same ``row_key`` cleanly supersedes the seed via the
    consolidator's last-writer-wins merge.

    Scope = the TARGET universe only (the configured cefi/defi venue lists). Venues
    whose discovery API is not yet live on ``date`` (pre-launch) are NOT seeded — they
    stay honestly absent, not a fake 0%. SPORTS / PREDICTION are intentionally excluded
    here: their manifest grain is ``(date, data_type[, league_id])``, not ``(date,
    venue)``, and their honest-absence is already materialised by the zero-fixture /
    empty paths + the sports could-exist roll-up — venue-grain EU seeding would write
    the wrong atom. Only the venue-grain AGs (cefi / defi / tradfi) are seeded.

    Idempotent: re-running a captured day re-seeds nothing (every venue is either in
    ``captured_venues`` or skip-gated by its existing captured row).
    """
    # Only the venue-grain asset_groups carry a (date, venue) EU atom. Sports/
    # prediction use a different grain (see docstring) and are materialised elsewhere.
    _venue_grain_ags = [c for c in asset_groups if c.upper() in _VENUE_GRAIN_ASSET_GROUP_TOKENS]
    if not _venue_grain_ags:
        return

    target_venues = _orch.get_venues_for_asset_groups(_venue_grain_ags)
    _seed_ts = _orch.datetime.now(_orch.UTC)
    _seeded = 0
    _pre_launch_stamped = 0
    for venue_str in target_venues:
        # Drop the sports/prediction venue names that "ALL" pulls in — they are NOT
        # venue-grain (their EU is materialised on the data_type grain elsewhere).
        if venue_str in _NON_VENUE_GRAIN_VENUE_NAMES:
            continue
        # Canonical manifest key (DeFi PROTOCOL-CHAIN → venue=PROTOCOL + chain) so the
        # seed matches the captured-row atom exactly.
        manifest_venue, manifest_chain = _orch._canonical_manifest_venue_chain(venue_str)
        row_key: dict[str, str] = {"date": date, "venue": manifest_venue}
        if manifest_chain:
            row_key["chain"] = manifest_chain
        # NEVER overwrite ANY existing row. EU is the LOWEST-information state — seeding
        # it over a captured / empty_confirmed / attempted_failed / EXPECTED_* cell
        # would MASK the real status (esp. attempted_failed → a hidden fetch failure
        # read as a 0% gap, breaking the honest 4-state). NB: this is a STRICTER test
        # than the capture-path ``_should_skip_shard`` (which re-attempts failed shards
        # on purpose) — EU seeding must leave attempted_failed visible.
        if manifest.lookup(row_key) is not None:
            continue
        # Out-of-universe for this day: the venue's discovery API is not live yet
        # (pre-launch). Stamp honest absence with typed reason instead of leaving cell
        # absent (Fix 1 — silent-absent for pre-genesis / pre-venue-launch dates).
        if not _orch.is_venue_available(venue_str, date):
            _pre_launch_reason = _pre_launch_empty_reason(venue_str, manifest_chain, date)
            manifest.record_expected_empty(
                row_key=row_key,
                reason=_pre_launch_reason,
                attempted_at=_seed_ts,
                pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                source=source_string_for(_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE),
            )
            _pre_launch_stamped += 1
            continue
        if venue_str in captured_venues:
            continue  # captured this run — record_captured already wrote the cell
        manifest.record_expected_unattempted(
            row_key=row_key,
            attempted_at=_seed_ts,
            pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
        )
        _seeded += 1
    if _seeded:
        _orch.logger.info(
            "EU seeding: wrote expected_unattempted for %d target-universe venue cells on date=%s",
            _seeded,
            date,
        )
    if _pre_launch_stamped:
        _orch.logger.info(
            "EU seeding: wrote pre-launch empty_confirmed for %d out-of-universe venue cells on date=%s",
            _pre_launch_stamped,
            date,
        )
