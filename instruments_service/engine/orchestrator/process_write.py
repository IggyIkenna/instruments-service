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

from dataclasses import dataclass
from typing import TYPE_CHECKING

from unified_api_contracts import source_string_for

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_WriteOutcome",
    "_records_to_dataframe",
    "_validate_records",
    "_write_all_venues",
]


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
        _captured_lids.add(_league_id_str)
        _league_df_clean = _league_df.drop(columns=["_league_id"])
        _stamped_fixture_df = _orch.stamp_available_at_explicit(_league_df_clean, when=_orch.datetime.now(_orch.UTC))
        _orch._gated_sink_write(
            sink,
            data=_stamped_fixture_df,
            partition={
                "day": date,
                "venue": venue_str,
                "league": _orch._canonical_league_id(_league_id_str),
            },
            filename="instruments.parquet",
            venue=venue_str,
            entity="instruments",
        )
        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
            row_key={
                "date": date,
                "data_type": "FIXTURES",
                "league_id": _orch._canonical_league_id(_league_id_str),
            },
            df=_stamped_fixture_df,
            asset_group="sports",
            instrument_type="",
            data_type="FIXTURES",
            league_id=_orch._canonical_league_id(_league_id_str),
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
    # Use the first (primary) category to route to the correct category-specific bucket.
    # UCI naming: instruments-store-{category.lower()}-{project}
    # e.g. DEFI → instruments-store-defi-{gcp_project_id}
    primary_asset_group = asset_groups[0] if asset_groups else None
    bucket = _orch._get_instruments_bucket(primary_asset_group)
    # prefix ensures writes land at instrument_availability/by_date/{day=X}/{venue=Y}/
    sink = _orch.get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")
    # Separate sink for prediction market lifecycle parquet (per predictions_master Phase 3 L618).
    # Path: market_lifecycle/by_canonical_group/group={g}/day={d}/market_lifecycle.parquet
    lifecycle_sink = _orch.get_data_sink(bucket=bucket, prefix="market_lifecycle/by_canonical_group")

    manifest = _orch.ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            venue_str = str(venue_name)
            if venue_str == "API_FOOTBALL":
                _write_sports_fixture_venue(
                    venue_str=venue_str,
                    venue_df=venue_df,
                    date=date,
                    league_filter=league_filter,
                    sink=sink,
                    manifest=manifest,
                    counts=counts,
                    sampler=sampler,
                )
            elif venue_str.upper() in ("POLYMARKET", "KALSHI") and "base_asset" in venue_df.columns:
                _write_prediction_venue(
                    venue_str=venue_str,
                    venue_df=venue_df,
                    date=date,
                    sink=sink,
                    lifecycle_sink=lifecycle_sink,
                    manifest=manifest,
                    counts=counts,
                    sampler=sampler,
                )
            else:
                _orch._write_venue(venue_str, venue_df, date, bucket, sink, counts, sampler, manifest)
                # Phase 4.2 (tradfi_canonical_futures_contract_hard_required_fields_2026_05_13):
                # For TradFi futures venues (CME, ICE), also write CanonicalFuturesContract
                # records alongside the InstrumentRecord instruments.parquet.
                # build_futures_contracts() derives all 5 hard-required lifecycle dates
                # from InstrumentRecord.expiry using physical/cash-settled conventions.
                # Shard-level isolation: a write failure here does NOT abort the instruments
                # write — the futures_contracts.parquet is best-effort on the same date.
                if venue_str in frozenset(_orch._TRADFI_VENUES):
                    _venue_instrument_records = [r for r in records if r.venue == venue_str]
                    _orch._write_futures_contracts(
                        venue_str=venue_str,
                        instrument_records=_venue_instrument_records,
                        date=date,
                        bucket=bucket,
                        sink=sink,
                    )
    else:
        _orch._write_venue("all", df, date, bucket, sink, counts, sampler, manifest)

    # Write 0-count manifest entries for TRADFI venues that returned 0 instruments
    # because the date is a non-trading day (weekend/holiday). Without this, those
    # venues have no manifest entry and appear as permanent gaps in the data status.
    _tradfi_set_for_manifest = frozenset(_orch._TRADFI_VENUES)
    tradfi_empty = non_error_venues - set(counts.keys())
    tradfi_empty = {v for v in tradfi_empty if v in _tradfi_set_for_manifest}
    if tradfi_empty:
        target_dt = _orch.date_type.fromisoformat(date)
        non_trading = {v for v in tradfi_empty if _orch.is_non_trading_day(v, target_dt)}
        if non_trading:
            _nt_attempt_ts = _orch.datetime.now(_orch.UTC)
            for venue in sorted(non_trading):
                # Honest-coverage Phase 2.E.2: discriminate weekend vs holiday
                # so the manifest carries an EXPECTED_* row per (shard_key, day).
                # See header note on TradFi non-trading day pipeline_mode: this
                # is the instruments-service catalog asserting absence; tag
                # with BATCH_INSTRUMENTS_SERVICE.
                _reason = _orch.non_trading_day_reason(venue, target_dt) or "EXPECTED_WEEKEND"
                manifest.record_expected_empty(
                    row_key={"date": date, "venue": venue},
                    reason=_reason,
                    attempted_at=_nt_attempt_ts,
                    pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                )
                counts[venue] = 0
            _orch.logger.info(
                "TRADFI non-trading day manifest: date=%s venues=%s — wrote empty_confirmed entries",
                date,
                sorted(non_trading),
            )

    # Flush all manifest records in one batched write (one GCS round-trip
    # instead of N per venue). Generation-match lock handles concurrency.
    manifest.close()

    return _WriteOutcome(counts=counts, bucket=bucket, sink=sink, sampler=sampler)
