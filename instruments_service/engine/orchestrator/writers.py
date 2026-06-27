"""Per-venue instruments writers: venue parquet, futures contracts, market lifecycle, venues-from-teams.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
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
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_build_market_lifecycle_df",
    "_canonical_manifest_venue_chain",
    "_write_futures_contracts",
    "_write_market_lifecycle",
    "_write_venue",
    "_write_venues_from_teams",
]


def _canonical_manifest_venue_chain(venue_str: str) -> tuple[str, str]:
    """Return the canonical ``(manifest_venue, manifest_chain)`` for a non-sports venue.

    The IS instruments manifest atom for cefi/tradfi is ``(date, venue)``; for DeFi
    it is ``(date, venue=PROTOCOL, chain)``. A DeFi orchestrator venue string is the
    glued ``PROTOCOL-CHAIN`` form (e.g. ``AAVE_V3-ETHEREUM``), which the manifest
    splits into ``venue=AAVE_V3`` + ``chain=ETHEREUM`` (canonical v5 shard-key
    matrix — the DeFi axis is ``chain``, not packed into ``venue``).

    Returns ``(venue_str, "")`` for cefi/tradfi (no chain). The captured-row writer
    (:func:`_write_venue`) and the EU seeder (``process_eu_seed``) MUST share this
    split so an ``expected_unattempted`` seed and a later ``record_captured`` land on
    the IDENTICAL ``row_key`` and the capture cleanly supersedes the seed.
    """
    if "-" not in venue_str:
        return venue_str, ""
    # On-chain CeFi perp CLOBs (LIGHTER-ZKSYNC / PACIFICA-SOLANA / EXTENDED-STARKNET)
    # are GLUED ``VENUE-CHAIN`` strings whose suffix IS a KNOWN_CHAIN, but they are
    # CeFi venues (in ``_CEFI_VENUES``, like HYPERLIQUID/ASTER) — NOT defi pools. The
    # split below is the DeFi PROTOCOL-CHAIN→venue+chain rule; applying it to these
    # mis-stamped the manifest ``asset_group=defi`` + ``chain=<L2>`` (the G1.3 320-row
    # contamination). Keep them as the full cefi venue with ``chain=""`` so the
    # manifest row matches the by_date PATH (``venue=LIGHTER-ZKSYNC``) and the cefi
    # asset_group resolution at the call site (_cat = "cefi" when chain is "").
    if venue_str in _orch._CEFI_VENUES:
        return venue_str, ""
    from unified_api_contracts.registry.capability_declarations._defi import (  # noqa: imports-inside-functions
        KNOWN_CHAINS,
        parse_defi_venue,
    )

    try:
        _protocol, _chain = parse_defi_venue(venue_str)
    except ValueError:
        return venue_str, ""
    if _chain in KNOWN_CHAINS:
        return _protocol.upper(), _chain
    return venue_str, ""


def _derive_instrument_type(df: _orch.pd.DataFrame) -> str:
    """Return the single ``instrument_type`` for a venue's instrument-definition df.

    The manifest row is at (venue, date) grain. The instrument-definition parquet
    (``InstrumentRecord.model_dump()``) carries an ``instrument_type`` column. When
    every instrument in this venue x date shard is the SAME type (the common case —
    a venue df is typically one type, e.g. all PERPETUAL or all SPOT_PAIR), stamp
    that real type so the v9 manifest carries per-instrument_type counts (Audit §K).

    Returns "" (honest blank — never fabricated) when the column is absent, empty,
    or holds MIXED types (a single-type tag would misrepresent the shard).
    """
    if "instrument_type" not in df.columns or df.empty:
        return ""
    # Coerce to plain str (NaN/None → ""), drop blanks, then require a SINGLE
    # distinct non-blank type. Uses pandas string-dtype ops (no .tolist()/Any).
    col: _orch.pd.Series = df["instrument_type"].astype("string").fillna("").astype(str)
    nonblank: _orch.pd.Series = col[col.str.len() > 0]
    distinct: list[str] = [str(v) for v in nonblank.drop_duplicates()]
    if len(distinct) != 1:
        return ""
    return distinct[0]


def _write_venue(
    venue_str: str,
    df: _orch.pd.DataFrame,
    date: str,
    bucket: str,
    sink: _orch.DataSink,
    counts: dict[str, int],
    sampler: _orch.SamplingService,
    manifest: _orch.ManifestWriter | None = None,
) -> None:
    """Write one venue's DataFrame to storage, catalogue, and CSV sample.

    Retries transient GCS/network errors up to 3 times with exponential backoff
    (1s, 2s) to avoid wasting the expensive fetch work that produced this data.

    If ``manifest`` is provided, adds the catalogue record to the shared writer
    (caller flushes once after all venues). Otherwise uses the per-venue
    ``_write_catalogue_record`` path.
    """
    import time as _time

    # Canonicalize the combined DeFi venue partition BEFORE any write so the parquet
    # path (venue=...) matches the canonical manifest venue. Previously the parquet was
    # written under the raw glued caller form (e.g. AAVEV3-ARBITRUM) while the manifest
    # recorded the canonical form (AAVE_V3-ARBITRUM), so deployment-ui pool-breakdown
    # (which resolves the canonical manifest venue to a GCS path) could not find the
    # parquet → "No pool data". ``canonicalize_defi_venue_combined`` inserts the missing
    # underscore using the authoritative PROTOCOL_CAPABILITIES venue_prefix set, mapping
    # AAVEV3-ARBITRUM → AAVE_V3-ARBITRUM, UNISWAPV3-ETHEREUM → UNISWAP_V3-ETHEREUM, etc.
    # It is a no-op for already-canonical venues, venues whose canonical prefix is itself
    # glued (VELODROMEV2/TRADER_JOEV2), unknown protocols, and non-DeFi venues (no known
    # chain suffix). Sports-reference venues (API_FOOTBALL etc.) are guarded explicitly so
    # their data_type-bearing names are never touched. SSOT:
    # plans/active/issues/defi_coverage_capability_alignment_2026_05_22.md Bug 5.1.
    _sports_ref_prefixes = ("API_FOOTBALL", "TRANSFERMARKT", "FOOTYSTATS", "SFI", "UNDERSTAT", "WEATHER")
    if not venue_str.startswith(_sports_ref_prefixes):
        from unified_api_contracts.registry.capability_declarations._defi import (
            canonicalize_defi_venue_combined,
        )

        _canonical_venue = canonicalize_defi_venue_combined(venue_str)
        if _canonical_venue != venue_str:
            venue_str = _canonical_venue

    max_attempts = 3
    # Stamp available_at before any write so the parquet carries the column.
    _stamped_df = _orch.stamp_available_at_explicit(df, when=_orch.datetime.now(_orch.UTC))
    for attempt in range(max_attempts):
        try:
            _orch._gated_sink_write(
                sink,
                data=_stamped_df,
                partition={"day": date, "venue": venue_str},
                filename="instruments.parquet",
                venue=venue_str,
                entity="instruments",
            )
            # Add to batched manifest writer (flushed by caller) or legacy per-venue write
            # v4: Sports reference entities write data_type (not venue).
            #     API_FOOTBALL → data_type=FIXTURES, venue=""
            #     API_FOOTBALL_INJURIES → data_type=INJURIES, venue=""
            #     Other asset groups keep venue as-is.
            _sports_prefixes = ("API_FOOTBALL", "TRANSFERMARKT", "FOOTYSTATS", "SFI", "UNDERSTAT", "WEATHER")
            is_sports_ref = venue_str.startswith(_sports_prefixes)
            if is_sports_ref:
                # Extract data_type: API_FOOTBALL_INJURIES → INJURIES, API_FOOTBALL → FIXTURES
                if venue_str == "API_FOOTBALL":
                    manifest_data_type = "FIXTURES"
                elif "_" in venue_str:
                    # Strip the provider prefix: API_FOOTBALL_INJURIES → INJURIES
                    for pfx in _sports_prefixes:
                        if venue_str.startswith(pfx + "_"):
                            manifest_data_type = venue_str[len(pfx) + 1 :]
                            break
                    else:
                        manifest_data_type = venue_str
                else:
                    manifest_data_type = venue_str
                manifest_venue = ""
            else:
                manifest_venue = venue_str
                manifest_data_type = ""
            # DeFi: split AAVE_V3-ETHEREUM → venue=AAVE_V3, chain=ETHEREUM per the
            # canonical v5 shard-key matrix (DeFi axis is `chain`, not packed
            # into venue). The path-based legacy writer at the bottom of this
            # module already does this; the batched manifest writer used here
            # was missing the split, so DeFi rows from the orchestrator landed
            # as `venue=AAVE_V3-ETHEREUM, chain=''` and were filtered out by the
            # coverage-summary's legacy-row drop, hiding recent DeFi captures.
            manifest_chain = ""
            if not is_sports_ref:
                # Shared canonical split (DeFi PROTOCOL-CHAIN → venue=PROTOCOL +
                # chain=CHAIN) — the SAME helper the EU seeder uses, so a seed and a
                # later capture key-match exactly. No-op for cefi/tradfi.
                manifest_venue, manifest_chain = _canonical_manifest_venue_chain(venue_str)
            if manifest is not None:
                _stamped_venue_df = _stamped_df
                if is_sports_ref:
                    try:
                        _venue_pm = _orch._pipeline_mode_for_sports_data_type(manifest_data_type)
                    except KeyError:
                        _venue_pm = _orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE
                    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key={"date": date, "data_type": manifest_data_type},
                        df=_stamped_venue_df,
                        asset_group="sports",
                        instrument_type="",
                        data_type=manifest_data_type,
                        pipeline_mode=_venue_pm,
                        service_emission_state=None,
                    )
                else:
                    _cat = "defi" if manifest_chain else ("tradfi" if venue_str in _orch._TRADFI_VENUES else "cefi")
                    _rk: dict[str, str] = {"date": date, "venue": manifest_venue}
                    if manifest_chain:
                        _rk["chain"] = manifest_chain
                    # NOTE: instrument-definition rows are PRODUCER-emitted
                    # (pipeline_mode=BATCH_INSTRUMENTS_SERVICE). Per the C-#6
                    # source⇔pipeline_mode contract (2026-06-07) a BATCH row's
                    # explicit source must equal source_string_for(pipeline_mode),
                    # so we do NOT stamp the vendor (massive/databento) here — the
                    # source auto-resolves (blank/instruments_service). Which vendor
                    # served the snapshot is the adapter's routing concern, not a
                    # per-row manifest tag for producer rows.
                    # v9 instrument_type column (Audit §K): stamp the REAL type when
                    # the venue x date shard is single-type, "" when mixed/absent. This
                    # enables per-instrument_type counts off the manifest.
                    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
                        row_key=_rk,
                        df=_stamped_venue_df,
                        asset_group=_cat,
                        instrument_type=_derive_instrument_type(_stamped_venue_df),
                        data_type="",
                        venue=manifest_venue,
                        chain=manifest_chain,
                        pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
                        service_emission_state=None,
                    )
            else:
                path = f"instrument_availability/by_date/day={date}/venue={venue_str}/instruments.parquet"
                _orch._write_catalogue_record(bucket, path, date, len(df))
            # CSV sample in dev mode — generate_csv_sample is the SamplingService API
            if sampler.enable_sampling:
                sampler.generate_csv_sample(df, filename_prefix=f"instruments_{venue_str}_{date}")
            counts[venue_str] = len(df)
            return  # success
        except (OSError, ConnectionError, TimeoutError) as exc:
            if attempt < max_attempts - 1:
                delay = 2**attempt  # 1s, 2s
                _orch.logger.warning(
                    "Write retry %d/%d for venue=%s date=%s (next in %ds): %s",
                    attempt + 1,
                    max_attempts,
                    venue_str,
                    date,
                    delay,
                    exc,
                )
                _time.sleep(delay)
            else:
                _orch.logger.error(
                    "Write FAILED after %d attempts for venue=%s date=%s: %s",
                    max_attempts,
                    venue_str,
                    date,
                    exc,
                )
                _orch.log_event(
                    "WRITE_FAILED",
                    details={"venue": venue_str, "date": date, "error": str(exc), "attempts": max_attempts},
                )
        except ValueError as exc:
            # Serialization/validation errors — not transient, don't retry
            _orch.logger.error("Write failed for venue=%s date=%s: %s", venue_str, date, exc)
            _orch.log_event("WRITE_FAILED", details={"venue": venue_str, "date": date, "error": str(exc)})
            return


def _write_futures_contracts(
    venue_str: str,
    instrument_records: list[_orch.InstrumentRecord],
    date: str,
    bucket: str,
    sink: _orch.DataSink,
) -> None:
    """Write CanonicalFuturesContract records to ``futures_contracts.parquet``.

    Called after _write_venue() for TradFi futures venues (CME, ICE). Builds
    CanonicalFuturesContract from InstrumentRecord using ``build_futures_contracts()``,
    which derives all 5 hard-required lifecycle dates (expiry_date, last_trading_date,
    first_notice_date, delivery_date, settlement_date) from the single expiry timestamp
    that Databento provides.

    Output path mirrors the instruments.parquet partition:
        instrument_availability/by_date/day={date}/venue={venue}/futures_contracts.parquet

    Shard-level isolation: errors are logged but never propagate — a failure here
    does not abort the instruments.parquet write that already succeeded.

    Plan: tradfi_canonical_futures_contract_hard_required_fields_2026_05_13.md Phase 4.2.
    """
    try:
        today = _orch.date_type.fromisoformat(date)
        contracts = _orch.build_futures_contracts(instrument_records, today=today)
        if not contracts:
            _orch.logger.debug(
                "_write_futures_contracts: no futures contracts for venue=%s date=%s — skipping",
                venue_str,
                date,
            )
            return
        rows = [c.model_dump(mode="json") for c in contracts]
        df = _orch.stamp_available_at_explicit(_orch.pd.DataFrame(rows), when=_orch.datetime.now(_orch.UTC))
        _orch._gated_sink_write(
            sink,
            data=df,
            partition={"day": date, "venue": venue_str},
            filename="futures_contracts.parquet",
            venue=venue_str,
            entity="futures_contracts",
        )
        _orch.logger.info(
            "_write_futures_contracts: wrote %d contracts for venue=%s date=%s",
            len(contracts),
            venue_str,
            date,
        )
    except Exception as exc:  # broad-except-ok — shard-level isolation per CLAUDE.md
        _orch.logger.error(
            "_write_futures_contracts: failed for venue=%s date=%s: %s",
            venue_str,
            date,
            exc,
        )
        _orch.log_event(
            "WRITE_FAILED",
            details={
                "venue": venue_str,
                "date": date,
                "entity": "futures_contracts",
                "error": str(exc),
            },
        )


def _build_market_lifecycle_df(
    group_df: _orch.pd.DataFrame,
    canonical_group_str: str,
    now: _orch.datetime,
) -> _orch.pd.DataFrame:
    """Build a MARKET_LIFECYCLE DataFrame from prediction InstrumentRecord rows.

    Extracts lifecycle fields present in InstrumentRecord:
      market_created_at  ← available_from_datetime (set by adapter.classify_lifecycle)
      settlement_time    ← available_to_datetime
      resolution_time    ← settlement_time - CANONICAL_GROUP_METADATA[group].settlement_lag
      status             ← "settled" if settlement_time ≤ now else "active"

    Returns an empty DataFrame if the required columns are absent or all values are null.
    Rows missing either available_from_datetime or available_to_datetime are dropped.
    """
    required = {"instrument_key", "available_from_datetime", "available_to_datetime"}
    if not required.issubset(group_df.columns):
        return _orch.pd.DataFrame()

    rows = group_df[group_df["available_from_datetime"].notna() & group_df["available_to_datetime"].notna()].copy()
    if rows.empty:
        return _orch.pd.DataFrame()

    try:
        group_enum = _orch.CanonicalQuestionGroup(canonical_group_str)
    except ValueError:
        group_enum = _orch.CanonicalQuestionGroup.OTHER
    settlement_lag = _orch.CANONICAL_GROUP_METADATA[group_enum].settlement_lag

    now_ts = (
        _orch.pd.Timestamp(now).tz_convert("UTC")
        if _orch.pd.Timestamp(now).tzinfo
        else _orch.pd.Timestamp(now, tz="UTC")
    )
    rows["settlement_time"] = _orch.pd.to_datetime(rows["available_to_datetime"], utc=True)
    rows["resolution_time"] = rows["settlement_time"] - settlement_lag
    rows["market_created_at"] = _orch.pd.to_datetime(rows["available_from_datetime"], utc=True)
    settled_mask: _orch.pd.Series[bool] = rows["settlement_time"] <= now_ts
    rows["status"] = _orch.pd.Series(
        ["settled" if v else "active" for v in settled_mask],
        index=rows.index,
        dtype=str,
    )
    rows["canonical_question_group"] = canonical_group_str
    rows["market_id"] = rows["instrument_key"].astype(str)

    return rows[
        ["market_id", "canonical_question_group", "market_created_at", "resolution_time", "settlement_time", "status"]
    ].copy()


def _write_market_lifecycle(
    sink: _orch.DataSink,
    group_df: _orch.pd.DataFrame,
    canonical_group_str: str,
    date: str,
    manifest_venue: str,
    manifest: _orch.ManifestWriter,
    pipeline_mode: _orch.PipelineMode,
) -> None:
    """Write MARKET_LIFECYCLE parquet alongside instruments.parquet for a prediction group.

    Output: market_lifecycle/by_canonical_group/group={g}/day={d}/market_lifecycle.parquet
    Shard-level isolation: errors are logged but do not abort the instruments write.
    Plan: predictions_master.md Phase 3 L618.
    """
    try:
        now = _orch.datetime.now(_orch.UTC)
        out_df = _orch._build_market_lifecycle_df(group_df, canonical_group_str, now)
        if out_df.empty:
            _orch.logger.debug(
                "_write_market_lifecycle: no lifecycle rows for venue=%s group=%s date=%s",
                manifest_venue,
                canonical_group_str,
                date,
            )
            return
        out_df = _orch.stamp_available_at_explicit(out_df, when=now)
        _orch._gated_sink_write(
            sink,
            data=out_df,
            partition={"group": canonical_group_str, "day": date},
            filename="market_lifecycle.parquet",
            venue=manifest_venue,
            entity="market_lifecycle",
        )
        manifest.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
            row_key={
                "date": date,
                "data_type": "prediction_market_lifecycle",
                "venue": manifest_venue,
                "underlying": canonical_group_str,
            },
            total_rows=len(out_df),
            # SP-10: prediction_market_lifecycle is the lifecycle table itself for a single
            # (group, day) cell — it is the SOURCE of the expected-market-id set, not a bundle
            # validated against one, so there is no upstream cluster contract to assert here.
            expected_root_clusters={},
            observed_clusters={"": len(out_df)},
            available_at_envelope=_orch.pd.Timestamp(now),
            pipeline_mode=pipeline_mode,
            service_emission_state=None,
        )
        _orch.logger.info(
            "_write_market_lifecycle: %d rows venue=%s group=%s date=%s",
            len(out_df),
            manifest_venue,
            canonical_group_str,
            date,
        )
    except Exception as exc:  # broad-except-ok — shard-level isolation per CLAUDE.md
        _orch.logger.error(
            "_write_market_lifecycle: failed venue=%s group=%s date=%s: %s",
            manifest_venue,
            canonical_group_str,
            date,
            exc,
        )
        _orch.log_event(
            "WRITE_FAILED",
            details={
                "venue": manifest_venue,
                "canonical_question_group": canonical_group_str,
                "date": date,
                "operation": "market_lifecycle_write",
                "error": str(exc),
            },
        )


def _write_venues_from_teams(teams_df: _orch.pd.DataFrame, bucket: str) -> None:
    """Extract venue metadata from teams and write a global venues.parquet.

    The features-sports-service reads venues from a flat path:
        sports_reference/venues/venues.parquet
    (not date-partitioned -- venues are slow-moving reference data).

    Venue coordinates are enriched by the API Football adapter via the UAC
    static venue coordinates registry. This function extracts the venue dict
    from each team row and writes a deduplicated venues table.
    """
    if "venue" not in teams_df.columns:
        _orch.logger.warning("No 'venue' column in teams_df — cannot extract venues")
        return

    venue_rows: list[dict[str, object]] = []
    for _, row in teams_df.iterrows():
        venue_data = row.get("venue")
        if not isinstance(venue_data, dict):
            continue
        venue_id = venue_data.get("venue_id")
        if not venue_id:
            continue
        venue_rows.append(
            {
                "venue_id": str(venue_id),
                "name": venue_data.get("name", ""),
                "city": venue_data.get("city"),
                "country": venue_data.get("country"),
                "capacity": venue_data.get("capacity"),
                "surface": venue_data.get("surface"),
                "latitude": venue_data.get("latitude"),
                "longitude": venue_data.get("longitude"),
            }
        )

    if not venue_rows:
        _orch.logger.warning("No venue data extracted from teams — skipping venues.parquet")
        return

    venues_df = _orch.pd.DataFrame(venue_rows).drop_duplicates(subset=["venue_id"])

    venues_sink = _orch.get_data_sink(bucket=bucket, prefix="sports_reference/venues")
    venues_sink.write(
        data=venues_df,
        partition={},
        format="parquet",
        filename="venues.parquet",
    )
    coords_count = int(venues_df["latitude"].notna().sum())
    _orch.logger.info(
        "Venues: %d unique venues written (%d with coordinates)",
        len(venues_df),
        coords_count,
    )
