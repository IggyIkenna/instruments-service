"""Instruments engine orchestrator — the entire processing logic of the service.

IMPORT CONTRACT
---------------
This module imports from:
  1. unified_trading_library (UTL) — all infrastructure, framework, validation, storage
  2. unified_api_contracts (T0) — domain types (venue-agnostic enums)

No direct imports from UEI, UCI, UMI, UDC, UCC. If something is needed from
those libraries, it must come through UTL's re-exported surface.

PROCESS FLOW
------------
For each date:
  1. Skip venues not yet launched on that date (startup dates in _VENUE_LAUNCH_DATES)
  2. Fetch InstrumentRecord[] from URDI via urdi_reference_provider
  3. Filter to instruments active on the requested date (available_since ≤ date ≤ available_to)
  4. Fail shard if zero records after filtering
  5. Validate with DomainValidationService("instruments") (UTL)
  6. Write per-venue parquet + catalogue record (UTL get_data_sink / ManifestWriter)
  7. Drop CSV sample in dev mode (UTL create_sampling_service)
"""

from __future__ import annotations

import io
import logging
import re
from datetime import UTC, datetime
from datetime import date as date_type

import pandas as pd
from unified_api_contracts import VenueMapping
from unified_api_contracts.sports import (
    BUNDESLIGA_TEAM_ALIASES,
    CANONICAL_TO_ODDS_API_BUNDESLIGA,
    CANONICAL_TO_ODDS_API_EPL,
    CANONICAL_TO_UNDERSTAT_EPL,
    EPL_TEAM_ALIASES,
    get_prediction_leagues,
)
from unified_sports_reference_interface import create_sports_reference_adapter
from unified_trading_library import (
    DataSink,
    DomainValidationService,
    ManifestWriter,
    SamplingService,
    create_sampling_service,
    get_bucket_name,
    get_data_sink,
    get_storage_client,
    log_event,
)
from unified_trading_library import unified_config as _uc

from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets

logger = logging.getLogger(__name__)

# Venue launch dates SSOT: UAC VenueMapping.venue_start_dates (canonical PROTOCOL-CHAIN format).
# No local copy — read from VenueMapping at module load.
_VENUE_MAPPING = VenueMapping()
_VENUE_LAUNCH_DATES: dict[str, str] = _VENUE_MAPPING.venue_start_dates

_DEFI_VENUES: list[str] = [
    "UNISWAPV2-ETHEREUM",
    "UNISWAPV3-ETHEREUM",
    "UNISWAPV4-ETHEREUM",
    "CURVE-ETHEREUM",
    "BALANCER-ETHEREUM",
    "AAVEV3-ETHEREUM",
    "MORPHO-ETHEREUM",
    "EULER-ETHEREUM",
    "FLUID-ETHEREUM",
    "LIDO-ETHEREUM",
    "ETHERFI-ETHEREUM",
    "ETHENA-ETHEREUM",
]

_CEFI_VENUES: list[str] = [
    "BINANCE-SPOT",
    "BINANCE-FUTURES",
    "BYBIT",
    "OKX",
    "OKX-SPOT",
    "OKX-FUTURES",
    "DERIBIT",
    "COINBASE-SPOT",
    "HYPERLIQUID",
    "UPBIT",
    "ASTER",
]

_TRADFI_VENUES: list[str] = [
    "CME",
    "NASDAQ",
    "NYSE",
    "CBOE",
    "ICE",
    "FX",
]

# ---------------------------------------------------------------------------
# DEFI instrument relevance filter
# ---------------------------------------------------------------------------
_DEX_VENUE_KEYWORDS = frozenset({"UNISWAP", "BALANCER", "CURVE"})


def filter_defi_instruments_by_relevance(records: list) -> list:
    """Filter DEFI instruments to major liquid assets only.

    The asset whitelist comes from config_reloaders.get_defi_major_assets()
    (InstrumentsDomainConfigState), which defaults to the hardcoded ETH/BTC/
    USDT/USDC and derivatives set and can be overridden via cloud ConfigStore.

    Rules:
    - DEX pools (Uniswap, Balancer, Curve): both base AND quote must be in
      the major assets set. Eliminates long-tail pairs like PEPE/WETH or
      FAITH/MILAREPA while keeping WETH/USDC, WBTC/WETH, stETH/WETH, etc.
    - Lending protocols (Aave, Morpho, Fluid, Euler, LST services): base
      asset must be in the major assets set. Keeps aWETH, aWBTC, aUSDC etc.
    """
    major = get_defi_major_assets()  # reads from config_reloaders (hot-reloadable)
    result = []
    for r in records:
        base = (getattr(r, "base_asset", None) or "").upper().strip()
        quote = (getattr(r, "quote_asset", None) or "").upper().strip()
        venue = (getattr(r, "venue", None) or "").upper()
        is_dex = any(kw in venue for kw in _DEX_VENUE_KEYWORDS)
        if is_dex:
            if base in major and quote in major:
                result.append(r)
        else:
            if base in major:
                result.append(r)
    return result


def filter_instruments_by_date(
    records: list,
    date_dt: datetime,
    defi_venues: frozenset[str] | None = None,
) -> list:
    """Return only instruments active on the given UTC datetime.

    An instrument is active on `date_dt` when:
    - available_since is None OR available_since <= date_dt
    - available_to   is None OR available_to   >= date_dt

    This is required because URDI adapters return the full historical universe.
    function reduces them to only the instruments tradeable on the requested day.

    Args:
        records: InstrumentRecord list from URDI.
        date_dt: UTC datetime representing the requested processing date.
        defi_venues: Optional set of DeFi venue names (uppercase). When provided,
            a WARNING is emitted for any DeFi instrument where available_since=None
            because on-chain creation timestamps are expected for all DeFi instruments
            and absence indicates the URDI adapter did not provide them (data quality
            is degraded — the instrument will still be included but with unknown
            listing date).
    """
    result = []
    for r in records:
        since: datetime | None = getattr(r, "available_since", None)
        until: datetime | None = getattr(r, "available_to", None)
        since_ok = since is None or since <= date_dt
        until_ok = until is None or until >= date_dt
        if since_ok and until_ok:
            if defi_venues is not None and since is None:
                venue = (getattr(r, "venue", None) or "").upper()
                if venue in defi_venues:
                    key = getattr(r, "instrument_key", repr(r))
                    logger.warning(
                        "DeFi instrument %s has available_since=None — "
                        "URDI adapter did not provide on-chain creation timestamp; "
                        "date accuracy degraded",
                        key,
                    )
            result.append(r)
    return result


def get_venues_for_categories(categories: list[str]) -> list[str]:
    """Return UAC canonical venue names for the requested market categories."""
    venues: list[str] = []
    for cat in categories:
        cat_upper = cat.upper()
        if cat_upper in ("CEFI", "ALL"):
            venues.extend(_CEFI_VENUES)
        if cat_upper in ("TRADFI", "ALL"):
            venues.extend(_TRADFI_VENUES)
        if cat_upper in ("DEFI", "ALL"):
            venues.extend(_DEFI_VENUES)
        if cat_upper in ("SPORTS", "ALL"):
            # instruments-service owns fixtures + slow-moving reference data
            # (teams, leagues, players, referees, venues) via API-Football.
            # Betting market instruments (the actual tradeable positions) come from
            # market-tick-data-service via Odds API — documented exception because
            # markets are only discoverable alongside odds data.
            venues.extend(["API_FOOTBALL"])
        if cat_upper in ("PREDICTION", "ALL"):
            # POLYMARKET + KALSHI: prediction market instruments (crypto up/down, soccer, macro).
            # No auth required — Gamma API (Polymarket) and public API (Kalshi) are keyless.
            venues.extend(["POLYMARKET", "KALSHI"])
    return list(dict.fromkeys(venues))


def is_venue_available(venue: str, date: str) -> bool:
    """Return True if the venue was launched on or before this date."""
    launch_date = _VENUE_LAUNCH_DATES.get(venue)
    if launch_date is None:
        return True  # Unknown venue — assume always available
    return date >= launch_date


async def process_instruments(
    date: str | datetime,
    categories: list[str],
    redo_all: bool = False,
    api_keys: dict[str, str] | None = None,
    venue_override: list[str] | None = None,
) -> dict[str, int]:
    """Process instruments for a single date and set of market categories.

    Returns:
        Dict mapping venue → record count written.

    Raises:
        RuntimeError: If URDI returns zero total records (fail the shard).
    """
    _ = get_config()  # ensure config is initialized

    # Normalise date: BatchIO passes datetime objects from get_date_range(),
    # but all downstream code (URDI, date filter, partition keys) needs str YYYY-MM-DD.
    if isinstance(date, datetime):
        date = date.strftime("%Y-%m-%d")

    # venue_override bypasses category lookup when --venues filter is active (sharding)
    venues = venue_override if venue_override is not None else get_venues_for_categories(categories)

    # 1. Skip venues not yet launched
    active_venues = [v for v in venues if is_venue_available(v, date)]
    if not active_venues:
        logger.info("No active venues for date=%s categories=%s", date, categories)
        return {}

    log_event(
        "PROCESSING_STARTED",
        details={"date": date, "categories": categories, "venue_count": len(active_venues)},
    )

    # 2. Fetch from URDI — sole external API path
    # api_keys injected from preflight() → validate_api_keys_for_venues() → Secret Manager
    # date passed so date-aware adapters (e.g. API-Football) can filter server-side
    records = await fetch_instruments_for_all_venues(active_venues, api_keys=api_keys, date=date)

    # 3. Filter to instruments active on the requested date.
    # URDI adapters return the full historical instrument universe; this reduces
    # it to only instruments tradeable on the requested day.
    # Pass the DeFi venue set so the filter can warn on missing available_since.
    is_defi_run = any(c.upper() in ("DEFI", "ALL") for c in categories)
    defi_venue_set: frozenset[str] | None = frozenset(_DEFI_VENUES) if is_defi_run else None
    date_dt = datetime.fromisoformat(date).replace(tzinfo=UTC)
    records = filter_instruments_by_date(records, date_dt, defi_venues=defi_venue_set)
    logger.info(
        "Date filter %s: %d instruments active (from URDI fetch)",
        date,
        len(records),
    )

    # 3a. DeFi available_since coverage summary.
    # Counts how many DeFi instruments in the date-filtered set have a populated
    # available_since vs None. Low coverage indicates URDI adapters are not
    # returning on-chain creation timestamps and the date filter is permissive
    # (treating None as "always available").
    if is_defi_run and records:
        defi_records = [r for r in records if (getattr(r, "venue", "") or "").upper() in _DEFI_VENUES]
        if defi_records:
            populated = sum(1 for r in defi_records if getattr(r, "available_since", None) is not None)
            total_defi = len(defi_records)
            pct = int(populated * 100 / total_defi)
            logger.info(
                "Date accuracy: %d/%d DeFi instruments have available_since populated (%d%% coverage)",
                populated,
                total_defi,
                pct,
            )

    # 3b. DEFI relevance filter: keep only instruments involving major liquid assets.
    # Whitelist is from config_reloaders.get_defi_major_assets() — defaults to
    # ETH/BTC/USDT/USDC and known derivatives; can be overridden via ConfigStore.
    if any(c.upper() in ("DEFI", "ALL") for c in categories):
        before = len(records)
        records = filter_defi_instruments_by_relevance(records)
        logger.info(
            "DEFI relevance filter: %d → %d instruments (removed %d long-tail)",
            before,
            len(records),
            before - len(records),
        )

    # 4. Fail shard on zero records — never silently succeed with empty output
    if not records:
        msg = (
            f"URDI returned zero records for date={date} categories={categories}. "
            f"Venues attempted: {active_venues}. "
            "Check URDI adapter coverage and network connectivity."
        )
        logger.error(msg)
        log_event("PROCESSING_FAILED", details={"date": date, "reason": msg})
        raise RuntimeError(msg)

    df = pd.DataFrame([r.model_dump() for r in records])

    # 5. Domain validation — logs anomalies, doesn't raise for instruments domain
    DomainValidationService("instruments").validate_for_domain(df)

    # 6. Write per-venue parquet + catalogue + CSV sample
    # Pass config explicitly — _uc is read at call time so sampling honours
    # ENABLE_CSV_SAMPLING even when set after the singleton initialised.
    counts: dict[str, int] = {}
    sampler = create_sampling_service(
        {
            "enable_sampling": _uc.enable_csv_sampling,
            "sample_size": _uc.csv_sample_size,
            "sample_dir": _uc.csv_sample_dir,
        }
    )
    # Use the first (primary) category to route to the correct category-specific bucket.
    # UCI naming: instruments-store-{category.lower()}-{project}
    # e.g. DEFI → instruments-store-defi-{gcp_project_id}
    primary_category = categories[0] if categories else None
    bucket = _get_instruments_bucket(primary_category)
    # prefix ensures writes land at instrument_availability/by_date/{day=X}/{venue=Y}/
    sink = get_data_sink(bucket=bucket, prefix="instrument_availability/by_date")

    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            _write_venue(str(venue_name), venue_df, date, bucket, sink, counts, sampler)
    else:
        _write_venue("all", df, date, bucket, sink, counts, sampler)

    # 7. SPORTS enrichment: fetch and write reference data (teams, leagues, etc.)
    # alongside fixtures. These are slow-moving entities that don't change per-date
    # but are re-fetched to capture transfers, promotions, new seasons.
    is_sports = any(c.upper() in ("SPORTS", "ALL") for c in categories)
    if is_sports and api_keys:
        api_football_key = api_keys.get("api_football")
        if not api_football_key:
            logger.warning("api_football key missing from api_keys — skipping sports reference data")
        else:
            sports_ref_counts = await _fetch_sports_reference_data(
                date=date,
                api_key=api_football_key,
                bucket=bucket,
            )
            for k, v in sports_ref_counts.items():
                counts[k] = counts.get(k, 0) + v

    total = sum(counts.values())
    log_event(
        "PROCESSING_COMPLETED",
        details={"date": date, "total_records": total, "venues": len(counts)},
    )
    logger.info("instruments: date=%s wrote %d records across %d venues", date, total, len(counts))
    return counts


def _write_venue(
    venue_str: str,
    df: pd.DataFrame,
    date: str,
    bucket: str,
    sink: DataSink,
    counts: dict[str, int],
    sampler: SamplingService,
) -> None:
    """Write one venue's DataFrame to storage, catalogue, and CSV sample."""
    try:
        sink.write(
            data=df,
            partition={"day": date, "venue": venue_str},
            format="parquet",
            filename="instruments.parquet",
        )
        path = f"instrument_availability/by_date/day={date}/venue={venue_str}/instruments.parquet"
        _write_catalogue_record(bucket, path, date, len(df))
        # CSV sample in dev mode — generate_csv_sample is the SamplingService API
        if sampler.enable_sampling:
            sampler.generate_csv_sample(df, filename_prefix=f"instruments_{venue_str}_{date}")
        counts[venue_str] = len(df)
    except (OSError, ConnectionError, TimeoutError, ValueError) as exc:
        logger.error("Write failed for venue=%s date=%s: %s", venue_str, date, exc)
        log_event("WRITE_FAILED", details={"venue": venue_str, "date": date, "error": str(exc)})
    # Programming errors propagate — fail the shard


async def _fetch_sports_reference_data(
    date: str,
    api_key: str,
    bucket: str,
) -> dict[str, int]:
    """Fetch sports reference data (teams, leagues, standings, injuries, etc.).

    Calls USRI api_football adapter for enrichment data that describes the
    instruments (fixtures). Written to the same bucket as instruments under
    separate hive-partitioned prefixes:
        sports_reference/by_date/day={date}/entity={type}/{type}.parquet
        sports_reference/mappings/team_mapping.parquet
        sports_reference/mappings/fixture_mapping.parquet

    This data is slow-moving (leagues/teams change per season, not per day)
    but we re-fetch on each run to capture mid-season transfers, promotions,
    and new referee assignments.
    """
    adapter = create_sports_reference_adapter("api_football", api_key=api_key)
    sink = get_data_sink(bucket=bucket, prefix="sports_reference/by_date")
    counts: dict[str, int] = {}

    # Leagues — all known leagues
    try:
        leagues = await adapter.get_leagues()
        if leagues:
            df = pd.DataFrame([lg.model_dump() for lg in leagues])
            sink.write(
                data=df, partition={"day": date, "entity": "leagues"}, format="parquet", filename="leagues.parquet"
            )
            counts["leagues"] = len(df)
            logger.info("Sports reference: %d leagues written", len(df))
    except Exception as exc:
        logger.warning("Sports reference leagues fetch failed: %s", exc)

    # Teams — for each prediction league
    all_teams: list[dict[str, object]] = []
    prediction_league_ids: list[int] = []
    try:
        for league_def in get_prediction_leagues():
            if league_def.api_football_id is None:
                continue
            prediction_league_ids.append(league_def.api_football_id)
            try:
                teams = await adapter.get_teams(league_def.api_football_id)
                for t in teams:
                    all_teams.append(t.model_dump())
            except Exception as exc:
                logger.warning(
                    "Sports reference teams fetch failed for league %s: %s",
                    league_def.league_id,
                    exc,
                )
        if all_teams:
            df = pd.DataFrame(all_teams)
            sink.write(data=df, partition={"day": date, "entity": "teams"}, format="parquet", filename="teams.parquet")
            counts["teams"] = len(df)
            logger.info("Sports reference: %d teams written", len(df))
    except Exception as exc:
        logger.warning("Sports reference teams batch failed: %s", exc)

    # Standings — for each prediction league
    all_standings: list[dict[str, object]] = []
    for lid in prediction_league_ids:
        try:
            standings = await adapter.get_standings(lid)
            for row in standings:
                row["league_id"] = lid
                all_standings.append(row)
        except Exception as exc:
            logger.warning("Standings fetch failed for league %d: %s", lid, exc)
    if all_standings:
        df = pd.DataFrame(all_standings)
        sink.write(
            data=df, partition={"day": date, "entity": "standings"}, format="parquet", filename="standings.parquet"
        )
        counts["standings"] = len(df)
        logger.info("Sports reference: %d standing rows written", len(df))

    # Injuries — for the target date
    try:
        injuries = await adapter.get_injuries(date)
        if injuries:
            df = pd.DataFrame(injuries)
            sink.write(
                data=df, partition={"day": date, "entity": "injuries"}, format="parquet", filename="injuries.parquet"
            )
            counts["injuries"] = len(df)
            logger.info("Sports reference: %d injuries written", len(df))
    except Exception as exc:
        logger.warning("Injuries fetch failed: %s", exc)

    # Cross-provider mapping tables
    _write_team_mapping(bucket)
    _write_fixture_mapping(bucket, date)

    return counts


def _write_team_mapping(bucket: str) -> None:
    """Build and write TeamMapping table to GCS.

    Combines UAC team_mappings.py (API-Football names) and team_names.py
    (Odds API / Understat names) into a single lookup table keyed by
    canonical team_id. Used by FSS to resolve provider-specific IDs.

    Path: sports_reference/mappings/team_mapping.parquet
    """
    rows: list[dict[str, str]] = []

    # EPL teams
    epl_ids: set[str] = set(EPL_TEAM_ALIASES.keys()) | set(CANONICAL_TO_ODDS_API_EPL.keys())
    for canonical_id in sorted(epl_ids):
        aliases = EPL_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": CANONICAL_TO_ODDS_API_EPL.get(canonical_id, ""),
                "understat_name": CANONICAL_TO_UNDERSTAT_EPL.get(canonical_id, ""),
                "league": "EPL",
            }
        )

    # Bundesliga teams
    bun_ids: set[str] = set(BUNDESLIGA_TEAM_ALIASES.keys()) | set(CANONICAL_TO_ODDS_API_BUNDESLIGA.keys())
    for canonical_id in sorted(bun_ids):
        aliases = BUNDESLIGA_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": CANONICAL_TO_ODDS_API_BUNDESLIGA.get(canonical_id, ""),
                "understat_name": "",
                "league": "BUNDESLIGA",
            }
        )

    if not rows:
        return

    try:
        df = pd.DataFrame(rows)
        mapping_sink = get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=df, partition={}, format="parquet", filename="team_mapping.parquet")
        logger.info("Team mapping: %d entries written", len(df))
    except Exception as exc:
        logger.warning("Team mapping write failed: %s", exc)


def _write_fixture_mapping(bucket: str, date: str) -> None:
    """Build and write FixtureMapping table to GCS.

    Reads the fixture instruments already written for today, extracts the
    canonical_fixture_id and the API-Football numeric fixture_id, and
    writes a mapping table that FSS uses to resolve provider-specific IDs.

    Path: sports_reference/mappings/fixture_mapping.parquet
    """
    try:
        # Read today's fixtures from the instruments parquet we just wrote
        blob_path = f"instrument_availability/by_date/day={date}/venue=API_FOOTBALL/instruments.parquet"
        storage = get_storage_client()
        raw = storage.download_bytes(bucket, blob_path)
        if raw is None:
            logger.debug("Fixture mapping: no fixtures parquet found for %s", date)
            return
        df = pd.read_parquet(io.BytesIO(raw))
        if df.empty:
            logger.debug("Fixture mapping: no fixtures found for %s", date)
            return

        rows: list[dict[str, str]] = []
        for _, row in df.iterrows():
            instrument_key = str(row["instrument_key"]) if "instrument_key" in row.index else ""
            raw_symbol = str(row["raw_symbol"]) if "raw_symbol" in row.index else ""
            # Extract API-Football numeric ID from the symbol or key
            # The instrument_key is canonical: ENGLAND_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322
            # The raw_symbol is: Arsenal vs Chelsea
            rows.append(
                {
                    "canonical_fixture_id": instrument_key,
                    "raw_symbol": raw_symbol,
                    "date": date,
                    "venue": "API_FOOTBALL",
                }
            )

        if not rows:
            return

        mapping_df = pd.DataFrame(rows)
        mapping_sink = get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=mapping_df, partition={}, format="parquet", filename="fixture_mapping.parquet")
        logger.info("Fixture mapping: %d entries written for %s", len(mapping_df), date)
    except (FileNotFoundError, OSError) as exc:
        logger.debug("Fixture mapping: could not read fixtures for %s: %s", date, exc)
    except Exception as exc:
        logger.warning("Fixture mapping write failed: %s", exc)


def _get_instruments_bucket(category: str | None = None) -> str:
    """Resolve the instruments write bucket for the given category.

    Prod:  instruments-store-{category.lower()}-{project}
    Test:  instruments-store-{category.lower()}-{project}-test

    Test buckets follow the same naming as prod with -test appended after
    the project ID. IS_TEST_RUN=true writes to the test variant so prod
    data is never touched during local dev / E2E runs.
    """
    cfg = get_config()
    project = cfg.gcp_project_id or "test-project"

    try:
        prod_bucket = get_bucket_name("instruments", category)
    except (ImportError, AttributeError):
        cat_lower = category.lower() if category else None
        prefix = cfg.instruments_bucket_prefix
        prod_bucket = f"{prefix}-{cat_lower}-{project}" if cat_lower else f"{prefix}-{project}"

    return f"{prod_bucket}-test" if cfg.is_test_run else prod_bucket


def _write_catalogue_record(bucket: str, path: str, date: str, record_count: int) -> None:
    """Update the consolidated availability index in the instruments bucket.

    Merges this venue/date record into:
      gs://{bucket}/_index/availability_index.parquet

    Downstream services call read_availability_index(bucket) to check completeness
    without listing thousands of GCS blobs.
    """
    try:
        venue_match = re.search(r"venue=([^/]+)", path)
        venue_str = venue_match.group(1) if venue_match else ""
        date_match = re.search(r"day=(\d{4}-\d{2}-\d{2})", path)
        date_str = date_match.group(1) if date_match else date
        parsed = date_type.fromisoformat(date_str)
        writer = ManifestWriter(
            service_name="instruments-service",
            catalogue_bucket=bucket,
        )
        writer.add(processing_date=parsed, row_count=record_count, venue=venue_str)
        writer.write()
    except Exception as exc:
        logger.debug("ManifestWriter failed (non-blocking): %s", exc)
