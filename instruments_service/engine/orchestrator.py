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

import logging
import re
from datetime import UTC, datetime
from datetime import date as date_type

import pandas as pd
from unified_trading_library import (
    DataSink,
    DomainValidationService,
    ManifestWriter,
    SamplingService,
    create_sampling_service,
    get_bucket_name,
    get_data_sink,
    log_event,
)
from unified_trading_library import unified_config as _uc

from instruments_service.adapters.urdi_reference_provider import fetch_instruments_for_all_venues
from instruments_service.config import get_config
from instruments_service.config_reloaders import get_defi_major_assets

logger = logging.getLogger(__name__)

# Venue startup dates (URDI canonical names) — skip venues before their launch date.
# Dates from official protocol deployment records.
_VENUE_LAUNCH_DATES: dict[str, str] = {
    "UNISWAPV2-ETHEREUM": "2020-05-18",
    "UNISWAPV3-ETHEREUM": "2021-05-05",
    "UNISWAPV4-ETHEREUM": "2025-01-31",
    "CURVE-ETHEREUM": "2020-01-20",
    "BALANCER-ETHEREUM": "2020-03-31",
    "AAVEV3-ETHEREUM": "2023-01-27",
    "MORPHO-ETHEREUM": "2024-01-08",
    "EULER-ETHEREUM": "2023-12-18",
    "FLUID-ETHEREUM": "2024-03-01",
    "LIDO-ETHEREUM": "2020-12-18",
    "ETHERFI-ETHEREUM": "2023-11-01",
    "ETHENA-ETHEREUM": "2024-02-19",
    "HYPERLIQUID": "2023-01-01",
    "BINANCE-SPOT": "2017-07-14",
    "BINANCE-FUTURES": "2019-09-13",
    "BYBIT": "2018-11-01",
    "COINBASE-SPOT": "2014-01-01",
    "COINBASE": "2014-01-01",
    "DERIBIT": "2016-06-01",
}

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
    "GEMINI-SPOT",
    "PHEMEX-SPOT",
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
) -> list:
    """Return only instruments active on the given UTC datetime.

    An instrument is active on `date_dt` when:
    - available_since is None OR available_since <= date_dt
    - available_to   is None OR available_to   >= date_dt

    This is required because URDI adapters return the full historical universe.
    function reduces them to only the instruments tradeable on the requested day.
    """
    result = []
    for r in records:
        since: datetime | None = getattr(r, "available_since", None)
        until: datetime | None = getattr(r, "available_to", None)
        since_ok = since is None or since <= date_dt
        until_ok = until is None or until >= date_dt
        if since_ok and until_ok:
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
            # API_FOOTBALL is the source for sports reference data (fixtures, teams, leagues).
            # BETFAIR is for live odds / tick data — belongs in market-tick-data-service, not here.
            venues.extend(["API_FOOTBALL"])
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
    date_dt = datetime.fromisoformat(date).replace(tzinfo=UTC)
    records = filter_instruments_by_date(records, date_dt)
    logger.info(
        "Date filter %s: %d instruments active (from URDI fetch)",
        date,
        len(records),
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
    """Write to the data catalogue within the instruments bucket.

    Uses ManifestWriter with catalogue_bucket=bucket and catalogue_prefix="_catalogue"
    so the manifest lands at:
      {instruments-bucket}/_catalogue/instruments-service/day={date}/manifest.parquet

    This co-locates the catalogue with the data (per-bucket basis) rather than
    requiring a separate data-catalogue-* bucket.
    """
    try:
        date_match = re.search(r"day=(\d{4}-\d{2}-\d{2})", path)
        date_str = date_match.group(1) if date_match else date
        parsed = date_type.fromisoformat(date_str)
        writer = ManifestWriter(
            service_name="instruments-service",
            catalogue_bucket=bucket,
            catalogue_prefix="_catalogue",
        )
        writer.add(
            dataset_id="instruments",
            category="",
            processing_date=parsed,
            row_count=record_count,
            gcs_bucket=bucket,
            gcs_prefix=path,
        )
        writer.write()
    except Exception as exc:
        logger.debug("ManifestWriter failed (non-blocking): %s", exc)
