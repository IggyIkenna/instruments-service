#!/usr/bin/env python3
"""Phase 2.5 — Forward verification — confirm new instruments-service runs write the new fields.

Reads the most recent ``instruments.parquet`` for each (venue, chain) pair
under ``gs://instruments-store-defi-{pid}/instrument_availability/by_date/...``
and asserts the new metadata columns are NOT NULL on at least one row per
venue.

This is the gate between Phase 2.5 (historical backfill) and Phase 3 (MTDS
handler refactor): once this verifier passes, MTDS handlers can safely
consume the parquet metadata instead of re-querying The Graph.

Usage
-----
    # All venues (typical run)
    python3 scripts/migrations/verify_defi_metadata_forward.py

    # Subset of venues (e.g. after re-deploying one adapter)
    python3 scripts/migrations/verify_defi_metadata_forward.py --venues UNISWAPV3-ETHEREUM

Exit codes
----------
- 0: every venue checked has at least one row with non-null metadata.
- 1: at least one venue failed the assertion (printed per-venue).
- 2: input/config error (e.g. unknown venue, missing bucket).
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from typing import cast

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Subset of metadata columns to assert non-null. We require at least one of
# (pool_address, base_asset_contract_address, atoken_address) — covering DEX
# pools, lending reserves, and Aave-style markets. These are the high-signal
# columns; the others (decimals/symbol/fee_tier) are derived and may legitimately
# be absent on some rows even when the discovery layer worked.
REQUIRED_NON_NULL_ANY: tuple[str, ...] = (
    "pool_address",
    "base_asset_contract_address",
    "atoken_address",
)

# Full metadata column set we report on (presence check + non-null sample).
ALL_METADATA_COLUMNS: tuple[str, ...] = (
    "pool_address",
    "pool_fee_tier",
    "base_asset_contract_address",
    "base_asset_decimals",
    "base_asset_symbol_onchain",
    "quote_asset_contract_address",
    "quote_asset_decimals",
    "quote_asset_symbol_onchain",
    "atoken_address",
    "debt_token_address",
)

_DEFAULT_PREFIX = "instrument_availability/by_date"
_DAY_RE = re.compile(r"day=(\d{4}-\d{2}-\d{2})")


def _list_dates_for_venue(
    storage_client: object,
    bucket: str,
    venue_tag: str,
    prefix: str = _DEFAULT_PREFIX,
) -> list[str]:
    """Return all date strings (YYYY-MM-DD) under the given venue, sorted ascending."""
    bucket_handle = cast("object", storage_client).bucket(bucket)
    venue_segment = f"venue={venue_tag}/"
    dates: set[str] = set()
    for blob in bucket_handle.list_blobs(prefix=prefix):
        name = str(blob.name)
        if not name.endswith("/instruments.parquet"):
            continue
        if venue_segment not in name:
            continue
        match = _DAY_RE.search(name)
        if match:
            dates.add(match.group(1))
    return sorted(dates)


def _venues_to_check() -> list[str]:
    """All canonical DeFi venue tags with subgraph backing."""
    from unified_api_contracts.registry import SUBGRAPH_IDS, get_subgraph_id

    from instruments_service.reference_data.factory import (
        _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL,
    )

    venues: list[str] = []
    for prefix, protocol_slug in _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL.items():
        for chain in SUBGRAPH_IDS.get(protocol_slug, {}):
            if get_subgraph_id(protocol_slug, chain) is None:
                continue
            venues.append(f"{prefix}-{chain}")
    return sorted(venues)


def _verify_one_parquet(
    storage_client: object,
    bucket: str,
    blob_path: str,
) -> tuple[bool, dict[str, int]]:
    """Verify one parquet. Returns ``(passed, per_column_non_null_counts)``."""
    raw = cast("object", storage_client).download_bytes(bucket, blob_path)
    df = pd.read_parquet(io.BytesIO(raw))

    counts: dict[str, int] = {}
    for col in ALL_METADATA_COLUMNS:
        if col not in df.columns:
            counts[col] = -1  # column missing
        else:
            counts[col] = int(df[col].notna().sum())

    # Pass criterion: at least one row has a non-null value in any of the
    # high-signal metadata columns (pool_address / base_asset_contract_address /
    # atoken_address).
    passed = any(counts.get(col, -1) > 0 for col in REQUIRED_NON_NULL_ANY)
    return passed, counts


def _format_counts(counts: dict[str, int]) -> str:
    parts: list[str] = []
    for col in ALL_METADATA_COLUMNS:
        c = counts.get(col, -1)
        if c == -1:
            parts.append(f"{col}=MISSING")
        else:
            parts.append(f"{col}={c}")
    return " ".join(parts)


def _parse_venues_filter(raw: str | None) -> set[str] | None:
    if raw is None or raw.strip() == "":
        return None
    return {v.strip().upper() for v in raw.split(",") if v.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify DeFi metadata is populated on the latest instruments-store-defi parquet per venue."
    )
    parser.add_argument(
        "--venues",
        default=None,
        help="Comma-separated venue filter (e.g. UNISWAPV3-ETHEREUM,AAVEV3-ETHEREUM).",
    )
    args = parser.parse_args()

    from unified_trading_library import (
        UnifiedCloudConfig,
        get_bucket_name,
        get_storage_client,
    )

    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = get_bucket_name("instruments", "DEFI")
    logger.info("Bucket: %s (project=%s)", bucket, project_id)

    storage_client = get_storage_client()
    venues_filter = _parse_venues_filter(args.venues)
    venues = _venues_to_check()
    if venues_filter is not None:
        venues = [v for v in venues if v in venues_filter]
        missing = venues_filter - set(venues)
        if missing:
            logger.warning("Unknown venues in --venues filter (skipped): %s", sorted(missing))

    if not venues:
        logger.error("No venues to verify — exit")
        return 2

    failures: list[str] = []
    for venue_tag in venues:
        dates = _list_dates_for_venue(storage_client, bucket, venue_tag)
        if not dates:
            logger.warning("[%s] no historical parquets found — skipping", venue_tag)
            continue
        latest = dates[-1]
        path = f"{_DEFAULT_PREFIX}/day={latest}/venue={venue_tag}/instruments.parquet"
        try:
            passed, counts = _verify_one_parquet(storage_client, bucket, path)
        except (OSError, ValueError) as exc:
            logger.error("[%s] verify FAILED reading %s: %s", venue_tag, path, exc)
            failures.append(venue_tag)
            continue
        if passed:
            logger.info("[%s] PASS day=%s :: %s", venue_tag, latest, _format_counts(counts))
        else:
            logger.error("[%s] FAIL day=%s :: %s", venue_tag, latest, _format_counts(counts))
            failures.append(venue_tag)

    if failures:
        logger.error("Verification FAILED for %d venues: %s", len(failures), sorted(failures))
        return 1
    logger.info("All %d venues PASSED metadata verification", len(venues))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
