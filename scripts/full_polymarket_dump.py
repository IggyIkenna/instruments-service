#!/usr/bin/env python3
"""Full Polymarket instrument dump via CLOB API cursor pagination.

Scans ALL 863K+ markets on CLOB API (cursor pagination), groups by end_date,
and writes per-date instrument parquets + manifest entries.

CLOB has the full market history — Gamma prunes old resolved markets (~49K remain).
CLOB cursor pagination is reliable (no sparse offset gaps).

Usage:
    python3 scripts/full_polymarket_dump.py --dry-run          # preview counts
    python3 scripts/full_polymarket_dump.py                    # full dump to GCS
    python3 scripts/full_polymarket_dump.py --min-date 2026-01-01  # recent only
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import UTC, datetime
from datetime import date as date_type

import pandas as pd
import requests
from unified_api_contracts import PipelineMode, classify_venue_error
from unified_trading_library import (
    CaptureStatus,
    ManifestRow,
    ManifestWriter,
    UnifiedCloudConfig,
    get_storage_client,
    log_event,
    setup_events,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _should_skip_polymarket_shard(
    manifest: ManifestWriter,
    *,
    end_date: str,
    market: str,
    force: bool,
) -> bool:
    """Honest-coverage pre-flight: skip if shard is captured / empty_confirmed.

    See ``honest_coverage_metrics_2026_04_19`` plan + ``_should_skip_shard``
    in ``instruments_service.engine.orchestrator``.
    """
    if force:
        return False
    prev: ManifestRow | None = manifest.lookup(
        {
            "date": end_date,
            "venue": "POLYMARKET",
            "data_type": market,
        }
    )
    if prev is None:
        return False
    return prev.capture_status in (
        CaptureStatus.CAPTURED.value,
        CaptureStatus.EMPTY_CONFIRMED.value,
    )


_CLOB = "https://clob.polymarket.com"
_BATCH_SIZE = 1000  # CLOB supports up to 1000 per page
_RATE_LIMIT_SLEEP = 0.05  # 50ms between requests (CLOB is stricter than Gamma)


def _extract_market(base_asset: str) -> str:
    """Extract market shard from base_asset. No allowlist."""
    parts = base_asset.split(":")
    if len(parts) >= 2 and parts[0] == "FOOTBALL":
        return "FOOTBALL"
    if len(parts) >= 4 and parts[2] == "UP_DOWN":
        return parts[3]
    return "OTHER"


def _enrich_clob_market(raw: dict[str, object]) -> None:
    """Enrich CLOB market dict with fields expected by PolymarketGammaMarket.

    Fixes two CLOB→Gamma incompatibilities:
    1. CLOB returns tokens[].outcome, not top-level outcomes list
    2. CLOB returns tags as list of strings, Gamma returns list of tag objects
    """
    # Extract outcomes from tokens
    tokens = raw.get("tokens")
    if isinstance(tokens, list) and tokens and "outcomes" not in raw:
        outcomes = []
        for t in tokens:
            if isinstance(t, dict) and "outcome" in t:
                outcomes.append(str(t["outcome"]))
        if outcomes:
            raw["outcomes"] = outcomes

    # Convert string tags to PolymarketGammaTag-compatible dicts
    tags = raw.get("tags")
    if isinstance(tags, list) and tags and isinstance(tags[0], str):
        raw["tags"] = [{"slug": t.lower().replace(" ", "-"), "label": t} for t in tags]


def main() -> None:
    parser = argparse.ArgumentParser(description="Full Polymarket instrument dump via CLOB API")
    parser.add_argument("--dry-run", action="store_true", help="Preview market counts without writing to GCS")
    parser.add_argument("--min-date", default="2025-03-01", help="Only include markets ending after this date")
    parser.add_argument("--max-pages", type=int, default=1000, help="Safety cap on number of pages to scan")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Re-attempt every (date, market) shard even if the manifest already "
            "shows captured / empty_confirmed.  Default skips already-attempted "
            "shards (honest-coverage pre-flight)."
        ),
    )
    args = parser.parse_args()
    setup_events(service_name="instruments-service")

    logger.info("Scanning CLOB API (min_date=%s, max_pages=%d)...", args.min_date, args.max_pages)

    # Phase 1: Scan ALL markets from CLOB via cursor pagination
    all_markets: list[dict[str, object]] = []
    cursor = ""
    page = 0
    total_scanned = 0

    while page < args.max_pages:
        params: dict[str, str] = {"limit": str(_BATCH_SIZE)}
        if cursor:
            params["next_cursor"] = cursor

        try:
            resp = requests.get(
                f"{_CLOB}/markets",
                params=params,
                timeout=30,
            )
            if resp.status_code == 429:
                logger.warning("Rate limited at page %d, sleeping 5s", page)
                time.sleep(5)
                continue
            if resp.status_code != 200:
                logger.warning("HTTP %d at page %d", resp.status_code, page)
                time.sleep(1)
                page += 1
                continue
            data = resp.json()
        except Exception as exc:
            logger.warning("Error at page %d: %s", page, exc)
            time.sleep(1)
            page += 1
            continue

        raw_markets = data.get("data", [])
        if not raw_markets:
            logger.info("Empty page at page %d — done scanning", page)
            break

        total_scanned += len(raw_markets)

        for m in raw_markets:
            if not isinstance(m, dict):
                continue
            end_date_raw = m.get("end_date_iso") or m.get("endDateIso") or ""
            end_date = str(end_date_raw)[:10]
            if end_date < args.min_date:
                continue
            _enrich_clob_market(m)
            all_markets.append(m)

        cursor = str(data.get("next_cursor", ""))
        # CLOB signals end with cursor "-1" (base64: "LTE=")
        if not cursor or cursor == "LTE=":
            logger.info("Reached end of CLOB pagination at page %d", page)
            break

        page += 1
        if page % 50 == 0:
            logger.info("  page %d — %d/%d markets collected/scanned", page, len(all_markets), total_scanned)
        time.sleep(_RATE_LIMIT_SLEEP)

    logger.info(
        "Scan complete: %d markets collected from %d total scanned (%d pages)",
        len(all_markets),
        total_scanned,
        page + 1,
    )

    if not all_markets:
        logger.error("No markets found!")
        return

    # Phase 2: Group by end_date
    df = pd.DataFrame(all_markets)
    df["_end_date"] = df.apply(
        lambda r: str(r.get("end_date_iso") or r.get("endDateIso") or "")[:10],
        axis=1,
    )
    date_groups = df.groupby("_end_date")
    logger.info("Grouped into %d dates", len(date_groups))

    if args.dry_run:
        # Show summary stats
        logger.info("=== Date distribution (showing first/last 20 + our target range) ===")
        sorted_groups = sorted(date_groups, key=lambda x: x[0])
        for i, (d, group) in enumerate(sorted_groups):
            # Show first 10, last 10, and anything in our target range (2025-03 to 2026-04)
            if i < 10 or i >= len(sorted_groups) - 10 or "2025-03" <= d <= "2026-05":
                logger.info("  %s: %d markets", d, len(group))
        return

    # Phase 3: Write per-date instrument files
    from unified_api_contracts import PolymarketGammaMarket

    client = get_storage_client()
    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = f"instruments-store-prediction-{project_id}"

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket, batch_size=100)
    dates_written = 0
    dates_skipped = 0

    for end_date, group in sorted(date_groups, key=lambda x: x[0]):
        if not end_date or end_date < args.min_date:
            continue

        # Honest-coverage attempt-stamp for this (date, *) shard family.  We
        # use a single timestamp for every per-market split below so they all
        # share an attempt epoch.  Per-market pre-flight runs in the inner
        # loop after we know the market shard label.
        attempt_ts = datetime.now(UTC)

        # Validate through PolymarketGammaMarket — same pipeline as the adapter.
        # Per-shard isolation: catch validation errors per-row and continue;
        # never raise out of the per-date loop.
        valid_rows: list[dict[str, object]] = []
        for _, row in group.iterrows():
            raw = row.to_dict()
            try:
                market = PolymarketGammaMarket.model_validate(raw)
                valid_rows.append(market.model_dump(by_alias=False))
            except Exception as exc:
                err_code = type(exc).__name__
                classification = classify_venue_error("polymarket", err_code)
                _err = classification.error_code if classification is not None else err_code
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "POLYMARKET",
                        "endpoint": "clob_market_validate",
                        "date": end_date,
                        "error_code": _err,
                    },
                )
                continue

        if not valid_rows:
            # Honest-coverage: every row failed validation OR the date had
            # zero markets.  We attempted, so write empty_confirmed at the
            # date level (no market shard known).  Operator can re-run with
            # --force to retry.
            manifest.record_empty(
                row_key={
                    "date": str(end_date),
                    "venue": "POLYMARKET",
                    "data_type": "ALL",
                },
                attempted_at=attempt_ts,
                pipeline_mode=PipelineMode.BATCH_POLYMARKET_GAMMA_API,
            )
            dates_skipped += 1
            continue

        inst_df = pd.DataFrame(valid_rows)

        # Write per-market split using question text to identify market shard.
        # Since CLOB doesn't have base_asset, derive market from question.
        inst_df["_market"] = inst_df.apply(_classify_market_from_question, axis=1)

        # Pre-flight per (date, market) shard so re-runs honour skip semantics.
        # We compute the full set of markets for this date FIRST, then filter
        # out already-attempted ones unless --force.
        all_markets_today = sorted({str(m) for m in inst_df["_market"].unique()})
        markets_to_write = [
            m
            for m in all_markets_today
            if not _should_skip_polymarket_shard(manifest, end_date=str(end_date), market=m, force=args.force)
        ]
        if not markets_to_write:
            dates_skipped += 1
            continue

        # Write combined file (always, since at least one market shard is being attempted)
        path = f"instrument_availability/by_date/day={end_date}/venue=POLYMARKET/instruments.parquet"
        buf = inst_df.to_parquet(index=False)
        client.upload_bytes(bucket, path, buf, content_type="application/octet-stream")

        for mkt, mkt_df in inst_df.groupby("_market"):
            mkt_str = str(mkt)
            if mkt_str not in markets_to_write:
                continue
            mkt_path = (
                f"instrument_availability/by_date/day={end_date}/venue=POLYMARKET/market={mkt_str}/instruments.parquet"
            )
            mkt_buf = mkt_df.drop(columns=["_market"]).to_parquet(index=False)
            client.upload_bytes(bucket, mkt_path, mkt_buf, content_type="application/octet-stream")

            manifest.add(
                processing_date=date_type.fromisoformat(str(end_date)),
                row_count=len(mkt_df),
                venue="POLYMARKET",
                data_type=mkt_str,
            )

        dates_written += 1
        if dates_written % 50 == 0:
            logger.info("  Written %d dates (%d skipped)", dates_written, dates_skipped)

    manifest.close()
    logger.info("Done: %d dates written, %d skipped, bucket=%s", dates_written, dates_skipped, bucket)


# Market classification from question text (same keywords as the adapter)
_CRYPTO_KEYWORDS: dict[str, str] = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
    "xrp": "XRP",
    "dogecoin": "DOGE",
    "doge": "DOGE",
    "bnb": "BNB",
    "hyperliquid": "HYPE",
    "hype": "HYPE",
}

_MACRO_KEYWORDS: dict[str, str] = {
    "s&p 500": "SPX",
    "(spx)": "SPX",
    "nasdaq": "NDX",
    "dow jones": "DJIA",
    "crude oil": "CRUDE_OIL",
    "gold (gc)": "GOLD",
    "gold": "GOLD",
    "silver (si)": "SILVER",
    "silver": "SILVER",
}


def _classify_market_from_question(row: pd.Series) -> str:
    """Classify market shard from question text."""
    question = str(row.get("question") or "").lower()

    # Check if it's an "Up or Down" market first
    if "up or down" not in question:
        # Check for sports
        tags = row.get("tags")
        if isinstance(tags, list):
            for t in tags:
                if isinstance(t, str) and t.lower() in ("soccer", "football", "nba", "nfl", "mlb", "nhl"):
                    return "FOOTBALL"
        return "OTHER"

    # UP_DOWN market — classify by underlying
    for kw, canonical in _CRYPTO_KEYWORDS.items():
        if kw in question:
            return canonical
    for kw, canonical in _MACRO_KEYWORDS.items():
        if kw in question:
            return canonical
    return "OTHER"


if __name__ == "__main__":
    main()
