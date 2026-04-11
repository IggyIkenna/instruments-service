"""Generate and upload instruments domain config to cloud storage.

This is the TEMPLATE config generation script for the unified trading system.
Each service domain should have a similar script.

The script uses UCI ConfigStore (cloud-agnostic -- routes to GCS/S3 based on CLOUD_PROVIDER).
Services never know about GCS/S3 directly.

Usage:
    # Preview YAML (no upload)
    python scripts/generate_domain_config.py --dry-run

    # Upload to cloud storage
    python scripts/generate_domain_config.py --upload --project-id my-project

    # Upload with specific effective date
    python scripts/generate_domain_config.py --upload --project-id my-project --effective-date 2026-01-05

    # Upload with custom bucket
    python scripts/generate_domain_config.py --upload --project-id my-project --bucket config-store-my-project
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import yaml
from unified_api_contracts import KNOWN_ETFS, VenueMapping
from unified_trading_library import (
    ConfigStore,
    InstrumentDomainConfig,
    TickerUniverseConfig,
    TimeSeriesConfigStore,
)

logger = logging.getLogger(__name__)

# Resolve tickers.json relative to instruments_service package
TICKERS_JSON_PATH = Path(__file__).resolve().parent.parent / "instruments_service" / "config" / "data" / "tickers.json"

SERVICE_NAME = "instruments-service"
SCHEMA_VERSION = "1.0"


def _load_tickers() -> dict[str, list[str] | str]:
    """Load ticker data from instruments_service/config/data/tickers.json."""
    if not TICKERS_JSON_PATH.exists():
        logger.error("tickers.json not found at %s", TICKERS_JSON_PATH)
        sys.exit(1)

    with open(TICKERS_JSON_PATH) as f:
        data: dict[str, list[str] | str] = json.load(f)

    logger.info(
        "Loaded tickers.json: %d SP500, %d ETF, %d NASDAQ",
        len(data.get("sp500_tickers", [])),
        len(data.get("etf_tickers", [])),
        len(data.get("nasdaq_tickers", [])),
    )
    return data


def _build_ticker_universe_config(
    tickers_data: dict[str, list[str] | str],
    effective_date: str,
) -> TickerUniverseConfig:
    """Construct TickerUniverseConfig from tickers.json and UAC KNOWN_ETFS."""
    sp500_raw = tickers_data.get("sp500_tickers", [])
    etf_raw = tickers_data.get("etf_tickers", [])
    nasdaq_raw = tickers_data.get("nasdaq_tickers", [])

    sp500_tickers: list[str] = sp500_raw if isinstance(sp500_raw, list) else []
    etf_tickers: list[str] = etf_raw if isinstance(etf_raw, list) else []
    nasdaq_tickers: list[str] = nasdaq_raw if isinstance(nasdaq_raw, list) else []

    # Merge ETF tickers from tickers.json with UAC KNOWN_ETFS (SSOT for ETF classification)
    known_etf_symbols = sorted(set(etf_tickers) | KNOWN_ETFS)

    # Use _env_file=None to prevent BaseSettings from reading .env
    # (which contains instruments-service vars that conflict with extra="forbid")
    return TickerUniverseConfig(
        _env_file=None,
        effective_date=effective_date,
        sp500_tickers=sp500_tickers,
        etf_tickers=etf_tickers,
        nasdaq_tickers=nasdaq_tickers,
        known_etf_symbols=known_etf_symbols,
    )


def _build_instrument_domain_config() -> InstrumentDomainConfig:
    """Construct InstrumentDomainConfig with default values.

    Enabled venues are derived from UAC VenueMapping (SSOT for venue identity).
    """
    venue_mapping = VenueMapping()
    enabled_venues = sorted(venue_mapping.all_exchanges)

    return InstrumentDomainConfig(
        _env_file=None,
        subscription_list=[],
        enabled_venues=enabled_venues,
        categories=["cefi", "tradfi", "defi"],
    )


def _print_yaml(label: str, config_obj: TickerUniverseConfig | InstrumentDomainConfig) -> None:
    """Print config as YAML to stdout for dry-run preview."""
    config_dict: dict[str, object] = config_obj.model_dump(mode="json")
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    print(yaml.safe_dump(config_dict, default_flow_style=False, sort_keys=False))


def _upload_ticker_universe(
    config: TickerUniverseConfig,
    bucket: str,
    project_id: str,
) -> str:
    """Upload TickerUniverseConfig via TimeSeriesConfigStore."""
    store = TimeSeriesConfigStore(
        bucket_name=bucket,
        service_name=SERVICE_NAME,
        schema_version=SCHEMA_VERSION,
        project_id=project_id,
    )
    path = store.save_config(
        config,
        metadata={
            "generator": "generate_domain_config.py",
            "effective_date": config.effective_date,
        },
        author="generate_domain_config",
    )
    logger.info("TickerUniverseConfig uploaded to gs://%s/%s", bucket, path)
    return path


def _upload_instrument_domain(
    config: InstrumentDomainConfig,
    bucket: str,
    project_id: str,
) -> str:
    """Upload InstrumentDomainConfig via regular ConfigStore."""
    store = ConfigStore(
        bucket_name=bucket,
        service_name=f"{SERVICE_NAME}-domain",
        schema_version=SCHEMA_VERSION,
        project_id=project_id,
    )
    path = store.save_config(
        config,
        metadata={
            "generator": "generate_domain_config.py",
            "venue_count": str(len(config.enabled_venues)),
            "categories": ",".join(config.categories),
        },
        author="generate_domain_config",
    )
    logger.info("InstrumentDomainConfig uploaded to gs://%s/%s", bucket, path)
    return path


def main() -> None:
    """CLI entrypoint for config generation."""
    parser = argparse.ArgumentParser(
        description="Generate and upload instruments domain config to cloud storage.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview YAML output to stdout (no upload)",
    )
    mode_group.add_argument(
        "--upload",
        action="store_true",
        help="Upload config to cloud storage",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default=None,
        help="GCP project ID (required for --upload)",
    )
    parser.add_argument(
        "--effective-date",
        type=str,
        default=None,
        help="Effective date for TickerUniverseConfig (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="Config store bucket name. Defaults to config-store-{project-id}.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Resolve effective date
    effective_date: str = args.effective_date or date.today().isoformat()

    # Validate date format
    try:
        datetime.strptime(effective_date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid --effective-date format: %s (expected YYYY-MM-DD)", effective_date)
        sys.exit(1)

    # Load source data and build configs
    tickers_data = _load_tickers()
    ticker_universe = _build_ticker_universe_config(tickers_data, effective_date)
    instrument_domain = _build_instrument_domain_config()

    if args.dry_run:
        _print_yaml("TickerUniverseConfig", ticker_universe)
        _print_yaml("InstrumentDomainConfig", instrument_domain)
        print(
            f"\nTicker universe: {len(ticker_universe.sp500_tickers)} SP500, "
            f"{len(ticker_universe.etf_tickers)} ETF (tickers.json), "
            f"{len(ticker_universe.known_etf_symbols)} known ETF symbols (merged with UAC KNOWN_ETFS)"
        )
        print(
            f"Instrument domain: {len(instrument_domain.enabled_venues)} venues, "
            f"{len(instrument_domain.categories)} categories"
        )
        print(f"Effective date: {effective_date}")
        print("\nDry run complete. Use --upload to push to cloud storage.")
        return

    # Upload mode -- validate required args
    if not args.project_id:
        logger.error("--project-id is required for --upload mode")
        sys.exit(1)

    bucket: str = args.bucket or f"config-store-{args.project_id}"

    logger.info("Uploading to bucket=%s, project=%s", bucket, args.project_id)

    ticker_path = _upload_ticker_universe(ticker_universe, bucket, args.project_id)
    domain_path = _upload_instrument_domain(instrument_domain, bucket, args.project_id)

    logger.info("Upload complete.")
    logger.info("  TickerUniverseConfig -> %s", ticker_path)
    logger.info("  InstrumentDomainConfig -> %s", domain_path)

    # Summary
    now_utc = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\nConfig generation complete at {now_utc}")
    print(f"  Bucket: {bucket}")
    print(f"  TickerUniverseConfig: {ticker_path}")
    print(f"  InstrumentDomainConfig: {domain_path}")
    print(f"  Effective date: {effective_date}")


if __name__ == "__main__":
    main()
