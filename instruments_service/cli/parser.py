"""
CLI Argument Parser

Argument parsing utilities for instruments-service CLI.
Provides comprehensive CLI argument parsing with validation.

Note: Query functionality has been moved to unified-trading-library.
Use InstrumentsDomainClient from unified-trading-library to query instruments.
"""

import argparse
import logging
from typing import Protocol, cast

logger = logging.getLogger(__name__)


class ParsedArgs(Protocol):
    """Typed protocol for parsed CLI arguments (argparse.Namespace attributes)."""

    mode: str
    run_mode: str
    log_level: str
    start_date: str | None
    end_date: str | None
    project_id: str | None
    gcs_bucket: str | None
    bigquery_dataset: str
    redo_all: bool
    force: bool
    dry_run: bool
    max_workers: int
    tickers: list[str] | None
    output_format: str
    upload_to_gcs: bool
    parallel_workers: int
    days_threshold: int
    input_dir: str | None
    output_dir: str | None
    max_retries: int
    category: list[str] | None
    CEFI: bool
    TRADFI: bool
    DEFI: bool
    SPORTS: bool
    venues: list[str] | None
    interval: int | None  # Live mode only (may not exist)


def parse_arguments() -> ParsedArgs:
    """
    Parse command line arguments for instruments-service.

    Returns:
        Parsed arguments namespace with all CLI options

    Example:
        >>> args = parse_arguments()
        >>> args.mode  # Returns the selected mode
        >>> args.start_date  # Returns the start date
    """
    parser = argparse.ArgumentParser(
        description="Instruments Service - Generate canonical instrument definitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_get_examples_text(),
    )

    # Mode selection (operation type)
    parser.add_argument(
        "--mode",
        choices=[
            "instruments",
            "aggregate",
            "corporate_actions",
            "corporate_actions_backfill",
            "generate_date_views",
            "corporate_actions_update",
            "corporate_actions_production",
        ],
        required=True,
        help=(
            "Operation mode: instruments (generate instrument definitions), aggregate "
            "(deduplicate instruments to aggregated/ per category bucket), corporate_actions "
            "(TRADFI only: fetch dividends, splits, earnings), corporate_actions_backfill "
            "(fetch full history per ticker), generate_date_views (transform by_ticker to by_date), "
            "corporate_actions_update (incremental), corporate_actions_production (complete pipeline)"
        ),
    )

    # Run mode (batch vs live execution)
    parser.add_argument(
        "--run-mode",
        choices=["batch", "live"],
        required=True,
        help="Execution mode: batch for historical date range, live for continuous",
    )

    # Date range (required for instruments mode)
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (required)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (defaults to start-date if not provided)",
    )

    # Configuration
    parser.add_argument(
        "--project-id",
        type=str,
        default=None,  # Will use config default if not specified
        help="GCP project ID (default: from service config)",
    )
    parser.add_argument(
        "--gcs-bucket",
        type=str,
        default=None,  # Category-specific buckets are used automatically
        help="GCS bucket name (default: uses category-specific buckets from .env)",
    )
    parser.add_argument(
        "--bigquery-dataset",
        type=str,
        default="market_data_hft",
        help="BigQuery dataset name (default: market_data_hft)",
    )

    # Aggregate mode options
    parser.add_argument(
        "--redo-all",
        action="store_true",
        help="[aggregate mode] Full rebuild from all by-date/venue files (default: delta-only, previous day)",
    )

    # Processing options
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force processing all dates (overrides existence checks)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't upload to GCS, dump to local data/sample/ directory instead",
    )

    # Market type filters (can be combined, default is ALL if none specified)
    parser.add_argument(
        "--category",
        nargs="+",
        choices=["CEFI", "TRADFI", "DEFI", "SPORTS"],
        type=str.upper,  # Accept any case (tradfi -> TRADFI)
        help=(
            "Market categories to process (can specify multiple: --category CEFI TRADFI). "
            "Alternative to --CEFI --TRADFI --DEFI --SPORTS flags."
        ),
    )

    # Venue filter (optional - filter to specific venues within a category)
    parser.add_argument(
        "--venues",
        nargs="+",
        type=str.upper,  # Accept any case (aave_v3_eth -> AAVE_V3_ETH)
        help=(
            "Specific venues to process (space-separated). "
            "Examples: --venues AAVE_V3_ETH LIDO, --venues BINANCE-SPOT BYBIT"
        ),
    )
    parser.add_argument(
        "--CEFI",
        action="store_true",
        help=(
            "Include CEFI (Centralized Finance) exchanges via Tardis (binance, deribit, bybit, okx, etc.). "
            "Default: Process all market types if no flags specified."
        ),
    )
    parser.add_argument(
        "--TRADFI",
        action="store_true",
        help=(
            "Include TradFi (Traditional Finance) exchanges via Databento (CME, NASDAQ, NYSE, etc.). "
            "Default: Process all market types if no flags specified."
        ),
    )
    parser.add_argument(
        "--DEFI",
        action="store_true",
        help=(
            "Include DeFi (Decentralized Finance) protocols via The Graph (uniswap_v3, curve, aave_v3, etc.). "
            "Default: Process all market types if no flags specified."
        ),
    )
    parser.add_argument(
        "--SPORTS",
        action="store_true",
        help=(
            "Include Sports fixtures (EPL, NBA, NFL, MLB, NHL, etc.). "
            "Default: Process all market types if no flags specified."
        ),
    )

    # Corporate actions options
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers for corporate_actions mode (default: S&P 500)",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format for corporate_actions mode (default: parquet)",
    )
    parser.add_argument(
        "--upload-to-gcs",
        action="store_true",
        help="Upload corporate actions to GCS (default: save locally only)",
    )

    # Concurrency options
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel workers for processing (default: 4)",
    )

    # Backfill/update options
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=2,  # Changed from 10 to 2 to avoid rate limiting
        help="Number of parallel workers for backfill/update (default: 2)",
    )
    parser.add_argument(
        "--days-threshold",
        type=int,
        default=7,
        help="Days before ticker is considered outdated for updates (default: 7)",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        help="Input directory for date views generation",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for date views generation",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per ticker (default: 3)",
    )

    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    # Parse arguments
    args = parser.parse_args()

    # Validate arguments
    validate_arguments(args)

    return cast(ParsedArgs, cast(object, args))


def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate parsed arguments for consistency.

    Args:
        args: Parsed arguments namespace

    Raises:
        ValueError: If arguments are invalid or inconsistent
    """
    mode = cast(str, args.mode)
    start_date = cast(str | None, args.start_date)
    end_date = cast(str | None, args.end_date)

    # Validate date range for instruments mode
    if mode == "instruments":
        if not start_date:
            raise ValueError("--start-date is required for instruments mode")
        if not end_date:
            args.end_date = start_date

    # Validate date range for corporate_actions mode
    if mode == "corporate_actions":
        if not start_date:
            raise ValueError("--start-date is required for corporate_actions mode")
        if not end_date:
            args.end_date = start_date

    # Market type filters can be combined (e.g., --CEFI --TRADFI)
    # If none specified, all will be processed by default
    # No validation needed - flags are additive


def _get_examples_text() -> str:
    """Get help text with usage examples."""
    return """
Examples:

  # Generate instruments for a date range (default: ALL market types - CEFI, TRADFI, DEFI)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-24

  # Generate CEFI instruments only (Tardis: binance, deribit, bybit, okx, etc.)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --force

  # Generate TRADFI instruments only (Databento: CME, NASDAQ, NYSE, etc.)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --TRADFI --force

  # Generate DEFI instruments only (The Graph: uniswap_v3, curve, aave_v3, etc.)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --DEFI --force

  # Generate instruments for specific venues only (filter within a category)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --DEFI \\
    --venues AAVE_V3_ETH LIDO --force
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --CEFI \\
    --venues BINANCE-SPOT BYBIT --force

  # Generate CEFI and TRADFI (combine flags OR use --category)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 \\
    --CEFI --TRADFI --force
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 \\
    --category CEFI TRADFI --force

  # Generate instruments with force flag
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --force

  # Dry run (don't upload to GCS, save to local data/sample/)
  python -m instruments_service --mode instruments --run-mode batch --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --dry-run

Aggregate (deduplicate instruments to aggregated/ per category bucket):

  # Delta-only: merge previous day with existing aggregated file (default, daily batch)
  python -m instruments_service --mode aggregate --run-mode batch

  # Full rebuild from all by-date/venue files (schema changes)
  python -m instruments_service --mode aggregate --run-mode batch --redo-all

Corporate Actions (dividends, splits, earnings):

  # Fetch corporate actions for S&P 500 (last 6 years)
  python -m instruments_service --mode corporate_actions --run-mode batch --start-date 2020-01-01 --end-date 2026-01-25

  # Fetch corporate actions for specific tickers
  python -m instruments_service --mode corporate_actions --run-mode batch --start-date 2020-01-01 --end-date 2026-01-25 \\
    --tickers AAPL MSFT GOOGL

  # Save as CSV instead of Parquet
  python -m instruments_service --mode corporate_actions --run-mode batch --start-date 2020-01-01 --end-date 2026-01-25 \\
    --output-format csv

Query Instruments (use unified-trading-library):

  # Query instruments from Python
  from unified_trading_library import StandardizedDomainCloudService, CloudTarget
  service = StandardizedDomainCloudService(domain='instruments', cloud_target=CloudTarget(...))
  df = service.download_from_gcs(
      gcs_path='CEFI/by_date/day-2023-05-23/instruments.parquet'
  )
"""
