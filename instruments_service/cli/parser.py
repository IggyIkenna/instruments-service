"""
CLI Argument Parser

Argument parsing utilities for instruments-service CLI.
Provides comprehensive CLI argument parsing with validation.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import argparse
import logging

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments for instruments-service.

    Returns:
        Parsed arguments namespace with all CLI options

    Example:
        >>> args = parse_arguments()
        >>> print(f"Mode: {args.mode}")
        >>> print(f"Date range: {args.start_date} to {args.end_date}")
    """
    parser = argparse.ArgumentParser(
        description="Instruments Service - Generate canonical instrument definitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_get_examples_text(),
    )

    # Mode selection
    parser.add_argument(
        "--mode",
        choices=[
            "instruments",
            "corporate_actions",
            "corporate_actions_backfill",
            "generate_date_views",
            "corporate_actions_update",
            "corporate_actions_production",
        ],
        required=True,
        help="Operation mode: instruments (generate instrument definitions), corporate_actions (TRADFI only: fetch dividends, splits, earnings for equities), corporate_actions_backfill (fetch full history per ticker), generate_date_views (transform by_ticker to by_date), corporate_actions_update (incremental updates), corporate_actions_production (complete production pipeline)",
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
        choices=["CEFI", "TRADFI", "DEFI"],
        type=str.upper,  # Accept any case (tradfi -> TRADFI)
        help="Market categories to process (can specify multiple: --category CEFI TRADFI). Alternative to --CEFI --TRADFI --DEFI flags.",
    )

    # Venue filter (optional - filter to specific venues within a category)
    parser.add_argument(
        "--venues",
        nargs="+",
        type=str.upper,  # Accept any case (aave_v3_eth -> AAVE_V3_ETH)
        help="Specific venues to process (space-separated). Examples: --venues AAVE_V3_ETH LIDO, --venues BINANCE-SPOT BYBIT",
    )
    parser.add_argument(
        "--CEFI",
        action="store_true",
        help="Include CEFI (Centralized Finance) exchanges via Tardis (binance, deribit, bybit, okx, etc.). Default: Process all market types if no flags specified.",
    )
    parser.add_argument(
        "--TRADFI",
        action="store_true",
        help="Include TradFi (Traditional Finance) exchanges via Databento (CME, NASDAQ, NYSE, etc.). Default: Process all market types if no flags specified.",
    )
    parser.add_argument(
        "--DEFI",
        action="store_true",
        help="Include DeFi (Decentralized Finance) protocols via The Graph (uniswap_v3, curve, aave_v3, etc.). Default: Process all market types if no flags specified.",
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

    return args


def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate parsed arguments for consistency.

    Args:
        args: Parsed arguments namespace

    Raises:
        ValueError: If arguments are invalid or inconsistent
    """
    # Validate date range for instruments mode
    if args.mode == "instruments":
        if not args.start_date:
            raise ValueError("--start-date is required for instruments mode")
        if not args.end_date:
            # Default to same as start_date if not provided
            args.end_date = args.start_date

    # Validate date range for corporate_actions mode
    if args.mode == "corporate_actions":
        if not args.start_date:
            raise ValueError("--start-date is required for corporate_actions mode")
        if not args.end_date:
            # Default to same as start_date if not provided
            args.end_date = args.start_date

    # Market type filters can be combined (e.g., --CEFI --TRADFI)
    # If none specified, all will be processed by default
    # No validation needed - flags are additive


def _get_examples_text() -> str:
    """Get help text with usage examples."""
    return """
Examples:

  # Generate instruments for a date range (default: ALL market types - CEFI, TRADFI, DEFI)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24

  # Generate CEFI instruments only (Tardis: binance, deribit, bybit, okx, etc.)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --force

  # Generate TRADFI instruments only (Databento: CME, NASDAQ, NYSE, etc.)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --TRADFI --force

  # Generate DEFI instruments only (The Graph: uniswap_v3, curve, aave_v3, etc.)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --DEFI --force

  # Generate instruments for specific venues only (filter within a category)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --DEFI --venues AAVE_V3_ETH LIDO --force
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --venues BINANCE-SPOT BYBIT --force

  # Generate CEFI and TRADFI (combine flags OR use --category)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --TRADFI --force
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --category CEFI TRADFI --force

  # Generate instruments with force flag
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force

  # Dry run (don't upload to GCS, save to local data/sample/ directory)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --dry-run

Corporate Actions (dividends, splits, earnings):

  # Fetch corporate actions for S&P 500 (last 6 years)
  python -m instruments_service --mode corporate_actions --start-date 2020-01-01 --end-date 2026-01-25

  # Fetch corporate actions for specific tickers
  python -m instruments_service --mode corporate_actions --start-date 2020-01-01 --end-date 2026-01-25 --tickers AAPL MSFT GOOGL

  # Save as CSV instead of Parquet
  python -m instruments_service --mode corporate_actions --start-date 2020-01-01 --end-date 2026-01-25 --output-format csv

Query Instruments (use unified-cloud-services):

  # Query instruments from Python
  from unified_cloud_services import StandardizedDomainCloudService, CloudTarget
  service = StandardizedDomainCloudService(domain='instruments', cloud_target=CloudTarget(...))
  df = service.download_from_gcs(gcs_path='CEFI/by_date/day-2023-05-23/instruments.parquet')
"""
