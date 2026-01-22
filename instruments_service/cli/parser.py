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

    # Mode selection (only instruments generation now, query moved to unified-cloud-services)
    parser.add_argument(
        "--mode",
        choices=["instruments"],
        required=True,
        help="Operation mode: instruments (generate instrument definitions)",
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
        default="central-element-323112",
        help="GCP project ID (default: central-element-323112)",
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
        "--exchanges",
        nargs="+",
        help="Exchanges to process (binance, binance-futures, deribit, bybit, okx, etc.)",
    )

    # Market type filters (can be combined, default is ALL if none specified)
    parser.add_argument(
        "--category",
        nargs="+",
        choices=["CEFI", "TRADFI", "DEFI"],
        type=str.upper,  # Accept any case (tradfi -> TRADFI)
        help="Market categories to process (can specify multiple: --category CEFI TRADFI). Alternative to --CEFI --TRADFI --DEFI flags.",
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

  # Generate CEFI and TRADFI (combine flags OR use --category)
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --CEFI --TRADFI --force
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --category CEFI TRADFI --force

  # Generate instruments with force flag
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force

Query Instruments (use unified-cloud-services):

  # Query instruments from Python
  from unified_cloud_services import StandardizedDomainCloudService, CloudTarget
  service = StandardizedDomainCloudService(domain='instruments', cloud_target=CloudTarget(...))
  df = service.download_from_gcs(gcs_path='CEFI/by_date/day-2023-05-23/instruments.parquet')
"""
