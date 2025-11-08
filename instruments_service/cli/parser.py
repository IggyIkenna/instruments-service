"""
CLI Argument Parser

Argument parsing utilities for instruments-service CLI.
Provides comprehensive CLI argument parsing with validation.
"""

import argparse
import logging
from typing import Any, Dict, Optional

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
        description='Instruments Service - Generate and query canonical instrument definitions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_get_examples_text()
    )
    
    # Mode selection
    parser.add_argument(
        '--mode', 
        choices=['instruments', 'instruments-query'],
        required=True,
        help='Operation mode: instruments (generate) or instruments-query (query)'
    )
    
    # Date range (required for instruments mode, optional for query)
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format (required for instruments mode)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format (required for instruments mode, optional for query)'
    )
    
    # Configuration
    parser.add_argument(
        '--project-id',
        type=str,
        default='central-element-323112',
        help='GCP project ID (default: central-element-323112)'
    )
    parser.add_argument(
        '--gcs-bucket',
        type=str,
        default='market-data-tick',
        help='GCS bucket name (default: market-data-tick)'
    )
    parser.add_argument(
        '--bigquery-dataset',
        type=str,
        default='market_data_hft',
        help='BigQuery dataset name (default: market_data_hft)'
    )
    
    # Processing options
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force processing all dates (overrides existence checks)'
    )
    parser.add_argument(
        '--exchanges',
        nargs='+',
        help='Exchanges to process (binance, binance-futures, deribit, bybit, okx, etc.)'
    )
    
    # Instruments query specific arguments
    parser.add_argument(
        '--query-type',
        choices=['list', 'summary', 'details', 'trading-params', 'data-types', 'expiring'],
        default='list',
        help='Type of instruments query to perform'
    )
    parser.add_argument(
        '--venues',
        nargs='+',
        help='Filter by venues (BINANCE, BINANCE-FUTURES, DERIBIT, BYBIT, OKX, etc.)'
    )
    parser.add_argument(
        '--instrument-types',
        nargs='+',
        help='Filter by instrument types (SPOT_PAIR, PERPETUAL, FUTURE, OPTION, etc.)'
    )
    parser.add_argument(
        '--base-currency',
        help='Filter by base currency (BTC, ETH, SOL, etc.)'
    )
    parser.add_argument(
        '--quote-currency', 
        help='Filter by quote currency (USDT, USD, USDC, etc.)'
    )
    parser.add_argument(
        '--symbol-pattern',
        help='Regex pattern to match symbols (e.g., BTC.*, .*USDT)'
    )
    parser.add_argument(
        '--instrument-id',
        help='Specific instrument ID for details/trading-params queries'
    )
    parser.add_argument(
        '--instrument-ids',
        nargs='+',
        help='List of specific instrument IDs to include'
    )
    parser.add_argument(
        '--data-type',
        help='Data type to filter by (trades, book_snapshot_5, derivative_ticker, etc.)'
    )
    parser.add_argument(
        '--days-until-expiry',
        type=int,
        default=30,
        help='Days ahead to look for expiring instruments'
    )
    parser.add_argument(
        '--output-format',
        choices=['summary', 'json', 'csv'],
        default='summary',
        help='Output format for instruments query results'
    )
    parser.add_argument(
        '--output-file',
        help='Output file path for CSV format'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Maximum number of instruments to return'
    )
    
    # Logging
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
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
    if args.mode == 'instruments':
        if not args.start_date:
            raise ValueError("--start-date is required for instruments mode")
        if not args.end_date:
            # Default to same as start_date if not provided
            args.end_date = args.start_date
    
    # Validate query-specific arguments
    if args.mode == 'instruments-query':
        if args.query_type in ['details', 'trading-params'] and not args.instrument_id:
            raise ValueError(f"--instrument-id is required for query-type={args.query_type}")
        if args.query_type == 'data-types' and not args.data_type:
            raise ValueError("--data-type is required for query-type=data-types")


def _get_examples_text() -> str:
    """Get help text with usage examples."""
    return """
Examples:

  # Generate instruments for a date range
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-24

  # Generate instruments with force flag
  python -m instruments_service --mode instruments --start-date 2023-05-23 --end-date 2023-05-23 --force

  # Query instruments for a specific date
  python -m instruments_service --mode instruments-query --start-date 2023-05-23

  # Query instruments with filters
  python -m instruments_service --mode instruments-query --start-date 2023-05-23 \\
      --venues BINANCE-FUTURES --instrument-types PERPETUAL --base-currency BTC

  # Get instrument details
  python -m instruments_service --mode instruments-query --start-date 2023-05-23 \\
      --query-type details --instrument-id BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN

  # Export instruments to CSV
  python -m instruments_service --mode instruments-query --start-date 2023-05-23 \\
      --output-format csv --output-file instruments.csv
"""

