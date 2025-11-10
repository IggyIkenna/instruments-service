"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).
"""

import sys
import logging
import os
from pathlib import Path
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from .parser import parse_arguments
from .handlers import get_handler_for_mode


def main() -> Dict[str, Any]:
    """
    Main CLI entry point for instruments-service.

    Returns:
        Dictionary with operation results
    """
    try:
        # Parse arguments
        args = parse_arguments()

        # Setup logging level
        logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

        logger.info(f"🚀 Starting {args.mode} operation")
        if args.start_date:
            logger.info(
                f"📅 Date range: {args.start_date} to {args.end_date or args.start_date}"
            )

        # Build configuration from arguments
        config = {
            "project_id": args.project_id,
            "gcs_bucket": args.gcs_bucket,
            "bigquery_dataset": args.bigquery_dataset,
        }

        # Get handler for mode
        handler = get_handler_for_mode(args.mode, config)

        # Prepare arguments for handler
        handler_kwargs = {}

        # Date range
        if args.start_date:
            handler_kwargs["start_date"] = args.start_date
        if args.end_date:
            handler_kwargs["end_date"] = args.end_date

        # Common options
        if args.force:
            handler_kwargs["force"] = args.force
        if args.exchanges:
            handler_kwargs["exchanges"] = args.exchanges
        
        # Market type filters
        if args.CEFI:
            handler_kwargs["cefi"] = True
        if args.TRADFI:
            handler_kwargs["tradfi"] = True
        if args.DEFI:
            handler_kwargs["defi"] = True

        # Query-specific arguments
        if args.mode == "instruments-query":
            handler_kwargs["query_type"] = args.query_type
            if args.venues:
                handler_kwargs["venues"] = args.venues
            if args.instrument_types:
                handler_kwargs["instrument_types"] = args.instrument_types
            if args.base_currency:
                handler_kwargs["base_currency"] = args.base_currency
            if args.quote_currency:
                handler_kwargs["quote_currency"] = args.quote_currency
            if args.symbol_pattern:
                handler_kwargs["symbol_pattern"] = args.symbol_pattern
            if args.instrument_id:
                handler_kwargs["instrument_id"] = args.instrument_id
            if args.instrument_ids:
                handler_kwargs["instrument_ids"] = args.instrument_ids
            if args.data_type:
                handler_kwargs["data_type"] = args.data_type
            if args.days_until_expiry:
                handler_kwargs["days_until_expiry"] = args.days_until_expiry
            if args.output_format:
                handler_kwargs["output_format"] = args.output_format
            if args.output_file:
                handler_kwargs["output_file"] = args.output_file
            if args.limit:
                handler_kwargs["limit"] = args.limit

        # Execute handler
        result = handler.run(**handler_kwargs)

        # Cleanup
        handler.cleanup()

        # Log success
        if result.get("status") == "success" or result.get("success", True):
            logger.info(f"✅ {args.mode} operation completed successfully")
        else:
            logger.error(f"❌ {args.mode} operation failed")

        return result

    except Exception as e:
        logger.error(f"❌ CLI execution failed: {e}", exc_info=True)
        return {"success": False, "status": "error", "error": str(e)}


def run_cli():
    """Synchronous CLI execution"""
    try:
        result = main()
        return result
    except KeyboardInterrupt:
        logger.info("🛑 Operation cancelled by user")
        return {"success": False, "status": "error", "error": "Cancelled by user"}
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        return {"success": False, "status": "error", "error": str(e)}


if __name__ == "__main__":
    result = run_cli()
    exit_code = (
        0 if result.get("success", False) or result.get("status") == "success" else 1
    )
    sys.exit(exit_code)
