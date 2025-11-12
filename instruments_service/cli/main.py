"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).
"""

import sys
import logging
import os
import json
from pathlib import Path
from typing import Dict, Any

# Load environment variables from .env file (if it exists)
# This must happen BEFORE any other imports that might use environment variables
try:
    from dotenv import load_dotenv

    # Find .env file in instruments-service directory (parent of this file)
    # Path structure: instruments_service/cli/main.py -> instruments_service -> instruments-service -> .env
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        # Use print for early loading (before logging is configured)
        if os.getenv("DEBUG", "").lower() == "true":
            print(f"✅ Loaded environment variables from {env_path}")
except ImportError:
    # python-dotenv not installed, skip loading .env
    pass

# Setup logging (after .env is loaded so LOG_LEVEL can be read from .env)
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
            logger.info(f"📅 Date range: {args.start_date} to {args.end_date or args.start_date}")

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

        # Venue filters (for both generation and query modes)
        if args.venues:
            handler_kwargs["venues"] = args.venues

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

        # Print query results to stdout for instruments-query mode
        if args.mode == "instruments-query":
            output_format = getattr(args, "output_format", "summary")

            if output_format == "json":
                # Print JSON output to stdout
                print(json.dumps(result, indent=2, default=str))
            elif output_format == "csv":
                # CSV is already saved to file, just print file location
                if "results" in result and "csv_file" in result["results"]:
                    print(f"\n📄 CSV file saved: {result['results']['csv_file']}")
                    print(f"   Rows: {result['results'].get('rows', 0)}")
            else:
                # Summary format - print formatted summary
                if "results" in result:
                    results = result["results"]
                    print("\n" + "=" * 70)
                    print("QUERY RESULTS")
                    print("=" * 70)

                    if "instruments_found" in results:
                        print(f"📊 Instruments Found: {results['instruments_found']}")

                    if "venues" in results and results["venues"]:
                        print(
                            f"🏢 Venues ({len(results['venues'])}): {', '.join(results['venues'][:10])}"
                        )
                        if len(results["venues"]) > 10:
                            print(f"   ... and {len(results['venues']) - 10} more")

                    if "instrument_types" in results and results["instrument_types"]:
                        print(
                            f"📈 Instrument Types ({len(results['instrument_types'])}): {', '.join(results['instrument_types'])}"
                        )

                    if "sample_instruments" in results and results["sample_instruments"]:
                        print(f"\n📋 Sample Instruments (first 10):")
                        for inst in results["sample_instruments"][:10]:
                            print(f"   - {inst}")

                    if "total_instruments" in results:
                        print(f"\n📊 Total Instruments: {results['total_instruments']}")

                    print("=" * 70)

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
    exit_code = 0 if result.get("success", False) or result.get("status") == "success" else 1
    sys.exit(exit_code)
