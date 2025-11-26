"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).
"""

import sys
import logging
import json
import os
from pathlib import Path
from typing import Dict, Any

# CRITICAL: Load .env file explicitly before any other imports
def _load_env_early():
    try:
        from dotenv import load_dotenv
        # Find .env file in current directory or project root
        env_path = Path(".env")
        if not env_path.exists():
            # Try parent directory once
            env_path = Path(__file__).parent.parent.parent / ".env"
        
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
    except ImportError:
        pass

_load_env_early()

# Setup basic logging immediately for module-level operations
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from instruments_service.settings import instruments_config

# CRITICAL: Patch unified_cloud_services config to use instruments_config
# This ensures that get_bucket_for_category() uses the correct bucket configuration
# from instruments-service instead of the default BaseServiceConfig
try:
    import unified_cloud_services.core.market_category
    unified_cloud_services.core.market_category.unified_config = instruments_config
    logger.info("✅ Patched unified_cloud_services config with instruments_config")
except ImportError:
    logger.warning("⚠️ Could not patch unified_cloud_services config (module not found)")
except Exception as e:
    logger.warning(f"⚠️ Failed to patch unified_cloud_services config: {e}")

from instruments_service.cli.parser import parse_arguments
from instruments_service.cli.handlers import get_handler_for_mode
from instruments_service.cli.base_handler import ModeHandler

# Logging setup moved to top of file
# logging.basicConfig(...)
# logger = logging.getLogger(__name__)


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
        handler: ModeHandler = get_handler_for_mode(args.mode, config)

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

        # Instrument ID filters (for both generation and query modes)
        if args.instrument_ids:
            handler_kwargs["instrument_ids"] = args.instrument_ids

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
