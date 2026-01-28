"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).

Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import sys
import logging
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

# Setup structured JSON logging for Cloud Run visibility with resource monitoring
from unified_cloud_services import setup_cloud_logging
from unified_cloud_services.core.signal_handler import GracefulShutdownHandler

setup_cloud_logging(
    log_level="INFO",
    json_format=True,
    enable_resource_monitoring=True,  # Log CPU/memory/disk every 30s for crash diagnostics
)
logger = logging.getLogger(__name__)

# Global shutdown handler (initialized in main())
_shutdown_handler = None

from instruments_service.config import instruments_config

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


def main() -> Dict[str, Any]:
    """
    Main CLI entry point for instruments-service.

    Returns:
        Dictionary with operation results
    """
    global _shutdown_handler
    mode_handler = None  # Track mode handler for cleanup on signal
    
    def cleanup_on_signal():
        """Cleanup function called on SIGTERM/SIGINT."""
        nonlocal mode_handler
        if mode_handler is not None:
            try:
                mode_handler.cleanup()
                logger.info("Cleanup completed on signal")
            except Exception as e:
                logger.warning(f"Cleanup error on signal: {e}")
    
    # Initialize graceful shutdown handler (handles SIGTERM/SIGINT)
    _shutdown_handler = GracefulShutdownHandler(cleanup_callback=cleanup_on_signal)
    
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
        mode_handler = handler  # Track for cleanup on signal

        # Prepare arguments for handler
        handler_kwargs = {}

        # Date range
        if args.start_date:
            handler_kwargs["start_date"] = args.start_date
        if args.end_date:
            handler_kwargs["end_date"] = args.end_date

        # Common options
        if hasattr(args, 'force') and args.force:
            handler_kwargs["force"] = args.force
        if hasattr(args, 'dry_run') and args.dry_run:
            handler_kwargs["dry_run"] = args.dry_run

        # Corporate actions specific options
        if hasattr(args, 'tickers') and args.tickers:
            handler_kwargs["tickers"] = args.tickers
        if hasattr(args, 'output_format') and args.output_format:
            handler_kwargs["output_format"] = args.output_format
        if hasattr(args, 'upload_to_gcs') and args.upload_to_gcs:
            handler_kwargs["upload_to_gcs"] = args.upload_to_gcs

        # Market type filters
        # Priority: --category flag takes precedence, then individual flags
        if hasattr(args, 'category') and args.category:
            # --category can be a list (e.g., --category CEFI TRADFI)
            for cat in args.category:
                if cat.upper() == "CEFI":
                    handler_kwargs["cefi"] = True
                elif cat.upper() == "TRADFI":
                    handler_kwargs["tradfi"] = True
                elif cat.upper() == "DEFI":
                    handler_kwargs["defi"] = True
        else:
            # Fallback to individual flags
            if args.CEFI:
                handler_kwargs["cefi"] = True
            if args.TRADFI:
                handler_kwargs["tradfi"] = True
            if args.DEFI:
                handler_kwargs["defi"] = True

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
    exit_code = 0 if result.get("success", False) or result.get("status") == "success" else 1
    sys.exit(exit_code)
