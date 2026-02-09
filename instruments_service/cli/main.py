"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).

Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict


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

from instruments_service.cli.base_handler import ModeHandler
from instruments_service.cli.handlers import get_handler_for_mode
from instruments_service.cli.parser import parse_arguments


def validate_startup(config) -> bool:
    """
    Validate cloud connections and buckets before processing.

    CRITICAL: Fail fast if cloud connectivity or bucket access is unavailable.
    This prevents silent failures and wasted compute time.

    Args:
        config: Service config object

    Returns:
        True if validation passes, False otherwise
    """
    try:
        from unified_cloud_services import get_storage_client

        logger.info("🔍 Validating cloud connectivity and bucket access...")

        # Check storage connection
        try:
            project_id = config.gcp_project_id if hasattr(config, "gcp_project_id") else None
            storage_client = get_storage_client(project_id=project_id)
            if storage_client is None:
                logger.error("❌ Failed to initialize storage client")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to connect to cloud storage: {e}")
            return False

        # Check output buckets exist and are accessible
        # Use config to get bucket names for each category
        categories = ["CEFI", "TRADFI", "DEFI"]
        for category in categories:
            try:
                # Get bucket name from config
                bucket_name = config.get_bucket_for_category(category.lower())

                if bucket_name:
                    # Try to list blobs (checks bucket access)
                    try:
                        list(storage_client.list_blobs(bucket=bucket_name, prefix="", max_results=1))
                        logger.info(f"✅ Bucket accessible: {bucket_name}")
                    except Exception as e:
                        logger.error(f"❌ Cannot access bucket {bucket_name}: {e}")
                        return False

            except Exception as e:
                logger.error(f"❌ Error checking bucket for {category}: {e}")
                return False

        logger.info("✅ Startup validation passed - cloud connectivity and bucket access verified")
        return True

    except Exception as e:
        logger.error(f"❌ Startup validation failed: {e}", exc_info=True)
        return False


def main() -> Dict[str, Any]:
    """
    Main CLI entry point for instruments-service.

    Returns:
        Dictionary with operation results
    """
    global _shutdown_handler
    mode_handler = None  # Track mode handler for cleanup on signal

    def cleanup_on_signal():
        """Cleanup function called on SIGTERM/SIGINT.

        Note: During atexit, stdout/stderr may be closed. We suppress logging
        errors by setting logging.raiseExceptions = False, which prevents the
        logging module from printing error messages when streams are closed.
        """
        # Suppress logging error messages during interpreter shutdown
        logging.raiseExceptions = False

        nonlocal mode_handler
        if mode_handler is not None:
            try:
                mode_handler.cleanup()
            except Exception:
                pass  # Suppress all errors during shutdown

    # Initialize graceful shutdown handler (handles SIGTERM/SIGINT)
    _shutdown_handler = GracefulShutdownHandler(cleanup_callback=cleanup_on_signal)

    try:
        # Parse arguments
        args = parse_arguments()

        # Setup logging level
        logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

        # CRITICAL: Validate cloud connectivity and bucket access before processing
        # Skip validation in dry-run mode (no cloud operations)
        if not (hasattr(args, "dry_run") and args.dry_run):
            if not validate_startup(instruments_config):
                logger.error("❌ Startup validation failed - cannot proceed without cloud connectivity")
                sys.exit(1)

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
        if hasattr(args, "force") and args.force:
            handler_kwargs["force"] = args.force
        if hasattr(args, "dry_run") and args.dry_run:
            handler_kwargs["dry_run"] = args.dry_run

        # Concurrency
        if hasattr(args, "max_workers") and args.max_workers:
            handler_kwargs["max_workers"] = args.max_workers

        # Corporate actions specific options
        if hasattr(args, "tickers") and args.tickers:
            handler_kwargs["tickers"] = args.tickers
        if hasattr(args, "output_format") and args.output_format:
            handler_kwargs["output_format"] = args.output_format
        if hasattr(args, "upload_to_gcs") and args.upload_to_gcs:
            handler_kwargs["upload_to_gcs"] = args.upload_to_gcs

        # Backfill/update specific options
        if hasattr(args, "parallel_workers") and args.parallel_workers:
            handler_kwargs["parallel_workers"] = args.parallel_workers
        if hasattr(args, "days_threshold") and args.days_threshold:
            handler_kwargs["days_threshold"] = args.days_threshold
        if hasattr(args, "input_dir") and args.input_dir:
            handler_kwargs["input_dir"] = args.input_dir
        if hasattr(args, "output_dir") and args.output_dir:
            handler_kwargs["output_dir"] = args.output_dir
        if hasattr(args, "max_retries") and args.max_retries:
            handler_kwargs["max_retries"] = args.max_retries

        # Market type filters
        # Priority: --category flag takes precedence, then individual flags
        if hasattr(args, "category") and args.category:
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

        # Venue filter (optional - filter to specific venues within a category)
        if hasattr(args, "venues") and args.venues:
            handler_kwargs["venues"] = args.venues

        # Execute handler
        result = handler.run(**handler_kwargs)

        # Cleanup
        handler.cleanup()

        # Determine success: status must be explicitly "success"
        # "partial", "error", "warning" are all non-success states
        is_success = result.get("status") == "success"

        if is_success:
            logger.info(f"✅ {args.mode} operation completed successfully")
        else:
            status = result.get("status", "unknown")
            logger.error(
                f"❌ {args.mode} operation failed (status={status}). "
                f"Details: {result.get('error', result.get('message', 'no details'))}"
            )

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
    # STRICT: Only exit 0 when status is explicitly "success"
    # All other states (error, partial, warning, unknown) exit non-zero
    # This prevents silent failures in VM/Cloud Run deployments
    exit_code = 0 if result.get("status") == "success" else 1
    sys.exit(exit_code)
