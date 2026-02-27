"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-cloud-services
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).

Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import contextlib
import logging
import sys
from pathlib import Path
from typing import cast


# CRITICAL: Load .env file explicitly before any other imports
def _load_env_early():
    from dotenv import load_dotenv

    env_path = Path(".env")
    if not env_path.exists():
        env_path = Path(__file__).parent.parent.parent / ".env"

    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)


_load_env_early()

# Setup structured JSON logging (split libraries - direct import per dependency matrix)
from unified_cloud_services import GCSEventSink, GracefulShutdownHandler
from unified_events_interface import log_event, setup_events

logger = logging.getLogger(__name__)

# Global shutdown handler (initialized in main())
_shutdown_handler = None

# CRITICAL: Patch unified_cloud_services config to use instruments_config
# This ensures that get_bucket_for_category() uses the correct bucket configuration
# from instruments-service instead of the default BaseServiceConfig
import unified_cloud_services.core.market_category as market_category_module

from instruments_service.config import instruments_config

market_category_module.unified_config = instruments_config
logger.info("✅ Patched unified_cloud_services config with instruments_config")

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.cli.handlers import get_handler_for_mode
from instruments_service.cli.parser import parse_arguments


def main() -> dict[str, HandlerResultValue]:
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
            with contextlib.suppress(RuntimeError, OSError, ValueError):
                mode_handler.cleanup()

    # Initialize graceful shutdown handler (handles SIGTERM/SIGINT)
    _shutdown_handler = GracefulShutdownHandler(cleanup_callback=cleanup_on_signal)

    try:
        # Parse arguments
        args = parse_arguments()

        # Setup events with GCSEventSink now that we have config
        setup_events(
            mode="batch",
            service_name="instruments-service",
            sink=GCSEventSink(
                project_id=instruments_config.gcp_project_id,
                bucket=getattr(instruments_config, "events_bucket", f"{instruments_config.gcp_project_id}-events"),
                service_name="instruments-service",
            ),
        )

        # Setup logging level (getattr returns Any; cast to int for setLevel)
        log_level_int = cast(int, getattr(logging, args.log_level.upper()))
        logging.getLogger().setLevel(log_level_int)

        logger.info(f"🚀 Starting {args.mode} operation")
        if args.start_date:
            logger.info(f"📅 Date range: {args.start_date} to {args.end_date or args.start_date}")

        # Build configuration from arguments
        config: dict[str, str | None] = {
            "project_id": args.project_id,
            "gcs_bucket": args.gcs_bucket,
            "bigquery_dataset": args.bigquery_dataset,
        }

        # Get handler for mode
        handler: ModeHandler = get_handler_for_mode(args.mode, config)
        mode_handler = handler  # Track for cleanup on signal

        # Prepare arguments for handler (typed for HandlerResultValue alignment)
        handler_kwargs: dict[str, HandlerResultValue] = {}

        # Date range
        if args.start_date:
            handler_kwargs["start_date"] = args.start_date
        if args.end_date:
            handler_kwargs["end_date"] = args.end_date

        # Aggregate mode options
        if hasattr(args, "redo_all") and args.redo_all:
            handler_kwargs["redo_all"] = args.redo_all

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

        # Live mode options
        if hasattr(args, "interval") and args.interval:
            handler_kwargs["interval"] = args.interval

        # Market type filters
        if args.mode == "live":
            # Live mode: convert flags to category list
            categories: list[str] = []
            if hasattr(args, "category") and args.category:
                categories = [cat.upper() for cat in args.category]
            else:
                # Use individual flags
                if args.CEFI:
                    categories.append("CEFI")
                if args.TRADFI:
                    categories.append("TRADFI")
                if args.DEFI:
                    categories.append("DEFI")

            if categories:
                handler_kwargs["category"] = categories
        else:
            # Batch mode: keep boolean flags
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
        log_event("PROCESSING_COMPLETED", details={"mode": args.mode})

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


def run_cli() -> dict[str, HandlerResultValue]:
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
