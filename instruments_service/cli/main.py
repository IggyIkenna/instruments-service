"""
Instruments Service CLI Entry Point

Clean CLI entry point following unified repository structure.

Note: Credentials are automatically handled by unified-trading-library
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).

Query functionality has been moved to unified-trading-library.
Use InstrumentsDomainClient from unified-trading-library to query instruments.
"""

import argparse
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
from unified_events_interface import log_event, setup_events
from unified_trading_library import BaseModeHandler, GCSEventSink, GracefulShutdownHandler, ServiceCLI, setup_tracing

logger = logging.getLogger(__name__)

# Global shutdown handler (initialized in main())
_shutdown_handler = None

# CRITICAL: Patch unified_trading_library config to use instruments_config
# This ensures that get_bucket_for_category() uses the correct bucket configuration
# from instruments-service instead of the default BaseServiceConfig
import unified_trading_library.core.market_category as market_category_module

from instruments_service.config import instruments_config

market_category_module.unified_config = instruments_config
logger.info("✅ Patched unified_trading_library config with instruments_config")

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.cli.handlers import get_handler_for_mode
from instruments_service.cli.parser import parse_arguments
from instruments_service.config_reloaders import start_domain_config_reloaders, stop_domain_config_reloaders

# ---------------------------------------------------------------------------
# Pre-parser helpers (used by main_service_cli)
# ---------------------------------------------------------------------------


def _build_pre_parser() -> argparse.ArgumentParser:
    """Build a pre-parser that strips instruments-specific flags before ServiceCLI."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--start-date", dest="start_date", default=None)
    parser.add_argument("--end-date", dest="end_date", default=None)
    parser.add_argument("--CEFI", action="store_true", default=False)
    parser.add_argument("--TRADFI", action="store_true", default=False)
    parser.add_argument("--DEFI", action="store_true", default=False)
    parser.add_argument("--SPORTS", action="store_true", default=False)
    parser.add_argument("--category", nargs="+", default=None)
    parser.add_argument("--tickers", nargs="+", default=None)
    parser.add_argument("--redo-all", dest="redo_all", action="store_true", default=False)
    parser.add_argument("--log-level", dest="log_level", default="INFO")
    return parser


def _resolve_categories(config: dict[str, object]) -> dict[str, object]:
    """Expand a ``category`` list into boolean ``cefi``/``tradfi``/``defi`` keys."""
    result = dict(config)
    categories: list[str] | None = cast(list[str] | None, result.pop("category", None))
    if categories:
        for cat in categories:
            result[cat.lower()] = True
    return result


# ---------------------------------------------------------------------------
# ServiceCLI-style async handler wrappers
# ---------------------------------------------------------------------------


class InstrumentsBatchHandler(BaseModeHandler):
    """Async wrapper for the ``instruments`` mode handler."""

    def validate_config(self) -> bool:
        return bool(self.config.get("project_id"))

    async def run(self) -> dict[str, object]:
        start_date = cast(str | None, self.config.get("start_date"))
        if not start_date:
            raise ValueError("--start-date is required for instruments mode")

        end_date = cast(str, self.config.get("end_date") or start_date)
        cfg: dict[str, object] = {k: v for k, v in self.config.items() if k not in ("start_date", "end_date")}
        handler = get_handler_for_mode("instruments", cfg)
        result = handler.run(
            start_date=start_date,
            end_date=end_date,
            cefi=bool(self.config.get("cefi", False)),
            tradfi=bool(self.config.get("tradfi", False)),
            defi=bool(self.config.get("defi", False)),
            sports=bool(self.config.get("sports", False)),
        )
        return cast(dict[str, object], result)


class AggregateServiceHandler(BaseModeHandler):
    """Async wrapper for the ``aggregate`` mode handler."""

    async def run(self) -> dict[str, object]:
        redo_all = bool(self.config.get("redo_all", False))
        handler = get_handler_for_mode("aggregate", self.config)
        result = handler.run(redo_all=redo_all)
        return cast(dict[str, object], result)


class CorporateActionsServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions`` mode handler."""

    async def run(self) -> dict[str, object]:
        start_date = cast(str | None, self.config.get("start_date"))
        if not start_date:
            raise ValueError("--start-date is required for corporate_actions mode")

        handler = get_handler_for_mode("corporate_actions", self.config)
        result = handler.run(
            start_date=start_date,
            end_date=cast(str | None, self.config.get("end_date")),
            tickers=cast(list[str] | None, self.config.get("tickers")),
            output_format=cast(str | None, self.config.get("output_format")),
            upload_to_gcs=bool(self.config.get("upload_to_gcs", False)),
        )
        return cast(dict[str, object], result)


class CorporateActionsBackfillServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_backfill`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_backfill", self.config)
        result = handler.run(
            tickers=cast(list[str] | None, self.config.get("tickers")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
            max_retries=cast(int | None, self.config.get("max_retries")),
        )
        return cast(dict[str, object], result)


class GenerateDateViewsServiceHandler(BaseModeHandler):
    """Async wrapper for the ``generate_date_views`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("generate_date_views", self.config)
        result = handler.run(
            input_dir=cast(str | None, self.config.get("input_dir")),
            output_dir=cast(str | None, self.config.get("output_dir")),
        )
        return cast(dict[str, object], result)


class CorporateActionsUpdateServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_update`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_update", self.config)
        result = handler.run(
            days_threshold=cast(int | None, self.config.get("days_threshold")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
        )
        return cast(dict[str, object], result)


class CorporateActionsProductionServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_production`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_production", self.config)
        result = handler.run(
            tickers=cast(list[str] | None, self.config.get("tickers")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
            upload_to_gcs=bool(self.config.get("upload_to_gcs", True)),
        )
        return cast(dict[str, object], result)


# ---------------------------------------------------------------------------
# ServiceCLI entry-point
# ---------------------------------------------------------------------------

_SERVICE_HANDLERS: dict[str, type[BaseModeHandler]] = {
    "instruments": InstrumentsBatchHandler,
    "aggregate": AggregateServiceHandler,
    "corporate_actions": CorporateActionsServiceHandler,
    "corporate_actions_backfill": CorporateActionsBackfillServiceHandler,
    "generate_date_views": GenerateDateViewsServiceHandler,
    "corporate_actions_update": CorporateActionsUpdateServiceHandler,
    "corporate_actions_production": CorporateActionsProductionServiceHandler,
}


def main_service_cli() -> None:
    """
    ServiceCLI-based entry point for instruments-service.

    Pre-parses instruments-specific flags, then delegates to ServiceCLI.
    """
    setup_tracing("instruments-service")
    original_argv = sys.argv[:]
    try:
        pre_parser = _build_pre_parser()
        pre_args, remaining = pre_parser.parse_known_args()

        start_date: str | None = cast(str | None, pre_args.start_date)
        end_date: str | None = cast(str | None, pre_args.end_date)
        cefi: bool = cast(bool, pre_args.CEFI)
        tradfi: bool = cast(bool, pre_args.TRADFI)
        defi: bool = cast(bool, pre_args.DEFI)
        sports: bool = cast(bool, pre_args.SPORTS)
        redo_all: bool = cast(bool, pre_args.redo_all)
        category: list[str] | None = cast(list[str] | None, pre_args.category)
        tickers: list[str] | None = cast(list[str] | None, pre_args.tickers)

        config: dict[str, object] = {
            "project_id": instruments_config.gcp_project_id,
            "start_date": start_date,
            "end_date": end_date,
            "cefi": cefi,
            "tradfi": tradfi,
            "defi": defi,
            "sports": sports,
            "redo_all": redo_all,
        }
        if category:
            config = _resolve_categories({**config, "category": category})
        if tickers:
            config["tickers"] = tickers

        sys.argv = [original_argv[0], *remaining]

        cli = ServiceCLI(
            service_name="instruments-service",
            handlers=_SERVICE_HANDLERS,
            config=config,
        )
        cli.run()
    finally:
        sys.argv = original_argv


# ---------------------------------------------------------------------------
# Legacy synchronous CLI
# ---------------------------------------------------------------------------


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
        _sink = GCSEventSink(
            project_id=instruments_config.gcp_project_id,
            bucket=getattr(instruments_config, "events_bucket", f"{instruments_config.gcp_project_id}-events"),
            service_name="instruments-service",
        )
        setup_events(sink=_sink, mode=args.run_mode, service_name="instruments-service")
        setup_tracing("instruments-service")

        start_domain_config_reloaders(instruments_config)

        # Setup logging level (getattr returns Any; cast to int for setLevel)
        log_level_int = cast(int, getattr(logging, args.log_level.upper()))
        logging.getLogger().setLevel(log_level_int)

        logger.info("🚀 Starting %s operation", args.mode)
        if args.start_date:
            logger.info("📅 Date range: %s to %s", args.start_date, args.end_date or args.start_date)

        # Build configuration from arguments
        config: dict[str, object] = {
            "project_id": cast(str | None, args.project_id),
            "sink_bucket": cast(str | None, args.sink_bucket),
            "analytics_dataset": cast(str, args.analytics_dataset),
        }

        # Get handler for mode
        handler: ModeHandler = get_handler_for_mode(cast(str, args.mode), config)
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
                if args.SPORTS:
                    categories.append("SPORTS")

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
                    elif cat.upper() == "SPORTS":
                        handler_kwargs["sports"] = True
            else:
                # Fallback to individual flags
                if args.CEFI:
                    handler_kwargs["cefi"] = True
                if args.TRADFI:
                    handler_kwargs["tradfi"] = True
                if args.DEFI:
                    handler_kwargs["defi"] = True
                if args.SPORTS:
                    handler_kwargs["sports"] = True

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
            logger.info("✅ %s operation completed successfully", args.mode)
        else:
            status = result.get("status", "unknown")
            logger.error(
                "%s operation failed (status=%s). Details: %s",
                args.mode,
                status,
                result.get("error", result.get("message", "no details")),
            )

        return result

    except (ValueError, KeyError, TypeError, IndexError) as e:
        logger.exception("CLI execution failed: %s", e)
        return {"success": False, "status": "error", "error": str(e)}
    finally:
        stop_domain_config_reloaders()


def run_cli() -> dict[str, HandlerResultValue]:
    """Synchronous CLI execution"""
    try:
        result = main()
        return result
    except KeyboardInterrupt:
        logger.info("🛑 Operation cancelled by user")
        return {"success": False, "status": "error", "error": "Cancelled by user"}
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Unexpected error: %s", e)
        return {"success": False, "status": "error", "error": str(e)}


if __name__ == "__main__":
    result = run_cli()
    # STRICT: Only exit 0 when status is explicitly "success"
    # All other states (error, partial, warning, unknown) exit non-zero
    # This prevents silent failures in VM/Cloud Run deployments
    exit_code = 0 if result.get("status") == "success" else 1
    sys.exit(exit_code)
