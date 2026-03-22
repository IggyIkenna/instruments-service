"""
Instruments Service CLI Entry Point

Uses ServiceBootstrap from unified-trading-library for infrastructure
boilerplate. Domain-specific handlers delegate to mode-specific processors.

Note: Credentials are automatically handled by unified-trading-library
based on ENVIRONMENT variable (dev mode: auto-detects, production: VM service account).

Query functionality has been moved to unified-trading-library.
Use InstrumentsDomainClient from unified-trading-library to query instruments.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import cast

from dotenv import load_dotenv

# CRITICAL: Load .env before library imports so env vars are available at import time
_env_path = Path(".env")
if not _env_path.exists():
    _env_path = Path(__file__).parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(dotenv_path=_env_path, override=False)  # Shell env vars win over .env defaults

from unified_trading_library import (
    BaseModeHandler,
    ServiceBootstrap,
)

logger = logging.getLogger(__name__)

_SERVICE_NAME = "instruments-service"

# CRITICAL: Patch unified_trading_library config to use instruments_config
# This ensures that get_bucket_for_category() uses the correct bucket configuration
# from instruments-service instead of the default BaseServiceConfig
import unified_trading_library.core.market_category as market_category_module

from instruments_service.config import instruments_config

market_category_module.unified_config = instruments_config
logger.info("Patched unified_trading_library config with instruments_config")

from instruments_service.cli.handlers import get_handler_for_mode

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
    parser.add_argument("--interval", type=int, default=15)
    return parser


_VALID_CATEGORIES = frozenset({"CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION", "ONCHAIN_PERPS"})


def _resolve_categories(config: dict[str, object]) -> dict[str, object]:
    """Expand a ``category`` list into boolean ``cefi``/``tradfi``/``defi`` keys.

    Validates that all category values are known. Raises ValueError for unknown
    categories to prevent silent fallthrough (Issue #8).
    """
    result = dict(config)
    categories: list[str] | None = cast(list[str] | None, result.pop("category", None))
    if categories:
        for cat in categories:
            upper = cat.upper()
            if upper not in _VALID_CATEGORIES:
                msg = f"Unknown category '{cat}'. Valid: {sorted(_VALID_CATEGORIES)}"
                raise ValueError(msg)
            result[upper.lower()] = True
    else:
        # No category specified = process all known categories (explicit)
        for cat in ("cefi", "tradfi", "defi"):
            result[cat] = True
    return result


# ---------------------------------------------------------------------------
# ServiceBootstrap async handler wrappers
# ---------------------------------------------------------------------------


async def _run_sync_handler_in_thread(fn: object, **kwargs: object) -> dict[str, object]:
    """Run a sync handler.run() in a thread so it can use asyncio.run() internally.

    The inner handlers (instrument_handler, live_mode_handler) call asyncio.run()
    for async operations. When ServiceCLI already wraps us in asyncio.run(),
    nesting fails. Running in a thread gives the inner code its own event loop.
    """
    import asyncio
    import functools

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, functools.partial(fn, **kwargs))
    return cast(dict[str, object], result)


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
        result = await _run_sync_handler_in_thread(
            handler.run,
            start_date=start_date,
            end_date=end_date,
            cefi=bool(self.config.get("cefi", False)),
            tradfi=bool(self.config.get("tradfi", False)),
            defi=bool(self.config.get("defi", False)),
            sports=bool(self.config.get("sports", False)),
            prediction=bool(self.config.get("prediction", False)),
        )
        return cast(dict[str, object], result)


class AggregateServiceHandler(BaseModeHandler):
    """Async wrapper for the ``aggregate`` mode handler."""

    async def run(self) -> dict[str, object]:
        redo_all = bool(self.config.get("redo_all", False))
        handler = get_handler_for_mode("aggregate", self.config)
        result = await _run_sync_handler_in_thread(handler.run, redo_all=redo_all)
        return cast(dict[str, object], result)


class CorporateActionsServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions`` mode handler."""

    async def run(self) -> dict[str, object]:
        start_date = cast(str | None, self.config.get("start_date"))
        if not start_date:
            raise ValueError("--start-date is required for corporate_actions mode")

        handler = get_handler_for_mode("corporate_actions", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            start_date=start_date,
            end_date=cast(str | None, self.config.get("end_date")),
            tickers=cast(list[str] | None, self.config.get("tickers")),
            output_format=cast(str | None, self.config.get("output_format")),
            upload_to_storage=bool(self.config.get("upload_to_storage", False)),
        )
        return cast(dict[str, object], result)


class CorporateActionsBackfillServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_backfill`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_backfill", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            tickers=cast(list[str] | None, self.config.get("tickers")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
            max_retries=cast(int | None, self.config.get("max_retries")),
        )
        return cast(dict[str, object], result)


class GenerateDateViewsServiceHandler(BaseModeHandler):
    """Async wrapper for the ``generate_date_views`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("generate_date_views", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            input_dir=cast(str | None, self.config.get("input_dir")),
            output_dir=cast(str | None, self.config.get("output_dir")),
        )
        return cast(dict[str, object], result)


class CorporateActionsUpdateServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_update`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_update", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            days_threshold=cast(int | None, self.config.get("days_threshold")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
        )
        return cast(dict[str, object], result)


class CorporateActionsProductionServiceHandler(BaseModeHandler):
    """Async wrapper for the ``corporate_actions_production`` mode handler."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("corporate_actions_production", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            tickers=cast(list[str] | None, self.config.get("tickers")),
            parallel_workers=cast(int | None, self.config.get("parallel_workers")),
            upload_to_storage=bool(self.config.get("upload_to_storage", True)),
        )
        return cast(dict[str, object], result)


class InstrumentsLiveHandler(BaseModeHandler):
    """Async wrapper for the ``live`` mode handler (instruments live processing)."""

    async def run(self) -> dict[str, object]:
        handler = get_handler_for_mode("live", self.config)
        result = await _run_sync_handler_in_thread(
            handler.run,
            interval=cast(int, self.config.get("interval", 15)),
            category=cast(list[str] | None, self.config.get("category")),
            venues=cast(list[str] | None, self.config.get("venues")),
        )
        return cast(dict[str, object], result)


# ---------------------------------------------------------------------------
# ServiceCLI entry-point (ServiceBootstrap)
# ---------------------------------------------------------------------------

_SERVICE_HANDLERS: dict[str, type[BaseModeHandler]] = {
    "instruments": InstrumentsBatchHandler,
    "live": InstrumentsLiveHandler,
    "aggregate": AggregateServiceHandler,
    "corporate_actions": CorporateActionsServiceHandler,
    "corporate_actions_backfill": CorporateActionsBackfillServiceHandler,
    "generate_date_views": GenerateDateViewsServiceHandler,
    "corporate_actions_update": CorporateActionsUpdateServiceHandler,
    "corporate_actions_production": CorporateActionsProductionServiceHandler,
}


def main_service_cli() -> None:
    """ServiceBootstrap entry point for instruments-service.

    SERVICE_EVENT: STARTED
    SERVICE_EVENT: STOPPED
    SERVICE_EVENT: FAILED

    Pre-parses instruments-specific flags, then delegates to ServiceBootstrap.
    """
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
        interval: int = cast(int, pre_args.interval)

        config: dict[str, object] = {
            "project_id": instruments_config.gcp_project_id,
            "start_date": start_date,
            "end_date": end_date,
            "cefi": cefi,
            "tradfi": tradfi,
            "defi": defi,
            "sports": sports,
            "redo_all": redo_all,
            "interval": interval,
        }
        if category:
            config = _resolve_categories({**config, "category": category})
        if tickers:
            config["tickers"] = tickers

        sys.argv = [original_argv[0], *remaining]

        ServiceBootstrap(
            service_name=_SERVICE_NAME,
            operations=_SERVICE_HANDLERS,
            config=config,
        ).run()
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    main_service_cli()
