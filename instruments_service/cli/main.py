"""instruments-service CLI entry point.

ServiceBootstrap handles all infrastructure:
  --mode batch  → UTL BatchIO (date range iteration)
  --mode live   → UTL ScheduledIO (wall-clock aligned, live_trigger="scheduled")

Standard args provided by ServiceCLI (no service code needed):
  --mode, --category, --start-date, --end-date, --log-level, --venues

Service adds only its own domain-specific flag: --redo-all.
"""

from __future__ import annotations

import argparse

from unified_trading_library import ServiceBootstrap

from instruments_service.cli.handlers.instruments_handler import InstrumentsHandler
from instruments_service.config import get_config

_SERVICE_NAME = "instruments-service"  # pragma: no cover


def _add_service_args(parser: argparse.ArgumentParser) -> None:  # pragma: no cover
    parser.add_argument(
        "--redo-all",
        action="store_true",
        default=False,
        help="Reprocess dates even if already present in storage",
    )
    # --venues is now a standard ServiceCLI arg — no need to add it here


def main_service_cli() -> None:  # pragma: no cover
    """ServiceBootstrap entry point for instruments-service."""
    ServiceBootstrap(
        service_name=_SERVICE_NAME,
        operations={"instruments": InstrumentsHandler},
        config=get_config(),
        live_trigger="scheduled",  # --mode live → UTL ScheduledIO (15-min wall-clock)
        interval_seconds=900,  # 15-minute intervals
        extra_args_fn=_add_service_args,
    ).run()
