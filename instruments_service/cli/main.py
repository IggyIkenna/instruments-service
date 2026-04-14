"""instruments-service CLI entry point.

ServiceBootstrap handles all infrastructure:
  --mode batch  → UTL BatchIO (date range iteration)
  --mode live   → UTL ScheduledIO (wall-clock aligned, live_trigger="scheduled")

Standard args provided by ServiceCLI (no service code needed):
  --mode, --category, --start-date, --end-date, --log-level, --venues, --force

Custom args (registered via extra_args_fn):
  --sports-entity  Restrict to a single sports manifest entity (e.g. API_FOOTBALL_INJURIES).
                   Used for entity-scoped parallel VMs where one VM handles one entity type.
"""

from __future__ import annotations

import argparse

from unified_trading_library import ServiceBootstrap

from instruments_service.cli.instruments_handler import InstrumentsHandler
from instruments_service.config import get_config

_SERVICE_NAME = "instruments-service"  # pragma: no cover


def _add_instruments_extra_args(parser: argparse.ArgumentParser) -> None:  # pragma: no cover
    parser.add_argument(
        "--sports-entity",
        type=str,
        default=None,
        help=(
            "Restrict this run to a single sports manifest entity "
            "(e.g. INJURIES, FIXTURE_STATS, XG, PREDICTIONS). "
            "Used for entity-scoped parallel VMs."
        ),
    )
    parser.add_argument(
        "--league",
        type=str,
        default=None,
        help=(
            "Comma-separated list of canonical league IDs to process "
            "(e.g. EPL,BUNDESLIGA). Default: all prediction leagues."
        ),
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help=(
            "Override season year for Transfermarkt squad data fetch "
            "(e.g. --season 2022). Default: current year. "
            "Used for historical backfill of player values."
        ),
    )


def main_service_cli() -> None:  # pragma: no cover
    """ServiceBootstrap entry point for instruments-service."""
    ServiceBootstrap(
        service_name=_SERVICE_NAME,
        operations={"instruments": InstrumentsHandler},
        config=get_config(),
        extra_args_fn=_add_instruments_extra_args,
    ).run()
