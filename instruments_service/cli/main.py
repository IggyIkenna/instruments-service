"""instruments-service CLI entry point.

ServiceBootstrap handles all infrastructure:
  --mode batch  → UTL BatchIO (date range iteration)
  --mode live   → UTL ScheduledIO (wall-clock aligned, live_trigger="scheduled")

Standard args provided by ServiceCLI (no service code needed):
  --mode, --category, --start-date, --end-date, --log-level, --venues, --force

Custom args (registered via extra_args_fn):
  --sports-entity  Restrict to a single sports manifest entity (e.g. API_FOOTBALL_INJURIES).
                   Used for entity-scoped parallel VMs where one VM handles one entity type.
  --lookback-days / --lookahead-days / --force-window
                   Rolling-window forward-poll shape (SSOT: codex/02-data/
                   sports-scheduling-and-sharding.md §4). Resolved to
                   --start-date / --end-date / --force before ServiceCLI parses
                   argv; see ``rolling_window.resolve_rolling_window_args``.
"""

from __future__ import annotations

import argparse
import sys

from unified_trading_library import ServiceBootstrap

from instruments_service.cli.instruments_handler import InstrumentsHandler
from instruments_service.cli.rolling_window import resolve_rolling_window_args
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
    parser.add_argument(
        "--sports-provider",
        type=str,
        default=None,
        help=(
            "Restrict SPORTS runs to a single data provider. "
            "Prevents wasting API credits on other providers. "
            "Values: API_FOOTBALL, API_FOOTBALL_ENRICHMENT, OPEN_METEO, "
            "TRANSFERMARKT, SOCCER_FOOTBALL_INFO, UNDERSTAT, FOOTYSTATS. "
            "Used for per-provider VMs."
        ),
    )
    parser.add_argument(
        "--run-tag",
        type=str,
        default="batch",
        help=(
            "GCS output prefix tag (default: batch; use 'live' for live partition, 't1-recon' for T+1 reconciliation)"
        ),
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help=(
            "Rolling forward-poll lookback (days before today, UTC). Resolved to "
            "--start-date=today-N before argparse. Mutually exclusive with "
            "--start-date/--end-date. See codex/02-data/"
            "sports-scheduling-and-sharding.md §4."
        ),
    )
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=None,
        help=(
            "Rolling forward-poll lookahead (days after today, UTC). Resolved to "
            "--end-date=today+M before argparse. Mutually exclusive with "
            "--start-date/--end-date."
        ),
    )
    parser.add_argument(
        "--force-window",
        action="store_true",
        default=False,
        help=(
            "Force-overwrite every date in the resolved window: disables the "
            "orchestrator's skip-if-exists freshness check (equivalent to "
            "--force, propagates as redo_all=True). Use with rolling flags for "
            "forward-poll contracts that mandate re-fetch (§4)."
        ),
    )


def main_service_cli() -> None:  # pragma: no cover
    """ServiceBootstrap entry point for instruments-service.

    Pre-resolves rolling-window flags on sys.argv so UTL's ``_Adapter`` sees
    explicit --start-date / --end-date when it builds BatchIO.
    """
    sys.argv = [sys.argv[0], *resolve_rolling_window_args(sys.argv[1:])]
    ServiceBootstrap(
        service_name=_SERVICE_NAME,
        operations={"instruments": InstrumentsHandler},
        config=get_config(),
        extra_args_fn=_add_instruments_extra_args,
    ).run()
