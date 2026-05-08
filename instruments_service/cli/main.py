"""instruments-service CLI entry point.

ServiceBootstrap handles all infrastructure:
  --mode batch  → UTL BatchIO (date range iteration)
  --mode live   → UTL ScheduledIO (wall-clock aligned, live_trigger="scheduled")

Standard args provided by ServiceCLI (no service code needed):
  --mode, --asset-group, --start-date, --end-date, --log-level, --venues, --force,
  --lookback-days, --lookahead-days, --force-window
    (rolling-window flags are registered by UTL's ServiceCLI when
    add_date_args=True; SSOT for resolution logic is
    unified_trading_library/service_framework/rolling_window.py, contract in
    codex/02-data/sports-scheduling-and-sharding.md §4.)

Custom args (registered via extra_args_fn):
  --sports-entity  Restrict to a single sports manifest entity (e.g. API_FOOTBALL_INJURIES).
                   Used for entity-scoped parallel VMs where one VM handles one entity type.
  --trigger        Live-mode trigger name selector (e.g. ``cefi.instruments.daily_refresh``,
                   ``defi.token_lists.refresh``, ``sports.fixtures.daily_repoll``). Closed
                   per-asset-group taxonomy lives in UAC (Phase A.6 of
                   ``instruments_live_master_2026_05_08``); until the UAC enum lands the flag
                   accepts any string and the downstream trigger dispatcher (Phase B.1+)
                   validates the name. Pairs with ``--mode live`` — selects which entity-type
                   subset to refresh + which source adapter to invoke. Same single CLI codepath
                   as batch; no new entry-points; live-mode differs only in (a) source adapter
                   pick and (b) lookback window (now-anchored vs historical date).
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
        "--recovery-fixture-ids",
        type=str,
        default=None,
        help=(
            "Path to a parquet of (canonical_league_id, af_fixture_id, ...) rows describing the fixture-set "
            "we want to recover per-fixture entities for. Accepts ``gs://...`` or a local path. When set, "
            "the per-fixture entity loops (PLAYER_STATS / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS) "
            "filter fixture_ids to this allowlist BEFORE calling api_football, and the per-league parquet "
            "writes do read-modify-write merges so existing fixtures' rows are preserved. Use this for "
            "targeted recovery work (e.g. fix only the 39k fixtures from Phase 1's truth-set audit). "
            "Bypasses the date-level pre-flight skip so already-captured (date, league) cells are still "
            "drilled into for our specific fixture_ids."
        ),
    )
    parser.add_argument(
        "--trigger",
        type=str,
        default=None,
        help=(
            "Live-mode trigger name (closed-set per-asset-group, UAC-defined). Pairs with "
            "``--mode live``. Examples: ``cefi.instruments.daily_refresh``, "
            "``defi.token_lists.refresh``, ``sports.fixtures.daily_repoll``, "
            "``sports.lineups.pre_kickoff``, ``prediction.markets.discover``. The trigger name "
            "selects which entity-type subset is refreshed + which source adapter is invoked; "
            "downstream dispatch (Phase B.1+ of instruments_live_master_2026_05_08) validates "
            "the name against the UAC trigger taxonomy. Until the UAC enum lands the flag is a "
            "free-form string — the dispatcher fail-loud-rejects unknown names there, not here."
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
