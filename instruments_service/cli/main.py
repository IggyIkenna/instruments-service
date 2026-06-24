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
  --trigger        Live-mode trigger name selector (e.g. ``sports.fixtures.daily_repoll``,
                   ``cefi.instruments.daily_refresh``, ``prediction.markets.discover``).
                   Closed per-asset-group taxonomy lives in UAC (Phase A.6 of
                   ``instruments_master``); until the UAC enum lands the flag
                   accepts any string and the downstream trigger dispatcher (Phase B.1+)
                   validates the name. Pairs with ``--mode live`` — selects which entity-type
                   subset to refresh + which source adapter to invoke. Same single CLI codepath
                   as batch; no new entry-points; live-mode differs only in (a) source adapter
                   pick and (b) lookback window (now-anchored vs historical date).
                   NOTE: DeFi live-mode runs via ``--mode live`` (no ``--trigger``); DeFi
                   on-chain instrument triggers are owned by ``defi_master`` (separate epic),
                   not this service's trigger taxonomy.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

import pandas as pd  # pyright: ignore[reportMissingImports]
from unified_trading_library import ServiceBootstrap, get_write_bucket_name

from instruments_service.cli.instruments_handler import InstrumentsHandler
from instruments_service.config import get_config

_SERVICE_NAME = "instruments-service"  # pragma: no cover


def _run_coverage_status(argv: list[str] | None = None) -> None:  # pragma: no cover
    """Print honest-coverage JSON for an instruments-service bucket.

    Called when ``--operation=status`` is passed. Bypasses the ServiceBootstrap
    date-loop — this is a read-only diagnostic, not a data-fetching operation.

    Output shape (one JSON object to stdout):
        {
          "bucket": "<name>",
          "rows": [
            {"asset_group": "defi", "data_type": "DEX_POOLS",
             "counts": {5 fields...}, "coverage": 0.9831},
            ...
          ]
        }
    """
    from unified_trading_library import (  # pyright: ignore[reportUnknownVariableType]  # noqa: imports-inside-functions
        compute_coverage_for_bucket,
        read_availability_index,
    )

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--asset-group", default=None)
    parser.add_argument("--bucket", default=None)
    args, _ = parser.parse_known_args(argv)

    asset_group: str | None = str(args.asset_group) if args.asset_group is not None else None  # pyright: ignore[reportAny]
    bucket: str = str(args.bucket) if args.bucket else get_write_bucket_name("instruments", asset_group or "defi")  # pyright: ignore[reportAny]

    try:
        index: pd.DataFrame = read_availability_index(bucket)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
    except Exception as exc:
        print(json.dumps({"error": str(exc), "bucket": bucket}), file=sys.stderr)
        sys.exit(1)

    if index.empty or "data_type" not in index.columns:  # pyright: ignore[reportUnknownMemberType]
        print(json.dumps({"bucket": bucket, "rows": []}))
        return

    unique_values: object = index["data_type"].dropna().unique()
    data_types: list[str] = sorted(str(v) for v in unique_values if v is not None)  # pyright: ignore[reportArgumentType,reportUnknownVariableType]
    rows: list[dict[str, object]] = []
    for dt in data_types:
        counts, ratio = compute_coverage_for_bucket(bucket, asset_group=asset_group, data_type=dt)
        rows.append(
            {
                "asset_group": asset_group,
                "data_type": dt,
                "counts": counts._asdict(),
                "coverage": round(ratio, 6),
            }
        )

    print(json.dumps({"bucket": bucket, "rows": rows}, indent=2))


def _run_refresh_league_entity_coverage(argv: list[str] | None = None) -> None:  # pragma: no cover
    """Regenerate the UAC observed (league x per-fixture-entity) coverage map from captured rows.

    Called when ``--operation=refresh-league-entity-coverage`` is passed. Bypasses the
    ServiceBootstrap date-loop — this is a corpus-wide maintenance verb, not a date-fetch.

    The map ``unified_api_contracts/registry/data/sports_league_entity_coverage.json`` is DERIVED
    data: a ``(league, entity)`` pair is *covered* iff >=1 ``captured`` manifest row exists for it
    across history. ``is_league_entity_covered`` reads it to gate enrichment capture so a league
    lacking provider coverage records ``EXPECTED_NO_PROVIDER_COVERAGE`` (honest absence, not
    retried). This verb keeps the registry honest as new (league, entity) cells start producing
    captured rows (the 2014-2026 backfill, promotion/relegation, a league newly gaining coverage).

    Default prints the diff vs the committed map; ``--write`` overwrites the UAC JSON in place
    (then commit it in UAC via quickmerge). READ-ONLY against GCS — never writes the manifest.
    Replaces the retired one-off ``scripts/refresh_sports_league_entity_coverage_2026_06_21.py``.
    """
    from pathlib import Path  # noqa: imports-inside-functions

    import unified_api_contracts.registry.sports_league_entity_coverage as _cov_mod  # noqa: imports-inside-functions
    from unified_api_contracts.registry import (  # noqa: imports-inside-functions
        LEAGUE_ENTITY_COVERAGE_ENTITIES,
    )
    from unified_trading_library import read_availability_index  # noqa: imports-inside-functions

    _default_json = Path(_cov_mod.__file__).parent / "data" / "sports_league_entity_coverage.json"
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--write", action="store_true", help="Overwrite the UAC JSON in place (then commit in UAC).")
    parser.add_argument("--uac-json", default=None, help="Override the UAC coverage JSON path.")
    args, _ = parser.parse_known_args(argv)
    uac_json = Path(str(args.uac_json)) if args.uac_json else _default_json  # pyright: ignore[reportAny]

    bucket: str = get_write_bucket_name("instruments", "sports")
    index: pd.DataFrame = read_availability_index(bucket)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
    if index.empty or "data_type" not in index.columns:  # pyright: ignore[reportUnknownMemberType]
        print(json.dumps({"error": "empty or schema-less _index", "bucket": bucket}), file=sys.stderr)
        sys.exit(1)

    entities = frozenset(str(e).upper() for e in LEAGUE_ENTITY_COVERAGE_ENTITIES)
    dt = index["data_type"].astype("string").str.upper()  # pyright: ignore[reportUnknownMemberType]
    cap = index[(index["capture_status"].astype("string") == "captured") & dt.isin(entities)].copy()  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    cap["e"] = cap["data_type"].astype("string").str.upper()  # pyright: ignore[reportUnknownMemberType]
    cap["l"] = cap["league_id"].astype("string").str.upper()  # pyright: ignore[reportUnknownMemberType]
    cap = cap[cap["l"].notna() & (cap["l"].str.len() > 0)]  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    accum: dict[str, set[str]] = {entity: set() for entity in sorted(entities)}
    for entity, lg in zip(cap["e"].tolist(), cap["l"].tolist(), strict=True):  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType, reportAny]
        accum.setdefault(str(entity), set()).add(str(lg))  # pyright: ignore[reportAny]
    new_map: dict[str, list[str]] = {entity: sorted(leagues) for entity, leagues in sorted(accum.items())}

    pairs = sum(len(v) for v in new_map.values())
    print(f"Observed coverage: {len(new_map)} entities, {pairs} (entity, league) pairs (bucket={bucket})")
    old_map: dict[str, list[str]] = json.loads(uac_json.read_text()) if uac_json.exists() else {}
    added = {e: sorted(set(new_map.get(e, [])) - set(old_map.get(e, []))) for e in new_map}
    added = {e: lg for e, lg in added.items() if lg}
    removed = {e: sorted(set(old_map.get(e, [])) - set(new_map.get(e, []))) for e in old_map}
    removed = {e: lg for e, lg in removed.items() if lg}
    if added:
        print(f"New (entity, league) pairs vs committed map: {json.dumps(added, sort_keys=True)}")
    if removed:
        print(f"Leagues absent from new scan (kept unless --write): {json.dumps(removed, sort_keys=True)}")
    if not added and not removed:
        print("No drift vs committed coverage map.")

    if bool(args.write):  # pyright: ignore[reportAny]
        uac_json.write_text(json.dumps(new_map, indent=2, sort_keys=True) + "\n")
        print(f"Wrote refreshed map -> {uac_json} (commit in UAC via quickmerge).")
    else:
        print("DRY RUN — committed UAC JSON untouched (use --write to overwrite).")


def _add_instruments_extra_args(parser: argparse.ArgumentParser) -> None:  # pragma: no cover
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=(
            "Override the reference-data source for TradFi venues. "
            "Default (unset/'databento') uses Databento; '--source massive' routes "
            "CME/NASDAQ/NYSE/CBOE/ICE/FX to the Massive (Polygon.io-compatible) "
            "adapter and fetches MASSIVE_API_KEY. Same canonical InstrumentRecord "
            "output regardless of source (UAC normalisers)."
        ),
    )
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
            "``--mode live``. Implemented triggers: ``sports.fixtures.daily_repoll``. "
            "Planned (instruments_master Phase C-E): ``cefi.instruments.daily_refresh``, "
            "``sports.lineups.pre_kickoff``, ``prediction.markets.discover``. "
            "NOTE: DeFi live-mode runs via ``--mode live`` (no trigger); DeFi on-chain "
            "instrument triggers are in the defi_master epic, not this service. "
            "The trigger name selects which entity-type subset is refreshed + which source "
            "adapter is invoked; downstream dispatch (Phase B.1+ of instruments_master) validates "
            "the name against the UAC trigger taxonomy. Until the UAC enum lands the flag is a "
            "free-form string — the dispatcher fail-loud-rejects unknown names there, not here."
        ),
    )


def _arg_present(argv: list[str], name: str) -> bool:
    """True if ``--name`` (or ``--name=...``) appears in argv."""
    return any(a == name or a.startswith(name + "=") for a in argv)


def _arg_value(argv: list[str], name: str) -> str | None:
    """Return the value of ``--name value`` or ``--name=value`` in argv, else None."""
    for i, a in enumerate(argv):
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
        if a == name and i + 1 < len(argv):
            return argv[i + 1]
    return None


def _default_recon_dates_to_today() -> None:
    """Self-default ``--start-date``/``--end-date`` to TODAY for the daily recon run.

    The recurring cefi IS fetch is the Cloud Run job
    ``uts-prod-instruments-service-cefi-t1-recon`` (scheduler 06:00 UTC). The
    UTL date-loop framework REQUIRES explicit ``--start-date``/``--end-date``
    and raises ``ValueError: Invalid date format ''`` when the scheduler's empty
    ``httpTarget.body`` injects none — so the recon job had to carry a HARDCODED
    date, which goes stale the next day (re-fetches yesterday's snapshot forever).

    Fix (instruments-service-scoped, no scheduler edit needed): when the run is a
    recon run (``--run-tag=t1-recon``) and BOTH dates are unset, inject TODAY
    (UTC) for both. Instrument reference data is a CURRENT-listing snapshot (the
    Tardis no-auth enumeration / native venue APIs return today's universe), so
    TODAY is the correct anchor — not a T+1 lag. Idempotent: a no-op when either
    date is already supplied (manual backfills keep full control).
    """
    argv = sys.argv[1:]
    if _arg_value(argv, "--run-tag") != "t1-recon":
        return
    if _arg_present(argv, "--start-date") or _arg_present(argv, "--end-date"):
        return
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    sys.argv.extend(["--start-date", today, "--end-date", today])


def main_service_cli() -> None:  # pragma: no cover
    """ServiceBootstrap entry point for instruments-service."""
    # --operation=status is a read-only diagnostic; bypass the date-loop framework.
    _argv = sys.argv[1:]
    if "--operation=status" in _argv or ("--operation" in _argv and _argv[_argv.index("--operation") + 1] == "status"):
        _run_coverage_status(_argv)
        return
    # --operation=refresh-league-entity-coverage regenerates the UAC observed-coverage map from
    # captured rows (recurring successor to the retired one-off refresh script); read-only diagnostic.
    _refresh_op = "refresh-league-entity-coverage"
    if f"--operation={_refresh_op}" in _argv or (
        "--operation" in _argv and _argv[_argv.index("--operation") + 1] == _refresh_op
    ):
        _run_refresh_league_entity_coverage(_argv)
        return
    # Daily recon run self-defaults its date window to TODAY so the scheduled
    # 06:00 UTC job never re-fetches a stale hardcoded date (incident 2026-06-23).
    _default_recon_dates_to_today()
    ServiceBootstrap(
        service_name=_SERVICE_NAME,
        operations={"instruments": InstrumentsHandler},
        config=get_config(),
        add_venues_arg=True,
        extra_args_fn=_add_instruments_extra_args,
    ).run()
