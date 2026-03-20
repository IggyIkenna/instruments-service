#!/usr/bin/env python3
"""Generate mock instrument definitions for local dev / CI mock mode.

instruments-service is Layer 1 (the ROOT) of the mock data dependency chain.
Every downstream service depends on these instrument definitions.

Uses InstrumentGenerator from unified-internal-contracts to produce realistic
instruments across all venues with real venue rules (expiry calendars, strike
intervals, naming conventions, wrapped tokens, pool addresses).

Generates instruments for a 30-day range (one call per day) to capture futures
and options that expire mid-month.  Deduplicates by instrument_key, keeping
the latest occurrence.

Usage:
    python scripts/seed_mock_data.py --scenario normal --seed 42 --env local
    python scripts/seed_mock_data.py --scenario normal --env dev
    python scripts/seed_mock_data.py --date 2025-01-15 --days 30
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Final

import pandas as pd
from unified_api_contracts import CanonicalInstrument
from unified_internal_contracts.modes import MockScenario
from unified_internal_contracts.testing.instrument_generator import InstrumentGenerator
from unified_internal_contracts.testing.scenario_config import ScenarioConfig
from unified_trading_library import CloudWriter, LocalWriter, get_seed_writer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — path templates match what downstream DependencyCheckers expect
# ---------------------------------------------------------------------------

# GCS path template: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
PATH_PREFIX: Final[str] = "instrument_availability/by_date"

DEFAULT_DATE_RANGE_DAYS: Final[int] = 30


# ---------------------------------------------------------------------------
# Categorisation helper
# ---------------------------------------------------------------------------


def _categorize_instrument(inst: CanonicalInstrument) -> str:
    """Return market category for an instrument based on asset_class or venue."""
    ac = inst.asset_class or ""
    if "defi" in ac:
        return "defi"
    if "tradfi" in ac:
        return "tradfi"
    if ac in ("sports", "prediction"):
        return "sports"
    # Fall back to venue-based heuristics
    venue = inst.venue
    defi_venues = {
        "AAVE_V3",
        "AAVE_V3_ETH",
        "COMPOUND_V3_ETH",
        "LIDO",
        "ETHERFI",
        "ETHENA",
        "UNISWAPV3-ETH",
        "UNISWAPV2-ETH",
        "UNISWAPV4-ETH",
        "CURVE-ETH",
        "BALANCER-ETH",
        "AERODROME-BASE",
        "MORPHO-ETHEREUM",
    }
    tradfi_venues = {"CME", "NASDAQ", "NYSE", "ICE", "CBOE"}
    sports_venues = {"PINNACLE", "BETFAIR", "POLYMARKET"}
    if venue in defi_venues:
        return "defi"
    if venue in tradfi_venues:
        return "tradfi"
    if venue in sports_venues:
        return "sports"
    return "cefi"


# ---------------------------------------------------------------------------
# Main generation logic
# ---------------------------------------------------------------------------


def generate_instruments_for_date_range(
    gen: InstrumentGenerator,
    target_date: date,
    days: int,
    include_options_chain: bool = True,
    options_underlying: str = "BTC",
) -> tuple[list[CanonicalInstrument], dict[str, int], dict[str, int]]:
    """Generate instruments across a date range, deduplicating by instrument_key.

    Iterating over multiple days captures futures/options that expire mid-month
    and would be missed by a single-day generation.

    Args:
        gen: InstrumentGenerator instance (seeded for determinism).
        target_date: End date of the range.
        days: Number of days to look back from target_date.
        include_options_chain: Whether to include the full options chain.
        options_underlying: Underlying for the options chain.

    Returns:
        Tuple of (deduplicated instruments, counts by category, counts by venue).
    """
    # Generate for each day in the range
    by_key: dict[str, CanonicalInstrument] = {}
    start_date = target_date - timedelta(days=days - 1)

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        daily_instruments = gen.generate_all(
            ref_date=current_date,
            include_options_chain=include_options_chain,
            options_underlying=options_underlying,
        )
        # Latest occurrence wins (overwrites earlier entries with same key)
        for inst in daily_instruments:
            by_key[inst.instrument_key] = inst

    instruments = list(by_key.values())

    # Count by category and by venue
    category_counts: dict[str, int] = Counter(_categorize_instrument(inst) for inst in instruments)
    venue_counts: dict[str, int] = Counter(inst.venue for inst in instruments)

    log.info(
        "Generated %d unique instruments from %d-day range (%s to %s)",
        len(instruments),
        days,
        start_date.isoformat(),
        target_date.isoformat(),
    )
    return instruments, dict(category_counts), dict(venue_counts)


# ---------------------------------------------------------------------------
# Output writers (reuse existing Parquet layout)
# ---------------------------------------------------------------------------


def write_parquet_by_venue(
    instruments: list[CanonicalInstrument],
    writer: LocalWriter | CloudWriter,
    date_str: str,
) -> list[str]:
    """Write instrument parquet files partitioned by venue (matching GCS layout).

    Path template: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
    """
    # Group instruments by venue
    by_venue: dict[str, list[dict[str, object]]] = {}
    timestamp = datetime.now(UTC)

    for inst in instruments:
        venue = inst.venue
        row = inst.model_dump()
        row["timestamp"] = timestamp
        if venue not in by_venue:
            by_venue[venue] = []
        by_venue[venue].append(row)

    written_paths: list[str] = []

    for venue, rows in sorted(by_venue.items()):
        venue_folder = venue.replace("/", "-").replace("\\", "-")
        relative_path = f"{PATH_PREFIX}/day={date_str}/venue={venue_folder}/instruments.parquet"

        df = pd.DataFrame(rows)
        out = writer.write_parquet(df, relative_path)
        written_paths.append(out)
        log.info("  %s: %d instruments", venue, len(rows))

    return written_paths


def write_combined_parquet(
    instruments: list[CanonicalInstrument],
    writer: LocalWriter | CloudWriter,
    date_str: str,
) -> str:
    """Write a single combined parquet file with all instruments (for downstream convenience)."""
    timestamp = datetime.now(UTC)
    rows: list[dict[str, object]] = []
    for inst in instruments:
        row = inst.model_dump()
        row["timestamp"] = timestamp
        rows.append(row)

    relative_path = f"{PATH_PREFIX}/day={date_str}/all_instruments.parquet"
    df = pd.DataFrame(rows)
    out = writer.write_parquet(df, relative_path)
    log.info("Combined: %d instruments", len(instruments))
    return out


def write_seed_manifest(
    writer: LocalWriter | CloudWriter,
    category_counts: dict[str, int],
    venue_counts: dict[str, int],
    written_paths: list[str],
    scenario_name: str,
    seed: int,
    date_str: str,
    days: int,
    options_chain_size: int,
) -> str:
    """Write a JSON manifest summarising the seed run."""
    manifest: dict[str, object] = {
        "service": "instruments-service",
        "layer": 1,
        "scenario": scenario_name,
        "seed": seed,
        "date": date_str,
        "date_range_days": days,
        "generated_at": datetime.now(UTC).isoformat(),
        "instrument_counts_by_category": category_counts,
        "instrument_counts_by_venue": venue_counts,
        "options_chain_size": options_chain_size,
        "total_instruments": sum(category_counts.values()),
        "files": written_paths,
        "generator": "InstrumentGenerator (unified-internal-contracts)",
    }

    return writer.write_json(manifest, "seed_manifest.json")


def write_seed_complete_marker(writer: LocalWriter | CloudWriter) -> str:
    """Write .seed-complete marker file (used by validate-mock-upstream.sh)."""
    marker_data = json.dumps(
        {
            "service": "instruments-service",
            "completed_at": datetime.now(UTC).isoformat(),
            "layer": 1,
        }
    )
    return writer.write_text(marker_data, ".seed-complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate mock instrument definitions for local dev / CI mock mode.",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="normal",
        choices=[s.value for s in MockScenario],
        help="Named scenario for deterministic mock generation (default: normal)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic output (default: 42, overridden by scenario if set)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="local",
        choices=["local", "dev"],
        help="Target environment: local (filesystem) or dev (would use GCS in future)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(UTC).strftime("%Y-%m-%d"),
        help="Target date for partitioned output (default: today)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DATE_RANGE_DAYS,
        help=f"Number of days to generate (default: {DEFAULT_DATE_RANGE_DAYS})",
    )
    parser.add_argument(
        "--no-options-chain",
        action="store_true",
        help="Skip generating the options chain (faster for testing)",
    )
    parser.add_argument(
        "--options-underlying",
        type=str,
        default="BTC",
        help="Underlying for the options chain (default: BTC)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="Override output directory (default: $WORKSPACE_ROOT/.local-dev-cache/mock-seed/instruments-service)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for seed_mock_data."""
    args = parse_args(argv)

    # Load scenario config
    scenario_enum = MockScenario(args.scenario)
    scenario_cfg = ScenarioConfig.load(scenario_enum)
    effective_seed: int = scenario_cfg.seed if scenario_cfg.seed != 0 else args.seed

    target_date = date.fromisoformat(args.date)
    include_options = not args.no_options_chain

    log.info("=== instruments-service mock data seed ===")
    log.info("Scenario: %s (seed=%d)", scenario_enum.value, effective_seed)
    log.info("Env: %s, Date: %s, Days: %d", args.env, args.date, args.days)
    log.info("Options chain: %s (underlying=%s)", include_options, args.options_underlying)

    # Create seed writer (local filesystem or cloud storage)
    writer = get_seed_writer(args.env, "instruments-service", args.output_dir)
    log.info("Writer: %s", type(writer).__name__)

    # Generate instruments via InstrumentGenerator
    gen = InstrumentGenerator(seed=effective_seed)
    instruments, category_counts, venue_counts = generate_instruments_for_date_range(
        gen=gen,
        target_date=target_date,
        days=args.days,
        include_options_chain=include_options,
        options_underlying=args.options_underlying,
    )

    # Count options for the summary
    options_chain_size = sum(
        1 for inst in instruments if inst.instrument_type is not None and inst.instrument_type.value == "OPTION"
    )

    # Write partitioned parquet files (by venue)
    written = write_parquet_by_venue(instruments, writer, args.date)

    # Write combined parquet (all instruments in one file)
    combined_path = write_combined_parquet(instruments, writer, args.date)
    written.append(combined_path)

    # Write manifest
    write_seed_manifest(
        writer,
        category_counts,
        venue_counts,
        written,
        scenario_enum.value,
        effective_seed,
        args.date,
        args.days,
        options_chain_size,
    )

    # Write .seed-complete marker
    write_seed_complete_marker(writer)

    # Print summary
    total = sum(category_counts.values())
    log.info("=== SEED COMPLETE ===")
    log.info("Total instruments: %d", total)
    log.info("--- By category ---")
    for cat, count in sorted(category_counts.items()):
        log.info("  %-12s %d", cat, count)
    log.info("--- By venue ---")
    for venue, count in sorted(venue_counts.items()):
        log.info("  %-20s %d", venue, count)
    log.info("Options chain size: %d", options_chain_size)
    log.info("Files written: %d parquet files", len(written))

    return 0


if __name__ == "__main__":
    sys.exit(main())
