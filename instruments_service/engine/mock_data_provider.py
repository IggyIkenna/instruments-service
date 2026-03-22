"""Mock data provider for instruments-service.

In mock mode (CLOUD_MOCK_MODE=true), generates instruments using
InstrumentGenerator from unified-internal-contracts instead of hitting
real exchange APIs (Tardis, Databento, DeFi RPCs, USRI).

Reads existing seed data from .local-dev-cache/mock-seed/instruments-service/
if available, otherwise generates inline. Writes output to the same seed
path so downstream services can consume it.
"""

from __future__ import annotations
from unified_trading_library import classify_and_emit_error

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unified_api_contracts import CanonicalInstrument
from unified_internal_contracts.testing.instrument_generator import InstrumentGenerator

from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED_DIR_NAME = "instruments-service"
SEED_SUBDIR = "instrument_availability/by_date"


def _get_workspace_root() -> Path:
    """Resolve workspace root from env or heuristics."""
    import os

    workspace = os.environ.get(
        "WORKSPACE_ROOT",
        os.environ.get("UNIFIED_TRADING_WORKSPACE_ROOT", ""),
    )
    if workspace:
        return Path(workspace)
    # Heuristic: instruments-service repo is at <workspace>/instruments-service
    return Path(__file__).resolve().parents[3]


def _get_seed_base() -> Path:
    """Return the seed data directory for instruments-service."""
    return _get_workspace_root() / ".local-dev-cache" / "mock-seed" / SEED_DIR_NAME


def _seed_exists_for_date(date_str: str) -> bool:
    """Check if seed data exists for a given date."""
    seed_dir = _get_seed_base() / SEED_SUBDIR / f"day={date_str}"
    combined = seed_dir / "all_instruments.parquet"
    if combined.exists():
        return True
    # Check for venue-partitioned files
    venue_files = list(seed_dir.glob("venue=*/instruments.parquet"))
    return len(venue_files) > 0


def load_seed_instruments(date_str: str) -> dict[str, InstrumentDefinition]:
    """Load instruments from local seed data for a given date.

    Returns:
        Dict mapping instrument_key to InstrumentDefinition.
    """
    seed_dir = _get_seed_base() / SEED_SUBDIR / f"day={date_str}"
    combined = seed_dir / "all_instruments.parquet"

    df: pd.DataFrame
    if combined.exists():
        df = pd.read_parquet(combined)
    else:
        # Read venue-partitioned files
        frames: list[pd.DataFrame] = []
        for pq_file in seed_dir.glob("venue=*/instruments.parquet"):
            frames.append(pd.read_parquet(pq_file))
        if not frames:
            return {}
        df = pd.concat(frames, ignore_index=True)

    logger.info("Loaded %d instruments from seed data for %s", len(df), date_str)
    return _dataframe_to_instrument_defs(df)


def generate_mock_instruments(
    date: datetime,
    seed: int = 42,
    scenario_name: str | None = None,
) -> dict[str, InstrumentDefinition]:
    """Generate instruments using InstrumentGenerator (deterministic).

    This is the inline fallback when no seed data exists on disk.
    Uses the same generator as seed_mock_data.py for consistency.

    When scenario_name is provided, applies instrument_overrides from the
    scenario config (expire/delist instruments, inject malformed ones).

    Args:
        date: Target date for instrument generation.
        seed: RNG seed for deterministic output.
        scenario_name: Optional MockScenario name for instrument lifecycle mutations.

    Returns:
        Dict mapping instrument_key to InstrumentDefinition.
    """
    ref_date = date.date() if isinstance(date, datetime) else date
    gen = InstrumentGenerator(seed=seed)

    # Apply scenario instrument_overrides if provided
    if scenario_name:
        from unified_internal_contracts.modes import MockScenario
        from unified_internal_contracts.testing.scenario_config import ScenarioConfig

        cfg = ScenarioConfig.load(MockScenario(scenario_name))
        for override in cfg.instrument_overrides:
            if override.action == "expire":
                gen.expire_instrument(override.instrument_key or override.pattern)
            elif override.action == "delist":
                gen.delete_instrument(override.instrument_key or override.pattern)

    instruments: list[CanonicalInstrument] = gen.generate_all(
        ref_date=ref_date,
        include_options_chain=False,  # Skip options for speed in mock mode
    )

    logger.info(
        "InstrumentGenerator produced %d instruments for %s (mock mode)",
        len(instruments),
        ref_date.isoformat(),
    )

    # Get the set of fields that InstrumentDefinition accepts
    valid_fields = set(InstrumentDefinition.model_fields.keys())

    # Fields that must be str (not None) in InstrumentDefinition
    str_fields = {
        name
        for name, field in InstrumentDefinition.model_fields.items()
        if field.annotation is str
        or (hasattr(field.annotation, "__args__") and str in getattr(field.annotation, "__args__", ()))
    }

    result: dict[str, InstrumentDefinition] = {}
    for inst in instruments:
        inst_dict = cast(dict[str, object], inst.model_dump())
        filtered_dict: dict[str, object] = {}
        for k, v in inst_dict.items():
            if k not in valid_fields:
                continue
            # datetime → ISO string
            if isinstance(v, datetime):
                filtered_dict[k] = v.isoformat()
            # None → "" for str fields, False for bool
            elif v is None and k in str_fields:
                filtered_dict[k] = ""
            elif v is None and k == "inverse":
                filtered_dict[k] = False
            # float/Decimal → str for tick_size, min_size, multiplier
            elif k in ("tick_size", "min_size", "multiplier") and not isinstance(v, str):
                filtered_dict[k] = str(v) if v is not None else ""
            else:
                filtered_dict[k] = v
        try:
            result[inst.instrument_key] = InstrumentDefinition(**filtered_dict)
        except Exception as exc:
            classify_and_emit_error(
                exc,
                service_name="instruments-service",
                operation="mock_instrument_def",
                instrument_key=inst.instrument_key,
            )

    return result


def write_mock_output(
    instruments: dict[str, InstrumentDefinition],
    date: datetime,
) -> bool:
    """Write generated instruments to the local seed path.

    This makes the output available to downstream services
    (market-tick-data-service, etc.) via the standard seed data path.

    Args:
        instruments: Dict of instrument_key -> InstrumentDefinition.
        date: Target date for partition path.

    Returns:
        True on success.
    """
    if not instruments:
        logger.warning("No instruments to write for mock output")
        return True

    date_str = date.strftime("%Y-%m-%d")
    seed_base = _get_seed_base()
    output_dir = seed_base / SEED_SUBDIR / f"day={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert to DataFrame
    rows: list[dict[str, object]] = []
    for inst in instruments.values():
        if hasattr(inst, "model_dump"):
            rows.append(cast(dict[str, object], inst.model_dump()))
        else:
            rows.append(cast(dict[str, object], cast(object, inst)))

    df = pd.DataFrame(rows)

    # Write combined parquet
    combined_path = output_dir / "all_instruments.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(combined_path), compression="snappy")
    logger.info(
        "Wrote %d mock instruments to %s",
        len(df),
        combined_path,
    )

    # Write per-venue partitioned files (matching seed_mock_data.py layout)
    if "venue" in df.columns:
        for venue_name, venue_df in df.groupby("venue"):
            venue_folder = str(venue_name).replace("/", "-").replace("\\", "-")
            venue_dir = output_dir / f"venue={venue_folder}"
            venue_dir.mkdir(parents=True, exist_ok=True)
            venue_path = venue_dir / "instruments.parquet"
            venue_table = pa.Table.from_pandas(
                venue_df.reset_index(drop=True),
                preserve_index=False,
            )
            pq.write_table(venue_table, str(venue_path), compression="snappy")

    # Write .seed-complete marker
    marker_path = seed_base / ".seed-complete"
    marker_data = json.dumps(
        {
            "service": "instruments-service",
            "completed_at": datetime.now(UTC).isoformat(),
            "layer": 1,
            "mock_mode": True,
        }
    )
    marker_path.write_text(marker_data)

    return True


def _dataframe_to_instrument_defs(
    df: pd.DataFrame,
) -> dict[str, InstrumentDefinition]:
    """Convert a DataFrame of instruments to a dict of InstrumentDefinition."""
    valid_fields = set(InstrumentDefinition.model_fields.keys())
    result: dict[str, InstrumentDefinition] = {}
    for _, row in df.iterrows():
        row_dict = cast(dict[str, object], row.to_dict())
        inst_key = str(row_dict.get("instrument_key", ""))
        if inst_key:
            filtered: dict[str, object] = {k: v for k, v in row_dict.items() if k in valid_fields}
            result[inst_key] = InstrumentDefinition(**filtered)
    return result
