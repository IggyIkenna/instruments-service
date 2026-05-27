"""Data processing utilities for instruments-service orchestrator.

Extracted from orchestrator.py to reduce file size and improve modularity.
These functions handle data normalization, validation, and transformation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


def canonical_league_id(lid_raw: object) -> str:
    """Extract canonical league ID from various input formats."""
    if isinstance(lid_raw, str):
        return lid_raw.strip()
    if isinstance(lid_raw, (int, float)):
        return str(int(lid_raw))
    if hasattr(lid_raw, "league_id"):
        return str(lid_raw.league_id)
    return str(lid_raw)


def coerce_adapter_output(item: object) -> dict[str, object]:
    """Coerce adapter output to dict format for consistency."""
    if isinstance(item, dict):
        return item
    if hasattr(item, "__dict__"):
        return item.__dict__
    if hasattr(item, "_asdict"):
        return item._asdict()  # type: ignore[reportAttributeAccessIssue]
    return {"raw": item}


def af_id_from_canonical(obj: object) -> int | None:
    """Extract API-Football fixture ID from canonical fixture object."""
    if isinstance(obj, dict):
        af_id = obj.get("af_fixture_id")
        if af_id is not None:
            return int(af_id) if str(af_id).isdigit() else None

    # Try object attributes
    if hasattr(obj, "af_fixture_id"):
        af_id = obj.af_fixture_id  # pyright: ignore[reportAny]
        if af_id is not None:
            return int(af_id) if str(af_id).isdigit() else None

    return None


def normalize_wrapped_token(symbol: str) -> str:
    """Normalize wrapped token symbols to their base form."""
    # WETH → ETH, WBTC → BTC, etc.
    if symbol.startswith("W") and len(symbol) > 1:
        base = symbol[1:]
        # Only strip W if the result is a known base token
        if base in ("ETH", "BTC", "SOL", "AVAX", "MATIC", "BNB"):
            return base
    return symbol


def extract_prediction_canonical_group(row: pd.Series) -> str:
    """Extract canonical group name from prediction market row."""
    # Default to "other" if no group info available
    if "category" in row:
        return str(row["category"]).lower().strip()
    if "group" in row:
        return str(row["group"]).lower().strip()
    return "other"


def compute_prediction_shards(df: pd.DataFrame) -> dict[str, int]:
    """Compute shard counts for prediction markets by group."""
    if df.empty or "group" not in df.columns:
        return {}

    group_counts = df["group"].value_counts().to_dict()
    return {str(k): int(v) for k, v in group_counts.items()}


def count_per_venue(records: Iterable[object]) -> dict[str, int]:
    """Count records per venue for monitoring."""
    counts: dict[str, int] = {}
    for record in records:
        venue = "unknown"
        if isinstance(record, dict):
            venue = str(record.get("venue", "unknown"))
        elif hasattr(record, "venue"):
            venue = str(record.venue)

        counts[venue] = counts.get(venue, 0) + 1

    return counts
