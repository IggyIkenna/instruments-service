"""CSV dump utility for sampling and debugging instrument data."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_CSV_SAMPLE_DIR = os.getenv("CSV_SAMPLE_DIR", "/tmp/instruments_csv_samples")
_ENABLED = os.getenv("ENABLE_CSV_SAMPLING", "false").lower() == "true"


def dump_to_csv(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame to CSV for sampling/debugging when ENABLE_CSV_SAMPLING=true."""
    if not _ENABLED or df.empty:
        return
    try:
        sample_dir = Path(_CSV_SAMPLE_DIR)
        sample_dir.mkdir(parents=True, exist_ok=True)
        output_path = sample_dir / filename
        df.to_csv(output_path, index=False)
        logger.debug("CSV sample written: %s", output_path)
    except Exception as e:
        logger.warning("Failed to write CSV sample %s: %s", filename, e)
