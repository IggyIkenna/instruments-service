"""CSV dump utility for sampling and debugging instrument data."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from unified_config_interface import UnifiedCloudConfig

logger = logging.getLogger(__name__)

_config = UnifiedCloudConfig()
_CSV_SAMPLE_DIR = getattr(_config, "csv_sample_dir", None) or "/tmp/instruments_csv_samples"
_csv_sampling_raw = getattr(_config, "enable_csv_sampling", None)
_ENABLED = (
    bool(_csv_sampling_raw)
    if isinstance(_csv_sampling_raw, bool)
    else str(_csv_sampling_raw or "false").lower() == "true"
)


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
    except (OSError, PermissionError) as e:
        logger.warning("Failed to write CSV sample %s: %s", filename, e)
