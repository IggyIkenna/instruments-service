"""
Corporate Actions Utilities

Shared utility functions for corporate actions handlers.
"""

import logging
from datetime import date
from typing import cast

import pandas as pd
from unified_cloud_interface import DataSource, get_data_source

logger = logging.getLogger(__name__)


def get_tickers_from_gcs(
    project_id: str,
    reference_date: date | None = None,
) -> list[str]:
    """
    Fetch equity tickers from TRADFI instruments store via UCI DataSource.

    Reads TRADFI instrument definitions and extracts exchange_raw_symbol
    for NYSE and NASDAQ venues (equities).

    Args:
        project_id: Ignored (kept for API compatibility; routing configured via UCI env vars)
        reference_date: Date to use for instrument lookup (default: finds file with equities)

    Returns:
        List of ticker symbols (e.g., ['AAPL', 'MSFT', ...])
    """
    try:
        data_source: DataSource = get_data_source(
            routing_key="tradfi", prefix="instrument_availability/by_date"
        )

        def try_load_tickers(date_str: str) -> list[str]:
            """Try to load tickers from a specific date partition."""
            try:
                raw = data_source.read(partition={"day": date_str}, format="parquet")
                if not isinstance(raw, pd.DataFrame):
                    return []
                df = cast(pd.DataFrame, raw)
                if df.empty:
                    return []

                venue_series: pd.Series = df["venue"]
                equities: pd.DataFrame = df[venue_series.isin(["NYSE", "NASDAQ"])]
                symbol_arr = equities["exchange_raw_symbol"].dropna().unique()
                symbol_list: list[str | int | float] = cast(list[str | int | float], symbol_arr.tolist())
                tickers_raw: list[str] = [str(s).strip() for s in symbol_list if s is not None and str(s).strip()]
                tickers = [t.strip() for t in tickers_raw if t and t.strip()]
                return sorted(tickers)
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                return []

        if reference_date is not None:
            tickers = try_load_tickers(str(reference_date))
            if tickers:
                logger.info("Using instruments from: day=%s", reference_date)
                logger.info("Loaded %s equity tickers from storage", len(tickers))
                return tickers
            logger.warning("No equities found for %s", reference_date)

        known_good_dates = [
            "2024-07-01",
            "2024-06-01",
            "2024-05-01",
            "2023-05-23",
        ]

        for date_str in known_good_dates:
            tickers = try_load_tickers(date_str)
            if tickers:
                logger.info("Using instruments from: day=%s", date_str)
                logger.info("Loaded %s equity tickers from storage", len(tickers))
                return tickers

        logger.warning("No equity tickers found in any instruments partition")
        return []

    except (OSError, ValueError, TypeError, KeyError) as e:
        logger.error("Failed to load tickers from storage: %s", e)
        return []
