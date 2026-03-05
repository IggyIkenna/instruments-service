"""
Corporate Actions Utilities

Shared utility functions for corporate actions handlers.
"""

import logging
from datetime import date
from typing import cast

import pandas as pd

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


def get_tradfi_bucket() -> str:
    """Get TRADFI bucket name from config."""
    return instruments_config.get_bucket_for_category("tradfi")


def get_tickers_from_gcs(
    project_id: str,
    reference_date: date | None = None,
) -> list[str]:
    """
    Fetch equity tickers from GCS instruments store.

    Reads TRADFI instrument definitions and extracts exchange_raw_symbol
    for NYSE and NASDAQ venues (equities).

    Args:
        project_id: GCP project ID
        reference_date: Date to use for instrument lookup (default: finds file with equities)

    Returns:
        List of ticker symbols (e.g., ['AAPL', 'MSFT', ...])
    """
    from unified_trading_library import CloudTarget, StandardizedDomainCloudService

    try:
        bucket_name = instruments_config.gcs_bucket_tradfi or get_tradfi_bucket()

        target = CloudTarget(
            project_id=project_id,
            gcs_bucket=bucket_name,
            bigquery_dataset=instruments_config.bigquery_dataset,
        )
        service = StandardizedDomainCloudService(domain="instruments", cloud_target=target)

        def try_load_tickers(gcs_path: str) -> list[str]:
            """Try to load tickers from a specific GCS path."""
            try:
                raw = service.download_from_gcs(gcs_path=gcs_path, format="parquet", log_errors=False)
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
            except (OSError, FileNotFoundError, RuntimeError, ValueError):
                return []

        if reference_date is not None:
            gcs_path = f"instrument_availability/by_date/day={reference_date}/instruments.parquet"
            tickers = try_load_tickers(gcs_path)
            if tickers:
                logger.info("📂 Using instruments from: day=%s", reference_date)
                logger.info("📊 Loaded %s equity tickers from GCS", len(tickers))
                return tickers
            logger.warning("⚠️ No equities found for %s", reference_date)

        known_good_dates = [
            "2024-07-01",
            "2024-06-01",
            "2024-05-01",
            "2023-05-23",
        ]

        for date_str in known_good_dates:
            gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"
            tickers = try_load_tickers(gcs_path)
            if tickers:
                logger.info("📂 Using instruments from: day=%s", date_str)
                logger.info("📊 Loaded %s equity tickers from GCS", len(tickers))
                return tickers

        logger.warning("⚠️ No equity tickers found in any GCS instruments file")
        return []

    except (OSError, ValueError, TypeError, KeyError) as e:
        logger.error("❌ Failed to load tickers from GCS: %s", e)
        return []
