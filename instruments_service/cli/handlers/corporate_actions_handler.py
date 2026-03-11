"""
Corporate Actions Handler

CLI handler for fetching and storing corporate actions data (TRADFI only).
Supports dividends, stock splits, and earnings dates for US equities.

Note: Corporate actions are TRADFI-only. Crypto and DeFi do not have
traditional corporate actions like dividends or stock splits.

Tickers are sourced from GCS instruments store (instruments-store-tradfi),
ensuring the same universe as instrument definitions.

Output Structure:
    corporate_actions/by_date/day={date}/dividends.parquet
    corporate_actions/by_date/day={date}/splits.parquet
    corporate_actions/by_date/day={date}/earnings.parquet
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import cast

import pandas as pd
from unified_cloud_interface import DataSink, DataSource, get_data_sink, get_data_source

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import instruments_config
from instruments_service.corporate_actions.adapter import CorporateActionsAdapter
from instruments_service.corporate_actions.models import CorporateActionsBundle

logger = logging.getLogger(__name__)

# =============================================================================
# Schema Definitions
# =============================================================================
# Centralized column schemas for corporate actions output files.
# Update these if the schema changes - they're used for filtering and
# ensuring consistent output structure (including empty files).

DIVIDENDS_SCHEMA: list[str] = [
    "ticker",
    "ex_date",
    "pay_date",
    "record_date",
    "declaration_date",
    "amount",
    "dividend_type",
    "currency",
    "source",
    "fetched_at",
    "instrument_key",
]

SPLITS_SCHEMA: list[str] = [
    "ticker",
    "effective_date",
    "ratio",
    "split_from",
    "split_to",
    "is_reverse_split",
    "adjustment_factor",
    "source",
    "fetched_at",
    "instrument_key",
]

EARNINGS_SCHEMA: list[str] = [
    "ticker",
    "earnings_date",
    "earnings_time",
    "fiscal_quarter",
    "fiscal_year",
    "reported_eps",
    "estimated_eps",
    "surprise_pct",
    "revenue",
    "estimated_revenue",
    "source",
    "fetched_at",
    "instrument_key",
]


def parse_date(date_str: str) -> date:
    """Parse date string to date object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD format.") from e


class CorporateActionsHandler(ModeHandler):
    """
    Handler for fetching corporate actions (dividends, splits, earnings).

    TRADFI-only: Corporate actions apply to equities. Crypto and DeFi
    do not have traditional corporate actions.

    Tickers are sourced from GCS instruments store to ensure same universe
    as instrument definitions. Requires --mode instruments to be run first.

    Data is fetched from yfinance (free, no API key required).
    Results are saved to Parquet files locally and optionally to GCS.
    """

    def __init__(self, config: dict[str, object]) -> None:
        super().__init__(config)

        raw_project_id = config.get("project_id")
        self.project_id: str = (
            str(raw_project_id)
            if raw_project_id is not None
            else str(getattr(instruments_config, "gcp_project_id", "") or "")
        )

        # Initialize adapter (yfinance only - free, no API key needed)
        self.adapter = CorporateActionsAdapter()

        # Output directory for local files
        self.output_dir = Path("corporate_actions_output")
        self.output_dir.mkdir(exist_ok=True)

        logger.info("✅ CorporateActionsHandler initialized (TRADFI only)")

    def _get_tickers_from_gcs(self, reference_date: date | None = None) -> list[str]:
        """
        Fetch equity tickers from TRADFI instruments store via UCI DataSource.

        Reads TRADFI instrument definitions and extracts exchange_raw_symbol
        for NYSE and NASDAQ venues (equities).

        Args:
            reference_date: Date to use for instrument lookup (default: finds file with equities)

        Returns:
            List of ticker symbols (e.g., ['AAPL', 'MSFT', ...])
        """
        try:
            data_source: DataSource = get_data_source(routing_key="tradfi", prefix="instrument_availability/by_date")

            def try_load_tickers(date_str: str) -> list[str]:
                """Try to load tickers from a specific date partition."""
                try:
                    raw = data_source.read(partition={"day": date_str}, format="parquet")
                    if not isinstance(raw, pd.DataFrame):
                        return []
                    df = cast(pd.DataFrame, raw)
                    if df.empty:
                        return []

                    # Filter for equities (NYSE, NASDAQ)
                    venue_series: pd.Series = df["venue"]
                    equities: pd.DataFrame = df[venue_series.isin(["NYSE", "NASDAQ"])]
                    symbol_arr = equities["exchange_raw_symbol"].dropna().unique()
                    symbol_list: list[str | int | float] = cast(list[str | int | float], symbol_arr.tolist())
                    tickers_raw: list[str] = [str(s).strip() for s in symbol_list if s is not None and str(s).strip()]
                    tickers = [t.strip() for t in tickers_raw if t and t.strip()]
                    return sorted(tickers)
                except (FileNotFoundError, OSError, RuntimeError, ValueError):
                    return []

            # If specific date provided, use it
            if reference_date is not None:
                tickers = try_load_tickers(str(reference_date))
                if tickers:
                    logger.info("Using instruments from: day=%s", reference_date)
                    logger.info("Loaded %s equity tickers from storage", len(tickers))
                    return tickers
                logger.warning("No equities found for %s", reference_date)

            # Try known good dates that have equities (2024-07-01 verified to have 596 equities)
            known_good_dates = [
                "2024-07-01",  # Verified: 596 equities
                "2024-06-01",
                "2024-05-01",
                "2023-05-23",  # Benchmark date
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

    def _get_tickers(self, tickers: list[str] | None = None, reference_date: date | None = None) -> list[str]:
        """
        Get list of tickers to process.

        Priority:
        1. Explicit tickers from CLI args
        2. Tickers from GCS instruments store
        3. Fallback minimal list for testing
        """
        if tickers:
            return [t.upper() for t in tickers]

        # Fetch from GCS instruments store (single source of truth)
        gcs_tickers = self._get_tickers_from_gcs(reference_date)
        if gcs_tickers:
            return gcs_tickers

        # Fallback minimal list for testing (if GCS unavailable)
        logger.warning("⚠️ Could not load tickers from GCS, using minimal test list")
        logger.warning("   Run --mode instruments first to populate GCS with instrument definitions")
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "BRK-B", "JPM", "V"]

    def _progress_callback(self, ticker: str, current: int, total: int):
        """Progress callback for batch processing."""
        pct = (current / total) * 100
        logger.info("📊 [%s/%s] (%..1f%) Processing %s", current, total, pct, ticker)

    def run(self, **kwargs: object) -> dict[str, HandlerResultValue]:
        """
        Execute corporate actions fetch.

        Args (via kwargs):
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            tickers: Optional list of tickers (defaults to S&P 500)
            output_format: Output format ('parquet' or 'csv')
            upload_to_storage: Whether to upload to GCS

        Returns:
            Result dictionary with status and statistics
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        if start_date is None or start_date == "":
            raise ValueError("start_date is required for corporate actions")
        if end_date is None or end_date == "":
            raise ValueError("end_date is required for corporate actions")
        tickers = kwargs.get("tickers")
        output_format = str(kwargs.get("output_format", "parquet"))
        upload_to_storage = bool(kwargs.get("upload_to_storage", False))

        # Parse dates (narrow type for type checker)
        start = parse_date(str(start_date)) if isinstance(start_date, str) else start_date
        end = parse_date(str(end_date)) if isinstance(end_date, str) else end_date
        if not isinstance(start, date) or not isinstance(end, date):
            raise ValueError("start_date and end_date must be date or YYYY-MM-DD string")

        # Get tickers
        ticker_list = self._get_tickers(cast(list[str] | None, tickers))

        logger.info("🚀 Fetching corporate actions for %s tickers", len(ticker_list))
        logger.info("📅 Date range: %s to %s", start, end)

        # Fetch data
        bundles: dict[str, CorporateActionsBundle] = self.adapter.fetch_batch(
            tickers=ticker_list,
            start_date=start,
            end_date=end,
            progress_callback=self._progress_callback,
        )

        # Convert to DataFrames
        dividends_df, splits_df, earnings_df = self.adapter.to_dataframes(bundles)

        # Calculate statistics
        stats: dict[str, object] = {
            "tickers_requested": len(ticker_list),
            "tickers_fetched": len(bundles),
            "dividends_count": len(dividends_df),
            "splits_count": len(splits_df),
            "earnings_count": len(earnings_df),
            "date_range": f"{start} to {end}",
        }

        logger.info(
            "📈 Results: %s dividends, %s splits, %s earnings",
            stats["dividends_count"],
            stats["splits_count"],
            stats["earnings_count"],
        )

        # Save to files
        output_files = self._save_results(
            dividends_df,
            splits_df,
            earnings_df,
            start,
            end,
            output_format,
        )
        stats["output_files"] = output_files

        # Optionally upload to GCS
        if upload_to_storage:
            gcs_paths = self._upload_to_storage(output_files)
            stats["gcs_paths"] = gcs_paths

        return {
            "success": True,
            "status": "success",
            "statistics": stats,
            "bundles": cast(dict[str, object], bundles),  # For programmatic access
        }

    def _save_results(
        self,
        dividends_df: pd.DataFrame,
        splits_df: pd.DataFrame,
        earnings_df: pd.DataFrame,
        start_date: date,
        end_date: date,
        output_format: str,
    ) -> list[dict[str, str]]:
        """
        Save results to local files partitioned by day.

        Only creates files for days that have at least one corporate action.
        Empty days are skipped to avoid proliferating empty files.

        Returns:
            List of dicts with action_type, date, and path for each file created.
        """
        output_files: list[dict[str, str]] = []

        # Generate date range
        date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="D")

        # Normalize date columns for filtering
        dividends_df = self._normalize_date_column(dividends_df, "ex_date")
        splits_df = self._normalize_date_column(splits_df, "effective_date")
        earnings_df = self._normalize_date_column(earnings_df, "earnings_date")

        days_with_data = 0
        for day in date_range:
            day_date: date = day.date()
            day_str: str = day_date.isoformat()

            # Filter data for this specific day
            day_dividends = self._filter_by_date(dividends_df, "ex_date", day_date, DIVIDENDS_SCHEMA)
            day_splits = self._filter_by_date(splits_df, "effective_date", day_date, SPLITS_SCHEMA)
            day_earnings = self._filter_by_date(earnings_df, "earnings_date", day_date, EARNINGS_SCHEMA)

            # Skip days with no data to avoid empty file proliferation
            if day_dividends.empty and day_splits.empty and day_earnings.empty:
                continue

            days_with_data += 1

            # Create day directory
            day_dir = self.output_dir / "by_date" / f"day={day_str}"
            day_dir.mkdir(parents=True, exist_ok=True)

            # Write files for this day
            day_files = self._write_day_files(
                day_dir=day_dir,
                day_str=day_str,
                output_format=output_format,
                dividends_df=day_dividends,
                splits_df=day_splits,
                earnings_df=day_earnings,
            )
            output_files.extend(day_files)

        logger.info("💾 Saved files for %s days with corporate actions", days_with_data)
        return output_files

    def _normalize_date_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Normalize a date column to datetime.date for consistent filtering."""
        if df.empty or column not in df.columns:
            return df
        df = df.copy()
        df[column] = pd.to_datetime(df[column], errors="coerce", utc=True).dt.date
        return df

    def _filter_by_date(
        self,
        df: pd.DataFrame,
        column: str,
        target_date: date,
        schema: list[str],
    ) -> pd.DataFrame:
        """
        Filter dataframe by date, preserving schema for output.

        Args:
            df: DataFrame to filter
            column: Date column to filter on
            target_date: Date to match
            schema: List of columns to include in output

        Returns:
            Filtered DataFrame with schema columns (may be empty)
        """
        if df.empty or column not in df.columns:
            return pd.DataFrame(columns=schema)

        filtered: pd.DataFrame = df[df[column] == target_date]
        if filtered.empty:
            return pd.DataFrame(columns=schema)

        # Select only schema columns that exist in the dataframe
        available_cols: list[str] = [c for c in schema if c in filtered.columns]
        return filtered[available_cols].copy()

    def _write_day_files(
        self,
        day_dir: Path,
        day_str: str,
        output_format: str,
        dividends_df: pd.DataFrame,
        splits_df: pd.DataFrame,
        earnings_df: pd.DataFrame,
    ) -> list[dict[str, str]]:
        """
        Write all corporate actions files for a specific day.

        Only writes files for action types that have data.
        """
        output_files: list[dict[str, str]] = []
        suffix = "parquet" if output_format == "parquet" else "csv"

        # Write dividends if present
        if not dividends_df.empty:
            path = day_dir / f"dividends.{suffix}"
            if output_format == "parquet":
                dividends_df.to_parquet(path, index=False)
            else:
                dividends_df.to_csv(path, index=False)
            output_files.append({"action_type": "dividends", "date": day_str, "path": str(path)})
            logger.debug("💾 Saved %s dividends for %s", len(dividends_df), day_str)

        # Write splits if present
        if not splits_df.empty:
            path = day_dir / f"splits.{suffix}"
            if output_format == "parquet":
                splits_df.to_parquet(path, index=False)
            else:
                splits_df.to_csv(path, index=False)
            output_files.append({"action_type": "splits", "date": day_str, "path": str(path)})
            logger.debug("💾 Saved %s splits for %s", len(splits_df), day_str)

        # Write earnings if present
        if not earnings_df.empty:
            path = day_dir / f"earnings.{suffix}"
            if output_format == "parquet":
                earnings_df.to_parquet(path, index=False)
            else:
                earnings_df.to_csv(path, index=False)
            output_files.append({"action_type": "earnings", "date": day_str, "path": str(path)})
            logger.debug("💾 Saved %s earnings for %s", len(earnings_df), day_str)

        return output_files

    def _upload_to_storage(self, output_files: list[dict[str, str]]) -> dict[str, str]:
        """
        Upload corporate actions files via UCI DataSink intent API (TRADFI routing).

        Target path: corporate_actions/by_date/day={date}/{action_type}.parquet

        Args:
            output_files: List of dicts with action_type, date, and path keys

        Returns:
            Dict mapping "{action_type}:{date}" to the written path
        """
        written_paths: dict[str, str] = {}

        if not output_files:
            logger.info("📭 No files to upload to GCS")
            return written_paths

        try:
            # PROTOCOL_DATA_SINK_BUCKET_TRADFI — set by deployment bootstrap
            data_sink: DataSink = get_data_sink(routing_key="tradfi")

            for entry in output_files:
                action_type = entry["action_type"]
                day_str = entry["date"]
                local_path = entry["path"]

                # Read file as DataFrame and write via DataSink
                df = pd.read_parquet(local_path)
                written_path = data_sink.write(
                    df,
                    partition={"day": day_str, "action_type": action_type},
                    format="parquet",
                )

                written_paths[f"{action_type}:{day_str}"] = written_path
                logger.debug("☁️ Uploaded %s (%s) to %s", action_type, day_str, written_path)

            logger.info("✅ Uploaded %s files to storage", len(written_paths))
            return written_paths

        except (OSError, ValueError, TypeError, KeyError) as e:
            logger.error("❌ Failed to upload to storage: %s", e)
            return {}

    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
