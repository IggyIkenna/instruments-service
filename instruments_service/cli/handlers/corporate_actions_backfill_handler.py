"""
Corporate Actions Backfill Handler

Fetches full history for all tickers ONCE and stores by ticker.
This is Phase 1 of the optimized corporate actions pipeline.

Key Features:
- Fetches FULL history from yfinance (all available data)
- Stores in by_ticker/{ticker}/ structure (CSV format)
- Supports append mode (only adds new events)
- Updates ticker_registry.yaml with metadata
- Generates coverage_report.json

Usage:
    python -m instruments_service \\
        --mode corporate_actions_backfill \\
        --tickers AAPL MSFT JPM KO TSLA \\
        --parallel-workers 10

Storage Structure:
    corporate_actions_backfill_output/
    ├── by_ticker/
    │   ├── AAPL/
    │   │   ├── dividends.csv
    │   │   ├── splits.csv
    │   │   └── earnings.csv
    │   ├── MSFT/
    │   │   ├── dividends.csv
    │   │   ├── splits.csv
    │   │   └── earnings.csv
    │   └── ...
    └── metadata/
        ├── ticker_registry.yaml
        └── coverage_report.json
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import yaml
from unified_cloud_interface import DataSource, get_data_source

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import instruments_config
from instruments_service.corporate_actions.adapter import CorporateActionsAdapter

logger = logging.getLogger(__name__)


class CorporateActionsBackfillHandler(ModeHandler):
    """
    Handler for backfilling corporate actions (full history per ticker).

    This handler fetches the ENTIRE history available from yfinance for each ticker
    and stores it in a by_ticker structure. This is much more efficient than
    repeatedly fetching the same data for different date ranges.

    Phase 1 of the optimized pipeline.
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
        self.output_dir = Path("corporate_actions_backfill_output")
        self.output_dir.mkdir(exist_ok=True)

        # Create subdirectories
        self.by_ticker_dir = self.output_dir / "by_ticker"
        self.by_ticker_dir.mkdir(exist_ok=True)

        self.metadata_dir = self.output_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)

        logger.info("✅ CorporateActionsBackfillHandler initialized")

    def _get_tickers_from_gcs(self, reference_date: date | None = None) -> list[str]:
        """
        Fetch equity tickers from TRADFI instruments store via UCI DataSource.

        Same logic as original corporate_actions_handler.
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
                    equities: pd.DataFrame = df[df["venue"].isin(["NYSE", "NASDAQ"])]
                    tickers_raw: list[str] = list(equities["exchange_raw_symbol"].dropna().unique().tolist())
                    tickers: list[str] = [str(t).strip() for t in tickers_raw if t and str(t).strip()]
                    return sorted(tickers)
                except (FileNotFoundError, OSError, RuntimeError, ValueError):
                    return []

            # Try known good dates
            known_good_dates = [
                "2024-07-01",  # Verified: 596 equities
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

    def _get_tickers(self, tickers: list[str] | None = None) -> list[str]:
        """Get list of tickers to process."""
        if tickers:
            return [t.upper() for t in tickers]

        # Fetch from GCS instruments store
        gcs_tickers = self._get_tickers_from_gcs()
        if gcs_tickers:
            return gcs_tickers

        # Fallback minimal list
        logger.warning("⚠️ Could not load tickers from GCS, using minimal test list")
        return ["AAPL", "MSFT", "JPM", "KO", "TSLA"]

    def _fetch_ticker_full_history(
        self,
        ticker: str,
        append_mode: bool = True,
        max_retries: int = 3,
    ) -> dict[str, object]:
        """
        Fetch FULL history for a single ticker and save to CSV.

        Args:
            ticker: Stock ticker symbol
            append_mode: If True, only append new events to existing data
            max_retries: Maximum number of retry attempts

        Returns:
            Dict with status and statistics
        """
        ticker_dir = self.by_ticker_dir / ticker
        ticker_dir.mkdir(exist_ok=True)

        result: dict[str, object] = {
            "ticker": ticker,
            "success": False,
            "dividends_count": 0,
            "splits_count": 0,
            "earnings_count": 0,
            "error": None,
        }

        for attempt in range(1, max_retries + 1):
            try:
                # Fetch FULL history (no date range limits)
                # We use a very wide date range to get everything
                start_date = date(1900, 1, 1)
                end_date = date(2100, 12, 31)

                bundle = self.adapter.fetch_corporate_actions(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                )

                # Convert to DataFrames
                dividends_df, splits_df, earnings_df = self.adapter.to_dataframes({ticker: bundle})

                # Handle append mode
                if append_mode:
                    dividends_df = self._append_to_existing(ticker_dir / "dividends.csv", dividends_df, "ex_date")
                    splits_df = self._append_to_existing(ticker_dir / "splits.csv", splits_df, "effective_date")
                    earnings_df = self._append_to_existing(ticker_dir / "earnings.csv", earnings_df, "earnings_date")

                # Save to CSV
                if not dividends_df.empty:
                    dividends_df.to_csv(ticker_dir / "dividends.csv", index=False)
                    result["dividends_count"] = len(dividends_df)

                if not splits_df.empty:
                    splits_df.to_csv(ticker_dir / "splits.csv", index=False)
                    result["splits_count"] = len(splits_df)

                if not earnings_df.empty:
                    earnings_df.to_csv(ticker_dir / "earnings.csv", index=False)
                    result["earnings_count"] = len(earnings_df)

                result["success"] = True
                logger.debug(
                    "✅ %s: %s dividends, %s splits, %s earnings",
                    ticker,
                    result["dividends_count"],
                    result["splits_count"],
                    result["earnings_count"],
                )
                break  # Success, exit retry loop

            except (OSError, ValueError, TypeError, KeyError, ConnectionError) as e:
                result["error"] = str(e)
                if attempt < max_retries:
                    logger.warning("⚠️ %s: Attempt %s/%s failed: %s. Retrying...", ticker, attempt, max_retries, e)
                else:
                    logger.error("❌ %s: All %s attempts failed: %s", ticker, max_retries, e)

        return result

    def _append_to_existing(
        self,
        csv_path: Path,
        new_df: pd.DataFrame,
        date_column: str,
    ) -> pd.DataFrame:
        """
        Append new data to existing CSV, deduplicating by date.

        Args:
            csv_path: Path to existing CSV file
            new_df: New DataFrame to append
            date_column: Column name for date (used for deduplication)

        Returns:
            Combined DataFrame with duplicates removed
        """
        if not csv_path.exists() or new_df.empty:
            return new_df

        try:
            existing_df: pd.DataFrame = pd.read_csv(csv_path)
            if existing_df.empty:
                return new_df

            # Convert date columns to date for comparison
            existing_df[date_column] = pd.to_datetime(existing_df[date_column], utc=True).dt.date
            new_df[date_column] = pd.to_datetime(new_df[date_column], utc=True).dt.date

            # Get existing dates
            existing_dates: set[object] = set(existing_df[date_column])

            # Filter new_df to only NEW dates
            new_events: pd.DataFrame = new_df[~new_df[date_column].isin(existing_dates)]

            if new_events.empty:
                logger.debug("No new events to append for %s", csv_path.name)
                return existing_df

            # Combine and sort
            combined: pd.DataFrame = pd.concat([existing_df, new_events], ignore_index=True)
            combined = combined.sort_values([date_column])

            logger.debug("Appended %s new events to %s", len(new_events), csv_path.name)
            return combined

        except (OSError, ValueError, TypeError, KeyError) as e:
            logger.warning("Failed to append to %s: %s. Returning new data only.", csv_path, e)
            return new_df

    def _update_ticker_registry(
        self,
        results: list[dict[str, object]],
    ) -> None:
        """
        Update ticker_registry.yaml with fetch results and statistics.

        Args:
            results: List of fetch results per ticker
        """
        registry_path = self.metadata_dir / "ticker_registry.yaml"

        # Load existing registry if exists
        registry: dict[str, object]
        if registry_path.exists():
            with open(registry_path) as f:
                registry = yaml.safe_load(f) or {}
        else:
            registry = {
                "version": "1.0",
                "tickers": {},
                "config": {
                    "update_frequency_days": 7,
                    "parallel_workers": 10,
                    "source": "yfinance",
                },
            }

        registry["last_updated"] = datetime.now(UTC).isoformat()
        registry["total_tickers"] = len(results)

        # Update ticker entries
        tickers_map: dict[str, object] = cast(dict[str, object], registry.get("tickers") or {})
        for result in results:
            ticker_val: object = result.get("ticker")
            ticker: str = str(ticker_val) if ticker_val is not None else ""

            raw_entry: dict[str, object] = cast(
                dict[str, object],
                tickers_map.get(
                    ticker,
                    {
                        "exchange": "UNKNOWN",
                        "instrument_type": "equity",
                        "fetch_history": [],
                    },
                ),
            )
            ticker_entry: dict[str, object] = (
                raw_entry
                if isinstance(raw_entry, dict)
                else {
                    "exchange": "UNKNOWN",
                    "instrument_type": "equity",
                    "fetch_history": [],
                }
            )

            # Update metadata
            ticker_entry["last_download_date"] = date.today().isoformat()
            ticker_entry["status"] = "active" if result["success"] else "error"

            if result["success"]:
                ticker_entry["stats"] = {
                    "total_dividends": result["dividends_count"],
                    "total_splits": result["splits_count"],
                    "total_earnings": result["earnings_count"],
                }
            else:
                ticker_entry["last_error"] = result["error"]

            # Add to fetch history
            fetch_history_raw: object = ticker_entry.get("fetch_history")
            fetch_history: list[dict[str, object]] = (
                list(cast(list[dict[str, object]], fetch_history_raw)) if isinstance(fetch_history_raw, list) else []
            )
            fetch_history.append(
                {
                    "date": datetime.now(UTC).isoformat(),
                    "result": "success" if result["success"] else "error",
                    "events_found": (
                        (
                            int(result["dividends_count"])
                            if isinstance(result["dividends_count"], (int, float, str))
                            else 0
                        )
                        + (int(result["splits_count"]) if isinstance(result["splits_count"], (int, float, str)) else 0)
                        + (
                            int(result["earnings_count"])
                            if isinstance(result["earnings_count"], (int, float, str))
                            else 0
                        )
                    ),
                    "error": result["error"] if not result["success"] else None,
                }
            )

            # Keep only last 10 fetch history entries
            ticker_entry["fetch_history"] = fetch_history[-10:]

            tickers_map[ticker] = ticker_entry

        # Ensure the updated tickers_map is reflected in registry
        registry["tickers"] = tickers_map

        # Save registry
        with open(registry_path, "w") as f:
            yaml.dump(registry, f, default_flow_style=False, sort_keys=False)

        logger.info("✅ Updated ticker registry: %s", registry_path)

    def _generate_coverage_report(
        self,
        results: list[dict[str, object]],
    ) -> None:
        """
        Generate coverage_report.json with data quality metrics.

        Args:
            results: List of fetch results per ticker
        """
        report_path = self.metadata_dir / "coverage_report.json"

        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        def _gt_zero(r: dict[str, object], key: str) -> bool:
            val = r.get(key)
            return isinstance(val, (int, float)) and val > 0

        def _eq_zero(r: dict[str, object], key: str) -> bool:
            val = r.get(key)
            return not isinstance(val, (int, float)) or val == 0

        tickers_with_dividends = [r["ticker"] for r in successful if _gt_zero(r, "dividends_count")]
        tickers_with_splits = [r["ticker"] for r in successful if _gt_zero(r, "splits_count")]
        tickers_with_earnings = [r["ticker"] for r in successful if _gt_zero(r, "earnings_count")]
        tickers_no_data = [
            r["ticker"]
            for r in successful
            if _eq_zero(r, "dividends_count") and _eq_zero(r, "splits_count") and _eq_zero(r, "earnings_count")
        ]

        def _count(r: dict[str, object], key: str) -> int:
            val = r[key]
            return int(val) if isinstance(val, (int, float, str)) else 0

        total_dividends = sum(_count(r, "dividends_count") for r in successful)
        total_splits = sum(_count(r, "splits_count") for r in successful)
        total_earnings = sum(_count(r, "earnings_count") for r in successful)

        report: dict[str, object] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "version": "1.0",
            "summary": {
                "total_tickers": len(results),
                "tickers_with_dividends": len(tickers_with_dividends),
                "tickers_with_splits": len(tickers_with_splits),
                "tickers_with_earnings": len(tickers_with_earnings),
                "total_events": total_dividends + total_splits + total_earnings,
            },
            "by_action_type": {
                "dividends": {
                    "total_events": total_dividends,
                    "tickers_with_data": len(tickers_with_dividends),
                },
                "splits": {
                    "total_events": total_splits,
                    "tickers_with_data": len(tickers_with_splits),
                },
                "earnings": {
                    "total_events": total_earnings,
                    "tickers_with_data": len(tickers_with_earnings),
                },
            },
            "data_quality": {
                "tickers_fetched_successfully": len(successful),
                "tickers_with_errors": [{"ticker": r["ticker"], "error": r["error"]} for r in failed],
                "tickers_no_data": tickers_no_data,
            },
            "last_update_check": datetime.now(UTC).isoformat(),
        }

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info("✅ Generated coverage report: %s", report_path)

    def run(
        self,
        tickers: list[str] | None = None,
        parallel_workers: int = 10,
        append_mode: bool = True,
        upload_to_gcs: bool = False,
        max_retries: int = 3,
        **kwargs: object,
    ) -> dict[str, HandlerResultValue]:
        """
        Execute corporate actions backfill.

        Args:
            tickers: Optional list of tickers (defaults to all from GCS)
            parallel_workers: Number of parallel workers (default: 10)
            append_mode: Only append new events to existing data (default: True)
            upload_to_gcs: Whether to upload to GCS (default: False for testing)
            max_retries: Maximum retry attempts per ticker (default: 3)

        Returns:
            Result dictionary with status and statistics
        """
        # Get tickers
        ticker_list = self._get_tickers(tickers)

        logger.info("🚀 Starting backfill for %s tickers", len(ticker_list))
        logger.info("⚙️ Parallel workers: %s", parallel_workers)
        logger.info("📝 Append mode: %s", append_mode)
        logger.info("🔄 Max retries: %s", max_retries)

        # Fetch data with parallel processing
        results: list[dict[str, object]] = []

        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(
                    self._fetch_ticker_full_history,
                    ticker,
                    append_mode,
                    max_retries,
                ): ticker
                for ticker in ticker_list
            }

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_ticker), 1):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    results.append(result)

                    pct = (i / len(ticker_list)) * 100
                    logger.info("📊 [%s/%s] (%..1f%) Processed %s", i, len(ticker_list), pct, ticker)

                except (OSError, ValueError, TypeError, KeyError, ConnectionError, TimeoutError) as e:
                    logger.error("❌ Unexpected error processing %s: %s", ticker, e)
                    results.append(
                        {
                            "ticker": ticker,
                            "success": False,
                            "dividends_count": 0,
                            "splits_count": 0,
                            "earnings_count": 0,
                            "error": str(e),
                        }
                    )

        # Update metadata
        self._update_ticker_registry(results)
        self._generate_coverage_report(results)

        # Calculate statistics
        successful: list[dict[str, object]] = [r for r in results if r["success"]]
        failed: list[dict[str, object]] = [r for r in results if not r["success"]]

        def _to_int(val: object) -> int:
            return int(val) if isinstance(val, (int, float, str)) else 0

        stats: dict[str, int] = {
            "tickers_requested": len(ticker_list),
            "tickers_successful": len(successful),
            "tickers_failed": len(failed),
            "total_dividends": sum(_to_int(r["dividends_count"]) for r in successful),
            "total_splits": sum(_to_int(r["splits_count"]) for r in successful),
            "total_earnings": sum(_to_int(r["earnings_count"]) for r in successful),
        }

        logger.info("📈 Results: %s/%s tickers successful", stats["tickers_successful"], stats["tickers_requested"])
        logger.info(
            "📈 Total events: %s dividends, %s splits, %s earnings",
            stats["total_dividends"],
            stats["total_splits"],
            stats["total_earnings"],
        )

        # TODO: Upload to GCS if requested
        if upload_to_gcs:
            logger.warning("⚠️ GCS upload not yet implemented (testing phase)")

        return cast(
            dict[str, HandlerResultValue],
            {
                "success": True,
                "status": "success",
                "statistics": stats,
                "results": results,
            },
        )

    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
