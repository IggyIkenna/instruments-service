"""
Corporate Actions Handler

CLI handler for fetching and storing corporate actions data (TRADFI only).
Supports dividends, stock splits, and earnings dates for US equities.

Note: Corporate actions are TRADFI-only. Crypto and DeFi do not have
traditional corporate actions like dividends or stock splits.

Tickers are sourced from GCS instruments store (instruments-store-tradfi),
ensuring the same universe as instrument definitions.
"""

import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from pathlib import Path

import pandas as pd

from instruments_service.cli.base_handler import ModeHandler
from instruments_service.corporate_actions.adapter import CorporateActionsAdapter
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)

# GCS bucket for TRADFI instruments
TRADFI_BUCKET = "instruments-store-tradfi-central-element-323112"


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

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        self.project_id = config.get("project_id", "central-element-323112")

        # Initialize adapter (yfinance only - free, no API key needed)
        self.adapter = CorporateActionsAdapter()

        # Output directory for local files
        self.output_dir = Path("corporate_actions_output")
        self.output_dir.mkdir(exist_ok=True)

        logger.info("✅ CorporateActionsHandler initialized (TRADFI only)")

    def _get_tickers_from_gcs(self, reference_date: Optional[date] = None) -> List[str]:
        """
        Fetch equity tickers from GCS instruments store.

        Reads TRADFI instrument definitions and extracts exchange_raw_symbol
        for NYSE and NASDAQ venues (equities).

        Args:
            reference_date: Date to use for instrument lookup (default: finds file with equities)

        Returns:
            List of ticker symbols (e.g., ['AAPL', 'MSFT', ...])
        """
        try:
            from unified_cloud_services import get_gcs_client
            import tempfile

            client = get_gcs_client(project_id=self.project_id)
            bucket_name = instruments_config.gcs_bucket_tradfi or TRADFI_BUCKET
            bucket = client.bucket(bucket_name)

            def try_load_tickers(gcs_path: str) -> List[str]:
                """Try to load tickers from a specific GCS path."""
                blob = bucket.blob(gcs_path)
                if not blob.exists():
                    return []

                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                    blob.download_to_filename(tmp.name)
                    df = pd.read_parquet(tmp.name)

                # Filter for equities (NYSE, NASDAQ)
                equities = df[df['venue'].isin(['NYSE', 'NASDAQ'])]
                tickers = equities['exchange_raw_symbol'].dropna().unique().tolist()
                tickers = [t.strip() for t in tickers if t and t.strip()]
                return sorted(tickers)

            # If specific date provided, use it
            if reference_date is not None:
                gcs_path = f"instrument_availability/by_date/day-{reference_date}/instruments.parquet"
                tickers = try_load_tickers(gcs_path)
                if tickers:
                    logger.info(f"📂 Using instruments from: day-{reference_date}")
                    logger.info(f"📊 Loaded {len(tickers)} equity tickers from GCS")
                    return tickers
                logger.warning(f"⚠️ No equities found for {reference_date}")

            # Try known good dates that have equities (2024-07-01 verified to have 596 equities)
            known_good_dates = [
                "2024-07-01",  # Verified: 596 equities
                "2024-06-01",
                "2024-05-01",
                "2023-05-23",  # Benchmark date
            ]

            for date_str in known_good_dates:
                gcs_path = f"instrument_availability/by_date/day-{date_str}/instruments.parquet"
                tickers = try_load_tickers(gcs_path)
                if tickers:
                    logger.info(f"📂 Using instruments from: day-{date_str}")
                    logger.info(f"📊 Loaded {len(tickers)} equity tickers from GCS")
                    return tickers

            logger.warning("⚠️ No equity tickers found in any GCS instruments file")
            return []

        except ImportError:
            logger.error("❌ google-cloud-storage not installed")
            return []
        except Exception as e:
            logger.error(f"❌ Failed to load tickers from GCS: {e}")
            return []

    def _get_tickers(self, tickers: Optional[List[str]] = None, reference_date: Optional[date] = None) -> List[str]:
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
        logger.info(f"📊 [{current}/{total}] ({pct:.1f}%) Processing {ticker}")

    def run(
        self,
        start_date: str,
        end_date: str,
        tickers: Optional[List[str]] = None,
        output_format: str = "parquet",
        upload_to_gcs: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Execute corporate actions fetch.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            tickers: Optional list of tickers (defaults to S&P 500)
            output_format: Output format ('parquet' or 'csv')
            upload_to_gcs: Whether to upload to GCS

        Returns:
            Result dictionary with status and statistics
        """
        # Parse dates
        start = parse_date(start_date) if isinstance(start_date, str) else start_date
        end = parse_date(end_date) if isinstance(end_date, str) else end_date

        # Get tickers
        ticker_list = self._get_tickers(tickers)

        logger.info(f"🚀 Fetching corporate actions for {len(ticker_list)} tickers")
        logger.info(f"📅 Date range: {start} to {end}")

        # Fetch data
        bundles = self.adapter.fetch_batch(
            tickers=ticker_list,
            start_date=start,
            end_date=end,
            progress_callback=self._progress_callback,
        )

        # Convert to DataFrames
        dividends_df, splits_df, earnings_df = self.adapter.to_dataframes(bundles)

        # Calculate statistics
        stats = {
            "tickers_requested": len(ticker_list),
            "tickers_fetched": len(bundles),
            "dividends_count": len(dividends_df),
            "splits_count": len(splits_df),
            "earnings_count": len(earnings_df),
            "date_range": f"{start} to {end}",
        }

        logger.info(f"📈 Results: {stats['dividends_count']} dividends, {stats['splits_count']} splits, {stats['earnings_count']} earnings")

        # Save to files
        output_files = self._save_results(
            dividends_df, splits_df, earnings_df,
            start, end, output_format,
        )
        stats["output_files"] = output_files

        # Optionally upload to GCS
        if upload_to_gcs:
            gcs_paths = self._upload_to_gcs(output_files)
            stats["gcs_paths"] = gcs_paths

        return {
            "success": True,
            "status": "success",
            "statistics": stats,
            "bundles": bundles,  # For programmatic access
        }

    def _save_results(
        self,
        dividends_df: pd.DataFrame,
        splits_df: pd.DataFrame,
        earnings_df: pd.DataFrame,
        start_date: date,
        end_date: date,
        output_format: str,
    ) -> Dict[str, str]:
        """Save results to local files."""
        output_files = {}

        # Create output directory with date range
        date_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        if output_format == "parquet":
            if not dividends_df.empty:
                path = self.output_dir / f"dividends_{date_suffix}.parquet"
                dividends_df.to_parquet(path, index=False)
                output_files["dividends"] = str(path)
                logger.info(f"💾 Saved dividends to {path}")

            if not splits_df.empty:
                path = self.output_dir / f"splits_{date_suffix}.parquet"
                splits_df.to_parquet(path, index=False)
                output_files["splits"] = str(path)
                logger.info(f"💾 Saved splits to {path}")

            if not earnings_df.empty:
                path = self.output_dir / f"earnings_{date_suffix}.parquet"
                earnings_df.to_parquet(path, index=False)
                output_files["earnings"] = str(path)
                logger.info(f"💾 Saved earnings to {path}")

        elif output_format == "csv":
            if not dividends_df.empty:
                path = self.output_dir / f"dividends_{date_suffix}.csv"
                dividends_df.to_csv(path, index=False)
                output_files["dividends"] = str(path)

            if not splits_df.empty:
                path = self.output_dir / f"splits_{date_suffix}.csv"
                splits_df.to_csv(path, index=False)
                output_files["splits"] = str(path)

            if not earnings_df.empty:
                path = self.output_dir / f"earnings_{date_suffix}.csv"
                earnings_df.to_csv(path, index=False)
                output_files["earnings"] = str(path)

        return output_files

    def _upload_to_gcs(self, output_files: Dict[str, str]) -> Dict[str, str]:
        """
        Upload corporate actions files to GCS TRADFI bucket.

        Target path: gs://instruments-store-tradfi-central-element-323112/corporate_actions/
        """
        from instruments_service.config import instruments_config

        gcs_paths = {}

        try:
            # Get TRADFI bucket (corporate actions are TRADFI only)
            bucket_name = instruments_config.gcs_bucket_tradfi
            if not bucket_name:
                logger.warning("⚠️ TRADFI GCS bucket not configured - skipping GCS upload")
                return {}

            # Import GCS client
            from unified_cloud_services import get_gcs_client
            client = get_gcs_client(project_id=self.project_id)
            bucket = client.bucket(bucket_name)

            for action_type, local_path in output_files.items():
                # GCS path: corporate_actions/{action_type}.parquet
                # e.g., corporate_actions/dividends_20200101_20260125.parquet
                filename = Path(local_path).name
                gcs_path = f"corporate_actions/{filename}"

                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)

                full_gcs_path = f"gs://{bucket_name}/{gcs_path}"
                gcs_paths[action_type] = full_gcs_path
                logger.info(f"☁️ Uploaded {action_type} to {full_gcs_path}")

            logger.info(f"✅ Uploaded {len(gcs_paths)} files to GCS")
            return gcs_paths

        except ImportError:
            logger.warning("⚠️ google-cloud-storage not installed - skipping GCS upload")
            return {}
        except Exception as e:
            logger.error(f"❌ Failed to upload to GCS: {e}")
            return {}

    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
