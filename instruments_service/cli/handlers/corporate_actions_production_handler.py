"""
Corporate Actions Production Pipeline Handler

Single unified handler that runs the complete pipeline:
1. Load metadata from GCS (or create if first run)
2. Get ticker list from config
3. Fetch data for all symbols and save immediately to by_ticker/ (parallel with rate limiting)
4. Combine all data into single DataFrames
5. Generate by_date views from combined data
6. Update metadata
7. Generate coverage report
8. Upload to GCS (optional)

Usage:
    python -m instruments_service.cli.main \
        --mode corporate_actions_production \
        --parallel-workers 2 \
        --upload-to-gcs
"""

import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import SP500_TICKERS, corporate_actions_start_date, instruments_config
from instruments_service.corporate_actions.adapter import CorporateActionsAdapter

logger = logging.getLogger(__name__)

# Rate limiting configuration
REQUEST_DELAY_MS = 100  # 100ms delay between requests to avoid rate limiting


class CorporateActionsProductionHandler(ModeHandler):
    """
    Production pipeline for corporate actions.

    Runs complete end-to-end pipeline:
    - Metadata management
    - Data fetching
    - by_ticker storage (Parquet)
    - by_date generation
    - Local storage (data/temp/)
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)

        self.project_id = config.get("project_id") or str(getattr(instruments_config, "gcp_project_id", "") or "")

        # Initialize adapter
        self.adapter = CorporateActionsAdapter()

        # Base output directory (GCS-like structure)
        self.base_dir = Path("data/temp/corporate_actions")
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Subdirectories
        self.by_ticker_dir = self.base_dir / "by_ticker"
        self.by_ticker_dir.mkdir(exist_ok=True)

        self.by_date_dir = self.base_dir / "by_date"
        self.by_date_dir.mkdir(exist_ok=True)

        self.metadata_dir = self.base_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)

        logger.info("✅ CorporateActionsProductionHandler initialized")
        logger.info(f"📁 Output directory: {self.base_dir}")

    def _load_metadata(self) -> dict[str, Any]:
        """
        Load metadata from local storage (in production: from GCS).

        Returns:
            Metadata dictionary with ticker registry
        """
        metadata_path = self.metadata_dir / "ticker_registry.json"

        if metadata_path.exists():
            logger.info(f"📂 Loading existing metadata from {metadata_path}")
            with open(metadata_path, "r") as f:
                return json.load(f)
        else:
            logger.info("📝 Creating new metadata (first run)")
            return {
                "version": "1.0",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "tickers": {},
                "config": {
                    "update_frequency_days": 7,
                    "parallel_workers": 10,
                    "source": "yfinance",
                },
            }

    def _save_metadata(self, metadata: dict[str, Any]) -> None:
        """
        Save metadata to local storage (in production: upload to GCS).

        Args:
            metadata: Metadata dictionary to save
        """
        metadata_path = self.metadata_dir / "ticker_registry.json"
        metadata["last_updated"] = datetime.now(timezone.utc).isoformat()

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"✅ Saved metadata to {metadata_path}")

    def _get_tickers_from_gcs(self) -> list[str]:
        """
        Fetch equity tickers from GCS instruments store.

        Returns:
            List of ticker symbols
        """
        try:
            from unified_cloud_services import CloudTarget, StandardizedDomainCloudService

            bucket_name = instruments_config.gcs_bucket_tradfi or instruments_config.get_bucket_for_category("tradfi")

            # Create cloud-agnostic service
            target = CloudTarget(
                project_id=self.project_id,
                gcs_bucket=bucket_name,
                bigquery_dataset=instruments_config.bigquery_dataset,
            )
            service = StandardizedDomainCloudService(domain="instruments", cloud_target=target)

            # Try known good dates
            known_good_dates = ["2024-07-01", "2024-06-01", "2024-05-01", "2023-05-23"]

            for date_str in known_good_dates:
                gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"

                try:
                    df: pd.DataFrame = service.download_from_gcs(gcs_path=gcs_path, format="parquet", log_errors=False)
                    if not df.empty:
                        # Filter for equities
                        equities: pd.DataFrame = df[df["venue"].isin(["NYSE", "NASDAQ"])]
                        tickers_raw: list[str] = list(equities["exchange_raw_symbol"].dropna().unique().tolist())
                        tickers: list[str] = [str(t).strip() for t in tickers_raw if t and str(t).strip()]

                        logger.info(f"📂 Loaded {len(tickers)} tickers from GCS (day={date_str})")
                        return sorted(tickers)
                except Exception:
                    continue

            logger.warning("⚠️ No equity tickers found in GCS")
            return []

        except Exception as e:
            logger.error(f"❌ Failed to load tickers from GCS: {e}")
            return []

    def _fetch_symbol_data(
        self,
        ticker: str,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Fetch all corporate actions for a single symbol.

        Args:
            ticker: Stock ticker symbol
            max_retries: Maximum retry attempts

        Returns:
            Dict with DataFrames and statistics
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "success": False,
            "dividends_df": pd.DataFrame(),
            "splits_df": pd.DataFrame(),
            "earnings_df": pd.DataFrame(),
            "error": None,
        }

        # Apply rate limiting - 100ms delay before each request
        time.sleep(REQUEST_DELAY_MS / 1000.0)

        for attempt in range(1, max_retries + 1):
            try:
                # Fetch data from corporate_actions_start_date (2020-01-01) onwards
                start_date = datetime.strptime(corporate_actions_start_date, "%Y-%m-%d").date()
                end_date = date(2100, 12, 31)

                bundle = self.adapter.fetch_corporate_actions(ticker, start_date, end_date)

                # Convert to DataFrames
                dividends_df, splits_df, earnings_df = self.adapter.to_dataframes({ticker: bundle})

                result["dividends_df"] = dividends_df
                result["splits_df"] = splits_df
                result["earnings_df"] = earnings_df
                result["success"] = True

                logger.debug(
                    f"✅ {ticker}: {len(dividends_df)} dividends, {len(splits_df)} splits, {len(earnings_df)} earnings"
                )
                break

            except Exception as e:
                result["error"] = str(e)
                if attempt < max_retries:
                    logger.warning(f"⚠️ {ticker}: Attempt {attempt}/{max_retries} failed: {e}. Retrying...")
                else:
                    logger.error(f"❌ {ticker}: All {max_retries} attempts failed: {e}")

        return result

    def _save_by_ticker(
        self,
        ticker: str,
        dividends_df: pd.DataFrame,
        splits_df: pd.DataFrame,
        earnings_df: pd.DataFrame,
    ) -> None:
        """
        Save ticker data to Parquet files in by_ticker folder.

        Args:
            ticker: Stock ticker symbol
            dividends_df: Dividends DataFrame
            splits_df: Splits DataFrame
            earnings_df: Earnings DataFrame
        """
        ticker_dir = self.by_ticker_dir / ticker
        ticker_dir.mkdir(exist_ok=True)

        if not dividends_df.empty:
            dividends_df.to_parquet(ticker_dir / "dividends.parquet", index=False)

        if not splits_df.empty:
            splits_df.to_parquet(ticker_dir / "splits.parquet", index=False)

        if not earnings_df.empty:
            earnings_df.to_parquet(ticker_dir / "earnings.parquet", index=False)

    def _generate_by_date(
        self,
        all_dividends: pd.DataFrame,
        all_splits: pd.DataFrame,
        all_earnings: pd.DataFrame,
    ) -> dict[str, int]:
        """
        Generate by_date partitions from combined DataFrames.

        Args:
            all_dividends: Combined dividends DataFrame
            all_splits: Combined splits DataFrame
            all_earnings: Combined earnings DataFrame

        Returns:
            Dict with statistics
        """
        stats = {
            "dividends_dates": 0,
            "splits_dates": 0,
            "earnings_dates": 0,
        }

        # Process dividends
        if not all_dividends.empty:
            all_dividends["ex_date"] = pd.to_datetime(all_dividends["ex_date"], utc=True).dt.date

            for day, group in all_dividends.groupby("ex_date"):
                if pd.isna(day):
                    continue

                day_str: str = day.isoformat()
                day_dir = self.by_date_dir / f"day={day_str}"
                day_dir.mkdir(exist_ok=True)

                group.to_parquet(day_dir / "dividends.parquet", index=False)
                stats["dividends_dates"] += 1

        # Process splits
        if not all_splits.empty:
            all_splits["effective_date"] = pd.to_datetime(all_splits["effective_date"], utc=True).dt.date

            for day, group in all_splits.groupby("effective_date"):
                if pd.isna(day):
                    continue

                day_str: str = day.isoformat()
                day_dir = self.by_date_dir / f"day={day_str}"
                day_dir.mkdir(exist_ok=True)

                group.to_parquet(day_dir / "splits.parquet", index=False)
                stats["splits_dates"] += 1

        # Process earnings
        if not all_earnings.empty:
            all_earnings["earnings_date"] = pd.to_datetime(all_earnings["earnings_date"], utc=True).dt.date

            for day, group in all_earnings.groupby("earnings_date"):
                if pd.isna(day):
                    continue

                day_str: str = day.isoformat()
                day_dir = self.by_date_dir / f"day={day_str}"
                day_dir.mkdir(exist_ok=True)

                group.to_parquet(day_dir / "earnings.parquet", index=False)
                stats["earnings_dates"] += 1

        return stats

    def _generate_coverage_report(
        self,
        results: list[dict[str, Any]],
        by_date_stats: dict[str, int],
    ) -> None:
        """
        Generate coverage report with statistics.

        Args:
            results: List of fetch results
            by_date_stats: Statistics from by_date generation
        """
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        total_dividends = sum(len(r["dividends_df"]) for r in successful)
        total_splits = sum(len(r["splits_df"]) for r in successful)
        total_earnings = sum(len(r["earnings_df"]) for r in successful)

        tickers_with_dividends = [r["ticker"] for r in successful if not r["dividends_df"].empty]
        tickers_with_splits = [r["ticker"] for r in successful if not r["splits_df"].empty]
        tickers_with_earnings = [r["ticker"] for r in successful if not r["earnings_df"].empty]

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0",
            "summary": {
                "total_tickers": len(results),
                "tickers_successful": len(successful),
                "tickers_failed": len(failed),
                "tickers_with_dividends": len(tickers_with_dividends),
                "tickers_with_splits": len(tickers_with_splits),
                "tickers_with_earnings": len(tickers_with_earnings),
                "total_events": total_dividends + total_splits + total_earnings,
            },
            "by_action_type": {
                "dividends": {
                    "total_events": total_dividends,
                    "tickers_with_data": len(tickers_with_dividends),
                    "date_partitions": by_date_stats["dividends_dates"],
                },
                "splits": {
                    "total_events": total_splits,
                    "tickers_with_data": len(tickers_with_splits),
                    "date_partitions": by_date_stats["splits_dates"],
                },
                "earnings": {
                    "total_events": total_earnings,
                    "tickers_with_data": len(tickers_with_earnings),
                    "date_partitions": by_date_stats["earnings_dates"],
                },
            },
            "data_quality": {
                "tickers_with_errors": [{"ticker": r["ticker"], "error": r["error"]} for r in failed],
            },
        }

        report_path = self.metadata_dir / "coverage_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"✅ Generated coverage report: {report_path}")

    def _upload_to_gcs(self) -> bool:
        """
        Upload all local data to GCS bucket.

        Returns:
            True if upload successful, False otherwise
        """
        try:
            # Use config or generate bucket name with project ID (cloud-agnostic)
            bucket_name = instruments_config.gcs_bucket_tradfi or instruments_config.get_bucket_for_category("tradfi")
            if not bucket_name:
                # Fallback: construct bucket name using project ID
                bucket_name = f"instruments-store-tradfi-{self.project_id}"
            gcs_path = f"gs://{bucket_name}/corporate_actions/"

            logger.info("\n📤 STEP 8: Uploading to GCS...")
            logger.info(f"📁 Source: {self.base_dir}")
            logger.info(f"☁️  Destination: {gcs_path}")

            # Use gsutil to upload entire directory
            cmd = ["gsutil", "-m", "cp", "-r", f"{self.base_dir}/*", gcs_path]

            subprocess.run(cmd, capture_output=True, text=True, check=True)

            logger.info("✅ Successfully uploaded to GCS")
            logger.info(f"☁️  Location: {gcs_path}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ GCS upload failed: {e}")
            logger.error(f"stderr: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error during GCS upload: {e}")
            return False

    def run(
        self,
        tickers: Optional[list[str]] = None,
        parallel_workers: int = 2,  # Changed default from 10 to 2
        max_retries: int = 3,
        upload_to_gcs: bool = True,  # New parameter to control GCS upload
        **kwargs: object,
    ) -> dict[str, HandlerResultValue]:
        """
        Execute complete production pipeline.

        Args:
            tickers: Optional list of tickers (defaults to all from GCS)
            parallel_workers: Number of parallel workers
            max_retries: Maximum retry attempts per ticker

        Returns:
            Result dictionary with statistics
        """
        logger.info("🚀 Starting Corporate Actions Production Pipeline")
        logger.info("=" * 60)

        # Step 1: Load metadata
        logger.info("\n📂 STEP 1: Loading metadata from storage...")
        metadata = self._load_metadata()

        # Step 2: Get ticker list
        logger.info("\n📋 STEP 2: Getting ticker list...")
        if tickers:
            ticker_list = [t.upper() for t in tickers]
            logger.info(f"Using provided tickers: {', '.join(ticker_list)}")
        else:
            # Use S&P 500 list from config
            ticker_list = SP500_TICKERS.copy()
            logger.info(f"Using S&P 500 tickers from config ({len(ticker_list)} tickers)")

        logger.info(f"📊 Processing {len(ticker_list)} tickers")
        logger.info(f"📅 Data start date: {corporate_actions_start_date}")

        # Step 3: Fetch data for all symbols (parallel) and save immediately
        logger.info(f"\n💾 STEP 3: Fetching data for all symbols (workers={parallel_workers})...")
        results = []

        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            future_to_ticker = {
                executor.submit(self._fetch_symbol_data, ticker, max_retries): ticker for ticker in ticker_list
            }

            for i, future in enumerate(as_completed(future_to_ticker), 1):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    results.append(result)

                    # Save immediately after fetch (Step 3b)
                    if result["success"]:
                        self._save_by_ticker(
                            result["ticker"],
                            result["dividends_df"],
                            result["splits_df"],
                            result["earnings_df"],
                        )
                        logger.info(f"💾 Saved {ticker} to by_ticker/")

                    pct = (i / len(ticker_list)) * 100
                    logger.info(f"📊 [{i}/{len(ticker_list)}] ({pct:.1f}%) Processed {ticker}")

                except Exception as e:
                    logger.error(f"❌ Unexpected error processing {ticker}: {e}")
                    results.append(
                        {
                            "ticker": ticker,
                            "success": False,
                            "dividends_df": pd.DataFrame(),
                            "splits_df": pd.DataFrame(),
                            "earnings_df": pd.DataFrame(),
                            "error": str(e),
                        }
                    )

        logger.info(
            f"\n✅ STEP 3 Complete: Fetched and saved {len([r for r in results if r['success']])} tickers to by_ticker/"
        )

        # Step 4: Combine into single DataFrames (removed separate save step)
        logger.info("\n🔄 STEP 4: Combining all data into single DataFrames...")

        # Handle empty DataFrames safely
        dividends_list = [r["dividends_df"] for r in results if not r["dividends_df"].empty]
        splits_list = [r["splits_df"] for r in results if not r["splits_df"].empty]
        earnings_list = [r["earnings_df"] for r in results if not r["earnings_df"].empty]

        all_dividends = pd.concat(dividends_list, ignore_index=True) if dividends_list else pd.DataFrame()
        all_splits = pd.concat(splits_list, ignore_index=True) if splits_list else pd.DataFrame()
        all_earnings = pd.concat(earnings_list, ignore_index=True) if earnings_list else pd.DataFrame()

        logger.info(
            f"📊 Combined: {len(all_dividends)} dividends, {len(all_splits)} splits, {len(all_earnings)} earnings"
        )

        # Step 5: Generate by_date partitions
        logger.info("\n📅 STEP 5: Generating by_date partitions...")
        by_date_stats = self._generate_by_date(all_dividends, all_splits, all_earnings)

        logger.info(f"✅ Created {by_date_stats['dividends_dates']} dividend dates")
        logger.info(f"✅ Created {by_date_stats['splits_dates']} split dates")
        logger.info(f"✅ Created {by_date_stats['earnings_dates']} earnings dates")

        # Step 6: Update metadata
        logger.info("\n📝 STEP 6: Updating metadata...")
        for result in results:
            ticker = result["ticker"]
            metadata["tickers"][ticker] = {
                "last_download_date": date.today().isoformat(),
                "status": "active" if result["success"] else "error",
                "stats": {
                    "total_dividends": len(result["dividends_df"]),
                    "total_splits": len(result["splits_df"]),
                    "total_earnings": len(result["earnings_df"]),
                },
            }
            if not result["success"]:
                metadata["tickers"][ticker]["last_error"] = result["error"]

        self._save_metadata(metadata)

        # Step 7: Generate coverage report
        logger.info("\n📊 STEP 7: Generating coverage report...")
        self._generate_coverage_report(results, by_date_stats)

        # Step 8: Upload to GCS (if requested)
        gcs_upload_success = False
        if upload_to_gcs:
            gcs_upload_success = self._upload_to_gcs()
        else:
            logger.info("\n⏭️  Skipping GCS upload (upload_to_gcs=False)")

        # Summary
        successful = [r for r in results if r["success"]]
        logger.info("\n" + "=" * 60)
        logger.info("✅ PIPELINE COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"📊 Tickers processed: {len(successful)}/{len(ticker_list)}")
        logger.info(f"📈 Total events: {len(all_dividends) + len(all_splits) + len(all_earnings)}")
        logger.info(f"📁 Local output: {self.base_dir}")
        if upload_to_gcs:
            logger.info(f"☁️  GCS upload: {'✅ Success' if gcs_upload_success else '❌ Failed'}")
        logger.info("=" * 60)

        return {
            "success": True,
            "status": "success",
            "gcs_upload_success": gcs_upload_success,
            "statistics": {
                "tickers_requested": len(ticker_list),
                "tickers_successful": len(successful),
                "tickers_failed": len(ticker_list) - len(successful),
                "total_dividends": len(all_dividends),
                "total_splits": len(all_splits),
                "total_earnings": len(all_earnings),
                "by_date_stats": by_date_stats,
            },
        }

    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
