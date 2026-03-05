"""
Generate Date Views Handler

Transforms by_ticker data into by_date structure (NO API CALLS).
This is Phase 2 of the optimized corporate actions pipeline.

Key Features:
- Reads all by_ticker CSVs
- Groups by date (ex_date, effective_date, earnings_date)
- Generates by_date Parquet files
- No API calls - pure data transformation

Usage:
    python -m instruments_service \\
        --mode generate_date_views \\
        --input-dir corporate_actions_backfill_output/by_ticker \\
        --output-dir corporate_actions_output/by_date

Input Structure:
    by_ticker/
    ├── AAPL/
    │   ├── dividends.csv
    │   ├── splits.csv
    │   └── earnings.csv
    └── ...

Output Structure:
    by_date/
    ├── day-2024-01-04/
    │   └── dividends.parquet
    ├── day-2024-02-09/
    │   └── dividends.parquet
    └── ...
"""

import logging
from datetime import date
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

import pandas as pd
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler

logger = logging.getLogger(__name__)


class _ActionTypeStats(TypedDict):
    total_records: int
    dates_with_data: int
    date_counts: dict[str, int]


class GenerateDateViewsHandler(ModeHandler):
    """
    Handler for generating date-partitioned views from by_ticker data.

    This is a pure data transformation - no API calls are made.
    Reads CSV files from by_ticker/ and generates Parquet files in by_date/.

    Phase 2 of the optimized pipeline.
    """

    def __init__(self, config: dict[str, object]) -> None:
        super().__init__(config)
        logger.info("✅ GenerateDateViewsHandler initialized")

    def _load_all_tickers_data(
        self,
        by_ticker_dir: Path,
        action_type: str,
    ) -> pd.DataFrame:
        """
        Load all ticker data for a specific action type.

        Args:
            by_ticker_dir: Directory containing by_ticker/{ticker}/ folders
            action_type: One of 'dividends', 'splits', 'earnings'

        Returns:
            Combined DataFrame with all tickers
        """
        all_data: list[pd.DataFrame] = []
        filename = f"{action_type}.csv"

        ticker_dirs = [d for d in by_ticker_dir.iterdir() if d.is_dir()]

        for ticker_dir in ticker_dirs:
            csv_path = ticker_dir / filename
            if not csv_path.exists():
                continue

            try:
                df = pd.read_csv(csv_path)
                if not df.empty:
                    all_data.append(df)
            except (OSError, PermissionError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.HIGH,
                    recovery_strategy=ErrorRecoveryStrategy.RETRY,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e
        if not all_data:
            return pd.DataFrame()

        combined = pd.concat(all_data, ignore_index=True)
        logger.info("📂 Loaded %s %s records from %s tickers", len(combined), action_type, len(all_data))
        return combined

    def _generate_date_partitions(
        self,
        df: pd.DataFrame,
        date_column: str,
        action_type: str,
        output_dir: Path,
    ) -> dict[str, int]:
        """
        Generate date-partitioned Parquet files.

        Args:
            df: Combined DataFrame with all data
            date_column: Column name for date partitioning
            action_type: One of 'dividends', 'splits', 'earnings'
            output_dir: Output directory for by_date files

        Returns:
            Dict mapping date -> number of records
        """
        if df.empty:
            logger.info("No %s data to partition", action_type)
            return {}

        # Convert date column to date type
        df[date_column] = pd.to_datetime(df[date_column], utc=True).dt.date

        # Group by date
        date_groups = df.groupby(date_column)

        date_counts: dict[str, int] = {}
        for day, group in date_groups:
            if pd.isna(day) or not isinstance(day, date):
                continue

            day_str = day.isoformat()
            day_dir = output_dir / f"day={day_str}"
            day_dir.mkdir(parents=True, exist_ok=True)

            # Save as Parquet
            output_path = day_dir / f"{action_type}.parquet"
            group.to_parquet(output_path, index=False)

            date_counts[day_str] = len(group)
            logger.debug("💾 Saved %s %s for %s", len(group), action_type, day_str)

        logger.info("✅ Generated %s date partitions for %s", len(date_counts), action_type)
        return date_counts

    def run(
        self,
        input_dir: str | None = None,
        output_dir: str | None = None,
        upload_to_gcs: bool = False,
        **kwargs: object,
    ) -> dict[str, HandlerResultValue]:
        """
        Execute date views generation.

        Args:
            input_dir: Input directory with by_ticker data (default: corporate_actions_backfill_output/by_ticker)
            output_dir: Output directory for by_date files (default: corporate_actions_output/by_date)
            upload_to_gcs: Whether to upload to GCS (default: False for testing)

        Returns:
            Result dictionary with status and statistics
        """
        # Set default directories
        if input_dir is None:
            input_dir = "corporate_actions_backfill_output/by_ticker"
        if output_dir is None:
            output_dir = "corporate_actions_output/by_date"

        by_ticker_dir = Path(input_dir)
        by_date_dir = Path(output_dir)

        # Validate input directory
        if not by_ticker_dir.exists():
            raise ValueError(f"Input directory not found: {by_ticker_dir}")

        # Create output directory
        by_date_dir.mkdir(parents=True, exist_ok=True)

        logger.info("🚀 Generating date views")
        logger.info("📂 Input: %s", by_ticker_dir)
        logger.info("📂 Output: %s", by_date_dir)

        action_types: dict[str, _ActionTypeStats] = {}

        # Process each action type
        for action_type in ["dividends", "splits", "earnings"]:
            logger.info("\n📊 Processing %s...", action_type)

            # Determine date column
            if action_type == "dividends":
                date_column = "ex_date"
            elif action_type == "splits":
                date_column = "effective_date"
            else:  # earnings
                date_column = "earnings_date"

            # Load all ticker data
            combined_df = self._load_all_tickers_data(by_ticker_dir, action_type)

            # Generate date partitions
            date_counts = self._generate_date_partitions(
                combined_df,
                date_column,
                action_type,
                by_date_dir,
            )

            action_types[action_type] = {
                "total_records": len(combined_df),
                "dates_with_data": len(date_counts),
                "date_counts": date_counts,
            }

        # Summary statistics
        total_records = sum(s["total_records"] for s in action_types.values())
        total_dates = sum(s["dates_with_data"] for s in action_types.values())

        logger.info("\n✅ Date views generation complete")
        logger.info("📈 Total records: %s", total_records)
        logger.info("📈 Total date files: %s", total_dates)

        # TODO: Upload to GCS if requested
        if upload_to_gcs:
            logger.warning("⚠️ GCS upload not yet implemented (testing phase)")

        return {
            "success": True,
            "status": "success",
            "statistics": {"action_types": action_types},
        }

    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
