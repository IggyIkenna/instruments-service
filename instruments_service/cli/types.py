"""
Type definitions for CLI handlers.

This module defines TypedDict structures to replace Any types throughout
the CLI handlers, providing better type safety and IDE support.
"""

from typing import NotRequired, TypedDict


class HandlerConfig(TypedDict):
    """Configuration dictionary for CLI handlers."""

    project_id: NotRequired[object]
    gcp_project_id: NotRequired[object]
    gcs_bucket_tradfi: NotRequired[object]
    gcs_bucket_cefi: NotRequired[object]
    gcs_bucket_defi: NotRequired[object]
    events_bucket: NotRequired[object]


class TickerMetadata(TypedDict):
    """Metadata for a single ticker in the registry."""

    fetch_history: NotRequired[list["FetchHistoryEntry"]]
    last_updated: NotRequired[str]
    status: NotRequired[str]


class FetchHistoryEntry(TypedDict):
    """Entry in ticker fetch history."""

    timestamp: str
    success: bool
    events_count: NotRequired[int]
    error: NotRequired[str]


class TickerRegistry(TypedDict):
    """Complete ticker registry structure."""

    tickers: dict[str, TickerMetadata]
    last_updated: NotRequired[str]
    version: NotRequired[str]


class BackfillResult(TypedDict):
    """Result from backfilling a single ticker."""

    ticker: str
    success: bool
    events_count: NotRequired[int]
    error: NotRequired[str]
    timestamp: str
    dividends_count: NotRequired[int]
    splits_count: NotRequired[int]
    earnings_count: NotRequired[int]


class CoverageReport(TypedDict):
    """Coverage report for backfill operation."""

    total_tickers: int
    successful_tickers: int
    failed_tickers: int
    total_events: int
    report_timestamp: str
    coverage_percentage: float
    failed_ticker_list: list[str]


class ProductionMetadata(TypedDict):
    """Metadata for production corporate actions."""

    last_run_date: NotRequired[str]
    run_history: NotRequired[list["ProductionRunEntry"]]
    version: NotRequired[str]


class ProductionRunEntry(TypedDict):
    """Entry in production run history."""

    date: str
    timestamp: str
    success: bool
    tickers_processed: int
    events_found: int
    error: NotRequired[str]


class BundleStats(TypedDict):
    """Statistics for a date bundle."""

    date: str
    tickers_processed: int
    total_events: int
    dividends: int
    splits: int
    earnings: int
    success_rate: float


class OperationStats(TypedDict):
    """Overall operation statistics."""

    start_time: str
    end_time: str
    duration_seconds: float
    bundles_processed: int
    total_tickers: int
    total_events: int
    success_rate: float


class BackfillStats(TypedDict):
    """Statistics for backfill operation."""

    tickers_requested: int
    tickers_successful: int
    tickers_failed: int
    total_dividends: int
    total_splits: int
    total_earnings: int


class CliArgs(TypedDict, total=False):
    """CLI arguments for handlers."""

    # Common arguments
    start_date: str
    end_date: str
    force: bool
    upload_to_gcs: bool
    parallel_workers: int

    # Live mode arguments
    interval: int
    category: list[str]
    venues: list[str] | None

    # Backfill arguments
    tickers: list[str] | None
    append_mode: bool
    max_retries: int

    # Date views arguments
    input_dir: str
    output_dir: str
