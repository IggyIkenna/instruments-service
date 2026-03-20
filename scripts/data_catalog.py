#!/usr/bin/env python3
"""
Data Catalog Script for instruments-service.

Generates a catalog of instrument definitions in GCS, showing completion
status per category and date range.

Usage:
    python scripts/data_catalog.py --start-date 2024-01-01 --end-date 2024-01-31
    python scripts/data_catalog.py --start-date 2024-01-01 --end-date 2024-01-31 --category CEFI
    python scripts/data_catalog.py --start-date 2024-01-01 --end-date 2024-01-31 --output catalog.json
"""

import argparse
import io
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TypedDict
from uuid import uuid4

from unified_cloud_interface import download_from_storage
from unified_config_interface import UnifiedCloudConfig
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Cloud project/account configuration via UnifiedCloudConfig (fail-fast)
# scripts only: UnifiedCloudConfig() single use — caching not required
_cloud_config = UnifiedCloudConfig()
PROJECT_ID = _cloud_config.gcp_project_id
if not PROJECT_ID:
    raise ValueError("PROJECT_ID not found. Set GCP_PROJECT_ID environment variable in .env or environment.")
CLOUD_PROVIDER = _cloud_config.cloud_provider.lower() if _cloud_config.cloud_provider else "gcp"
CATEGORIES = ["CEFI", "TRADFI", "DEFI"]


def _get_bucket_name(category: str) -> str:
    """Get bucket name for category with multi-cloud support."""
    category_upper = category.upper()

    # Check for bucket override from config
    bucket_attr = f"instruments_gcs_bucket_{category_upper.lower()}"
    bucket_from_config = getattr(_cloud_config, bucket_attr, None)
    if bucket_from_config:
        return str(bucket_from_config)

    # Generate bucket name based on cloud provider
    if CLOUD_PROVIDER == "aws":
        return f"unified-trading-instruments-{category.lower()}-{PROJECT_ID}"
    else:
        return f"instruments-store-{category.lower()}-{PROJECT_ID}"


# Storage bucket configuration (dynamically generated)
GCS_BUCKETS = {
    "CEFI": _get_bucket_name("CEFI"),
    "TRADFI": _get_bucket_name("TRADFI"),
    "DEFI": _get_bucket_name("DEFI"),
}

# Path pattern: instrument_availability/by_date/day={YYYY-MM-DD}/instruments.parquet
PATH_TEMPLATE = "instrument_availability/by_date/day={date}/instruments.parquet"


class CatalogEntryDict(TypedDict):
    """Serialized form of CatalogEntry."""

    category: str
    date: str
    exists: bool
    gcs_path: str
    file_size: int | None
    instrument_count: int | None


class CategorySummaryDict(TypedDict):
    """Serialized form of CategorySummary."""

    category: str
    total_dates: int
    existing_dates: int
    missing_dates: int
    completion_pct: float
    missing_date_list: list[str]


class CatalogReportDict(TypedDict):
    """Serialized form of CatalogReport."""

    service: str
    start_date: str
    end_date: str
    generated_at: str
    overall_completion: float
    category_summaries: list[CategorySummaryDict]
    entries: list[CatalogEntryDict]


@dataclass
class CatalogEntry:
    """Single entry in the data catalog."""

    category: str
    date: str
    exists: bool
    gcs_path: str
    file_size: int | None = None
    instrument_count: int | None = None

    def to_dict(self) -> CatalogEntryDict:
        return CatalogEntryDict(
            category=self.category,
            date=self.date,
            exists=self.exists,
            gcs_path=self.gcs_path,
            file_size=self.file_size,
            instrument_count=self.instrument_count,
        )


@dataclass
class CategorySummary:
    """Summary for a single category."""

    category: str
    total_dates: int
    existing_dates: int
    missing_dates: int
    completion_pct: float
    missing_date_list: list[str] = field(default_factory=list)

    def to_dict(self) -> CategorySummaryDict:
        return CategorySummaryDict(
            category=self.category,
            total_dates=self.total_dates,
            existing_dates=self.existing_dates,
            missing_dates=self.missing_dates,
            completion_pct=self.completion_pct,
            missing_date_list=self.missing_date_list[:10],
        )


@dataclass
class CatalogReport:
    """Complete catalog report."""

    service: str
    start_date: str
    end_date: str
    generated_at: str
    overall_completion: float
    category_summaries: list[CategorySummary]
    entries: list[CatalogEntry]

    def to_dict(self) -> CatalogReportDict:
        return CatalogReportDict(
            service=self.service,
            start_date=self.start_date,
            end_date=self.end_date,
            generated_at=self.generated_at,
            overall_completion=self.overall_completion,
            category_summaries=[s.to_dict() for s in self.category_summaries],
            entries=[e.to_dict() for e in self.entries],
        )


class _BucketDownloader:
    """Minimal UCI-backed storage reader for catalog checks."""

    def __init__(self, bucket: str) -> None:
        self._bucket = bucket

    def download_from_gcs(
        self,
        gcs_path: str,
        format: str = "parquet",
        log_errors: bool = True,
    ) -> object:
        import pandas as pd

        path = gcs_path.lstrip("/")
        data = download_from_storage(self._bucket, path)
        if format == "parquet":
            return pd.read_parquet(io.BytesIO(data))
        return data


def get_cloud_service(bucket_name: str) -> _BucketDownloader | None:
    """Get cloud-agnostic service for a bucket."""
    mock_mode = _cloud_config.is_mock_mode()

    if mock_mode:
        logger.warning("Running in MOCK mode - no real storage checks")
        return None

    try:
        return _BucketDownloader(bucket=bucket_name)
    except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        logger.warning("Could not initialize cloud service: %s", e)
        return None


def check_file_exists(service, bucket_name: str, blob_path: str) -> tuple:
    """
    Check if a file exists using cloud-agnostic service.

    Returns:
        Tuple of (exists: bool, size: Optional[int])
    """
    if service is None:
        return False, None

    try:
        # Try to download (with log_errors=False to avoid logging expected misses)
        df = service.download_from_gcs(gcs_path=blob_path, format="parquet", log_errors=False)
        if not df.empty:
            # Estimate size (approximate)
            size = len(df) * len(df.columns) * 8  # Rough estimate
            return True, size
        return False, None
    except (OSError, ValueError, RuntimeError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
        logger.debug("Error checking %s/%s: %s", bucket_name, blob_path, e)
        return False, None


def generate_date_range(start_date: date, end_date: date) -> list[date]:
    """Generate all dates in range."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def generate_catalog(
    start_date: date,
    end_date: date,
    categories: list[str] | None = None,
    include_entries: bool = True,
) -> CatalogReport:
    """
    Generate the data catalog.

    Args:
        start_date: Start of date range
        end_date: End of date range
        categories: Categories to check (default: all)
        include_entries: Whether to include individual entries

    Returns:
        CatalogReport with results
    """
    if categories is None:
        categories = CATEGORIES

    dates = generate_date_range(start_date, end_date)

    entries = []
    category_summaries = []
    total_existing = 0
    total_expected = 0

    for category in categories:
        bucket_name = GCS_BUCKETS.get(category)
        if not bucket_name:
            logger.warning("No bucket configured for category: %s", category)
            continue

        # Create cloud service for this bucket
        service = get_cloud_service(bucket_name)
        if service is None:
            logger.warning("Could not initialize cloud service for %s", bucket_name)
            continue

        existing_count = 0
        missing_dates = []

        for d in dates:
            date_str = d.isoformat()
            blob_path = PATH_TEMPLATE.format(date=date_str)
            gcs_path = f"gs://{bucket_name}/{blob_path}"

            exists, size = check_file_exists(service, bucket_name, blob_path)

            if exists:
                existing_count += 1
            else:
                missing_dates.append(date_str)

            if include_entries:
                entries.append(
                    CatalogEntry(
                        category=category,
                        date=date_str,
                        exists=exists,
                        gcs_path=gcs_path,
                        file_size=size,
                    )
                )

        total_expected += len(dates)
        total_existing += existing_count

        completion_pct = (existing_count / len(dates) * 100) if dates else 0

        category_summaries.append(
            CategorySummary(
                category=category,
                total_dates=len(dates),
                existing_dates=existing_count,
                missing_dates=len(missing_dates),
                completion_pct=round(completion_pct, 2),
                missing_date_list=missing_dates,
            )
        )

        logger.info("%s: %s/%s dates (%.1f%%)", category, existing_count, len(dates), completion_pct)

    overall_completion = (total_existing / total_expected * 100) if total_expected > 0 else 0

    return CatalogReport(
        service="instruments-service",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        generated_at=datetime.now(UTC).isoformat(),
        overall_completion=round(overall_completion, 2),
        category_summaries=category_summaries,
        entries=entries if include_entries else [],
    )


def print_report(report: CatalogReport) -> None:
    """Log a human-readable report."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("INSTRUMENTS-SERVICE DATA CATALOG")
    logger.info("=" * 60)
    logger.info("Date Range: %s to %s", report.start_date, report.end_date)
    logger.info("Generated: %s", report.generated_at)
    logger.info("Overall Completion: %s%%", report.overall_completion)
    logger.info("")

    for summary in report.category_summaries:
        status = "COMPLETE" if summary.completion_pct == 100 else "PARTIAL" if summary.completion_pct > 0 else "MISSING"
        logger.info(
            "[%s] %s: %s%% (%s/%s dates)",
            status,
            summary.category,
            summary.completion_pct,
            summary.existing_dates,
            summary.total_dates,
        )

        if summary.missing_dates > 0 and summary.missing_date_list:
            missing_preview = ", ".join(summary.missing_date_list[:5])
            if summary.missing_dates > 5:
                logger.info("   Missing: %s ... and %s more", missing_preview, summary.missing_dates - 5)
            else:
                logger.info("   Missing: %s", missing_preview)

    logger.info("")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate data catalog for instruments-service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--category",
        type=str,
        choices=CATEGORIES,
        action="append",
        help="Category to check (can specify multiple, default: all)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--no-entries",
        action="store_true",
        help="Exclude individual entries from output (summary only)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )

    args = parser.parse_args()

    categories = args.category if args.category else None

    report = generate_catalog(
        start_date=args.start_date,
        end_date=args.end_date,
        categories=categories,
        include_entries=not args.no_entries,
    )

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info("Report saved to %s", args.output)

    if args.json:
        logger.info(json.dumps(report.to_dict(), indent=2))
    else:
        print_report(report)

    # Exit with non-zero if not 100% complete
    if report.overall_completion < 100:
        sys.exit(1)


if __name__ == "__main__":
    main()
