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
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Cloud project/account configuration (multi-cloud support)
PROJECT_ID = (
    os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("AWS_PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or os.environ.get("AWS_ACCOUNT_ID")
)
if not PROJECT_ID:
    raise ValueError("PROJECT_ID not found. Set GCP_PROJECT_ID or AWS_PROJECT_ID environment variable.")
CLOUD_PROVIDER = os.environ.get("CLOUD_PROVIDER", "gcp").lower()
CATEGORIES = ["CEFI", "TRADFI", "DEFI"]


def _get_bucket_name(category: str) -> str:
    """Get bucket name for category with multi-cloud support."""
    category_upper = category.upper()

    # Check for environment variable override
    env_key = f"INSTRUMENTS_GCS_BUCKET_{category_upper}"
    bucket_from_env = os.environ.get(env_key)
    if bucket_from_env:
        return bucket_from_env

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


@dataclass
class CatalogEntry:
    """Single entry in the data catalog."""

    category: str
    date: str
    exists: bool
    gcs_path: str
    file_size: Optional[int] = None
    instrument_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "date": self.date,
            "exists": self.exists,
            "gcs_path": self.gcs_path,
            "file_size": self.file_size,
            "instrument_count": self.instrument_count,
        }


@dataclass
class CategorySummary:
    """Summary for a single category."""

    category: str
    total_dates: int
    existing_dates: int
    missing_dates: int
    completion_pct: float
    missing_date_list: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "total_dates": self.total_dates,
            "existing_dates": self.existing_dates,
            "missing_dates": self.missing_dates,
            "completion_pct": self.completion_pct,
            "missing_date_list": self.missing_date_list[:10],  # First 10 only
        }


@dataclass
class CatalogReport:
    """Complete catalog report."""

    service: str
    start_date: str
    end_date: str
    generated_at: str
    overall_completion: float
    category_summaries: List[CategorySummary]
    entries: List[CatalogEntry]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "generated_at": self.generated_at,
            "overall_completion": self.overall_completion,
            "category_summaries": [s.to_dict() for s in self.category_summaries],
            "entries": [e.to_dict() for e in self.entries],
        }


def get_cloud_service(bucket_name: str):
    """Get cloud-agnostic service for a bucket."""
    mock_mode = (os.environ.get("CLOUD_MOCK_MODE") or "").lower() == "true"

    if mock_mode:
        logger.warning("Running in MOCK mode - no real storage checks")
        return None

    try:
        from unified_cloud_services import CloudTarget, StandardizedDomainCloudService

        target = CloudTarget(
            project_id=PROJECT_ID,
            gcs_bucket=bucket_name,
            bigquery_dataset="instruments",
        )
        return StandardizedDomainCloudService(domain="instruments", cloud_target=target)
    except Exception as e:
        logger.warning(f"Could not initialize cloud service: {e}")
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
    except Exception as e:
        logger.debug(f"Error checking {bucket_name}/{blob_path}: {e}")
        return False, None


def generate_date_range(start_date: date, end_date: date) -> List[date]:
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
    categories: Optional[List[str]] = None,
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
            logger.warning(f"No bucket configured for category: {category}")
            continue

        # Create cloud service for this bucket
        service = get_cloud_service(bucket_name)
        if service is None:
            logger.warning(f"Could not initialize cloud service for {bucket_name}")
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

        logger.info(f"{category}: {existing_count}/{len(dates)} dates ({completion_pct:.1f}%)")

    overall_completion = (total_existing / total_expected * 100) if total_expected > 0 else 0

    return CatalogReport(
        service="instruments-service",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        generated_at=datetime.now(timezone.utc).isoformat(),
        overall_completion=round(overall_completion, 2),
        category_summaries=category_summaries,
        entries=entries if include_entries else [],
    )


def print_report(report: CatalogReport):
    """Print a human-readable report."""
    print()
    print("=" * 60)
    print("INSTRUMENTS-SERVICE DATA CATALOG")
    print("=" * 60)
    print(f"Date Range: {report.start_date} to {report.end_date}")
    print(f"Generated: {report.generated_at}")
    print(f"Overall Completion: {report.overall_completion}%")
    print()

    for summary in report.category_summaries:
        status = "✅" if summary.completion_pct == 100 else "⏳" if summary.completion_pct > 0 else "❌"
        print(
            f"{status} {summary.category}: {summary.completion_pct}% ({summary.existing_dates}/{summary.total_dates} dates)"
        )

        if summary.missing_dates > 0 and summary.missing_date_list:
            print(f"   Missing: {', '.join(summary.missing_date_list[:5])}", end="")
            if summary.missing_dates > 5:
                print(f" ... and {summary.missing_dates - 5} more")
            else:
                print()

    print()
    print("=" * 60)


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
        logger.info(f"Report saved to {args.output}")

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print_report(report)

    # Exit with non-zero if not 100% complete
    if report.overall_completion < 100:
        sys.exit(1)


if __name__ == "__main__":
    main()
