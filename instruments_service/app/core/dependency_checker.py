"""
Dependency Checker for instruments-service.

This service is the ROOT of the dependency graph - it has no upstream
data dependencies. All external dependencies are API calls to:
- Tardis API (CeFi)
- Databento API (TradFi)
- The Graph / Protocol APIs (DeFi)

This module provides:
1. Documentation of the dependency position
2. Runtime validation of external API connectivity
3. Output path validation

Usage:
    >>> from instruments_service.app.core.dependency_checker import DependencyChecker
    >>> checker = DependencyChecker()
    >>> status = checker.check_external_apis()
    >>> checker.validate_output_path(category="CEFI", date="2024-01-15")
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from unified_cloud_services import CloudTarget, StandardizedDomainCloudService, get_secret

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


@dataclass
class DependencyStatus:
    """Status of a dependency check."""

    name: str
    available: bool
    message: str
    required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "available": self.available,
            "message": self.message,
            "required": self.required,
        }


@dataclass
class DependencyReport:
    """Report of all dependency checks."""

    service: str
    all_available: bool
    required_available: bool
    checks: List[DependencyStatus]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "all_available": self.all_available,
            "required_available": self.required_available,
            "checks": [c.to_dict() for c in self.checks],
        }


class DependencyChecker:
    """
    Dependency checker for instruments-service.

    instruments-service is the ROOT of the data pipeline - it has NO
    upstream GCS data dependencies. Its dependencies are:

    1. External APIs (for data):
       - Tardis API (CeFi exchanges) - requires API key
       - Databento API (TradFi markets) - requires API key
       - The Graph API (DeFi protocols) - requires API key
       - Protocol-specific APIs (AAVE, Hyperliquid, etc.)

    2. Infrastructure:
       - GCS buckets for output
       - GCP Secret Manager for API keys

    Downstream services that depend on instruments-service:
       - market-tick-data-handler
       - strategy-service
       - execution-services
    """

    # GCS output buckets
    OUTPUT_BUCKETS = {
        "CEFI": "instruments-store-cefi-{project_id}",
        "TRADFI": "instruments-store-tradfi-{project_id}",
        "DEFI": "instruments-store-defi-{project_id}",
    }

    # Output path template
    OUTPUT_PATH_TEMPLATE = "instrument_availability/by_date/day={date}/instruments.parquet"

    # External API dependencies by category
    EXTERNAL_APIS = {
        "CEFI": [
            {"name": "tardis_api", "secret": "tardis-api-key", "required": True},
        ],
        "TRADFI": [
            {"name": "databento_api", "secret": "databento-api-key", "required": True},
        ],
        "DEFI": [
            {"name": "the_graph_api", "secret": "graph-api-key", "required": True},
            {"name": "alchemy_api", "secret": "alchemy-api-key", "required": False},
        ],
    }

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the dependency checker.

        Args:
            project_id: GCP project ID (default from config)
        """
        self.project_id = project_id or instruments_config.gcp_project_id

    def get_upstream_dependencies(self) -> List[str]:
        """
        Get list of upstream service dependencies.

        Returns:
            Empty list - instruments-service has no upstream deps
        """
        return []

    def get_downstream_dependents(self) -> List[str]:
        """
        Get list of services that depend on instruments-service output.

        Returns:
            List of downstream service names
        """
        return [
            "market-tick-data-handler",
            "strategy-service",
            "execution-services",
        ]

    def check_external_apis(
        self,
        categories: Optional[List[str]] = None,
    ) -> DependencyReport:
        """
        Check availability of external APIs.

        Args:
            categories: Categories to check (default: all)

        Returns:
            DependencyReport with API availability status
        """
        if categories is None:
            categories = list(self.EXTERNAL_APIS.keys())

        checks = []
        all_available = True
        required_available = True

        for category in categories:
            apis = self.EXTERNAL_APIS.get(category, [])

            for api in apis:
                status = self._check_api_key(api["name"], api["secret"])
                status.required = api.get("required", True)
                checks.append(status)

                if not status.available:
                    all_available = False
                    if status.required:
                        required_available = False

        return DependencyReport(
            service="instruments-service",
            all_available=all_available,
            required_available=required_available,
            checks=checks,
        )

    def _check_api_key(self, api_name: str, secret_name: str) -> DependencyStatus:
        """Check if an API key is available in Secret Manager."""
        try:
            secret = get_secret(secret_name, project_id=self.project_id)
            if secret:
                return DependencyStatus(
                    name=api_name,
                    available=True,
                    message=f"API key found in Secret Manager ({secret_name})",
                )
            else:
                return DependencyStatus(
                    name=api_name,
                    available=False,
                    message=f"API key not found: {secret_name}",
                )
        except ImportError:
            # Check environment variable as fallback
            env_var = secret_name.upper().replace("-", "_")
            if os.environ.get(env_var):
                return DependencyStatus(
                    name=api_name,
                    available=True,
                    message=f"API key found in environment ({env_var})",
                )
            return DependencyStatus(
                name=api_name,
                available=False,
                message=f"unified-cloud-services not available and {env_var} not set",
            )
        except Exception as e:
            return DependencyStatus(
                name=api_name,
                available=False,
                message=f"Error checking API key: {e}",
            )

    def get_output_path(self, category: str, date: str) -> str:
        """
        Get the expected GCS output path for a date/category.

        Args:
            category: Market category (CEFI, TRADFI, DEFI)
            date: Date string (YYYY-MM-DD)

        Returns:
            Full GCS path
        """
        bucket = self.OUTPUT_BUCKETS[category].format(project_id=self.project_id)
        path = self.OUTPUT_PATH_TEMPLATE.format(date=date)
        return f"gs://{bucket}/{path}"

    def check_output_exists(self, category: str, date: str) -> bool:
        """
        Check if output exists for a date/category.

        Args:
            category: Market category
            date: Date string

        Returns:
            True if output file exists
        """
        try:
            bucket_name = self.OUTPUT_BUCKETS[category].format(project_id=self.project_id)
            blob_path = self.OUTPUT_PATH_TEMPLATE.format(date=date)

            # Use cloud-agnostic service to check existence
            target = CloudTarget(
                project_id=self.project_id,
                gcs_bucket=bucket_name,
                bigquery_dataset="instruments",
            )
            service = StandardizedDomainCloudService(domain="instruments", cloud_target=target)

            # Check if file exists by attempting download with log_errors=False
            df = service.download_from_gcs(gcs_path=blob_path, format="parquet", log_errors=False)
            return not df.empty
        except Exception as e:
            logger.warning(f"Error checking output: {e}")
            return False

    def validate_can_run(
        self,
        categories: Optional[List[str]] = None,
        fail_on_optional: bool = False,
    ) -> bool:
        """
        Validate that the service can run.

        Since instruments-service has no upstream data dependencies,
        this checks external API availability.

        Args:
            categories: Categories to check
            fail_on_optional: Whether to fail on missing optional APIs

        Returns:
            True if service can run

        Raises:
            RuntimeError: If required dependencies are not met
        """
        report = self.check_external_apis(categories)

        if not report.required_available:
            missing = [c for c in report.checks if c.required and not c.available]
            missing_names = [m.name for m in missing]
            raise RuntimeError(f"instruments-service cannot run: missing required API keys: {missing_names}")

        if fail_on_optional and not report.all_available:
            logger.warning("Some optional API keys are missing")
            return False

        return True


# Convenience function for CLI/scripts
def check_dependencies(categories: Optional[List[str]] = None) -> DependencyReport:
    """
    Check all dependencies for instruments-service.

    Args:
        categories: Categories to check (default: all)

    Returns:
        DependencyReport with status
    """
    checker = DependencyChecker()
    return checker.check_external_apis(categories)
