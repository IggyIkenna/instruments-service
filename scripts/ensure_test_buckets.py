import logging
import sys
from uuid import uuid4

from unified_config_interface import UnifiedCloudConfig
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext
from unified_trading_library import get_storage_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def create_bucket_if_not_exists(bucket_name: str, location: str = "asia-northeast1") -> None:
    """Create bucket using get_storage_client from unified-trading-library."""
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            logger.info("Creating bucket %s in %s...", bucket_name, location)
            bucket.create(location=location)
            logger.info("Created %s", bucket_name)
        else:
            logger.info("Bucket %s already exists", bucket_name)
    except (OSError, ValueError, RuntimeError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.error("Error checking/creating %s: %s (correlation_id=%s)", bucket_name, e, _err.correlation_id)


if __name__ == "__main__":
    logger.info("Ensuring test buckets exist...")

    # Get project ID from UnifiedCloudConfig (fail-fast)
    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id

    if not project_id:
        logger.error("PROJECT_ID not found. Set GCP_PROJECT_ID environment variable.")
        sys.exit(1)

    # List of buckets to ensure exist
    # We include both suffix and infix patterns to cover configuration drift
    buckets = [
        f"instruments-store-test-{project_id}",
        # Suffix style (what failing code is currently trying to use)
        f"instruments-store-cefi-{project_id}-test",
        f"instruments-store-tradfi-{project_id}-test",
        f"instruments-store-defi-{project_id}-test",
        # Infix style (what is defined in .env)
        f"instruments-store-test-cefi-{project_id}",
        f"instruments-store-test-tradfi-{project_id}",
        f"instruments-store-test-defi-{project_id}",
    ]

    for b in buckets:
        create_bucket_if_not_exists(b)

    logger.info("Done.")
