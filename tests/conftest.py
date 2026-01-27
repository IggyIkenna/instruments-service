"""
Pytest configuration and fixtures for instruments-service tests.

Provides:
- Test bucket configuration (market-data-tick-test)
- Automatic test bucket creation and permission setup
- Secret Manager setup for API keys
- Real GCP credentials setup
- Cloud target fixtures for test environment
"""

# ============================================================================
# CRITICAL: Load .env file FIRST, before ANY imports that depend on env vars
# This must happen at module level, before pytest collects fixtures
# ============================================================================
import os
from pathlib import Path

def _load_env_early():
    """Load .env file and resolve relative credential paths."""
    # Find the project root (parent of tests directory)
    tests_dir = Path(__file__).parent
    project_root = tests_dir.parent
    env_path = project_root / ".env"

    if env_path.exists():
        try:
            from dotenv import load_dotenv
            # Use override=True to ensure .env values take precedence over shell environment
            # This prevents stale/invalid shell environment variables from breaking tests
            load_dotenv(dotenv_path=env_path, override=True)

            # Resolve relative GOOGLE_APPLICATION_CREDENTIALS path
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path and not Path(creds_path).is_absolute():
                abs_creds_path = (project_root / creds_path).resolve()
                if abs_creds_path.exists():
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(abs_creds_path)
                else:
                    # Try parent directory
                    parent_creds = project_root.parent / creds_path
                    if parent_creds.exists():
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(parent_creds.resolve())
        except ImportError:
            # python-dotenv not installed, skip
            pass

# Load env vars immediately at import time
_load_env_early()

# ============================================================================
# Now safe to import modules that depend on environment variables
# ============================================================================
import pytest
import json
from typing import Optional

from google.cloud import storage
from google.oauth2 import service_account
from unified_cloud_services import CloudTarget, get_secret_with_fallback
from instruments_service.config import instruments_config


def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get configuration value from environment or instruments_config.

    Args:
        key: Configuration key to retrieve
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    # First try environment variable
    env_value = os.getenv(key)
    if env_value is not None:
        return env_value

    # Then try instruments_config
    config_key = key.lower()
    if hasattr(instruments_config, config_key):
        return getattr(instruments_config, config_key)

    return default


def cred_file_exists() -> Optional[str]:
    """Find GCP credentials file in common locations."""
    if os.path.exists(instruments_config.google_application_credentials_path):
        return instruments_config.google_application_credentials_path
    return None


@pytest.fixture(scope="session")
def gcp_credentials():
    """Setup GCP credentials for tests."""
    cred_file = cred_file_exists()
    if not cred_file:
        pytest.skip("GCP credentials file not found")
    return cred_file


@pytest.fixture(scope="session")
def gcp_project_id():
    """GCP project ID for tests."""
    return instruments_config.gcp_project_id


@pytest.fixture(scope="session")
def test_bucket_name():
    """Test bucket name (market-data-tick-test)."""
    return instruments_config.gcs_bucket_test


@pytest.fixture(scope="session")
def prod_bucket_name():
    """Prod bucket name (for verification that we don't write to it)."""
    return instruments_config.gcs_bucket


@pytest.fixture(scope="session")
def bigquery_dataset():
    """BigQuery dataset for tests."""
    return instruments_config.bigquery_dataset


def get_service_account_email(credentials_file: str) -> Optional[str]:
    """Extract service account email from credentials file."""
    try:
        with open(credentials_file, "r") as f:
            creds = json.load(f)
            return creds.get("client_email")
    except Exception:
        return None


def ensure_test_bucket_exists(
    project_id: str,
    bucket_name: str,
    credentials_file: str,
    location: str = "asia-northeast1",
) -> bool:
    """
    Ensure test bucket exists and service account has permissions.

    Creates bucket if it doesn't exist and grants storage.objectAdmin role
    to the service account.

    Returns:
        True if bucket exists and is accessible, False otherwise
    """
    try:
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(credentials_file)
        storage_client = storage.Client(project=project_id, credentials=credentials)

        # Check if bucket exists
        bucket = storage_client.bucket(bucket_name)
        if bucket.exists():
            # Bucket exists, verify we can access it
            try:
                bucket.reload()
                return True
            except Exception as e:
                pytest.skip(f"Test bucket exists but not accessible: {e}")
                return False

        # Bucket doesn't exist, create it
        print(f"📦 Creating test bucket: {bucket_name} in {location}")
        bucket.create(location=location)
        print(f"✅ Created test bucket: {bucket_name}")

        # Grant permissions to service account
        service_account_email = get_service_account_email(credentials_file)
        if service_account_email:
            try:
                # Grant storage.objectAdmin role
                policy = bucket.get_iam_policy(requested_policy_version=3)

                # Find or create binding for storage.objectAdmin role
                binding_found = False
                for binding in policy.bindings:
                    if binding["role"] == "roles/storage.objectAdmin":
                        # Add service account to members if not already present
                        member = f"serviceAccount:{service_account_email}"
                        if member not in binding["members"]:
                            binding["members"].add(member)
                        binding_found = True
                        break

                # Create new binding if role doesn't exist
                if not binding_found:
                    policy.bindings.append(
                        {
                            "role": "roles/storage.objectAdmin",
                            "members": {f"serviceAccount:{service_account_email}"},
                        }
                    )

                bucket.set_iam_policy(policy)
                print(f"✅ Granted permissions to {service_account_email}")
            except Exception as e:
                # If we can't set IAM policy, that's okay - might already have project-level permissions
                # or service account might not have IAM admin permissions (which is fine for tests)
                print(f"⚠️  Could not set IAM policy (might have project-level permissions): {e}")

        return True

    except Exception as e:
        pytest.skip(f"Could not create/access test bucket {bucket_name}: {e}")
        return False


@pytest.fixture(scope="session")
def ensure_test_resources(gcp_credentials, gcp_project_id, test_bucket_name):
    """
    Ensure test resources (bucket) exist and have proper permissions.

    Automatically creates test bucket if it doesn't exist and grants
    permissions to the service account.
    """
    if not gcp_credentials:
        pytest.skip("GCP credentials required for test resource setup")

    # Get location from env (default to asia-northeast1)
    # Ensure test bucket exists
    ensure_test_bucket_exists(
        project_id=gcp_project_id,
        bucket_name=test_bucket_name,
        credentials_file=gcp_credentials,
        location=instruments_config.gcs_location,
    )

    yield

    # Cleanup: Could delete test bucket here if desired, but we keep it for reuse


@pytest.fixture(scope="session")
def test_cloud_target(gcp_project_id, test_bucket_name, bigquery_dataset, ensure_test_resources):
    """Cloud target configured for test bucket."""
    return CloudTarget(
        project_id=gcp_project_id,
        gcs_bucket=test_bucket_name,
        bigquery_dataset=bigquery_dataset,
        bigquery_location=instruments_config.bigquery_location,
    )


@pytest.fixture(scope="session")
def tardis_api_key(gcp_project_id, gcp_credentials):
    """Get Tardis API key from Secret Manager."""
    if not gcp_credentials:
        pytest.skip("GCP credentials required for Secret Manager access")

    api_key = get_secret_with_fallback(
        project_id=gcp_project_id,
        secret_name="tardis-api-key",
        fallback_env_var="TARDIS_API_KEY",
    )

    if not api_key:
        pytest.skip("Tardis API key not available from Secret Manager or env var")

    return api_key


@pytest.fixture(scope="session")
def csv_sample_dir():
    """CSV sample directory for tests."""
    sample_dir = Path(instruments_config.csv_sample_dir)
    sample_dir.mkdir(parents=True, exist_ok=True)
    return sample_dir


@pytest.fixture(autouse=True)
def setup_test_environment(gcp_credentials, test_bucket_name):
    """Automatically setup test environment for all tests."""
    # Ensure test bucket is used (not prod)
    os.environ["INSTRUMENTS_GCS_BUCKET_TEST"] = test_bucket_name

    # Set category-specific test buckets if not already set
    # These are needed by get_bucket_for_category when test_mode=True
    # Note: get_bucket_for_category uses getattr(unified_config, env_var) which requires
    # the attribute to exist. Since BaseServiceConfig doesn't have these fields,
    # we patch get_bucket_for_category to use os.getenv as fallback
    if "INSTRUMENTS_GCS_BUCKET_CEFI_TEST" not in os.environ:
        os.environ["INSTRUMENTS_GCS_BUCKET_CEFI_TEST"] = "instruments-store-test-cefi-central-element-323112"
    if "INSTRUMENTS_GCS_BUCKET_TRADFI_TEST" not in os.environ:
        os.environ["INSTRUMENTS_GCS_BUCKET_TRADFI_TEST"] = "instruments-store-test-tradfi-central-element-323112"
    if "INSTRUMENTS_GCS_BUCKET_DEFI_TEST" not in os.environ:
        os.environ["INSTRUMENTS_GCS_BUCKET_DEFI_TEST"] = "instruments-store-test-defi-central-element-323112"

    # Enable CSV sampling for tests if not explicitly disabled
    if "ENABLE_CSV_SAMPLING" not in os.environ:
        os.environ["ENABLE_CSV_SAMPLING"] = "true"

    # Patch unified_config in unified_cloud_services to use instruments_config
    # This ensures that get_bucket_for_category() uses the correct bucket configuration
    # from instruments-service (which has the category-specific properties)
    # instead of the default BaseServiceConfig
    from unittest.mock import patch

    with patch("unified_cloud_services.core.market_category.unified_config", instruments_config):
        yield

    # Cleanup if needed
    pass
