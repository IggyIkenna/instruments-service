"""
Pytest configuration and fixtures for instruments-service tests.

Provides:
- Test bucket configuration (market-data-tick-test)
- Automatic test bucket creation and permission setup
- Secret Manager setup for API keys
- Real GCP credentials setup
- Cloud target fixtures for test environment
"""

import os
import pytest
import json
from pathlib import Path
from typing import Optional

# Load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    # Find .env file in instruments-service directory
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
except ImportError:
    # python-dotenv not available, skip loading .env
    pass

# Import unified-cloud-services components
# Handle circular import gracefully - don't skip all tests, just mark as unavailable
UNIFIED_CLOUD_SERVICES_AVAILABLE = False
CloudTarget = None
get_secret_with_fallback = None
storage = None
service_account = None

try:
    from unified_cloud_services import CloudTarget, get_secret_with_fallback
    from google.cloud import storage
    from google.oauth2 import service_account
    UNIFIED_CLOUD_SERVICES_AVAILABLE = True
except (ImportError, AttributeError) as e:
    # If unified-cloud-services has circular import issues, mark as unavailable
    # Individual fixtures will skip tests that require it, but other tests can still run
    UNIFIED_CLOUD_SERVICES_AVAILABLE = False
    # Try to import google.cloud libraries separately (they might be available)
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
    except ImportError:
        pass


def get_config(key: str, default: str = "") -> str:
    """Get config value from environment variable (avoids circular import)."""
    return os.getenv(key, default)


def find_credentials_file() -> Optional[str]:
    """Find GCP credentials file in common locations."""
    project_root = Path(__file__).parent.parent
    cred_locations = [
        project_root / "central-element-323112-e35fb0ddafe2.json",
        project_root.parent / "central-element-323112-e35fb0ddafe2.json",
        (
            Path(get_config("GOOGLE_APPLICATION_CREDENTIALS", ""))
            if get_config("GOOGLE_APPLICATION_CREDENTIALS")
            else None
        ),
    ]

    for loc in cred_locations:
        if loc and Path(loc).exists():
            return str(Path(loc).absolute())

    return None


@pytest.fixture(scope="session")
def gcp_credentials():
    """Setup GCP credentials for tests."""
    cred_file = find_credentials_file()
    if cred_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_file
        return cred_file
    else:
        pytest.skip("GCP credentials file not found")


@pytest.fixture(scope="session")
def gcp_project_id():
    """GCP project ID for tests."""
    return get_config("GCP_PROJECT_ID", "central-element-323112")


@pytest.fixture(scope="session")
def test_bucket_name():
    """Test bucket name (market-data-tick-test)."""
    return get_config("INSTRUMENTS_GCS_BUCKET_TEST", "market-data-tick-test")


@pytest.fixture(scope="session")
def prod_bucket_name():
    """Prod bucket name (for verification that we don't write to it)."""
    return get_config("INSTRUMENTS_GCS_BUCKET", "market-data-tick")


@pytest.fixture(scope="session")
def bigquery_dataset():
    """BigQuery dataset for tests."""
    return get_config("INSTRUMENTS_BIGQUERY_DATASET", "market_data_hft")


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
        # Import google.cloud libraries if not already imported
        if storage is None or service_account is None:
            from google.cloud import storage
            from google.oauth2 import service_account
        
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
    location = get_config("GCS_LOCATION", "asia-northeast1")

    # Ensure test bucket exists
    ensure_test_bucket_exists(
        project_id=gcp_project_id,
        bucket_name=test_bucket_name,
        credentials_file=gcp_credentials,
        location=location,
    )

    yield

    # Cleanup: Could delete test bucket here if desired, but we keep it for reuse


@pytest.fixture(scope="session")
def test_cloud_target(gcp_project_id, test_bucket_name, bigquery_dataset, ensure_test_resources):
    """Cloud target configured for test bucket."""
    if not UNIFIED_CLOUD_SERVICES_AVAILABLE or CloudTarget is None:
        pytest.skip("unified-cloud-services not available")
    return CloudTarget(
        project_id=gcp_project_id,
        gcs_bucket=test_bucket_name,
        bigquery_dataset=bigquery_dataset,
        bigquery_location=get_config(
            "BIGQUERY_LOCATION", "asia-northeast1"
        ),  # Default to asia-northeast1 per .env
    )


@pytest.fixture(scope="session")
def tardis_api_key(gcp_project_id, gcp_credentials):
    """Get Tardis API key from Secret Manager."""
    if not UNIFIED_CLOUD_SERVICES_AVAILABLE or get_secret_with_fallback is None:
        pytest.skip("unified-cloud-services not available")
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
    sample_dir = Path(get_config("CSV_SAMPLE_DIR", "./data/samples"))
    sample_dir.mkdir(parents=True, exist_ok=True)
    return sample_dir


@pytest.fixture(autouse=True)
def setup_test_environment(gcp_credentials, test_bucket_name):
    """Automatically setup test environment for all tests."""
    # Ensure test bucket is used (not prod)
    os.environ["INSTRUMENTS_GCS_BUCKET_TEST"] = test_bucket_name
    # Enable CSV sampling for tests if not explicitly disabled
    if "ENABLE_CSV_SAMPLING" not in os.environ:
        os.environ["ENABLE_CSV_SAMPLING"] = "true"
    yield
    # Cleanup if needed
    pass
