"""
Pytest configuration and fixtures for instruments-service tests.

AUTH PHILOSOPHY
---------------
Auth (GCP credentials, service account tokens) is NOT tested here.
That is CI's job:
  - GCP_PROJECT_ID presence: validated by QG base-service.sh line 44 + validate-build-auth.py
  - Service account token validity: validated by python-quality-gates.yml env injection
  - Code using auth correctly (UnifiedCloudConfig not os.getenv): enforced by QG STEP 5 static checks

Tests verify CODE BEHAVIOUR, not credential existence.

MARKER SEMANTICS
----------------
  @pytest.mark.unit        — pure Python, no I/O, always run
  @pytest.mark.integration — library contract tests; verify imported symbols exist and
                             have the expected API; no real GCS/PubSub/network calls;
                             always run (no credentials needed)
  @pytest.mark.e2e         — real GCS writes to test bucket; requires IS_TEST_RUN=true
  @pytest.mark.live        — live external API calls; requires IS_TEST_RUN=true
"""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest_plugins = ["unified_api_contracts.testing.network_block_plugin"]


def _load_env_early() -> None:
    """Load .env file and set safe defaults for unit/integration test runs."""
    tests_dir = Path(__file__).parent
    project_root = tests_dir.parent
    env_path = project_root / ".env"

    if env_path.exists():
        try:
            from dotenv import load_dotenv

            load_dotenv(dotenv_path=env_path, override=True)
        except ImportError:
            pass

    for key, default_val in [
        ("GCP_PROJECT_ID", "test-project"),
        ("CLOUD_MOCK_MODE", "true"),
        ("INSTRUMENTS_CATALOGUE_PATH", ""),
    ]:
        if not (os.getenv(key) or "").strip():
            os.environ[key] = default_val


_load_env_early()

# ============================================================================
# Now safe to import modules that depend on environment variables
# ============================================================================
import json
import logging

from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from unified_cloud_interface import get_secret_client, get_storage_client
from unified_trading_library import CloudTarget

from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: mark test as integration test (requires external services)")


def get_config(key: str, default: str | None = None) -> str | None:
    """
    Get configuration value from environment or instruments_config.

    Args:
        key: Configuration key to retrieve
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    # First try environment variable via config
    from unified_config_interface import UnifiedCloudConfig

    config = UnifiedCloudConfig()
    # For test configuration, allow fallback to raw os.getenv
    env_value = getattr(config, key.lower(), None) or os.getenv(key)
    if env_value is not None:
        return env_value

    # Then try instruments_config
    config_key = key.lower()
    if hasattr(instruments_config, config_key):
        return getattr(instruments_config, config_key)

    return default


@pytest.fixture(scope="session")
def gcp_auth_info():
    """Resolve GCP credentials using SA key file or ADC."""
    creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_file and Path(creds_file).exists():
        credentials = service_account.Credentials.from_service_account_file(creds_file)
        with open(creds_file) as f:
            project_id = json.load(f).get("project_id") or "test-project"
        return credentials, project_id, creds_file
    try:
        credentials, project = default()
        project_id = project or os.getenv("GCP_PROJECT_ID") or "test-project"
        return credentials, project_id, None
    except DefaultCredentialsError:
        pass
    return None, "test-project", None


@pytest.fixture(autouse=True)
def _skip_integration_without_creds(request: pytest.FixtureRequest, gcp_auth_info: tuple) -> None:
    """Skip @pytest.mark.integration tests when no GCP credentials are available."""
    if "integration" in request.keywords:
        credentials, _, _ = gcp_auth_info
        if credentials is None:
            pytest.skip("No GCP credentials — skipping integration test")


@pytest.fixture(scope="session")
def gcp_credentials(gcp_auth_info: tuple) -> str | None:
    """GCP credentials file path (None when using ADC)."""
    _, _, creds_file = gcp_auth_info
    return creds_file


@pytest.fixture(scope="session")
def gcp_project_id():
    """GCP project ID for tests."""
    return instruments_config.gcp_project_id


@pytest.fixture(scope="session")
def test_bucket_name():
    """Test bucket name."""
    return instruments_config.sink_bucket_test


@pytest.fixture(scope="session")
def prod_bucket_name():
    """Prod bucket name (for verification that we don't write to it)."""
    return instruments_config.sink_bucket


@pytest.fixture(scope="session")
def bigquery_dataset():
    """Analytics dataset for tests."""
    return instruments_config.analytics_dataset


def get_service_account_email(credentials_file: str) -> str | None:
    """Extract service account email from credentials file."""
    try:
        with open(credentials_file) as f:
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
        # Set credentials for unified-trading-services
        import os

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file
        storage_client = get_storage_client(project_id=project_id)

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
        logger.info("Creating test bucket: %s in %s", bucket_name, location)
        bucket.create(location=location)
        logger.info("Created test bucket: %s", bucket_name)

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
                logger.info("Granted permissions to %s", service_account_email)
            except Exception as e:
                # If we can't set IAM policy, that's okay - might already have project-level permissions
                # or service account might not have IAM admin permissions (which is fine for tests)
                logger.warning(
                    "Could not set IAM policy (might have project-level permissions): %s",
                    e,
                )

        return True

    except Exception as e:
        pytest.skip(f"Could not create/access test bucket {bucket_name}: {e}")
        return False


@pytest.fixture(scope="session")
def ensure_test_resources(gcp_credentials: str | None, gcp_project_id: str, test_bucket_name: str):
    """
    Ensure test resources (bucket) exist and have proper permissions.

    Automatically creates test bucket if it doesn't exist and grants
    permissions to the service account.
    """
    if gcp_credentials:
        ensure_test_bucket_exists(
            project_id=gcp_project_id,
            bucket_name=test_bucket_name,
            credentials_file=gcp_credentials,
            location=instruments_config.gcs_location,
        )

    yield


@pytest.fixture(scope="session")
def test_cloud_target(gcp_project_id, test_bucket_name, bigquery_dataset, ensure_test_resources):
    """Cloud target configured for test bucket."""
    return CloudTarget(
        project_id=gcp_project_id,
        storage_bucket=test_bucket_name,
        analytics_dataset=bigquery_dataset,
        bigquery_location=instruments_config.bigquery_location,
    )


@pytest.fixture(scope="session")
def tardis_api_key(gcp_project_id: str, gcp_credentials: str | None) -> str:
    """Get Tardis API key from Secret Manager."""
    api_key = get_secret_client(
        project_id=gcp_project_id,
        secret_name="tardis-api-key",
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


@pytest.fixture(autouse=True, scope="session")
def setup_event_logging_for_tests():
    """Initialize event logging in mock mode for all tests."""
    from unified_trading_library import MockEventSink, setup_events

    setup_events(service_name="instruments-service", mode="test", sink=MockEventSink())
    yield


@pytest.fixture(autouse=True)
def _skip_real_infra_without_opt_in(request: pytest.FixtureRequest) -> None:
    """Gate @pytest.mark.e2e and @pytest.mark.live tests behind IS_TEST_RUN=true.

    These markers mean the test makes real GCS writes or real external API calls.
    They require the caller to explicitly opt in — not just having credentials present.

    IS_TEST_RUN=true routes writes to the test bucket (instruments-store-test-*)
    and is the canonical signal that a developer intends to run against real infra.

    Auth correctness (credentials exist, service account is valid) is enforced by
    CI pre-flight, not by test fixtures. We do not inspect credential objects here.
    """
    real_infra_markers = {"e2e", "live"}
    if real_infra_markers.intersection(request.keywords):
        is_opted_in = os.getenv("IS_TEST_RUN", "false").lower() in ("true", "1")
        if not is_opted_in:
            pytest.skip("Real-infra test — set IS_TEST_RUN=true to run (writes to test bucket)")


@pytest.fixture
def mock_ib() -> MagicMock:
    """MagicMock(spec=IB) fixture for IBKR adapter unit tests."""
    from ib_insync import IB

    ib = MagicMock(spec=IB)
    ib.reqContractDetailsAsync = AsyncMock(return_value=[])
    ib.reqMatchingSymbolsAsync = AsyncMock(return_value=[])
    return ib


@pytest.fixture
def ibkr_adapter(mock_ib: MagicMock):
    """IBKRReferenceDataAdapter with injected mock IB connection."""
    from instruments_service.reference_data.adapters.tradfi.ibkr import IBKRReferenceDataAdapter

    return IBKRReferenceDataAdapter(ib=mock_ib)
