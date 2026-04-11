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
