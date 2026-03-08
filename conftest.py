"""
Root conftest: set env vars before ANY imports.

Sets CLOUD_MOCK_MODE=true so live-only integration tests are automatically
skipped when GCP credentials point to a test/mock project environment.
"""

import os

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
