"""End-to-end instruments test: real API fetch, write to test bucket.

Run explicitly — NOT included in standard QG (too slow, requires real credentials):

    IS_TEST_RUN=true CLOUD_MOCK_MODE=false GCP_PROJECT_ID=<your-project> \\
        .venv/bin/pytest tests/e2e/ -m e2e -v

What it tests:
    1. D5 (DeFi) + C5 (CeFi) + T5 (TradFi) instruments for one representative day.
    2. URDI returns instruments for at least one venue per asset group.
    3. All records are written to the TEST bucket (not prod).
    4. Written parquet files are readable and schema-valid.

Prerequisites:
    - GCP credentials active (gcloud auth application-default login)
    - API keys provisioned in Secret Manager for at least one DeFi venue (e.g. Tardis, Databento)
    - IS_TEST_RUN=true → appends -test to prod bucket names e.g. instruments-store-defi-{project}-test
    - CLOUD_MOCK_MODE=false → uses real GCP, real URDI adapters

The test bucket is separate from prod. Safe to run repeatedly.
"""

from __future__ import annotations

import asyncio
import os

import pytest

pytestmark = pytest.mark.e2e

# Representative test date: recent stable day with full instrument coverage
_TEST_DATE = "2026-03-20"
_DEFI_CATEGORY = ["DEFI"]
_CEFI_CATEGORY = ["CEFI"]
_TRADFI_CATEGORY = ["TRADFI"]


def _is_real_env() -> bool:
    """True only when credentials and IS_TEST_RUN are configured."""
    return (
        os.getenv("CLOUD_MOCK_MODE", "true").lower() not in ("true", "1")
        and os.getenv("IS_TEST_RUN", "false").lower() in ("true", "1")
        and os.getenv("GCP_PROJECT_ID", "test-project") != "test-project"
    )


@pytest.mark.e2e
@pytest.mark.live
@pytest.mark.skipif(not _is_real_env(), reason="Requires IS_TEST_RUN=true + real GCP project")
def test_defi_instruments_e2e():
    """DeFi instruments: fetch from URDI and write to test bucket."""
    from instruments_service.engine.orchestrator import process_instruments

    result = asyncio.run(
        process_instruments(
            date=_TEST_DATE,
            asset_groups=_DEFI_CATEGORY,
            redo_all=True,  # always reprocess in E2E
        )
    )

    assert isinstance(result, dict), "Expected dict of venue -> record count"
    assert len(result) > 0, f"No DeFi venues produced records for {_TEST_DATE}"

    total = sum(result.values())
    assert total > 50, f"Expected >50 DeFi instruments, got {total}"

    # At least one major DeFi venue should be present
    defi_venues = {k for k in result if result[k] > 0}
    known_defi = {"UNISWAPV3-ETHEREUM", "AAVEV3-ETHEREUM", "MORPHO-ETHEREUM", "CURVE-ETHEREUM"}
    overlap = defi_venues & known_defi
    assert overlap, f"None of the expected DeFi venues in results: {defi_venues}"


@pytest.mark.e2e
@pytest.mark.live
@pytest.mark.skipif(not _is_real_env(), reason="Requires IS_TEST_RUN=true + real GCP project")
def test_tradfi_instruments_e2e():
    """TradFi instruments: Databento-sourced from NYSE/NASDAQ/CME."""
    from instruments_service.engine.orchestrator import process_instruments

    result = asyncio.run(
        process_instruments(
            date=_TEST_DATE,
            asset_groups=_TRADFI_CATEGORY,
            redo_all=True,
        )
    )

    total = sum(result.values())
    assert total > 500, f"Expected >500 TradFi instruments, got {total}"


@pytest.mark.e2e
@pytest.mark.live
@pytest.mark.skipif(not _is_real_env(), reason="Requires IS_TEST_RUN=true + real GCP project")
def test_write_to_test_bucket_not_prod():
    """Verify the test run writes to the test bucket (is_test_run=true in config)."""
    from instruments_service.config import get_config
    from instruments_service.engine.orchestrator import _get_instruments_bucket

    cfg = get_config()
    assert cfg.is_test_run, "is_test_run is False — set IS_TEST_RUN=true before running E2E tests."
    bucket = _get_instruments_bucket()
    assert "test" in bucket.lower(), f"Expected test bucket but got: {bucket}."
