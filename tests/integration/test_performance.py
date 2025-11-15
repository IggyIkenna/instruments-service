"""
Performance Tests for Instruments Service

Tests that instrument generation completes within acceptable time limits.
Uses November 10, 2025 as standard test date.

These tests use real services and require GCP credentials.
"""

import pytest
import os
import time
from datetime import datetime, timezone
from instruments_service.app.core.instruments_service import InstrumentsService

# Import get_config from conftest (avoids circular import issues)
from tests.conftest import get_config


class TestPerformance:
    """Performance benchmarks for instrument generation."""

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        ),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_cefi_performance(self):
        """Test CEFI instrument generation performance (target: <30s)."""
        config = {
            "project_id": get_config("GCP_PROJECT_ID", "central-element-323112"),
        }
        service = InstrumentsService(config)
        test_date = datetime(2025, 11, 10, tzinfo=timezone.utc)

        start_time = time.time()

        # Generate CEFI instruments
        result = await service.generate_instruments_for_date(
            date=test_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )

        elapsed = time.time() - start_time

        instruments_count = result.get("instruments_generated", 0)
        print(f"\n🚀 CEFI Performance: {elapsed:.2f}s ({instruments_count} instruments)")

        # Assert performance target
        assert elapsed < 30, f"CEFI generation took {elapsed:.2f}s (target: <30s)"
        assert instruments_count > 0, "No CEFI instruments generated"

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        ),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_tradfi_performance(self):
        """Test TRADFI instrument generation performance (target: <30s)."""
        config = {
            "project_id": get_config("GCP_PROJECT_ID", "central-element-323112"),
        }
        service = InstrumentsService(config)
        test_date = datetime(2025, 11, 10, tzinfo=timezone.utc)

        start_time = time.time()

        # Generate TRADFI instruments (CME + VIX)
        result = await service.generate_instruments_for_date(
            date=test_date,
            cefi=False,
            tradfi=True,
            defi=False,
        )

        elapsed = time.time() - start_time

        instruments_count = result.get("instruments_generated", 0)
        print(f"\n🚀 TRADFI Performance: {elapsed:.2f}s ({instruments_count} instruments)")

        # Assert performance target
        assert elapsed < 30, f"TRADFI generation took {elapsed:.2f}s (target: <30s)"
        assert instruments_count > 0, "No TRADFI instruments generated"

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        ),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_defi_performance(self):
        """Test DEFI instrument generation performance (target: <40s)."""
        config = {
            "project_id": get_config("GCP_PROJECT_ID", "central-element-323112"),
        }
        service = InstrumentsService(config)
        test_date = datetime(2025, 11, 10, tzinfo=timezone.utc)

        start_time = time.time()

        # Generate DEFI instruments
        result = await service.generate_instruments_for_date(
            date=test_date,
            cefi=False,
            tradfi=False,
            defi=True,
        )

        elapsed = time.time() - start_time

        instruments_count = result.get("instruments_generated", 0)
        print(f"\n🚀 DEFI Performance: {elapsed:.2f}s ({instruments_count} instruments)")

        # Assert performance target
        assert elapsed < 40, f"DEFI generation took {elapsed:.2f}s (target: <40s)"
        assert instruments_count > 0, "No DEFI instruments generated"

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        ),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_full_pipeline_performance(self):
        """Test full pipeline (CEFI + TRADFI + DEFI) performance (target: <60s)."""
        config = {
            "project_id": get_config("GCP_PROJECT_ID", "central-element-323112"),
        }
        service = InstrumentsService(config)
        test_date = datetime(2025, 11, 10, tzinfo=timezone.utc)

        start_time = time.time()

        # Generate all instruments
        result = await service.generate_instruments_for_date(
            date=test_date,
            cefi=True,
            tradfi=True,
            defi=True,
        )

        elapsed = time.time() - start_time

        instruments_count = result.get("instruments_generated", 0)
        print(f"\n🚀 FULL Pipeline Performance: {elapsed:.2f}s ({instruments_count} instruments)")

        # Assert performance target
        assert elapsed < 60, f"Full pipeline took {elapsed:.2f}s (target: <60s)"
        assert instruments_count > 100, f"Expected >100 instruments, got {instruments_count}"
