"""
Performance Tests for Instruments Service

Tests that instrument generation completes within acceptable time limits.
Uses November 10, 2025 as standard test date.

These tests use real services and require GCP credentials.
"""

import os
import time
from datetime import datetime, timezone

import pytest

from instruments_service.app.core.instruments_service import InstrumentsService

# Import get_config from conftest (avoids circular import issues)
from tests.conftest import get_config


class TestPerformance:
    """Performance benchmarks for instrument generation."""

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_cefi_performance(self):
        """Test CEFI instrument generation performance (target: <45s)."""
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
        assert elapsed < 45, f"CEFI generation took {elapsed:.2f}s (target: <45s)"
        assert instruments_count > 0, "No CEFI instruments generated"

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
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
        and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_defi_performance(self):
        """Test DEFI instrument generation performance (target: <180s).

        Note: DEFI is slower due to multiple external API calls (The Graph, AAVE,
        HYPERLIQUID RPC, Uniswap subgraph, etc.) that may timeout or require fallbacks.
        The 180s target accounts for cold-start scenarios with API fallbacks.
        """
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

        # Assert performance target (180s accounts for API fallbacks and cold start)
        assert elapsed < 180, f"DEFI generation took {elapsed:.2f}s (target: <180s)"
        assert instruments_count > 0, "No DEFI instruments generated"

    @pytest.mark.skipif(
        not get_config("GCP_PROJECT_ID")
        and not os.path.exists(os.path.expanduser("~/.config/gcloud/application_default_credentials.json")),
        reason="Requires GCP credentials for performance testing",
    )
    @pytest.mark.asyncio
    async def test_full_pipeline_performance(self):
        """Test full pipeline (CEFI + TRADFI + DEFI) performance (target: <200s).

        Note: Full pipeline includes DEFI which has external API dependencies.
        The 200s target accounts for cold-start scenarios where:
        - CEFI: ~35s (CCXT calls)
        - TRADFI: ~15s (Databento API)
        - DEFI: ~150s (Graph/RPC calls with possible fallbacks)
        With some parallelism, total is usually <150s.
        """
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

        # Assert performance target (200s accounts for DEFI API fallbacks)
        assert elapsed < 200, f"Full pipeline took {elapsed:.2f}s (target: <200s)"
        assert instruments_count > 100, f"Expected >100 instruments, got {instruments_count}"
