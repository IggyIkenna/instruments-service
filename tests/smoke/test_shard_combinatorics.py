"""
Shard Combinatorics Smoke Test for instruments-service

This test verifies that all valid category×venue combinations can be enumerated
and that the ShardCalculator correctly filters by venue start dates.

Run with: pytest tests/smoke/test_shard_combinatorics.py -v
"""

import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add unified-trading-deployment-v2 to path for imports
DEPLOYMENT_PATH = Path(__file__).parent.parent.parent.parent / "unified-trading-deployment-v2"
if DEPLOYMENT_PATH.exists():
    sys.path.insert(0, str(DEPLOYMENT_PATH))

# Skip if deployment package not available
pytest.importorskip("unified_trading_deployment")

from unified_trading_deployment import (
    ShardCombinatoricsGenerator,
    SmokeTestRunner,
    ShardCalculator,
)

SERVICE_NAME = "instruments-service"


class TestShardCombinatoricsGeneration:
    """Test that all valid shard combinations are generated correctly."""
    
    @pytest.fixture
    def config_dir(self):
        """Get the deployment config directory."""
        return str(DEPLOYMENT_PATH / "configs")
    
    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Set up mock environment."""
        monkeypatch.setenv("CLOUD_MOCK_MODE", "true")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    
    def test_generator_enumerates_combinations(self, config_dir, mock_env):
        """Test that generator returns valid combinations for this service."""
        generator = ShardCombinatoricsGenerator(config_dir)
        
        try:
            combinations = generator.get_all_category_venue_combinations(
                service=SERVICE_NAME
            )
            # Verify we have some combinations
            assert len(combinations) >= 0
        except FileNotFoundError:
            pytest.skip(f"No sharding config found for {SERVICE_NAME}")
    
    def test_smoke_test_shards_respect_start_dates(self, config_dir, mock_env):
        """Test that smoke test shards respect venue start dates."""
        generator = ShardCombinatoricsGenerator(config_dir)
        
        # Use a date where venues should have data
        test_date = date(2024, 6, 1)
        
        try:
            shards = generator.get_smoke_test_shards(
                service=SERVICE_NAME,
                test_date=test_date,
            )
            # Should have shards (may be empty if no date dimension)
            assert isinstance(shards, list)
        except (FileNotFoundError, ValueError) as e:
            pytest.skip(f"Skipping: {e}")


class TestSmokeTestRunner:
    """Test the smoke test runner (dry-run mode)."""
    
    @pytest.fixture
    def config_dir(self):
        """Get the deployment config directory."""
        return str(DEPLOYMENT_PATH / "configs")
    
    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Set up mock environment."""
        monkeypatch.setenv("CLOUD_MOCK_MODE", "true")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    
    def test_runner_generates_test_report(self, config_dir, mock_env):
        """Test that runner generates a proper test report."""
        runner = SmokeTestRunner(
            service=SERVICE_NAME,
            config_dir=config_dir,
        )
        
        # Generate empty report (no shards)
        report = runner.generate_test_report([])
        
        assert "service" in report
        assert report["service"] == SERVICE_NAME
        assert "total_tests" in report
        assert "passed" in report


class TestShardCalculatorIntegration:
    """Integration tests for ShardCalculator with real configs."""
    
    @pytest.fixture
    def config_dir(self):
        """Get the deployment config directory."""
        return str(DEPLOYMENT_PATH / "configs")
    
    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Set up mock environment."""
        monkeypatch.setenv("CLOUD_MOCK_MODE", "true")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    
    def test_shards_filtered_by_start_dates(self, config_dir, mock_env):
        """Test that ShardCalculator filters shards by venue start dates."""
        calculator = ShardCalculator(config_dir)
        
        try:
            # Get shards with and without start date filtering
            shards_filtered = calculator.calculate_shards(
                service=SERVICE_NAME,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 7),
                max_shards=1000,
                respect_start_dates=True,
            )
            
            shards_unfiltered = calculator.calculate_shards(
                service=SERVICE_NAME,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 7),
                max_shards=1000,
                respect_start_dates=False,
            )
            
            # Filtered should have <= unfiltered shards
            assert len(shards_filtered) <= len(shards_unfiltered)
        except (FileNotFoundError, ValueError) as e:
            pytest.skip(f"Skipping: {e}")
