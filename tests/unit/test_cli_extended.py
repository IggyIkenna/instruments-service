"""
Extended unit tests for CLI components to increase coverage to 80%+.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
from instruments_service.cli.parser import parse_arguments, validate_arguments
from instruments_service.cli.base_handler import ModeHandler
import argparse


class TestCLIParserExtended:
    """Extended tests for CLI parser."""

    def test_parser_all_modes(self):
        """Test parser with all available modes."""
        # Test instruments mode
        sys.argv = [
            "test",
            "--mode",
            "instruments",
            "--start-date",
            "2025-08-01",
            "--end-date",
            "2025-08-02",
        ]
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.start_date == "2025-08-01"
        assert args.end_date == "2025-08-02"

    def test_validate_arguments_instruments_mode(self):
        """Test argument validation for instruments mode."""
        args = argparse.Namespace(
            mode="instruments",
            start_date="2025-07-01",
            end_date="2025-07-02",
            query_type="list",
            instrument_id=None,
            data_type=None,
        )
        # Should not raise
        validate_arguments(args)

    def test_validate_arguments_query_mode(self):
        """Test argument validation for query mode."""
        args = argparse.Namespace(
            mode="instruments-query",
            start_date="2025-08-01",
            query_type="details",
            instrument_id="TEST:SPOT_PAIR:BTC-USDT",
            data_type=None,
        )
        # Should not raise
        validate_arguments(args)

    def test_validate_arguments_missing_start_date(self):
        """Test validation error for missing start_date in instruments mode."""
        args = argparse.Namespace(
            mode="instruments",
            start_date=None,
            query_type="list",
            instrument_id=None,
            data_type=None,
        )
        with pytest.raises(ValueError, match="--start-date is required"):
            validate_arguments(args)

    def test_validate_arguments_query_details_missing_id(self):
        """Test validation error for missing instrument_id in details query."""
        args = argparse.Namespace(
            mode="instruments-query",
            start_date="2025-08-01",
            query_type="details",
            instrument_id=None,
            data_type=None,
        )
        with pytest.raises(ValueError, match="--instrument-id is required"):
            validate_arguments(args)


class TestModeHandlerExtended:
    """Extended tests for ModeHandler base class."""

    def test_mode_handler_initialization(self):
        """Test ModeHandler initialization."""
        config = {"test": "config"}
        # ModeHandler is abstract, so we can't instantiate it directly
        # Test that it requires config
        with pytest.raises(TypeError):
            # Abstract class can't be instantiated
            handler = ModeHandler(config)

    def test_mode_handler_validation(self):
        """Test ModeHandler config validation."""
        # Test that empty config raises error
        with pytest.raises(ValueError, match="Configuration is required"):
            # We need to create a concrete implementation to test
            class TestHandler(ModeHandler):
                def run(self, **kwargs):
                    return {"status": "success"}

            TestHandler({})  # Empty config should raise
