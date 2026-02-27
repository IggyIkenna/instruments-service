"""
Extended unit tests for CLI components to increase coverage to 80%+.

Note: Query functionality has been moved to unified-trading-services.
Use InstrumentsDomainClient from unified-trading-services to query instruments.
"""

import argparse
import sys

import pytest

from instruments_service.cli.base_handler import ModeHandler
from instruments_service.cli.parser import parse_arguments, validate_arguments


class TestCLIParserExtended:
    """Extended tests for CLI parser."""

    def test_parser_instruments_mode(self):
        """Test parser with instruments mode."""
        sys.argv = [
            "test",
            "--mode",
            "instruments",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-02",
        ]
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.start_date == "2023-01-01"
        assert args.end_date == "2023-01-02"

    def test_parser_with_all_categories(self):
        """Test parser with all market category flags."""
        sys.argv = [
            "test",
            "--mode",
            "instruments",
            "--start-date",
            "2023-01-01",
            "--CEFI",
            "--TRADFI",
            "--DEFI",
        ]
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.CEFI is True
        assert args.TRADFI is True
        assert args.DEFI is True

    def test_validate_arguments_instruments_mode(self):
        """Test argument validation for instruments mode."""
        args = argparse.Namespace(
            mode="instruments",
            start_date="2023-01-01",
            end_date="2023-01-02",
        )
        # Should not raise
        validate_arguments(args)

    def test_validate_arguments_missing_start_date(self):
        """Test validation error for missing start_date in instruments mode."""
        args = argparse.Namespace(
            mode="instruments",
            start_date=None,
            end_date="2023-01-02",
        )
        with pytest.raises(ValueError, match="--start-date is required"):
            validate_arguments(args)

    def test_validate_arguments_end_date_defaults(self):
        """Test that end_date defaults to start_date."""
        args = argparse.Namespace(
            mode="instruments",
            start_date="2023-01-01",
            end_date=None,
        )
        validate_arguments(args)
        assert args.end_date == "2023-01-01"


class TestModeHandlerExtended:
    """Extended tests for ModeHandler base class."""

    def test_mode_handler_initialization(self):
        """Test ModeHandler initialization."""
        config = {"test": "config"}
        # ModeHandler is abstract, so we can't instantiate it directly
        # Test that it requires config
        with pytest.raises(TypeError):
            # Abstract class can't be instantiated
            ModeHandler(config)

    def test_mode_handler_validation(self):
        """Test ModeHandler config validation."""
        # Test that empty config raises error
        with pytest.raises(ValueError, match="Configuration is required"):
            # We need to create a concrete implementation to test
            class TestHandler(ModeHandler):
                def run(self, **kwargs):
                    return {"status": "success"}

            TestHandler({})  # Empty config should raise
