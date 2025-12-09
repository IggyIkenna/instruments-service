"""
Unit tests for CLI parser.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import pytest
import sys
from unittest.mock import patch

from instruments_service.cli.parser import parse_arguments, validate_arguments


def test_parse_arguments_instruments_mode():
    """Test parsing arguments for instruments mode."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--end-date",
        "2023-05-23",
        "--force",
    ]

    with patch.object(sys, "argv", ["parser"] + test_args):
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.start_date == "2023-05-23"
        assert args.end_date == "2023-05-23"
        assert args.force is True


def test_parse_arguments_instruments_mode_with_categories():
    """Test parsing arguments for instruments mode with market categories."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--CEFI",
        "--TRADFI",
    ]

    with patch.object(sys, "argv", ["parser"] + test_args):
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.start_date == "2023-05-23"
        assert args.CEFI is True
        assert args.TRADFI is True
        assert args.DEFI is False


def test_validate_arguments_instruments_mode():
    """Test validation for instruments mode."""
    from argparse import Namespace

    # Valid arguments
    args = Namespace(mode="instruments", start_date="2023-05-23", end_date="2023-05-23")
    validate_arguments(args)  # Should not raise

    # Missing start_date
    args = Namespace(mode="instruments", start_date=None, end_date="2023-05-23")
    with pytest.raises(ValueError, match="--start-date is required"):
        validate_arguments(args)


def test_validate_arguments_default_end_date():
    """Test that end_date defaults to start_date if not provided."""
    from argparse import Namespace

    args = Namespace(mode="instruments", start_date="2023-05-23", end_date=None)
    validate_arguments(args)
    assert args.end_date == "2023-05-23"
