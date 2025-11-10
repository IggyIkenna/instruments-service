"""
Unit tests for CLI parser.
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


def test_parse_arguments_query_mode():
    """Test parsing arguments for query mode."""
    test_args = [
        "--mode",
        "instruments-query",
        "--start-date",
        "2023-05-23",
        "--query-type",
        "details",
        "--instrument-id",
        "TEST:SPOT_PAIR:BTC-USDT",
    ]

    with patch.object(sys, "argv", ["parser"] + test_args):
        args = parse_arguments()
        assert args.mode == "instruments-query"
        assert args.start_date == "2023-05-23"
        assert args.query_type == "details"
        assert args.instrument_id == "TEST:SPOT_PAIR:BTC-USDT"


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


def test_validate_arguments_query_mode():
    """Test validation for query mode."""
    from argparse import Namespace

    # Valid arguments
    args = Namespace(
        mode="instruments-query", start_date="2023-05-23", query_type="list"
    )
    validate_arguments(args)  # Should not raise

    # Missing instrument_id for details query
    args = Namespace(
        mode="instruments-query",
        start_date="2023-05-23",
        query_type="details",
        instrument_id=None,
    )
    with pytest.raises(ValueError, match="--instrument-id is required"):
        validate_arguments(args)
