"""
Unit tests for CLI parser.

Comprehensive tests for all CLI argument combinations used in sharding/deployment.

Note: Query functionality has been moved to unified-cloud-services.
Use InstrumentsDomainClient from unified-cloud-services to query instruments.
"""

import sys
from unittest.mock import patch

import pytest

from instruments_service.cli.parser import parse_arguments, validate_arguments

# =============================================================================
# Instruments Mode Tests
# =============================================================================


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

    with patch.object(sys, "argv", ["parser", *test_args]):
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

    with patch.object(sys, "argv", ["parser", *test_args]):
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


# =============================================================================
# Category Multi-Value Tests (--category CEFI TRADFI)
# =============================================================================


@pytest.mark.parametrize(
    "categories,expected",
    [
        (["CEFI"], ["CEFI"]),
        (["TRADFI"], ["TRADFI"]),
        (["DEFI"], ["DEFI"]),
        (["CEFI", "TRADFI"], ["CEFI", "TRADFI"]),
        (["CEFI", "DEFI"], ["CEFI", "DEFI"]),
        (["TRADFI", "DEFI"], ["TRADFI", "DEFI"]),
        (["CEFI", "TRADFI", "DEFI"], ["CEFI", "TRADFI", "DEFI"]),
    ],
)
def test_parse_category_multi_value(categories, expected):
    """Test --category with multiple values."""
    test_args = ["--mode", "instruments", "--start-date", "2023-05-23", "--category", *categories]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.category == expected


def test_parse_category_case_insensitive():
    """Test that --category accepts lowercase and converts to uppercase."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--category",
        "cefi",
        "tradfi",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.category == ["CEFI", "TRADFI"]


def test_category_flag_vs_category_arg():
    """Test that --category arg and --CEFI flags can coexist."""
    # Using --category
    test_args_category = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--category",
        "CEFI",
        "TRADFI",
    ]

    # Using flags
    test_args_flags = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--CEFI",
        "--TRADFI",
    ]

    with patch.object(sys, "argv", ["parser", *test_args_category]):
        args_cat = parse_arguments()
        assert args_cat.category == ["CEFI", "TRADFI"]

    with patch.object(sys, "argv", ["parser", *test_args_flags]):
        args_flags = parse_arguments()
        assert args_flags.CEFI is True
        assert args_flags.TRADFI is True


# =============================================================================
# Dry Run Tests (--dry-run)
# =============================================================================


def test_parse_dry_run_flag():
    """Test --dry-run flag."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--dry-run",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.dry_run is True


def test_parse_dry_run_default_false():
    """Test --dry-run defaults to False."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.dry_run is False


def test_parse_dry_run_with_force():
    """Test combining --dry-run and --force."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--dry-run",
        "--force",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.dry_run is True
        assert args.force is True


# =============================================================================
# Corporate Actions Mode Tests
# =============================================================================


def test_parse_corporate_actions_mode():
    """Test parsing arguments for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.mode == "corporate_actions"
        assert args.start_date == "2020-01-01"
        assert args.end_date == "2026-01-25"


def test_parse_corporate_actions_with_tickers():
    """Test --tickers for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
        "--tickers",
        "AAPL",
        "MSFT",
        "GOOGL",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.mode == "corporate_actions"
        assert args.tickers == ["AAPL", "MSFT", "GOOGL"]


def test_parse_corporate_actions_output_format_csv():
    """Test --output-format csv for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
        "--output-format",
        "csv",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.output_format == "csv"


def test_parse_corporate_actions_output_format_parquet():
    """Test --output-format parquet (default) for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.output_format == "parquet"  # default


def test_parse_corporate_actions_upload_to_gcs():
    """Test --upload-to-gcs flag for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
        "--upload-to-gcs",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.upload_to_gcs is True


def test_parse_corporate_actions_full_args():
    """Test full argument set for corporate_actions mode."""
    test_args = [
        "--mode",
        "corporate_actions",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-01-25",
        "--tickers",
        "AAPL",
        "MSFT",
        "--output-format",
        "csv",
        "--upload-to-gcs",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.mode == "corporate_actions"
        assert args.start_date == "2020-01-01"
        assert args.end_date == "2026-01-25"
        assert args.tickers == ["AAPL", "MSFT"]
        assert args.output_format == "csv"
        assert args.upload_to_gcs is True


def test_validate_corporate_actions_requires_start_date():
    """Test that corporate_actions mode requires start_date."""
    from argparse import Namespace

    args = Namespace(mode="corporate_actions", start_date=None, end_date="2026-01-25")
    with pytest.raises(ValueError, match="--start-date is required"):
        validate_arguments(args)


def test_validate_corporate_actions_default_end_date():
    """Test that corporate_actions end_date defaults to start_date."""
    from argparse import Namespace

    args = Namespace(mode="corporate_actions", start_date="2020-01-01", end_date=None)
    validate_arguments(args)
    assert args.end_date == "2020-01-01"


# =============================================================================
# Configuration Options Tests
# =============================================================================


def test_parse_project_id():
    """Test --project-id argument."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--project-id",
        "my-custom-project",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.project_id == "my-custom-project"


def test_parse_project_id_default():
    """Test default project-id is None (service config provides default)."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        # Parser returns None, service config provides the default
        assert args.project_id is None


def test_parse_gcs_bucket():
    """Test --gcs-bucket argument."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--gcs-bucket",
        "my-custom-bucket",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.gcs_bucket == "my-custom-bucket"


def test_parse_log_level():
    """Test --log-level argument."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
        "--log-level",
        "DEBUG",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.log_level == "DEBUG"


# =============================================================================
# Full Combinatoric Tests (Sharding Integration)
# =============================================================================


@pytest.mark.parametrize(
    "category_flags",
    [
        ["--CEFI"],
        ["--TRADFI"],
        ["--DEFI"],
        ["--CEFI", "--TRADFI"],
        ["--CEFI", "--DEFI"],
        ["--TRADFI", "--DEFI"],
        ["--CEFI", "--TRADFI", "--DEFI"],
    ],
)
def test_category_flag_combinations(category_flags):
    """Test all combinations of category flags for sharding."""
    test_args = ["--mode", "instruments", "--start-date", "2023-05-23", *category_flags]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.start_date == "2023-05-23"
        # Verify flags are set correctly
        if "--CEFI" in category_flags:
            assert args.CEFI is True
        if "--TRADFI" in category_flags:
            assert args.TRADFI is True
        if "--DEFI" in category_flags:
            assert args.DEFI is True


def test_no_category_flags_means_all():
    """Test that no category flags means process all categories."""
    test_args = [
        "--mode",
        "instruments",
        "--start-date",
        "2023-05-23",
    ]

    with patch.object(sys, "argv", ["parser", *test_args]):
        args = parse_arguments()
        # All flags should be False (meaning process all)
        assert args.CEFI is False
        assert args.TRADFI is False
        assert args.DEFI is False
        assert args.category is None
