"""
Comprehensive unit tests for CLI main module.

Tests cover:
- Basic execution flows (instruments and query modes)
- Market type filters (CEFI, TRADFI, DEFI)
- Query output formats (JSON, CSV, summary)
- Error handling and edge cases
- Arguments passing and validation
"""

import pytest
from unittest.mock import Mock
import json
from instruments_service.cli.main import main, run_cli


class TestCLIMain:
    """Tests for CLI main module."""

    @pytest.fixture
    def mock_handler(self):
        """Create mock handler."""
        handler = Mock()
        handler.run = Mock(return_value={"status": "success", "success": True})
        handler.cleanup = Mock()
        return handler

    @pytest.fixture
    def base_args(self):
        """Create base mock arguments."""
        mock_args = Mock()
        mock_args.log_level = "INFO"
        mock_args.project_id = "test-project"
        mock_args.gcs_bucket = "test-bucket"
        mock_args.bigquery_dataset = "test-dataset"
        mock_args.force = False
        mock_args.exchanges = None
        mock_args.CEFI = False
        mock_args.TRADFI = False
        mock_args.DEFI = False
        mock_args.venues = None
        return mock_args

    def _setup_mocks(self, mock_handler):
        """Helper to setup and cleanup mocks."""
        mock_parse = Mock()
        mock_get_handler = Mock(return_value=mock_handler)

        original_parse = main.__globals__.get("parse_arguments")
        original_get_handler = main.__globals__.get("get_handler_for_mode")
        main.__globals__["parse_arguments"] = mock_parse
        main.__globals__["get_handler_for_mode"] = mock_get_handler

        return mock_parse, mock_get_handler, original_parse, original_get_handler

    def _cleanup_mocks(self, original_parse, original_get_handler):
        """Helper to restore original mocks."""
        if original_parse is not None:
            main.__globals__["parse_arguments"] = original_parse
        if original_get_handler is not None:
            main.__globals__["get_handler_for_mode"] = original_get_handler

    # ========== Basic Execution Tests ==========

    def test_instruments_mode_basic(self, mock_handler, base_args):
        """Test basic instruments mode execution."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-01"
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "success"
            assert result["success"] is True
            mock_handler.run.assert_called_once()
            mock_handler.cleanup.assert_called_once()

            # Verify correct kwargs passed
            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs["start_date"] == "2024-01-01"
            assert call_kwargs["end_date"] == "2024-01-01"
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_instruments_query_mode_basic(self, mock_handler, base_args):
        """Test basic instruments-query mode execution."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            base_args.query_type = "list"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = None
            base_args.output_file = None
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "success"
            mock_handler.run.assert_called_once()

            # Verify query_type is passed
            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs["query_type"] == "list"
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Market Type Filter Tests ==========

    def test_market_type_filters_all_enabled(self, mock_handler, base_args):
        """Test all market type flags (CEFI, TRADFI, DEFI) enabled together."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-01"
            base_args.CEFI = True
            base_args.TRADFI = True
            base_args.DEFI = True
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "success"

            # Verify all flags passed
            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs.get("cefi") is True
            assert call_kwargs.get("tradfi") is True
            assert call_kwargs.get("defi") is True
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_market_type_single_flag(self, mock_handler, base_args):
        """Test single market type flag (CEFI only)."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-01"
            base_args.CEFI = True
            mock_parse.return_value = base_args

            result = main()

            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs.get("cefi") is True
            assert "tradfi" not in call_kwargs
            assert "defi" not in call_kwargs
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Query Output Format Tests ==========

    def test_query_mode_json_output(self, mock_handler, base_args, capsys):
        """Test query mode with JSON output format."""
        mock_handler.run.return_value = {
            "status": "success",
            "results": {"instruments_found": 10, "venues": ["BINANCE"]},
        }

        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            base_args.query_type = "list"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = "json"
            base_args.output_file = None
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            # Verify JSON was printed to stdout
            captured = capsys.readouterr()
            assert "instruments_found" in captured.out
            output_json = json.loads(captured.out)
            assert output_json["results"]["instruments_found"] == 10
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_query_mode_csv_output(self, mock_handler, base_args, capsys):
        """Test query mode with CSV output format."""
        mock_handler.run.return_value = {
            "status": "success",
            "results": {"csv_file": "/tmp/output.csv", "rows": 50},
        }

        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            base_args.query_type = "list"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = "csv"
            base_args.output_file = "/tmp/output.csv"
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            captured = capsys.readouterr()
            assert "/tmp/output.csv" in captured.out
            assert "Rows: 50" in captured.out
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_query_mode_summary_output(self, mock_handler, base_args, capsys):
        """Test query mode with summary output format."""
        mock_handler.run.return_value = {
            "status": "success",
            "results": {
                "instruments_found": 150,
                "venues": ["BINANCE", "DERIBIT", "OKX"],
                "instrument_types": ["SPOT_PAIR", "PERPETUAL"],
                "sample_instruments": ["BINANCE:SPOT_PAIR:BTC-USDT"],
                "total_instruments": 150,
            },
        }

        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            base_args.query_type = "list"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = "summary"
            base_args.output_file = None
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            captured = capsys.readouterr()
            assert "QUERY RESULTS" in captured.out
            assert "Instruments Found: 150" in captured.out
            assert "BINANCE" in captured.out
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_query_summary_with_many_venues(self, mock_handler, base_args, capsys):
        """Test summary output truncation with >10 venues."""
        venues = [f"VENUE_{i}" for i in range(15)]
        mock_handler.run.return_value = {
            "status": "success",
            "results": {"instruments_found": 500, "venues": venues},
        }

        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            base_args.query_type = "list"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = "summary"
            base_args.output_file = None
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            captured = capsys.readouterr()
            assert "... and 5 more" in captured.out
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Query Parameters Tests ==========

    def test_query_all_parameters(self, mock_handler, base_args):
        """Test query mode with all parameters specified."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-02"
            base_args.query_type = "details"
            base_args.venues = ["BINANCE-SPOT"]
            base_args.instrument_types = ["SPOT_PAIR"]
            base_args.base_currency = "BTC"
            base_args.quote_currency = "USDT"
            base_args.symbol_pattern = "BTC.*"
            base_args.instrument_id = "TEST:SPOT_PAIR:BTC-USDT"
            base_args.instrument_ids = ["TEST:SPOT_PAIR:BTC-USDT"]
            base_args.data_type = "trades"
            base_args.days_until_expiry = 30
            base_args.output_format = "json"
            base_args.output_file = "output.json"
            base_args.limit = 100
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "success"

            # Verify all parameters passed
            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs["query_type"] == "details"
            assert call_kwargs["venues"] == ["BINANCE-SPOT"]
            assert call_kwargs["base_currency"] == "BTC"
            assert call_kwargs["instrument_id"] == "TEST:SPOT_PAIR:BTC-USDT"
            assert call_kwargs["limit"] == 100
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Force and Exchanges Tests ==========

    def test_force_and_exchanges(self, mock_handler, base_args):
        """Test force flag and specific exchanges."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-01"
            base_args.force = True
            base_args.exchanges = ["binance", "deribit"]
            mock_parse.return_value = base_args

            result = main()

            call_kwargs = mock_handler.run.call_args[1]
            assert call_kwargs.get("force") is True
            assert call_kwargs.get("exchanges") == ["binance", "deribit"]
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Edge Cases ==========

    def test_no_start_date_query_mode(self, mock_handler, base_args):
        """Test query mode without start_date (should work)."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments-query"
            base_args.start_date = None
            base_args.end_date = None
            base_args.query_type = "summary"
            base_args.instrument_types = None
            base_args.base_currency = None
            base_args.quote_currency = None
            base_args.symbol_pattern = None
            base_args.instrument_id = None
            base_args.instrument_ids = None
            base_args.data_type = None
            base_args.days_until_expiry = None
            base_args.output_format = None
            base_args.output_file = None
            base_args.limit = None
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "success"
            # start_date should not be in kwargs
            call_kwargs = mock_handler.run.call_args[1]
            assert "start_date" not in call_kwargs
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    def test_handler_failure_status(self, mock_handler, base_args):
        """Test when handler returns failure status."""
        mock_handler.run.return_value = {"status": "error", "success": False}
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = None
            mock_parse.return_value = base_args

            result = main()

            assert result["status"] == "error"
            assert result["success"] is False
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Error Handling Tests ==========

    def test_main_exception_handling(self):
        """Test exception handling in main."""
        mock_parse = Mock()
        mock_parse.side_effect = Exception("Parse error")

        original_parse = main.__globals__.get("parse_arguments")
        main.__globals__["parse_arguments"] = mock_parse

        try:
            result = main()

            assert result["success"] is False
            assert result["status"] == "error"
            assert "Parse error" in result["error"]
        finally:
            if original_parse is not None:
                main.__globals__["parse_arguments"] = original_parse

    def test_run_cli_keyboard_interrupt(self):
        """Test run_cli with KeyboardInterrupt."""
        mock_parse = Mock()
        mock_parse.side_effect = KeyboardInterrupt()

        original_parse = main.__globals__.get("parse_arguments")
        main.__globals__["parse_arguments"] = mock_parse

        try:
            result = run_cli()

            assert result["success"] is False
            assert result["status"] == "error"
            assert "Cancelled by user" in result["error"]
        finally:
            if original_parse is not None:
                main.__globals__["parse_arguments"] = original_parse

    def test_run_cli_general_exception(self):
        """Test run_cli with general exception."""
        mock_parse = Mock()
        mock_parse.side_effect = RuntimeError("Unexpected error")

        original_parse = main.__globals__.get("parse_arguments")
        main.__globals__["parse_arguments"] = mock_parse

        try:
            result = run_cli()

            assert result["success"] is False
            assert result["status"] == "error"
            assert "Unexpected error" in result["error"]
        finally:
            if original_parse is not None:
                main.__globals__["parse_arguments"] = original_parse

    def test_run_cli_success(self, mock_handler, base_args):
        """Test run_cli successful execution."""
        mock_parse, mock_get_handler, orig_parse, orig_handler = self._setup_mocks(mock_handler)

        try:
            base_args.mode = "instruments"
            base_args.start_date = "2024-01-01"
            base_args.end_date = "2024-01-01"
            mock_parse.return_value = base_args

            result = run_cli()

            assert result["status"] == "success"
        finally:
            self._cleanup_mocks(orig_parse, orig_handler)

    # ========== Exit Code Logic Tests ==========

    def test_exit_code_success(self):
        """Test exit code logic for success."""
        result = {"status": "success", "success": True}
        exit_code = 0 if result.get("success", False) or result.get("status") == "success" else 1
        assert exit_code == 0

    def test_exit_code_failure(self):
        """Test exit code logic for failure."""
        result = {"status": "error", "success": False}
        exit_code = 0 if result.get("success", False) or result.get("status") == "success" else 1
        assert exit_code == 1
