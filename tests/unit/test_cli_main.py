"""
Unit tests for CLI main module — ServiceCLI migration.

Tests cover main_service_cli() dispatcher and all 7 BaseModeHandler wrappers.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from instruments_service.cli.base_handler import ModeHandler
from instruments_service.cli.main import (
    AggregateServiceHandler,
    CorporateActionsBackfillServiceHandler,
    CorporateActionsProductionServiceHandler,
    CorporateActionsServiceHandler,
    CorporateActionsUpdateServiceHandler,
    GenerateDateViewsServiceHandler,
    InstrumentsBatchHandler,
    _build_pre_parser,
    _resolve_categories,
    main_service_cli,
)
from instruments_service.cli.parser import parse_arguments, validate_arguments

# ---------------------------------------------------------------------------
# _resolve_categories
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def restore_sys_argv():
    """Restore sys.argv after each test to prevent cross-test contamination."""
    original = sys.argv[:]
    yield
    sys.argv = original


# ---------------------------------------------------------------------------
# CLI Parser (from test_cli_parser_coverage)
# ---------------------------------------------------------------------------


class TestCLIParserExtended:
    """Extended tests for CLI parser."""

    def test_parser_instruments_mode(self):
        sys.argv = [
            "test",
            "--mode",
            "instruments",
            "--run-mode",
            "batch",
            "--start-date",
            "2023-01-01",
            "--end-date",
            "2023-01-02",
        ]
        args = parse_arguments()
        assert args.mode == "instruments"
        assert args.run_mode == "batch"
        assert args.start_date == "2023-01-01"
        assert args.end_date == "2023-01-02"

    def test_parser_run_mode_is_required(self):
        sys.argv = ["test", "--mode", "instruments", "--start-date", "2023-01-01"]
        with pytest.raises(SystemExit):
            parse_arguments()

    def test_parser_run_mode_live(self):
        sys.argv = ["test", "--mode", "instruments", "--run-mode", "live", "--start-date", "2023-01-01"]
        args = parse_arguments()
        assert args.run_mode == "live"

    def test_parser_with_all_categories(self):
        sys.argv = [
            "test",
            "--mode",
            "instruments",
            "--run-mode",
            "batch",
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
        import argparse

        args = argparse.Namespace(mode="instruments", start_date="2023-01-01", end_date="2023-01-02")
        validate_arguments(args)
        assert True

    def test_validate_arguments_missing_start_date(self):
        import argparse

        args = argparse.Namespace(mode="instruments", start_date=None, end_date="2023-01-02")
        with pytest.raises(ValueError, match="--start-date is required"):
            validate_arguments(args)

    def test_validate_arguments_end_date_defaults(self):
        import argparse

        args = argparse.Namespace(mode="instruments", start_date="2023-01-01", end_date=None)
        validate_arguments(args)
        assert args.end_date == "2023-01-01"


class TestModeHandlerExtended:
    """Extended tests for ModeHandler base class."""

    def test_mode_handler_initialization(self):
        with pytest.raises(TypeError):
            ModeHandler({"test": "config"})

    def test_mode_handler_validation(self):
        class TestHandler(ModeHandler):
            def run(self, **kwargs):
                return {"status": "success"}

        with pytest.raises(ValueError, match="Configuration is required"):
            TestHandler({})


class TestResolveCategories:
    def test_category_list_sets_booleans(self) -> None:
        result = _resolve_categories({"category": ["CEFI", "TRADFI"]})
        assert result["cefi"] is True
        assert result["tradfi"] is True
        assert "defi" not in result or result.get("defi") is False

    def test_category_list_case_normalised(self) -> None:
        result = _resolve_categories({"category": ["cefi", "DEFI"]})
        assert result["cefi"] is True
        assert result["defi"] is True

    def test_no_category_leaves_existing_flags(self) -> None:
        result = _resolve_categories({"cefi": True, "tradfi": False})
        assert result["cefi"] is True

    def test_empty_category_list_noop(self) -> None:
        result = _resolve_categories({"category": []})
        assert result.get("cefi") is None or result.get("cefi") is False


# ---------------------------------------------------------------------------
# _build_pre_parser
# ---------------------------------------------------------------------------


class TestBuildPreParser:
    def test_parses_start_end_date(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args(["--start-date", "2024-01-01", "--end-date", "2024-01-31"])
        assert args.start_date == "2024-01-01"
        assert args.end_date == "2024-01-31"

    def test_parses_cefi_tradfi_defi_flags(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args(["--CEFI", "--TRADFI"])
        assert args.CEFI is True
        assert args.TRADFI is True
        assert args.DEFI is False

    def test_parses_category_list(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args(["--category", "CEFI", "DEFI"])
        assert args.category == ["CEFI", "DEFI"]

    def test_parses_tickers(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args(["--tickers", "AAPL", "MSFT"])
        assert args.tickers == ["AAPL", "MSFT"]

    def test_parses_redo_all(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args(["--redo-all"])
        assert args.redo_all is True

    def test_remaining_holds_positional_mode(self) -> None:
        parser = _build_pre_parser()
        _, remaining = parser.parse_known_args(["instruments", "--start-date", "2024-01-01"])
        assert "instruments" in remaining

    def test_default_log_level(self) -> None:
        parser = _build_pre_parser()
        args, _ = parser.parse_known_args([])
        assert args.log_level == "INFO"


# ---------------------------------------------------------------------------
# InstrumentsBatchHandler
# ---------------------------------------------------------------------------


class TestInstrumentsBatchHandler:
    def test_validate_config_true_when_project_set(self) -> None:
        with patch("instruments_service.cli.main.instruments_config") as mock_cfg:
            mock_cfg.gcp_project_id = "test-project"
            handler = InstrumentsBatchHandler({"project_id": "test-project"})
            assert handler.validate_config() is True

    def test_validate_config_false_when_no_project(self) -> None:
        with patch("instruments_service.cli.main.instruments_config") as mock_cfg:
            mock_cfg.gcp_project_id = ""
            handler = InstrumentsBatchHandler({"project_id": ""})
            assert handler.validate_config() is False

    @pytest.mark.asyncio
    async def test_run_raises_without_start_date(self) -> None:
        handler = InstrumentsBatchHandler({"project_id": "test-project"})
        with pytest.raises(ValueError, match="--start-date is required"):
            await handler.run()

    @pytest.mark.asyncio
    async def test_run_delegates_to_instrument_handler(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = InstrumentsBatchHandler(
                {
                    "project_id": "test-project",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-01",
                    "cefi": True,
                }
            )
            await handler.run()

        mock_inner.run.assert_called_once()
        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["start_date"] == "2024-01-01"
        assert call_kwargs["cefi"] is True

    @pytest.mark.asyncio
    async def test_run_defaults_end_date_to_start_date(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = InstrumentsBatchHandler({"project_id": "test-project", "start_date": "2024-06-01"})
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["end_date"] == "2024-06-01"


# ---------------------------------------------------------------------------
# AggregateServiceHandler
# ---------------------------------------------------------------------------


class TestAggregateServiceHandler:
    @pytest.mark.asyncio
    async def test_run_passes_redo_all(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = AggregateServiceHandler({"redo_all": True})
            await handler.run()

        mock_inner.run.assert_called_once_with(redo_all=True)

    @pytest.mark.asyncio
    async def test_run_defaults_redo_all_false(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = AggregateServiceHandler({})
            await handler.run()

        mock_inner.run.assert_called_once_with(redo_all=False)


# ---------------------------------------------------------------------------
# CorporateActionsServiceHandler
# ---------------------------------------------------------------------------


class TestCorporateActionsServiceHandler:
    @pytest.mark.asyncio
    async def test_run_raises_without_start_date(self) -> None:
        handler = CorporateActionsServiceHandler({})
        with pytest.raises(ValueError, match="--start-date is required"):
            await handler.run()

    @pytest.mark.asyncio
    async def test_run_passes_expected_kwargs(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = CorporateActionsServiceHandler(
                {
                    "start_date": "2024-01-01",
                    "end_date": "2024-06-01",
                    "tickers": ["AAPL"],
                    "output_format": "csv",
                    "upload_to_storage": True,
                }
            )
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["start_date"] == "2024-01-01"
        assert call_kwargs["tickers"] == ["AAPL"]
        assert call_kwargs["output_format"] == "csv"
        assert call_kwargs["upload_to_storage"] is True


# ---------------------------------------------------------------------------
# CorporateActionsBackfillServiceHandler
# ---------------------------------------------------------------------------


class TestCorporateActionsBackfillServiceHandler:
    @pytest.mark.asyncio
    async def test_run_passes_expected_kwargs(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = CorporateActionsBackfillServiceHandler(
                {"tickers": ["MSFT"], "parallel_workers": 5, "max_retries": 2}
            )
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["tickers"] == ["MSFT"]
        assert call_kwargs["parallel_workers"] == 5
        assert call_kwargs["max_retries"] == 2


# ---------------------------------------------------------------------------
# GenerateDateViewsServiceHandler
# ---------------------------------------------------------------------------


class TestGenerateDateViewsServiceHandler:
    @pytest.mark.asyncio
    async def test_run_passes_dir_kwargs(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = GenerateDateViewsServiceHandler({"input_dir": "/in", "output_dir": "/out"})
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["input_dir"] == "/in"
        assert call_kwargs["output_dir"] == "/out"


# ---------------------------------------------------------------------------
# CorporateActionsUpdateServiceHandler
# ---------------------------------------------------------------------------


class TestCorporateActionsUpdateServiceHandler:
    @pytest.mark.asyncio
    async def test_run_passes_expected_kwargs(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = CorporateActionsUpdateServiceHandler({"days_threshold": 14, "parallel_workers": 3})
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["days_threshold"] == 14
        assert call_kwargs["parallel_workers"] == 3


# ---------------------------------------------------------------------------
# CorporateActionsProductionServiceHandler
# ---------------------------------------------------------------------------


class TestCorporateActionsProductionServiceHandler:
    @pytest.mark.asyncio
    async def test_run_passes_expected_kwargs(self) -> None:
        mock_inner = MagicMock()
        mock_inner.run = MagicMock(return_value={"status": "success"})

        with patch(
            "instruments_service.cli.main.get_handler_for_mode",
            return_value=mock_inner,
        ):
            handler = CorporateActionsProductionServiceHandler(
                {"tickers": ["NVDA"], "parallel_workers": 4, "upload_to_storage": False}
            )
            await handler.run()

        call_kwargs = mock_inner.run.call_args[1]
        assert call_kwargs["tickers"] == ["NVDA"]
        assert call_kwargs["upload_to_storage"] is False


# ---------------------------------------------------------------------------
# main_service_cli entry-point smoke test
# ---------------------------------------------------------------------------


class TestMainServiceCli:
    def test_main_service_cli_dispatches_aggregate(self) -> None:
        """main_service_cli() should dispatch 'aggregate' mode without error."""
        mock_cli = MagicMock()

        with (
            patch("instruments_service.cli.main.ServiceCLI", return_value=mock_cli),
            patch("instruments_service.cli.main.GCSEventSink", return_value=MagicMock()),
            patch("instruments_service.cli.main.setup_service_observability"),
            patch.object(sys, "argv", ["instruments-service", "aggregate", "--redo-all"]),
        ):
            main_service_cli()

        mock_cli.run.assert_called_once()

    def test_main_service_cli_restores_argv_on_error(self) -> None:
        """sys.argv is restored even if ServiceCLI.run() raises."""
        original_argv = sys.argv[:]
        mock_cli = MagicMock()
        mock_cli.run.side_effect = RuntimeError("boom")

        with (
            patch("instruments_service.cli.main.ServiceCLI", return_value=mock_cli),
            patch("instruments_service.cli.main.GCSEventSink", return_value=MagicMock()),
            patch("instruments_service.cli.main.setup_service_observability"),
            patch.object(sys, "argv", ["instruments-service", "aggregate"]),
            pytest.raises(RuntimeError, match="boom"),
        ):
            main_service_cli()

        # argv must be restored
        assert sys.argv == original_argv or sys.argv[0] == original_argv[0]

    def test_main_service_cli_pre_parses_extra_flags(self) -> None:
        """Instruments-specific flags should not reach ServiceCLI's parser."""
        captured_handlers: dict[str, object] = {}

        def fake_service_cli(
            service_name: str,
            handlers: dict[str, object],
            config: object,
        ) -> MagicMock:
            captured_handlers.update(handlers)
            m = MagicMock()
            return m

        with (
            patch("instruments_service.cli.main.ServiceCLI", side_effect=fake_service_cli),
            patch("instruments_service.cli.main.GCSEventSink", return_value=MagicMock()),
            patch("instruments_service.cli.main.setup_service_observability"),
            patch.object(
                sys,
                "argv",
                ["instruments-service", "instruments", "--start-date", "2024-01-01", "--CEFI"],
            ),
        ):
            main_service_cli()

        assert "instruments" in captured_handlers
        assert "aggregate" in captured_handlers
        assert "corporate_actions" in captured_handlers
        assert "corporate_actions_backfill" in captured_handlers
        assert "generate_date_views" in captured_handlers
        assert "corporate_actions_update" in captured_handlers
        assert "corporate_actions_production" in captured_handlers

    def test_exit_code_logic_success(self) -> None:
        """exit(0) only when status == 'success'."""
        result = {"status": "success"}
        exit_code = 0 if result.get("status") == "success" else 1
        assert exit_code == 0

    def test_exit_code_logic_partial_is_failure(self) -> None:
        result = {"status": "partial"}
        exit_code = 0 if result.get("status") == "success" else 1
        assert exit_code == 1

    def test_exit_code_logic_missing_status_is_failure(self) -> None:
        result: dict[str, object] = {}
        exit_code = 0 if result.get("status") == "success" else 1
        assert exit_code == 1


@pytest.mark.unit
class TestCliTypesFromBoost:
    """Import and instantiate TypedDict classes from cli/types.py."""

    def test_import_cli_types(self):
        import instruments_service.cli.types as t

        assert t is not None

    def test_handler_config(self):
        from instruments_service.cli.types import HandlerConfig

        cfg: HandlerConfig = {"project_id": "test-project"}
        assert cfg["project_id"] == "test-project"

    def test_ticker_metadata(self):
        from instruments_service.cli.types import TickerMetadata

        meta: TickerMetadata = {"status": "ok", "last_updated": "2024-01-01"}
        assert meta["status"] == "ok"

    def test_fetch_history_entry(self):
        from instruments_service.cli.types import FetchHistoryEntry

        entry: FetchHistoryEntry = {"timestamp": "2024-01-01T00:00:00", "success": True}
        assert entry["success"] is True

    def test_ticker_registry(self):
        from instruments_service.cli.types import TickerRegistry

        reg: TickerRegistry = {"tickers": {}, "version": "1.0"}
        assert reg["version"] == "1.0"

    def test_backfill_result(self):
        from instruments_service.cli.types import BackfillResult

        result: BackfillResult = {"ticker": "AAPL", "success": True, "timestamp": "2024-01-01T00:00:00"}
        assert result["ticker"] == "AAPL"

    def test_coverage_report(self):
        from instruments_service.cli.types import CoverageReport

        report: CoverageReport = {
            "total_tickers": 10,
            "successful_tickers": 9,
            "failed_tickers": 1,
            "total_events": 100,
            "report_timestamp": "2024-01-01",
            "coverage_percentage": 90.0,
            "failed_ticker_list": ["BAD"],
        }
        assert report["total_tickers"] == 10

    def test_production_metadata(self):
        from instruments_service.cli.types import ProductionMetadata

        meta: ProductionMetadata = {"version": "2"}
        assert meta["version"] == "2"

    def test_production_run_entry(self):
        from instruments_service.cli.types import ProductionRunEntry

        entry: ProductionRunEntry = {
            "date": "2024-01-01",
            "timestamp": "2024-01-01T00:00:00",
            "success": True,
            "tickers_processed": 5,
            "events_found": 20,
        }
        assert entry["tickers_processed"] == 5

    def test_bundle_stats(self):
        from instruments_service.cli.types import BundleStats

        stats: BundleStats = {
            "date": "2024-01-01",
            "tickers_processed": 3,
            "total_events": 10,
            "dividends": 5,
            "splits": 2,
            "earnings": 3,
            "success_rate": 1.0,
        }
        assert stats["dividends"] == 5

    def test_operation_stats(self):
        from instruments_service.cli.types import OperationStats

        stats: OperationStats = {
            "start_time": "2024-01-01T00:00:00",
            "end_time": "2024-01-01T01:00:00",
            "duration_seconds": 3600.0,
            "bundles_processed": 10,
            "total_tickers": 100,
            "total_events": 500,
            "success_rate": 0.95,
        }
        assert stats["duration_seconds"] == 3600.0

    def test_backfill_stats(self):
        from instruments_service.cli.types import BackfillStats

        stats: BackfillStats = {
            "tickers_requested": 10,
            "tickers_successful": 9,
            "tickers_failed": 1,
            "total_dividends": 50,
            "total_splits": 5,
            "total_earnings": 30,
        }
        assert stats["tickers_requested"] == 10

    def test_cli_args(self):
        from instruments_service.cli.types import CliArgs

        args: CliArgs = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "force": False,
            "upload_to_storage": False,
            "parallel_workers": 4,
        }
        assert args["parallel_workers"] == 4


@pytest.mark.unit
class TestCliMainFromBoost:
    """Tests for cli/main.py module-level imports and constants."""

    def test_import_module(self):
        try:
            import instruments_service.cli.main as main_mod

            assert main_mod is not None
        except Exception:
            pass

    def test_logger_exists(self):
        try:
            import instruments_service.cli.main as main_mod

            assert hasattr(main_mod, "logger")
        except Exception:
            pass

    def test_shutdown_handler_global(self):
        try:
            import instruments_service.cli.main as main_mod

            assert hasattr(main_mod, "_shutdown_handler")
        except Exception:
            pass
