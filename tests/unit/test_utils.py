"""
Unit tests for utils module - ErrorWarningCounter, dump_to_csv.
"""

import logging
from unittest.mock import patch

import pandas as pd

from instruments_service.utils.error_warning_counter import ErrorWarningCounter


class TestErrorWarningCounter:
    def test_initial_counts_zero(self):
        counter = ErrorWarningCounter()
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_counts_error_records(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_error")
        root.addHandler(counter)
        root.setLevel(logging.ERROR)
        root.error("test error")
        root.removeHandler(counter)
        assert counter.error_count == 1
        assert counter.warning_count == 0

    def test_counts_warning_records(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_warning")
        root.addHandler(counter)
        root.setLevel(logging.WARNING)
        root.warning("test warning")
        root.removeHandler(counter)
        assert counter.warning_count == 1
        assert counter.error_count == 0

    def test_ignores_info_and_below(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_info")
        root.addHandler(counter)
        root.setLevel(logging.DEBUG)
        root.info("info message")
        root.debug("debug message")
        root.removeHandler(counter)
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_reset_clears_counts(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_reset")
        root.addHandler(counter)
        root.setLevel(logging.ERROR)
        root.error("err")
        root.warning("warn")
        root.removeHandler(counter)
        counter.reset()
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_critical_counts_as_error(self):
        counter = ErrorWarningCounter()
        root = logging.getLogger("test_ewc_critical")
        root.addHandler(counter)
        root.setLevel(logging.CRITICAL)
        root.critical("critical error")
        root.removeHandler(counter)
        assert counter.error_count == 1


class TestDumpToCsv:
    """Test dump_to_csv utility via module-level patching."""

    def test_no_op_when_disabled(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original = dump_module._ENABLED
        try:
            dump_module._ENABLED = False
            df = pd.DataFrame([{"a": 1}])
            # Should not create any file
            dump_module.dump_to_csv(df, "test_output.csv")
            assert not (tmp_path / "test_output.csv").exists()
        finally:
            dump_module._ENABLED = original

    def test_no_op_when_empty_df(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = str(tmp_path)
            df = pd.DataFrame()
            dump_module.dump_to_csv(df, "empty_output.csv")
            assert not (tmp_path / "empty_output.csv").exists()
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir

    def test_writes_csv_when_enabled(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = str(tmp_path)
            df = pd.DataFrame([{"col": "val"}])
            dump_module.dump_to_csv(df, "test_output.csv")
            assert (tmp_path / "test_output.csv").exists()
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir

    def test_handles_os_error_gracefully(self, tmp_path):
        from instruments_service.utils import dump_to_csv as dump_module

        original_enabled = dump_module._ENABLED
        original_dir = dump_module._CSV_SAMPLE_DIR
        try:
            dump_module._ENABLED = True
            dump_module._CSV_SAMPLE_DIR = "/nonexistent/path/that/cannot/be/created"
            df = pd.DataFrame([{"col": "val"}])
            # Should not raise
            with patch("pathlib.Path.mkdir", side_effect=OSError("permission denied")):
                dump_module.dump_to_csv(df, "should_fail.csv")
        finally:
            dump_module._ENABLED = original_enabled
            dump_module._CSV_SAMPLE_DIR = original_dir
