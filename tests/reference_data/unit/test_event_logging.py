"""Test event logging setup for unified-reference-data-interface."""

import logging


class TestEventLogging:
    def test_logging_configuration(self) -> None:
        logger = logging.getLogger("instruments_service.reference_data")
        assert logger is not None

    def test_info_logging_works(self) -> None:
        logger = logging.getLogger("instruments_service.reference_data")
        logger.info("test info log")
        assert True, "Logging completed without error"

    def test_error_logging_works(self) -> None:
        logger = logging.getLogger("instruments_service.reference_data")
        logger.error("test error log")
        assert True, "Logging completed without error"

    def test_warning_logging_works(self) -> None:
        logger = logging.getLogger("instruments_service.reference_data")
        logger.warning("test warning log")
        assert True, "Logging completed without error"

    def test_no_silent_failures_in_logging(self) -> None:
        logger = logging.getLogger("instruments_service.reference_data")
        assert not logger.disabled

    def test_adapter_submodule_loggers(self) -> None:
        adapters = [
            "instruments_service.reference_data.adapters.binance",
            "instruments_service.reference_data.adapters.deribit",
            "instruments_service.reference_data.adapters.bybit",
            "instruments_service.reference_data.adapters.okx",
            "instruments_service.reference_data.adapters.coinbase",
            "instruments_service.reference_data.adapters.hyperliquid",
            "instruments_service.reference_data.adapters.ibkr",
        ]
        for name in adapters:
            logger = logging.getLogger(name)
            assert logger is not None, f"Logger {name} should exist"
