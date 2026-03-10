"""Unit tests for InstrumentsFreshnessChecker in instruments-service.

Verifies that the checker:
- Instantiates with a valid DataFreshnessContract
- Delegates to FreshnessMonitor.log_if_stale() with correct age
- Handles naive timestamps gracefully
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from unified_events_interface import setup_events

from instruments_service.monitors import InstrumentsFreshnessChecker


@pytest.fixture(autouse=True)
def _test_mode() -> None:
    """Ensure UEI is in test mode so log_event() doesn't require a sink."""
    setup_events(service_name="instruments-service-test", mode="test")


class TestInstrumentsFreshnessCheckerInit:
    def test_instantiation(self) -> None:
        checker = InstrumentsFreshnessChecker()
        assert checker is not None

    def test_contract_is_accessible(self) -> None:
        checker = InstrumentsFreshnessChecker()
        contract = checker.contract
        assert contract is not None
        assert contract.source == "instruments-service"

    def test_contract_criticality_is_important(self) -> None:
        checker = InstrumentsFreshnessChecker()
        # The default contract sets criticality="important"
        assert checker.contract.criticality == "important"

    def test_contract_max_age_is_positive(self) -> None:
        checker = InstrumentsFreshnessChecker()
        assert checker.contract.max_age_seconds > 0

    def test_contract_warn_age_less_than_max(self) -> None:
        checker = InstrumentsFreshnessChecker()
        assert checker.contract.warn_age_seconds < checker.contract.max_age_seconds


class TestInstrumentsFreshnessCheckerCheck:
    def test_fresh_timestamp_calls_log_if_stale(self) -> None:
        checker = InstrumentsFreshnessChecker()
        with patch("instruments_service.monitors.instruments_freshness.FreshnessMonitor.log_if_stale") as mock:
            checker.check(last_fetch_ts=datetime.now(UTC))
            mock.assert_called_once()

    def test_stale_timestamp_passes_correct_age(self) -> None:
        checker = InstrumentsFreshnessChecker()
        stale_seconds = 1900  # 31 min — above default warn_age of 1800s
        stale_ts = datetime.now(UTC) - timedelta(seconds=stale_seconds)
        with patch("instruments_service.monitors.instruments_freshness.FreshnessMonitor.log_if_stale") as mock:
            checker.check(last_fetch_ts=stale_ts)
            mock.assert_called_once()
            age_arg = mock.call_args.kwargs.get("last_update_seconds_ago")
            if age_arg is None:
                age_arg = mock.call_args[0][1]
            assert age_arg >= stale_seconds - 1, f"Expected age >= {stale_seconds - 1}s, got {age_arg}"

    def test_naive_timestamp_does_not_raise(self) -> None:
        checker = InstrumentsFreshnessChecker()
        naive_ts = datetime.utcnow()  # naive (no tzinfo)
        checker.check(last_fetch_ts=naive_ts)
