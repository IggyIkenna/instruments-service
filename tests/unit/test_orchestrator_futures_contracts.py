"""Unit tests for _write_futures_contracts orchestrator helper.

Tests the Phase 4.2 (tradfi_canonical_futures_contract_hard_required_fields_2026_05_13)
futures contract write-path in the instruments-service orchestrator.

No GCS calls; no Databento API calls. All sink/bucket/log_event calls are mocked.

Plan: tradfi_canonical_futures_contract_hard_required_fields_2026_05_13.md
Phase 4.2 — instruments-service write-path for CanonicalFuturesContract.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from unified_api_contracts.internal import AssetClass, InstrumentRecord, InstrumentType

from instruments_service.engine.orchestrator import _write_futures_contracts

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _make_future(
    raw_symbol: str,
    venue: str = "CME",
    expiry: datetime | None = None,
) -> InstrumentRecord:
    """Build a minimal FUTURE InstrumentRecord."""
    return InstrumentRecord(
        instrument_key=f"{venue}:FUTURE:{raw_symbol}",
        venue=venue,
        asset_class=AssetClass.EQUITY,
        raw_symbol=raw_symbol,
        instrument_type=InstrumentType.FUTURE,
        base_asset="",
        quote_asset="USD",
        tick_size=Decimal("0.25"),
        min_size=Decimal("1"),
        contract_size=Decimal("50"),
        expiry=expiry,
    )


def _make_mock_sink() -> MagicMock:
    """Build a mock DataSink that accepts write() calls."""
    sink = MagicMock()
    sink.write.return_value = "gs://mock-bucket/mock-path/futures_contracts.parquet"
    return sink


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_basic(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """Basic: one ESH26 future → _gated_sink_write called with futures_contracts.parquet."""
    expiry_dt = datetime(2026, 3, 20, 8, 30, 0, tzinfo=UTC)
    records = [_make_future("ESH26", expiry=expiry_dt)]
    sink = _make_mock_sink()

    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-03-01",
        bucket="mock-instruments-bucket",
        sink=sink,
    )

    # _WRITE_GATE.validate_and_write should have been called once
    mock_gate.validate_and_write.assert_called_once()
    call_kwargs = mock_gate.validate_and_write.call_args.kwargs
    assert call_kwargs["filename"] == "futures_contracts.parquet"
    assert call_kwargs["partition"]["venue"] == "CME"
    assert call_kwargs["partition"]["day"] == "2026-03-01"
    assert call_kwargs["entity"] == "futures_contracts"

    # No error events
    mock_log_event.assert_not_called()


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_df_contains_required_fields(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """DataFrame passed to sink contains all 5 lifecycle date fields + lifecycle_phase."""
    expiry_dt = datetime(2026, 6, 19, 8, 30, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    sink = _make_mock_sink()

    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-05-01",
        bucket="mock-bucket",
        sink=sink,
    )

    df: pd.DataFrame = mock_gate.validate_and_write.call_args.kwargs["data"]
    assert len(df) == 1
    required_cols = {
        "expiry_date",
        "last_trading_date",
        "first_notice_date",
        "delivery_date",
        "settlement_date",
        "lifecycle_phase",
        "root",
        "contract_symbol",
        "venue",
    }
    missing = required_cols - set(df.columns)
    assert not missing, f"Missing required columns: {missing}"


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_empty_records_skips_write(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """No FUTURE instruments → write gate not called (skip, no error)."""
    sink = _make_mock_sink()
    _write_futures_contracts(
        venue_str="CME",
        instrument_records=[],
        date="2026-05-01",
        bucket="mock-bucket",
        sink=sink,
    )
    mock_gate.validate_and_write.assert_not_called()
    mock_log_event.assert_not_called()


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_no_expiry_records_skips_write(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """All FUTURE records have no expiry → build_futures_contracts returns [] → skip write."""
    records: list[InstrumentRecord] = []
    sink = _make_mock_sink()
    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-05-01",
        bucket="mock-bucket",
        sink=sink,
    )
    # build_futures_contracts skips records with no expiry → contracts = [] → no write
    mock_gate.validate_and_write.assert_not_called()


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_write_failure_is_isolated(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """If _WRITE_GATE.validate_and_write raises, error is logged but not propagated."""
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    sink = _make_mock_sink()

    mock_gate.validate_and_write.side_effect = OSError("GCS write failed")

    # Must NOT raise — shard-level isolation
    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-05-01",
        bucket="mock-bucket",
        sink=sink,
    )

    # WRITE_FAILED event logged
    mock_log_event.assert_called_once()
    call_args = mock_log_event.call_args
    assert call_args.args[0] == "WRITE_FAILED"
    assert call_args.kwargs["details"]["entity"] == "futures_contracts"


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_multiple_records(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """Three futures → DataFrame has three rows."""
    records = [
        _make_future("ESH26", expiry=datetime(2026, 3, 20, 0, 0, 0, tzinfo=UTC)),
        _make_future("ESM26", expiry=datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)),
        _make_future("ESU26", expiry=datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)),
    ]
    sink = _make_mock_sink()

    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-02-01",
        bucket="mock-bucket",
        sink=sink,
    )

    df: pd.DataFrame = mock_gate.validate_and_write.call_args.kwargs["data"]
    assert len(df) == 3


@pytest.mark.unit
@patch("instruments_service.engine.orchestrator.log_event")
@patch("instruments_service.engine.orchestrator._WRITE_GATE")
def test_write_futures_contracts_physical_delivery_cl(mock_gate: MagicMock, mock_log_event: MagicMock) -> None:
    """CL physical delivery: first_notice_date != expiry_date in output."""
    expiry_dt = datetime(2026, 5, 20, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("CLM26", expiry=expiry_dt)]
    sink = _make_mock_sink()

    _write_futures_contracts(
        venue_str="CME",
        instrument_records=records,
        date="2026-04-01",
        bucket="mock-bucket",
        sink=sink,
    )

    df: pd.DataFrame = mock_gate.validate_and_write.call_args.kwargs["data"]
    assert len(df) == 1
    row = df.iloc[0]
    # Physical: first_notice_date should differ from expiry_date
    assert row["first_notice_date"] != row["expiry_date"]
