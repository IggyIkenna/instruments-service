"""Unit tests for the shared `*-PERP` write-time guardrail (_perp_write_guard).

Covers the guard function directly — the per-adapter integration (kalshi_perp,
polymarket_perp) is covered in each adapter's own test file.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.cefi._perp_write_guard import (
    PerpRecordRejectedError,
    validate_perp_instrument_record,
)


def _make_record(instrument_key: str, instrument_type: InstrumentType = InstrumentType.PERPETUAL) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue="KALSHI-PERP",
        instrument_type=instrument_type,
        raw_symbol=instrument_key,
        base_asset="BTC",
        quote_asset="USD",
        tick_size=Decimal("0.01"),
        min_size=Decimal("1"),
        contract_size=Decimal("1"),
        settle_asset="USD",
        status=InstrumentStatus.ACTIVE,
    )


class TestValidatePerpInstrumentRecord:
    def test_genuine_perpetual_record_passes(self) -> None:
        record = _make_record("KXBTCUSD-PERP")
        validate_perp_instrument_record(record)  # no raise

    def test_non_perpetual_instrument_type_rejected(self) -> None:
        record = _make_record("KXBTCUSD-PERP", instrument_type=InstrumentType.SPOT_PAIR)
        with pytest.raises(PerpRecordRejectedError, match="non-PERPETUAL"):
            validate_perp_instrument_record(record)

    def test_event_contract_ticker_rejected(self) -> None:
        record = _make_record("KXMVESPORTSMULTIGAMEEXTENDED-26JUL05")
        with pytest.raises(PerpRecordRejectedError, match="event-contract prefix"):
            validate_perp_instrument_record(record)

    def test_event_contract_ticker_rejected_case_insensitive(self) -> None:
        record = _make_record("kxmvecrosscategory-x")
        with pytest.raises(PerpRecordRejectedError, match="event-contract prefix"):
            validate_perp_instrument_record(record)
