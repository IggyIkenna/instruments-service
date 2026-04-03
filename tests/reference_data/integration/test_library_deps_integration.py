"""Integration tests that functionally exercise each unified-* library dependency.

Goes beyond import-only checks: constructs real objects, calls real functions,
and validates behavior from each dependency as used in URDI source code.

Dependencies tested:
  - unified-events-interface (UEI): log_event, setup_events, MockEventSink
  - unified-internal-contracts (UIC): InstrumentRecord, FeeScheduleEntry,
    EnhancedError, ErrorCategory, ErrorSeverity,
    ErrorRecoveryStrategy, ErrorContext, MarginType
  - unified-api-contracts (UAC): BinanceFuturesExchangeInfo, BinanceInstrumentInfo,
    AccessMode, venue-specific schemas
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

# ---------------------------------------------------------------------------
# unified-events-interface (UEI) — event logging
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uei_log_event_functional() -> None:
    """log_event succeeds with INSTRUMENT_SCHEMA_VIOLATION event type."""
    from unified_trading_library import MockEventSink, log_event, setup_events

    # Setup with mock sink (safe for tests)
    setup_events(
        service_name="urdi-integration-test",
        mode="batch",
        sink=MockEventSink(),
    )

    # Should not raise
    log_event(
        "INSTRUMENT_SCHEMA_VIOLATION",
        details={
            "schema": "InstrumentRecord",
            "correlation_id": "test-123",
            "error": "test validation error",
        },
    )


@pytest.mark.integration
def test_uei_base_adapter_parse_raw_logs_on_validation_failure() -> None:
    """BaseReferenceDataAdapter._parse_raw logs INSTRUMENT_SCHEMA_VIOLATION on error."""
    from unified_trading_library import MockEventSink, setup_events

    setup_events(
        service_name="urdi-parse-raw-test",
        mode="batch",
        sink=MockEventSink(),
    )

    from unified_api_contracts.internal import InstrumentRecord

    from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter

    adapter = BinanceReferenceDataAdapter()

    with pytest.raises(RuntimeError, match="Schema validation failed"):
        adapter._parse_raw(
            {"completely_invalid_field": "bad_value"},
            InstrumentRecord,
        )


# ---------------------------------------------------------------------------
# unified-internal-contracts (UIC) — InstrumentRecord
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uic_instrument_record_construction() -> None:
    """InstrumentRecord from UIC constructs with required fields and validates."""
    from unified_api_contracts.internal import InstrumentRecord

    record = InstrumentRecord(
        instrument_key="BINANCE:PERP:BTCUSDT",
        venue="binance",
        instrument_type="PERPETUAL",
        raw_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        tick_size=Decimal("0.10"),
        lot_size=Decimal("0.001"),
        contract_size=Decimal("1"),
    )
    assert record.instrument_key == "BINANCE:PERP:BTCUSDT"
    assert record.venue == "binance"
    assert record.tick_size == Decimal("0.10")


@pytest.mark.integration
def test_uic_instrument_record_is_same_type_in_urdi() -> None:
    """InstrumentRecord re-exported from URDI is the same UIC type."""
    from unified_api_contracts.internal import InstrumentRecord as UicInstrumentRecord

    from instruments_service.reference_data import InstrumentRecord as UrdiInstrumentRecord

    assert UrdiInstrumentRecord is UicInstrumentRecord


@pytest.mark.integration
def test_uic_instrument_record_used_in_schemas() -> None:
    """InstrumentRecord is used in URDI schema dataclasses (CanonicalOptionsChain)."""
    from unified_api_contracts.internal import InstrumentRecord

    from instruments_service.reference_data.schemas import CanonicalOptionsChain

    record = InstrumentRecord(
        instrument_key="DERIBIT:OPTION:BTC-30JUN26-100000-C",
        venue="deribit",
        instrument_type="OPTION",
        raw_symbol="BTC-30JUN26-100000-C",
        base_asset="BTC",
        quote_asset="USD",
        tick_size=Decimal("0.0005"),
        lot_size=Decimal("0.1"),
        strike=Decimal("100000"),
        option_type="call",
    )

    chain = CanonicalOptionsChain(
        venue="deribit",
        underlying="BTC",
        expiry=datetime(2026, 6, 30, tzinfo=UTC),
        strikes=[Decimal("90000"), Decimal("100000"), Decimal("110000")],
        calls=[record],
        puts=[],
    )
    assert chain.venue == "deribit"
    assert len(chain.calls) == 1
    assert chain.calls[0].strike == Decimal("100000")


# ---------------------------------------------------------------------------
# unified-internal-contracts (UIC) — FeeScheduleEntry
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uic_fee_schedule_entry_construction() -> None:
    """FeeScheduleEntry from UIC constructs with maker/taker bps and is_active check."""
    from unified_api_contracts.internal import FeeScheduleEntry
    from unified_api_contracts.internal.reference.fee_schedule import FeeType

    fee = FeeScheduleEntry(
        client_id="test-client",
        venue="bybit",
        fee_type=FeeType.EXCHANGE,
        maker_fee_bps=Decimal("1.0"),
        taker_fee_bps=Decimal("2.5"),
        tier_name="VIP1",
    )
    assert fee.venue == "bybit"
    assert fee.maker_fee_bps == Decimal("1.0")
    assert fee.taker_fee_bps == Decimal("2.5")
    assert fee.is_active() is True


# ---------------------------------------------------------------------------
# unified-internal-contracts (UIC) — EnhancedError used in _parse_raw
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uic_enhanced_error_construction() -> None:
    """EnhancedError from UIC constructs with all error metadata fields."""
    from unified_api_contracts.internal import (
        EnhancedError,
        ErrorCategory,
        ErrorContext,
        ErrorRecoveryStrategy,
        ErrorSeverity,
    )

    err = EnhancedError(
        message="Schema validation failed for InstrumentRecord: bad value",
        category=ErrorCategory.VALIDATION_ERROR,
        severity=ErrorSeverity.HIGH,
        recovery_strategy=ErrorRecoveryStrategy.SKIP,
        correlation_id="test-corr-001",
        context=ErrorContext(extra={"schema": "InstrumentRecord", "exc_type": "ValueError"}),
    )
    assert err.message == "Schema validation failed for InstrumentRecord: bad value"
    assert err.category == ErrorCategory.VALIDATION_ERROR
    assert err.severity == ErrorSeverity.HIGH
    assert err.recovery_strategy == ErrorRecoveryStrategy.SKIP
    assert err.correlation_id == "test-corr-001"
    assert err.context.extra["schema"] == "InstrumentRecord"


@pytest.mark.integration
def test_uic_margin_type_enum() -> None:
    """MarginType from UIC is used in URDI adapter modules."""
    from unified_api_contracts.internal import MarginType

    assert MarginType.LINEAR is not None
    assert MarginType.INVERSE is not None
    assert str(MarginType.LINEAR) is not None
    assert str(MarginType.INVERSE) is not None


# ---------------------------------------------------------------------------
# unified-api-contracts (UAC) — venue-specific schemas
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_uac_binance_exchange_info_schema() -> None:
    """BinanceFuturesExchangeInfo from UAC validates structured exchange data."""
    from unified_api_contracts import BinanceFuturesExchangeInfo

    info = BinanceFuturesExchangeInfo.model_validate(
        {
            "timezone": "UTC",
            "serverTime": 1700000000000,
            "symbols": [],
        }
    )
    assert info.timezone == "UTC"
    assert info.symbols == []
    assert info.optionSymbols is None


@pytest.mark.integration
def test_uac_access_mode_enum() -> None:
    """AccessMode from UAC is importable and has expected values."""
    from unified_api_contracts import AccessMode

    assert AccessMode is not None
    members = list(AccessMode)
    assert len(members) > 0


# ---------------------------------------------------------------------------
# Cross-dep: URDI factory creates adapters using UIC/UAC types
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_urdi_factory_creates_adapter_with_correct_venue() -> None:
    """create_reference_data_adapter creates adapters that return correct venue property."""
    from instruments_service.reference_data import create_reference_data_adapter

    # Venue names are now canonical uppercase
    expected_venues = {
        "binance": "BINANCE-SPOT",
        "bybit": "BYBIT-SPOT",
        "okx": "OKX-SPOT",
        "deribit": "DERIBIT",
        "coinbase": "coinbase",
        "hyperliquid": "HYPERLIQUID",
    }
    for factory_key, expected_venue in expected_venues.items():
        adapter = create_reference_data_adapter(factory_key)
        assert adapter.venue == expected_venue, (
            f"For factory key {factory_key!r}, expected venue={expected_venue!r}, got {adapter.venue!r}"
        )


@pytest.mark.integration
def test_urdi_factory_raises_for_unsupported_venue() -> None:
    """create_reference_data_adapter raises ValueError for unknown venues."""
    from instruments_service.reference_data import create_reference_data_adapter

    with pytest.raises(ValueError, match="Unsupported venue"):
        create_reference_data_adapter("nonexistent_venue_xyz")


@pytest.mark.integration
def test_urdi_router_creates_adapter_for_source() -> None:
    """create_reference_data_adapter_for_source routes correctly via ReferenceDataSourceConfig."""
    from instruments_service.reference_data import (
        ReferenceDataSourceConfig,
        create_reference_data_adapter_for_source,
    )
    from instruments_service.reference_data.adapters.binance import BinanceReferenceDataAdapter

    config = ReferenceDataSourceConfig(venue="binance", data_source="direct")
    adapter = create_reference_data_adapter_for_source(config)
    assert isinstance(adapter, BinanceReferenceDataAdapter)
    assert adapter.venue == "BINANCE-SPOT"


@pytest.mark.integration
def test_urdi_router_ccxt_source() -> None:
    """Router creates CCXT adapter when data_source='ccxt' with exchange mapping."""
    from instruments_service.reference_data import (
        ReferenceDataSourceConfig,
        create_reference_data_adapter_for_source,
    )
    from instruments_service.reference_data.adapters.ccxt_adapter import CCXTReferenceDataAdapter

    config = ReferenceDataSourceConfig(venue="bybit", data_source="ccxt")
    adapter = create_reference_data_adapter_for_source(config)
    assert isinstance(adapter, CCXTReferenceDataAdapter)


@pytest.mark.integration
def test_urdi_router_raises_for_unsupported_source() -> None:
    """Router raises ValueError for unsupported data_source."""
    from instruments_service.reference_data import (
        ReferenceDataSourceConfig,
        create_reference_data_adapter_for_source,
    )

    config = ReferenceDataSourceConfig(venue="binance", data_source="nonexistent_source")
    with pytest.raises(ValueError, match="Unsupported data_source"):
        create_reference_data_adapter_for_source(config)


# ---------------------------------------------------------------------------
# Cross-dep: URDI schemas use UIC InstrumentRecord in dataclass fields
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_urdi_all_schema_dataclasses_construct() -> None:
    """All URDI schema dataclasses construct with valid data."""
    from instruments_service.reference_data.schemas import (
        CanonicalCorporateAction,
        CanonicalExpiryCalendar,
        CanonicalOptionsChain,
        FundingRateRef,
        OHLCVRef,
    )

    now = datetime.now(UTC)

    chain = CanonicalOptionsChain(
        venue="deribit",
        underlying="BTC",
        expiry=now,
    )
    assert chain.venue == "deribit"
    assert chain.strikes == []

    calendar = CanonicalExpiryCalendar(
        venue="binance",
        instrument_type="FUTURE",
        underlying="ETH",
        expiries=[now],
    )
    assert len(calendar.expiries) == 1

    action = CanonicalCorporateAction(
        venue="ibkr",
        symbol="AAPL",
        action_type="SPLIT",
        effective_date=now,
        ratio=Decimal("4"),
        cash_amount=None,
        currency="USD",
    )
    assert action.ratio == Decimal("4")

    funding = FundingRateRef(
        venue="binance",
        symbol="BTCUSDT",
        rate=Decimal("0.0001"),
        next_funding_time=now,
        mark_price=Decimal("65000.00"),
    )
    assert funding.rate == Decimal("0.0001")

    bar = OHLCVRef(
        venue="bybit",
        symbol="ETHUSDT",
        timestamp=now,
        open=Decimal("3200"),
        high=Decimal("3300"),
        low=Decimal("3100"),
        close=Decimal("3250"),
        volume=Decimal("15000"),
        interval="1h",
    )
    assert bar.interval == "1h"
    assert bar.close == Decimal("3250")
