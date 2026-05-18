"""Contract tests for unified-reference-data-interface (URDI) schemas.

Integration Layer 0 — URDI side.

Verifies:
- All URDI canonical schemas (CanonicalOptionsChain, CanonicalExpiryCalendar,
  CanonicalCorporateAction, FundingRateRef, OHLCVRef) are serializable.
- Round-trip: create dataclass → asdict() → recreate from dict.
- URDI schema Decimal fields remain Decimal through round-trip.
- URDI schemas import InstrumentRecord from UIC (not from UAC) — correct direction.
- BaseReferenceDataAdapter abstract interface is correctly defined.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter
from instruments_service.reference_data.schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)


class _RetryTestAdapter(BaseReferenceDataAdapter):
    """Concrete adapter used by TestGetWithRetry to exercise _get_with_retry."""

    @property
    def venue(self) -> str:
        return "test_retry_venue"

    async def get_instruments(self, instrument_type: str | None = None) -> list[InstrumentRecord]:
        return []

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        return CanonicalOptionsChain(venue=self.venue, underlying=underlying, expiry=expiry or datetime.now(UTC))

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "FUTURE") -> CanonicalExpiryCalendar:
        return CanonicalExpiryCalendar(venue=self.venue, instrument_type=instrument_type, underlying=underlying)

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        return []


# ---------------------------------------------------------------------------
# CanonicalOptionsChain
# ---------------------------------------------------------------------------


class TestCanonicalOptionsChain:
    """CanonicalOptionsChain: create, verify fields, round-trip via dataclass."""

    def test_import_and_instantiate(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalOptionsChain

        now = datetime.now(UTC)
        chain = CanonicalOptionsChain(
            venue="deribit",
            underlying="BTC",
            expiry=now,
            strikes=[Decimal("50000"), Decimal("55000")],
            calls=[],
            puts=[],
        )
        assert chain.venue == "deribit"
        assert chain.underlying == "BTC"
        assert len(chain.strikes) == 2

    def test_strikes_are_decimal(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalOptionsChain

        chain = CanonicalOptionsChain(
            venue="binance",
            underlying="ETH",
            expiry=datetime.now(UTC),
            strikes=[Decimal("2000.00"), Decimal("2500.00")],
        )
        for strike in chain.strikes:
            assert isinstance(strike, Decimal), f"Strike {strike!r} must be Decimal"

    def test_defaults(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalOptionsChain

        chain = CanonicalOptionsChain(
            venue="okx",
            underlying="BTC",
            expiry=datetime.now(UTC),
        )
        assert chain.strikes == []
        assert chain.calls == []
        assert chain.puts == []
        assert chain.fetched_at is not None

    def test_round_trip_via_asdict(self) -> None:
        """Create → asdict() → reconstruct. Verifies schema is a clean dataclass."""
        from instruments_service.reference_data.schemas import CanonicalOptionsChain

        expiry = datetime(2025, 3, 28, tzinfo=UTC)
        chain = CanonicalOptionsChain(
            venue="deribit",
            underlying="BTC",
            expiry=expiry,
            strikes=[Decimal("50000")],
            calls=[],
            puts=[],
        )
        d = dataclasses.asdict(chain)
        assert d["venue"] == "deribit"
        assert d["underlying"] == "BTC"
        # Reconstruct (note: asdict converts Decimal → Decimal stays as-is for dataclasses)
        chain2 = CanonicalOptionsChain(**dict(d.items()))
        assert chain2.venue == chain.venue
        assert chain2.underlying == chain.underlying


# ---------------------------------------------------------------------------
# CanonicalExpiryCalendar
# ---------------------------------------------------------------------------


class TestCanonicalExpiryCalendar:
    """CanonicalExpiryCalendar: create, verify fields, round-trip."""

    def test_import_and_instantiate(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalExpiryCalendar

        cal = CanonicalExpiryCalendar(
            venue="binance",
            instrument_type="FUTURE",
            underlying="BTC",
            expiries=[datetime(2025, 3, 28, tzinfo=UTC), datetime(2025, 6, 27, tzinfo=UTC)],
            settlement_assets={"2025-03-28": "USDT"},
        )
        assert cal.venue == "binance"
        assert cal.underlying == "BTC"
        assert len(cal.expiries) == 2

    def test_defaults(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalExpiryCalendar

        cal = CanonicalExpiryCalendar(
            venue="deribit",
            instrument_type="OPTION",
            underlying="ETH",
        )
        assert cal.expiries == []
        assert cal.settlement_assets == {}

    def test_round_trip_via_asdict(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalExpiryCalendar

        cal = CanonicalExpiryCalendar(
            venue="bybit",
            instrument_type="FUTURE",
            underlying="SOL",
        )
        d = dataclasses.asdict(cal)
        cal2 = CanonicalExpiryCalendar(**dict(d.items()))
        assert cal2.venue == "bybit"
        assert cal2.instrument_type == "FUTURE"
        assert cal2.underlying == "SOL"


# ---------------------------------------------------------------------------
# CanonicalCorporateAction
# ---------------------------------------------------------------------------


class TestCanonicalCorporateAction:
    """CanonicalCorporateAction: TradFi corporate action schema."""

    def test_dividend_action(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalCorporateAction

        action = CanonicalCorporateAction(
            venue="ibkr",
            symbol="AAPL",
            action_type="dividend",
            effective_date=datetime(2024, 11, 15, tzinfo=UTC),
            ratio=None,
            cash_amount=Decimal("0.25"),
            currency="USD",
        )
        assert action.action_type == "dividend"
        assert action.cash_amount == Decimal("0.25")
        assert isinstance(action.cash_amount, Decimal)

    def test_split_action(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalCorporateAction

        action = CanonicalCorporateAction(
            venue="ibkr",
            symbol="NVDA",
            action_type="split",
            effective_date=datetime(2024, 6, 10, tzinfo=UTC),
            ratio=Decimal("10"),
            cash_amount=None,
            currency=None,
            description="10-for-1 stock split",
        )
        assert action.ratio == Decimal("10")
        assert isinstance(action.ratio, Decimal)
        assert action.description == "10-for-1 stock split"

    def test_round_trip_via_asdict(self) -> None:
        from instruments_service.reference_data.schemas import CanonicalCorporateAction

        action = CanonicalCorporateAction(
            venue="bloomberg",
            symbol="TSLA",
            action_type="dividend",
            effective_date=datetime(2024, 12, 1, tzinfo=UTC),
            ratio=None,
            cash_amount=Decimal("1.00"),
            currency="USD",
        )
        d = dataclasses.asdict(action)
        assert d["venue"] == "bloomberg"
        # Reconstruct
        action2 = CanonicalCorporateAction(**dict(d.items()))
        assert action2.symbol == action.symbol
        assert action2.action_type == action.action_type


# ---------------------------------------------------------------------------
# FundingRateRef
# ---------------------------------------------------------------------------


class TestFundingRateRef:
    """FundingRateRef: Decimal rate field, round-trip."""

    def test_import_and_instantiate(self) -> None:
        from instruments_service.reference_data.schemas import FundingRateRef

        ref = FundingRateRef(
            venue="binance",
            symbol="BTCUSDT",
            rate=Decimal("0.0001"),
            next_funding_time=datetime(2024, 1, 15, 16, 0, tzinfo=UTC),
            mark_price=Decimal("50000.00"),
        )
        assert ref.venue == "binance"
        assert ref.symbol == "BTCUSDT"
        assert isinstance(ref.rate, Decimal)

    def test_rate_is_decimal(self) -> None:
        from instruments_service.reference_data.schemas import FundingRateRef

        ref = FundingRateRef(
            venue="okx",
            symbol="ETHUSDT",
            rate=Decimal("-0.0002"),
            next_funding_time=datetime(2024, 1, 15, 8, 0, tzinfo=UTC),
        )
        assert isinstance(ref.rate, Decimal), "rate must be Decimal, not float"
        assert ref.mark_price is None

    def test_round_trip_via_asdict(self) -> None:
        from instruments_service.reference_data.schemas import FundingRateRef

        ref = FundingRateRef(
            venue="bybit",
            symbol="SOLUSDT",
            rate=Decimal("0.0003"),
            next_funding_time=datetime(2024, 1, 15, 8, 0, tzinfo=UTC),
        )
        d = dataclasses.asdict(ref)
        ref2 = FundingRateRef(**dict(d.items()))
        assert ref2.venue == ref.venue
        assert ref2.symbol == ref.symbol
        assert ref2.rate == ref.rate


# ---------------------------------------------------------------------------
# OHLCVRef
# ---------------------------------------------------------------------------


class TestOHLCVRef:
    """OHLCVRef: Decimal OHLCV fields, round-trip."""

    def test_import_and_instantiate(self) -> None:
        from instruments_service.reference_data.schemas import OHLCVRef

        bar = OHLCVRef(
            venue="binance",
            symbol="BTCUSDT",
            timestamp=datetime(2024, 1, 14, tzinfo=UTC),
            open=Decimal("48000.00"),
            high=Decimal("51000.00"),
            low=Decimal("47500.00"),
            close=Decimal("50000.00"),
            volume=Decimal("12345.67"),
            interval="1d",
        )
        assert bar.venue == "binance"
        assert bar.interval == "1d"

    def test_ohlcv_fields_are_decimal(self) -> None:
        from instruments_service.reference_data.schemas import OHLCVRef

        bar = OHLCVRef(
            venue="kucoin",
            symbol="ETHUSD",
            timestamp=datetime(2024, 1, 14, tzinfo=UTC),
            open=Decimal("3000"),
            high=Decimal("3200"),
            low=Decimal("2900"),
            close=Decimal("3150"),
            volume=Decimal("5000"),
        )
        for field_name in ("open", "high", "low", "close", "volume"):
            val = getattr(bar, field_name)
            assert isinstance(val, Decimal), f"OHLCVRef.{field_name} must be Decimal, got {type(val)}"

    def test_round_trip_via_asdict(self) -> None:
        from instruments_service.reference_data.schemas import OHLCVRef

        bar = OHLCVRef(
            venue="coinbase",
            symbol="BTCUSD",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            open=Decimal("49000"),
            high=Decimal("50500"),
            low=Decimal("48500"),
            close=Decimal("50000"),
            volume=Decimal("8000"),
            interval="4h",
        )
        d = dataclasses.asdict(bar)
        bar2 = OHLCVRef(**dict(d.items()))
        assert bar2.venue == bar.venue
        assert bar2.interval == "4h"
        assert bar2.close == bar.close


# ---------------------------------------------------------------------------
# Import boundary: URDI schemas import InstrumentRecord from UIC (not UAC)
# ---------------------------------------------------------------------------


class TestURDIImportBoundary:
    """URDI schemas.py imports InstrumentRecord from unified_api_contracts.internal (UIC), not UAC."""

    def test_instrument_record_from_uic(self) -> None:
        """InstrumentRecord re-exported from URDI must come from UIC, not UAC."""
        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data import InstrumentRecord as URDIInstrumentRecord

        assert URDIInstrumentRecord is InstrumentRecord, "URDI InstrumentRecord must alias UIC InstrumentRecord"

    def test_backward_compat_aliases_removed(self) -> None:
        """CanonicalInstrument and InstrumentRef shims must not be present in schemas."""
        import instruments_service.reference_data.schemas as schemas

        assert not hasattr(schemas, "CanonicalInstrument"), (
            "CanonicalInstrument backward-compat alias must be removed from schemas"
        )
        assert not hasattr(schemas, "InstrumentRef"), "InstrumentRef backward-compat alias must be removed from schemas"

    def test_urdi_schemas_module_does_not_import_from_uac_normalised(self) -> None:
        """URDI schemas.py must not import from unified_api_contracts.canonical.

        URDI is T0 — must only import from UIC for domain types, not from AC normalised contracts.
        """
        from pathlib import Path

        import instruments_service.reference_data

        schemas_path = Path(instruments_service.reference_data.__file__).resolve().parent / "schemas.py"
        source = schemas_path.read_text()
        forbidden = "unified_api_contracts.canonical"
        assert forbidden not in source, f"URDI schemas.py imports from {forbidden!r} — should use UIC types instead"


# ---------------------------------------------------------------------------
# BaseReferenceDataAdapter abstract interface
# ---------------------------------------------------------------------------


class TestBaseReferenceDataAdapterInterface:
    """BaseReferenceDataAdapter defines the correct abstract methods."""

    def test_abstract_methods_defined(self) -> None:
        import inspect

        from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter

        abstract_methods = {
            name
            for name, method in inspect.getmembers(BaseReferenceDataAdapter, predicate=inspect.isfunction)
            if getattr(method, "__isabstractmethod__", False)
        }
        required = {
            "get_instruments",
            "get_instrument",
            "get_options_chain",
            "get_expiry_calendar",
            "get_funding_rate",
            "get_ohlcv",
        }
        missing = required - abstract_methods
        assert missing == set(), f"BaseReferenceDataAdapter missing abstract methods: {missing}"

    def test_parse_raw_method_present(self) -> None:
        from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter

        assert hasattr(BaseReferenceDataAdapter, "_parse_raw"), "BaseReferenceDataAdapter must have _parse_raw() method"

    def test_venue_property_is_abstract(self) -> None:
        from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter

        # venue must be an abstract property
        assert "venue" in BaseReferenceDataAdapter.__abstractmethods__, (
            "venue must be declared as @abstractmethod on BaseReferenceDataAdapter"
        )


# ---------------------------------------------------------------------------
# _get_with_retry: success, retryable status, all-attempts-failed paths
# ---------------------------------------------------------------------------


class _ConcreteRetryAdapter:
    """Minimal concrete adapter to test _get_with_retry without requiring all abstract methods."""

    @property
    def venue(self) -> str:
        return "test_retry_venue"


class TestGetWithRetry:
    """_get_with_retry on BaseReferenceDataAdapter covers success, retry, and failure paths."""

    def _make_adapter(self) -> _RetryTestAdapter:
        """Return a concrete adapter for testing _get_with_retry."""
        return _RetryTestAdapter()

    @pytest.mark.asyncio
    async def test_get_with_retry_success(self) -> None:
        """_get_with_retry returns parsed JSON on successful 200 response."""
        import aiohttp

        adapter = self._make_adapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"key": "value"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock(spec=aiohttp.ClientSession)
        mock_session.get = MagicMock(return_value=mock_cm)

        result = await adapter._get_with_retry(mock_session, "https://api.example.com/data")
        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_get_with_retry_raises_on_client_error(self) -> None:
        """_get_with_retry raises RuntimeError after all retries on ClientError."""
        import aiohttp

        adapter = self._make_adapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock(spec=aiohttp.ClientSession)
        mock_session.get = MagicMock(return_value=mock_cm)

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="attempts failed"),
        ):
            await adapter._get_with_retry(
                mock_session, "https://api.example.com/data"
            )

    @pytest.mark.asyncio
    async def test_get_with_retry_retryable_status_raises_after_max_attempts(self) -> None:
        """_get_with_retry raises RuntimeError on persistent 429 (retryable status)."""
        import aiohttp

        adapter = self._make_adapter()
        mock_resp = AsyncMock()
        mock_resp.status = 429  # retryable
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock(spec=aiohttp.ClientSession)
        mock_session.get = MagicMock(return_value=mock_cm)

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="429"),
        ):
            await adapter._get_with_retry(
                mock_session, "https://api.example.com/data"
            )
