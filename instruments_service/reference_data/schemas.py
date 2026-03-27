"""Canonical reference/static data schemas for unified-reference-data-interface.

Reference data is slow-moving and cached — distinct from live market data (UMI)
and from config (UCI). Data originates from external exchange REST APIs.

# SCHEMA_PROVENANCE_EXEMPT — these types are the public interface contract for
# instruments_service.reference_data (folded from unified-reference-data-interface).
# They are service-specific and not general-purpose domain contracts.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord


@dataclass
class CanonicalOptionsChain:
    """Options chain snapshot — available strikes and expiries for an underlying."""

    venue: str
    underlying: str
    expiry: datetime
    strikes: list[Decimal] = field(default_factory=list)
    calls: list[InstrumentRecord] = field(default_factory=list)
    puts: list[InstrumentRecord] = field(default_factory=list)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CanonicalExpiryCalendar:
    """Expiry/delivery schedule for futures and options."""

    venue: str
    instrument_type: str
    underlying: str
    expiries: list[datetime] = field(default_factory=list)
    settlement_assets: dict[str, str] = field(default_factory=dict)
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CanonicalCorporateAction:
    """Corporate action event (TradFi only — IBKR, Bloomberg)."""

    venue: str
    symbol: str
    action_type: str
    effective_date: datetime
    ratio: Decimal | None
    cash_amount: Decimal | None
    currency: str | None
    description: str = ""


@dataclass
class FundingRateRef:
    """Funding rate snapshot for a perpetual contract."""

    venue: str
    symbol: str
    rate: Decimal
    next_funding_time: datetime
    mark_price: Decimal | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class OHLCVRef:
    """OHLCV bar (candlestick) for a symbol at a given interval."""

    venue: str
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    interval: str = "1d"
