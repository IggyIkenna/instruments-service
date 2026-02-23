"""
Corporate Actions Data Models

Defines Pydantic models for dividends, stock splits, and earnings records.
These are reference data tied to equity instruments for price normalization.
"""

import math
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class CorporateActionType(str, Enum):
    """Types of corporate actions."""

    DIVIDEND = "dividend"
    SPLIT = "split"
    EARNINGS = "earnings"
    SPINOFF = "spinoff"


class DividendType(str, Enum):
    """Types of dividends."""

    REGULAR = "regular"  # Normal quarterly/monthly dividend
    SPECIAL = "special"  # One-time special dividend
    QUALIFIED = "qualified"  # Qualified for tax purposes
    UNSPECIFIED = "unspecified"  # Unknown/not specified


class DividendRecord(BaseModel):
    """
    Dividend record for an equity instrument.

    Used for:
    - Price adjustment (adjusted close calculations)
    - Dividend reinvestment modeling
    - Yield calculations
    """

    ticker: str = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    ex_date: date = Field(..., description="Ex-dividend date (date after which buyers don't receive dividend)")
    pay_date: Optional[date] = Field(default=None, description="Payment date when dividend is distributed")
    record_date: Optional[date] = Field(default=None, description="Record date for determining eligible shareholders")
    declaration_date: Optional[date] = Field(default=None, description="Date dividend was announced")
    amount: float = Field(..., ge=0, description="Dividend amount per share in USD")
    dividend_type: DividendType = Field(default=DividendType.UNSPECIFIED, description="Type of dividend")
    currency: str = Field(default="USD", description="Currency of dividend payment")
    source: str = Field(default="yfinance", description="Data source")
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="When data was fetched"
    )

    # For canonical instrument key matching
    instrument_key: Optional[str] = Field(default=None, description="Canonical instrument key (e.g., NYSE:EQUITY:AAPL)")

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Ensure ticker is uppercase and non-empty."""
        if not v or not v.strip():
            raise ValueError("Ticker cannot be empty")
        return v.upper().strip()

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, v: object) -> float:  # type: ignore[reportAny]
        """Handle NaN and None values."""
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return 0.0
        return float(v)

    def to_dict(self) -> dict[str, str | float | None]:
        """Convert to dictionary for DataFrame/Parquet storage."""
        return {
            "ticker": self.ticker,
            "ex_date": self.ex_date.isoformat(),
            "pay_date": self.pay_date.isoformat() if self.pay_date else None,
            "record_date": self.record_date.isoformat() if self.record_date else None,
            "declaration_date": self.declaration_date.isoformat() if self.declaration_date else None,
            "amount": self.amount,
            "dividend_type": self.dividend_type.value,
            "currency": self.currency,
            "source": self.source,
            "fetched_at": self.fetched_at.isoformat(),
            "instrument_key": self.instrument_key,
        }


class StockSplitRecord(BaseModel):
    """
    Stock split record for an equity instrument.

    Used for:
    - Historical price adjustment (split-adjusted prices)
    - Volume adjustment
    - Share count calculations

    Split ratios:
    - Forward split (e.g., 4:1): ratio = 4.0 (each share becomes 4)
    - Reverse split (e.g., 1:10): ratio = 0.1 (10 shares become 1)
    """

    ticker: str = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    effective_date: date = Field(..., description="Date split takes effect")
    ratio: float = Field(..., gt=0, description="Split ratio (4.0 for 4:1 split, 0.1 for 1:10 reverse split)")
    split_from: int = Field(default=1, ge=1, description="Original share count (e.g., 1 in 4:1 split)")
    split_to: int = Field(default=1, ge=1, description="New share count (e.g., 4 in 4:1 split)")
    source: str = Field(default="yfinance", description="Data source")
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="When data was fetched"
    )

    # For canonical instrument key matching
    instrument_key: Optional[str] = Field(default=None, description="Canonical instrument key")

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Ensure ticker is uppercase and non-empty."""
        if not v or not v.strip():
            raise ValueError("Ticker cannot be empty")
        return v.upper().strip()

    @field_validator("ratio", mode="before")
    @classmethod
    def validate_ratio(cls, v: str | float) -> float:
        """Handle string ratios like '4:1'."""
        if isinstance(v, str):
            if ":" in v:
                parts = v.split(":")
                if len(parts) == 2:
                    return float(parts[0]) / float(parts[1])
            return float(v)
        return v

    @property
    def is_reverse_split(self) -> bool:
        """True if this is a reverse split (ratio < 1)."""
        return self.ratio < 1.0

    @property
    def adjustment_factor(self) -> float:
        """
        Factor to multiply historical prices by to get split-adjusted prices.
        For a 4:1 split, historical prices should be divided by 4 (factor = 0.25).
        For a 1:10 reverse split, historical prices should be multiplied by 10 (factor = 10).
        """
        return 1.0 / self.ratio

    def to_dict(self) -> dict[str, str | float | int | bool | None]:
        """Convert to dictionary for DataFrame/Parquet storage."""
        return {
            "ticker": self.ticker,
            "effective_date": self.effective_date.isoformat(),
            "ratio": self.ratio,
            "split_from": self.split_from,
            "split_to": self.split_to,
            "is_reverse_split": self.is_reverse_split,
            "adjustment_factor": self.adjustment_factor,
            "source": self.source,
            "fetched_at": self.fetched_at.isoformat(),
            "instrument_key": self.instrument_key,
        }


class EarningsRecord(BaseModel):
    """
    Earnings announcement record for an equity instrument.

    Used for:
    - Volatility modeling (earnings volatility premium)
    - Event-driven strategies
    - Fundamental analysis
    """

    ticker: str = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    earnings_date: date = Field(..., description="Earnings announcement date")
    earnings_time: Optional[str] = Field(
        default=None,
        description="Time of announcement: 'BMO' (before market open), 'AMC' (after market close), or None",
    )
    fiscal_quarter: Optional[str] = Field(default=None, description="Fiscal quarter (e.g., 'Q1 2024')")
    fiscal_year: Optional[int] = Field(default=None, description="Fiscal year")
    reported_eps: Optional[float] = Field(default=None, description="Actual reported EPS")
    estimated_eps: Optional[float] = Field(default=None, description="Consensus estimated EPS before announcement")
    surprise_pct: Optional[float] = Field(
        default=None, description="Earnings surprise percentage ((actual-estimate)/estimate * 100)"
    )
    revenue: Optional[float] = Field(default=None, description="Reported revenue in USD")
    estimated_revenue: Optional[float] = Field(default=None, description="Estimated revenue in USD")
    source: str = Field(default="yfinance", description="Data source")
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When data was fetched",
    )

    # For canonical instrument key matching
    instrument_key: Optional[str] = Field(default=None, description="Canonical instrument key")

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Ensure ticker is uppercase and non-empty."""
        if not v or not v.strip():
            raise ValueError("Ticker cannot be empty")
        return v.upper().strip()

    @field_validator("earnings_time")
    @classmethod
    def validate_earnings_time(cls, v: str | None) -> str | None:
        """Validate earnings time format."""
        if v is None:
            return None
        v = v.upper().strip()
        valid_times = ["BMO", "AMC", "TAS", "TNS"]  # Before Market Open, After Market Close, Time Not Supplied
        if v not in valid_times:
            return None  # Return None for unknown formats
        return v

    def to_dict(self) -> dict[str, str | float | int | None]:
        """Convert to dictionary for DataFrame/Parquet storage."""
        return {
            "ticker": self.ticker,
            "earnings_date": self.earnings_date.isoformat(),
            "earnings_time": self.earnings_time,
            "fiscal_quarter": self.fiscal_quarter,
            "fiscal_year": self.fiscal_year,
            "reported_eps": self.reported_eps,
            "estimated_eps": self.estimated_eps,
            "surprise_pct": self.surprise_pct,
            "revenue": self.revenue,
            "estimated_revenue": self.estimated_revenue,
            "source": self.source,
            "fetched_at": self.fetched_at.isoformat(),
            "instrument_key": self.instrument_key,
        }


class CorporateActionsBundle(BaseModel):
    """
    Bundle of all corporate actions for a ticker.

    Used for batch fetching and storage.
    """

    ticker: str = Field(..., description="Stock ticker symbol")
    dividends: list[DividendRecord] = Field(default_factory=list, description="Dividend records")
    splits: list[StockSplitRecord] = Field(default_factory=list, description="Stock split records")
    earnings: list[EarningsRecord] = Field(default_factory=list, description="Earnings records")
    start_date: date = Field(..., description="Start of data range")
    end_date: date = Field(..., description="End of data range")
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="yfinance")

    @property
    def total_records(self) -> int:
        """Total number of corporate action records."""
        return len(self.dividends) + len(self.splits) + len(self.earnings)
