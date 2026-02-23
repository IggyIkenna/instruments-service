"""
Corporate Actions Adapter

Fetches corporate actions data for US equities (TRADFI).
Supports dividends, stock splits, and earnings dates.

Data Source:
- yfinance: Free, no API key, good historical coverage (20+ years)

Usage:
    adapter = CorporateActionsAdapter()
    bundle = adapter.fetch_corporate_actions("AAPL", start_date, end_date)
    dividends = adapter.fetch_dividends("AAPL", start_date, end_date)
    splits = adapter.fetch_splits("AAPL", start_date, end_date)
    earnings = adapter.fetch_earnings("AAPL", start_date, end_date)
    bundles = adapter.fetch_batch(["AAPL", "MSFT", "GOOGL"], start_date, end_date)

Note: Corporate actions are TRADFI-only (equities). Crypto/DeFi do not have
traditional corporate actions like dividends or stock splits.
"""

import logging
import time
from collections.abc import Callable
from datetime import date
from typing import Any, Optional, cast

import pandas as pd
import yfinance as yf

from instruments_service.corporate_actions.models import (
    CorporateActionsBundle,
    DividendRecord,
    DividendType,
    EarningsRecord,
    StockSplitRecord,
)

logger = logging.getLogger(__name__)

# Rate limiting for yfinance (to avoid IP blocks)
YFINANCE_RATE_LIMIT_DELAY = 0.1  # 100ms between requests


class CorporateActionsAdapter:
    """
    Adapter for fetching corporate actions data from yfinance.

    TRADFI-only: Corporate actions (dividends, splits, earnings) apply to
    equities only. Crypto and DeFi do not have traditional corporate actions.
    """

    def __init__(
        self,
        rate_limit_delay: float = YFINANCE_RATE_LIMIT_DELAY,
    ):
        """
        Initialize corporate actions adapter.

        Args:
            rate_limit_delay: Delay between requests in seconds (default: 100ms)
        """
        self.rate_limit_delay = rate_limit_delay

        # Track last request time for rate limiting
        self._last_request_time: float = 0

        # Import providers lazily
        self._yf = None

        logger.info("CorporateActionsAdapter initialized")

    @property
    def yf(self):
        """Lazy load yfinance (cached)."""
        if self._yf is None:
            self._yf = yf
        return self._yf

    def _rate_limit(self, delay: Optional[float] = None) -> None:
        """Apply rate limiting between requests."""
        delay = delay or self.rate_limit_delay
        elapsed = time.time() - self._last_request_time
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()

    def _parse_dividend_type(self, row: pd.Series) -> DividendType:
        """Infer dividend type from data."""
        # yfinance doesn't provide dividend type directly
        # We could infer "special" from unusually large amounts, but safer to mark as unspecified
        return DividendType.UNSPECIFIED

    def fetch_dividends(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> list[DividendRecord]:
        """
        Fetch dividend history for a ticker.

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL")
            start_date: Start date for data range
            end_date: End date for data range
            instrument_key: Optional canonical instrument key

        Returns:
            List of DividendRecord objects
        """
        return self._fetch_dividends_yfinance(ticker, start_date, end_date, instrument_key)

    def _fetch_dividends_yfinance(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> list[DividendRecord]:
        """Fetch dividends using yfinance."""
        self._rate_limit()
        dividends: list[DividendRecord] = []

        try:
            stock = self.yf.Ticker(ticker)
            div_df: pd.Series = stock.dividends

            if div_df is None or div_df.empty:
                logger.debug(f"No dividends found for {ticker}")
                return []

            # Convert index to date for filtering
            div_df.index = pd.to_datetime(div_df.index, utc=True).date

            # Filter by date range
            mask = (div_df.index >= start_date) & (div_df.index <= end_date)
            div_df = div_df[mask]

            for ex_date, amount in div_df.items():  # type: ignore[reportAny]
                if not isinstance(amount, (int, float)) or pd.isna(amount) or amount <= 0:
                    continue
                amount_float = float(amount)

                try:
                    record = DividendRecord(
                        ticker=ticker,
                        ex_date=cast(date, ex_date),
                        amount=amount_float,
                        dividend_type=DividendType.UNSPECIFIED,
                        source="yfinance",
                        instrument_key=instrument_key,
                    )
                    dividends.append(record)
                except Exception as e:
                    logger.warning(f"Failed to parse dividend for {ticker} on {ex_date}: {e}")

            logger.debug(f"Fetched {len(dividends)} dividends for {ticker}")

        except Exception as e:
            logger.error(f"Failed to fetch dividends for {ticker}: {e}")

        return dividends

    def fetch_splits(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> list[StockSplitRecord]:
        """
        Fetch stock split history for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data range
            end_date: End date for data range
            instrument_key: Optional canonical instrument key

        Returns:
            List of StockSplitRecord objects
        """
        self._rate_limit()
        splits: list[StockSplitRecord] = []

        try:
            stock = self.yf.Ticker(ticker)
            splits_df: pd.Series = stock.splits  # type: ignore[assignment]

            if splits_df is None or splits_df.empty:
                logger.debug(f"No splits found for {ticker}")
                return []

            # Convert index to date for filtering
            splits_df.index = pd.to_datetime(splits_df.index, utc=True).date

            # Filter by date range
            mask = (splits_df.index >= start_date) & (splits_df.index <= end_date)
            splits_df = splits_df[mask]

            for effective_date, ratio in splits_df.items():  # type: ignore[reportAny]
                if not isinstance(ratio, (int, float)) or pd.isna(ratio) or ratio <= 0:
                    continue
                ratio_float = float(ratio)

                try:
                    # yfinance returns ratio as "new shares per old share"
                    # e.g., 4.0 means 4:1 split (1 share becomes 4)

                    # Determine split_from and split_to
                    if ratio_float >= 1:
                        split_to = int(ratio_float)
                        split_from = 1
                    else:
                        # Reverse split
                        split_from = int(1 / ratio_float)
                        split_to = 1

                    record = StockSplitRecord(
                        ticker=ticker,
                        effective_date=cast(date, effective_date),
                        ratio=ratio_float,
                        split_from=split_from,
                        split_to=split_to,
                        source="yfinance",
                        instrument_key=instrument_key,
                    )
                    splits.append(record)
                except Exception as e:
                    logger.warning(f"Failed to parse split for {ticker} on {effective_date}: {e}")

            logger.debug(f"Fetched {len(splits)} splits for {ticker}")

        except Exception as e:
            logger.error(f"Failed to fetch splits for {ticker}: {e}")

        return splits

    def fetch_earnings(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> list[EarningsRecord]:
        """
        Fetch earnings history for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data range
            end_date: End date for data range
            instrument_key: Optional canonical instrument key

        Returns:
            List of EarningsRecord objects
        """
        return self._fetch_earnings_yfinance(ticker, start_date, end_date, instrument_key)

    def _fetch_earnings_yfinance(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> list[EarningsRecord]:
        """Fetch earnings using yfinance."""
        self._rate_limit()
        earnings: list[EarningsRecord] = []

        try:
            stock = self.yf.Ticker(ticker)

            # Try to get earnings dates from calendar
            try:
                calendar: object = stock.calendar  # type: ignore[reportAny]
                if calendar is not None:
                    cal_df = cast(pd.DataFrame, calendar)
                    if not cal_df.empty and "Earnings Date" in cal_df.index:
                        _ = cal_df.loc["Earnings Date"]  # noqa: F841
                        # This is typically future dates, not historical
            except Exception as e:
                logger.debug(f"Failed to process earnings date from calendar: {e}")
                # Error handled, continue processing next calendar entry
                pass

            # Get historical earnings from earnings_history or quarterly_earnings
            try:
                earnings_df: pd.DataFrame = stock.earnings_dates  # type: ignore[assignment]

                if earnings_df is not None and not earnings_df.empty:
                    for idx, row in earnings_df.iterrows():
                        try:
                            # idx is the earnings date (datetime)
                            earnings_date = pd.to_datetime(cast(float | str | date, idx), utc=True).date()

                            # Filter by date range
                            if earnings_date < start_date or earnings_date > end_date:
                                continue

                            reported_eps: float | None = row.get("Reported EPS", None)
                            estimated_eps: float | None = row.get("EPS Estimate", None)
                            surprise_pct: float | None = row.get("Surprise(%)", None)

                            # Clean up NaN values
                            if reported_eps is not None and pd.isna(reported_eps):
                                reported_eps = None
                            if estimated_eps is not None and pd.isna(estimated_eps):
                                estimated_eps = None
                            if surprise_pct is not None and pd.isna(surprise_pct):
                                surprise_pct = None

                            record = EarningsRecord(
                                ticker=ticker,
                                earnings_date=earnings_date,
                                reported_eps=reported_eps,
                                estimated_eps=estimated_eps,
                                surprise_pct=surprise_pct,
                                source="yfinance",
                                instrument_key=instrument_key,
                            )
                            earnings.append(record)
                        except Exception as e:
                            logger.warning(f"Failed to parse earnings for {ticker} on {idx}: {e}")

            except Exception as e:
                logger.debug(f"No earnings_dates for {ticker}: {e}")

            logger.debug(f"Fetched {len(earnings)} earnings records for {ticker}")

        except Exception as e:
            logger.error(f"Failed to fetch earnings for {ticker}: {e}")

        return earnings

    def fetch_corporate_actions(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        instrument_key: Optional[str] = None,
    ) -> CorporateActionsBundle:
        """
        Fetch all corporate actions for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data range
            end_date: End date for data range
            instrument_key: Optional canonical instrument key

        Returns:
            CorporateActionsBundle with dividends, splits, and earnings
        """
        dividends = self.fetch_dividends(ticker, start_date, end_date, instrument_key)
        splits = self.fetch_splits(ticker, start_date, end_date, instrument_key)
        earnings = self.fetch_earnings(ticker, start_date, end_date, instrument_key)

        return CorporateActionsBundle(
            ticker=ticker,
            dividends=dividends,
            splits=splits,
            earnings=earnings,
            start_date=start_date,
            end_date=end_date,
            source="yfinance",
        )

    def fetch_batch(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> dict[str, CorporateActionsBundle]:
        """
        Fetch corporate actions for multiple tickers.

        Args:
            tickers: List of stock ticker symbols
            start_date: Start date for data range
            end_date: End date for data range
            progress_callback: Optional callback(ticker, current, total) for progress

        Returns:
            Dict mapping ticker -> CorporateActionsBundle
        """
        results: dict[str, CorporateActionsBundle] = {}
        total = len(tickers)

        logger.info(f"Fetching corporate actions for {total} tickers ({start_date} to {end_date})")

        for i, ticker in enumerate(tickers):
            try:
                bundle = self.fetch_corporate_actions(ticker, start_date, end_date)
                results[ticker] = bundle

                if progress_callback:
                    progress_callback(ticker, i + 1, total)

                if (i + 1) % 50 == 0:
                    logger.info(f"Progress: {i + 1}/{total} tickers processed")

            except Exception as e:
                logger.error(f"Failed to fetch corporate actions for {ticker}: {e}")
                # Continue with next ticker

        logger.info(f"Completed: {len(results)}/{total} tickers fetched successfully")
        return results

    def to_dataframes(
        self,
        bundles: dict[str, CorporateActionsBundle],
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Convert bundles to DataFrames for storage.

        Args:
            bundles: Dict of ticker -> CorporateActionsBundle

        Returns:
            Tuple of (dividends_df, splits_df, earnings_df)
        """
        dividends: list[dict[str, Any]] = []
        splits: list[dict[str, Any]] = []
        earnings: list[dict[str, Any]] = []

        for _, bundle in bundles.items():
            for div in bundle.dividends:
                dividends.append(div.to_dict())
            for split in bundle.splits:
                splits.append(split.to_dict())
            for earn in bundle.earnings:
                earnings.append(earn.to_dict())

        dividends_df = pd.DataFrame(dividends) if dividends else pd.DataFrame()
        splits_df = pd.DataFrame(splits) if splits else pd.DataFrame()
        earnings_df = pd.DataFrame(earnings) if earnings else pd.DataFrame()

        # Sort by date
        if not dividends_df.empty:
            dividends_df = dividends_df.sort_values(["ticker", "ex_date"])
        if not splits_df.empty:
            splits_df = splits_df.sort_values(["ticker", "effective_date"])
        if not earnings_df.empty:
            earnings_df = earnings_df.sort_values(["ticker", "earnings_date"])

        return dividends_df, splits_df, earnings_df
