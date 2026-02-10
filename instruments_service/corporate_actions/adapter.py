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
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

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
        """Lazy load yfinance."""
        if self._yf is None:
            import yfinance as yf

            self._yf = yf
        return self._yf

    def _rate_limit(self, delay: Optional[float] = None):
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
    ) -> List[DividendRecord]:
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
    ) -> List[DividendRecord]:
        """Fetch dividends using yfinance."""
        self._rate_limit()
        dividends = []

        try:
            stock = self.yf.Ticker(ticker)
            div_df = stock.dividends

            if div_df is None or div_df.empty:
                logger.debug(f"No dividends found for {ticker}")
                return []

            # Convert index to date for filtering
            div_df.index = pd.to_datetime(div_df.index, utc=True).date

            # Filter by date range
            mask = (div_df.index >= start_date) & (div_df.index <= end_date)
            div_df = div_df[mask]

            for ex_date, amount in div_df.items():
                if pd.isna(amount) or amount <= 0:
                    continue

                try:
                    record = DividendRecord(
                        ticker=ticker,
                        ex_date=ex_date,
                        amount=float(amount),
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
    ) -> List[StockSplitRecord]:
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
        splits = []

        try:
            stock = self.yf.Ticker(ticker)
            splits_df = stock.splits

            if splits_df is None or splits_df.empty:
                logger.debug(f"No splits found for {ticker}")
                return []

            # Convert index to date for filtering
            splits_df.index = pd.to_datetime(splits_df.index, utc=True).date

            # Filter by date range
            mask = (splits_df.index >= start_date) & (splits_df.index <= end_date)
            splits_df = splits_df[mask]

            for effective_date, ratio in splits_df.items():
                if pd.isna(ratio) or ratio <= 0:
                    continue

                try:
                    # yfinance returns ratio as "new shares per old share"
                    # e.g., 4.0 means 4:1 split (1 share becomes 4)
                    ratio_float = float(ratio)

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
                        effective_date=effective_date,
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
    ) -> List[EarningsRecord]:
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
    ) -> List[EarningsRecord]:
        """Fetch earnings using yfinance."""
        self._rate_limit()
        earnings = []

        try:
            stock = self.yf.Ticker(ticker)

            # Try to get earnings dates from calendar
            try:
                calendar = stock.calendar
                if calendar is not None and not calendar.empty:
                    # Future earnings date
                    if "Earnings Date" in calendar.index:
                        calendar.loc["Earnings Date"]
                        # This is typically future dates, not historical
            except Exception as e:
                logger.debug(f"Failed to process earnings date from calendar: {e}")
                # Error handled, continue processing next calendar entry
                pass

            # Get historical earnings from earnings_history or quarterly_earnings
            try:
                earnings_df = stock.earnings_dates

                if earnings_df is not None and not earnings_df.empty:
                    for idx, row in earnings_df.iterrows():
                        try:
                            # idx is the earnings date (datetime)
                            earnings_date = pd.to_datetime(idx, utc=True).date()

                            # Filter by date range
                            if earnings_date < start_date or earnings_date > end_date:
                                continue

                            reported_eps = row.get("Reported EPS", None)
                            estimated_eps = row.get("EPS Estimate", None)
                            surprise_pct = row.get("Surprise(%)", None)

                            # Clean up NaN values
                            if pd.isna(reported_eps):
                                reported_eps = None
                            if pd.isna(estimated_eps):
                                estimated_eps = None
                            if pd.isna(surprise_pct):
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
        tickers: List[str],
        start_date: date,
        end_date: date,
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, CorporateActionsBundle]:
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
        results = {}
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
        bundles: Dict[str, CorporateActionsBundle],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Convert bundles to DataFrames for storage.

        Args:
            bundles: Dict of ticker -> CorporateActionsBundle

        Returns:
            Tuple of (dividends_df, splits_df, earnings_df)
        """
        dividends = []
        splits = []
        earnings = []

        for ticker, bundle in bundles.items():
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
