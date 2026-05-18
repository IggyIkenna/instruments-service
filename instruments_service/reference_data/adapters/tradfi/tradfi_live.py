"""TradFi live-mode adapter — GCS-first with Databento fallback.

In live mode, reads the most recent GCS instrument snapshot for the venue,
filters expired instruments, and returns. Falls back to Databento (T-3 days
due to T+2 embargo) if no GCS data is available.

This avoids the Databento API call on every live refresh and lets the system
react to expiries from the existing instrument universe.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import UTC, date, datetime, timedelta

import pandas as pd
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import get_bucket_name, get_storage_client

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from .databento import DatabentoReferenceDataAdapter

logger = logging.getLogger(__name__)

# Maximum number of days to look back for a GCS snapshot.
_GCS_LOOKBACK_DAYS = 7


def _dataframe_to_instrument_records(df: pd.DataFrame) -> list[InstrumentRecord]:
    """Convert a parquet-deserialized DataFrame back to InstrumentRecord list.

    Handles type coercion:
    - ``legs`` column: stored as JSON string → parsed back to list[dict]
    - ``NaT`` / ``NaN`` → None for optional fields
    - Pydantic model_validate handles Decimal/enum coercion automatically.
    """
    records: list[InstrumentRecord] = []
    for _, row_series in df.iterrows():
        row_dict: dict[str, object] = dict(row_series)
        # Convert pandas NaT / NaN to None for optional fields
        cleaned: dict[str, object] = {}
        for k_raw, v_raw in row_dict.items():
            k = str(k_raw)
            if v_raw is None or (isinstance(v_raw, float) and v_raw != v_raw):
                # NaN check: float NaN != itself. Also catches pd.NaT via None.
                cleaned[k] = None
            elif hasattr(v_raw, "isoformat") and str(v_raw) == "NaT":
                cleaned[k] = None
            else:
                cleaned[k] = v_raw

        # Parse legs JSON string back to list[dict]
        legs_val = cleaned.get("legs")
        if isinstance(legs_val, str):
            try:
                cleaned["legs"] = json.loads(legs_val)
            except (ValueError, json.JSONDecodeError):
                cleaned["legs"] = None

        try:
            records.append(InstrumentRecord.model_validate(cleaned))
        except Exception as exc:
            logger.debug("Skipping record during GCS deserialization: %s", exc)
    return records


class TradFiLiveReferenceDataAdapter(BaseReferenceDataAdapter):
    """GCS-first live mode for TradFi instruments.

    1. Reads the most recent GCS instrument snapshot for the venue
       (tries today, yesterday, ... up to 7 days back).
    2. Filters out expired instruments (expiry < now).
    3. Returns the active set.
    4. If no GCS data found, falls back to Databento (T-3 due to embargo).
    """

    def __init__(
        self,
        venue_filter: str | None = None,
        api_key: str | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._venue_filter = venue_filter

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._venue_filter or "TRADFI"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch instruments: GCS snapshot first, Databento fallback."""
        # 1. Try GCS — find the most recent snapshot
        records = self._read_most_recent_gcs_snapshot()
        if records is not None:
            now = datetime.now(UTC)
            active = [r for r in records if r.expiry is None or r.expiry >= now]
            logger.info(
                "TradFi live [%s]: %d active instruments from GCS (%d expired filtered)",
                self._venue_filter,
                len(active),
                len(records) - len(active),
            )
            if instrument_type is not None:
                active = [r for r in active if r.instrument_type == instrument_type]
            return active

        # 2. Fallback: Databento with T-3 days (T+2 embargo)
        logger.info(
            "TradFi live [%s]: no GCS snapshot found in last %d days — falling back to Databento",
            self._venue_filter,
            _GCS_LOOKBACK_DAYS,
        )

        target = date.today() - timedelta(days=3)
        fallback = DatabentoReferenceDataAdapter(
            project_id=self._project_id,
            api_key=self._api_key,
            target_date=target,
            venue_filter=self._venue_filter,
        )
        results = await fallback.get_instruments(instrument_type=instrument_type)
        # Filter expired even from Databento (3-day-old data may include expired instruments)
        now = datetime.now(UTC)
        return [r for r in results if r.expiry is None or r.expiry >= now]

    def _read_most_recent_gcs_snapshot(self) -> list[InstrumentRecord] | None:
        """Try the last N days of GCS snapshots, return the first hit."""
        try:
            storage = get_storage_client()
            bucket = get_bucket_name("instruments", "tradfi")
            venue = self._venue_filter or ""

            today = date.today()
            for days_ago in range(_GCS_LOOKBACK_DAYS):
                target_day = (today - timedelta(days=days_ago)).isoformat()
                blob_path = f"instrument_availability/by_date/day={target_day}/venue={venue}/instruments.parquet"
                data: bytes | None = storage.download_bytes(bucket, blob_path)
                if data:
                    df = pd.read_parquet(io.BytesIO(data))
                    if df.empty:
                        continue
                    records = _dataframe_to_instrument_records(df)
                    logger.info(
                        "TradFi live [%s]: loaded %d instruments from GCS (day=%s, %d days ago)",
                        venue,
                        len(records),
                        target_day,
                        days_ago,
                    )
                    return records
        except Exception as exc:
            logger.warning("TradFi live GCS read failed for %s: %s", self._venue_filter, exc)
        return None

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Use batch mode for TradFi options chain queries")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Use batch mode for TradFi expiry calendar queries")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("TradFi has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("TradFi OHLCV not supported via reference data")
