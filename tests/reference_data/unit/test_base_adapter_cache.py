"""Tests for BaseReferenceDataAdapter caching and retry mechanisms."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.base_adapter import BaseReferenceDataAdapter
from instruments_service.reference_data.schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)


class _CacheTestAdapter(BaseReferenceDataAdapter):
    """Concrete adapter for testing caching behavior."""

    call_count = 0

    @property
    def venue(self) -> str:
        return "test_cache"

    async def get_instruments(self, instrument_type: str | None = None) -> list[InstrumentRecord]:
        self.call_count += 1
        return [
            InstrumentRecord(
                instrument_key="TEST:BTCUSDT",
                venue="test_cache",
                instrument_type="SPOT_PAIR",
                base_asset="BTC",
                quote_asset="USDT",
            )
        ]

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        return CanonicalOptionsChain(venue="test_cache", underlying=underlying, expiry=expiry or datetime.now(UTC))

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "FUTURE") -> CanonicalExpiryCalendar:
        return CanonicalExpiryCalendar(venue="test_cache", instrument_type=instrument_type, underlying=underlying)

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        return []


class TestGetInstrumentsCached:
    @pytest.mark.asyncio
    async def test_cache_hit_avoids_refetch(self) -> None:
        adapter = _CacheTestAdapter()
        adapter.call_count = 0
        r1 = await adapter.get_instruments_cached()
        r2 = await adapter.get_instruments_cached()
        assert len(r1) == 1
        assert len(r2) == 1
        assert adapter.call_count == 1  # only one real fetch

    @pytest.mark.asyncio
    async def test_cache_miss_on_different_type(self) -> None:
        adapter = _CacheTestAdapter()
        adapter.call_count = 0
        await adapter.get_instruments_cached(instrument_type=None)
        await adapter.get_instruments_cached(instrument_type="SPOT_PAIR")
        assert adapter.call_count == 2  # different keys = different caches

    @pytest.mark.asyncio
    async def test_cache_expired_refetches(self) -> None:
        adapter = _CacheTestAdapter()
        adapter._cache_ttl = 0.01  # 10ms TTL
        adapter.call_count = 0
        await adapter.get_instruments_cached()
        time.sleep(0.02)  # Wait for TTL to expire
        await adapter.get_instruments_cached()
        assert adapter.call_count == 2  # TTL expired, refetched

    def test_clear_cache(self) -> None:
        adapter = _CacheTestAdapter()
        adapter._instruments_cache["test"] = ([], time.monotonic())
        adapter.clear_cache()
        assert len(adapter._instruments_cache) == 0


class TestGetWithRetry:
    @pytest.mark.asyncio
    async def test_success_on_first_try(self) -> None:
        adapter = _CacheTestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"data": "ok"})
        mock_resp.raise_for_status = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        result = await adapter._get_with_retry(mock_session, "http://test.com/api")
        assert result == {"data": "ok"}

    @pytest.mark.asyncio
    async def test_all_retries_fail_raises(self) -> None:
        import aiohttp

        adapter = _CacheTestAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("fail"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with pytest.raises(RuntimeError, match="All 3 attempts failed"):
            await adapter._get_with_retry(mock_session, "http://test.com/api")


class TestGetExchangeFeeSchedule:
    @pytest.mark.asyncio
    async def test_default_returns_none(self) -> None:
        adapter = _CacheTestAdapter()
        result = await adapter.get_exchange_fee_schedule("client-123")
        assert result is None


class TestGetCorporateActionsDefault:
    @pytest.mark.asyncio
    async def test_default_returns_empty(self) -> None:
        adapter = _CacheTestAdapter()
        result = await adapter.get_corporate_actions("BTC", datetime.now(UTC))
        assert result == []
