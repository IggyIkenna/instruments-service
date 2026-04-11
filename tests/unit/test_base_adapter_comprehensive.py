"""Comprehensive tests for BaseReferenceDataAdapter — cache, retry, and parse logic.

Extends the existing test_base_adapter.py with coverage for:
- get_instruments_cached() cache hit/miss and TTL expiry
- _get_with_retry() exponential backoff and error handling
- _handle_retryable_response() for various HTTP status codes
- clear_cache()
- get_exchange_fee_schedule() default implementation
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data.base_adapter import (
    _RETRY_ATTEMPTS,
    _RETRYABLE_STATUS_CODES,
    BaseReferenceDataAdapter,
)
from instruments_service.reference_data.schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)


class _TestAdapter(BaseReferenceDataAdapter):
    """Concrete adapter for testing base class functionality."""

    @property
    def venue(self) -> str:
        return "test_venue"

    async def get_instruments(self, instrument_type: str | None = None) -> list[InstrumentRecord]:
        return [
            InstrumentRecord(
                instrument_key="TEST:SPOT:BTCUSDT",
                venue="test_venue",
                instrument_type="SPOT_PAIR",
                base_asset="BTC",
                quote_asset="USDT",
            )
        ]

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=expiry or datetime.now(UTC),
            fetched_at=datetime.now(UTC),
        )

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "FUTURE") -> CanonicalExpiryCalendar:
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("test stub")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        return []


class _DateAwareAdapter(_TestAdapter):
    """Adapter that accepts date parameter in get_instruments."""

    async def get_instruments(
        self, instrument_type: str | None = None, date: str | None = None
    ) -> list[InstrumentRecord]:
        return [
            InstrumentRecord(
                instrument_key=f"TEST:SPOT:BTCUSDT:{date or 'no-date'}",
                venue="test_venue",
                instrument_type="SPOT_PAIR",
                base_asset="BTC",
                quote_asset="USDT",
            )
        ]


# ---------------------------------------------------------------------------
# get_instruments_cached
# ---------------------------------------------------------------------------


class TestGetInstrumentsCached:
    @pytest.mark.asyncio
    async def test_cache_miss_fetches_and_caches(self) -> None:
        adapter = _TestAdapter()
        result = await adapter.get_instruments_cached()
        assert len(result) == 1
        # Second call should return cached
        result2 = await adapter.get_instruments_cached()
        assert len(result2) == 1

    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached(self) -> None:
        adapter = _TestAdapter()
        # Pre-populate cache
        records = [
            InstrumentRecord(
                instrument_key="CACHED",
                venue="test_venue",
                instrument_type="SPOT_PAIR",
                base_asset="ETH",
                quote_asset="USD",
            )
        ]
        adapter._instruments_cache[(None, None)] = (records, time.monotonic())
        result = await adapter.get_instruments_cached()
        assert len(result) == 1
        assert result[0].instrument_key == "CACHED"

    @pytest.mark.asyncio
    async def test_cache_expired_refetches(self) -> None:
        adapter = _TestAdapter()
        adapter._cache_ttl = 0.0  # Immediate expiry
        records = [
            InstrumentRecord(
                instrument_key="OLD",
                venue="test_venue",
                instrument_type="SPOT_PAIR",
                base_asset="ETH",
                quote_asset="USD",
            )
        ]
        adapter._instruments_cache[(None, None)] = (records, time.monotonic() - 1.0)
        result = await adapter.get_instruments_cached()
        # Should re-fetch and get new data
        assert result[0].instrument_key == "TEST:SPOT:BTCUSDT"

    @pytest.mark.asyncio
    async def test_cache_with_instrument_type_key(self) -> None:
        adapter = _TestAdapter()
        result1 = await adapter.get_instruments_cached(instrument_type="SPOT_PAIR")
        result2 = await adapter.get_instruments_cached(instrument_type="PERPETUAL")
        # Different keys should both be cached independently
        assert (("SPOT_PAIR", None)) in adapter._instruments_cache
        assert (("PERPETUAL", None)) in adapter._instruments_cache

    @pytest.mark.asyncio
    async def test_date_aware_adapter_passes_date(self) -> None:
        adapter = _DateAwareAdapter()
        result = await adapter.get_instruments_cached(date="2024-01-01")
        assert len(result) == 1
        assert "2024-01-01" in result[0].instrument_key


class TestClearCache:
    def test_clear_cache_empties_all(self) -> None:
        adapter = _TestAdapter()
        adapter._instruments_cache[("key1", None)] = ([], time.monotonic())
        adapter._instruments_cache[("key2", None)] = ([], time.monotonic())
        adapter.clear_cache()
        assert len(adapter._instruments_cache) == 0


# ---------------------------------------------------------------------------
# _handle_retryable_response
# ---------------------------------------------------------------------------


class TestHandleRetryableResponse:
    @pytest.mark.asyncio
    async def test_retryable_status_returns_none_on_non_last_attempt(self) -> None:
        adapter = _TestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 429
        result = await adapter._handle_retryable_response(mock_resp, "https://test", attempt=0, delay=0.0)
        assert result is None

    @pytest.mark.asyncio
    async def test_retryable_status_raises_on_last_attempt(self) -> None:
        adapter = _TestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 503
        with pytest.raises(RuntimeError, match="HTTP 503"):
            await adapter._handle_retryable_response(mock_resp, "https://test", attempt=_RETRY_ATTEMPTS - 1, delay=0.0)

    @pytest.mark.asyncio
    async def test_success_returns_json(self) -> None:
        adapter = _TestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"data": "value"})
        mock_resp.raise_for_status = MagicMock()
        result = await adapter._handle_retryable_response(mock_resp, "https://test", attempt=0, delay=0.0)
        assert result == {"data": "value"}

    @pytest.mark.asyncio
    async def test_all_retryable_status_codes(self) -> None:
        adapter = _TestAdapter()
        for status in _RETRYABLE_STATUS_CODES:
            mock_resp = MagicMock()
            mock_resp.status = status
            result = await adapter._handle_retryable_response(mock_resp, "https://test", attempt=0, delay=0.0)
            assert result is None


# ---------------------------------------------------------------------------
# _get_with_retry
# ---------------------------------------------------------------------------


class TestGetWithRetry:
    @pytest.mark.asyncio
    async def test_success_on_first_try(self) -> None:
        adapter = _TestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"result": "ok"})
        mock_resp.raise_for_status = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get.return_value = mock_cm

        result = await adapter._get_with_retry(mock_session, "https://test")
        assert result == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_all_retries_fail_raises(self) -> None:
        adapter = _TestAdapter()
        mock_session = MagicMock()
        mock_session.get.side_effect = aiohttp.ClientError("connection refused")

        with pytest.raises(RuntimeError, match="All 3 attempts failed"):
            await adapter._get_with_retry(mock_session, "https://test")

    @pytest.mark.asyncio
    async def test_retry_on_429_then_success(self) -> None:
        adapter = _TestAdapter()
        # First call: 429, second call: 200
        mock_resp_429 = MagicMock()
        mock_resp_429.status = 429
        mock_cm_429 = MagicMock()
        mock_cm_429.__aenter__ = AsyncMock(return_value=mock_resp_429)
        mock_cm_429.__aexit__ = AsyncMock(return_value=None)

        mock_resp_200 = MagicMock()
        mock_resp_200.status = 200
        mock_resp_200.json = AsyncMock(return_value={"ok": True})
        mock_resp_200.raise_for_status = MagicMock()
        mock_cm_200 = MagicMock()
        mock_cm_200.__aenter__ = AsyncMock(return_value=mock_resp_200)
        mock_cm_200.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get.side_effect = [mock_cm_429, mock_cm_200]

        result = await adapter._get_with_retry(mock_session, "https://test")
        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_passes_params_and_headers(self) -> None:
        adapter = _TestAdapter()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={})
        mock_resp.raise_for_status = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get.return_value = mock_cm

        await adapter._get_with_retry(
            mock_session,
            "https://test",
            params={"key": "val"},
            headers={"Authorization": "Bearer token"},
        )
        mock_session.get.assert_called_once_with(
            "https://test",
            params={"key": "val"},
            headers={"Authorization": "Bearer token"},
        )


# ---------------------------------------------------------------------------
# get_exchange_fee_schedule
# ---------------------------------------------------------------------------


class TestGetExchangeFeeSchedule:
    @pytest.mark.asyncio
    async def test_default_returns_none(self) -> None:
        adapter = _TestAdapter()
        result = await adapter.get_exchange_fee_schedule("client-123")
        assert result is None


# ---------------------------------------------------------------------------
# _parse_raw edge cases
# ---------------------------------------------------------------------------


class TestParseRawEdgeCases:
    def test_connection_error_raises_runtime(self) -> None:
        from pydantic import BaseModel

        class BadSchema(BaseModel):
            val: int

            def __init__(self, **data: object) -> None:
                raise ConnectionError("simulated")

        adapter = _TestAdapter()
        with (
            patch("instruments_service.reference_data.base_adapter.log_event"),
            pytest.raises(RuntimeError, match="Schema validation failed"),
        ):
            adapter._parse_raw({"val": 1}, BadSchema)

    def test_timeout_error_raises_runtime(self) -> None:
        from pydantic import BaseModel

        class BadSchema(BaseModel):
            val: int

            def __init__(self, **data: object) -> None:
                raise TimeoutError("simulated")

        adapter = _TestAdapter()
        with (
            patch("instruments_service.reference_data.base_adapter.log_event"),
            pytest.raises(RuntimeError, match="Schema validation failed"),
        ):
            adapter._parse_raw({"val": 1}, BadSchema)
