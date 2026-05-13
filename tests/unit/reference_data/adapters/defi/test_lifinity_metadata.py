"""Unit tests — Lifinity Protocol reference-data adapter (Solana PMM).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_pool_record helper and by mocking
_fetch_pools via monkeypatch.

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 4.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.lifinity import (
    LifinityReferenceDataAdapter,
    _classify_lifinity_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_SOL_USDC_POOL_PARAMS: dict[str, object] = {
    "pool_address": "8cjtn4GEw6eVhZ9r1YatfiU65aDEBf1Fof5sTuuH6yVM",
    "name": "SOL-USDC",
    "token_0": {"symbol": "SOL", "name": "Solana"},
    "token_1": {"symbol": "USDC", "name": "USD Coin"},
    "tickSize": "0.01",
}

_MSOL_USDC_POOL: dict[str, object] = {
    "pool_address": "5UqpnLr5HV8dQFEBNtjxgkKTMqhFbQBEzrFBqBbTkQ28",
    "name": "mSOL-USDC",
    "token_0": {"symbol": "MSOL"},
    "token_1": {"symbol": "USDC"},
}

_SLASH_NAME_POOL: dict[str, object] = {
    "pool_address": "3yL2HkiXbq9YNQgfX3d6YeUWFqb4yHiJBFYM8LHG2ZNg",
    "name": "JUP/SOL",
}

_DASH_NAME_POOL: dict[str, object] = {
    "pool_address": "9Pg3oi3xX7vwF4JSmkEWqX2gJ2EHBJFxvGPJYFzYwbkm",
    "name": "BONK-USDC",
}

_EMPTY_POOL: dict[str, object] = {}

_EXPECTED_DEPLOY_DATE = datetime(2022, 3, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = LifinityReferenceDataAdapter()
    assert adapter.venue == "LIFINITY-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = LifinityReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "LIFINITY-DEVNET"


def test_build_pool_record_sol_usdc_params() -> None:
    adapter = LifinityReferenceDataAdapter()
    record = adapter._build_pool_record(_SOL_USDC_POOL_PARAMS)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "LIFINITY-SOLANA"
    assert record.instrument_key == "LIFINITY-SOLANA:SPOT:SOL-USDC"
    assert record.raw_symbol == "8cjtn4GEw6eVhZ9r1YatfiU65aDEBf1Fof5sTuuH6yVM"
    assert record.instrument_type == InstrumentType.SPOT
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.01")


def test_build_pool_record_msol_usdc() -> None:
    adapter = LifinityReferenceDataAdapter()
    record = adapter._build_pool_record(_MSOL_USDC_POOL)
    assert record is not None
    assert record.base_asset == "MSOL"
    assert record.quote_asset == "USDC"


def test_build_pool_record_slash_name_fallback() -> None:
    adapter = LifinityReferenceDataAdapter()
    record = adapter._build_pool_record(_SLASH_NAME_POOL)
    assert record is not None
    assert record.base_asset == "JUP"
    assert record.quote_asset == "SOL"


def test_build_pool_record_dash_name_fallback() -> None:
    adapter = LifinityReferenceDataAdapter()
    record = adapter._build_pool_record(_DASH_NAME_POOL)
    assert record is not None
    assert record.base_asset == "BONK"
    assert record.quote_asset == "USDC"


def test_build_pool_record_empty_returns_none() -> None:
    adapter = LifinityReferenceDataAdapter()
    assert adapter._build_pool_record(_EMPTY_POOL) is None


@pytest.mark.asyncio
async def test_get_instruments_only_returns_spot() -> None:
    adapter = LifinityReferenceDataAdapter()
    with patch.object(
        adapter, "_fetch_pools", new_callable=AsyncMock, return_value=[_SOL_USDC_POOL_PARAMS, _MSOL_USDC_POOL]
    ):
        # SPOT type returns results
        results = await adapter.get_instruments(instrument_type="spot")
        assert len(results) == 2

        # PERPETUAL type returns empty
        results_perp = await adapter.get_instruments(instrument_type="perpetual")
        assert len(results_perp) == 0


@pytest.mark.asyncio
async def test_get_instruments_returns_all_active() -> None:
    adapter = LifinityReferenceDataAdapter()
    with patch.object(
        adapter,
        "_fetch_pools",
        new_callable=AsyncMock,
        return_value=[_SOL_USDC_POOL_PARAMS, _MSOL_USDC_POOL, _DASH_NAME_POOL],
    ):
        results = await adapter.get_instruments()
        assert len(results) == 3
        assert all(r.instrument_type == InstrumentType.SPOT for r in results)


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = LifinityReferenceDataAdapter()
    with patch.object(adapter, "_fetch_pools", new_callable=AsyncMock, return_value=[_SOL_USDC_POOL_PARAMS]):
        record = await adapter.get_instrument("SOL")
        assert record is not None
        assert record.base_asset == "SOL"

        missing = await adapter.get_instrument("NONEXISTENT")
        assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_lifinity_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_lifinity_error(Exception("rate limit exceeded")) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_lifinity_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_lifinity_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_lifinity_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = LifinityReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL-USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL-USDC")
