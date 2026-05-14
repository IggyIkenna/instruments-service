"""Unit tests — Meteora Dynamic Liquidity reference-data adapter (Solana AMM).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_pool_record helper and by mocking
_fetch_pools via monkeypatch.

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 1.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.meteora import (
    MeteoraReferenceDataAdapter,
    _classify_meteora_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_SOL_USDC_POOL: dict[str, object] = {
    "pool_address": "2SF1Xba1ug7WNF3o7Xs4NNmVp4jNGWQcZnZLqxSbmfe",
    "pool_name": "SOL-USDC",
    "pool_type": "DLMM",
    "bin_step": 10,
    "current_price": "157.23",
    "liquidity": "1250000",
}

_JUP_USDC_POOL: dict[str, object] = {
    "pool_address": "C1MgLojNLWBKADvu9BHdtgzz1oZX4dZ5zGWe4wR7zd7h",
    "pool_name": "JUP-USDC",
    "pool_type": "DLMM",
    "bin_step": 20,
}

_BONK_SOL_POOL: dict[str, object] = {
    "pool_address": "5vGJqzGqb6nAnFXFWNMjA1TsZFbmSg5pKfvTiUMPxmfP",
    "pool_name": "BONK-SOL",
    "bin_step": 100,
}

_EMPTY_POOL: dict[str, object] = {}

_EXPECTED_DEPLOY_DATE = datetime(2022, 9, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = MeteoraReferenceDataAdapter()
    assert adapter.venue == "METEORA-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = MeteoraReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "METEORA-DEVNET"


def test_build_pool_record_sol_usdc() -> None:
    adapter = MeteoraReferenceDataAdapter()
    record = adapter._build_pool_record(_SOL_USDC_POOL)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "METEORA-SOLANA"
    assert record.instrument_key == "METEORA-SOLANA:SPOT:SOL-USDC"
    assert record.raw_symbol == "2SF1Xba1ug7WNF3o7Xs4NNmVp4jNGWQcZnZLqxSbmfe"
    assert record.instrument_type == InstrumentType.SPOT_PAIR
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    # bin_step=10 → tick_size=10/10000=0.001
    assert record.tick_size == Decimal("0.001")


def test_build_pool_record_jup_usdc() -> None:
    adapter = MeteoraReferenceDataAdapter()
    record = adapter._build_pool_record(_JUP_USDC_POOL)
    assert record is not None
    assert record.base_asset == "JUP"
    assert record.quote_asset == "USDC"
    # bin_step=20 → tick_size=0.002
    assert record.tick_size == Decimal("0.002")


def test_build_pool_record_bonk_sol() -> None:
    adapter = MeteoraReferenceDataAdapter()
    record = adapter._build_pool_record(_BONK_SOL_POOL)
    assert record is not None
    assert record.base_asset == "BONK"
    assert record.quote_asset == "SOL"


def test_build_pool_record_empty_returns_none() -> None:
    adapter = MeteoraReferenceDataAdapter()
    assert adapter._build_pool_record(_EMPTY_POOL) is None


def test_build_pool_record_name_only() -> None:
    adapter = MeteoraReferenceDataAdapter()
    pool = {"pool_name": "ORCA-USDC", "bin_step": 5}
    record = adapter._build_pool_record(pool)
    assert record is not None
    assert record.base_asset == "ORCA"
    # When no address, name is used as raw_symbol
    assert record.raw_symbol == "ORCA-USDC"


@pytest.mark.asyncio
async def test_get_instruments_only_returns_spot() -> None:
    adapter = MeteoraReferenceDataAdapter()
    with patch.object(adapter, "_fetch_pools", new_callable=AsyncMock, return_value=[_SOL_USDC_POOL, _JUP_USDC_POOL]):
        # SPOT type returns results
        results = await adapter.get_instruments(instrument_type="spot")
        assert len(results) == 2

        # PERPETUAL type returns empty
        results_perp = await adapter.get_instruments(instrument_type="perpetual")
        assert len(results_perp) == 0


@pytest.mark.asyncio
async def test_get_instruments_returns_all_active() -> None:
    adapter = MeteoraReferenceDataAdapter()
    with patch.object(
        adapter, "_fetch_pools", new_callable=AsyncMock, return_value=[_SOL_USDC_POOL, _JUP_USDC_POOL, _BONK_SOL_POOL]
    ):
        results = await adapter.get_instruments()
        assert len(results) == 3
        assert all(r.instrument_type == InstrumentType.SPOT_PAIR for r in results)


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = MeteoraReferenceDataAdapter()
    with patch.object(adapter, "_fetch_pools", new_callable=AsyncMock, return_value=[_SOL_USDC_POOL]):
        record = await adapter.get_instrument("SOL")
        assert record is not None
        assert record.base_asset == "SOL"

        missing = await adapter.get_instrument("NONEXISTENT")
        assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_meteora_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_meteora_error(Exception("rate limit exceeded")) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_meteora_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_meteora_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_meteora_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = MeteoraReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL-USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL-USDC")
