"""Unit tests — Zeta Markets reference-data adapter (Solana perp DEX).

Tests are credential-free and offline — the REST fetch is exercised via
direct calls to the internal _build_perp_record helper and by mocking
_fetch_perp_markets via monkeypatch.

Plan: solana_perp_dex_adapters_2026_05_13 Phase 3.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.zeta import (
    ZetaReferenceDataAdapter,
    _classify_zeta_error,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

_SOL_PERP: dict[str, object] = {
    "symbol": "SOL-PERP",
    "asset": "SOL",
    "kind": "perp",
    "isActive": True,
    "tickSize": "0.0001",
    "minLotSize": "0.01",
}

_BTC_PERP: dict[str, object] = {
    "symbol": "BTC-PERP",
    "kind": "future",
    "isActive": True,
}

_INACTIVE_PERP: dict[str, object] = {
    "symbol": "ETH-PERP",
    "kind": "perp",
    "isActive": False,
}

_NON_PERP: dict[str, object] = {
    "symbol": "SOL-USDC",
    "kind": "spot",
    "isActive": True,
}

_EXPECTED_DEPLOY_DATE = datetime(2022, 4, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = ZetaReferenceDataAdapter()
    assert adapter.venue == "ZETA-SOLANA"


def test_build_perp_record_sol() -> None:
    adapter = ZetaReferenceDataAdapter()
    record = adapter._build_perp_record(_SOL_PERP)
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "ZETA-SOLANA"
    assert record.instrument_key == "ZETA-SOLANA:PERP:SOL"
    assert record.raw_symbol == "SOL-PERP"
    assert record.instrument_type == InstrumentType.PERPETUAL
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.0001")
    assert record.min_size == Decimal("0.01")


def test_build_perp_record_btc_minimal() -> None:
    adapter = ZetaReferenceDataAdapter()
    record = adapter._build_perp_record(_BTC_PERP)
    assert record is not None
    assert record.base_asset == "BTC"
    assert record.tick_size == Decimal("0.0001")
    assert record.min_size == Decimal("0.001")


def test_build_perp_record_inactive_returns_none() -> None:
    adapter = ZetaReferenceDataAdapter()
    assert adapter._build_perp_record(_INACTIVE_PERP) is None


def test_build_perp_record_non_perp_kind_returns_none() -> None:
    adapter = ZetaReferenceDataAdapter()
    assert adapter._build_perp_record(_NON_PERP) is None


def test_build_perp_record_empty_returns_none() -> None:
    adapter = ZetaReferenceDataAdapter()
    assert adapter._build_perp_record({}) is None


@pytest.mark.asyncio
async def test_get_instruments_filters_perps_only() -> None:
    adapter = ZetaReferenceDataAdapter()
    markets = [_SOL_PERP, _BTC_PERP, _INACTIVE_PERP, _NON_PERP]
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=markets):
        results = await adapter.get_instruments()
        assert len(results) == 2
        symbols = {r.base_asset for r in results}
        assert symbols == {"SOL", "BTC"}


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = ZetaReferenceDataAdapter()
    with patch.object(adapter, "_fetch_perp_markets", new_callable=AsyncMock, return_value=[_SOL_PERP]):
        by_raw = await adapter.get_instrument("SOL-PERP")
        assert by_raw is not None
        assert by_raw.base_asset == "SOL"

        by_base = await adapter.get_instrument("SOL")
        assert by_base is not None

        missing = await adapter.get_instrument("XYZ")
        assert missing is None


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_zeta_error(Exception("rate limit"), status=429) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_zeta_error(Exception("unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_zeta_error(Exception("server error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_zeta_error(Exception("timeout"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = ZetaReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL-PERP")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL-PERP")
