"""Unit tests — Jupiter Aggregator reference-data adapter (Solana DEX aggregator).

Tests are credential-free and offline — the pair building logic is exercised
via direct calls to _build_pair_record, and the core instrument list is
verified without network calls.

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 3.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.jupiter import (
    _CORE_ROUTABLE_PAIRS,
    JupiterReferenceDataAdapter,
    _classify_jupiter_error,
)

# ── Constants ─────────────────────────────────────────────────────────────────

_EXPECTED_DEPLOY_DATE = datetime(2021, 11, 1, tzinfo=UTC)


# ── Adapter unit tests ────────────────────────────────────────────────────────


def test_venue_name() -> None:
    adapter = JupiterReferenceDataAdapter()
    assert adapter.venue == "JUPITER-SOLANA"


def test_venue_custom_chain() -> None:
    adapter = JupiterReferenceDataAdapter(chain="devnet")
    assert adapter.venue == "JUPITER-DEVNET"


def test_build_pair_record_sol_usdc() -> None:
    adapter = JupiterReferenceDataAdapter()
    record = adapter._build_pair_record("SOL", "USDC")
    assert record is not None
    assert isinstance(record, InstrumentRecord)
    assert record.venue == "JUPITER-SOLANA"
    assert record.instrument_key == "JUPITER-SOLANA:SPOT:SOL-USDC"
    assert record.raw_symbol == "SOL/USDC"
    assert record.instrument_type == InstrumentType.SPOT_PAIR
    assert record.base_asset == "SOL"
    assert record.quote_asset == "USDC"
    assert record.settle_asset == "USDC"
    assert record.status == InstrumentStatus.ACTIVE
    assert record.available_from_datetime == _EXPECTED_DEPLOY_DATE
    assert record.tick_size == Decimal("0.000001")


def test_build_pair_record_jitosol_usdc() -> None:
    adapter = JupiterReferenceDataAdapter()
    record = adapter._build_pair_record("JITOSOL", "USDC")
    assert record is not None
    assert record.base_asset == "JITOSOL"
    assert record.quote_asset == "USDC"
    assert record.instrument_key == "JUPITER-SOLANA:SPOT:JITOSOL-USDC"


def test_build_pair_record_msol_sol() -> None:
    adapter = JupiterReferenceDataAdapter()
    record = adapter._build_pair_record("MSOL", "SOL")
    assert record is not None
    assert record.base_asset == "MSOL"
    assert record.quote_asset == "SOL"


def test_build_pair_record_empty_returns_none() -> None:
    adapter = JupiterReferenceDataAdapter()
    assert adapter._build_pair_record("", "USDC") is None
    assert adapter._build_pair_record("SOL", "") is None


def test_core_routable_pairs_not_empty() -> None:
    """Core pair list must have LST pairs for arbitrage_price_dispersion."""
    assert len(_CORE_ROUTABLE_PAIRS) >= 10
    pair_set = {f"{b}/{q}" for b, q in _CORE_ROUTABLE_PAIRS}
    # Strategy-critical pairs
    assert "SOL/USDC" in pair_set
    assert "JITOSOL/USDC" in pair_set
    assert "MSOL/USDC" in pair_set
    assert "JITOSOL/SOL" in pair_set
    assert "MSOL/SOL" in pair_set


@pytest.mark.asyncio
async def test_get_instruments_returns_core_pairs() -> None:
    adapter = JupiterReferenceDataAdapter()
    results = await adapter.get_instruments()
    assert len(results) == len(_CORE_ROUTABLE_PAIRS)
    assert all(r.instrument_type == InstrumentType.SPOT_PAIR for r in results)


@pytest.mark.asyncio
async def test_get_instruments_only_returns_spot() -> None:
    adapter = JupiterReferenceDataAdapter()
    # SPOT type returns results
    results = await adapter.get_instruments(instrument_type="spot")
    assert len(results) == len(_CORE_ROUTABLE_PAIRS)

    # PERPETUAL type returns empty
    results_perp = await adapter.get_instruments(instrument_type="perpetual")
    assert len(results_perp) == 0


@pytest.mark.asyncio
async def test_get_instrument_by_symbol() -> None:
    adapter = JupiterReferenceDataAdapter()
    record = await adapter.get_instrument("SOL")
    assert record is not None
    assert record.base_asset == "SOL"

    missing = await adapter.get_instrument("NONEXISTENT")
    assert missing is None


@pytest.mark.asyncio
async def test_fetch_token_list_mock() -> None:
    """Verify token list fetch parses a list of tokens."""
    adapter = JupiterReferenceDataAdapter()
    mock_tokens = [{"symbol": "SOL", "address": "So11111111111111111111111111111111111111112"}]
    with patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value=mock_tokens):
        tokens = await adapter._fetch_token_list()
        assert tokens == mock_tokens


# ── Error classification ──────────────────────────────────────────────────────


def test_classify_rate_limit() -> None:
    assert _classify_jupiter_error(Exception("429 Too Many Requests")) == "RATE_LIMIT"
    assert _classify_jupiter_error(Exception("rate limit exceeded")) == "RATE_LIMIT"


def test_classify_503() -> None:
    assert _classify_jupiter_error(Exception("Service Unavailable"), status=503) == "503"


def test_classify_500() -> None:
    assert _classify_jupiter_error(Exception("Internal Server Error"), status=500) == "500"


def test_classify_unknown() -> None:
    assert _classify_jupiter_error(Exception("Connection reset"), status=None) == "UNKNOWN"


# ── NotImplemented methods ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_methods_raise() -> None:
    adapter = JupiterReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("SOL/USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("SOL/USDC")
