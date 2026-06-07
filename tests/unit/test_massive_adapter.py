"""Unit + live tests for the Massive reference-data adapter.

Unit tests mock the HTTP layer (``_get_json``) so they are credential-free and
network-free. They verify venue filtering, canonical normalisation (via the UAC
normalisers), honest absence on fetch failure, and factory source-routing.

The ``live`` test hits the real Massive REST API and is skipped unless
``MASSIVE_API_KEY`` is available — it is the credentials-gated integration check.
"""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts.internal import InstrumentType

from instruments_service.reference_data.adapters.tradfi.databento import DatabentoReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.massive import (
    MASSIVE_SOURCE,
    MassiveReferenceDataAdapter,
    _classify_massive_error,
)
from instruments_service.reference_data.factory import (
    _ADAPTERS,
    ADAPTER_DATA_SOURCES,
    get_adapter_for_canonical_venue,
)

_TARGET = date(2026, 6, 5)

_EQUITY_PAGE = {
    "status": "OK",
    "results": [
        {"ticker": "AAPL", "name": "Apple Inc.", "primary_exchange": "XNAS", "type": "CS", "active": True},
        {"ticker": "SPY", "name": "SPDR S&P 500 ETF", "primary_exchange": "ARCX", "type": "ETF", "active": True},
        {"ticker": "NOTINUNIVERSE", "primary_exchange": "XNAS", "type": "CS", "active": True},
    ],
}
_FUTURES_PRODUCT_PAGE = {
    "status": "OK",
    "results": [
        {"product_code": "ES", "trading_venue": "XCME", "asset_sub_class": "equity", "unit_of_measure_qty": 50.0}
    ],
}
_FUTURES_CONTRACT_PAGE = {
    "status": "OK",
    # The adapter passes ``date=`` so Massive returns the point-in-time snapshot
    # (alive contracts only). This mock therefore contains alive contracts + a
    # duplicate (pagination-overlap safety net) + a calendar spread (excluded).
    "results": [
        {
            "ticker": "ESM6",
            "product_code": "ES",
            "trading_venue": "XCME",
            "first_trade_date": "2024-01-02",
            "last_trade_date": "2026-06-19",
        },
        {
            # Duplicate ticker → deduped to a single record.
            "ticker": "ESM6",
            "product_code": "ES",
            "trading_venue": "XCME",
            "first_trade_date": "2024-01-02",
            "last_trade_date": "2026-06-19",
        },
        {
            # Calendar spread (combo leg) → excluded (not an outright future).
            "ticker": "ESH7-ESM7",
            "product_code": "ES",
            "trading_venue": "XCME",
            "first_trade_date": "2024-01-02",
            "last_trade_date": "2027-03-19",
        },
    ],
}
_FX_PAGE = {
    "status": "OK",
    "results": [
        {"ticker": "C:KRWUSD", "market": "fx", "base_currency_symbol": "KRW", "currency_symbol": "USD"},
    ],
}
_INDEX_PAGE = {
    "status": "OK",
    "results": [{"ticker": "I:VIX", "name": "Cboe Volatility Index", "market": "indices"}],
}
_INDEX_OPTIONS_PAGE = {
    "status": "OK",
    "results": [
        {
            "ticker": "O:SPX260618C05000000",
            "underlying_ticker": "SPX",
            "contract_type": "call",
            "expiration_date": "2026-06-18",
            "strike_price": 5000,
            "shares_per_contract": 100,
            "primary_exchange": "XCBO",
        }
    ],
}


def _surface_router(*pages: dict[str, object]) -> AsyncMock:
    """Build an AsyncMock for ``_get_json`` returning a canned page per surface."""

    async def _fn(_session: object, _url: str, _headers: object, _params: object, surface: str):
        mapping: dict[str, dict[str, object]] = {
            "equities": _EQUITY_PAGE,
            "futures_products": _FUTURES_PRODUCT_PAGE,
            "futures_contracts": _FUTURES_CONTRACT_PAGE,
            "fx": _FX_PAGE,
            "indices": _INDEX_PAGE,
            "index_options": _INDEX_OPTIONS_PAGE,
        }
        return mapping.get(surface)

    mock = AsyncMock(side_effect=_fn)
    _ = pages
    return mock


class TestMassiveAdapterUnit:
    async def test_nasdaq_filters_to_curated_and_venue(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="NASDAQ", target_date=_TARGET)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", _surface_router()):
            recs = await adapter.get_instruments()
        keys = {r.instrument_key for r in recs}
        # AAPL (XNAS→NASDAQ, in curated universe) kept; SPY routed to NYSE; NOTINUNIVERSE dropped.
        assert "NASDAQ:EQUITY:AAPL" in keys
        assert all(r.venue == "NASDAQ" for r in recs)
        assert not any("NOTINUNIVERSE" in k for k in keys)

    async def test_nyse_gets_arca_etf(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="NYSE", target_date=_TARGET)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", _surface_router()):
            recs = await adapter.get_instruments()
        assert any(r.instrument_key == "NYSE:ETF:SPY" for r in recs)

    async def test_cme_futures_snapshot_dedup_and_spread_exclusion(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="CME", target_date=_TARGET)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", _surface_router()):
            recs = await adapter.get_instruments()
        keys = [r.instrument_key for r in recs]
        assert keys.count("CME:FUTURE:ESM6") == 1  # duplicate deduped
        assert "CME:FUTURE:ESH7-ESM7" not in keys  # calendar spread excluded
        es = next(r for r in recs if r.instrument_key == "CME:FUTURE:ESM6")
        assert es.venue == "CME"
        assert es.instrument_type == InstrumentType.FUTURE
        assert es.contract_size is not None and str(es.contract_size) == "50.0"

    async def test_fx_venue(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="FX", target_date=_TARGET)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", _surface_router()):
            recs = await adapter.get_instruments()
        assert any(r.instrument_key == "FX:SPOT_PAIR:KRW-USD" for r in recs)

    async def test_cboe_index_and_index_options(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="CBOE", target_date=_TARGET)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", _surface_router()):
            recs = await adapter.get_instruments()
        assert any(r.instrument_key == "CBOE:INDEX:VIX" for r in recs)
        # SPX index option present + tagged CBOE (OPRA XCBO MIC overridden to CBOE).
        opt = next((r for r in recs if r.instrument_key == "CBOE:OPTION:O:SPX260618C05000000"), None)
        assert opt is not None
        assert opt.venue == "CBOE"
        assert opt.instrument_type == InstrumentType.OPTION
        assert opt.underlying == "SPX"

    async def test_honest_absence_on_fetch_failure(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key="k", venue_filter="NASDAQ", target_date=_TARGET)
        failing = AsyncMock(return_value=None)
        with patch.object(MassiveReferenceDataAdapter, "_get_json", failing):
            recs = await adapter.get_instruments()
        assert recs == []  # no placeholder rows on failure

    async def test_missing_api_key_raises(self) -> None:
        adapter = MassiveReferenceDataAdapter(api_key=None, venue_filter="NASDAQ")
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    def test_error_classification(self) -> None:
        assert _classify_massive_error(RuntimeError("HTTP 429 rate")) == "RATE_LIMIT"
        assert _classify_massive_error(RuntimeError("HTTP 503")) == "SERVER_ERROR"
        assert _classify_massive_error(RuntimeError("401 Unauthorized")) == "AUTH_ERROR"

    def test_venue_property(self) -> None:
        assert MassiveReferenceDataAdapter(venue_filter="CME").venue == "CME"
        assert MassiveReferenceDataAdapter().venue == "massive"


class TestFactorySourceRouting:
    def test_massive_registered(self) -> None:
        assert _ADAPTERS["massive"] is MassiveReferenceDataAdapter
        assert ADAPTER_DATA_SOURCES["massive"] == MASSIVE_SOURCE

    def test_source_massive_routes_tradfi_to_massive(self) -> None:
        adapter = get_adapter_for_canonical_venue("CME", api_key="k", date="2026-06-05", source="massive")
        assert isinstance(adapter, MassiveReferenceDataAdapter)
        assert adapter.venue == "CME"

    def test_default_source_routes_to_databento(self) -> None:
        adapter = get_adapter_for_canonical_venue("CME", api_key="k", date="2026-06-05")
        assert isinstance(adapter, DatabentoReferenceDataAdapter)


@pytest.mark.live
class TestMassiveAdapterLive:
    async def test_live_equities_and_futures(self) -> None:
        """Hit the real Massive API — skipped unless MASSIVE_API_KEY is present."""
        key = os.environ.get("MASSIVE_API_KEY")
        if not key:
            pytest.skip("MASSIVE_API_KEY not set — live Massive integration test skipped")
        eq = MassiveReferenceDataAdapter(api_key=key, venue_filter="NASDAQ", target_date=date.today())
        eq_recs = await eq.get_instruments()
        assert any(r.venue == "NASDAQ" for r in eq_recs)
        fut = MassiveReferenceDataAdapter(api_key=key, venue_filter="CME", target_date=date.today())
        fut_recs = await fut.get_instruments()
        assert any(r.instrument_type == InstrumentType.FUTURE and r.venue == "CME" for r in fut_recs)
