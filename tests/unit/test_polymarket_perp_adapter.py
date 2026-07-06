"""Unit tests for the Polymarket crypto-perp reference data adapter (POLYMARKET-PERP).

Enumeration is DISABLED pending the Phase-3 repoint against Polymarket's real
perps API (``_REPOINT_PENDING``; plan
``prediction_capture_incident_remediation_2026_07_06.md`` Workstream B). These
tests encode that contract: the adapter emits 0 records and makes no network call
while disabled. The parser/helper tests remain so Phase 3 has a baseline. All
HTTP is mocked; the live integration test is skipped.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.cefi.polymarket_perp import (
    PolymarketPerpReferenceDataAdapter,
    _classify_polymarket_perp_error,
    _extract_base_asset,
)

_SAMPLE_MARKET: dict[str, object] = {
    "market_id": "BTC-USD",
    "title": "BTC/USD Perpetual",
    "base_asset": "BTC",
    "quote_asset": "USD",
    "status": "active",
}


def _make_session_mock(json_payload: object) -> tuple[MagicMock, MagicMock]:
    """Return (mock_session_cm, mock_session_obj) that yield the given JSON."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_payload)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm, mock_session_obj


# ---------------------------------------------------------------------------
# Tests: venue property
# ---------------------------------------------------------------------------


class TestVenueProperty:
    def test_venue_returns_polymarket_perp(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        assert adapter.venue == "POLYMARKET-PERP"


# ---------------------------------------------------------------------------
# Tests: repoint-pending guard — DISABLED, emits 0, no network
# ---------------------------------------------------------------------------


class TestRepointPendingGuard:
    @pytest.mark.asyncio
    async def test_get_instruments_returns_empty_and_makes_no_network_call(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session") as mock_make_session:
            results = await adapter.get_instruments()

        assert results == []
        mock_make_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_instruments_empty_even_with_payload(self) -> None:
        """Even fed a plausible perps payload, the guard short-circuits to []."""
        payload = {"markets": [_SAMPLE_MARKET]}
        mock_session_cm, mock_session_obj = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert results == []
        mock_session_obj.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_no_network(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session") as mock_make_session:
            result = await adapter.get_instrument("BTC-USD")

        assert result is None
        mock_make_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_instrument_type_filter_non_perpetual_returns_empty(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []


# ---------------------------------------------------------------------------
# Tests: _parse_market baseline (parser retained for Phase 3 repoint)
# ---------------------------------------------------------------------------


class TestParseMarket:
    def test_parses_sample_market_to_perpetual_record(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        record = adapter._parse_market(_SAMPLE_MARKET, datetime.now(UTC))
        assert record is not None
        assert record.instrument_key == "BTC-USD"
        assert record.instrument_type == InstrumentType.PERPETUAL
        assert record.venue == "POLYMARKET-PERP"
        assert record.base_asset == "BTC"
        assert record.status == InstrumentStatus.ACTIVE

    def test_missing_id_parses_to_none(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        assert adapter._parse_market({"title": "no id"}, datetime.now(UTC)) is None


# ---------------------------------------------------------------------------
# Tests: error classification (pure fn — reused by the Phase-3 repoint path)
# ---------------------------------------------------------------------------


class TestErrorClassification:
    def test_status_429_maps_to_rate_limit(self) -> None:
        assert _classify_polymarket_perp_error(Exception("boom"), status=429) == "429"

    def test_status_5xx_maps_to_500(self) -> None:
        assert _classify_polymarket_perp_error(Exception("boom"), status=502) == "500"

    def test_message_pattern_without_status(self) -> None:
        assert _classify_polymarket_perp_error(Exception("403 forbidden")) == "403"

    def test_unknown_error(self) -> None:
        assert _classify_polymarket_perp_error(Exception("weird")) == "UNKNOWN"


# ---------------------------------------------------------------------------
# Tests: _extract_base_asset helper (unchanged; reused by Phase 3)
# ---------------------------------------------------------------------------


class TestExtractBaseAsset:
    def test_dash_separated(self) -> None:
        assert _extract_base_asset("BTC-USD") == "BTC"

    def test_slash_separated(self) -> None:
        assert _extract_base_asset("ETH/USD") == "ETH"

    def test_usd_suffix_stripped(self) -> None:
        assert _extract_base_asset("SOLUSDC") == "SOL"

    def test_bare_symbol(self) -> None:
        assert _extract_base_asset("AVAX") == "AVAX"

    def test_perp_suffix(self) -> None:
        assert _extract_base_asset("DOGEUSD") == "DOGE"


# ---------------------------------------------------------------------------
# Tests: fetch/error path with the guard LIFTED — coverage of the reusable HTTP
# machinery (session, response-shape branches, error classification, single
# lookup) that the Phase-3 repoint builds on. Unit test of the plumbing.
# ---------------------------------------------------------------------------


_POLY_MODULE = "instruments_service.reference_data.adapters.cefi.polymarket_perp"


def _client_error_session() -> MagicMock:
    import aiohttp

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock(side_effect=aiohttp.ClientError("timeout"))

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


def _status_only_session(status: int) -> MagicMock:
    mock_resp = AsyncMock()
    mock_resp.status = status

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


class TestFetchPathWhenEnabled:
    @pytest.mark.asyncio
    async def test_enumerates_markets_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        eth: dict[str, object] = {**_SAMPLE_MARKET, "market_id": "ETH-USD", "base_asset": "ETH"}
        payload = {"markets": [_SAMPLE_MARKET, eth]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert {r.instrument_key for r in results} == {"BTC-USD", "ETH-USD"}
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)

    @pytest.mark.asyncio
    async def test_accepts_data_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        payload = {"data": [_SAMPLE_MARKET]}
        mock_session_cm, _ = _make_session_mock(payload)
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_accepts_direct_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        payload = [_SAMPLE_MARKET]
        mock_session_cm, _ = _make_session_mock(payload)
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_network_error_raises_and_emits_fetch_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        adapter = PolymarketPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_client_error_session()),
            patch(f"{_POLY_MODULE}.log_event") as mock_log,
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()
        mock_log.assert_called()
        assert mock_log.call_args[0][0] == "ADAPTER_FETCH_FAILED"

    @pytest.mark.asyncio
    async def test_get_instrument_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        payload = {"market": _SAMPLE_MARKET}
        mock_session_cm, _ = _make_session_mock(payload)
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            result = await adapter.get_instrument("BTC-USD")
        assert result is not None
        assert result.instrument_key == "BTC-USD"

    @pytest.mark.asyncio
    async def test_get_instrument_404_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_POLY_MODULE}._REPOINT_PENDING", False)
        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_status_only_session(404)):
            result = await adapter.get_instrument("UNKNOWN-MARKET")
        assert result is None


# ---------------------------------------------------------------------------
# Integration test (skipped — re-enable in Phase 3 against the real perps API)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="adapter disabled pending Phase-3 repoint — re-enable against confirmed Polymarket perps API")
@pytest.mark.asyncio
async def test_get_instruments_live_perps_api() -> None:
    """Phase 3: hits the real Polymarket perps API once the endpoint is confirmed."""
    adapter = PolymarketPerpReferenceDataAdapter()
    results = await adapter.get_instruments()
    for record in results:
        assert record.instrument_type == InstrumentType.PERPETUAL
        assert record.venue == "POLYMARKET-PERP"
