"""Unit tests for the Kalshi crypto-perp reference data adapter (KALSHI-PERP).

Enumeration is DISABLED pending the margin-API repoint (``_REPOINT_PENDING``;
plan ``prediction_capture_incident_remediation_2026_07_06.md`` Workstream B).
These tests encode that contract: the adapter emits 0 records from the (wrong)
events host, makes no network call while disabled, and the ``_parse_market``
filter rejects the binary event universe (the root cause of the 25,473-row cefi
contamination). All HTTP is mocked; the live integration test is skipped.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.cefi.kalshi_perp import (
    KalshiPerpReferenceDataAdapter,
    _classify_kalshi_perp_error,
    _extract_base_asset,
)

# A realistic Kalshi *events*-host market: binary contract, category null (the
# host ignores the ?category=Crypto param), ticker in the KXMVE* event family
# that contaminated cefi. This is what the adapter used to emit as fake PERPETUAL.
_EVENTS_HOST_BINARY_MARKET: dict[str, object] = {
    "ticker": "KXMVESPORTSMULTIGAMEEXTENDED-26JUL05",
    "title": "Multi-game extended",
    "category": None,
    "status": "active",
    "market_type": "binary",
}

# A market that IS explicitly tagged crypto — used only to prove the parser still
# accepts genuine crypto rows when the adapter is re-enabled (Phase 2).
_CRYPTO_TAGGED_MARKET: dict[str, object] = {
    "ticker": "KXBTCUSD",
    "title": "BTC/USD Perpetual",
    "category": "Crypto",
    "status": "active",
    "underlying": "BTCUSD",
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
    def test_venue_returns_kalshi_perp(self) -> None:
        adapter = KalshiPerpReferenceDataAdapter()
        assert adapter.venue == "KALSHI-PERP"


# ---------------------------------------------------------------------------
# Tests: repoint-pending guard — the adapter is DISABLED, emits 0, no network
# ---------------------------------------------------------------------------


class TestRepointPendingGuard:
    @pytest.mark.asyncio
    async def test_get_instruments_returns_empty_and_makes_no_network_call(self) -> None:
        """DISABLED: get_instruments() returns [] BEFORE any session is opened."""
        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session") as mock_make_session:
            results = await adapter.get_instruments()

        assert results == []
        mock_make_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_instruments_empty_even_with_events_host_payload(self) -> None:
        """Fed a realistic events-host response (binary KXMVE* markets), the adapter
        yields 0 records — the guard short-circuits before the fetch, so the binary
        event universe can never be emitted as fake PERPETUAL."""
        payload = {"markets": [_EVENTS_HOST_BINARY_MARKET, _EVENTS_HOST_BINARY_MARKET], "cursor": ""}
        mock_session_cm, mock_session_obj = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert results == []
        mock_session_obj.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_instrument_returns_none_no_network(self) -> None:
        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session") as mock_make_session:
            result = await adapter.get_instrument("KXBTCUSD")

        assert result is None
        mock_make_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_instrument_type_filter_non_perpetual_returns_empty(self) -> None:
        """Filtering by a non-PERPETUAL type returns [] (checked before the guard)."""
        adapter = KalshiPerpReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []


# ---------------------------------------------------------------------------
# Tests: _parse_market filter — events-host binary rejected, crypto accepted
# ---------------------------------------------------------------------------


class TestParseMarketFilter:
    def test_events_host_binary_market_is_rejected(self) -> None:
        """An events-host binary contract (category=null) parses to None — the
        empty-category 'pass' that caused the 25,473-row contamination is fixed."""
        adapter = KalshiPerpReferenceDataAdapter()
        assert adapter._parse_market(_EVENTS_HOST_BINARY_MARKET, datetime.now(UTC)) is None

    def test_empty_string_category_is_rejected(self) -> None:
        adapter = KalshiPerpReferenceDataAdapter()
        market: dict[str, object] = {"ticker": "KXMVECROSSCATEGORY-X", "category": "", "status": "active"}
        assert adapter._parse_market(market, datetime.now(UTC)) is None

    def test_explicit_crypto_market_is_accepted(self) -> None:
        """A genuinely crypto-tagged market still parses to a PERPETUAL record, so
        the parser is correct when the adapter is re-enabled against the perps API."""
        adapter = KalshiPerpReferenceDataAdapter()
        record = adapter._parse_market(_CRYPTO_TAGGED_MARKET, datetime.now(UTC))
        assert record is not None
        assert record.instrument_type == InstrumentType.PERPETUAL
        assert record.venue == "KALSHI-PERP"
        assert record.status == InstrumentStatus.ACTIVE
        assert record.quote_asset == "USD"


# ---------------------------------------------------------------------------
# Tests: error classification (pure fn — reused by the Phase-2 margin-API path)
# ---------------------------------------------------------------------------


class TestErrorClassification:
    def test_status_429_maps_to_rate_limit(self) -> None:
        assert _classify_kalshi_perp_error(Exception("boom"), status=429) == "429"

    def test_status_5xx_maps_to_500(self) -> None:
        assert _classify_kalshi_perp_error(Exception("boom"), status=503) == "500"

    def test_message_pattern_without_status(self) -> None:
        assert _classify_kalshi_perp_error(Exception("401 unauthorized")) == "401"

    def test_unknown_error(self) -> None:
        assert _classify_kalshi_perp_error(Exception("something odd")) == "UNKNOWN"


# ---------------------------------------------------------------------------
# Tests: _extract_base_asset helper (unchanged; reused by Phase 2)
# ---------------------------------------------------------------------------


class TestExtractBaseAsset:
    def test_kx_prefix_stripped(self) -> None:
        assert _extract_base_asset("KXBTCUSD") == "BTC"

    def test_usd_suffix_stripped(self) -> None:
        assert _extract_base_asset("ETHUSD") == "ETH"

    def test_bare_symbol(self) -> None:
        assert _extract_base_asset("SOL") == "SOL"

    def test_usdt_suffix_stripped(self) -> None:
        assert _extract_base_asset("SOLUSDT") == "SOL"

    def test_perp_suffix_stripped(self) -> None:
        assert _extract_base_asset("BTCPERP") == "BTC"


# ---------------------------------------------------------------------------
# Tests: fetch/error path with the guard LIFTED — coverage of the reusable HTTP
# machinery (session, pagination, error classification, single-lookup) that the
# Phase-2 margin-API repoint builds on. Inputs are synthetic crypto-tagged
# markets; this is a unit test of the plumbing, NOT a claim about the events host.
# ---------------------------------------------------------------------------


_KALSHI_MODULE = "instruments_service.reference_data.adapters.cefi.kalshi_perp"


def _client_error_session() -> MagicMock:
    """A session whose response.raise_for_status raises an aiohttp.ClientError."""
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
    """A session returning a response with just a status code (no body read)."""
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
    async def test_enumerates_crypto_markets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_KALSHI_MODULE}._REPOINT_PENDING", False)
        eth: dict[str, object] = {**_CRYPTO_TAGGED_MARKET, "ticker": "KXETHUSD", "underlying": "ETHUSD"}
        payload = {"markets": [_CRYPTO_TAGGED_MARKET, eth], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 2
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)
        assert {r.instrument_key for r in results} == {"KXBTCUSD", "KXETHUSD"}

    @pytest.mark.asyncio
    async def test_network_error_raises_and_emits_fetch_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_KALSHI_MODULE}._REPOINT_PENDING", False)
        adapter = KalshiPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_client_error_session()),
            patch(f"{_KALSHI_MODULE}.log_event") as mock_log,
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()
        mock_log.assert_called()
        assert mock_log.call_args[0][0] == "ADAPTER_FETCH_FAILED"

    @pytest.mark.asyncio
    async def test_http_401_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_KALSHI_MODULE}._REPOINT_PENDING", False)
        adapter = KalshiPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_status_only_session(401)),
            patch(f"{_KALSHI_MODULE}.log_event"),
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instrument_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_KALSHI_MODULE}._REPOINT_PENDING", False)
        payload = {"market": _CRYPTO_TAGGED_MARKET}
        mock_session_cm, _ = _make_session_mock(payload)
        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            result = await adapter.get_instrument("KXBTCUSD")
        assert result is not None
        assert result.instrument_key == "KXBTCUSD"

    @pytest.mark.asyncio
    async def test_get_instrument_404_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(f"{_KALSHI_MODULE}._REPOINT_PENDING", False)
        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_status_only_session(404)):
            result = await adapter.get_instrument("NOPE")
        assert result is None


# ---------------------------------------------------------------------------
# Integration test (skipped — re-enable in Phase 2 against the margin API)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="adapter disabled pending margin-API repoint (Phase 2) — re-enable against demo host")
@pytest.mark.asyncio
async def test_get_instruments_live_margin_api() -> None:
    """Phase 2: hits the real Kalshi margin/perps API (demo external-api.demo.kalshi.co).

    Requires RSA-PSS auth + member-rollout enrollment. Skipped by default.
    Expected: genuine perpetuals (e.g. BTC-PERPETUAL), 0 KXMVE* event contracts.
    """
    adapter = KalshiPerpReferenceDataAdapter()
    results = await adapter.get_instruments()
    assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)
    assert all(not r.instrument_key.startswith("KXMVE") for r in results)
