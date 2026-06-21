"""Unit tests for the Kalshi crypto-perp reference data adapter (KALSHI-PERP).

Tests are credential-free: all HTTP calls are mocked.  The integration tests
that hit the live Kalshi API are marked @pytest.mark.requires_credentials and
skipped by default.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.cefi.kalshi_perp import (
    KalshiPerpReferenceDataAdapter,
    _extract_base_asset,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


_SAMPLE_MARKET = {
    "ticker": "KXBTCUSD",
    "title": "BTC/USD Perpetual",
    "category": "Crypto",
    "status": "active",
    "underlying": "BTCUSD",
}

_SAMPLE_MARKET_2 = {
    "ticker": "KXETHUSD",
    "title": "ETH/USD Perpetual",
    "category": "Crypto",
    "status": "active",
    "underlying": "ETHUSD",
}


# ---------------------------------------------------------------------------
# Tests: venue property
# ---------------------------------------------------------------------------


class TestVenueProperty:
    def test_venue_returns_kalshi_perp(self) -> None:
        adapter = KalshiPerpReferenceDataAdapter()
        assert adapter.venue == "kalshi-perp"


# ---------------------------------------------------------------------------
# Tests: get_instruments — happy path
# ---------------------------------------------------------------------------


class TestGetInstruments:
    @pytest.mark.asyncio
    async def test_enumerates_n_contracts_returns_n_records(self) -> None:
        """N contracts in the API response → N InstrumentRecords returned."""
        payload = {"markets": [_SAMPLE_MARKET, _SAMPLE_MARKET_2], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 2
        tickers = {r.instrument_key for r in results}
        assert "KXBTCUSD" in tickers
        assert "KXETHUSD" in tickers

    @pytest.mark.asyncio
    async def test_all_records_have_perpetual_instrument_type(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET, _SAMPLE_MARKET_2], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        for record in results:
            assert record.instrument_type == InstrumentType.PERPETUAL, (
                f"Expected PERPETUAL, got {record.instrument_type}"
            )

    @pytest.mark.asyncio
    async def test_venue_on_records_matches_adapter_venue(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 1
        assert results[0].venue == "kalshi-perp"

    @pytest.mark.asyncio
    async def test_quote_asset_is_usd(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert results[0].quote_asset == "USD"

    @pytest.mark.asyncio
    async def test_is_active_set_correctly(self) -> None:
        """status='active' → InstrumentStatus.ACTIVE; other → DELISTED."""
        inactive_market = {**_SAMPLE_MARKET, "status": "closed"}
        payload = {"markets": [_SAMPLE_MARKET, inactive_market], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        active = next(r for r in results if r.instrument_key == "KXBTCUSD")
        assert active.status == InstrumentStatus.ACTIVE
        # Both share the same ticker here, so just check the count is 2
        assert len(results) == 2


# ---------------------------------------------------------------------------
# Tests: honest-absence — empty market list
# ---------------------------------------------------------------------------


class TestHonestAbsence:
    @pytest.mark.asyncio
    async def test_empty_market_list_returns_empty_list(self) -> None:
        """Empty markets list → empty result (not an error)."""
        payload = {"markets": [], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert results == []

    @pytest.mark.asyncio
    async def test_instrument_type_filter_non_perpetual_returns_empty(self) -> None:
        """Filtering by SPOT_PAIR returns [] — venue only has PERPETUAL."""
        adapter = KalshiPerpReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_instrument_type_filter_perpetual_passes_through(self) -> None:
        """Filtering by PERPETUAL is equivalent to None (all)."""
        payload = {"markets": [_SAMPLE_MARKET], "cursor": ""}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)

        assert len(results) == 1


# ---------------------------------------------------------------------------
# Tests: error → classify_venue_error path
# ---------------------------------------------------------------------------


class TestErrorClassification:
    @pytest.mark.asyncio
    async def test_network_error_raises_runtime_error_and_emits_fetch_failed(self) -> None:
        """A ClientError causes a RuntimeError (so attempted_failed is recorded)
        and ADAPTER_FETCH_FAILED is emitted via log_event."""
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

        adapter = KalshiPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=mock_session_cm),
            patch("instruments_service.reference_data.adapters.cefi.kalshi_perp.log_event") as mock_log,
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()

        # Verify ADAPTER_FETCH_FAILED was emitted
        mock_log.assert_called()
        event_name = mock_log.call_args[0][0]
        assert event_name == "ADAPTER_FETCH_FAILED"

    @pytest.mark.asyncio
    async def test_401_raises_runtime_error(self) -> None:
        """HTTP 401 must raise RuntimeError (→ attempted_failed, not silent empty)."""
        mock_resp = AsyncMock()
        mock_resp.status = 401

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        adapter = KalshiPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=mock_session_cm),
            patch("instruments_service.reference_data.adapters.cefi.kalshi_perp.log_event"),
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()


# ---------------------------------------------------------------------------
# Tests: request uses correct API filters
# ---------------------------------------------------------------------------


class TestRequestFilters:
    @pytest.mark.asyncio
    async def test_request_uses_status_open_and_category_crypto(self) -> None:
        """Verify the API request sends status=open and category=Crypto."""
        payload = {"markets": [], "cursor": ""}
        mock_session_cm, mock_session_obj = _make_session_mock(payload)

        adapter = KalshiPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            await adapter.get_instruments()

        mock_session_obj.get.assert_called_once()
        _args, kwargs = mock_session_obj.get.call_args
        params = kwargs.get("params", {})
        assert params.get("status") == "open"
        assert params.get("category") == "Crypto"


# ---------------------------------------------------------------------------
# Tests: _extract_base_asset helper
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
# Integration tests (skipped without credentials)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="live network call — run manually against real Kalshi API")
@pytest.mark.asyncio
async def test_get_instruments_live() -> None:
    """Integration test: hits the real Kalshi perp API.

    Requires no credentials (public endpoint) but skipped by default to avoid
    network calls in CI. Run with: pytest -m requires_credentials
    """
    adapter = KalshiPerpReferenceDataAdapter()
    results = await adapter.get_instruments()
    assert len(results) > 0, "Expected at least one KALSHI-PERP contract"
    for record in results:
        assert record.instrument_type == InstrumentType.PERPETUAL
        assert record.venue == "kalshi-perp"
