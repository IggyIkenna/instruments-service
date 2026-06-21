"""Unit tests for the Polymarket crypto-perp reference data adapter (POLYMARKET-PERP).

Tests are credential-free: all HTTP calls are mocked.  The integration tests
that hit the live Polymarket perp API are marked @pytest.mark.requires_credentials
and skipped by default.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.cefi.polymarket_perp import (
    PolymarketPerpReferenceDataAdapter,
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
    "market_id": "BTC-USD",
    "title": "BTC/USD Perpetual",
    "base_asset": "BTC",
    "quote_asset": "USD",
    "status": "active",
}

_SAMPLE_MARKET_2 = {
    "market_id": "ETH-USD",
    "title": "ETH/USD Perpetual",
    "base_asset": "ETH",
    "quote_asset": "USD",
    "status": "active",
}


# ---------------------------------------------------------------------------
# Tests: venue property
# ---------------------------------------------------------------------------


class TestVenueProperty:
    def test_venue_returns_polymarket_perp(self) -> None:
        adapter = PolymarketPerpReferenceDataAdapter()
        assert adapter.venue == "polymarket-perp"


# ---------------------------------------------------------------------------
# Tests: get_instruments — happy path
# ---------------------------------------------------------------------------


class TestGetInstruments:
    @pytest.mark.asyncio
    async def test_enumerates_n_contracts_returns_n_records(self) -> None:
        """N contracts in the API response → N InstrumentRecords returned."""
        # Beta API returns a list-of-markets under "markets" key.
        payload = {"markets": [_SAMPLE_MARKET, _SAMPLE_MARKET_2]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 2
        ids = {r.instrument_key for r in results}
        assert "BTC-USD" in ids
        assert "ETH-USD" in ids

    @pytest.mark.asyncio
    async def test_all_records_have_perpetual_instrument_type(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET, _SAMPLE_MARKET_2]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        for record in results:
            assert record.instrument_type == InstrumentType.PERPETUAL, (
                f"Expected PERPETUAL, got {record.instrument_type}"
            )

    @pytest.mark.asyncio
    async def test_venue_on_records_matches_adapter_venue(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 1
        assert results[0].venue == "polymarket-perp"

    @pytest.mark.asyncio
    async def test_base_and_quote_assets_extracted_from_fields(self) -> None:
        payload = {"markets": [_SAMPLE_MARKET]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        record = results[0]
        assert record.base_asset == "BTC"
        assert record.quote_asset == "USD"

    @pytest.mark.asyncio
    async def test_base_asset_falls_back_to_market_id_parsing(self) -> None:
        """When base_asset field is absent, parse from market_id."""
        market_no_base = {
            "market_id": "SOL-USD",
            "title": "SOL/USD Perpetual",
            "status": "active",
        }
        payload = {"markets": [market_no_base]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 1
        assert results[0].base_asset == "SOL"

    @pytest.mark.asyncio
    async def test_accepts_data_key_in_response(self) -> None:
        """Beta API may return markets under 'data' key instead of 'markets'."""
        payload = {"data": [_SAMPLE_MARKET]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_accepts_direct_list_response(self) -> None:
        """Beta API may return a raw list of markets (no wrapper key)."""
        payload = [_SAMPLE_MARKET, _SAMPLE_MARKET_2]
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_is_active_correctly_set(self) -> None:
        inactive = {**_SAMPLE_MARKET_2, "status": "closed"}
        payload = {"markets": [_SAMPLE_MARKET, inactive]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert len(results) == 2
        btc = next(r for r in results if r.instrument_key == "BTC-USD")
        eth = next(r for r in results if r.instrument_key == "ETH-USD")
        assert btc.status == InstrumentStatus.ACTIVE
        assert eth.status == InstrumentStatus.DELISTED


# ---------------------------------------------------------------------------
# Tests: honest-absence — empty market list
# ---------------------------------------------------------------------------


class TestHonestAbsence:
    @pytest.mark.asyncio
    async def test_empty_market_list_returns_empty_list(self) -> None:
        """Empty markets list → empty result (not an error)."""
        payload = {"markets": []}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        assert results == []

    @pytest.mark.asyncio
    async def test_instrument_type_filter_non_perpetual_returns_empty(self) -> None:
        """Filtering by SPOT_PAIR returns [] — venue only has PERPETUAL."""
        adapter = PolymarketPerpReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_instrument_type_filter_perpetual_passes_through(self) -> None:
        """Filtering by PERPETUAL is equivalent to None (all)."""
        payload = {"markets": [_SAMPLE_MARKET]}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)

        assert len(results) == 1


# ---------------------------------------------------------------------------
# Tests: error → classify_venue_error path
# ---------------------------------------------------------------------------


class TestErrorClassification:
    @pytest.mark.asyncio
    async def test_network_error_raises_runtime_error_and_emits_fetch_failed(self) -> None:
        """A ClientError causes a RuntimeError and ADAPTER_FETCH_FAILED is emitted."""
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

        adapter = PolymarketPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=mock_session_cm),
            patch("instruments_service.reference_data.adapters.cefi.polymarket_perp.log_event") as mock_log,
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()

        mock_log.assert_called()
        event_name = mock_log.call_args[0][0]
        assert event_name == "ADAPTER_FETCH_FAILED"

    @pytest.mark.asyncio
    async def test_all_pages_fail_raises_runtime_error(self) -> None:
        """All-pages-failed with 0 records → raises RuntimeError (not silent empty)."""
        import aiohttp

        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock(side_effect=aiohttp.ClientError("connection reset"))
        mock_resp.status = 500

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        adapter = PolymarketPerpReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=mock_session_cm),
            patch("instruments_service.reference_data.adapters.cefi.polymarket_perp.log_event"),
            pytest.raises(RuntimeError, match="recording attempted_failed"),
        ):
            await adapter.get_instruments()


# ---------------------------------------------------------------------------
# Tests: get_instrument (single contract)
# ---------------------------------------------------------------------------


class TestGetInstrument:
    @pytest.mark.asyncio
    async def test_returns_none_on_404(self) -> None:
        mock_resp = AsyncMock()
        mock_resp.status = 404

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            result = await adapter.get_instrument("UNKNOWN-MARKET")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_record_for_known_market(self) -> None:
        payload = {"market": _SAMPLE_MARKET}
        mock_session_cm, _ = _make_session_mock(payload)

        adapter = PolymarketPerpReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=mock_session_cm):
            result = await adapter.get_instrument("BTC-USD")

        assert result is not None
        assert result.instrument_key == "BTC-USD"
        assert result.instrument_type == InstrumentType.PERPETUAL


# ---------------------------------------------------------------------------
# Tests: _extract_base_asset helper
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
# Integration tests (skipped without credentials)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="live network call — run manually against real Polymarket perp API")
@pytest.mark.asyncio
async def test_get_instruments_live() -> None:
    """Integration test: hits the real Polymarket perp API.

    The beta endpoint is public-read so no credential is technically required;
    marked requires_credentials anyway to avoid CI network calls and because
    the beta endpoint schema is still being confirmed.

    Run with: pytest -m requires_credentials
    """
    adapter = PolymarketPerpReferenceDataAdapter()
    results = await adapter.get_instruments()
    # Beta may return 0 contracts if the endpoint is down or unreachable.
    # Just verify shape when we get records back.
    for record in results:
        assert record.instrument_type == InstrumentType.PERPETUAL
        assert record.venue == "polymarket-perp"
