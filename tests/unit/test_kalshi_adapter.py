"""Unit tests for the Kalshi prediction market adapter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.prediction.kalshi import KalshiReferenceDataAdapter


class TestKalshiAdapter:
    def test_venue_name(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        assert adapter.venue == "KALSHI"

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTC-26MAR-90000",
                        "event_ticker": "KXBTC",
                        "title": "BTC above $90,000?",
                        "subtitle": "",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 65,
                        "yes_ask": 68,
                        "no_bid": 32,
                        "no_ask": 35,
                        "open_time": "2026-03-25T00:00:00Z",
                        "close_time": "2026-03-26T00:00:00Z",
                        "expiration_time": "2026-03-26T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) >= 1
        assert results[0].venue == "KALSHI"

    @pytest.mark.asyncio
    async def test_get_instruments_uses_open_status_filter_not_active(self) -> None:
        """R5-fix-4 regression: the markets request MUST filter ``status=open``,
        never ``status=active``.

        Kalshi's ``status`` query param is a lifecycle filter whose valid values
        are ``unopened``/``open``/``closed``/``settled`` — ``status=active`` is
        rejected with HTTP 400 ``"invalid status filter"`` (verified live
        2026-06-16). The per-market ``status`` field for tradeable markets is
        ``"active"``, so ``open`` is the correct REQUEST filter that returns
        those active markets.
        """
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"markets": [], "cursor": ""})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            await adapter.get_instruments()

        mock_session_obj.get.assert_called_once()
        _args, kwargs = mock_session_obj.get.call_args
        params = kwargs.get("params", {})
        assert params.get("status") == "open"
        assert params.get("status") != "active"

    @pytest.mark.asyncio
    async def test_get_instruments_401_raises_not_swallowed(self) -> None:
        """CF-11 regression: a 401 on the first page must RAISE (→ attempted_failed),
        never return [] (which urdi_reference_provider would record as a silent
        honest-empty / drop from the expected denominator)."""
        adapter = KalshiReferenceDataAdapter()
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
        with patch("aiohttp.ClientSession", return_value=mock_session_cm), pytest.raises(RuntimeError):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_transport_error_raises_not_swallowed(self) -> None:
        """CF-11 regression: an aiohttp transport error on the first page must RAISE
        (as a RuntimeError caught by urdi _fetch_one → attempted_failed), not return []."""
        import aiohttp

        adapter = KalshiReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection reset"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm), pytest.raises(RuntimeError):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "market": {
                    "ticker": "KXBTC-26MAR-90000",
                    "event_ticker": "KXBTC",
                    "title": "BTC above $90,000?",
                    "category": "Crypto",
                    "status": "active",
                    "yes_bid": 65,
                    "yes_ask": 68,
                }
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("KXBTC-26MAR-90000")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = KalshiReferenceDataAdapter()
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
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("BTC")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("BTC")

    # -- date-aware live↔historical routing + RSA-PSS auth (2026-06-20) --

    def test_parse_kalshi_creds_rsa_blob(self) -> None:
        """RSA credential JSON blob → (api_key_id, private_key_pem); enables signing."""
        import json as _json

        blob = _json.dumps({"api_key_id": "kid-123", "private_key": "-----BEGIN RSA-----x"})
        adapter = KalshiReferenceDataAdapter(api_key=blob)
        assert adapter._kalshi_key_id == "kid-123"
        assert adapter._kalshi_private_key_pem == "-----BEGIN RSA-----x"
        assert adapter._can_sign is True

    def test_parse_kalshi_creds_none_and_legacy(self) -> None:
        """Missing / non-JSON credential → no signing (live unauthenticated is OK)."""
        assert KalshiReferenceDataAdapter()._can_sign is False
        assert KalshiReferenceDataAdapter(api_key="legacy-single-key")._can_sign is False

    def test_signed_headers_present_only_when_creds(self) -> None:
        """`_signed_headers` adds KALSHI-ACCESS-* only when RSA creds are present."""
        plain = KalshiReferenceDataAdapter()._signed_headers("GET", "/trade-api/v2/markets")
        assert "KALSHI-ACCESS-SIGNATURE" not in plain
        assert plain["Accept"] == "application/json"

    @pytest.mark.asyncio
    async def test_deep_date_is_honest_absence(self) -> None:
        """A date far before the cutoff returns [] (deep history = bulk seed) — it
        must NOT futile-paginate ``/historical/markets`` (only the cutoff lookup)."""
        adapter = KalshiReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"market_settled_ts": "2026-04-21T00:00:00Z"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instruments(date="2022-06-01")
        assert result == []
        # exactly one call — the /historical/cutoff lookup; no market pagination
        mock_session_obj.get.assert_called_once()
        assert "historical/cutoff" in mock_session_obj.get.call_args.args[0]
