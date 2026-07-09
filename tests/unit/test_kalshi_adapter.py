"""Unit tests for the Kalshi prediction market adapter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.prediction.kalshi import KalshiReferenceDataAdapter


def _make_cm(resp: object) -> MagicMock:
    """Wrap a mock response in a context-manager mock (for ``async with session.get(...)`` usage)."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


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

        # The snapshot call is the first call to session.get; the series-scoped batch
        # makes additional calls.  Assert the snapshot call uses status=open.
        mock_session_obj.get.assert_called()
        first_call = mock_session_obj.get.call_args_list[0]
        _args, kwargs = first_call
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

    # -- series-scoped capture (KXMVE-flood fix, 2026-06-23) --

    @pytest.mark.asyncio
    async def test_series_scoped_capture_adds_kxbtcd_markets(self) -> None:
        """Series-scoped path fetches KXBTCD markets that the KXMVE-flood snapshot misses.

        /markets?status=open (snapshot) returns only KXMVE markets.
        /series?category=Crypto returns [KXBTCD, KXMVESPORTS].
        classify_kalshi_to_canonical_group: KXBTCD → non-OTHER, KXMVESPORTS → OTHER.
        /markets?status=open&series_ticker=KXBTCD returns one BTC-daily market.
        Expected: KXBTCD-26JUN23-T90000 is in the final output.
        """
        adapter = KalshiReferenceDataAdapter()

        snapshot_resp = AsyncMock()
        snapshot_resp.status = 200
        snapshot_resp.raise_for_status = MagicMock()
        snapshot_resp.json = AsyncMock(return_value={"markets": [], "cursor": ""})

        series_crypto_resp = AsyncMock()
        series_crypto_resp.status = 200
        series_crypto_resp.raise_for_status = MagicMock()
        series_crypto_resp.json = AsyncMock(return_value={"series": [{"ticker": "KXBTCD"}, {"ticker": "KXMVESPORTS"}]})

        series_empty_resp = AsyncMock()
        series_empty_resp.status = 200
        series_empty_resp.raise_for_status = MagicMock()
        series_empty_resp.json = AsyncMock(return_value={"series": []})

        btc_market_resp = AsyncMock()
        btc_market_resp.status = 200
        btc_market_resp.raise_for_status = MagicMock()
        btc_market_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTCD-26JUN23-T90000",
                        "event_ticker": "KXBTCD-26JUN23",
                        "series_ticker": "KXBTCD",
                        "title": "BTC daily above $90k?",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 60,
                        "yes_ask": 65,
                        "open_time": "2026-06-23T00:00:00Z",
                        "close_time": "2026-06-23T23:59:59Z",
                        "expiration_time": "2026-06-23T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )

        def get_side_effect(url: str, **kwargs: object) -> object:
            params = kwargs.get("params", {})
            if isinstance(params, dict) and params.get("series_ticker") == "KXBTCD":
                return _make_cm(btc_market_resp)
            if "/series" in url:
                cat = params.get("category") if isinstance(params, dict) else None
                if cat == "Crypto":
                    return _make_cm(series_crypto_resp)
                return _make_cm(series_empty_resp)
            return _make_cm(snapshot_resp)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=get_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()

        tickers = {r.instrument_key for r in results}
        assert "KALSHI:PREDICTION_MARKET:KXBTCD-26JUN23-T90000" in tickers, (
            f"Series-scoped capture must include KXBTCD markets; got: {tickers}"
        )

    @pytest.mark.asyncio
    async def test_series_scoped_per_series_failure_is_shard_isolated(self) -> None:
        """A 500 on one series' /markets fetch must NOT abort the batch.

        KXBTCD /markets 500s; KXETHD /markets succeeds.
        Expected: KXETHD market present, no RuntimeError raised.
        """
        import aiohttp as _aiohttp

        adapter = KalshiReferenceDataAdapter()

        snapshot_resp = AsyncMock()
        snapshot_resp.status = 200
        snapshot_resp.raise_for_status = MagicMock()
        snapshot_resp.json = AsyncMock(return_value={"markets": [], "cursor": ""})

        series_crypto_resp = AsyncMock()
        series_crypto_resp.status = 200
        series_crypto_resp.raise_for_status = MagicMock()
        series_crypto_resp.json = AsyncMock(return_value={"series": [{"ticker": "KXBTCD"}, {"ticker": "KXETHD"}]})

        series_empty_resp = AsyncMock()
        series_empty_resp.status = 200
        series_empty_resp.raise_for_status = MagicMock()
        series_empty_resp.json = AsyncMock(return_value={"series": []})

        btc_500_resp = AsyncMock()
        btc_500_resp.status = 500
        btc_500_resp.raise_for_status = MagicMock(side_effect=_aiohttp.ClientError("HTTP 500"))

        eth_market_resp = AsyncMock()
        eth_market_resp.status = 200
        eth_market_resp.raise_for_status = MagicMock()
        eth_market_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXETHD-26JUN23-T3000",
                        "event_ticker": "KXETHD-26JUN23",
                        "series_ticker": "KXETHD",
                        "title": "ETH daily above $3k?",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 40,
                        "yes_ask": 45,
                        "open_time": "2026-06-23T00:00:00Z",
                        "close_time": "2026-06-23T23:59:59Z",
                        "expiration_time": "2026-06-23T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )

        def get_side_effect(url: str, **kwargs: object) -> object:
            params = kwargs.get("params", {})
            if isinstance(params, dict) and params.get("series_ticker") == "KXBTCD":
                return _make_cm(btc_500_resp)
            if isinstance(params, dict) and params.get("series_ticker") == "KXETHD":
                return _make_cm(eth_market_resp)
            if "/series" in url:
                cat = params.get("category") if isinstance(params, dict) else None
                if cat == "Crypto":
                    return _make_cm(series_crypto_resp)
                return _make_cm(series_empty_resp)
            return _make_cm(snapshot_resp)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=get_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()  # must NOT raise

        tickers = {r.instrument_key for r in results}
        assert "KALSHI:PREDICTION_MARKET:KXETHD-26JUN23-T3000" in tickers, (
            f"KXETHD market must survive when KXBTCD fetch 500s; got: {tickers}"
        )

    @pytest.mark.asyncio
    async def test_series_scoped_429_is_retried_not_dropped(self) -> None:
        """A 429 on a series' /markets fetch is backed-off + retried, not shard-skipped.

        The ~40 rapid per-series fetches rate-limit; the page helper must retry the
        page (bounded exp backoff) so the series' markets survive a transient 429.
        KXBTCD /markets returns 429 once, then 200 — the BTC market must appear.
        """
        adapter = KalshiReferenceDataAdapter()

        snapshot_resp = AsyncMock()
        snapshot_resp.status = 200
        snapshot_resp.raise_for_status = MagicMock()
        snapshot_resp.json = AsyncMock(return_value={"markets": [], "cursor": ""})

        series_crypto_resp = AsyncMock()
        series_crypto_resp.status = 200
        series_crypto_resp.raise_for_status = MagicMock()
        series_crypto_resp.json = AsyncMock(return_value={"series": [{"ticker": "KXBTCD"}]})

        series_empty_resp = AsyncMock()
        series_empty_resp.status = 200
        series_empty_resp.raise_for_status = MagicMock()
        series_empty_resp.json = AsyncMock(return_value={"series": []})

        btc_429_resp = AsyncMock()
        btc_429_resp.status = 429
        btc_429_resp.raise_for_status = MagicMock()  # never reached — code checks status first

        btc_ok_resp = AsyncMock()
        btc_ok_resp.status = 200
        btc_ok_resp.raise_for_status = MagicMock()
        btc_ok_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTCD-26JUN23-T90000",
                        "event_ticker": "KXBTCD-26JUN23",
                        "series_ticker": "KXBTCD",
                        "title": "BTC daily above $90k?",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 60,
                        "yes_ask": 65,
                        "open_time": "2026-06-23T00:00:00Z",
                        "close_time": "2026-06-23T23:59:59Z",
                        "expiration_time": "2026-06-23T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )

        btc_calls = {"n": 0}

        def get_side_effect(url: str, **kwargs: object) -> object:
            params = kwargs.get("params", {})
            if isinstance(params, dict) and params.get("series_ticker") == "KXBTCD":
                btc_calls["n"] += 1
                return _make_cm(btc_429_resp if btc_calls["n"] == 1 else btc_ok_resp)
            if "/series" in url:
                cat = params.get("category") if isinstance(params, dict) else None
                return _make_cm(series_crypto_resp if cat == "Crypto" else series_empty_resp)
            return _make_cm(snapshot_resp)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=get_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm), patch("asyncio.sleep", AsyncMock()):
            results = await adapter.get_instruments()

        tickers = {r.instrument_key for r in results}
        assert btc_calls["n"] >= 2, "KXBTCD page must be retried after the 429"
        assert "KALSHI:PREDICTION_MARKET:KXBTCD-26JUN23-T90000" in tickers, (
            f"429 must be retried, not dropped; got: {tickers}"
        )

    @pytest.mark.asyncio
    async def test_series_scoped_skips_historical_path(self) -> None:
        """Series-scoped capture must NOT run for a dated (historical) request."""
        adapter = KalshiReferenceDataAdapter()

        cutoff_resp = AsyncMock()
        cutoff_resp.status = 200
        cutoff_resp.raise_for_status = MagicMock()
        cutoff_resp.json = AsyncMock(return_value={"market_settled_ts": "2026-06-20T00:00:00Z"})

        hist_resp = AsyncMock()
        hist_resp.status = 200
        hist_resp.raise_for_status = MagicMock()
        hist_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTCD-26JUN19-T90000",
                        "event_ticker": "KXBTCD-26JUN19",
                        "series_ticker": "KXBTCD",
                        "title": "BTC daily",
                        "category": "Crypto",
                        "status": "settled",
                        "yes_bid": 0,
                        "yes_ask": 0,
                        "open_time": "2026-06-19T00:00:00Z",
                        "close_time": "2026-06-19T23:59:59Z",
                        "expiration_time": "2026-06-19T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )

        series_called: list[str] = []

        def get_side_effect(url: str, **kwargs: object) -> object:
            if "/series" in url:
                series_called.append(url)
            if "historical/cutoff" in url:
                return _make_cm(cutoff_resp)
            return _make_cm(hist_resp)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=get_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            await adapter.get_instruments(date="2026-06-19")

        assert series_called == [], f"/series must NOT be called for a dated historical request; got: {series_called}"

    @pytest.mark.asyncio
    async def test_series_scoped_runs_for_current_day_batch_after_cutoff(self) -> None:
        """A dated batch enum on/after the cutoff is still the LIVE path → series-scoped
        MUST run (regression: the `target is None` gate skipped a `--mode batch
        --start-date <today>` re-enum, leaving the catalogue all-OTHER)."""
        adapter = KalshiReferenceDataAdapter()

        cutoff_resp = AsyncMock()
        cutoff_resp.status = 200
        cutoff_resp.raise_for_status = MagicMock()
        cutoff_resp.json = AsyncMock(return_value={"market_settled_ts": "2026-06-20T00:00:00Z"})

        snapshot_resp = AsyncMock()
        snapshot_resp.status = 200
        snapshot_resp.raise_for_status = MagicMock()
        snapshot_resp.json = AsyncMock(return_value={"markets": [], "cursor": ""})

        series_crypto_resp = AsyncMock()
        series_crypto_resp.status = 200
        series_crypto_resp.raise_for_status = MagicMock()
        series_crypto_resp.json = AsyncMock(return_value={"series": [{"ticker": "KXBTCD"}]})

        series_empty_resp = AsyncMock()
        series_empty_resp.status = 200
        series_empty_resp.raise_for_status = MagicMock()
        series_empty_resp.json = AsyncMock(return_value={"series": []})

        btc_resp = AsyncMock()
        btc_resp.status = 200
        btc_resp.raise_for_status = MagicMock()
        btc_resp.json = AsyncMock(
            return_value={
                "markets": [
                    {
                        "ticker": "KXBTCD-26JUN23-T90000",
                        "event_ticker": "KXBTCD-26JUN23",
                        "series_ticker": "KXBTCD",
                        "title": "BTC daily above $90k?",
                        "category": "Crypto",
                        "status": "active",
                        "yes_bid": 60,
                        "yes_ask": 65,
                        "open_time": "2026-06-23T00:00:00Z",
                        "close_time": "2026-06-23T23:59:59Z",
                        "expiration_time": "2026-06-23T23:59:59Z",
                    }
                ],
                "cursor": "",
            }
        )

        def get_side_effect(url: str, **kwargs: object) -> object:
            params = kwargs.get("params", {})
            if "historical/cutoff" in url:
                return _make_cm(cutoff_resp)
            if isinstance(params, dict) and params.get("series_ticker") == "KXBTCD":
                return _make_cm(btc_resp)
            if "/series" in url:
                cat = params.get("category") if isinstance(params, dict) else None
                return _make_cm(series_crypto_resp if cat == "Crypto" else series_empty_resp)
            return _make_cm(snapshot_resp)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=get_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm), patch("asyncio.sleep", AsyncMock()):
            results = await adapter.get_instruments(date="2026-06-23")  # on/after cutoff = live

        tickers = {r.instrument_key for r in results}
        assert "KALSHI:PREDICTION_MARKET:KXBTCD-26JUN23-T90000" in tickers, (
            f"current-day batch must run series-scoped; got: {tickers}"
        )

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
