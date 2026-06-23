"""Coverage boost for cefi/tardis.py retry paths on the NO-AUTH metadata endpoint.

Targets inside ``_fetch_exchange_instruments`` after the 2026-06-23 rewrite to a
single FREE, NO-AUTH ``GET /v1/exchanges/{exchange}`` call (the authenticated
``/v1/instruments`` primary path + its fallback were removed — IS must not
consume the Tardis key; operator 2026-06-23):
- retryable HTTP status (429/5xx) — retry loop + non-final sleep + eventual success
- retryable status on the final attempt → ``raise_for_status`` → RuntimeError
- ``aiohttp.ClientError`` on all attempts → RuntimeError
- 404 → return []
- ``model_validate`` yields ``instruments=None`` after the loop → RuntimeError
- every call hits ``/v1/exchanges/`` (never ``/v1/instruments/``) with no auth header
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

# ---------------------------------------------------------------------------
# Adapter factory
# ---------------------------------------------------------------------------


def _make_adapter() -> object:
    from instruments_service.reference_data.adapters.cefi.tardis import TardisReferenceDataAdapter

    # No api_key: IS enumeration is keyless.
    return TardisReferenceDataAdapter(exchanges=["binance-futures"])


# ---------------------------------------------------------------------------
# Session mock helpers
# ---------------------------------------------------------------------------


def _resp_cm(status: int, json_data: object = None) -> MagicMock:
    """Build an async context-manager mock for session.get() that returns the given status."""
    resp = MagicMock()
    resp.status = status
    if status in (429, 500, 502, 503, 504):
        # raise_for_status raises ClientResponseError (a ClientError subclass)
        resp.raise_for_status.side_effect = aiohttp.ClientResponseError(MagicMock(), (), status=status)
    else:
        resp.raise_for_status = MagicMock()

    if json_data is not None:
        resp.json = AsyncMock(return_value=json_data)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


def _client_error_cm() -> MagicMock:
    """session.get() async context manager that raises ClientError on __aenter__."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("network error"))
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


# ---------------------------------------------------------------------------
# Tests — single no-auth /v1/exchanges path
# ---------------------------------------------------------------------------


class TestTardisRetryableHTTP:
    """Retryable HTTP codes on the free /v1/exchanges metadata endpoint."""

    @pytest.mark.asyncio
    async def test_retryable_status_non_final_attempts_sleep_and_continue(self) -> None:
        """503 → 503 → 200: non-final retryable attempts sleep + continue, then succeed."""
        adapter = _make_adapter()

        sleep_calls: list[float] = []
        call_count = 0
        exchange_json = {"id": "binance-futures", "name": "Binance Futures", "instruments": []}

        resp_503_a = _resp_cm(503)
        resp_503_b = _resp_cm(503)
        resp_200 = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            # Every call must hit the free endpoint, never the authenticated one.
            assert "/v1/exchanges/" in url
            assert "/v1/instruments/" not in url
            if call_count == 1:
                return resp_503_a
            if call_count == 2:
                return resp_503_b
            return resp_200

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        mock_exchange_detail = MagicMock()
        mock_exchange_detail.instruments = []

        async def sleep_tracker(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisExchangeDetail.model_validate",
                return_value=mock_exchange_detail,
            ),
            patch("asyncio.sleep", side_effect=sleep_tracker),
        ):
            result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, "binance-futures"
            )

        assert result == []
        # Two sleeps (attempt 0 and attempt 1)
        assert len(sleep_calls) == 2
        assert sleep_calls[0] < sleep_calls[1]  # delay doubles

    @pytest.mark.asyncio
    async def test_retryable_status_final_attempt_raises(self) -> None:
        """503 on all 3 attempts → raise_for_status → RuntimeError (no silent empty)."""
        adapter = _make_adapter()

        def get_side_effect(url: str) -> MagicMock:
            assert "/v1/exchanges/" in url
            return _resp_cm(503)

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.classify_venue_error",
                return_value=None,
            ),
            patch("instruments_service.reference_data.adapters.cefi.tardis.log_event"),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis._classify_tardis_error",
                return_value="SERVER_ERROR",
            ),
            pytest.raises((RuntimeError, aiohttp.ClientResponseError)),
        ):
            await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, "binance-futures"
            )


class TestTardisClientErrorRetry:
    """aiohttp.ClientError retry on the free endpoint."""

    @pytest.mark.asyncio
    async def test_client_error_all_attempts_raises(self) -> None:
        """ClientError on all 3 attempts → RuntimeError (CF-11: never silent clean-empty)."""
        adapter = _make_adapter()

        def get_side_effect(url: str) -> MagicMock:
            assert "/v1/exchanges/" in url
            return _client_error_cm()

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.classify_venue_error",
                return_value=None,
            ),
            patch("instruments_service.reference_data.adapters.cefi.tardis.log_event"),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis._classify_tardis_error",
                return_value="NETWORK_ERROR",
            ),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, "binance-futures"
            )

    @pytest.mark.asyncio
    async def test_client_error_non_final_attempt_sleeps(self) -> None:
        """Non-final ClientError attempt → sleep + continue (then eventual success)."""
        adapter = _make_adapter()

        call_count = 0
        sleep_calls: list[float] = []
        exchange_json = {"id": "binance-futures", "name": "Binance Futures", "instruments": []}
        resp_200 = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            assert "/v1/exchanges/" in url
            if call_count < 3:
                return _client_error_cm()
            return resp_200

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        mock_exchange_detail = MagicMock()
        mock_exchange_detail.instruments = []

        async def sleep_tracker(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch("asyncio.sleep", side_effect=sleep_tracker),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisExchangeDetail.model_validate",
                return_value=mock_exchange_detail,
            ),
        ):
            result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, "binance-futures"
            )

        assert result == []
        # First 2 ClientError attempts sleep; 3rd succeeds.
        assert len(sleep_calls) >= 2


class TestTardisExchangesPath:
    """Free /v1/exchanges endpoint terminal cases."""

    @pytest.mark.asyncio
    async def test_404_returns_empty(self) -> None:
        """/v1/exchanges 404 → return []."""
        adapter = _make_adapter()

        def get_side_effect(url: str) -> MagicMock:
            assert "/v1/exchanges/" in url
            return _resp_cm(404)

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
            mock_session, "binance-futures"
        )

        assert result == []


class TestTardisNullInstrumentsList:
    """model_validate yields instruments=None after the loop → RuntimeError."""

    @pytest.mark.asyncio
    async def test_instruments_list_none_raises(self) -> None:
        """A 200 whose parsed detail has instruments=None → instruments_list stays None → RuntimeError."""
        adapter = _make_adapter()

        exchange_json = {"id": "binance-futures", "name": "Binance Futures", "instruments": None}
        resp_200 = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str) -> MagicMock:
            assert "/v1/exchanges/" in url
            return resp_200

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        mock_exchange_detail = MagicMock()
        mock_exchange_detail.instruments = None  # key: instruments is None

        with (
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisExchangeDetail.model_validate",
                return_value=mock_exchange_detail,
            ),
            pytest.raises(RuntimeError, match="no instruments"),
        ):
            await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, "binance-futures"
            )
