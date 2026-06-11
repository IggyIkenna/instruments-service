"""Coverage boost for cefi/tardis.py uncovered retry and fallback paths.

Targets inside _fetch_exchange_instruments:
- Lines 902-913: retryable HTTP status (429/5xx) — retry loop + final raise_for_status
- Lines 920-935: aiohttp.ClientError retry — break on final attempt
- Lines 938-956: fallback /v1/exchanges path (404 + retryable + success)
- Lines 957-993: /v1/exchanges ClientError all attempts → RuntimeError
- Lines 995-1004: instruments_list is None after both paths → RuntimeError
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

    return TardisReferenceDataAdapter(exchanges=["binance-futures"], api_key=None)


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
# Tests
# ---------------------------------------------------------------------------


class TestTardisRetryableHTTP:
    """Lines 902-913 + 920-935: retryable HTTP codes path + aiohttp.ClientError break."""

    @pytest.mark.asyncio
    async def test_retryable_status_all_attempts_then_fallback_success(self) -> None:
        """Lines 902-913: 503 on all 3 primary attempts → last attempt raise_for_status
        → caught as ClientError → break → fallback /v1/exchanges succeeds.

        Also covers 920-935 (except branch) and 938-956 (fallback branch).
        """
        adapter = _make_adapter()

        primary_resp_1 = _resp_cm(503)
        primary_resp_2 = _resp_cm(503)
        primary_resp_3 = _resp_cm(503)

        # Fallback exchange response with valid data
        exchange_json = {"id": "binance-futures", "instruments": []}
        fallback_resp = _resp_cm(200, json_data=exchange_json)

        call_count = 0

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "instruments" in url:
                if call_count == 1:
                    return primary_resp_1
                if call_count == 2:
                    return primary_resp_2
                return primary_resp_3
            # /v1/exchanges call
            return fallback_resp

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        mock_exchange_detail = MagicMock()
        mock_exchange_detail.instruments = []

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisExchangeDetail.model_validate",
                return_value=mock_exchange_detail,
            ),
        ):
            result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, None, "binance-futures"
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_retryable_status_non_final_attempts_sleep_and_continue(self) -> None:
        """Lines 910-912: non-final retryable attempt → sleep + continue (not raise_for_status)."""
        adapter = _make_adapter()

        sleep_calls: list[float] = []

        # 2 retryable + 1 success
        call_count = 0
        instruments_json = [{"id": "BTCUSDT"}]

        resp_503_a = _resp_cm(503)
        resp_503_b = _resp_cm(503)
        resp_200 = _resp_cm(200, json_data=instruments_json)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp_503_a
            if call_count == 2:
                return resp_503_b
            return resp_200

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        async def sleep_tracker(delay: float) -> None:
            sleep_calls.append(delay)

        with (
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisInstrumentDetail.model_validate",
                return_value=MagicMock(),
            ),
            patch.object(adapter, "_parse_tardis_instrument", return_value=None),  # type: ignore[attr-defined]
            patch("asyncio.sleep", side_effect=sleep_tracker),
        ):
            result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, None, "binance-futures"
            )

        # Two sleeps (attempt 0 and attempt 1)
        assert len(sleep_calls) == 2
        assert sleep_calls[0] < sleep_calls[1]  # delay doubles


class TestTardisClientErrorRetry:
    """Lines 920-935: aiohttp.ClientError retry — break on final attempt."""

    @pytest.mark.asyncio
    async def test_primary_client_error_all_attempts_then_fallback_success(self) -> None:
        """ClientError on all 3 primary attempts → break → fallback returns instruments."""
        adapter = _make_adapter()

        call_count = 0
        exchange_json = {"id": "binance-futures", "instruments": []}
        fallback_resp = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "/instruments/" in url:
                return _client_error_cm()
            return fallback_resp

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        mock_exchange_detail = MagicMock()
        mock_exchange_detail.instruments = []

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch(
                "instruments_service.reference_data.adapters.cefi.tardis.TardisExchangeDetail.model_validate",
                return_value=mock_exchange_detail,
            ),
        ):
            result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, None, "binance-futures"
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_primary_client_error_retry_sleeps(self) -> None:
        """Lines 922-932: non-final ClientError attempt → sleep + continue."""
        adapter = _make_adapter()

        call_count = 0
        sleep_calls: list[float] = []

        exchange_json = {"id": "binance-futures", "instruments": []}
        fallback_resp = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "/instruments/" in url:
                return _client_error_cm()
            return fallback_resp

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
            await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, None, "binance-futures"
            )

        # First 2 ClientError attempts sleep; 3rd breaks to fallback (no sleep on 3rd primary)
        assert len(sleep_calls) >= 2


class TestTardisFallbackPath:
    """Lines 938-993: fallback /v1/exchanges path."""

    @pytest.mark.asyncio
    async def test_fallback_404_returns_empty(self) -> None:
        """Lines 944-946: /v1/exchanges 404 → return []."""
        adapter = _make_adapter()

        call_count = 0

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "/instruments/" in url:
                return _resp_cm(401)  # 401 → break immediately on primary
            return _resp_cm(404)  # /v1/exchanges 404

        mock_session = MagicMock()
        mock_session.get = get_side_effect

        result = await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
            mock_session, None, "binance-futures"
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_fallback_retryable_then_success(self) -> None:
        """Lines 947-951: retryable in /v1/exchanges on non-final attempt → sleep + continue."""
        adapter = _make_adapter()

        call_count = 0
        sleep_calls: list[float] = []

        exchange_json = {"id": "binance-futures", "instruments": []}
        resp_503 = _resp_cm(503)
        resp_200 = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "/instruments/" in url:
                return _resp_cm(401)  # break immediately on primary
            # Fallback: 503 → 503 → 200
            if call_count == 2:
                return resp_503
            if call_count == 3:
                return resp_503
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
                mock_session, None, "binance-futures"
            )

        assert result == []
        # At least one sleep from retryable fallback attempt
        assert len(sleep_calls) >= 1

    @pytest.mark.asyncio
    async def test_fallback_client_error_all_attempts_raises(self) -> None:
        """Lines 957-993: /v1/exchanges ClientError on all 3 attempts → RuntimeError."""
        adapter = _make_adapter()

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            if "/instruments/" in url:
                return _resp_cm(401)  # primary: 401 → break
            return _client_error_cm()  # fallback: ClientError every time

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
                mock_session, None, "binance-futures"
            )

    @pytest.mark.asyncio
    async def test_fallback_retryable_final_attempt_raises(self) -> None:
        """Lines 951: retryable code on final /v1/exchanges attempt → raise_for_status → RuntimeError."""
        adapter = _make_adapter()

        call_count = 0
        resp_503 = _resp_cm(503)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if "/instruments/" in url:
                return _resp_cm(401)  # primary: 401 → break
            # All 3 fallback attempts return 503
            return resp_503

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
            # raise_for_status on 503 raises ClientResponseError (→ caught as ClientError)
            # → on final attempt → RuntimeError at line 990
            await adapter._fetch_exchange_instruments(  # type: ignore[attr-defined]
                mock_session, None, "binance-futures"
            )


class TestTardisNullInstrumentsList:
    """Lines 995-1004: instruments_list is None after both paths → RuntimeError."""

    @pytest.mark.asyncio
    async def test_instruments_list_none_after_both_paths_raises(self) -> None:
        """Primary 401 → break; fallback: model_validate returns instruments=None → break.
        instruments_list remains None → RuntimeError raised.
        """
        adapter = _make_adapter()

        exchange_json = {"id": "binance-futures", "instruments": None}
        fallback_resp = _resp_cm(200, json_data=exchange_json)

        def get_side_effect(url: str, headers: dict) -> MagicMock:
            if "/instruments/" in url:
                return _resp_cm(401)  # primary: 401 → break
            return fallback_resp

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
                mock_session, None, "binance-futures"
            )
