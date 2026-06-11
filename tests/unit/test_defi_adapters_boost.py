"""Coverage boost for DeFi adapters (jupiter.py, pyth.py) uncovered branches.

Targets:
- jupiter._log_fetch_error (103-115)
- jupiter._fetch_token_list exception paths (154-159, 163)
- pyth._log_fetch_error (143-155)
- pyth.fetch_latest_prices (201, 210-215, 221)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ===========================================================================
# JupiterAdapter
# ===========================================================================


def _make_jupiter() -> object:
    from instruments_service.reference_data.adapters.defi.jupiter import JupiterReferenceDataAdapter

    return JupiterReferenceDataAdapter(chain="solana")


class TestJupiterLogFetchError:
    """Lines 103-115: _log_fetch_error classifies and emits."""

    @pytest.mark.asyncio
    async def test_log_fetch_error_with_classification(self) -> None:
        import aiohttp

        adapter = _make_jupiter()

        mock_cls = MagicMock()
        mock_cls.action = MagicMock()
        mock_cls.action.value = "fail"
        mock_cls.retry_safe = False

        with (
            patch(
                "instruments_service.reference_data.adapters.defi.jupiter.classify_venue_error",
                return_value=mock_cls,
            ),
            patch("instruments_service.reference_data.adapters.defi.jupiter.log_event") as mock_log,
        ):
            exc = aiohttp.ClientError("connection refused")
            adapter._log_fetch_error(exc, "tokens")  # type: ignore[attr-defined]

        mock_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_fetch_error_without_classification(self) -> None:
        import aiohttp

        adapter = _make_jupiter()

        with (
            patch(
                "instruments_service.reference_data.adapters.defi.jupiter.classify_venue_error",
                return_value=None,
            ),
            patch("instruments_service.reference_data.adapters.defi.jupiter.log_event") as mock_log,
        ):
            exc = aiohttp.ClientError("timeout")
            adapter._log_fetch_error(exc, "tokens")  # type: ignore[attr-defined]

        mock_log.assert_called_once()


class TestJupiterFetchTokenList:
    """Lines 154-163: exception paths + non-list return."""

    @pytest.mark.asyncio
    async def test_client_error_converts_to_connection_error(self) -> None:
        """Lines 155-157: aiohttp.ClientError → ConnectionError."""
        import aiohttp

        adapter = _make_jupiter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=aiohttp.ClientError("network error"),
            ),
            patch.object(adapter, "_log_fetch_error"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_token_list()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_runtime_error_reraises(self) -> None:
        """Lines 158-159: RuntimeError → logger.error + re-raise."""
        adapter = _make_jupiter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=RuntimeError("max retries exceeded"),
            ),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_token_list()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_non_list_response_returns_empty(self) -> None:
        """Line 163: response is not a list → return []."""
        adapter = _make_jupiter()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value={"data": "not a list"},
            ),
        ):
            result = await adapter._fetch_token_list()  # type: ignore[attr-defined]

        assert result == []


# ===========================================================================
# PythAdapter
# ===========================================================================


def _make_pyth() -> object:
    from instruments_service.reference_data.adapters.defi.pyth import PythOracleReferenceDataAdapter

    return PythOracleReferenceDataAdapter(chain="solana")


class TestPythLogFetchError:
    """Lines 143-155: _log_fetch_error classifies and emits."""

    @pytest.mark.asyncio
    async def test_log_fetch_error_with_classification(self) -> None:
        import aiohttp

        adapter = _make_pyth()

        mock_cls = MagicMock()
        mock_cls.action = MagicMock()
        mock_cls.action.value = "retry"
        mock_cls.retry_safe = True

        with (
            patch(
                "instruments_service.reference_data.adapters.defi.pyth.classify_venue_error",
                return_value=mock_cls,
            ),
            patch("instruments_service.reference_data.adapters.defi.pyth.log_event") as mock_log,
        ):
            exc = aiohttp.ClientError("connection refused")
            adapter._log_fetch_error(exc, "updates/price/latest")  # type: ignore[attr-defined]

        mock_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_fetch_error_without_classification(self) -> None:
        import aiohttp

        adapter = _make_pyth()

        with (
            patch(
                "instruments_service.reference_data.adapters.defi.pyth.classify_venue_error",
                return_value=None,
            ),
            patch("instruments_service.reference_data.adapters.defi.pyth.log_event") as mock_log,
        ):
            exc = aiohttp.ClientError("timeout")
            adapter._log_fetch_error(exc, "updates/price/latest")  # type: ignore[attr-defined]

        mock_log.assert_called_once()


class TestPythFetchLatestPrices:
    """Lines 201, 210-215, 221: default feed_ids + exception paths + non-dict return."""

    @pytest.mark.asyncio
    async def test_none_feed_ids_uses_defaults(self) -> None:
        """Line 201: feed_ids=None → populate from PYTH_PRICE_FEEDS."""
        adapter = _make_pyth()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        parsed_data = [{"id": "abc", "price": {"price": "100"}}]

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value={"parsed": parsed_data},
            ),
        ):
            result = await adapter.fetch_latest_prices(feed_ids=None)  # type: ignore[attr-defined]

        assert result == parsed_data

    @pytest.mark.asyncio
    async def test_client_error_converts_to_connection_error(self) -> None:
        """Lines 211-213: aiohttp.ClientError → ConnectionError."""
        import aiohttp

        adapter = _make_pyth()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=aiohttp.ClientError("connection error"),
            ),
            patch.object(adapter, "_log_fetch_error"),
            pytest.raises(ConnectionError),
        ):
            await adapter.fetch_latest_prices(["feed_1"])  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_runtime_error_reraises(self) -> None:
        """Lines 214-215: RuntimeError → logger.error + re-raise."""
        adapter = _make_pyth()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                side_effect=RuntimeError("retries exhausted"),
            ),
            pytest.raises(RuntimeError),
        ):
            await adapter.fetch_latest_prices(["feed_1"])  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_non_list_parsed_returns_empty(self) -> None:
        """Line 221: parsed is not a list → return []."""
        adapter = _make_pyth()

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=MagicMock())
        cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=cm),
            patch.object(
                adapter,
                "_get_with_retry",
                new_callable=AsyncMock,
                return_value={"parsed": "not_a_list"},
            ),
        ):
            result = await adapter.fetch_latest_prices(["feed_1"])  # type: ignore[attr-defined]

        assert result == []
