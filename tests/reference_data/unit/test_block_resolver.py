"""Tests for the date-to-block resolver utility."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.utils.block_resolver import (
    _resolve_alchemy_key,
    date_to_block,
)


class TestResolveAlchemyKey:
    def test_returns_key_when_provided(self) -> None:
        assert _resolve_alchemy_key("my-key") == "my-key"

    def test_fallback_to_secret_manager(self) -> None:
        mock_client = MagicMock()
        mock_client.get_secret.return_value = "sm-key"
        with patch(
            "instruments_service.reference_data.utils.block_resolver.get_secret_client",
            return_value=mock_client,
        ):
            result = _resolve_alchemy_key(None)
        assert result == "sm-key"

    def test_secret_manager_failure_returns_none(self) -> None:
        with patch(
            "instruments_service.reference_data.utils.block_resolver.get_secret_client",
            side_effect=ConnectionError("no SM"),
        ):
            result = _resolve_alchemy_key(None)
        assert result is None


class TestDateToBlock:
    @pytest.mark.asyncio
    async def test_non_ethereum_returns_none(self) -> None:
        result = await date_to_block("2024-01-01", chain="SOLANA")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_key_returns_none(self) -> None:
        with patch(
            "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
            return_value=None,
        ):
            result = await date_to_block("2024-01-01")
        assert result is None

    @pytest.mark.asyncio
    async def test_with_key_mocked_success(self) -> None:
        # Mock the binary search to return a block number
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "jsonrpc": "2.0",
                "result": {"number": "0x100000", "timestamp": "0x65b1e000"},
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
                return_value="test-key",
            ),
        ):
            result = await date_to_block("2024-01-01")
        assert result is not None
        assert isinstance(result, int)

    @pytest.mark.asyncio
    async def test_exception_returns_none(self) -> None:
        with (
            patch(
                "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
                return_value="test-key",
            ),
            patch("aiohttp.ClientSession", side_effect=RuntimeError("network fail")),
        ):
            result = await date_to_block("2024-01-01")
        assert result is None
