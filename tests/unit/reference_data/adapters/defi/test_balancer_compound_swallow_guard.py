"""Regression guards for DeFi-plan A8 — DeFi reference-data adapters must RAISE on a soft
``200-with-errors`` / missing-``data`` body instead of silently returning an empty universe.

Before the A8 fix these adapters did ``raw.get("data", {}).get(...)`` (subgraph) or
``raw.get("data") or {}`` (REST) — a transient upstream error (rate-limit / indexing lag / GraphQL
error envelope / REST ``success=false``) returned an empty instrument list with no exception, so
those instrument-days dropped out of the expected-coverage denominator entirely (false-complete
coverage) instead of being honestly recorded ``attempted_failed``.

Covered: balancer + compound_v3 (named in plan A8 follow-up) AND the four further swallow shapes
found by the slot-2 readiness audit 2026-06-04 — uniswap_v2, uniswap_v4 (The Graph subgraph),
curve, raydium (REST envelope).

Credential-free; no live network (``aiohttp.ClientSession`` / the retry helper are mocked).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter
from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

_FAKE_SUBGRAPH_URL = "https://gateway.example/subgraphs/id/abc"


def _mock_aiohttp_session(json_data: dict[str, object]) -> MagicMock:
    """Mock aiohttp session whose POST/GET returns ``json_data`` with HTTP 200."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_data)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)
    mock_session.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


# ── subgraph adapters (assert_subgraph_payload guard) ────────────────────────


class TestBalancerSwallowGuard:
    @pytest.mark.asyncio
    async def test_200_with_errors_raises(self) -> None:
        adapter = BalancerReferenceDataAdapter(chain="ethereum")
        body = {"errors": [{"message": "indexing in progress"}]}
        with (
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session(body)),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = BalancerReferenceDataAdapter(chain="ethereum")
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"data": {"poolGetPools": []}})):
            assert await adapter.get_instruments() == []


class TestUniswapV2SwallowGuard:
    @pytest.mark.asyncio
    async def test_200_with_errors_raises(self) -> None:
        adapter = UniswapV2ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch.object(adapter, "_resolve_block_num", AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"errors": [{"message": "x"}]})),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = UniswapV2ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch.object(adapter, "_resolve_block_num", AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"data": {"pairs": []}})),
        ):
            assert await adapter.get_instruments() == []


class TestUniswapV4SwallowGuard:
    @pytest.mark.asyncio
    async def test_200_with_errors_raises(self) -> None:
        adapter = UniswapV4ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch.object(adapter, "_resolve_block_num", AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"errors": [{"message": "x"}]})),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = UniswapV4ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch.object(adapter, "_resolve_block_num", AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"data": {"pools": []}})),
        ):
            assert await adapter.get_instruments() == []


# ── REST adapters (envelope guard: success=false / missing data → raise) ─────


class TestCompoundV3SwallowGuard:
    @pytest.mark.asyncio
    async def test_200_with_errors_raises(self) -> None:
        adapter = CompoundV3ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"errors": [{"message": "x"}]})),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = CompoundV3ReferenceDataAdapter(chain="ethereum")
        with (
            patch.object(adapter, "_resolve_api_url", return_value=_FAKE_SUBGRAPH_URL),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"data": {"markets": []}})),
        ):
            assert await adapter.get_instruments() == []


class TestCurveSwallowGuard:
    @pytest.mark.asyncio
    async def test_success_false_raises(self) -> None:
        adapter = CurveReferenceDataAdapter(chain="ethereum")
        with (
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"success": False})),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = CurveReferenceDataAdapter(chain="ethereum")
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session({"data": {"poolData": []}})):
            assert await adapter.get_instruments() == []


class TestRaydiumSwallowGuard:
    @pytest.mark.asyncio
    async def test_success_false_raises(self) -> None:
        adapter = RaydiumReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value={"success": False})),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_active_pools()

    @pytest.mark.asyncio
    async def test_legit_empty_returns_empty(self) -> None:
        adapter = RaydiumReferenceDataAdapter()
        with patch.object(adapter, "_get_with_retry", AsyncMock(return_value={"success": True, "data": {"data": []}})):
            assert await adapter._fetch_active_pools() == []
