"""Edge-case coverage for instruments_service.reference_data.utils.block_resolver."""

from __future__ import annotations

import pytest

from instruments_service.reference_data.utils import block_resolver
from instruments_service.reference_data.utils.block_resolver import date_to_block


@pytest.mark.asyncio
async def test_date_to_block_rejects_non_evm_chain() -> None:
    assert await date_to_block("2024-01-01", chain="SOLANA") is None


@pytest.mark.asyncio
async def test_date_to_block_rejects_unknown_evm_chain() -> None:
    assert await date_to_block("2024-01-01", chain="NOT_A_REAL_CHAIN_XYZ") is None


@pytest.mark.asyncio
async def test_date_to_block_returns_none_without_alchemy_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
        lambda _k: None,
    )
    assert await date_to_block("2024-01-01", chain="ETHEREUM") is None


@pytest.mark.asyncio
async def test_date_to_block_returns_none_without_rpc_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
        lambda _k: "test-key",
    )
    monkeypatch.setattr(
        "instruments_service.reference_data.utils.block_resolver.resolve_rpc_url",
        lambda *_a, **_k: None,
    )
    assert await date_to_block("2024-01-01", chain="ETHEREUM") is None


@pytest.mark.asyncio
async def test_date_to_block_cache_hit_short_circuits_network() -> None:
    cache_key = ("ETHEREUM", "2024-06-01")
    block_resolver._block_cache[cache_key] = 12_345
    try:
        assert await date_to_block("2024-06-01", chain="ETHEREUM") == 12_345
    finally:
        del block_resolver._block_cache[cache_key]


@pytest.mark.asyncio
async def test_date_to_block_invalid_date_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "instruments_service.reference_data.utils.block_resolver._resolve_alchemy_key",
        lambda _k: "test-key",
    )
    monkeypatch.setattr(
        "instruments_service.reference_data.utils.block_resolver.resolve_rpc_url",
        lambda *_a, **_k: "http://example.invalid",
    )
    assert await date_to_block("not-a-date", chain="ETHEREUM") is None
