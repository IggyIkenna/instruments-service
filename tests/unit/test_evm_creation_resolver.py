"""Comprehensive tests for evm_creation_resolver — EVM contract creation timestamp resolution.

All tests are credential-free with fully mocked RPC calls and GCS storage.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from instruments_service.reference_data.utils.evm_creation_resolver import (
    LENDING_PROTOCOL_DEPLOY_DATES,
    EvmCacheSession,
    _get_code_at_block,
    _get_latest_block,
    _get_shared_or_load_cache,
    _load_cache,
    _parse_json_cache,
    _resolve_rpc_url,
    _save_cache,
    _update_cache,
    batch_resolve_evm_creation_timestamps,
    block_to_timestamp,
    get_protocol_floor_date,
    resolve_contract_creation,
)

# ---------------------------------------------------------------------------
# Protocol floor dates
# ---------------------------------------------------------------------------


class TestGetProtocolFloorDate:
    def test_known_protocol_and_chain_uses_uac_ssot(self) -> None:
        # UAC PROTOCOL_LAUNCH_DATES is the canonical SSOT — ETHEREUM AAVEV3
        # mainnet deploy was 2022-03-14 (NOT the legacy 2023-01-27 fallback
        # which silent-zeroed AAVEV3 ETHEREUM 2022 dates pre-2026-05-08).
        dt = get_protocol_floor_date("aave_v3", "ETHEREUM")
        assert dt == datetime(2022, 3, 14, tzinfo=UTC)

    def test_known_protocol_unknown_chain_returns_2020(self) -> None:
        dt = get_protocol_floor_date("aave_v3", "FANTOM")
        assert dt == datetime(2020, 1, 1, tzinfo=UTC)

    def test_unknown_protocol_returns_2020(self) -> None:
        dt = get_protocol_floor_date("nonexistent", "ETHEREUM")
        assert dt == datetime(2020, 1, 1, tzinfo=UTC)

    def test_case_insensitive_chain(self) -> None:
        dt = get_protocol_floor_date("aave_v3", "ethereum")
        assert dt == datetime(2022, 3, 14, tzinfo=UTC)

    def test_compound_v3_arbitrum_uses_uac_ssot(self) -> None:
        # UAC says ("ARBITRUM", "COMPOUNDV3") = 2023-04-13. The local fallback
        # had 2023-06-28; UAC takes precedence.
        dt = get_protocol_floor_date("compound_v3", "ARBITRUM")
        assert dt == datetime(2023, 4, 13, tzinfo=UTC)

    def test_protocol_unknown_to_uac_falls_back_to_local(self) -> None:
        # Spark has no UAC PROTOCOL_LAUNCH_DATES entry — falls back to local
        # LENDING_PROTOCOL_DEPLOY_DATES (2023-05-09 per Maker fork go-live).
        dt = get_protocol_floor_date("spark", "ETHEREUM")
        assert dt == datetime(2023, 5, 9, tzinfo=UTC)

    def test_all_aave_v3_chains_have_dates(self) -> None:
        for chain in LENDING_PROTOCOL_DEPLOY_DATES.get("aave_v3", {}):
            dt = get_protocol_floor_date("aave_v3", chain)
            assert dt.tzinfo == UTC
            assert dt.year >= 2020


# ---------------------------------------------------------------------------
# RPC URL resolution
# ---------------------------------------------------------------------------


class TestResolveRpcUrl:
    def test_with_alchemy_key(self) -> None:
        with (
            patch(
                "unified_api_contracts.registry.capability_declarations._defi.CHAIN_RPC_TEMPLATES",
                {1: "https://eth-mainnet.g.alchemy.com/v2/{api_key}"},
            ),
            patch(
                "unified_api_contracts.registry.chain_env.MAINNET_CHAIN_IDS",
                {"ETHEREUM": 1},
            ),
        ):
            url = _resolve_rpc_url("ETHEREUM", alchemy_key="test-key")
        assert url is not None
        assert "test-key" in url

    def test_no_key_secret_manager_fallback(self) -> None:
        mock_sc = MagicMock()
        mock_sc.get_secret.return_value = "sm-key"
        with (
            patch(
                "unified_trading_library.get_secret_client",
                return_value=mock_sc,
            ),
            patch(
                "unified_api_contracts.registry.capability_declarations._defi.CHAIN_RPC_TEMPLATES",
                {1: "https://eth-mainnet.g.alchemy.com/v2/{api_key}"},
            ),
            patch(
                "unified_api_contracts.registry.chain_env.MAINNET_CHAIN_IDS",
                {"ETHEREUM": 1},
            ),
        ):
            url = _resolve_rpc_url("ETHEREUM")
        assert url is not None
        assert "sm-key" in url

    def test_no_key_no_secret_returns_none(self) -> None:
        with patch(
            "unified_trading_library.get_secret_client",
            side_effect=ImportError("no UTL"),
        ):
            url = _resolve_rpc_url("ETHEREUM")
        assert url is None

    def test_unknown_chain_returns_none(self) -> None:
        with (
            patch(
                "unified_api_contracts.registry.capability_declarations._defi.CHAIN_RPC_TEMPLATES",
                {},
            ),
            patch(
                "unified_api_contracts.registry.chain_env.MAINNET_CHAIN_IDS",
                {},
            ),
        ):
            url = _resolve_rpc_url("UNKNOWN_CHAIN", alchemy_key="key")
        assert url is None

    def test_chain_id_zero_returns_none(self) -> None:
        with (
            patch(
                "unified_api_contracts.registry.capability_declarations._defi.CHAIN_RPC_TEMPLATES",
                {},
            ),
            patch(
                "unified_api_contracts.registry.chain_env.MAINNET_CHAIN_IDS",
                {"TESTNET": 0},
            ),
        ):
            url = _resolve_rpc_url("TESTNET", alchemy_key="key")
        assert url is None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


class TestParseJsonCache:
    def test_valid_dict(self) -> None:
        result = _parse_json_cache('{"key": "val"}')
        assert result == {"key": "val"}

    def test_non_dict_returns_none(self) -> None:
        result = _parse_json_cache('["not", "a", "dict"]')
        assert result is None

    def test_bytes_input(self) -> None:
        result = _parse_json_cache(b'{"key": "val"}')
        assert result == {"key": "val"}


class TestLoadEvmCache:
    def test_gcs_load_success(self) -> None:
        cache_data = json.dumps({"ETHEREUM:0xabc": "2024-01-01T00:00:00+00:00"}).encode()
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = cache_data
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_gcs_bucket",
                return_value="test-bucket",
            ),
        ):
            result = _load_cache()
        assert "ETHEREUM:0xabc" in result

    def test_gcs_failure_local_fallback(self, tmp_path) -> None:
        cache_data = {"ETHEREUM:0xdef": "2024-06-01T00:00:00+00:00"}
        local_file = tmp_path / "evm_cache.json"
        local_file.write_text(json.dumps(cache_data))
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_FILE",
                local_file,
            ),
        ):
            result = _load_cache()
        assert "ETHEREUM:0xdef" in result

    def test_no_cache_returns_empty(self) -> None:
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_FILE",
                MagicMock(exists=MagicMock(return_value=False)),
            ),
        ):
            result = _load_cache()
        assert result == {}


class TestSaveEvmCache:
    def test_gcs_save_with_merge(self) -> None:
        existing_data = json.dumps({"existing": "2024-01-01T00:00:00+00:00"}).encode()
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = existing_data
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_gcs_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_DIR",
                MagicMock(),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_FILE",
                MagicMock(),
            ),
        ):
            cache = {"new_key": "2024-06-01T00:00:00+00:00"}
            _save_cache(cache)
        mock_storage.upload_bytes.assert_called_once()
        assert "existing" in cache

    def test_gcs_failure_writes_local(self, tmp_path) -> None:
        local_file = tmp_path / "evm_cache.json"
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_DIR",
                tmp_path,
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._LOCAL_CACHE_FILE",
                local_file,
            ),
        ):
            _save_cache({"key1": "2024-01-01T00:00:00+00:00"})
        assert local_file.exists()


# ---------------------------------------------------------------------------
# EvmCacheSession
# ---------------------------------------------------------------------------


class TestEvmCacheSession:
    def test_session_loads_and_saves(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old_shared = mod._shared_cache
        old_size = mod._shared_cache_initial_size
        try:
            with (
                patch.object(mod, "_load_cache", return_value={"a": "v1"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                with EvmCacheSession():
                    assert mod._shared_cache is not None
                    mod._shared_cache["b"] = "v2"
                mock_save.assert_called_once()
            assert mod._shared_cache is None
        finally:
            mod._shared_cache = old_shared
            mod._shared_cache_initial_size = old_size

    def test_session_no_new_entries_skips_save(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old_shared = mod._shared_cache
        old_size = mod._shared_cache_initial_size
        try:
            with (
                patch.object(mod, "_load_cache", return_value={"a": "v1"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                with EvmCacheSession():
                    pass
                mock_save.assert_not_called()
        finally:
            mod._shared_cache = old_shared
            mod._shared_cache_initial_size = old_size


class TestEvmSharedCacheHelpers:
    def test_shared_active_returns_shared(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = {"x": "v"}
            result = _get_shared_or_load_cache()
            assert result == {"x": "v"}
        finally:
            mod._shared_cache = old

    def test_shared_none_loads_fresh(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with patch.object(mod, "_load_cache", return_value={"fresh": "v"}):
                result = _get_shared_or_load_cache()
            assert result == {"fresh": "v"}
        finally:
            mod._shared_cache = old

    def test_update_cache_empty_is_noop(self) -> None:
        _update_cache({})

    def test_update_cache_with_session(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = {"old": "v"}
            _update_cache({"new": "v2"})
            assert mod._shared_cache["new"] == "v2"
        finally:
            mod._shared_cache = old

    def test_update_cache_without_session_saves(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value={"old": "v"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                _update_cache({"new": "v2"})
            mock_save.assert_called_once()
        finally:
            mod._shared_cache = old


# ---------------------------------------------------------------------------
# RPC calls
# ---------------------------------------------------------------------------


class TestGetCodeAtBlock:
    @pytest.mark.asyncio
    async def test_code_present(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": "0x6080604052"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm

        result = await _get_code_at_block(mock_session, "https://rpc", "0xabc", "0x100")
        assert result is True

    @pytest.mark.asyncio
    async def test_no_code(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": "0x"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm

        result = await _get_code_at_block(mock_session, "https://rpc", "0xabc", "0x100")
        assert result is False

    @pytest.mark.asyncio
    async def test_empty_code(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": ""})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm

        result = await _get_code_at_block(mock_session, "https://rpc", "0xabc", "0x100")
        assert result is False


class TestGetLatestBlock:
    @pytest.mark.asyncio
    async def test_success(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": {"number": "0x100000"}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm

        result = await _get_latest_block(mock_session, "https://rpc")
        assert result == 0x100000

    @pytest.mark.asyncio
    async def test_unexpected_response_raises(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": "not-a-dict"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm

        with pytest.raises(KeyError, match="Unexpected RPC response"):
            await _get_latest_block(mock_session, "https://rpc")


# ---------------------------------------------------------------------------
# resolve_contract_creation
# ---------------------------------------------------------------------------


class TestResolveContractCreation:
    @pytest.mark.asyncio
    async def test_no_rpc_url_returns_none(self) -> None:
        with patch(
            "instruments_service.reference_data.utils.evm_creation_resolver._resolve_rpc_url",
            return_value=None,
        ):
            result = await resolve_contract_creation("0xabc", "UNKNOWN_CHAIN")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_code_at_latest_returns_none(self) -> None:
        with (
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_latest_block",
                AsyncMock(return_value=1000000),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_code_at_block",
                AsyncMock(return_value=False),
            ),
        ):
            result = await resolve_contract_creation("0xabc", "ETHEREUM", rpc_url="https://rpc")
        assert result is None

    @pytest.mark.asyncio
    async def test_binary_search_finds_deployment_block(self) -> None:
        # Contract deployed at block 500 (has code from block 500 onwards)
        deploy_block = 500
        call_count = 0

        async def mock_get_code(session, url, address, block_hex):
            nonlocal call_count
            call_count += 1
            block_num = int(block_hex, 16)
            return block_num >= deploy_block

        with (
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_latest_block",
                AsyncMock(return_value=1000),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_code_at_block",
                side_effect=mock_get_code,
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_block_timestamp",
                AsyncMock(return_value=datetime(2024, 1, 15, tzinfo=UTC)),
            ),
        ):
            result = await resolve_contract_creation("0xabc", "ETHEREUM", rpc_url="https://rpc")
        assert result is not None
        assert result.year == 2024

    @pytest.mark.asyncio
    async def test_rpc_error_returns_none(self) -> None:
        with (
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_latest_block",
                AsyncMock(side_effect=aiohttp.ClientError("fail")),
            ),
        ):
            result = await resolve_contract_creation("0xabc", "ETHEREUM", rpc_url="https://rpc")
        assert result is None

    @pytest.mark.asyncio
    async def test_creates_and_closes_own_session(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.close = AsyncMock()

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_latest_block",
                AsyncMock(return_value=1000),
            ),
            patch(
                "instruments_service.reference_data.utils.evm_creation_resolver._get_code_at_block",
                AsyncMock(return_value=False),
            ),
        ):
            result = await resolve_contract_creation("0xabc", "ETHEREUM", rpc_url="https://rpc")
        assert result is None
        mock_session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# block_to_timestamp
# ---------------------------------------------------------------------------


class TestBlockToTimestamp:
    @pytest.mark.asyncio
    async def test_success(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": {"timestamp": "0x65b1e000", "number": "0x100"}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await block_to_timestamp(100, "ETHEREUM", rpc_url="https://rpc")
        assert result is not None
        assert result.tzinfo == UTC

    @pytest.mark.asyncio
    async def test_no_rpc_url_returns_none(self) -> None:
        with patch(
            "instruments_service.reference_data.utils.evm_creation_resolver._resolve_rpc_url",
            return_value=None,
        ):
            result = await block_to_timestamp(100, "UNKNOWN_CHAIN")
        assert result is None


# ---------------------------------------------------------------------------
# batch_resolve_evm_creation_timestamps
# ---------------------------------------------------------------------------


class TestBatchResolveEvmCreationTimestamps:
    @pytest.mark.asyncio
    async def test_all_cached(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        cached = {"ETHEREUM:0xabc": "2024-01-01T00:00:00+00:00"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with patch.object(mod, "_load_cache", return_value=cached):
                result = await batch_resolve_evm_creation_timestamps(["0xabc"], "ETHEREUM")
        finally:
            mod._shared_cache = old
        assert len(result) == 1
        assert result["0xabc"].year == 2024

    @pytest.mark.asyncio
    async def test_empty_addresses(self) -> None:
        result = await batch_resolve_evm_creation_timestamps([], "ETHEREUM")
        assert result == {}

    @pytest.mark.asyncio
    async def test_no_rpc_url_returns_cached_only(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value={}),
                patch.object(mod, "_resolve_rpc_url", return_value=None),
            ):
                result = await batch_resolve_evm_creation_timestamps(["0xabc"], "UNKNOWN")
        finally:
            mod._shared_cache = old
        assert result == {}

    @pytest.mark.asyncio
    async def test_mixed_cached_and_uncached(self) -> None:
        import instruments_service.reference_data.utils.evm_creation_resolver as mod

        cached = {"ETHEREUM:0xabc": "2024-01-01T00:00:00+00:00"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value=cached),
                patch.object(mod, "_resolve_rpc_url", return_value="https://rpc"),
                patch.object(
                    mod,
                    "resolve_contract_creation",
                    AsyncMock(return_value=datetime(2024, 6, 1, tzinfo=UTC)),
                ),
                patch.object(mod, "_update_cache"),
            ):
                result = await batch_resolve_evm_creation_timestamps(["0xabc", "0xdef"], "ETHEREUM")
        finally:
            mod._shared_cache = old
        assert len(result) == 2
