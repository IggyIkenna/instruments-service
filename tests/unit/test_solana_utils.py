"""Comprehensive tests for _solana_utils — Solana creation timestamp resolution, caching, and pool discovery.

All tests are credential-free with fully mocked RPC calls and GCS storage.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from instruments_service.reference_data.adapters.defi._solana_utils import (
    SOLANA_PROTOCOL_DEPLOY_DATES,
    SolanaCacheSession,
    _get_shared_or_load_cache,
    _get_solana_rpc_url,
    _is_public_rpc,
    _load_cache,
    _load_discovered_pools,
    _save_cache,
    _save_discovered_pools,
    _update_cache,
    _use_fast_resolve,
    batch_resolve_creation_timestamps,
    discover_program_pool_accounts,
    fill_solana_cache,
    get_account_creation_timestamp,
    get_protocol_floor_date,
)

# ---------------------------------------------------------------------------
# Protocol floor dates
# ---------------------------------------------------------------------------


class TestGetProtocolFloorDate:
    def test_known_protocol(self) -> None:
        dt = get_protocol_floor_date("drift")
        assert dt == datetime(2022, 11, 4, tzinfo=UTC)

    def test_case_insensitive(self) -> None:
        dt = get_protocol_floor_date("DRIFT")
        assert dt == datetime(2022, 11, 4, tzinfo=UTC)

    def test_all_registered_protocols(self) -> None:
        for protocol in SOLANA_PROTOCOL_DEPLOY_DATES:
            dt = get_protocol_floor_date(protocol)
            assert dt.tzinfo == UTC

    def test_unknown_protocol_raises_key_error(self) -> None:
        with pytest.raises(KeyError, match="No floor date for Solana protocol"):
            get_protocol_floor_date("nonexistent_protocol")


# ---------------------------------------------------------------------------
# RPC URL resolution
# ---------------------------------------------------------------------------


class TestGetSolanaRpcUrl:
    def test_alchemy_key_available(self) -> None:
        mock_secret = MagicMock()
        mock_secret.get_secret.return_value = "test-alchemy-key"
        mock_templates = {"alchemy": "https://solana-mainnet.g.alchemy.com/v2/{api_key}"}
        with (
            patch(
                "unified_trading_library.get_secret_client",
                return_value=mock_secret,
            ),
            patch(
                "unified_api_contracts.registry.capability_declarations._defi.SOLANA_RPC_TEMPLATES",
                mock_templates,
            ),
        ):
            url = _get_solana_rpc_url()
        assert "test-alchemy-key" in url

    def test_alchemy_key_not_available_falls_back(self) -> None:
        with patch(
            "unified_trading_library.get_secret_client",
            side_effect=ImportError("no UTL"),
        ):
            url = _get_solana_rpc_url()
        assert "api.mainnet-beta.solana.com" in url

    def test_secret_manager_returns_none(self) -> None:
        mock_secret = MagicMock()
        mock_secret.get_secret.return_value = None
        with patch(
            "unified_trading_library.get_secret_client",
            return_value=mock_secret,
        ):
            url = _get_solana_rpc_url()
        assert "api.mainnet-beta.solana.com" in url


class TestIsPublicRpc:
    def test_public_rpc_detected(self) -> None:
        assert _is_public_rpc("https://api.mainnet-beta.solana.com") is True

    def test_alchemy_rpc_not_public(self) -> None:
        assert _is_public_rpc("https://solana-mainnet.g.alchemy.com/v2/key") is False


class TestUseFastResolve:
    def test_always_returns_true(self) -> None:
        assert _use_fast_resolve() is True


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------


class TestLoadCache:
    def test_gcs_load_success(self) -> None:
        cache_data = json.dumps({"addr1": "2024-01-01T00:00:00+00:00"}).encode()
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = cache_data
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
        ):
            result = _load_cache()
        assert result == {"addr1": "2024-01-01T00:00:00+00:00"}

    def test_gcs_load_failure_falls_back_to_local(self, tmp_path) -> None:
        cache_data = {"addr2": "2024-06-01T00:00:00+00:00"}
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                tmp_path / "cache.json",
            ),
        ):
            (tmp_path / "cache.json").write_text(json.dumps(cache_data))
            result = _load_cache()
        assert result == cache_data

    def test_no_cache_returns_empty(self) -> None:
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                MagicMock(exists=MagicMock(return_value=False)),
            ),
        ):
            result = _load_cache()
        assert result == {}

    def test_gcs_returns_non_dict(self) -> None:
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = b'"not-a-dict"'
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                MagicMock(exists=MagicMock(return_value=False)),
            ),
        ):
            result = _load_cache()
        assert result == {}

    def test_local_cache_json_decode_error(self, tmp_path) -> None:
        bad_file = tmp_path / "cache.json"
        bad_file.write_text("{invalid json")
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                bad_file,
            ),
        ):
            result = _load_cache()
        assert result == {}


class TestSaveCache:
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
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_DIR",
                MagicMock(),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                MagicMock(),
            ),
        ):
            cache = {"new_addr": "2024-06-01T00:00:00+00:00"}
            _save_cache(cache)
        mock_storage.upload_bytes.assert_called_once()
        # After merge, cache should contain both entries
        assert "existing" in cache
        assert "new_addr" in cache

    def test_gcs_save_no_existing_data(self) -> None:
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_DIR",
                MagicMock(),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                MagicMock(),
            ),
        ):
            _save_cache({"addr": "2024-01-01T00:00:00+00:00"})
        mock_storage.upload_bytes.assert_called_once()

    def test_gcs_failure_still_writes_local(self, tmp_path) -> None:
        local_file = tmp_path / "cache.json"
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_DIR",
                tmp_path,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._LOCAL_CACHE_FILE",
                local_file,
            ),
        ):
            _save_cache({"addr1": "2024-01-01T00:00:00+00:00"})
        assert local_file.exists()
        data = json.loads(local_file.read_text())
        assert "addr1" in data


# ---------------------------------------------------------------------------
# SolanaCacheSession
# ---------------------------------------------------------------------------


class TestSolanaCacheSession:
    def test_session_loads_and_saves(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old_shared = mod._shared_cache
        old_size = mod._shared_cache_initial_size
        try:
            with (
                patch.object(mod, "_load_cache", return_value={"a": "2024-01-01T00:00:00+00:00"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                with SolanaCacheSession():
                    assert mod._shared_cache is not None
                    assert len(mod._shared_cache) == 1
                    # Simulate adding a new entry during session
                    mod._shared_cache["b"] = "2024-06-01T00:00:00+00:00"
                # After exit, save should have been called (new entries added)
                mock_save.assert_called_once()
            assert mod._shared_cache is None
        finally:
            mod._shared_cache = old_shared
            mod._shared_cache_initial_size = old_size

    def test_session_no_new_entries_skips_save(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old_shared = mod._shared_cache
        old_size = mod._shared_cache_initial_size
        try:
            with (
                patch.object(mod, "_load_cache", return_value={"a": "2024-01-01T00:00:00+00:00"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                with SolanaCacheSession():
                    pass  # No new entries
                mock_save.assert_not_called()
        finally:
            mod._shared_cache = old_shared
            mod._shared_cache_initial_size = old_size


# ---------------------------------------------------------------------------
# _get_shared_or_load_cache / _update_cache
# ---------------------------------------------------------------------------


class TestSharedCacheHelpers:
    def test_shared_active_returns_shared(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = {"x": "v"}
            result = _get_shared_or_load_cache()
            assert result == {"x": "v"}
        finally:
            mod._shared_cache = old

    def test_shared_none_loads_fresh(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with patch.object(mod, "_load_cache", return_value={"fresh": "v"}):
                result = _get_shared_or_load_cache()
            assert result == {"fresh": "v"}
        finally:
            mod._shared_cache = old

    def test_update_cache_empty_entries_noop(self) -> None:
        _update_cache({})  # Should not raise

    def test_update_cache_with_session(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = {"existing": "v"}
            _update_cache({"new": "v2"})
            assert mod._shared_cache["new"] == "v2"
        finally:
            mod._shared_cache = old

    def test_update_cache_without_session_new_entries(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

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

    def test_update_cache_without_session_no_new_entries(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value={"old": "v"}),
                patch.object(mod, "_save_cache") as mock_save,
            ):
                _update_cache({"old": "v"})  # Same key, doesn't grow
            mock_save.assert_not_called()
        finally:
            mod._shared_cache = old


# ---------------------------------------------------------------------------
# RPC call and account creation resolution
# ---------------------------------------------------------------------------


class TestGetAccountCreationTimestamp:
    @pytest.mark.asyncio
    async def test_creates_and_closes_own_session(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {"blockTime": 1700000000, "signature": "sig1"},
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm
        mock_session.close = AsyncMock()

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await get_account_creation_timestamp(
                "SomeAddress123",
                rpc_url="https://test.rpc",
            )
        assert result is not None
        assert result.year == 2023
        mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_uses_provided_session(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {"blockTime": 1700000000, "signature": "sig1"},
                ],
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm

        result = await get_account_creation_timestamp(
            "SomeAddress123",
            rpc_url="https://test.rpc",
            session=mock_session,
        )
        assert result is not None
        mock_session.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rpc_failure_returns_none(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.json = AsyncMock(return_value={})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm

        result = await get_account_creation_timestamp(
            "SomeAddress123",
            rpc_url="https://test.rpc",
            session=mock_session,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_client_error_returns_none(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.close = AsyncMock()
        mock_session.post.side_effect = aiohttp.ClientError("connection failed")

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await get_account_creation_timestamp(
                "SomeAddress123",
                rpc_url="https://test.rpc",
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_result_returns_none(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"result": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm

        result = await get_account_creation_timestamp(
            "SomeAddress123",
            rpc_url="https://test.rpc",
            session=mock_session,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_rpc_error_in_response(self) -> None:
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"error": {"code": -32600}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm

        result = await get_account_creation_timestamp(
            "SomeAddress123",
            rpc_url="https://test.rpc",
            session=mock_session,
        )
        assert result is None


# ---------------------------------------------------------------------------
# batch_resolve_creation_timestamps
# ---------------------------------------------------------------------------


class TestBatchResolveCreationTimestamps:
    @pytest.mark.asyncio
    async def test_all_cached(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        cached = {"addr1": "2024-01-01T00:00:00+00:00", "addr2": "2024-06-01T00:00:00+00:00"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value=cached),
                patch.object(mod, "_get_solana_rpc_url", return_value="https://test.rpc"),
            ):
                result = await batch_resolve_creation_timestamps(["addr1", "addr2"])
        finally:
            mod._shared_cache = old
        assert len(result) == 2
        assert result["addr1"].year == 2024

    @pytest.mark.asyncio
    async def test_empty_addresses(self) -> None:
        result = await batch_resolve_creation_timestamps([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_mixed_cached_and_uncached(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        cached = {"addr1": "2024-01-01T00:00:00+00:00"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value=cached),
                patch.object(mod, "_get_solana_rpc_url", return_value="https://test.rpc"),
                patch.object(
                    mod,
                    "get_account_creation_timestamp",
                    AsyncMock(return_value=datetime(2024, 6, 1, tzinfo=UTC)),
                ),
                patch.object(mod, "_update_cache"),
            ):
                result = await batch_resolve_creation_timestamps(["addr1", "addr2"])
        finally:
            mod._shared_cache = old
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_invalid_cached_timestamp_triggers_resolve(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        cached = {"addr1": "not-a-valid-iso-date"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value=cached),
                patch.object(mod, "_get_solana_rpc_url", return_value="https://test.rpc"),
                patch.object(
                    mod,
                    "get_account_creation_timestamp",
                    AsyncMock(return_value=datetime(2024, 6, 1, tzinfo=UTC)),
                ),
                patch.object(mod, "_update_cache"),
            ):
                result = await batch_resolve_creation_timestamps(["addr1"])
        finally:
            mod._shared_cache = old
        assert "addr1" in result


# ---------------------------------------------------------------------------
# fill_solana_cache
# ---------------------------------------------------------------------------


class TestFillSolanaCache:
    @pytest.mark.asyncio
    async def test_all_cached_returns_immediately(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        cached = {"addr1": "2024-01-01T00:00:00+00:00"}
        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with patch.object(mod, "_load_cache", return_value=cached):
                result = await fill_solana_cache(["addr1"])
        finally:
            mod._shared_cache = old
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_resolves_uncached_addresses(self) -> None:
        import instruments_service.reference_data.adapters.defi._solana_utils as mod

        old = mod._shared_cache
        try:
            mod._shared_cache = None
            with (
                patch.object(mod, "_load_cache", return_value={}),
                patch.object(mod, "_get_solana_rpc_url", return_value="https://test.rpc"),
                patch.object(
                    mod,
                    "get_account_creation_timestamp",
                    AsyncMock(return_value=datetime(2024, 1, 1, tzinfo=UTC)),
                ),
                patch.object(mod, "_update_cache"),
            ):
                result = await fill_solana_cache(["addr1", "addr2"], concurrency=2)
        finally:
            mod._shared_cache = old
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Pool discovery
# ---------------------------------------------------------------------------


class TestLoadDiscoveredPools:
    def test_gcs_load(self) -> None:
        pools = ["pool1", "pool2"]
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = json.dumps(pools).encode()
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
        ):
            result = _load_discovered_pools("raydium")
        assert result == ["pool1", "pool2"]

    def test_gcs_failure_local_fallback(self, tmp_path) -> None:
        local_file = tmp_path / "solana_discovered_pools_raydium.json"
        local_file.write_text(json.dumps(["pool3"]))
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._POOL_DISCOVERY_LOCAL_DIR",
                tmp_path,
            ),
        ):
            result = _load_discovered_pools("raydium")
        assert result == ["pool3"]

    def test_no_cache_returns_empty(self) -> None:
        with (
            patch(
                "unified_trading_library.get_storage_client",
                side_effect=ImportError("no GCS"),
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._POOL_DISCOVERY_LOCAL_DIR",
                MagicMock(**{"__truediv__": MagicMock(return_value=MagicMock(exists=MagicMock(return_value=False)))}),
            ),
        ):
            result = _load_discovered_pools("raydium")
        assert result == []


class TestSaveDiscoveredPools:
    def test_gcs_save_with_merge(self) -> None:
        existing_data = json.dumps(["existing_pool"]).encode()
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = existing_data
        with (
            patch(
                "unified_trading_library.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_gcs_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._POOL_DISCOVERY_LOCAL_DIR",
                MagicMock(),
            ),
        ):
            _save_discovered_pools("raydium", ["new_pool"])
        mock_storage.upload_bytes.assert_called_once()


class TestDiscoverProgramPoolAccounts:
    @pytest.mark.asyncio
    async def test_returns_cached_pools(self) -> None:
        with patch(
            "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
            return_value=["cached_pool1", "cached_pool2"],
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == ["cached_pool1", "cached_pool2"]

    @pytest.mark.asyncio
    async def test_rpc_discovery_success(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {"pubkey": "pool1", "account": {}},
                    {"pubkey": "pool2", "account": {}},
                ],
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
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
                return_value=[],
            ),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._save_discovered_pools",
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_solana_rpc_url",
                return_value="https://test.rpc",
            ),
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == ["pool1", "pool2"]

    @pytest.mark.asyncio
    async def test_rpc_http_error(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
                return_value=[],
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_solana_rpc_url",
                return_value="https://test.rpc",
            ),
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == []

    @pytest.mark.asyncio
    async def test_rpc_network_error(self) -> None:
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(side_effect=aiohttp.ClientError("fail"))
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
                return_value=[],
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_solana_rpc_url",
                return_value="https://test.rpc",
            ),
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == []

    @pytest.mark.asyncio
    async def test_rpc_error_in_response(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"error": {"code": -32600}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
                return_value=[],
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_solana_rpc_url",
                return_value="https://test.rpc",
            ),
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == []

    @pytest.mark.asyncio
    async def test_unexpected_result_type(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"result": "not-a-list"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._load_discovered_pools",
                return_value=[],
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils._get_solana_rpc_url",
                return_value="https://test.rpc",
            ),
        ):
            result = await discover_program_pool_accounts("ProgramID", "raydium", 752)
        assert result == []
