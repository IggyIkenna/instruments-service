"""Tests for the DeFi on-chain removal probe (Option B truth-gate).

Credential-free: all RPC + GCS calls are mocked. Verifies the conservative contract
(only a POSITIVE eth_getCode-absent stamps a removal; every uncertainty stays live)
and the side-artifact I/O.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine
from datetime import UTC, datetime
from unittest.mock import patch

import pandas as pd

from instruments_service.oracle import defi_removal_probe as probe

_EVM = "0x" + "a" * 40
_EVM2 = "0x" + "b" * 40
_AS_OF = datetime(2026, 7, 20, tzinfo=UTC)


class _FakeStorage:
    def __init__(self, blobs: dict[str, bytes] | None = None) -> None:
        self.blobs: dict[str, bytes] = dict(blobs or {})

    def blob_exists(self, bucket: str, blob: str) -> bool:
        return blob in self.blobs

    def download_bytes(self, bucket: str, blob: str) -> bytes:
        if blob not in self.blobs:
            raise FileNotFoundError(blob)
        return self.blobs[blob]

    def upload_bytes(self, bucket: str, blob: str, data: bytes, **_: object) -> None:
        self.blobs[blob] = data


def test_is_evm_address() -> None:
    assert probe._is_evm_address(_EVM)
    assert not probe._is_evm_address("0xshort")
    assert not probe._is_evm_address("0x" + "z" * 40)  # non-hex
    assert not probe._is_evm_address("So1anaBase58Address")
    assert not probe._is_evm_address("")


def test_probe_target_precedence_and_skips() -> None:
    # pool_address wins
    r = pd.Series({"chain": "ARBITRUM", "pool_address": _EVM, "raw_symbol": _EVM2, "instrument_id": "x"})
    assert probe.probe_target(r) == ("ARBITRUM", _EVM)
    # falls back to raw_symbol, then instrument_id
    assert probe.probe_target(pd.Series({"chain": "BASE", "pool_address": "", "raw_symbol": _EVM2})) == ("BASE", _EVM2)
    assert probe.probe_target(pd.Series({"chain": "BASE", "instrument_id": _EVM})) == ("BASE", _EVM)
    # blank chain → None; no evm address → None
    assert probe.probe_target(pd.Series({"chain": "", "pool_address": _EVM})) is None
    assert probe.probe_target(pd.Series({"chain": "SOLANA", "raw_symbol": "So1anaBase58"})) is None


def _run(coro: Coroutine[object, object, object]) -> object:
    return asyncio.run(coro)


def test_probe_removal_contract_still_exists_returns_none() -> None:
    with (
        patch.object(probe, "_resolve_rpc_url", return_value="http://rpc"),
        patch.object(probe, "_get_latest_block", return_value=100),
        patch.object(probe, "_get_code_at_block", return_value=True),  # has code = exists
    ):
        rec = _run(probe.probe_removal("ARBITRUM", _EVM, session=object(), alchemy_key="k", as_of=_AS_OF))
    assert rec is None  # still live → Option A stays


def test_probe_removal_contract_gone_records_removal() -> None:
    with (
        patch.object(probe, "_resolve_rpc_url", return_value="http://rpc"),
        patch.object(probe, "_get_latest_block", return_value=555),
        patch.object(probe, "_get_code_at_block", return_value=False),  # no code = gone
    ):
        rec = _run(probe.probe_removal("ARBITRUM", _EVM, session=object(), alchemy_key="k", as_of=_AS_OF))
    assert isinstance(rec, probe.RemovalRecord)
    assert rec.address == _EVM
    assert rec.delisted_at == "2026-07-20"
    assert rec.probe_block == 555
    assert rec.probe_kind == probe.PROBE_KIND_EVM


def test_probe_removal_unresolvable_url_stays_live() -> None:
    with patch.object(probe, "_resolve_rpc_url", return_value=None):  # non-EVM / unknown chain
        rec = _run(probe.probe_removal("SOLANA", _EVM, session=object(), alchemy_key="k", as_of=_AS_OF))
    assert rec is None  # cannot determine → never fabricate a removal


def test_probe_removal_rpc_error_stays_live() -> None:
    async def _boom(*_: object, **__: object) -> int:
        raise TimeoutError("rpc down")

    with (
        patch.object(probe, "_resolve_rpc_url", return_value="http://rpc"),
        patch.object(probe, "_get_latest_block", side_effect=_boom),
    ):
        rec = _run(probe.probe_removal("ARBITRUM", _EVM, session=object(), alchemy_key="k", as_of=_AS_OF))
    assert rec is None  # error → conservative, stays live


def test_load_removals_and_delisted_at_map() -> None:
    payload = {
        "schema": 1,
        "removals": [
            {
                "canonical_id": "UNISWAP_V3:POOL:X",
                "chain": "ARBITRUM",
                "address": _EVM,
                "delisted_at": "2026-07-20",
                "probe_block": 5,
                "probe_source": "alchemy_rpc",
                "probe_kind": "evm",
            },
        ],
    }
    fake = _FakeStorage({probe.GCS_REMOVALS_BLOB: json.dumps(payload).encode()})
    with patch("unified_trading_library.get_storage_client", return_value=fake):
        m = probe.load_removals(bucket="b")
        dm = probe.load_removal_delisted_at_map(bucket="b")
    # keyed by BOTH canonical_id and address (lower)
    assert m["uniswap_v3:pool:x"].delisted_at == "2026-07-20"
    assert m[_EVM.lower()].address == _EVM
    assert dm["uniswap_v3:pool:x"] == "2026-07-20"
    assert dm[_EVM.lower()] == "2026-07-20"


def test_load_removals_missing_artifact_is_empty() -> None:
    with patch("unified_trading_library.get_storage_client", return_value=_FakeStorage()):
        assert probe.load_removals(bucket="b") == {}
        assert probe.load_removal_delisted_at_map(bucket="b") == {}


def test_write_removals_merges_with_existing() -> None:
    existing = {
        "schema": 1,
        "removals": [
            {
                "canonical_id": "OLD",
                "chain": "BASE",
                "address": _EVM2,
                "delisted_at": "2026-06-01",
                "probe_block": 1,
                "probe_source": "alchemy_rpc",
                "probe_kind": "evm",
            }
        ],
    }
    fake = _FakeStorage({probe.GCS_REMOVALS_BLOB: json.dumps(existing).encode()})
    new = [probe.RemovalRecord("NEW", "ARBITRUM", _EVM, "2026-07-20", 9, "alchemy_rpc", "evm")]
    with patch("unified_trading_library.get_storage_client", return_value=fake):
        total = probe.write_removals(new, bucket="b", merge=True)
    assert total == 2  # merged: OLD kept + NEW added
    written = json.loads(fake.blobs[probe.GCS_REMOVALS_BLOB].decode())
    addrs = {r["address"] for r in written["removals"]}
    assert addrs == {_EVM, _EVM2}


def test_probe_catalogue_removals_only_probes_live_evm_rows() -> None:
    catalogue = pd.DataFrame(
        [
            {
                "instrument_id": "live-gone",
                "chain": "ARBITRUM",
                "pool_address": _EVM,
                "raw_symbol": _EVM,
                "available_to": None,
            },
            {
                "instrument_id": "already-closed",
                "chain": "ARBITRUM",
                "pool_address": _EVM2,
                "raw_symbol": _EVM2,
                "available_to": "2026-05-01",
            },
            {
                "instrument_id": "non-evm",
                "chain": "SOLANA",
                "pool_address": "",
                "raw_symbol": "Base58",
                "available_to": None,
            },
        ]
    )

    async def _no_code(*_: object, **__: object) -> bool:
        return False  # everything probed comes back gone

    async def _latest(*_: object, **__: object) -> int:
        return 42

    class _S:
        async def close(self) -> None:
            return None

    with (
        patch.object(probe, "_make_session", return_value=_S()),
        patch.object(probe, "_resolve_rpc_url", return_value="http://rpc"),
        patch.object(probe, "_get_latest_block", side_effect=_latest),
        patch.object(probe, "_get_code_at_block", side_effect=_no_code),
    ):
        removals = _run(probe.probe_catalogue_removals(catalogue, as_of=_AS_OF, concurrency=2))
    # only the LIVE, EVM-addressed row is probed → exactly one removal (already-closed + non-evm skipped)
    assert isinstance(removals, list)
    assert len(removals) == 1
    assert removals[0].canonical_id == "live-gone"
