"""Unit tests — MarginFi reference-data adapter (Solana lending Bank discovery).

Mocked bank/token metadata payloads mirror the REAL, live public caches
(verified 2026-07-09 via ``curl``):
    https://storage.googleapis.com/mrgn-public/mrgn-bank-metadata-cache.json
    https://storage.googleapis.com/mrgn-public/mrgn-token-metadata-cache.json
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.marginfi import (
    MarginfiReferenceDataAdapter,
    _classify_marginfi_error,
    _sanitize_symbol,
)

# Real sample entries (subset), shape verified live against the public caches.
_SAMPLE_BANKS: list[dict[str, object]] = [
    {
        "bankAddress": "CCKtUs6Cgwo4aaQUmBPmyoApH2gUDErxNZCAntD6LYGh",
        "tokenAddress": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tokenName": "USD Coin",
        "tokenSymbol": "USDC",
    },
    {
        "bankAddress": "6hS9i46WyTq1KXcoa2Chas2Txh9TJAVr6n1t3tnrE23K",
        "tokenAddress": "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
        "tokenName": "BlazeStake Staked SOL (bSOL)",
        "tokenSymbol": "bSOL",
    },
    {
        # No matching token-cache entry — decimals should resolve to None.
        "bankAddress": "MissingTokenBankAddress11111111111111111111",
        "tokenAddress": "MissingTokenMintAddress111111111111111111111",
        "tokenName": "Unknown",
        "tokenSymbol": "UNKTOK",
    },
]

_SAMPLE_TOKENS: list[dict[str, object]] = [
    {
        "symbol": "USDC",
        "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "chainId": 101,
        "decimals": 6,
        "name": "USD Coin",
    },
    {
        "symbol": "bSOL",
        "address": "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
        "chainId": 101,
        "decimals": 9,
        "name": "BlazeStake Staked SOL (bSOL)",
    },
]

_EXPECTED_DEPLOY_DATE = datetime(2023, 7, 1, tzinfo=UTC)


def test_venue() -> None:
    assert MarginfiReferenceDataAdapter().venue == "MARGINFI-SOLANA"


@pytest.mark.asyncio
async def test_get_instruments_wrong_type_returns_empty() -> None:
    adapter = MarginfiReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="PERPETUAL") == []


@pytest.mark.asyncio
async def test_get_instruments_success_builds_a_token_and_debt_token_pairs() -> None:
    adapter = MarginfiReferenceDataAdapter()
    with (
        patch.object(adapter, "_get_with_retry", side_effect=[_SAMPLE_BANKS, _SAMPLE_TOKENS]),
        patch(
            "instruments_service.reference_data.adapters.defi.marginfi.batch_resolve_creation_timestamps",
            return_value={},
        ),
    ):
        records = await adapter.get_instruments()

    # 2 of the 3 sample banks have a token-cache decimals match x 2 legs
    # (A_TOKEN + DEBT_TOKEN) = 4 records. The 3rd (UNKTOK, no token-cache
    # entry) is honestly skipped rather than fabricating decimals.
    assert len(records) == 4
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "MARGINFI-SOLANA"
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.instrument_type in (InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN)

    usdc_records = [r for r in records if r.base_asset == "USDC"]
    assert len(usdc_records) == 2
    a_token = next(r for r in usdc_records if r.instrument_type == InstrumentType.A_TOKEN)
    debt_token = next(r for r in usdc_records if r.instrument_type == InstrumentType.DEBT_TOKEN)
    assert a_token.instrument_key == "MARGINFI-SOLANA:A_TOKEN:AUSDC"
    assert debt_token.instrument_key == "MARGINFI-SOLANA:DEBT_TOKEN:DEBTUSDC"
    assert a_token.base_asset_contract_address == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    assert a_token.base_asset_decimals == 6
    assert a_token.pool_address == "CCKtUs6Cgwo4aaQUmBPmyoApH2gUDErxNZCAntD6LYGh"
    assert a_token.raw_symbol == "CCKtUs6Cgwo4aaQUmBPmyoApH2gUDErxNZCAntD6LYGh"

    # Bank whose mint has no token-cache match is honestly skipped — never
    # fabricate a decimals value (base_asset_decimals is a required,
    # price-normalisation-critical field for DeFi on-chain instrument types).
    unk_records = [r for r in records if r.base_asset == "UNKTOK"]
    assert unk_records == []


@pytest.mark.asyncio
async def test_get_instruments_empty_bank_cache_returns_empty() -> None:
    adapter = MarginfiReferenceDataAdapter()
    with patch.object(adapter, "_get_with_retry", side_effect=[[], []]):
        assert await adapter.get_instruments() == []


@pytest.mark.asyncio
async def test_get_instruments_http_error_raises_connection_error() -> None:
    adapter = MarginfiReferenceDataAdapter()
    with (
        patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("boom")),
        pytest.raises(ConnectionError),
    ):
        await adapter.get_instruments()


def test_build_bank_records_skips_missing_fields() -> None:
    assert MarginfiReferenceDataAdapter._build_bank_records({}, {}, "MARGINFI-SOLANA", _EXPECTED_DEPLOY_DATE) == []
    assert (
        MarginfiReferenceDataAdapter._build_bank_records(
            {"bankAddress": "x", "tokenAddress": "", "tokenSymbol": "SYM"},
            {},
            "MARGINFI-SOLANA",
            _EXPECTED_DEPLOY_DATE,
        )
        == []
    )


def test_sanitize_symbol_strips_spaces() -> None:
    assert _sanitize_symbol("SOL bSOL LP") == "SOLbSOLLP"
    assert _sanitize_symbol("USDC") == "USDC"


def test_classify_marginfi_error() -> None:
    assert _classify_marginfi_error(Exception("msg"), status=429) == "RATE_LIMIT"
    assert _classify_marginfi_error(Exception("msg"), status=503) == "503"
    assert _classify_marginfi_error(Exception("msg"), status=500) == "500"
    assert _classify_marginfi_error(Exception("unknown")) == "UNKNOWN"


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = MarginfiReferenceDataAdapter()
    # get_instrument() re-fetches via get_instruments() on every call (no
    # cache at this layer) — 2 lookups x 2 fetches (bank + token cache) each.
    with (
        patch.object(
            adapter,
            "_get_with_retry",
            side_effect=[_SAMPLE_BANKS[:1], _SAMPLE_TOKENS[:1], _SAMPLE_BANKS[:1], _SAMPLE_TOKENS[:1]],
        ),
        patch(
            "instruments_service.reference_data.adapters.defi.marginfi.batch_resolve_creation_timestamps",
            return_value={},
        ),
    ):
        found = await adapter.get_instrument("CCKtUs6Cgwo4aaQUmBPmyoApH2gUDErxNZCAntD6LYGh")
        missing = await adapter.get_instrument("NOPE")
    assert found is not None
    assert missing is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = MarginfiReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("USDC")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("USDC")
