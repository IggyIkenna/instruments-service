"""Unit tests — Solend reference-data adapter (Solana lending reserve discovery).

Mocked ``markets/configs`` payload mirrors the REAL, live public REST API
(verified 2026-07-09 via ``curl``):
    https://api.solend.fi/v1/markets/configs?scope=solend&deployment=production
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.solend import (
    SolendReferenceDataAdapter,
    _classify_solend_error,
    _sanitize_symbol,
)

# Real sample shape (subset of the live "main" market response, 2026-07-09).
_SAMPLE_MARKETS: list[dict[str, object]] = [
    {
        "name": "main",
        "isPrimary": True,
        "address": "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY",
        "reserves": [
            {
                "liquidityToken": {
                    "coingeckoID": "msol",
                    "decimals": 9,
                    "mint": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
                    "name": "Marinade staked SOL (mSOL)",
                    "symbol": "mSOL",
                },
                "address": "CCpirWrgNuBVLdkP2haxLTbD6XqEgaYuVXixbbpxUB6",
                "userBorrowCap": "500000",
                "userSupplyCap": "1500000",
            },
            {
                # Real Solend LP-style reserve — symbol carries a space.
                "liquidityToken": {
                    "coingeckoID": "",
                    "decimals": 9,
                    "mint": "GRJyCEezbZQibAEfBKCRAg5YoTPP2UcRSTC7RfEcuMkV",
                    "name": "SOL-mSOL MLP",
                    "symbol": "SOL-mSOL MLP",
                },
                "address": "LpReserveAddress1111111111111111111111111111",
            },
            {
                # Missing mint — should be skipped entirely.
                "liquidityToken": {"symbol": "BROKEN", "decimals": 6},
                "address": "BrokenReserveAddress1111111111111111111111",
            },
        ],
    },
    {
        "name": "Coin98",
        "isPrimary": False,
        "address": "7tiNvRHSjYDfc6usrWnSNPyuN68xQfKs1ZG2oqtR5F46",
        "reserves": [
            {
                "liquidityToken": {
                    "decimals": 6,
                    "mint": "IgnoredMint111111111111111111111111111111",
                    "symbol": "IGNORE",
                },
                "address": "IgnoredReserveAddress111111111111111111111",
            }
        ],
    },
]

_EXPECTED_DEPLOY_DATE = datetime(2021, 8, 13, tzinfo=UTC)


def test_venue() -> None:
    assert SolendReferenceDataAdapter().venue == "SOLEND-SOLANA"


@pytest.mark.asyncio
async def test_get_instruments_wrong_type_returns_empty() -> None:
    adapter = SolendReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="PERPETUAL") == []


@pytest.mark.asyncio
async def test_get_instruments_only_uses_primary_market() -> None:
    adapter = SolendReferenceDataAdapter()
    with (
        patch.object(adapter, "_get_with_retry", return_value=_SAMPLE_MARKETS),
        patch(
            "instruments_service.reference_data.adapters.defi.solend.batch_resolve_creation_timestamps",
            return_value={},
        ),
    ):
        records = await adapter.get_instruments()

    # Primary market has 3 reserves; one (BROKEN, missing mint) is skipped.
    # Remaining 2 reserves x 2 legs (A_TOKEN + DEBT_TOKEN) = 4 records.
    # The non-primary "Coin98" market's reserve must NOT appear.
    assert len(records) == 4
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "SOLEND-SOLANA"
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _EXPECTED_DEPLOY_DATE
        assert rec.instrument_type in (InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN)
        assert rec.base_asset != "IGNORE"

    msol_records = [r for r in records if r.base_asset == "mSOL"]
    assert len(msol_records) == 2
    a_token = next(r for r in msol_records if r.instrument_type == InstrumentType.A_TOKEN)
    debt_token = next(r for r in msol_records if r.instrument_type == InstrumentType.DEBT_TOKEN)
    assert a_token.instrument_key == "SOLEND-SOLANA:A_TOKEN:AmSOL"
    assert debt_token.instrument_key == "SOLEND-SOLANA:DEBT_TOKEN:DEBTmSOL"
    assert a_token.base_asset_contract_address == "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"
    assert a_token.base_asset_decimals == 9
    assert a_token.pool_address == "CCpirWrgNuBVLdkP2haxLTbD6XqEgaYuVXixbbpxUB6"

    # The space-carrying LP symbol is sanitized in the instrument_key but the
    # original (unsanitized) symbol is preserved in base_asset.
    lp_records = [r for r in records if r.base_asset == "SOL-mSOL MLP"]
    assert len(lp_records) == 2
    lp_a_token = next(r for r in lp_records if r.instrument_type == InstrumentType.A_TOKEN)
    assert lp_a_token.instrument_key == "SOLEND-SOLANA:A_TOKEN:ASOL-mSOLMLP"


@pytest.mark.asyncio
async def test_get_instruments_no_primary_market_returns_empty() -> None:
    adapter = SolendReferenceDataAdapter()
    non_primary_only = [{**m, "isPrimary": False} for m in _SAMPLE_MARKETS]
    with patch.object(adapter, "_get_with_retry", return_value=non_primary_only):
        assert await adapter.get_instruments() == []


@pytest.mark.asyncio
async def test_get_instruments_http_error_raises_connection_error() -> None:
    adapter = SolendReferenceDataAdapter()
    with (
        patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("boom")),
        pytest.raises(ConnectionError),
    ):
        await adapter.get_instruments()


def test_build_reserve_records_skips_missing_fields() -> None:
    assert SolendReferenceDataAdapter._build_reserve_records({}, "SOLEND-SOLANA", _EXPECTED_DEPLOY_DATE) == []
    assert (
        SolendReferenceDataAdapter._build_reserve_records(
            {"liquidityToken": {"symbol": "X"}, "address": "addr"},
            "SOLEND-SOLANA",
            _EXPECTED_DEPLOY_DATE,
        )
        == []
    )


def test_sanitize_symbol_strips_spaces() -> None:
    assert _sanitize_symbol("SOL-bSOL MLP") == "SOL-bSOLMLP"
    assert _sanitize_symbol("mSOL") == "mSOL"


def test_classify_solend_error() -> None:
    assert _classify_solend_error(Exception("msg"), status=429) == "RATE_LIMIT"
    assert _classify_solend_error(Exception("msg"), status=503) == "503"
    assert _classify_solend_error(Exception("msg"), status=500) == "500"
    assert _classify_solend_error(Exception("unknown")) == "UNKNOWN"


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = SolendReferenceDataAdapter()
    single_market = [_SAMPLE_MARKETS[0]]
    with (
        patch.object(adapter, "_get_with_retry", return_value=single_market),
        patch(
            "instruments_service.reference_data.adapters.defi.solend.batch_resolve_creation_timestamps",
            return_value={},
        ),
    ):
        found = await adapter.get_instrument("CCpirWrgNuBVLdkP2haxLTbD6XqEgaYuVXixbbpxUB6")
        missing = await adapter.get_instrument("NOPE")
    assert found is not None
    assert missing is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = SolendReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("SOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("mSOL")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("mSOL")
