"""Unit tests — Pendle Finance reference-data adapter (PT/YT/SY discovery).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded snapshot
of currently-active Pendle markets on Ethereum + Arbitrum. Each market emits
THREE records (PT, YT, SY). PT/YT carry maturity datetime; SY does not. Tests
are credential-free and offline.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from instruments_service.reference_data.adapters.defi.pendle import (
    PendleReferenceDataAdapter,
)

# Ethereum wstETH PT token (queried 2026-05-12 from api-v2.pendle.finance) —
# anchors the by-address lookup test.
_ETH_WSTETH_PT_ADDRESS = "0x9ce6478ef45bb1baac69efd8a3ea0ed110a43042"
_ETH_DEPLOY_DATE = datetime(2021, 6, 15, tzinfo=UTC)
_ARB_DEPLOY_DATE = datetime(2024, 1, 26, tzinfo=UTC)
# Earliest curated maturity is 2026-06-25; cutoff used to assert all PT/YT
# expiries land strictly after the snapshot date 2026-05-12.
_SNAPSHOT_DATE = datetime(2026, 5, 12, tzinfo=UTC)


def test_venue() -> None:
    assert PendleReferenceDataAdapter().venue == "pendle"
    assert PendleReferenceDataAdapter(chain="ARBITRUM").venue == "pendle"


@pytest.mark.asyncio
async def test_get_instruments_yields_pt_yt_sy_records() -> None:
    adapter = PendleReferenceDataAdapter()
    records = await adapter.get_instruments()

    # 5 curated Ethereum markets × 3 roles = 15 records.
    assert len(records) == 15

    # Every record's basics.
    for rec in records:
        assert isinstance(rec, InstrumentRecord)
        assert rec.venue == "PENDLE-ETHEREUM"
        assert rec.instrument_type == InstrumentType.YIELD_BEARING
        assert rec.status == InstrumentStatus.ACTIVE
        assert rec.available_from_datetime == _ETH_DEPLOY_DATE
        assert rec.instrument_key.startswith("PENDLE-ETHEREUM:")
        # raw_symbol is the on-chain token address (lowercase hex).
        assert rec.raw_symbol.startswith("0x") and len(rec.raw_symbol) == 42
        assert rec.base_asset_contract_address == rec.raw_symbol

    # At least one of each role present.
    roles = {rec.instrument_key.split(":")[1] for rec in records}
    assert roles == {"PT", "YT", "SY"}

    # Per-role count: every market emits exactly one PT, YT, SY token.
    for role in ("PT", "YT", "SY"):
        role_records = [r for r in records if r.instrument_key.split(":")[1] == role]
        assert len(role_records) == 5, f"expected 5 {role} records, got {len(role_records)}"


@pytest.mark.asyncio
async def test_get_instruments_arbitrum_yields_records() -> None:
    adapter = PendleReferenceDataAdapter(chain="ARBITRUM")
    records = await adapter.get_instruments()

    # 5 curated Arbitrum markets × 3 roles = 15 records.
    assert len(records) == 15
    for rec in records:
        assert rec.venue == "PENDLE-ARBITRUM"
        assert rec.available_from_datetime == _ARB_DEPLOY_DATE
        assert rec.instrument_key.startswith("PENDLE-ARBITRUM:")


@pytest.mark.asyncio
async def test_get_instruments_filters_on_instrument_type() -> None:
    adapter = PendleReferenceDataAdapter()
    assert await adapter.get_instruments(instrument_type="yield_bearing")
    assert await adapter.get_instruments(instrument_type="perpetual") == []
    assert await adapter.get_instruments(instrument_type="option") == []


@pytest.mark.asyncio
async def test_get_instrument_lookup() -> None:
    adapter = PendleReferenceDataAdapter()
    by_addr = await adapter.get_instrument(_ETH_WSTETH_PT_ADDRESS)
    by_symbol = await adapter.get_instrument("PT-wstETH-25JUN2026")
    assert by_addr is not None and by_symbol is not None
    assert by_addr.instrument_key == by_symbol.instrument_key
    assert by_addr.instrument_key == "PENDLE-ETHEREUM:PT:PT-wstETH-25JUN2026"
    # Unknown symbol returns None.
    assert await adapter.get_instrument("NOPE-PT-FAKE") is None


@pytest.mark.asyncio
async def test_unsupported_market_data_methods_raise() -> None:
    adapter = PendleReferenceDataAdapter()
    with pytest.raises(NotImplementedError):
        await adapter.get_options_chain("wstETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_expiry_calendar("wstETH")
    with pytest.raises(NotImplementedError):
        await adapter.get_funding_rate("PT-wstETH-25JUN2026")
    with pytest.raises(NotImplementedError):
        await adapter.get_ohlcv("PT-wstETH-25JUN2026")


@pytest.mark.asyncio
async def test_pt_yt_have_expiry_set() -> None:
    """Pendle-specific: every PT/YT record must carry a future maturity datetime;
    every SY record must have ``expiry=None`` (yield-wrappers are perpetual).
    """
    for chain in ("ETHEREUM", "ARBITRUM"):
        adapter = PendleReferenceDataAdapter(chain=chain)
        records = await adapter.get_instruments()
        assert records, f"no records returned for {chain}"

        for rec in records:
            role = rec.instrument_key.split(":")[1]
            if role == "SY":
                assert rec.expiry is None, f"{chain} {rec.instrument_key} SY must not carry expiry"
                continue
            assert rec.expiry is not None, f"{chain} {rec.instrument_key} {role} missing expiry"
            assert rec.expiry > _SNAPSHOT_DATE, (
                f"{chain} {rec.instrument_key} expiry {rec.expiry.isoformat()} is not after snapshot "
                f"date {_SNAPSHOT_DATE.isoformat()} — markets must be curated to currently-active only"
            )
