"""AAVE oracle reference data adapter — instrument discovery for AAVE V3 Ethereum
on-chain price-oracle reserves.

AAVE V3's price oracle (``AaveOracle.getAssetPrice``) is queryable on-chain for
every listed reserve; this adapter enumerates the reserves VERIFIED live via a
real ``eth_call`` (Phase 0 of ``lst_rate_honest_coverage_2026_07_21.md`` —
journal ``wf_f629fbb4-7da``) as ``SPOT_ASSET`` instruments under the single
``AAVE-ETHEREUM`` venue. This is a SEPARATE, oracle-context registration from
each token's own native-protocol instrument (e.g. ``ETHERFI-ETHEREUM:LST:WEETH``)
— the same on-chain address is also directly queryable via AAVE's oracle, which
is what this adapter's rows represent (mirrors ``chainlink.py``'s "the fetch
happens in MTDS, this adapter only enumerates the reference-data universe"
shape).

**EXCLUDED** (verified REVERTS, not real reserves — would seed permanent-false-RED
if registered): osETH (``getAssetPrice`` reverts, aToken=0x0) and
ezETH@``0x2416092f...`` (reverts; only the verified-live ezETH address below is
registered).

Enumeration is STATIC (the curated ``_AAVE_RESERVES`` registry below) — no
network call is made at reference-data discovery time, exactly like the
Chainlink/Pyth oracle adapters. The actual oracle prices are fetched by MTDS's
``oracle_prices`` handler at runtime via the ``getAssetPrice`` RPC selector
(lifted from ``aave_positions.py::_fetch_rpc_oracle_prices`` — dormant, not
missing).

Reference: https://docs.aave.com/developers/core-contracts/aaveoracle
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_api_contracts.registry import get_protocol_launch_date

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import build_spot_asset_record

logger = logging.getLogger(__name__)

_VENUE = "AAVE-ETHEREUM"
_CHAIN = "ETHEREUM"
_AAVE_V3_PROTOCOL = "AAVE_V3"
_DECIMALS = 18

# Phase-0-VERIFIED reserves (real eth_call getAssetPrice returns, 2026-07-21 —
# wf_f629fbb4-7da). symbol is lower-cased deliberately (instrument_id=<symbol_lower>
# per lst_rate_honest_coverage_2026_07_21.md's Phase-1 spec); base_asset carries
# the human-readable casing. protocol names the reserve's OWN issuing protocol,
# used to compute a conservative available_from floor (a reserve cannot be
# queryable before both AAVE V3 Ethereum AND the token itself existed).
_AAVE_RESERVES: tuple[dict[str, str], ...] = (
    {
        "symbol": "wsteth",
        "base_asset": "wstETH",
        "contract_address": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        "protocol": "LIDO",
    },
    {
        "symbol": "weeth",
        "base_asset": "weETH",
        "contract_address": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "protocol": "ETHERFI",
    },
    {
        "symbol": "reth",
        "base_asset": "rETH",
        "contract_address": "0xae78736Cd615f374D3085123A210448E74Fc6393",
        "protocol": "ROCKETPOOL",
    },
    {
        "symbol": "cbeth",
        "base_asset": "cbETH",
        "contract_address": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",
        "protocol": "COINBASE",
    },
    {
        "symbol": "rseth",
        "base_asset": "rsETH",
        "contract_address": "0xA1290d69c65A6Fe4DF752f95823fAe25cB99e5A7",
        "protocol": "KELPDAO",
    },
    {
        # Verified-live address ONLY — 0x2416092f... reverts and is deliberately
        # excluded (see module docstring).
        "symbol": "ezeth",
        "base_asset": "ezETH",
        "contract_address": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
        "protocol": "RENZO",
    },
)


def _reserve_available_from(protocol: str) -> datetime:
    """Conservative floor: max(AAVE V3 Ethereum launch, the reserve's own protocol launch).

    A reserve cannot be AAVE-oracle-queryable before AAVE V3 itself existed on
    Ethereum, nor before its own token existed — composing both per
    ``get_protocol_launch_date``'s documented usage avoids seeding a
    permanent-false-RED floor that is too early for either fact.
    """
    aave_v3_launch = get_protocol_launch_date(_CHAIN, _AAVE_V3_PROTOCOL) or "1970-01-01"
    own_launch = get_protocol_launch_date(_CHAIN, protocol) or aave_v3_launch
    floor = max(aave_v3_launch, own_launch)
    return datetime.fromisoformat(floor).replace(tzinfo=UTC)


class AaveOracleReferenceDataAdapter(BaseReferenceDataAdapter):
    """AAVE V3 Ethereum on-chain oracle reserves: static reserve enumeration.

    Enumerates the Phase-0-verified ``_AAVE_RESERVES`` registry as
    ``SPOT_ASSET`` instruments under the single ``AAVE-ETHEREUM`` venue. The
    actual oracle prices are fetched by MTDS at runtime — this adapter only
    exposes the reference-data universe.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)

    @property
    def venue(self) -> str:
        """Return the venue identifier (``AAVE-ETHEREUM``)."""
        return _VENUE

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return AAVE oracle reserve instruments as SPOT_ASSET records."""
        if instrument_type not in (None, InstrumentType.SPOT_ASSET, "spot"):
            logger.info("AAVE oracle only supports SPOT_ASSET instruments; requested %s", instrument_type)
            return []

        results: list[InstrumentRecord] = []
        for reserve in _AAVE_RESERVES:
            record = build_spot_asset_record(
                venue=_VENUE,
                symbol=reserve["symbol"],
                contract_address=reserve["contract_address"],
                decimals=_DECIMALS,
                available_from_datetime=_reserve_available_from(reserve["protocol"]),
                base_asset=reserve["base_asset"],
            )
            if record is not None:
                results.append(record)

        logger.info("AAVE oracle: built %d reserve instruments on %s", len(results), _CHAIN)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single reserve instrument by contract address or symbol."""
        instruments = await self.get_instruments()
        sym_lower = symbol.lower()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset.lower() == sym_lower:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("AAVE oracle does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("AAVE oracle reserves have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("AAVE oracle funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv; not supported for this venue."""
        raise NotImplementedError("AAVE oracle OHLCV not supported via reference data")
