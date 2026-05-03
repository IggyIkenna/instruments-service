"""Benqi Liquid Markets reference data adapter — Compound-fork lending on Avalanche.

Discovers Benqi qiToken lending markets. Avalanche-only deployment
(launched 2021-08-19). Markets are curated (top-of-TVL collateral assets).

Reference: https://app.benqi.fi/
"""

import logging
from datetime import datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.evm_creation_resolver import (
    batch_resolve_evm_creation_timestamps,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "AVALANCHE"

_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "AVAX",
        "borrow_asset": "USDC",
        "vault_address": "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c",  # qiAVAX
    },
    {
        "collateral_asset": "BTCB",
        "borrow_asset": "USDC",
        "vault_address": "0x89a415b3D20098E6A6C8f7a59001C67BD3129821",  # qiBTC.b
    },
    {
        "collateral_asset": "WETHE",
        "borrow_asset": "USDC",
        "vault_address": "0x334AD834Cd4481BB02d09615E7c11a00579A7909",  # qiETH
    },
    {
        "collateral_asset": "SAVAX",
        "borrow_asset": "AVAX",
        "vault_address": "0xF362feA9659cf036792c9cb02f8ff8198E21B4cB",  # qisAVAX (LSD recursive)
    },
]


class BenqiReferenceDataAdapter(BaseReferenceDataAdapter):
    """Benqi reference data: lending market discovery from curated qiTokens."""

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()

    @property
    def venue(self) -> str:
        return "benqi"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        if instrument_type not in (None, "lending_market"):
            return []
        if self._chain != "AVALANCHE":
            logger.info("Benqi: no markets — Benqi is AVALANCHE-only (got %s)", self._chain)
            return []

        venue_tag = f"BENQI-{self._chain}"
        floor_date = get_protocol_floor_date("benqi", self._chain)
        vault_addresses = [m["vault_address"] for m in _MVP_MARKETS]
        creation_ts_map = await batch_resolve_evm_creation_timestamps(
            vault_addresses,
            self._chain,
        )

        results: list[InstrumentRecord] = []
        for market in _MVP_MARKETS:
            collateral = market["collateral_asset"]
            borrow = market["borrow_asset"]
            address = market["vault_address"]
            symbol = f"{collateral}-{borrow}"
            instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}"
            available_since = creation_ts_map.get(address, floor_date)

            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.LENDING,
                    base_asset=collateral,
                    quote_asset=borrow,
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=available_since,
                )
            )

        logger.info(
            "Benqi: fetched %d lending market instruments on %s",
            len(results),
            self._chain,
        )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Benqi does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Benqi lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Benqi lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Benqi OHLCV not supported via reference data")
