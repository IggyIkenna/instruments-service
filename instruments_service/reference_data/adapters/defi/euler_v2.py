"""Euler V2 reference data adapter — instrument discovery via curated markets.

Discovers Euler V2 EVK lending vaults (re-launched 2024-08-29 after the
2023 V1 incident + full restitution). Markets are returned as
InstrumentRecord with instrument_type=LENDING.

Markets are curated (high-liquidity vaults with known addresses).
Reference: https://app.euler.finance/
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

_DEFAULT_CHAIN = "ETHEREUM"

# Curated Euler V2 EVK vaults (top-of-TVL by collateral / borrow asset).
_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "WETH",
        "borrow_asset": "USDC",
        "vault_address": "0xD8b27CF359b7D15710a5BE299AF6e7Bf904984C2",
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "WETH",
        "vault_address": "0xbC4B4AC47582c3E38Ce5940B80Da65401F4628f1",
    },
    {
        "collateral_asset": "USDC",
        "borrow_asset": "USDT",
        "vault_address": "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9",
    },
]


class EulerV2ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Euler V2 reference data: lending market discovery from curated vaults."""

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
        return "euler_v2"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        if instrument_type not in (None, "lending_market"):
            return []

        venue_tag = f"EULER_V2-{self._chain}"
        floor_date = get_protocol_floor_date("euler_v2", self._chain)
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
            "Euler V2: fetched %d lending market instruments on %s",
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
        raise NotImplementedError("Euler V2 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Euler V2 lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Euler V2 lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Euler V2 OHLCV not supported via reference data")
