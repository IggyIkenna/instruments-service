"""Radiant Capital reference data adapter — omnichain LayerZero lending.

Discovers Radiant V2 lending markets across the chains where it has
canonical deployments (Arbitrum primary, BSC, Ethereum). Markets are
returned as InstrumentRecord with instrument_type=LENDING.

Reference: https://radiant.capital/
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

_DEFAULT_CHAIN = "ARBITRUM"

# Curated Radiant V2 markets per chain. Underlying token addresses are
# the rTokens / variableDebtTokens; the canonical lending instrument is
# keyed by the (collateral, borrow) pair against the LendingPool.
_MVP_MARKETS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ARBITRUM": [
        # Arbitrum is the primary deployment.
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1",  # rWETH
        },
        {
            "collateral_asset": "WBTC",
            "borrow_asset": "USDC",
            "vault_address": "0x727354712BDFcd8596A3852Fd2065b3C34F4F770",  # rWBTC
        },
        {
            "collateral_asset": "ARB",
            "borrow_asset": "USDC",
            "vault_address": "0x42C248D137512907048021B30d9dA17f48B5b7B2",  # rARB
        },
    ],
    "BSC": [
        {
            "collateral_asset": "WBNB",
            "borrow_asset": "USDC",
            "vault_address": "0x468Cd12aa9e9fe4301DB146B0f7037831B52382d",  # rWBNB
        },
        {
            "collateral_asset": "BTCB",
            "borrow_asset": "USDC",
            "vault_address": "0xb14C36BfFc35B61D1F3DA9E7d5a0f6F42b2A7D0F",  # rBTCB
        },
    ],
    "ETHEREUM": [
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0x0dF5dfd95966753f01cb80E76dc20EA958238C46",  # rWETH-eth
        },
    ],
}


class RadiantReferenceDataAdapter(BaseReferenceDataAdapter):
    """Radiant V2 reference data: lending market discovery from curated rTokens."""

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
        return "radiant"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        if instrument_type not in (None, "lending_market"):
            return []

        markets = _MVP_MARKETS_BY_CHAIN.get(self._chain, [])
        if not markets:
            logger.info("Radiant: no curated markets for chain %s", self._chain)
            return []

        venue_tag = f"RADIANT-{self._chain}"
        floor_date = get_protocol_floor_date("radiant", self._chain)
        vault_addresses = [m["vault_address"] for m in markets]
        creation_ts_map = await batch_resolve_evm_creation_timestamps(
            vault_addresses,
            self._chain,
        )

        results: list[InstrumentRecord] = []
        for market in markets:
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
                    pool_address=address,
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
            "Radiant: fetched %d lending market instruments on %s",
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
        raise NotImplementedError("Radiant does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Radiant lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Radiant lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Radiant OHLCV not supported via reference data")
