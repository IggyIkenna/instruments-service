"""Fluid reference data adapter — instrument discovery via curated markets.

Discovers Fluid (Instadapp) lending markets on Ethereum. Markets are returned
as InstrumentRecord with instrument_type="lending_market".

Fluid markets are curated (high-liquidity vaults with known addresses).
Reference: https://fluid.instadapp.io/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Fluid (Instadapp) Ethereum mainnet deployment date (2024-03-01).
_FLUID_DEPLOY_DATE = datetime(2024, 3, 1, tzinfo=UTC)

# Curated Fluid high-liquidity vaults (same as UMI adapter)
_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "ETH",
        "borrow_asset": "USDC",
        "vault_address": "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C",
    },
    {
        "collateral_asset": "ETH",
        "borrow_asset": "USDT",
        "vault_address": "0xbEC491FeF7B4f666b270F9D5E5C3f443cBf20991",
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "ETH",
        "vault_address": "0xA0F83Fc5885cEBc0420ce7C7b139Adc80c4F4D91",
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "USDC",
        "vault_address": "0x51197586F6A9e2571868b6ffaef308f3bdfEd3aE",
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "USDT",
        "vault_address": "0x1c2bB46f36561bc4F05A94BD50916496aa501078",
    },
    {
        "collateral_asset": "WEETH",
        "borrow_asset": "WSTETH",
        "vault_address": "0x40D9b8417E6E1DcD358f04E3328bCEd061018A82",
    },
]


class FluidReferenceDataAdapter(BaseReferenceDataAdapter):
    """Fluid reference data: lending market discovery from curated vault list.

    Fluid launched October 2024 on Ethereum mainnet.
    """

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
        return "fluid"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch curated Fluid lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        venue_tag = f"FLUID-{self._chain}"

        for market in _MVP_MARKETS:
            collateral = market["collateral_asset"]
            borrow = market["borrow_asset"]
            address = market["vault_address"]
            symbol = f"{collateral}-{borrow}"
            instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}"

            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    venue=venue_tag,
                    symbol=symbol,
                    raw_symbol=address,
                    instrument_type="lending_market",
                    base_asset=collateral,
                    quote_asset=borrow,
                    tick_size=Decimal("0.000001"),
                    lot_size=Decimal("0.000001"),
                    min_order_size=Decimal("0"),
                    contract_size=Decimal("1"),
                    settlement_asset=borrow,
                    expiry=None,
                    strike=None,
                    option_type=None,
                    is_active=True,
                    updated_at=now,
                    available_since=_FLUID_DEPLOY_DATE,
                )
            )

        logger.info("Fluid: fetched %d lending market instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Fluid does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Fluid lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Fluid lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Fluid OHLCV not supported via reference data")
