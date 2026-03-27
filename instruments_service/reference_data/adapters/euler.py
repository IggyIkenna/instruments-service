"""Euler v2 reference data adapter — instrument discovery via curated markets.

Discovers Euler v2 (Plasma) lending markets on Ethereum. Markets are returned
as InstrumentRecord with instrument_type="lending_market".

Euler v2 markets are curated (high-liquidity vaults with known addresses).
Reference: https://www.euler.finance/
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

# Euler v2 (Plasma) Ethereum mainnet deployment date (2023-12-18).
_EULER_DEPLOY_DATE = datetime(2023, 12, 18, tzinfo=UTC)

# Curated Euler v2 high-liquidity markets (same as UMI adapter)
_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "USDC",
        "borrow_asset": "USDC",
        "market_address": "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9",
    },
    {
        "collateral_asset": "USDT",
        "borrow_asset": "USDT",
        "market_address": "0x313603FA690301b0CaeEf8069c065862f9162162",
    },
]


class EulerReferenceDataAdapter(BaseReferenceDataAdapter):
    """Euler v2 reference data: lending market discovery from curated list.

    Euler v2 launched November 2024. Markets are curated high-liquidity vaults.
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
        return "euler"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch curated Euler v2 lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        venue_tag = f"EULER-{self._chain}"

        for market in _MVP_MARKETS:
            collateral = market["collateral_asset"]
            borrow = market["borrow_asset"]
            address = market["market_address"]
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
                    available_since=_EULER_DEPLOY_DATE,
                )
            )

        logger.info("Euler: fetched %d lending market instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Euler does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Euler lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Euler lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Euler OHLCV not supported via reference data")
