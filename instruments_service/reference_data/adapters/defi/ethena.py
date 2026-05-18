"""Ethena reference data adapter — instrument discovery for yield-bearing sUSDe.

Ethena Protocol: Synthetic dollar (USDe) and staked USDe (sUSDe).
sUSDe is a single yield-bearing instrument returned with instrument_type="YIELD_BEARING".

Reference: https://ethena.fi/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Ethena Ethereum mainnet deployment date (2024-02-19).
_ETHENA_DEPLOY_DATE = datetime(2024, 2, 19, tzinfo=UTC)

# Ethena token addresses on Ethereum
_SUSDE_ADDRESS = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
_USDE_ADDRESS = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"


class EthenaReferenceDataAdapter(BaseReferenceDataAdapter):
    """Ethena reference data: sUSDe yield-bearing instrument discovery.

    Ethena has a single discoverable instrument — sUSDe (staked USDe).
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
        """Return the venue identifier."""
        return "ethena"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return sUSDe as a yield-bearing instrument."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        venue_tag = f"ETHENA-{self._chain}"

        results: list[InstrumentRecord] = [
            InstrumentRecord(
                instrument_key=f"{venue_tag}:YIELD_BEARING:sUSDe",
                venue=venue_tag,
                raw_symbol=_SUSDE_ADDRESS,
                base_asset_contract_address=_SUSDE_ADDRESS,
                instrument_type=InstrumentType.YIELD_BEARING,
                base_asset="sUSDe",
                quote_asset="",
                tick_size=Decimal("0.000001"),
                min_size=Decimal("0.000001"),
                contract_size=Decimal("1"),
                expiry=None,
                strike=None,
                option_type=None,
                status=InstrumentStatus.ACTIVE,
                underlying="USDe",
                available_from_datetime=_ETHENA_DEPLOY_DATE,
            )
        ]

        logger.info("Ethena: fetched %d yield-bearing instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Ethena does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Ethena instruments have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Ethena instruments have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Ethena OHLCV not supported via reference data")
