"""Puffer Finance reference data adapter — instrument discovery for the pufETH LRT.

Discovers the Puffer Finance liquid restaking token (pufETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="YIELD_BEARING".

References:
- https://www.puffer.fi/
- pufETH contract: https://etherscan.io/token/0xD9A442856C234a39a81a089C06451EBAa4306a72
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "PUFFER") == 2024-05-09).
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

# Puffer Finance Ethereum mainnet GA (2024-05-09) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "PUFFER")].
_PUFFER_DEPLOY_DATE = datetime(2024, 5, 9, tzinfo=UTC)

# pufETH token address on Ethereum (18 decimals).
_PUFETH_ADDRESS = "0xD9A442856C234a39a81a089C06451EBAa4306a72"
_PUFETH_DECIMALS = 18

_LRT_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "PUFETH",
        "contract_address": _PUFETH_ADDRESS,
        "underlying": "ETH",
    },
]


class PufferReferenceDataAdapter(BaseReferenceDataAdapter):
    """Puffer Finance reference data: pufETH LRT token discovery.

    Puffer Finance launched on Ethereum mainnet 2024-05-09. pufETH is a
    yield-bearing liquid restaking token built on EigenLayer; its exchange
    rate vs ETH appreciates as restaking rewards (EigenLayer AVS + native
    staking) accrue.
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
        return "puffer"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Puffer LRT tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"PUFFER-{self._chain}"

        for token in _LRT_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:LST:{symbol}",
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.YIELD_BEARING,
                    base_asset=underlying,
                    quote_asset="",
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=underlying,
                    available_from_datetime=_PUFFER_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_PUFETH_DECIMALS,
                )
            )

        logger.info("Puffer: fetched %d LRT instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key.endswith(f":{symbol}"):
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Puffer does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Puffer pufETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Puffer pufETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Puffer OHLCV not supported via reference data")
