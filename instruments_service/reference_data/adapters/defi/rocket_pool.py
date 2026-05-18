"""Rocket Pool reference data adapter — instrument discovery for the rETH LST.

Discovers the Rocket Pool liquid staking token (rETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="YIELD_BEARING".

References:
- https://rocketpool.net/
- rETH contract: https://etherscan.io/token/0xae78736Cd615f374D3085123A210448E74Fc6393
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "ROCKETPOOL") == 2021-11-08 — Rocket Pool mainnet GA).
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

# Rocket Pool Ethereum mainnet GA (2021-11-08) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "ROCKETPOOL")].
_ROCKET_POOL_DEPLOY_DATE = datetime(2021, 11, 8, tzinfo=UTC)

# rETH token address on Ethereum (18 decimals).
_RETH_ADDRESS = "0xae78736Cd615f374D3085123A210448E74Fc6393"
_RETH_DECIMALS = 18

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "RETH",
        "contract_address": _RETH_ADDRESS,
        "underlying": "ETH",
    },
]


class RocketPoolReferenceDataAdapter(BaseReferenceDataAdapter):
    """Rocket Pool reference data: rETH LST token discovery.

    Rocket Pool launched on Ethereum mainnet 2021-11-08. rETH is a yield-bearing
    liquid staking token backed by a decentralised set of node operators; its
    exchange rate vs ETH appreciates as staking rewards accrue.
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
        return "rocket_pool"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Rocket Pool LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"ROCKETPOOL-{self._chain}"

        for token in _LST_TOKENS:
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
                    available_from_datetime=_ROCKET_POOL_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_RETH_DECIMALS,
                )
            )

        logger.info("Rocket Pool: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Rocket Pool does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Rocket Pool rETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Rocket Pool rETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Rocket Pool OHLCV not supported via reference data")
