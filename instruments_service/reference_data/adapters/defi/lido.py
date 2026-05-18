"""Lido reference data adapter — instrument discovery for LST tokens.

Discovers Lido liquid staking tokens (stETH, wstETH) on Ethereum.
Tokens are returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Reference: https://lido.fi/
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

# Lido Ethereum mainnet deployment date (2020-12-18).
_LIDO_DEPLOY_DATE = datetime(2020, 12, 18, tzinfo=UTC)

# Lido token addresses on Ethereum
_STETH_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
_WSTETH_ADDRESS = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "STETH",
        "contract_address": _STETH_ADDRESS,
        "underlying": "ETH",
    },
    {
        "symbol": "WSTETH",
        "contract_address": _WSTETH_ADDRESS,
        "underlying": "ETH",
    },
]


class LidoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Lido reference data: LST token discovery (stETH, wstETH).

    Lido launched December 2020. Both stETH and wstETH are yield-bearing
    liquid staking tokens backed by staked ETH.
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
        return "lido"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Lido LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"LIDO-{self._chain}"

        for token in _LST_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:LST:{symbol}",
                    venue=venue_tag,
                    raw_symbol=address,
                    base_asset_contract_address=address,
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
                    available_from_datetime=_LIDO_DEPLOY_DATE,
                )
            )

        logger.info("Lido: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Lido does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Lido LST tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Lido LST tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Lido OHLCV not supported via reference data")
