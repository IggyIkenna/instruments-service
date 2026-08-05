"""Swell reference data adapter — instrument discovery for the swETH LST.

Discovers the Swell Network liquid staking token (swETH) on Ethereum.

References:
- https://www.swellnetwork.io/
- swETH contract: see ``_SWETH_ADDRESS`` below (Etherscan-verified; same
  address MTDS's ``lst_rates_handler.py`` / ``_instruments_metadata.py`` use).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "SWELL") == 2023-04-25, "Seawolf unleashed" mainnet launch).
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

# Swell swETH mainnet "Seawolf unleashed" launch (2023-04-25) —
# mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "SWELL")].
_SWELL_DEPLOY_DATE = datetime(2023, 4, 25, tzinfo=UTC)

# swETH token address on Ethereum (18 decimals).
_SWETH_ADDRESS = "0xf951E335afb289353dc249e82926178EaC7DEd78"  # DERIVED from ethereum etherscan
_SWETH_DECIMALS = 18

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "SWETH",
        "contract_address": _SWETH_ADDRESS,
        "underlying": "ETH",
    },
]


class SwellReferenceDataAdapter(BaseReferenceDataAdapter):
    """Swell Network reference data: swETH LST token discovery.

    Swell launched its swETH liquid staking token on Ethereum 2023-04-25.
    swETH is a yield-bearing token whose exchange rate
    (``swETHToETHRate()``) vs ETH appreciates as staking rewards accrue.
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
        return f"SWELL-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Swell LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SWELL-{self._chain}"

        for token in _LST_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = f"{venue_tag}:LST:{symbol}"

            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                    # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                    canonical_instrument_id=instrument_key,
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.LST,
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
                    available_from_datetime=_SWELL_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_SWETH_DECIMALS,
                )
            )

        logger.info("Swell: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Swell does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Swell swETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Swell swETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Swell OHLCV not supported via reference data")
