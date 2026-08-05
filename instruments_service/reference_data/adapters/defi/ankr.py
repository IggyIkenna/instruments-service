"""Ankr reference data adapter — instrument discovery for the ankrETH LST.

Discovers the Ankr liquid staking token (ankrETH) on Ethereum.

References:
- https://www.ankr.com/staking/
- ankrETH contract: see ``_ANKRETH_ADDRESS`` below (Etherscan-verified; same
  address MTDS's ``lst_rates_handler.py`` / ``_instruments_metadata.py`` use).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "ANKR") == 2020-12-01).
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

# Ankr ankrETH first-LST launch (2020-12-01) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "ANKR")].
_ANKR_DEPLOY_DATE = datetime(2020, 12, 1, tzinfo=UTC)

# ankrETH token address on Ethereum (18 decimals).
_ANKRETH_ADDRESS = "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb"  # DERIVED 2020-10-09 from ethereum etherscan
_ANKRETH_DECIMALS = 18

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "ANKRETH",
        "contract_address": _ANKRETH_ADDRESS,
        "underlying": "ETH",
    },
]


class AnkrReferenceDataAdapter(BaseReferenceDataAdapter):
    """Ankr reference data: ankrETH LST token discovery.

    Ankr launched its ankrETH liquid staking token on Ethereum 2020-12-01,
    one of the earliest LSTs alongside Lido stETH. ankrETH is a yield-bearing
    token whose exchange rate (``ratio()``) vs ETH appreciates as staking
    rewards accrue.
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
        return f"ANKR-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Ankr LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"ANKR-{self._chain}"

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
                    available_from_datetime=_ANKR_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_ANKRETH_DECIMALS,
                )
            )

        logger.info("Ankr: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Ankr does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Ankr ankrETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Ankr ankrETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Ankr OHLCV not supported via reference data")
