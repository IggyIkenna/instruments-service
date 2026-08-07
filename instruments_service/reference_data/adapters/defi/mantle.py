"""Mantle reference data adapter — instrument discovery for the mETH LST.

Discovers the Mantle Liquid Staking Protocol token (mETH) on Ethereum.

References:
- https://www.mantle.xyz/meth
- mETH contract: see ``_METH_ADDRESS`` below (Etherscan-verified; same
  address MTDS's ``lst_rates_handler.py`` / ``_instruments_metadata.py`` use).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "MANTLE") == 2023-12-04, mETH LSP Permissionless Mode launch).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Mantle mETH LSP Permissionless Mode launch (2023-12-04) —
# mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "MANTLE")].
_MANTLE_DEPLOY_DATE = datetime(2023, 12, 4, tzinfo=UTC)

# mETH token address on Ethereum (18 decimals).
_METH_ADDRESS = "0xe3cBd06D7dadB3F4e6557bAb7EdD924CD1489E8f"  # DERIVED from ethereum etherscan
_METH_DECIMALS = 18

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "METH",
        "contract_address": _METH_ADDRESS,
        "underlying": "ETH",
    },
]


class MantleReferenceDataAdapter(BaseReferenceDataAdapter):
    """Mantle reference data: mETH LST token discovery.

    Mantle's Liquid Staking Protocol launched its mETH token on Ethereum
    2023-12-04. mETH is a yield-bearing token whose exchange rate
    (``mETHToETH()``) vs ETH appreciates as staking rewards accrue.
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
        return f"MANTLE-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Mantle LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"MANTLE-{self._chain}"

        for token in _LST_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = build_instrument_id(venue_tag, InstrumentType.LST, symbol, passthrough=True)

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
                    available_from_datetime=_MANTLE_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_METH_DECIMALS,
                )
            )

        logger.info("Mantle: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Mantle does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Mantle mETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Mantle mETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Mantle OHLCV not supported via reference data")
