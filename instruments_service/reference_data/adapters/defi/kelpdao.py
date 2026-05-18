"""KelpDAO reference data adapter — instrument discovery for the rsETH LRT.

Discovers the KelpDAO liquid restaking token (rsETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="YIELD_BEARING".

References:
- https://www.kelpdao.xyz/
- rsETH contract: https://etherscan.io/token/0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "KELPDAO") == 2023-11-09).
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

# KelpDAO Ethereum mainnet GA (2023-11-09) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "KELPDAO")].
_KELPDAO_DEPLOY_DATE = datetime(2023, 11, 9, tzinfo=UTC)

# rsETH token address on Ethereum (18 decimals).
_RSETH_ADDRESS = "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7"
_RSETH_DECIMALS = 18

_LRT_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "RSETH",
        "contract_address": _RSETH_ADDRESS,
        "underlying": "ETH",
    },
]


class KelpDaoReferenceDataAdapter(BaseReferenceDataAdapter):
    """KelpDAO reference data: rsETH LRT token discovery.

    KelpDAO launched on Ethereum mainnet 2023-11-09. rsETH is a yield-bearing
    liquid restaking token; its exchange rate vs ETH appreciates as restaking
    rewards (EigenLayer / Symbiotic AVS + native staking) accrue.
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
        return "kelpdao"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return KelpDAO LRT tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"KELPDAO-{self._chain}"

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
                    available_from_datetime=_KELPDAO_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_RSETH_DECIMALS,
                )
            )

        logger.info("KelpDAO: fetched %d LRT instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("KelpDAO does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("KelpDAO rsETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("KelpDAO rsETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("KelpDAO OHLCV not supported via reference data")
