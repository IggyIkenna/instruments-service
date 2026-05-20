"""EtherFi reference data adapter — instrument discovery for LST tokens.

Discovers EtherFi liquid staking token (weETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Reference: https://www.ether.fi/
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

# EtherFi Ethereum mainnet deployment date (2023-11-01).
_ETHERFI_DEPLOY_DATE = datetime(2023, 11, 1, tzinfo=UTC)

# EtherFi token address on Ethereum
_WEETH_ADDRESS = "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"

# MTDS handlers derive the APY/rate fetch URL from this field; hardcoding is banned.
_ETHERFI_APY_URL_TEMPLATE = "https://api.etherfi.id/weeth/apr"

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "WEETH",
        "contract_address": _WEETH_ADDRESS,
        "underlying": "ETH",
    },
]


class EtherFiReferenceDataAdapter(BaseReferenceDataAdapter):
    """EtherFi reference data: weETH LST token discovery.

    EtherFi launched in 2024. weETH is a yield-bearing liquid restaking token.
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
        return "etherfi"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return EtherFi weETH as a yield-bearing instrument."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"ETHERFI-{self._chain}"

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
                    available_from_datetime=_ETHERFI_DEPLOY_DATE,
                    base_asset_decimals=18,
                    source_archive_url_template=_ETHERFI_APY_URL_TEMPLATE,
                )
            )

        logger.info("EtherFi: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("EtherFi does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("EtherFi LST tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("EtherFi LST tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("EtherFi OHLCV not supported via reference data")
