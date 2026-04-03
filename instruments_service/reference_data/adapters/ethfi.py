"""EtherFi ETHFI governance token reference data adapter.

Discovers the ETHFI governance token on Ethereum mainnet. ETHFI is the governance
token of the EtherFi protocol. It is distributed as seasonal (quarterly) rewards
to weETH holders and operators.

Contract address: 0xFe0c30065B384F05761f15d0CC899D4F9F9Cc0eB (ETHFI on Ethereum mainnet)
Reference: https://www.ether.fi/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# EtherFi ETHFI token TGE date (2024-03-18).
_ETHFI_DEPLOY_DATE = datetime(2024, 3, 18, tzinfo=UTC)

# ETHFI contract address on Ethereum mainnet
_ETHFI_ADDRESS = "0xFe0c30065B384F05761f15d0CC899D4F9F9Cc0eB"

# ETHFI is also traded on Binance as a spot pair
# Actual Binance spot pair instruments are produced by BinanceReferenceDataAdapter.
ETHFI_BINANCE_SYMBOLS = ["ETHFIUSDT", "ETHFIETH"]

_GOVERNANCE_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "ETHFI",
        "contract_address": _ETHFI_ADDRESS,
        "underlying": "ETH",
        "token_type": "GOVERNANCE_TOKEN",
    },
]


class EthFiGovernanceReferenceDataAdapter(BaseReferenceDataAdapter):
    """EtherFi ETHFI governance token reference data adapter.

    ETHFI launched March 2024 (TGE). It is the governance token for the EtherFi
    protocol and is distributed quarterly as seasonal airdrop rewards to weETH
    holders and node operators.

    Note: This adapter is ONLY for the ETHFI governance token.
    For weETH (yield-bearing LST) instruments, use EtherFiReferenceDataAdapter.
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
        return "etherfi-governance"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return ETHFI governance token as an instrument record."""
        if instrument_type not in (None, "GOVERNANCE_TOKEN", "governance_token"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"ETHERFI-GOV-{self._chain}"

        for token in _GOVERNANCE_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:GOVERNANCE_TOKEN:{symbol}",
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.SPOT_PAIR,
                    base_asset=symbol,
                    quote_asset=underlying,
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=underlying,
                    available_from_datetime=_ETHFI_DEPLOY_DATE,
                )
            )

        logger.info("EtherFi-governance: fetched %d ETHFI instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("ETHFI governance token does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("ETHFI governance token has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("ETHFI governance token has no on-chain funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("ETHFI OHLCV not supported via reference data — use MTDS/MDPS")
