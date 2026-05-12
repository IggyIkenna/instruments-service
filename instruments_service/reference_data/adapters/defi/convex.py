"""Convex Finance reference data adapter — instrument discovery for CVX and cvxCRV.

Discovers Convex Finance's primary yield-bearing tokens on Ethereum:
- CVX: Convex governance + staking token (earns protocol fees from Curve LP boosting)
- cvxCRV: CRV staked via Convex (earns veCRV rewards + trading fees)

Tokens are returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded token catalogue
with no network access. Tests are credential-free and offline.

References:
- https://www.convexfinance.com/
- CVX contract: https://etherscan.io/token/0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B
- cvxCRV contract: https://etherscan.io/token/0x62B9c7356A2Dc64a1969e19C23e4f579F9810Aa7
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "CONVEX") == 2021-05-17).
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

# Convex Finance Ethereum mainnet GA (2021-05-17) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "CONVEX")].
_CONVEX_DEPLOY_DATE = datetime(2021, 5, 17, tzinfo=UTC)

# Primary Convex yield-bearing tokens on Ethereum (all 18 decimals).
_YIELD_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "CVX",
        "contract_address": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B",
        "underlying": "ETH",
    },
    {
        "symbol": "CVXCRV",
        "contract_address": "0x62B9c7356A2Dc64a1969e19C23e4f579F9810Aa7",
        "underlying": "CRV",
    },
]
_TOKEN_DECIMALS = 18


class ConvexReferenceDataAdapter(BaseReferenceDataAdapter):
    """Convex Finance reference data: CVX + cvxCRV yield token discovery.

    Convex Finance launched on Ethereum mainnet 2021-05-17. Convex optimises
    Curve Finance LP rewards by staking CRV for veCRV on behalf of users,
    allowing LPs to earn boosted CRV without locking CRV themselves. CVX is
    the governance and fee-sharing token; cvxCRV is the liquid CRV staking
    derivative.
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
        return "convex"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Convex yield tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"CONVEX-{self._chain}"

        for token in _YIELD_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:VAULT:{symbol}",
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
                    available_from_datetime=_CONVEX_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_TOKEN_DECIMALS,
                )
            )

        logger.info("Convex: fetched %d yield instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
        raise NotImplementedError("Convex does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Convex tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Convex tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Convex OHLCV not supported via reference data")
