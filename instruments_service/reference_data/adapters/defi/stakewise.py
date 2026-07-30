"""StakeWise reference data adapter — instrument discovery for the osETH LST.

Discovers the StakeWise liquid staking token (osETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="LST" (same class
as rocket_pool.py / lido.py — see lido.py's module docstring for the full
key/field-mismatch rationale).

References:
- https://stakewise.io/
- osETH contract: see ``_OSETH_ADDRESS`` below (Etherscan-verified; identical
  to the address MTDS's ``_EVM_LST_STATIC_CONTRACT_ADDRESSES`` uses for the
  on-chain ``convertToAssets()`` rate read against the OsTokenVaultController).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "STAKEWISE") == 2023-11-28 — StakeWise V3 + osETH launch).
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

# StakeWise Ethereum mainnet GA (2023-11-28) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "STAKEWISE")].
_STAKEWISE_DEPLOY_DATE = datetime(2023, 11, 28, tzinfo=UTC)

# osETH token address on Ethereum (18 decimals).
_OSETH_ADDRESS = "0x2A261e60FB14586B474C208b1B7AC6D0f5000306"  # DERIVED from ethereum etherscan
_OSETH_DECIMALS = 18

_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "OSETH",
        "contract_address": _OSETH_ADDRESS,
        "underlying": "ETH",
    },
]


class StakewiseReferenceDataAdapter(BaseReferenceDataAdapter):
    """StakeWise reference data: osETH LST token discovery.

    StakeWise V3's osETH launched on Ethereum mainnet 2023-11-28. osETH is a
    yield-bearing liquid staking token whose exchange rate vs ETH appreciates
    as staking rewards accrue.
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
        return "stakewise"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return StakeWise LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"STAKEWISE-{self._chain}"

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
                    available_from_datetime=_STAKEWISE_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_OSETH_DECIMALS,
                )
            )

        logger.info("StakeWise: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("StakeWise does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("StakeWise osETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("StakeWise osETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("StakeWise OHLCV not supported via reference data")
