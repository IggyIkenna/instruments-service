"""Yearn Finance reference data adapter — instrument discovery for Yearn V3 vaults.

Discovers Yearn V3 yield vaults on Ethereum, Arbitrum, and Optimism. Vaults are
returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded curated list of
primary Yearn V3 vaults with no network access. Tests are credential-free and offline.

References:
- https://yearn.fi/
- Yearn V3 docs: https://docs.yearn.fi/developers/v3/overview
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "YEARN_V3") == 2024-03-20).

Note: Yearn V3 is a modular vault system where any strategist can deploy a strategy.
This adapter covers the primary Yearn V3 vaults — high-TVL vaults for major assets
(WETH, USDC, DAI, WBTC) on Ethereum mainnet and Arbitrum.
Beefy and Pendle are covered by separate adapters.
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

# Yearn V3 Ethereum mainnet GA (2024-03-20) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "YEARN_V3")].
_YEARN_ETH_DEPLOY_DATE = datetime(2024, 3, 20, tzinfo=UTC)
# Yearn V3 Arbitrum GA (2023-11-15) — mirrors PROTOCOL_LAUNCH_DATES[("ARBITRUM", "YEARN_V3")].
_YEARN_ARB_DEPLOY_DATE = datetime(2023, 11, 15, tzinfo=UTC)
# Yearn V3 Optimism GA (2023-11-15) — mirrors PROTOCOL_LAUNCH_DATES[("OPTIMISM", "YEARN_V3")].
_YEARN_OPT_DEPLOY_DATE = datetime(2023, 11, 15, tzinfo=UTC)

# Primary Yearn V3 vault addresses per chain.
# These are the high-TVL production vaults for major assets.
_YEARN_VAULTS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "YVWETH-1",
            "vault_address": "0xa258C4606Ca8206D8aA700cE2143D7db854D168c",
            "underlying": "WETH",
        },
        {
            "symbol": "YVDAI-1",
            "vault_address": "0xdA816459F1AB5631232FE5e97a05BBBb94970c95",
            "underlying": "DAI",
        },
        {
            "symbol": "YVUSDC-1",
            "vault_address": "0xa354F35829Ae975e850e23e9615b11Da1B3dC4DE",
            "underlying": "USDC",
        },
        {
            "symbol": "YVWBTC-1",
            "vault_address": "0xA696a63cc78DfFa1a63E9E50587C197387FF6C7E",
            "underlying": "WBTC",
        },
    ],
    "ARBITRUM": [
        {
            "symbol": "ARB-YVWETH-1",
            "vault_address": "0x1DB6DbF5A2a55cD75c14Da41b95B0F3DC0Fe7DaB",
            "underlying": "WETH",
        },
        {
            "symbol": "ARB-YVUSDC-1",
            "vault_address": "0x6Faf8b7fFeE3306EfcFc2Ba9Dded2b7f4A5E80F9",
            "underlying": "USDC",
        },
    ],
}

_ASSET_DECIMALS: dict[str, int] = {
    "WETH": 18,
    "DAI": 18,
    "USDC": 6,
    "USDT": 6,
    "WBTC": 8,
}


def _get_deploy_date(chain: str) -> datetime:
    if chain == "ARBITRUM":
        return _YEARN_ARB_DEPLOY_DATE
    if chain == "OPTIMISM":
        return _YEARN_OPT_DEPLOY_DATE
    return _YEARN_ETH_DEPLOY_DATE


class YearnReferenceDataAdapter(BaseReferenceDataAdapter):
    """Yearn Finance V3 reference data: yield vault discovery.

    Yearn V3 launched on Ethereum mainnet 2024-03-20. Yearn V3 is a modular
    ERC-4626 vault system with permissionless strategy deployment. Vaults
    automatically allocate capital across strategies to optimise yield.
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
        return "yearn"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Yearn V3 vaults as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"YEARN-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        vaults = _YEARN_VAULTS_BY_CHAIN.get(self._chain, [])

        for vault in vaults:
            symbol = vault["symbol"]
            vault_address = vault["vault_address"]
            underlying = vault["underlying"]
            decimals = _ASSET_DECIMALS.get(underlying, 18)

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:VAULT:{symbol}",
                    venue=venue_tag,
                    raw_symbol=vault_address,
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
                    available_from_datetime=deploy_date,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=decimals,
                )
            )

        logger.info("Yearn: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Yearn does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Yearn vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Yearn vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Yearn OHLCV not supported via reference data")
