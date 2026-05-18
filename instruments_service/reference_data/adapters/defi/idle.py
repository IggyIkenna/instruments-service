"""Idle Finance reference data adapter — instrument discovery for Idle senior tranche vaults.

Discovers Idle Finance yield optimiser vaults on Ethereum, Arbitrum, and Polygon.
Vaults are returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded curated list of
primary Idle senior tranche vaults with no network access. Tests are
credential-free and offline.

References:
- https://idle.finance/
- Idle docs: https://docs.idle.finance/
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "IDLE") == 2019-08-13).

Note: Idle has two strategy types — BEST (auto-rebalancing) and senior/junior tranches
(BY_RISK). This adapter covers the primary senior tranche vaults on Ethereum.
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

# Idle Finance Ethereum mainnet GA (2019-08-13) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "IDLE")].
_IDLE_ETH_DEPLOY_DATE = datetime(2019, 8, 13, tzinfo=UTC)
# Idle Arbitrum GA (2024-12-01) — mirrors PROTOCOL_LAUNCH_DATES[("ARBITRUM", "IDLE")].
_IDLE_ARB_DEPLOY_DATE = datetime(2024, 12, 1, tzinfo=UTC)
# Idle Polygon GA (2021-11-11) — mirrors PROTOCOL_LAUNCH_DATES[("POLYGON", "IDLE")].
_IDLE_POLY_DEPLOY_DATE = datetime(2021, 11, 11, tzinfo=UTC)

# Primary Idle senior tranche vault addresses on Ethereum mainnet.
# These are the idleCurrency-BEST optimising vaults (Yield Tranches senior side).
_IDLE_VAULTS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "IDLEDAI-BEST",
            "vault_address": "0x3fE7940616e5Bc47b0775a0dccf6237893353bB4",
            "underlying": "DAI",
        },
        {
            "symbol": "IDLEUSDC-BEST",
            "vault_address": "0x5274891bEC421B39D23760c04A6755eCB444797C",
            "underlying": "USDC",
        },
        {
            "symbol": "IDLEUSDT-BEST",
            "vault_address": "0xF34842d05A1c888Ca02769A633DF37177415C2f8",
            "underlying": "USDT",
        },
    ],
}

_VAULT_DECIMALS_BY_UNDERLYING: dict[str, int] = {
    "DAI": 18,
    "USDC": 6,
    "USDT": 6,
    "WETH": 18,
}


def _get_deploy_date(chain: str) -> datetime:
    if chain == "ARBITRUM":
        return _IDLE_ARB_DEPLOY_DATE
    if chain == "POLYGON":
        return _IDLE_POLY_DEPLOY_DATE
    return _IDLE_ETH_DEPLOY_DATE


class IdleReferenceDataAdapter(BaseReferenceDataAdapter):
    """Idle Finance reference data: yield vault discovery.

    Idle Finance launched on Ethereum mainnet 2019-08-13. Idle is a yield
    aggregator that optimises lending rates across Compound, Aave, Maker DSR,
    and other DeFi protocols. The BEST strategy automatically rebalances between
    protocols to maximise APY.
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
        return "idle"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Idle yield vaults as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"IDLE-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        vaults = _IDLE_VAULTS_BY_CHAIN.get(self._chain, [])

        for vault in vaults:
            symbol = vault["symbol"]
            vault_address = vault["vault_address"]
            underlying = vault["underlying"]
            decimals = _VAULT_DECIMALS_BY_UNDERLYING.get(underlying, 18)

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

        logger.info("Idle: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Idle does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Idle vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Idle vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Idle OHLCV not supported via reference data")
