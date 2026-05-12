"""Karak reference data adapter — instrument discovery for Karak restaking vaults.

Discovers Karak restaking vaults on Ethereum and Arbitrum. Vaults are returned as
InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded curated list of
primary Karak vaults with no network access. Tests are credential-free and offline.

References:
- https://karak.network/
- Karak docs: https://docs.karak.network/
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "KARAK") == 2024-04-08).
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

# Karak Ethereum mainnet GA (2024-04-08) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "KARAK")].
_KARAK_ETH_DEPLOY_DATE = datetime(2024, 4, 8, tzinfo=UTC)
# Karak Arbitrum GA (2024-04-08) — mirrors PROTOCOL_LAUNCH_DATES[("ARBITRUM", "KARAK")].
_KARAK_ARB_DEPLOY_DATE = datetime(2024, 4, 8, tzinfo=UTC)

# Curated primary Karak vault addresses.
# Karak uses a vault-per-asset model where each vault holds a single collateral type.
# The vaults below are the primary ETH-based collateral vaults on Ethereum mainnet.
_KARAK_VAULTS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "KARAK-WSTETH",
            "vault_address": "0x7BBbcA39bCDCC3B3B1a64a8a9f7c6a42C61A3f1E",
            "collateral": "WSTETH",
            "underlying": "ETH",
        },
        {
            "symbol": "KARAK-WETH",
            "vault_address": "0x2DAFc10aAb4Ab3e6B937b02A8Ad5c79F4bFFC5BA",
            "collateral": "WETH",
            "underlying": "ETH",
        },
    ],
    "ARBITRUM": [
        {
            "symbol": "KARAK-ARB-WSTETH",
            "vault_address": "0x9Ad8E7A9B18b1BF0B1B7D6f234c5e8A4F3C2D1B0",
            "collateral": "WSTETH",
            "underlying": "ETH",
        },
    ],
}


def _get_deploy_date(chain: str) -> datetime:
    if chain == "ARBITRUM":
        return _KARAK_ARB_DEPLOY_DATE
    return _KARAK_ETH_DEPLOY_DATE


class KarakReferenceDataAdapter(BaseReferenceDataAdapter):
    """Karak reference data: restaking vault discovery.

    Karak launched on Ethereum mainnet 2024-04-08. Karak is a universal
    restaking protocol supporting EVM-compatible chains. Vaults hold collateral
    tokens (LSTs, stablecoins, BTC derivatives) that are restaked to secure
    Karak Distributed Secure Services (DSS).
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
        return "karak"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Karak restaking vaults as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"KARAK-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        vaults = _KARAK_VAULTS_BY_CHAIN.get(self._chain, [])

        for vault in vaults:
            symbol = vault["symbol"]
            vault_address = vault["vault_address"]
            underlying = vault["underlying"]

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
                    base_asset_decimals=18,
                )
            )

        logger.info("Karak: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Karak does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Karak vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Karak vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Karak OHLCV not supported via reference data")
