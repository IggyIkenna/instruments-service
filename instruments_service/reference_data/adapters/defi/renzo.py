"""Renzo reference data adapter — instrument discovery for the ezETH LRT.

Discovers the Renzo liquid restaking token (ezETH) on Ethereum mainnet and on
Renzo's canonical-bridge L2s (Arbitrum). Tokens are returned as InstrumentRecord
with instrument_type="YIELD_BEARING".

References:
- https://www.renzoprotocol.com/
- ezETH Ethereum:  https://etherscan.io/token/0xbf5495Efe5DB9ce00f80364C8B423567e58d2110
- ezETH Arbitrum:  https://arbiscan.io/token/0x2416092f143378750bb29b79eD961ab195CcEea5
- Renzo L2 docs:   https://docs.renzoprotocol.com/docs/integrations/l2-native-restaking
- Launch dates sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "RENZO") == 2024-04-29; ("ARBITRUM", "RENZO") == 2024-02-29).

Per Renzo's canonical-bridge architecture, the ezETH token uses the SAME contract
address across all L2s; chain dimension is encoded in the canonical venue tag
(RENZO-ETHEREUM / RENZO-ARBITRUM) and is the per-chain shard atom in the manifest.
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

# Renzo Ethereum mainnet GA (2024-04-29) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "RENZO")].
_RENZO_ETH_DEPLOY_DATE = datetime(2024, 4, 29, tzinfo=UTC)
# Renzo Arbitrum L2 native-restaking GA (2024-02-29) — mirrors PROTOCOL_LAUNCH_DATES[("ARBITRUM", "RENZO")].
_RENZO_ARB_DEPLOY_DATE = datetime(2024, 2, 29, tzinfo=UTC)

# ezETH token address per chain. The Arbitrum address is the canonical bridged
# representation; same canonical address is used across other Renzo L2s if added later.
_EZETH_DECIMALS = 18
_EZETH_ETH_ADDRESS = "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110"
_EZETH_ARB_ADDRESS = "0x2416092f143378750bb29b79eD961ab195CcEea5"

_LRT_TOKENS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "EZETH",
            "contract_address": _EZETH_ETH_ADDRESS,
            "underlying": "ETH",
        },
    ],
    "ARBITRUM": [
        {
            "symbol": "EZETH",
            "contract_address": _EZETH_ARB_ADDRESS,
            "underlying": "ETH",
        },
    ],
}


def _get_deploy_date(chain: str) -> datetime:
    if chain == "ARBITRUM":
        return _RENZO_ARB_DEPLOY_DATE
    return _RENZO_ETH_DEPLOY_DATE


class RenzoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Renzo reference data: ezETH LRT token discovery (multi-chain).

    Renzo launched on Ethereum mainnet 2024-04-29 with native ETH restaking on
    EigenLayer. Renzo extended to Arbitrum on 2024-02-29 (L2-native restaking
    via Renzo's canonical bridge — users deposit on L2 and mint ezETH at L2
    speed/cost). ezETH is yield-bearing — its exchange rate vs ETH appreciates
    as restaking rewards (EigenLayer AVS + native staking) accrue.
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
        return "renzo"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Renzo LRT tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"RENZO-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        tokens = _LRT_TOKENS_BY_CHAIN.get(self._chain, [])

        for token in tokens:
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
                    available_from_datetime=deploy_date,
                    base_asset_contract_address=address,
                    base_asset_decimals=_EZETH_DECIMALS,
                )
            )

        logger.info("Renzo: fetched %d LRT instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Renzo does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Renzo ezETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Renzo ezETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Renzo OHLCV not supported via reference data")
