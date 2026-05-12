"""Renzo reference data adapter — instrument discovery for the ezETH LRT.

Discovers the Renzo liquid restaking token (ezETH) on Ethereum.
Token is returned as InstrumentRecord with instrument_type="YIELD_BEARING".

References:
- https://www.renzoprotocol.com/
- ezETH contract: https://etherscan.io/token/0xbf5495Efe5DB9ce00f80364C8B423567e58d2110
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "RENZO") == 2024-04-29).

Note: Renzo also has a bridged ezETH on Arbitrum (("ARBITRUM", "RENZO") == 2024-02-29) — the
multi-chain extension (parse `chain` from the venue name + per-chain address map + register
`RENZO-ARBITRUM` in factory.py's `defi_graph_adapters`) is a follow-up; this adapter currently
serves Ethereum only, matching the lido/etherfi single-chain precedent.
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
_RENZO_DEPLOY_DATE = datetime(2024, 4, 29, tzinfo=UTC)

# ezETH token address on Ethereum (18 decimals).
_EZETH_ADDRESS = "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110"
_EZETH_DECIMALS = 18

_LRT_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "EZETH",
        "contract_address": _EZETH_ADDRESS,
        "underlying": "ETH",
    },
]


class RenzoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Renzo reference data: ezETH LRT token discovery.

    Renzo launched on Ethereum mainnet 2024-04-29. ezETH is a yield-bearing
    liquid restaking token; its exchange rate vs ETH appreciates as restaking
    rewards (EigenLayer AVS + native staking) accrue.
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
                    available_from_datetime=_RENZO_DEPLOY_DATE,
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
