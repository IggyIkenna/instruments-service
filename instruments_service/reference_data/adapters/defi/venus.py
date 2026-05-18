"""Venus Protocol reference data adapter — Compound-fork lending on BSC + Ethereum.

Discovers Venus core-pool lending markets. BSC is the primary deployment
(launched 2020-09); Ethereum mainnet launched the IL Core Pool 2024-04-15.

Markets are curated (top-of-TVL by collateral asset).
Reference: https://app.venus.io/
"""

import logging
from datetime import datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.evm_creation_resolver import (
    batch_resolve_evm_creation_timestamps,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "BSC"

_MVP_MARKETS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "BSC": [
        {
            "collateral_asset": "BNB",
            "borrow_asset": "USDC",
            "vault_address": "0xA07c5b74C9B40447a954e1466938b865b6BBea36",  # vBNB
        },
        {
            "collateral_asset": "BTCB",
            "borrow_asset": "USDC",
            "vault_address": "0x882C173bC7Ff3b7786CA16dfeD3DFFfb9Ee7847B",  # vBTCB
        },
        {
            "collateral_asset": "USDT",
            "borrow_asset": "USDC",
            "vault_address": "0xfD5840Cd36d94D7229439859C0112a4185BC0255",  # vUSDT
        },
    ],
    "ETHEREUM": [
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0x7c8ff7d2A1372433726f879BD945fFb250B94c65",  # vWETH-Core
        },
    ],
}


class VenusReferenceDataAdapter(BaseReferenceDataAdapter):
    """Venus Protocol reference data: lending market discovery from curated vTokens."""

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
        return "venus"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        if instrument_type not in (None, "lending_market"):
            return []

        markets = _MVP_MARKETS_BY_CHAIN.get(self._chain, [])
        if not markets:
            logger.info("Venus: no curated markets for chain %s", self._chain)
            return []

        venue_tag = f"VENUS-{self._chain}"
        floor_date = get_protocol_floor_date("venus", self._chain)
        vault_addresses = [m["vault_address"] for m in markets]
        creation_ts_map = await batch_resolve_evm_creation_timestamps(
            vault_addresses,
            self._chain,
        )

        results: list[InstrumentRecord] = []
        for market in markets:
            collateral = market["collateral_asset"]
            borrow = market["borrow_asset"]
            address = market["vault_address"]
            symbol = f"{collateral}-{borrow}"
            instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}"
            available_since = creation_ts_map.get(address, floor_date)

            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    venue=venue_tag,
                    raw_symbol=address,
                    pool_address=address,
                    instrument_type=InstrumentType.LENDING,
                    base_asset=collateral,
                    quote_asset=borrow,
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=available_since,
                )
            )

        logger.info(
            "Venus: fetched %d lending market instruments on %s",
            len(results),
            self._chain,
        )
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
        raise NotImplementedError("Venus does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Venus lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Venus lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Venus OHLCV not supported via reference data")
