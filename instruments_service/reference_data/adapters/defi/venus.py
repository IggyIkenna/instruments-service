"""Venus Protocol reference data adapter — Compound-fork lending on BSC + Ethereum.

Discovers Venus core-pool lending markets. BSC is the primary deployment
(launched 2020-09); Ethereum mainnet launched the IL Core Pool 2024-04-15.
Each curated market emits a supply-side ``A_TOKEN`` instrument (collateral
deposited, vToken-equivalent) and a borrow-side ``DEBT_TOKEN`` instrument
(loan asset borrowed against that collateral) — the same A_TOKEN/DEBT_TOKEN
split ``aave_v3.py`` uses per reserve
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Markets are curated (top-of-TVL by collateral asset).
Reference: https://app.venus.io/
"""

import logging
from datetime import datetime
from decimal import Decimal

from unified_api_contracts import AssetGroup, build_canonical_instrument_id
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import resolve_evm_token_decimals
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
            "vault_address": "0xA07c5b74C9B40447a954e1466938b865b6BBea36",  # vBNB  # DERIVED 2020-09 from bsc app.venus.io
        },
        {
            "collateral_asset": "BTCB",
            "borrow_asset": "USDC",
            "vault_address": "0x882C173bC7Ff3b7786CA16dfeD3DFFfb9Ee7847B",  # vBTCB  # DERIVED 2020-09 from bsc app.venus.io
        },
        {
            "collateral_asset": "USDT",
            "borrow_asset": "USDC",
            "vault_address": "0xfD5840Cd36d94D7229439859C0112a4185BC0255",  # vUSDT  # DERIVED 2020-09 from bsc app.venus.io
        },
    ],
    "ETHEREUM": [
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0x7c8ff7d2A1372433726f879BD945fFb250B94c65",  # vWETH-Core  # DERIVED 2024-04-15 from ethereum app.venus.io
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
        if instrument_type not in (None, InstrumentType.LENDING):
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
            address = market["vault_address"]
            available_since = creation_ts_map.get(address, floor_date)
            results.extend(self._build_market_records(market, venue_tag, self._chain, available_since))

        logger.info(
            "Venus: fetched %d supply/debt instruments on %s",
            len(results),
            self._chain,
        )
        return results

    @staticmethod
    def _build_market_records(
        market: dict[str, str],
        venue_tag: str,
        chain: str,
        available_since: datetime,
    ) -> list[InstrumentRecord]:
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one curated vToken market.

        Mirrors ``aave_v3.py``'s ``_build_reserve_records`` split: the
        collateral asset deposited is the supply-side instrument, the
        borrow asset drawn against it is the debt-side instrument. Both
        share the market's on-chain identity (address, decimals,
        available-since).
        """
        collateral = market["collateral_asset"]
        borrow = market["borrow_asset"]
        address = market["vault_address"]
        pair_symbol = f"{collateral}-{borrow}"

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": address,
            "pool_address": address,
            "base_asset": collateral,
            "quote_asset": borrow,
            "tick_size": Decimal("0.000001"),
            "min_size": Decimal("0.000001"),
            "contract_size": Decimal("1"),
            "expiry": None,
            "strike": None,
            "option_type": None,
            "status": InstrumentStatus.ACTIVE,
            "available_from_datetime": available_since,
            "base_asset_decimals": resolve_evm_token_decimals(collateral),
        }

        return [
            InstrumentRecord(
                instrument_key=build_canonical_instrument_id(
                    AssetGroup.DEFI,
                    "venus",
                    InstrumentType.A_TOKEN,
                    f"A{pair_symbol}",
                    chain=chain,
                    passthrough=True,
                ),
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=build_canonical_instrument_id(
                    AssetGroup.DEFI,
                    "venus",
                    InstrumentType.DEBT_TOKEN,
                    f"DEBT{pair_symbol}",
                    chain=chain,
                    passthrough=True,
                ),
                instrument_type=InstrumentType.DEBT_TOKEN,
                **base_kwargs,
            ),
        ]

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
