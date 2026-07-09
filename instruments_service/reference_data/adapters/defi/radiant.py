"""Radiant Capital reference data adapter — omnichain LayerZero lending.

Discovers Radiant V2 lending markets across the chains where it has
canonical deployments (Arbitrum primary, BSC, Ethereum). Each curated
market emits a supply-side ``A_TOKEN`` instrument (collateral deposited,
rToken-equivalent) and a borrow-side ``DEBT_TOKEN`` instrument (loan asset
borrowed against that collateral, variableDebtToken-equivalent) — the same
A_TOKEN/DEBT_TOKEN split ``aave_v3.py`` uses per reserve (Radiant is an
Aave V2 fork; defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Reference: https://radiant.capital/
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

_DEFAULT_CHAIN = "ARBITRUM"

# Curated Radiant V2 markets per chain. Underlying token addresses are
# the rTokens / variableDebtTokens; the canonical lending instrument is
# keyed by the (collateral, borrow) pair against the LendingPool.
_MVP_MARKETS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ARBITRUM": [
        # Arbitrum is the primary deployment.
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1",  # rWETH  # DERIVED from arbitrum radiant.capital
        },
        {
            "collateral_asset": "WBTC",
            "borrow_asset": "USDC",
            "vault_address": "0x727354712BDFcd8596A3852Fd2065b3C34F4F770",  # rWBTC  # DERIVED from arbitrum radiant.capital
        },
        {
            "collateral_asset": "ARB",
            "borrow_asset": "USDC",
            "vault_address": "0x42C248D137512907048021B30d9dA17f48B5b7B2",  # rARB  # DERIVED from arbitrum radiant.capital
        },
    ],
    "BSC": [
        {
            "collateral_asset": "WBNB",
            "borrow_asset": "USDC",
            "vault_address": "0x468Cd12aa9e9fe4301DB146B0f7037831B52382d",  # rWBNB  # DERIVED from bsc radiant.capital
        },
        {
            "collateral_asset": "BTCB",
            "borrow_asset": "USDC",
            "vault_address": "0xb14C36BfFc35B61D1F3DA9E7d5a0f6F42b2A7D0F",  # rBTCB  # DERIVED from bsc radiant.capital
        },
    ],
    "ETHEREUM": [
        {
            "collateral_asset": "WETH",
            "borrow_asset": "USDC",
            "vault_address": "0x0dF5dfd95966753f01cb80E76dc20EA958238C46",  # rWETH-eth  # DERIVED from ethereum radiant.capital
        },
    ],
}


class RadiantReferenceDataAdapter(BaseReferenceDataAdapter):
    """Radiant V2 reference data: lending market discovery from curated rTokens."""

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
        return "radiant"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        markets = _MVP_MARKETS_BY_CHAIN.get(self._chain, [])
        if not markets:
            logger.info("Radiant: no curated markets for chain %s", self._chain)
            return []

        venue_tag = f"RADIANT-{self._chain}"
        floor_date = get_protocol_floor_date("radiant", self._chain)
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
            "Radiant: fetched %d supply/debt instruments on %s",
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
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one curated rToken market.

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
                    "radiant",
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
                    "radiant",
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
        raise NotImplementedError("Radiant does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Radiant lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Radiant lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Radiant OHLCV not supported via reference data")
