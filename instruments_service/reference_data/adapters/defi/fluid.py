"""Fluid reference data adapter — instrument discovery via curated markets.

Discovers Fluid (Instadapp) lending markets on Ethereum. Each curated
market emits a supply-side ``A_TOKEN`` instrument (collateral deposited
into the vault) and a borrow-side ``DEBT_TOKEN`` instrument (loan asset
borrowed against that collateral) — the same A_TOKEN/DEBT_TOKEN split
``aave_v3.py`` uses per reserve
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Fluid markets are curated (high-liquidity vaults with known addresses).
Reference: https://fluid.instadapp.io/
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
from ...utils.evm_creation_resolver import (
    batch_resolve_evm_creation_timestamps,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Per-chain deploy dates now in evm_creation_resolver.LENDING_PROTOCOL_DEPLOY_DATES.

# Curated Fluid high-liquidity vaults (same as UMI adapter)
_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "ETH",
        "borrow_asset": "USDC",
        "vault_address": "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
    {
        "collateral_asset": "ETH",
        "borrow_asset": "USDT",
        "vault_address": "0xbEC491FeF7B4f666b270F9D5E5C3f443cBf20991",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "ETH",
        "vault_address": "0xA0F83Fc5885cEBc0420ce7C7b139Adc80c4F4D91",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "USDC",
        "vault_address": "0x51197586F6A9e2571868b6ffaef308f3bdfEd3aE",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "USDT",
        "vault_address": "0x1c2bB46f36561bc4F05A94BD50916496aa501078",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
    {
        "collateral_asset": "WEETH",
        "borrow_asset": "WSTETH",
        "vault_address": "0x40D9b8417E6E1DcD358f04E3328bCEd061018A82",  # DERIVED 2024-10 from ethereum fluid.instadapp.io
    },
]


class FluidReferenceDataAdapter(BaseReferenceDataAdapter):
    """Fluid reference data: lending market discovery from curated vault list.

    Fluid launched October 2024 on Ethereum mainnet.
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
        return "fluid"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch curated Fluid lending markets as instruments."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        venue_tag = f"FLUID-{self._chain}"
        floor_date = get_protocol_floor_date("fluid", self._chain)

        # Resolve vault contract creation timestamps (cached after first run)
        vault_addresses = [m["vault_address"] for m in _MVP_MARKETS]
        creation_ts_map = await batch_resolve_evm_creation_timestamps(
            vault_addresses,
            self._chain,
        )

        results: list[InstrumentRecord] = []
        for market in _MVP_MARKETS:
            address = market["vault_address"]
            available_since = creation_ts_map.get(address, floor_date)
            results.extend(self._build_market_records(market, venue_tag, self._chain, available_since))

        logger.info("Fluid: fetched %d supply/debt instruments on %s", len(results), self._chain)
        return results

    @staticmethod
    def _build_market_records(
        market: dict[str, str],
        venue_tag: str,
        chain: str,
        available_since: datetime,
    ) -> list[InstrumentRecord]:
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one curated vault.

        Mirrors ``aave_v3.py``'s ``_build_reserve_records`` split: the
        collateral asset deposited into the vault is the supply-side
        instrument, the borrow asset drawn against it is the debt-side
        instrument. Both share the vault's on-chain identity (address,
        decimals, available-since).
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
            "base_asset_decimals": 18,
        }

        a_token_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "fluid",
            InstrumentType.A_TOKEN,
            f"A{pair_symbol}",
            chain=chain,
            passthrough=True,
        )
        debt_token_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "fluid",
            InstrumentType.DEBT_TOKEN,
            f"DEBT{pair_symbol}",
            chain=chain,
            passthrough=True,
        )

        return [
            InstrumentRecord(
                instrument_key=a_token_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=a_token_key,
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=debt_token_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=debt_token_key,
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
        raise NotImplementedError("Fluid does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Fluid lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Fluid lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Fluid OHLCV not supported via reference data")
