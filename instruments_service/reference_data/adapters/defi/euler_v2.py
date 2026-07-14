"""Euler V2 reference data adapter — instrument discovery via curated markets.

Discovers Euler V2 EVK lending vaults (re-launched 2024-08-29 after the
2023 V1 incident + full restitution). Each curated market emits a
supply-side ``A_TOKEN`` instrument (collateral deposited into the vault)
and a borrow-side ``DEBT_TOKEN`` instrument (loan asset borrowed against
that collateral) — the same A_TOKEN/DEBT_TOKEN split ``aave_v3.py`` uses
per reserve (defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Markets are curated (high-liquidity vaults with known addresses).
Reference: https://app.euler.finance/
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

_DEFAULT_CHAIN = "ETHEREUM"

# Curated Euler V2 EVK vaults (top-of-TVL by collateral / borrow asset).
_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "WETH",
        "borrow_asset": "USDC",
        "vault_address": "0xD8b27CF359b7D15710a5BE299AF6e7Bf904984C2",  # DERIVED 2024-08-29 from ethereum app.euler.finance
    },
    {
        "collateral_asset": "WSTETH",
        "borrow_asset": "WETH",
        "vault_address": "0xbC4B4AC47582c3E38Ce5940B80Da65401F4628f1",  # DERIVED 2024-08-29 from ethereum app.euler.finance
    },
    {
        "collateral_asset": "USDC",
        "borrow_asset": "USDT",
        "vault_address": "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9",  # DERIVED 2024-08-29 from ethereum app.euler.finance
    },
]


class EulerV2ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Euler V2 reference data: lending market discovery from curated vaults."""

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
        return "euler_v2"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        venue_tag = f"EULER_V2-{self._chain}"
        floor_date = get_protocol_floor_date("euler_v2", self._chain)
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

        logger.info(
            "Euler V2: fetched %d supply/debt instruments on %s",
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
            "base_asset_decimals": resolve_evm_token_decimals(collateral),
        }

        a_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "euler_v2",
            InstrumentType.A_TOKEN,
            f"A{pair_symbol}",
            chain=chain,
            passthrough=True,
        )
        debt_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "euler_v2",
            InstrumentType.DEBT_TOKEN,
            f"DEBT{pair_symbol}",
            chain=chain,
            passthrough=True,
        )
        return [
            InstrumentRecord(
                instrument_key=a_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=a_token_instrument_key,
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=debt_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=debt_token_instrument_key,
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
        raise NotImplementedError("Euler V2 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Euler V2 lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Euler V2 lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Euler V2 OHLCV not supported via reference data")
