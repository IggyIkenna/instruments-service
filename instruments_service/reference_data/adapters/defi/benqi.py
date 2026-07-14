"""Benqi Liquid Markets reference data adapter — Compound-fork lending on Avalanche.

Discovers Benqi qiToken lending markets. Avalanche-only deployment
(launched 2021-08-19). Markets are curated (top-of-TVL collateral assets).
Each curated market emits a supply-side ``A_TOKEN`` instrument (collateral
deposited, qiToken-equivalent) and a borrow-side ``DEBT_TOKEN`` instrument
(loan asset borrowed against that collateral) — the same A_TOKEN/DEBT_TOKEN
split ``aave_v3.py`` uses per reserve
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Reference: https://app.benqi.fi/
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

_DEFAULT_CHAIN = "AVALANCHE"

_MVP_MARKETS: list[dict[str, str]] = [
    {
        "collateral_asset": "AVAX",
        "borrow_asset": "USDC",
        "vault_address": "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c",  # qiAVAX  # DERIVED 2021-08-19 from avalanche app.benqi.fi
    },
    {
        "collateral_asset": "BTCB",
        "borrow_asset": "USDC",
        "vault_address": "0x89a415b3D20098E6A6C8f7a59001C67BD3129821",  # qiBTC.b  # DERIVED 2021-08-19 from avalanche app.benqi.fi
    },
    {
        "collateral_asset": "WETHE",
        "borrow_asset": "USDC",
        "vault_address": "0x334AD834Cd4481BB02d09615E7c11a00579A7909",  # qiETH  # DERIVED 2021-08-19 from avalanche app.benqi.fi
    },
    {
        "collateral_asset": "SAVAX",
        "borrow_asset": "AVAX",
        "vault_address": "0xF362feA9659cf036792c9cb02f8ff8198E21B4cB",  # qisAVAX (LSD recursive)  # DERIVED 2021-08-19 from avalanche app.benqi.fi
    },
]


class BenqiReferenceDataAdapter(BaseReferenceDataAdapter):
    """Benqi reference data: lending market discovery from curated qiTokens."""

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
        return "benqi"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []
        if self._chain != "AVALANCHE":
            logger.info("Benqi: no markets — Benqi is AVALANCHE-only (got %s)", self._chain)
            return []

        venue_tag = f"BENQI-{self._chain}"
        floor_date = get_protocol_floor_date("benqi", self._chain)
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
            "Benqi: fetched %d supply/debt instruments on %s",
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
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one curated qiToken market.

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

        a_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "benqi",
            InstrumentType.A_TOKEN,
            f"A{pair_symbol}",
            chain=chain,
            passthrough=True,
        )
        debt_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "benqi",
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
        raise NotImplementedError("Benqi does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Benqi lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Benqi lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Benqi OHLCV not supported via reference data")
