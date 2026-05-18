"""Sanctum reference data adapter — instrument discovery for Sanctum LST marketplace.

Discovers Sanctum-native liquid staking tokens (LSTs) on Solana.
Tokens are returned as InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded catalogue of
Sanctum-listed LSTs with active TVL. No network access required. Tests are
credential-free and offline.

Sanctum is a Solana LST infrastructure layer that:
- Routes SOL staking into any LST at 1:1 rates via its liquidity pool
- Manages the INF (Infinity) meta-LST that auto-reallocates across top LSTs
- Lists dozens of community-run stake pools (jupSOL, laineSOL, etc.)

References:
- https://sanctum.so/
- Sanctum Infinity (INF): https://extra.sanctum.so/
- INF mint: 5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm
- jupSOL (Jupiter Staked SOL) mint: jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v
- laineSOL (Laine Staked SOL) mint: LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZMTlkPradhmPuA
- Launch date: 2023-06-01 (conservative mainnet-launch floor from UAC chain_env.py).
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
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "SOLANA"

# Sanctum v1 mainnet launch (2023-06-01) — conservative floor per UAC chain_env.py.
_SANCTUM_DEPLOY_DATE: datetime = get_protocol_floor_date("sanctum")

# INF (Sanctum Infinity) — meta-LST that auto-reallocates across top Solana LSTs.
_INF_MINT = "5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm"

# jupSOL (Jupiter Staked SOL) — Jupiter's stake pool LST.
# Mint verified against Jupiter docs and sanctum.so marketplace (2026-05-14).
_JUPSOL_MINT = "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v"

# laineSOL (Laine Staked SOL) — Laine validator-hosted stake pool LST.
_LAINESOL_MINT = "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZMTlkPradhmPuA"

# Standard SPL token decimal precision
_SPL_DECIMALS = 9

# Sanctum LST tokens with active TVL as of 2026-05-14 snapshot.
# Extend this list as new Sanctum-listed LSTs reach material TVL.
_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "INF",
        "mint_address": _INF_MINT,
        "underlying": "SOL",
    },
    {
        "symbol": "JUPSOL",
        "mint_address": _JUPSOL_MINT,
        "underlying": "SOL",
    },
    {
        "symbol": "LAINESOL",
        "mint_address": _LAINESOL_MINT,
        "underlying": "SOL",
    },
]


class SanctumReferenceDataAdapter(BaseReferenceDataAdapter):
    """Sanctum reference data: LST token discovery for the Sanctum marketplace.

    Returns InstrumentRecord entries for the top Sanctum-listed LSTs (INF + jupSOL + laineSOL).
    Sanctum launched on Solana mainnet 2023-06-01. INF is a yield-bearing
    meta-LST; its exchange rate vs SOL appreciates as Solana staking rewards
    accrue across Sanctum's diversified stake pool allocation.
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
        return "sanctum"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Sanctum LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SANCTUM-{self._chain}"

        for token in _LST_TOKENS:
            symbol = token["symbol"]
            mint = token["mint_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:LST:{symbol}",
                    venue=venue_tag,
                    raw_symbol=mint,
                    instrument_type=InstrumentType.YIELD_BEARING,
                    base_asset=underlying,
                    quote_asset="",
                    tick_size=Decimal("0.000000001"),
                    min_size=Decimal("0.000000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=underlying,
                    available_from_datetime=_SANCTUM_DEPLOY_DATE,
                    base_asset_contract_address=mint,
                    base_asset_decimals=_SPL_DECIMALS,
                )
            )

        logger.info("Sanctum: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Sanctum does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Sanctum LSTs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Sanctum LSTs have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Sanctum OHLCV not supported via reference data")
