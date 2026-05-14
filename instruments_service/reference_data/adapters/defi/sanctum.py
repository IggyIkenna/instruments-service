"""Sanctum reference data adapter — instrument discovery for Sanctum LST tokens.

Discovers Sanctum liquid staking tokens (LSTs) on Solana. Tokens are returned
as InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter for reference data (instrument catalogue). The
actual LST→SOL exchange rate data (lst_rates) is fetched by MTDS via the
Sanctum REST API `https://extra.sanctum.so/v1/sol-value`.

References:
- https://sanctum.so
- INF token (Sanctum Infinity): 5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm
- jupSOL token (Jupiter Staked SOL): jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v
- laineSOL token: LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZMTlkPradhmPuA
- Launch date: 2023-06-01 (Sanctum v1 mainnet, conservative).
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

_DEFAULT_CHAIN = "SOLANA"

# Sanctum v1 mainnet launch — conservative floor used for EXPECTED_PRE_VENUE_LAUNCH
# coverage gating in MTDS oracle_prices / lst_rates handlers.
_SANCTUM_DEPLOY_DATE = datetime(2023, 6, 1, tzinfo=UTC)

# Sanctum LST token catalogue (9 decimals — standard SPL token).
# Sources: Sanctum docs + Solana mainnet mint addresses (verified 2026-05-14).
_LST_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "INF",
        "mint_address": "5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm",
        "underlying": "SOL",
        "name": "Sanctum Infinity Token",
    },
    {
        "symbol": "JUPSOL",
        "mint_address": "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v",
        "underlying": "SOL",
        "name": "Jupiter Staked SOL",
    },
    {
        "symbol": "LAINESOL",
        "mint_address": "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZMTlkPradhmPuA",
        "underlying": "SOL",
        "name": "Laine Staked SOL",
    },
]


class SanctumReferenceDataAdapter(BaseReferenceDataAdapter):
    """Sanctum reference data: INF + jupSOL + laineSOL LST token discovery.

    Sanctum launched on Solana mainnet 2023-06-01. Its flagship product is
    the Infinity (INF) token — a diversified LST index backed by multiple
    Solana stake pools. Tokens are yield-bearing instruments whose SOL exchange
    rate appreciates as staking rewards accrue.
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
                    base_asset_decimals=9,
                )
            )

        logger.info("Sanctum: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Sanctum does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Sanctum LSTs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Sanctum LSTs have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Sanctum OHLCV not supported via reference data")
