"""Convex Finance reference data adapter — instrument discovery for CVX and cvxCRV.

Discovers Convex Finance's primary yield-bearing tokens on Ethereum:
- CVX: Convex governance + staking token (earns protocol fees from Curve LP boosting)
- cvxCRV: CRV staked via Convex (earns veCRV rewards + trading fees)

Tokens are returned as InstrumentRecord with instrument_type="YIELD_BEARING"
(fixed 2026-07-08 — the `instrument_key`'s middle segment previously said the
shorthand `VAULT`, which is not a real `InstrumentType` enum member; see
`karak.py`'s module docstring for the full rationale).

Pure static-registry adapter: get_instruments returns a hardcoded token catalogue
with no network access. Tests are credential-free and offline.

References:
- https://www.convexfinance.com/
- CVX / cvxCRV contracts: see ``_YIELD_TOKENS`` below (Etherscan-verified; per-address DERIVED citation).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "CONVEX") == 2021-05-17).
"""

import logging
from datetime import UTC, datetime
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

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Convex Finance Ethereum mainnet GA (2021-05-17) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "CONVEX")].
_CONVEX_DEPLOY_DATE = datetime(2021, 5, 17, tzinfo=UTC)

# Primary Convex yield-bearing tokens on Ethereum (all 18 decimals).
_YIELD_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "CVX",
        "contract_address": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B",  # DERIVED 2021-05-17 from ethereum etherscan
        "underlying": "ETH",
    },
    {
        "symbol": "CVXCRV",
        "contract_address": "0x62B9c7356A2Dc64a1969e19C23e4f579F9810Aa7",  # DERIVED 2021-05-17 from ethereum etherscan
        "underlying": "CRV",
    },
]
_TOKEN_DECIMALS = 18


class ConvexReferenceDataAdapter(BaseReferenceDataAdapter):
    """Convex Finance reference data: CVX + cvxCRV yield token discovery.

    Convex Finance launched on Ethereum mainnet 2021-05-17. Convex optimises
    Curve Finance LP rewards by staking CRV for veCRV on behalf of users,
    allowing LPs to earn boosted CRV without locking CRV themselves. CVX is
    the governance and fee-sharing token; cvxCRV is the liquid CRV staking
    derivative.
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
        return "convex"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Convex yield tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"CONVEX-{self._chain}"

        for token in _YIELD_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = build_canonical_instrument_id(
                AssetGroup.DEFI, venue_tag, InstrumentType.YIELD_BEARING, symbol, passthrough=True
            )
            results.append(
                InstrumentRecord(
                    # Routed through the shared canonical builder (2026-07-09 retrofit,
                    # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 1) — DRY,
                    # no output change.
                    instrument_key=instrument_key,
                    # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                    # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                    canonical_instrument_id=instrument_key,
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
                    available_from_datetime=_CONVEX_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_TOKEN_DECIMALS,
                )
            )

        logger.info("Convex: fetched %d yield instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Convex does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Convex tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Convex tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Convex OHLCV not supported via reference data")
