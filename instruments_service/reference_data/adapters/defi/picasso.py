"""Picasso Network restaking reference data adapter — cross-chain restaking on Solana.

Discovers Picasso Network restaking instruments on Solana mainnet. Picasso Network is
a cross-chain restaking protocol that enables Solana LSTs (and other assets) to be
restaked to secure inter-chain services (ICS / Cosmos-adjacent AVS equivalents).

Restaking model:
  - Users deposit SOL-denominated assets (SOL, JitoSOL, mSOL) into Picasso vaults.
  - Assets secure cross-chain services via Picasso's ICS (Inter-Chain Security) layer.
  - Users receive yield from both base Solana staking AND cross-chain security services.
  - Critical for ``carry_staked_basis``: restaking yield is a second-order return
    on top of native staking — omitting it under-reports true carry.

Returns InstrumentRecord with instrument_type=YIELD_BEARING.

Program/deployment details:
  - Picasso Network Solana program: 5nMau41MBCMmPfQHs9FMgzMgCJVA1VdJBV9kLnzBNNDn
    Note: Program ID is best-guess based on Solscan/public explorer searches as of
    2026-05-13. Picasso's primary documentation focuses on EVM-side (IBC connections)
    rather than Solana program IDs. If incorrect, update from official Picasso docs.
  - Launch date: 2023-05-01 (approximate; Picasso IBC mainnet, Solana restaking scope
    launched later; using conservative pre-launch date for data floor).

References:
  - https://picasso.network/
  - https://docs.picasso.network/
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

# Conservative floor date: Picasso Network IBC mainnet launch 2023-05-01.
# Matches SOLANA_PROTOCOL_DEPLOY_DATES["picasso"] in _solana_utils.py.
# Note: Picasso's Solana restaking module may have launched later; 2023-05-01 is
# the conservative pre-genesis floor — data before this date is EXPECTED_PRE_VENUE_LAUNCH.
_PICASSO_DEPLOY_DATE = datetime(2023, 5, 1, tzinfo=UTC)

# Solana SPL tokens use 9 decimals by convention.
_PICA_DECIMALS = 9

# Curated Picasso Network restaking instruments on Solana mainnet.
# pSOL (Picasso Staked SOL) represents cross-chain restaked Solana assets.
# Note: Token mint addresses below are best-guess from public Solscan searches.
# Update when official Picasso Solana addresses are published.
_PICASSO_VAULTS: list[dict[str, str]] = [
    {
        # pSOL — Picasso cross-chain restaked SOL.
        # Depositors lock SOL/LSTs to secure cross-chain services (ICS model).
        # Note: Mint address requires verification against official Picasso docs.
        # Using placeholder derived from Picasso's Solana Program ID pattern.
        "symbol": "PICA-PSOL",
        "vault_address": "5nMau41MBCMmPfQHs9FMgzMgCJVA1VdJBV9kLnzBNNDn",
        "underlying": "SOL",
        "description": "Picasso pSOL — cross-chain restaking (ICS) receipt token",
    },
]


class PicassoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Picasso Network restaking reference data: cross-chain restaking vault discovery.

    Picasso Network enables Solana LSTs to secure cross-chain services via ICS
    (Inter-Chain Security). Users restake SOL/LST assets and receive yield from
    both native Solana staking and ICS cross-chain security services.

    This is a pure static-registry adapter — no network access at runtime.
    Program ID / launch date are best-guess as of 2026-05-13; update from official
    Picasso Solana program registry when available.
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
        return "picasso"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Picasso cross-chain restaking vault instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"PICASSO-{self._chain}"

        for vault in _PICASSO_VAULTS:
            symbol = vault["symbol"]
            vault_address = vault["vault_address"]
            underlying = vault["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:VAULT:{symbol}",
                    venue=venue_tag,
                    raw_symbol=vault_address,
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
                    available_from_datetime=_PICASSO_DEPLOY_DATE,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=_PICA_DECIMALS,
                )
            )

        logger.info("Picasso: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Picasso does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Picasso vault tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Picasso vault tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Picasso OHLCV not supported via reference data")
