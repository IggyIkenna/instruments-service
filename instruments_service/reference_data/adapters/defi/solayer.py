"""Solayer restaking reference data adapter — sSOL vault instrument discovery on Solana.

Discovers Solayer Endogenous AVS restaking instruments on Solana mainnet. Solayer
is a Solana-native restaking protocol that lets users restake SOL/LSTs into Actively
Validated Services (AVS) running on Solana. Depositors receive sSOL (Solayer Staked SOL)
which accrues both base staking rewards and additional AVS operator rewards.

Restaking model:
  - Users deposit SOL or LSTs (JitoSOL, mSOL, bSOL) into Solayer vaults.
  - In return they receive sSOL — a yield-bearing token that captures:
      (a) native SOL staking yield,
      (b) AVS operator rewards from securing Solana-native protocols.
  - Critical for ``carry_staked_basis`` carry computation: sSOL's total yield =
    native staking APY + restaking AVS reward APY. Under-counting the second term
    causes carry to be under-reported.

Returns InstrumentRecord with instrument_type=YIELD_BEARING.

NOT to be confused with:
  - `jito.py`            → venue="JITO-SOLANA", JitoSOL (MEV LST, not restaking).
  - `jito_restaking.py`  → venue="JITORESTAKING-SOLANA", Jito VRT vault system.

This adapter is a pure static-registry adapter — `get_instruments()` returns a
hardcoded curated list of public Solayer vault instruments with no network access.
Tests are credential-free and offline.

References:
  - https://www.solayer.org/
  - https://docs.solayer.org/
  - Program ID: SolayerEndoAVSSo11111111111111111111111111112 (best-guess; verify on-chain)
    Note: Solayer program ID not publicly published as of 2026-05-13.
    Using documented SSoL token mint as primary identifier.
  - sSOL token mint (Solscan): sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh
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

# Solayer mainnet launch: 2024-04-01 (approximate; endogenous AVS launched ~April 2024).
# Matches SOLANA_PROTOCOL_DEPLOY_DATES["solayer"] in _solana_utils.py.
_SOLAYER_DEPLOY_DATE = datetime(2024, 4, 1, tzinfo=UTC)

# Solana SPL tokens use 9 decimals by convention.
_SSOL_DECIMALS = 9

# Curated Solayer sSOL vault instruments on Solana mainnet.
# sSOL is the unified liquid restaking token representing a basket of staked SOL
# plus endogenous AVS rewards. Multiple underlying collateral types are accepted.
_SOLAYER_VAULTS: list[dict[str, str]] = [
    {
        # sSOL — Solayer Staked SOL (endogenous restaking receipt token).
        # Primary instrument: SOL deposited into Solayer earns base staking + AVS rewards.
        # Source: https://solscan.io/token/sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh
        "symbol": "SLAYER-SSOL",
        "vault_address": "sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh",
        "underlying": "SOL",
        "description": "Solayer sSOL — endogenous AVS restaking receipt token",
    },
    {
        # sSOL (JitoSOL route) — users may also deposit JitoSOL into Solayer,
        # layering Jito MEV rewards + Solayer AVS rewards on top of base SOL staking.
        # Instrument key uses SLAYER- prefix for namespace isolation.
        "symbol": "SLAYER-SSOL-JITOSOL",
        "vault_address": "sSoLEAPnEMEpFuDEy1C5bBtHRkBPbERgZvBjGGsMQqH",
        "underlying": "JITOSOL",
        "description": "Solayer sSOL (JitoSOL collateral) — JitoSOL + AVS restaking rewards",
    },
]


class SolayerReferenceDataAdapter(BaseReferenceDataAdapter):
    """Solayer restaking reference data: sSOL vault instrument discovery.

    Solayer is a Solana-native restaking protocol launched April 2024. Users stake
    SOL or LSTs into Solayer's endogenous vaults and receive sSOL — a yield-bearing
    token that accrues both base staking APY and AVS operator rewards.

    This adapter is intentionally distinct from:
      - JitoReferenceDataAdapter (jito.py): covers JitoSOL LST only.
      - JitoRestakingReferenceDataAdapter (jito_restaking.py): covers Jito VRT vaults.
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
        return "solayer"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Solayer sSOL vault instruments as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SOLAYER-{self._chain}"

        for vault in _SOLAYER_VAULTS:
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
                    available_from_datetime=_SOLAYER_DEPLOY_DATE,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=_SSOL_DECIMALS,
                )
            )

        logger.info("Solayer: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Solayer does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Solayer vault tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Solayer vault tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Solayer OHLCV not supported via reference data")
