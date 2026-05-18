"""Cambrian Network restaking reference data adapter — Solana restaking primitives.

Discovers Cambrian Network restaking instruments on Solana mainnet. Cambrian Network
provides foundational restaking infrastructure (AVS/NCN restaking primitives) for
Solana, enabling application-layer protocols to leverage Solana's economic security.

Restaking model:
  - Users deposit SOL/LST assets into Cambrian operator vaults.
  - The deposited stake secures Cambrian AVS networks (Actively Validated Services).
  - Depositors receive cSOL (Cambrian Staked SOL) representing their restaked position,
    which accrues both base staking APY and AVS operator reward APY.
  - Critical for ``carry_staked_basis``: cSOL yield captures two layers of return.
    Ignoring AVS rewards under-reports the true carry by the AVS premium component.

Returns InstrumentRecord with instrument_type=YIELD_BEARING.

Program/deployment details:
  - Cambrian mainnet: 2024-06-01 (approximate; Cambrian Solana AVS network launch).
    Note: Cambrian's exact Solana program ID requires verification from official docs.
    Program ID below is a best-guess placeholder; update from:
    https://docs.cambrian.network/developers/program-addresses
  - Estimated launch: Q2 2024 based on Cambrian whitepaper timeline.

References:
  - https://cambrian.network/
  - https://docs.cambrian.network/
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

# Conservative floor date: Cambrian Solana AVS mainnet launch ~2024-06-01.
# Matches SOLANA_PROTOCOL_DEPLOY_DATES["cambrian"] in _solana_utils.py.
# Data before this date is EXPECTED_PRE_VENUE_LAUNCH.
_CAMBRIAN_DEPLOY_DATE = datetime(2024, 6, 1, tzinfo=UTC)

# Solana SPL tokens use 9 decimals by convention.
_CSOL_DECIMALS = 9

# Curated Cambrian restaking instruments on Solana mainnet.
# cSOL is the canonical receipt token for Cambrian operator vault deposits.
# Note: Token mint addresses below are best-guess placeholders as of 2026-05-13.
# Cambrian's official Solana token registry is not yet publicly documented.
# Update when official addresses are published at https://docs.cambrian.network/
_CAMBRIAN_VAULTS: list[dict[str, str]] = [
    {
        # cSOL — Cambrian Staked SOL (operator vault receipt token).
        # Represents restaked SOL securing Cambrian AVS networks.
        # Note: Vault address requires verification against official Cambrian docs.
        "symbol": "CAMB-CSOL",
        "vault_address": "CAMBr1ANreStakingVau1tProgramSo1anaXXXXXXXXXX",
        "underlying": "SOL",
        "description": "Cambrian cSOL — AVS restaking operator vault receipt token",
    },
    {
        # cSOL (JitoSOL route) — JitoSOL restaked into Cambrian AVS vaults.
        # Layers JitoSOL MEV rewards + Cambrian AVS rewards for multi-layer yield.
        "symbol": "CAMB-CSOL-JITOSOL",
        "vault_address": "CAMBr1ANreStakingVau1tJitoSo1anaXXXXXXXXXXXXX",
        "underlying": "JITOSOL",
        "description": "Cambrian cSOL (JitoSOL collateral) — JitoSOL + AVS restaking rewards",
    },
]


class CambrianReferenceDataAdapter(BaseReferenceDataAdapter):
    """Cambrian Network restaking reference data: AVS vault instrument discovery.

    Cambrian Network provides Solana-native restaking primitives for AVS protocols.
    Users stake SOL/LSTs into Cambrian operator vaults, receiving cSOL that accrues
    both base staking and AVS operator rewards — two-layer carry for arbitrage strategies.

    This is a pure static-registry adapter — no network access at runtime.
    Vault addresses are best-guess placeholders as of 2026-05-13; update from official
    Cambrian program registry once publicly published.
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
        return "cambrian"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Cambrian AVS restaking vault instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"CAMBRIAN-{self._chain}"

        for vault in _CAMBRIAN_VAULTS:
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
                    available_from_datetime=_CAMBRIAN_DEPLOY_DATE,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=_CSOL_DECIMALS,
                )
            )

        logger.info("Cambrian: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Cambrian does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Cambrian vault tokens have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Cambrian vault tokens have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Cambrian OHLCV not supported via reference data")
