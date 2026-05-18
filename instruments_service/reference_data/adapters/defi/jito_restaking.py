"""Jito Restaking reference data adapter — VRT (Vault Receipt Token) discovery on Solana.

Discovers Jito Restaking Vault Receipt Tokens (VRTs) on Solana mainnet. VRTs are the
liquid representations users hold after depositing collateral (SOL / JitoSOL / other LSTs)
into a Jito Restaking vault that secures Node Consensus Networks (NCNs). Each VRT is
issued by an independent vault operator (Renzo, Fragmetric, Kyros, ...).

Returned as InstrumentRecord with instrument_type="YIELD_BEARING".

NOT to be confused with the sibling adapter `defi/jito.py`, which covers the Jito **LST**
(JitoSOL — the original MEV-enhanced liquid staking token, distinct from the restaking
primitive launched 2024-08-01). Both share the "Jito" brand but are separate protocols
with separate data shapes:

* `jito.py`        → venue="JITO-SOLANA", instrument_type=STAKING, single JitoSOL token.
* `jito_restaking.py` (this) → venue="JITORESTAKING-SOLANA", instrument_type=YIELD_BEARING,
                                multiple per-vault VRT mints.

Pure static-registry adapter: get_instruments returns a hardcoded curated list of public
Jito Restaking VRTs with no network access. Tests are credential-free and offline.

References:
- https://www.jito.network/restaking/
- https://docs.restaking.jito.network/
- https://github.com/jito-foundation/restaking
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("SOLANA", "JITORESTAKING") == 2024-08-01).

Note: The Jito Restaking ecosystem is young. The initial VRT-provider cohort was Renzo
($ezSOL), Fragmetric ($fragSOL), and Kyros ($kySOL). The registry below mirrors that
cohort; additional vaults can be added as more operators ship VRTs.
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

# Jito Restaking Solana mainnet GA (2024-08-01) —
# mirrors PROTOCOL_LAUNCH_DATES[("SOLANA", "JITORESTAKING")].
_JITO_RESTAKING_DEPLOY_DATE = datetime(2024, 8, 1, tzinfo=UTC)

# Solana SPL tokens use 9 decimals by convention; jitoSOL and the Jito Restaking VRTs
# all follow this (they are auto-rebasing wrappers around stake denominated in SOL).
_VRT_DECIMALS = 9

# Curated public Jito Restaking VRT (Vault Receipt Token) mint addresses on Solana mainnet.
# Each entry is the per-vault VRT mint (the LRT-equivalent the depositor holds) — NOT the
# global Jito Vault Program PDA (Vau1t6sLNxnzB7ZDsef8TLbPLfyZMYXH8WTNqUdm9g8). The mint
# address IS the canonical instrument identifier for restaking-position tracking.
_JITO_RESTAKING_VAULTS: list[dict[str, str]] = [
    {
        # Renzo Protocol — first cohort VRT operator on Jito Restaking.
        # Source: https://docs.renzoprotocol.com/docs/products/staking-suite/ezsol
        # Solscan / Phantom price page: ezSoL6fY1PVdJcJsUpe5CM3xkfmy3zoVCABybm5WtiC
        "symbol": "JTORK-EZSOL",
        "vault_address": "ezSoL6fY1PVdJcJsUpe5CM3xkfmy3zoVCABybm5WtiC",
        "underlying": "JITOSOL",
        "operator": "Renzo",
    },
    {
        # Fragmetric — first cohort VRT operator on Jito Restaking.
        # Source: https://docs.fragmetric.xyz/dev — Mint Addresses → fragSOL Token
        "symbol": "JTORK-FRAGSOL",
        "vault_address": "FRAGSEthVFL7fdqM8hxfxkfCZzUvmg21cqPJVvC1qdbo",
        "underlying": "JITOSOL",
        "operator": "Fragmetric",
    },
    {
        # Kyros — first cohort VRT operator on Jito Restaking.
        # Source: https://www.solflare.com/prices/kyros-restaked-sol/...
        # Token mint: kySo1nETpsZE2NWe5vj2C64mPSciH1SppmHb4XieQ7B
        "symbol": "JTORK-KYSOL",
        "vault_address": "kySo1nETpsZE2NWe5vj2C64mPSciH1SppmHb4XieQ7B",
        "underlying": "JITOSOL",
        "operator": "Kyros",
    },
]


class JitoRestakingReferenceDataAdapter(BaseReferenceDataAdapter):
    """Jito Restaking reference data: per-vault VRT (Vault Receipt Token) discovery.

    Jito Restaking launched on Solana mainnet 2024-08-01 as Solana's first generalised
    restaking primitive (analogous to EigenLayer on Ethereum). The protocol consists of
    two on-chain programs — the Restaking Program (NCN/operator registry) and the Vault
    Program (tokenised stake via VRTs) — and a permissionless cohort of operators who
    each ship their own VRT mint. Depositors lock SOL / JitoSOL / other LSTs into a
    vault and receive the operator's VRT in return.

    This adapter exposes the per-vault VRT mints as YIELD_BEARING instruments. It is
    intentionally distinct from the sibling `jito.py` adapter (which covers the Jito LST
    JitoSOL only, instrument_type=STAKING).
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
        return "jito_restaking"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Jito Restaking VRTs as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"JITORESTAKING-{self._chain}"

        for vault in _JITO_RESTAKING_VAULTS:
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
                    available_from_datetime=_JITO_RESTAKING_DEPLOY_DATE,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=_VRT_DECIMALS,
                )
            )

        logger.info("JitoRestaking: fetched %d VRT instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Jito Restaking does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Jito Restaking VRTs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Jito Restaking VRTs have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Jito Restaking OHLCV not supported via reference data")
