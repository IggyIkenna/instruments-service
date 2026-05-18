"""Beefy Finance reference data adapter — multi-chain yield vault discovery.

Discovers Beefy Finance ``mooXxx`` vault tokens across Ethereum, Arbitrum, Base,
BSC, and Avalanche. Vaults are returned as ``InstrumentRecord`` with
``instrument_type=YIELD_BEARING`` and ``raw_symbol`` set to the vault contract
(``earnContractAddress``).

Pure static-registry adapter: ``get_instruments`` returns a hardcoded curated
list of primary Beefy vaults with no network access. Tests are credential-free
and offline. The catalogue is a starting snapshot; operators can extend later
when MTDS-side discovery wires in.

Chain coverage:
- ETHEREUM, ARBITRUM, BASE, BSC, AVALANCHE.
- POLYGON intentionally NOT registered: a Beefy API survey on 2026-05-12 found
  zero active Polygon vaults in ``https://api.beefy.finance/vaults/polygon``
  (every entry was ``status=eol``). Re-add when Beefy ships fresh Polygon
  vaults.

References:
- https://beefy.com/
- Beefy docs: https://docs.beefy.finance/
- Beefy public vault registry: https://api.beefy.finance/vaults/<chain>
- Launch dates sourced from
  ``unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES``
  (per-chain ``("CHAIN", "BEEFY")`` entries).

Note: Beefy is a multi-chain yield aggregator with 200+ vaults across many
chains. The vaults below are the primary high-TVL active vaults curated from
the Beefy public API on 2026-05-12. Vault addresses are the
``earnContractAddress`` (the user-facing ``mooXxx`` ERC-20 receipt token) —
verified BeefyVaultV7 proxies on the relevant explorer (etherscan/bscscan
spot-checks 2026-05-12).

Pendle, Yearn, Karak, and other yield protocols are covered by separate
adapters.
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

_DEFAULT_CHAIN = "ETHEREUM"

# Beefy per-chain deploy dates — mirror PROTOCOL_LAUNCH_DATES[("<CHAIN>", "BEEFY")].
# Sources: unified-api-contracts/unified_api_contracts/registry/chain_env.py
# (see chain_env.py:344-349 for citations).
_BEEFY_ETH_DEPLOY_DATE = datetime(2021, 12, 1, tzinfo=UTC)
_BEEFY_ARB_DEPLOY_DATE = datetime(2021, 9, 20, tzinfo=UTC)
_BEEFY_BASE_DEPLOY_DATE = datetime(2023, 8, 15, tzinfo=UTC)
_BEEFY_BSC_DEPLOY_DATE = datetime(2020, 10, 8, tzinfo=UTC)
_BEEFY_AVAX_DEPLOY_DATE = datetime(2021, 3, 15, tzinfo=UTC)

# Curated Beefy vault snapshot per chain (2026-05-12).
# All entries verified active via https://api.beefy.finance/vaults/<chain>.
# Vault addresses are the BeefyVaultV7 proxy (earnContractAddress) — the
# ERC-20 ``moo`` receipt token a depositor mints when entering the vault.
# ``underlying`` is the asset the depositor effectively holds via the vault
# (LP-pair shorthand for LP vaults; single-asset for lending/restaking vaults).
_BEEFY_VAULTS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    # ETHEREUM — Morpho single-asset lending vaults (TVL leaders 2026-05-12).
    # Source: https://api.beefy.finance/vaults/ethereum (filtered status=active).
    "ETHEREUM": [
        {
            "symbol": "BEEFY-ETH-MORPHO-USDC-FRONTIER",
            "vault_address": "0x35E7Bf11193C40B943df1fb33e20f947a6EB04E4",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-ETH-MORPHO-V2-USDC-STEAKHOUSE",
            "vault_address": "0x4fB81F52e8606F5Eaf5E840DCAbF8611286Cc46b",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-ETH-MORPHO-V2-AUSD-STEAKHOUSE",
            "vault_address": "0x3988C372513F854Bb5fb980fF135567909012Bda",
            "underlying": "AUSD",
        },
    ],
    # ARBITRUM — mix of CL DEX LP vaults + Morpho single-asset lending vault.
    # Source: https://api.beefy.finance/vaults/arbitrum (filtered status=active).
    "ARBITRUM": [
        {
            "symbol": "BEEFY-ARB-MORPHO-USDC-STEAKHOUSE",
            "vault_address": "0x13EaA79178f2b6C0A43cA265B66d70b9d60F827a",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-ARB-CURVE-CRVUSD-USDC",
            "vault_address": "0x174897AD878c614D7Bc563Eaf23A4a0D75D58d2C",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-ARB-UNI-USDC-USDT",
            "vault_address": "0x195DE404648C4E9f8778816d1E31566272FBff6b",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-ARB-UNI-DAI-WETH",
            "vault_address": "0x155aEbAe69B3Fa2D135146Df6fECa0F8D6FabDD6",
            "underlying": "WETH",
        },
    ],
    # BASE — Aerodrome LP vaults (Aerodrome is the dominant Base DEX).
    # Source: https://api.beefy.finance/vaults/base (filtered status=active).
    "BASE": [
        {
            "symbol": "BEEFY-BASE-AERO-WETH-USDC",
            "vault_address": "0x09139A80454609B69700836a9eE12Db4b5DBB15f",
            "underlying": "WETH",
        },
        {
            "symbol": "BEEFY-BASE-AERO-DAI-USDC",
            "vault_address": "0x315043e79Cc1c2a71199769087CeF61f8a4297a0",
            "underlying": "USDC",
        },
        {
            "symbol": "BEEFY-BASE-AERO-USDC-MAI",
            "vault_address": "0x22F8ff24861a7F7e2ec9b19C5668b43bB69202E3",
            "underlying": "USDC",
        },
    ],
    # BSC — PancakeSwap CLAMM LP vaults (Pancake is the dominant BSC DEX).
    # Source: https://api.beefy.finance/vaults/bsc (filtered status=active).
    "BSC": [
        {
            "symbol": "BEEFY-BSC-PANCAKE-ETH-WBNB",
            "vault_address": "0x015E2d1b7aC621370A24B7dd426EF1ef4DA99B86",
            "underlying": "WBNB",
        },
        {
            "symbol": "BEEFY-BSC-PANCAKE-ETH-BTCB",
            "vault_address": "0x0A751Fec061327d7338a972d3D05b5d1A105bfDa",
            "underlying": "BTCB",
        },
        {
            "symbol": "BEEFY-BSC-PANCAKE-PEPE-WBNB",
            "vault_address": "0x13aD48480aC03233b4A1f73fABF7B7aa08cAfdbE",
            "underlying": "WBNB",
        },
    ],
    # AVALANCHE — TraderJoe single-asset + Blackhole LP vaults.
    # Source: https://api.beefy.finance/vaults/avax (filtered status=active).
    "AVALANCHE": [
        {
            "symbol": "BEEFY-AVAX-JOE",
            "vault_address": "0x282B11E65f0B49363D4505F91c7A44fBEe6bCc0b",
            "underlying": "JOE",
        },
        {
            "symbol": "BEEFY-AVAX-BLACK-BTCB-WAVAX",
            "vault_address": "0x0483130D4EFBC2e082653F88Bd52adF987545455",
            "underlying": "WAVAX",
        },
        {
            "symbol": "BEEFY-AVAX-BLACK-WETH-WAVAX",
            "vault_address": "0x424E9a7D393adFCfc1EEA03731f56F26e42eB27C",
            "underlying": "WAVAX",
        },
    ],
}

# Per-asset ERC-20 decimals lookup. Beefy moo tokens themselves are ERC-20
# wrappers but the depositor-facing decimals depend on the underlying that
# back the vault. We default to 18 (the moo-token decimals across BeefyVaultV7
# regardless of underlying) but expose a mapping for downstream consumers
# that need the underlying's true decimals (USDC=6, WBTC=8, USDT=6, etc.).
_ASSET_DECIMALS: dict[str, int] = {
    "WETH": 18,
    "WSTETH": 18,
    "DAI": 18,
    "AUSD": 18,
    "USDC": 6,
    "USDT": 6,
    "WBTC": 8,
    "BTCB": 18,
    "WBNB": 18,
    "WAVAX": 18,
    "JOE": 18,
}

_DEPLOY_DATE_BY_CHAIN: dict[str, datetime] = {
    "ETHEREUM": _BEEFY_ETH_DEPLOY_DATE,
    "ARBITRUM": _BEEFY_ARB_DEPLOY_DATE,
    "BASE": _BEEFY_BASE_DEPLOY_DATE,
    "BSC": _BEEFY_BSC_DEPLOY_DATE,
    "AVALANCHE": _BEEFY_AVAX_DEPLOY_DATE,
}


def _get_deploy_date(chain: str) -> datetime:
    return _DEPLOY_DATE_BY_CHAIN.get(chain, _BEEFY_ETH_DEPLOY_DATE)


class BeefyReferenceDataAdapter(BaseReferenceDataAdapter):
    """Beefy Finance reference data: yield-vault discovery across 5 chains.

    Beefy launched 2020-10-08 on BSC and expanded to Ethereum (2021-12),
    Avalanche (2021-03), Arbitrum (2021-09), and Base (2023-08). It is a
    multi-chain auto-compounding yield aggregator: depositors deposit a
    single asset or LP token and receive a ``mooXxx`` ERC-20 receipt token
    representing their pro-rata share of the auto-compounded vault. Vaults
    auto-harvest underlying rewards (CRV, AERO, JOE, etc.) and reinvest them
    into the underlying position, increasing the underlying-per-share over
    time without user interaction.

    POLYGON deploy date 2021-05-20 exists in the launch-date registry but is
    NOT registered here because Beefy retired all Polygon vaults (every
    entry in the public vault list returned ``status=eol`` on the 2026-05-12
    audit). Re-add Polygon when Beefy ships fresh active Polygon vaults.
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
        return "beefy"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Beefy vaults for the configured chain as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"BEEFY-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        vaults = _BEEFY_VAULTS_BY_CHAIN.get(self._chain, [])

        for vault in vaults:
            symbol = vault["symbol"]
            vault_address = vault["vault_address"]
            underlying = vault["underlying"]
            decimals = _ASSET_DECIMALS.get(underlying, 18)

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
                    available_from_datetime=deploy_date,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=decimals,
                )
            )

        logger.info("Beefy: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Beefy does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Beefy vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Beefy vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Beefy OHLCV not supported via reference data")
