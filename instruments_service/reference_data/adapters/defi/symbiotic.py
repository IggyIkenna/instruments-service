"""Symbiotic reference data adapter — instrument discovery for Symbiotic restaking vaults.

Discovers Symbiotic restaking vaults on Ethereum. Vaults are returned as
InstrumentRecord with instrument_type="YIELD_BEARING".

Pure static-registry adapter: get_instruments returns a hardcoded curated list of
primary Symbiotic collateral vaults with no network access. Tests are
credential-free and offline.

References:
- https://symbiotic.fi/
- Symbiotic docs: https://docs.symbiotic.fi/
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "SYMBIOTIC") == 2024-06-11).

Note: Symbiotic is a multi-vault restaking protocol. This adapter covers the primary
collateral vaults for major LSTs (wstETH, rETH, cbETH, swETH, ETHx, osETH).
The vault token is the share token representing the depositor's claim on the collateral.
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

# Symbiotic Ethereum mainnet GA (2024-06-11) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "SYMBIOTIC")].
_SYMBIOTIC_DEPLOY_DATE = datetime(2024, 6, 11, tzinfo=UTC)

# Curated primary Symbiotic collateral vault addresses on Ethereum.
# Each vault holds a specific collateral token; the vault address IS the instrument
# address for restaking tracking purposes.
_SYMBIOTIC_VAULTS: list[dict[str, str]] = [
    {
        "symbol": "SYMB-WSTETH",
        "vault_address": "0xC329400492c6ff2438472D4651Ad17389fCb843a",
        "collateral": "WSTETH",
        "underlying": "ETH",
    },
    {
        "symbol": "SYMB-RETH",
        "vault_address": "0x4Ec07dF977D5f7E6Bb3d10B1d01B7A7680e5E6B0",
        "collateral": "RETH",
        "underlying": "ETH",
    },
    {
        "symbol": "SYMB-CBETH",
        "vault_address": "0xB26ff591F44b04E78de18f43B46f8b70C6676984",
        "collateral": "CBETH",
        "underlying": "ETH",
    },
    {
        "symbol": "SYMB-ETHX",
        "vault_address": "0x40e65E81e7D39e593eE55C5eeE44A7a84B65cC40",
        "collateral": "ETHX",
        "underlying": "ETH",
    },
]


class SymbioticReferenceDataAdapter(BaseReferenceDataAdapter):
    """Symbiotic reference data: restaking vault discovery.

    Symbiotic launched on Ethereum mainnet 2024-06-11. Symbiotic is a shared
    security protocol where operators stake collateral tokens (LSTs) in vaults
    to secure networks (operators + networks). Vault tokens represent a depositor's
    share of the restaked collateral.
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
        return "symbiotic"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Symbiotic restaking vaults as yield-bearing instruments."""
        if instrument_type not in (None, "yield_bearing"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SYMBIOTIC-{self._chain}"

        for vault in _SYMBIOTIC_VAULTS:
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
                    available_from_datetime=_SYMBIOTIC_DEPLOY_DATE,
                    base_asset_contract_address=vault_address,
                    base_asset_decimals=18,
                )
            )

        logger.info("Symbiotic: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("Symbiotic does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Symbiotic vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Symbiotic vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Symbiotic OHLCV not supported via reference data")
