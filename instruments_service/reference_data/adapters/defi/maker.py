"""MakerDAO reference data adapter — instrument discovery for the sDAI savings vault.

Discovers MakerDAO's Dai Savings Rate vault token (sDAI) on Ethereum. sDAI is
an ERC-4626 vault share, NOT a liquid staking token (no validator staking
involved — see MTDS's ``vault_share_price_handler.py``, whose ``data_type=
vault_share_price`` is the correct home for its on-chain ``convertToAssets``
rate read). Returned as InstrumentRecord with instrument_type="YIELD_BEARING"
(same class as idle.py / yearn.py vault adapters).

References:
- https://docs.makerdao.com/smart-contract-modules/reward-modules/dsr-savings-rate
- sDAI contract: see ``_SDAI_ADDRESS`` below (Etherscan-verified; identical to
  the address MTDS's ``_EVM_LST_STATIC_CONTRACT_ADDRESSES`` /
  ``vault_share_price_handler.py`` use for the on-chain rate read).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "MAKER") == 2017-12-19 — MakerDAO single-collateral DAI launch).
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

# MakerDAO Ethereum mainnet GA (2017-12-19) — mirrors PROTOCOL_LAUNCH_DATES[("ETHEREUM", "MAKER")].
_MAKER_DEPLOY_DATE = datetime(2017, 12, 19, tzinfo=UTC)

# sDAI vault-share token address on Ethereum (18 decimals).
_SDAI_ADDRESS = "0x83F20F44975D03b1b09e64809B757c47f942BEeA"  # DERIVED from ethereum etherscan
_SDAI_DECIMALS = 18

_VAULT_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "SDAI",
        "vault_address": _SDAI_ADDRESS,
        "underlying": "DAI",
    },
]


class MakerReferenceDataAdapter(BaseReferenceDataAdapter):
    """MakerDAO reference data: sDAI savings-vault token discovery.

    MakerDAO launched on Ethereum mainnet 2017-12-19. sDAI (the ERC-4626
    wrapper over the Dai Savings Rate module) is a yield-bearing vault token
    whose exchange rate vs DAI appreciates as the DSR accrues.
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
        return "maker"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return MakerDAO sDAI vault tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"MAKER-{self._chain}"

        for vault in _VAULT_TOKENS:
            symbol = vault["symbol"]
            address = vault["vault_address"]
            underlying = vault["underlying"]

            instrument_key = f"{venue_tag}:YIELD_BEARING:{symbol}"

            results.append(
                InstrumentRecord(
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
                    available_from_datetime=_MAKER_DEPLOY_DATE,
                    base_asset_contract_address=address,
                    base_asset_decimals=_SDAI_DECIMALS,
                )
            )

        logger.info("MakerDAO: fetched %d vault instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("MakerDAO does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("MakerDAO sDAI has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("MakerDAO sDAI has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("MakerDAO OHLCV not supported via reference data")
