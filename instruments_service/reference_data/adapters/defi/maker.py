"""MakerDAO reference data adapter — instrument discovery for the sDAI vault share.

Discovers the MakerDAO Savings Dai (sDAI) ERC-4626 yield-bearing vault share
on Ethereum. Unlike ankr.py / stader.py / stakewise.py / swell.py / mantle.py,
sDAI is NOT a liquid staking token (no validator staking is involved — it is
the Dai Savings Rate wrapped as an ERC-4626 share, same shape as
``yearn.py``'s yvUSDC/yvDAI vault shares), so it is classified
``InstrumentType.YIELD_BEARING`` rather than ``LST``, matching the same
distinction MTDS's ``vault_share_price_handler.py`` (not ``lst_rates_handler.py``)
already draws for this exact token/address.

References:
- https://spark.fi/ (sDAI is the Spark Protocol's ERC-4626 wrapper of the
  MakerDAO Dai Savings Rate).
- sDAI contract: see ``_SDAI_ADDRESS`` below (Etherscan-verified; same address
  MTDS's ``vault_share_price_handler.py`` / ``_instruments_metadata.py`` use).
- Launch date: ``unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES``
  only has ``("ETHEREUM", "MAKER") == 2017-12-19``, which is MakerDAO's
  original single-collateral DAI launch, NOT this specific sDAI vault-share
  contract's deployment (it would over-claim ~5 years of availability for a
  token that didn't exist yet). Uses the sDAI-specific data floor from
  ``unified_api_contracts.registry.defi_venue_capabilities.DEFI_VENUE_DATA_TYPE_CAPABILITIES["MAKER-ETHEREUM"]``
  (2023-01-18) instead — the earliest verified real capture for this exact
  contract, sourced from the live GCS manifest.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# sDAI earliest verified real capture (2023-01-18) — see module docstring for why
# this is used instead of PROTOCOL_LAUNCH_DATES[("ETHEREUM", "MAKER")] (2017-12-19,
# MakerDAO's protocol launch, not this vault-share contract's deployment).
_MAKER_DEPLOY_DATE = datetime(2023, 1, 18, tzinfo=UTC)

# sDAI token address on Ethereum (18 decimals).
_SDAI_ADDRESS = "0x83F20F44975D03b1b09e64809B757c47f942BEeA"  # DERIVED from ethereum etherscan
_SDAI_DECIMALS = 18

_YIELD_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "SDAI",
        "contract_address": _SDAI_ADDRESS,
        "underlying": "DAI",
    },
]


class MakerReferenceDataAdapter(BaseReferenceDataAdapter):
    """MakerDAO reference data: sDAI yield-bearing vault-share discovery.

    sDAI wraps the MakerDAO Dai Savings Rate as an ERC-4626 vault share; its
    exchange rate (``convertToAssets()``) vs DAI appreciates as the DSR
    accrues. Verified real capture for this contract begins 2023-01-18.
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
        return f"MAKER-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return MakerDAO yield-bearing vault-share tokens."""
        if instrument_type not in (None, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"MAKER-{self._chain}"

        for token in _YIELD_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = build_instrument_id(venue_tag, InstrumentType.YIELD_BEARING, symbol, passthrough=True)

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

        logger.info("MakerDAO: fetched %d yield-bearing instruments on %s", len(results), self._chain)
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
