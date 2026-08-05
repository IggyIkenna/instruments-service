"""Coinbase cbETH reference data adapter — instrument discovery for the cbETH LST.

Discovers Coinbase Wrapped Staked ETH (cbETH) on Ethereum mainnet. cbETH is
Coinbase's liquid staking token: it is minted when a user stakes ETH through
Coinbase and its exchange rate versus ETH appreciates as staking rewards accrue.
Returned as an ``InstrumentRecord`` with instrument_type="LST" (same class as the
other single-token LST adapters — see ``renzo.py`` / ``lido.py`` for the full
rationale on the LST instrument_type choice).

References:
- https://www.coinbase.com/cbeth  (Coinbase cbETH product page)
- cbETH Ethereum address: see ``_CBETH_ETH_ADDRESS`` below (Etherscan-verified;
  identical to ``unified_api_contracts.registry.defi_major_assets.DEFI_MAJOR_ASSET_ADDRESSES["CBETH"]``).
- Launch date sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "COINBASE") == 2022-08-24) — mirrors
  ``venue_launch_dates.DEFI_VENUE_LAUNCH_DATES["COINBASE"]``.

The canonical venue tag is COINBASE-ETHEREUM (the issuing protocol is Coinbase);
chain is the per-chain shard atom in the manifest.
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
from ...utils.defi_utils import build_spot_asset_record

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Coinbase cbETH Ethereum mainnet GA (2022-08-24) — mirrors
# PROTOCOL_LAUNCH_DATES[("ETHEREUM", "COINBASE")].
_CBETH_ETH_DEPLOY_DATE = datetime(2022, 8, 24, tzinfo=UTC)

_CBETH_DECIMALS = 18
# cbETH token address. DERIVED 2022-08-24 from ethereum etherscan; identical to
# DEFI_MAJOR_ASSET_ADDRESSES["CBETH"] (kept inline per the single-token-adapter
# convention, mirroring renzo.py's ezETH address).
_CBETH_ETH_ADDRESS = "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704"  # DERIVED 2022-08-24 from ethereum etherscan

_LST_TOKENS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "CBETH",
            "contract_address": _CBETH_ETH_ADDRESS,
            "underlying": "ETH",
        },
    ],
}


def _get_deploy_date(chain: str) -> datetime:
    return _CBETH_ETH_DEPLOY_DATE


class CbethReferenceDataAdapter(BaseReferenceDataAdapter):
    """Coinbase cbETH reference data: single-token LST discovery (Ethereum).

    Coinbase launched cbETH on Ethereum mainnet 2022-08-24. cbETH is
    yield-bearing — its exchange rate versus ETH appreciates as Coinbase's
    ETH staking rewards accrue.
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
        return f"COINBASE-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Coinbase cbETH LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"COINBASE-{self._chain}"
        deploy_date = _get_deploy_date(self._chain)
        tokens = _LST_TOKENS_BY_CHAIN.get(self._chain, [])

        for token in tokens:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = build_instrument_id(venue_tag, InstrumentType.LST, symbol, passthrough=True)
            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                    # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                    canonical_instrument_id=instrument_key,
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.LST,
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
                    base_asset_contract_address=address,
                    base_asset_decimals=_CBETH_DECIMALS,
                )
            )
            # SPOT_ASSET sibling: the cbETH receipt token itself (NOT its "ETH"
            # economic-peg label) as a directly-queryable on-chain instrument,
            # reusing the SAME address/decimals just resolved above — no re-fetch.
            spot_asset = build_spot_asset_record(
                venue=venue_tag,
                symbol=symbol,
                contract_address=address,
                decimals=_CBETH_DECIMALS,
                available_from_datetime=deploy_date,
            )
            if spot_asset is not None:
                results.append(spot_asset)

        logger.info("cbETH: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("cbETH does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("cbETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("cbETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("cbETH OHLCV not supported via reference data")
