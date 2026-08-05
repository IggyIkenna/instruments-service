"""Binance wBETH reference data adapter — instrument discovery for the wBETH LST.

Discovers Wrapped Binance Beacon ETH (wBETH) on Ethereum mainnet and BNB Smart
Chain (BSC). wBETH is Binance's value-accruing liquid staking token: 1 wBETH
represents 1 staked ETH plus the ETH-staking rewards accrued since launch, so its
exchange rate versus ETH appreciates over time. Returned as an ``InstrumentRecord``
with instrument_type="LST" (same class as the other single-token LST adapters —
see ``renzo.py`` / ``lido.py`` for the full rationale on the LST instrument_type).

References:
- https://www.binance.com/en/support/faq/what-is-wbeth-e252366155174ba6887f6b32e3798273
  (Binance wBETH support FAQ) and the launch announcement
  https://www.binance.com/en/support/announcement/binance-introduces-wrapped-beacon-eth-wbeth-on-eth-staking-a1197f34d832445db41654ad01f56b4d
  (WBETH live on ETH Staking effective 2023-04-27 08:00 UTC).
- wBETH address: see ``_WBETH_ADDRESS`` below. Binance deploys wBETH at the SAME
  address on Ethereum mainnet AND BSC (Etherscan + BscScan-verified):
  https://etherscan.io/token/0xa2E3356610840701BDf5611a53974510Ae27E2e1  # DERIVED 2023-04-27 from ethereum etherscan
  and
  https://bscscan.com/token/0xa2E3356610840701BDf5611a53974510Ae27E2e1  # DERIVED 2023-04-27 from bsc bscscan
- Launch dates sourced from unified_api_contracts.registry.chain_env.PROTOCOL_LAUNCH_DATES
  (("ETHEREUM", "BINANCE") == 2023-04-27; ("BSC", "BINANCE") == 2023-04-27).

The canonical venue tag is BINANCE-<CHAIN> (the issuing protocol is Binance);
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

# wBETH GA on ETH Staking (2023-04-27 08:00 UTC) — mirrors
# PROTOCOL_LAUNCH_DATES[("ETHEREUM", "BINANCE")] / [("BSC", "BINANCE")]. The same
# token launched on both chains at the same address, so both chains share the date.
_WBETH_DEPLOY_DATE = datetime(2023, 4, 27, tzinfo=UTC)

_WBETH_DECIMALS = 18
# wBETH token address — identical on Ethereum mainnet AND BSC per Binance's
# deployment (DERIVED 2023-04-27 from ethereum etherscan + bsc bscscan).
_WBETH_ADDRESS = (
    "0xa2E3356610840701BDf5611a53974510Ae27E2e1"  # DERIVED 2023-04-27 from ethereum etherscan / bsc bscscan
)

_LST_TOKENS_BY_CHAIN: dict[str, list[dict[str, str]]] = {
    "ETHEREUM": [
        {
            "symbol": "WBETH",
            "contract_address": _WBETH_ADDRESS,
            "underlying": "ETH",
        },
    ],
    "BSC": [
        {
            "symbol": "WBETH",
            "contract_address": _WBETH_ADDRESS,
            "underlying": "ETH",
        },
    ],
}


def _get_deploy_date(chain: str) -> datetime:
    return _WBETH_DEPLOY_DATE


class WbethReferenceDataAdapter(BaseReferenceDataAdapter):
    """Binance wBETH reference data: single-token LST discovery (Ethereum + BSC).

    Binance launched wBETH on ETH Staking 2023-04-27. wBETH is a value-accruing
    liquid staking token deployed at the same contract address on Ethereum mainnet
    and BSC; chain is encoded in the canonical venue tag (BINANCE-ETHEREUM /
    BINANCE-BSC) and is the per-chain shard atom in the manifest.
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
        return f"BINANCE-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Binance wBETH LST tokens as yield-bearing instruments."""
        if instrument_type not in (None, InstrumentType.LST, InstrumentType.YIELD_BEARING):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"BINANCE-{self._chain}"
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
                    base_asset_decimals=_WBETH_DECIMALS,
                )
            )
            # SPOT_ASSET sibling: the wBETH receipt token itself (NOT its "ETH"
            # economic-peg label) as a directly-queryable on-chain instrument,
            # reusing the SAME address/decimals just resolved above — no re-fetch.
            spot_asset = build_spot_asset_record(
                venue=venue_tag,
                symbol=symbol,
                contract_address=address,
                decimals=_WBETH_DECIMALS,
                available_from_datetime=deploy_date,
            )
            if spot_asset is not None:
                results.append(spot_asset)

        logger.info("wBETH: fetched %d LST instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("wBETH does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("wBETH has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("wBETH has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("wBETH OHLCV not supported via reference data")
