"""EtherFi ETHFI governance token reference data adapter.

Discovers the ETHFI governance token on Ethereum mainnet. ETHFI is the governance
token of the EtherFi protocol. It is distributed as seasonal (quarterly) rewards
to weETH holders and operators.

Returned as InstrumentRecord with instrument_type=SPOT_ASSET — ETHFI is a SINGLE
on-chain governance token (its data need is oracle-price / transfer / governance),
NOT a two-token quoted market, so it is a `SPOT_ASSET`, not a `SPOT_PAIR`
(operator ruling 2026-07-18, defi_consolidated_closeout_2026_07_18.md
"SPOT_ASSET vs SPOT_PAIR vs POOL": for asset_group=defi a `SPOT_PAIR` REQUIRES a
two-token `BASE-QUOTE` symbol; a single on-chain token is a `SPOT_ASSET`).
Routed through `build_canonical_instrument_id` so the emitted `instrument_key`
TYPE segment is correct AT MINT TIME (`ETHERFI-GOV-ETHEREUM:SPOT_ASSET:ETHFI`) —
the UAC entry point now hard-rejects a single-token `SPOT_PAIR`. Because
`SPOT_ASSET` is a DeFi on-chain type, the record also carries
`base_asset_contract_address` + `base_asset_decimals` (the ERC-20 contract + 18
decimals) so the InstrumentRecord DeFi-on-chain validator is satisfied. The
`get_instruments` type-filter guard accepts the adapter's own
`InstrumentType.SPOT_ASSET` value (plus the legacy
`"GOVERNANCE_TOKEN"`/`"governance_token"` back-compat strings).

Contract address: see ``_ETHFI_ADDRESS`` below (ETHFI on Ethereum mainnet; Etherscan-verified).
Reference: https://www.ether.fi/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts import AssetGroup, build_canonical_instrument_id
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

# EtherFi ETHFI token TGE date (2024-03-18).
_ETHFI_DEPLOY_DATE = datetime(2024, 3, 18, tzinfo=UTC)

# ETHFI contract address on Ethereum mainnet
_ETHFI_ADDRESS = "0xFe0c30065B384F05761f15d0CC899D4F9F9Cc0eB"  # DERIVED 2024-03-18 from ethereum etherscan

# ETHFI is a standard ERC-20 governance token with 18 decimals — fixed contract metadata.
_ETHFI_DECIMALS = 18
_ETHFI_ONCHAIN_SYMBOL = "ETHFI"

# ETHFI is also traded on Binance as a spot pair
# Actual Binance spot pair instruments are produced by BinanceReferenceDataAdapter.
ETHFI_BINANCE_SYMBOLS = ["ETHFIUSDT", "ETHFIETH"]

_GOVERNANCE_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "ETHFI",
        "contract_address": _ETHFI_ADDRESS,
        "underlying": "ETH",
        "token_type": "GOVERNANCE_TOKEN",
    },
]


class EthFiGovernanceReferenceDataAdapter(BaseReferenceDataAdapter):
    """EtherFi ETHFI governance token reference data adapter.

    ETHFI launched March 2024 (TGE). It is the governance token for the EtherFi
    protocol and is distributed quarterly as seasonal airdrop rewards to weETH
    holders and node operators.

    Note: This adapter is ONLY for the ETHFI governance token.
    For weETH (yield-bearing LST) instruments, use EtherFiReferenceDataAdapter.
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
        return f"ETHERFI-GOV-{self._chain}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return ETHFI governance token as an instrument record."""
        if instrument_type not in (None, InstrumentType.SPOT_ASSET, "GOVERNANCE_TOKEN", "governance_token"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"ETHERFI-GOV-{self._chain}"

        for token in _GOVERNANCE_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            instrument_key = build_canonical_instrument_id(
                AssetGroup.DEFI, venue_tag, InstrumentType.SPOT_ASSET, symbol, passthrough=True
            )
            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                    # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                    canonical_instrument_id=instrument_key,
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.SPOT_ASSET,
                    base_asset=symbol,
                    quote_asset=underlying,
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=underlying,
                    available_from_datetime=_ETHFI_DEPLOY_DATE,
                    # ETHFI is a vanilla single-asset ERC-20 governance token —
                    # base_asset_contract_address is the ETHFI contract itself
                    # (it IS the on-chain instrument). Required for the SPOT_ASSET
                    # DeFi-on-chain InstrumentRecord validator (on-chain identifier
                    # + base_asset_decimals both non-null). 18 decimals is the
                    # standard ERC-20 default declared by the deployed contract.
                    base_asset_contract_address=address,
                    base_asset_decimals=_ETHFI_DECIMALS,
                    base_asset_symbol_onchain=_ETHFI_ONCHAIN_SYMBOL,
                )
            )

        logger.info("EtherFi-governance: fetched %d ETHFI instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("ETHFI governance token does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("ETHFI governance token has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("ETHFI governance token has no on-chain funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("ETHFI OHLCV not supported via reference data — use MTDS/MDPS")
