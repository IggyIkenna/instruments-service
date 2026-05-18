"""EigenLayer reference data adapter — instrument discovery for EIGEN governance token.

Discovers the EIGEN token on Ethereum mainnet. EIGEN is the governance and restaking
token of the EigenLayer protocol. It is staked on EigenLayer and earns weekly rewards
from AVS operators.

Contract address: 0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83 (EIGEN on Ethereum mainnet)
Reference: https://docs.eigenlayer.xyz/eigenlayer/restaking-guides/restaking-user-guide
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

# EigenLayer EIGEN token deployment date (2024-09-17 — TGE).
_EIGEN_DEPLOY_DATE = datetime(2024, 9, 17, tzinfo=UTC)

# EIGEN contract address on Ethereum mainnet
_EIGEN_ADDRESS = "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"

# EIGEN is a standard ERC-20 governance token with 18 decimals — fixed contract metadata.
_EIGEN_DECIMALS = 18
_EIGEN_ONCHAIN_SYMBOL = "EIGEN"

# EIGEN is also traded on Binance as a spot pair — defined here for cross-reference
# Actual Binance spot pair instruments are produced by the BinanceReferenceDataAdapter.
EIGEN_BINANCE_SYMBOLS = ["EIGENUSDT", "EIGENETH"]

_GOVERNANCE_TOKENS: list[dict[str, str]] = [
    {
        "symbol": "EIGEN",
        "contract_address": _EIGEN_ADDRESS,
        "underlying": "ETH",
        "token_type": "GOVERNANCE_TOKEN",
    },
]


class EigenLayerReferenceDataAdapter(BaseReferenceDataAdapter):
    """EigenLayer reference data: EIGEN governance token discovery.

    EIGEN launched September 2024. It is a governance token for EigenLayer
    that also serves as a restaking reward token — EigenLayer AVS operators
    distribute EIGEN rewards weekly via the RewardsCoordinator contract.
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
        return "eigenlayer"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return EIGEN governance token as an instrument record."""
        if instrument_type not in (None, "GOVERNANCE_TOKEN", "governance_token"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"EIGENLAYER-{self._chain}"

        for token in _GOVERNANCE_TOKENS:
            symbol = token["symbol"]
            address = token["contract_address"]
            underlying = token["underlying"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:GOVERNANCE_TOKEN:{symbol}",
                    venue=venue_tag,
                    raw_symbol=address,
                    instrument_type=InstrumentType.SPOT_PAIR,
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
                    available_from_datetime=_EIGEN_DEPLOY_DATE,
                    # DeFi metadata (Phase 2c of
                    # instruments_service_metadata_refactor_2026_04_29). EIGEN
                    # is a vanilla ERC-20 governance token — pool_address and
                    # base_asset_contract_address both resolve to the EIGEN
                    # contract itself (it IS the on-chain instrument). 18
                    # decimals is the standard ERC-20 default declared by the
                    # deployed contract. quote_asset/atoken/debt_token are
                    # left None — EIGEN is single-asset and not Aave-shaped.
                    pool_address=address,
                    base_asset_contract_address=address,
                    base_asset_decimals=_EIGEN_DECIMALS,
                    base_asset_symbol_onchain=_EIGEN_ONCHAIN_SYMBOL,
                )
            )

        logger.info("EigenLayer: fetched %d EIGEN instruments on %s", len(results), self._chain)
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
        raise NotImplementedError("EigenLayer EIGEN token does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("EigenLayer EIGEN token has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("EigenLayer EIGEN token has no on-chain funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("EigenLayer OHLCV not supported via reference data — use MTDS/MDPS")
