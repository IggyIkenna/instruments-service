"""Solana native staking reference data adapter — instrument discovery.

Provides a static instrument record for Solana native SOL staking.
The instrument represents the on-chain native staking yield vehicle:
delegating SOL to validators via the Solana stake program.

Instrument type: STAKING (not LST — no liquid token; funds are locked).
Venue: SOLANA-NATIVE (distinguishes from LST venues like JITO-SOLANA).
Data type: native_staking_rates (UAC SchemaContract added 2026-05-15).

Market data (per-epoch base_apy, mev_apy, total_apy) is collected by the
MTDS native_staking_handler using:
  - Solana RPC: getInflationRate + getEpochInfo (no API key)
  - Helius APY endpoint: per-validator APY breakdown (BLOCKED-CREDENTIALS)

Solana mainnet genesis: 2020-03-16. Pre-genesis dates yield
record_empty(reason=EXPECTED_PRE_GENESIS_CHAIN) from the MTDS handler.
"""

from __future__ import annotations

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

# Solana mainnet genesis block: 2020-03-16 (epoch 0 start).
_SOLANA_GENESIS_DATE = datetime(2020, 3, 16, tzinfo=UTC)

# Venue tag for Solana native staking (distinct from LST venues).
_VENUE_TAG = "SOLANA-NATIVE"

# The SOL native token mint address (Wrapped SOL on Solana).
# Native staking uses the Solana stake program directly; "raw_symbol"
# here is the canonical SOL mint for instrument identification.
_SOL_MINT = "So11111111111111111111111111111111111111112"
_SOL_DECIMALS = 9

_STAKING_INSTRUMENTS: list[dict[str, str]] = [
    {
        "symbol": "SOL",
        "mint_address": _SOL_MINT,
        "description": "Solana native staking (network aggregate)",
    },
]


class SolanaNativeStakingAdapter(BaseReferenceDataAdapter):
    """Solana native staking reference data: SOL staking instrument discovery.

    Provides a single STAKING instrument for Solana native SOL staking.
    The instrument represents delegated SOL (network-aggregate view); the
    MTDS native_staking_handler queries per-epoch rates from Solana RPC.

    Pure static-registry adapter: credential-free, no network calls.

    BLOCKED-CREDENTIALS (partial): Per-validator APY breakdown requires a
    Helius API key. The MTDS handler uses it for mev_apy field. Operator
    ping filed in ikenna_orchestrator/pings/slot_2.md (2026-05-15).
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
        return "solana_native"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return Solana native staking as a STAKING instrument."""
        if instrument_type not in (None, "staking", "STAKING"):
            return []

        results: list[InstrumentRecord] = []
        venue_tag = f"SOLANA-NATIVE-{self._chain}"

        for inst in _STAKING_INSTRUMENTS:
            symbol = inst["symbol"]
            mint = inst["mint_address"]

            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:STAKING:{symbol}",
                    venue=venue_tag,
                    raw_symbol=mint,
                    instrument_type=InstrumentType.STAKING,
                    base_asset=symbol,
                    quote_asset="",
                    tick_size=Decimal("0.000000001"),
                    min_size=Decimal("0.000000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=symbol,
                    available_from_datetime=_SOLANA_GENESIS_DATE,
                    base_asset_contract_address=mint,
                    base_asset_decimals=_SOL_DECIMALS,
                )
            )

        logger.info(
            "SolanaNativeStaking: fetched %d STAKING instruments on %s",
            len(results),
            self._chain,
        )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
        raise NotImplementedError("Solana native staking has no options chain")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Solana native staking has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Solana native staking has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Solana native staking OHLCV not supported")
