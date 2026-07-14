"""Pacifica reference data adapter — curated Solana perp DEX instrument list.

Pacifica is a Solana DEX (Hyperliquid clone, mainnet 2025-06). The API does
not expose a public markets discovery endpoint, so the instrument universe is
derived from the same curated top-coin list used by the MTDS tick adapter.
All coins are returned as ACTIVE perpetual instruments.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Mirrors MTDS _PACIFICA_TOP_COINS (umi_tick_provider.py:682).
# Update both lists when new markets are confirmed live.
_PACIFICA_TOP_COINS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "HYPE",
    "XRP",
    "DOGE",
    "BNB",
    "SUI",
    "PUMP",
    "FARTCOIN",
)

# Pacifica Solana mainnet launch (2025-06-01 per MTDS _PACIFICA_FUNDING_START_MS).
_PACIFICA_DEPLOY_DATE = datetime(2025, 6, 1, tzinfo=UTC)

# Real margin type (2026-07-09, instrument_id_format_canonicalization_2026_07_08.md
# finding 1's PERPETUAL scope-expansion) — confirmed via real web research 2026-07-09:
# "Pacifica's core product is linear perpetual contracts" (industry coverage of
# Pacifica's own product docs), consistent with the already-confirmed USDC unified
# margin (docs.pacifica.fi/trading-on-pacifica/unified-margin, 2026-07-08) — margin
# and PnL are USDC-denominated for every market, not coin-margined. Derives the
# @LIN/@INV instrument_id marker FROM this field so the two can never drift.
_MARGIN_TYPE = MarginType.LINEAR
_MARGIN_MARKER = _MARGIN_TYPE.value[:3].upper()


class PacificaReferenceDataAdapter(BaseReferenceDataAdapter):
    """Pacifica reference data: curated perpetual instrument list.

    Returns all known Pacifica perp markets as InstrumentRecord objects with
    instrument_type=PERPETUAL, settle_asset=USDC, available_from=2025-06-01.

    No live API call is made — the instrument universe is the curated top-coin
    list, which matches the MTDS tick adapter. When Pacifica exposes a public
    /markets endpoint, this adapter should be updated to call it dynamically.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "PACIFICA-SOLANA"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return curated Pacifica perp instruments."""
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        results: list[InstrumentRecord] = []
        for coin in _PACIFICA_TOP_COINS:
            sym = f"{coin}-PERP"
            pacifica_instrument_key = build_instrument_id(
                "PACIFICA-SOLANA", InstrumentType.PERPETUAL, f"{coin}-USDC@{_MARGIN_MARKER}"
            )
            results.append(
                InstrumentRecord(
                    # Canonical instrument_id: VENUE:PERPETUAL:BASE-QUOTE@LIN|@INV
                    # (2026-07-08 canonicalization — dropped the PERP shorthand + the
                    # fake "-PERP" quote segment in favour of the real settlement
                    # currency. Confirmed live via docs.pacifica.fi/trading-on-pacifica/
                    # unified-margin 2026-07-08: "Pacifica users' account's USDC
                    # balance, unrealized PnL, and spot holdings are margined
                    # together" — perp PnL/margin is USDC-denominated for all
                    # markets. 2026-07-09 scope-expansion — added the real @LIN margin
                    # marker, see _MARGIN_TYPE above for the verification method). SSOT:
                    # plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
                    # finding 1 (2026-07-09 PERPETUAL scope-expansion) + finding 3+4;
                    # plans/active/canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md.
                    # Routed through the shared UAC builder (2026-07-09 retrofit,
                    # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 4) — the
                    # marker is embedded in the symbol passed to the builder (PERPETUAL's
                    # ``_build_cefi_simple`` upper-cases the symbol verbatim, same
                    # convention DeFi POOL fee-tiers already use).
                    instrument_key=pacifica_instrument_key,
                    # No CeFi raw-code-to-human-name translation gap (see other CeFi
                    # adapters' identical comment) — canonical_instrument_id mirrors
                    # instrument_key.
                    canonical_instrument_id=pacifica_instrument_key,
                    venue=self.venue,
                    raw_symbol=sym,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=coin,
                    quote_asset="USDC",
                    settle_asset="USDC",
                    margin_type=_MARGIN_TYPE,
                    tick_size=Decimal("0.0001"),
                    min_size=Decimal("0.001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=_PACIFICA_DEPLOY_DATE,
                    timezone="UTC",
                )
            )

        logger.info("Pacifica: returning %d curated perp instruments", len(results))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        for inst in await self.get_instruments():
            if inst.raw_symbol == symbol or inst.raw_symbol == symbol.upper():
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Pacifica does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Pacifica perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Pacifica funding rate not supported via reference data adapter")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Pacifica OHLCV not supported via reference data adapter")
