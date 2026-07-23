"""FX reference data adapter — instrument discovery for the curated FX spot pair universe.

FX has no Databento adapter (VENUE_TO_ADAPTER_KEY has no Databento entry for FX;
market data is Yahoo Finance via a hardcoded venue_upper == "FX" branch in MTDS's
umi_tick_provider.py). The instrument universe itself is small, static, and
already owned by UAC (``FX_SPOT_PAIRS``) — this adapter just emits it as
InstrumentRecords, the same way MTDS's Yahoo adapter already emits it as
canonical FX:SPOT_PAIR:{BASE}-{QUOTE} tick rows. No vendor API call needed:
the pair list barely changes, so there is nothing to "fetch".

Reference: unified_api_contracts.registry.tradfi_instrument_universe.FX_SPOT_PAIRS.
"""

import logging
from datetime import UTC, datetime

from unified_api_contracts import AssetClass, InstrumentType, build_instrument_id
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus
from unified_api_contracts.registry import FX_SPOT_PAIRS

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Matches MTDS's own FX coverage-start declaration
# (unified_api_contracts.registry.market_data_categories: "FX": {"ohlcv_24h": "2020-01-01"}).
_FX_AVAILABLE_FROM = datetime(2020, 1, 1, tzinfo=UTC)


class FxReferenceDataAdapter(BaseReferenceDataAdapter):
    """FX reference data: the curated spot-pair universe, static (no vendor call)."""

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "FX"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return the curated FX spot pair universe as SPOT_PAIR instruments."""
        if instrument_type not in (None, InstrumentType.SPOT_PAIR):
            return []

        results: list[InstrumentRecord] = []
        for pair in FX_SPOT_PAIRS:
            symbol = f"{pair.base}-{pair.quote}"
            # Byte-identical construction to MTDS's own
            # derive_tradfi_row_instrument_id(..., venue="FX", instrument_type=SPOT_PAIR)
            # -- both resolve through build_instrument_id's plain VENUE:TYPE:SYMBOL
            # branch (FX spot pairs carry no expiry/strike), giving FX:SPOT_PAIR:BASE-QUOTE.
            instrument_key = build_instrument_id("FX", InstrumentType.SPOT_PAIR, symbol)
            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    canonical_instrument_id=instrument_key,
                    venue="FX",
                    raw_symbol=pair.yahoo_ticker,
                    base_asset=pair.base,
                    quote_asset=pair.quote,
                    instrument_type=InstrumentType.SPOT_PAIR,
                    status=InstrumentStatus.ACTIVE,
                    asset_class=AssetClass.FX,
                    available_from_datetime=_FX_AVAILABLE_FROM,
                )
            )

        logger.info("FX: fetched %d static spot-pair instruments", len(results))
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
        raise NotImplementedError("FX spot pairs have no options chain")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("FX spot pairs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("FX spot pairs have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv; not supported via reference data (see MTDS for FX tick/OHLCV data)."""
        raise NotImplementedError("FX OHLCV not supported via reference data")
