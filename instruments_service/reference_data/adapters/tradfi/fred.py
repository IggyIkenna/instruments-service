"""FRED reference data adapter — instrument discovery for the curated macro-series universe.

FRED (Federal Reserve Economic Data) has no Databento adapter (VENUE_TO_ADAPTER_KEY
had no reference-data entry for it until this file — see
unified-trading-pm/plans/active/issues/macro_micro_econ_data_capture_audit_2026_06_05.md,
"Build the instruments-service 'fred' URDI reference-data adapter"). Market-data TICK
capture is already fully wired independent of this adapter
(market-tick-data-service's ``market_interface/adapters/tradfi/fred_adapter.py`` +
the ``_umi_fred`` dispatch route) — this adapter only closes the reference-data
(instrument-catalogue) side, mirroring the FX precedent
(``instruments_service/reference_data/adapters/tradfi/fx.py``): a small static
list, no vendor call.

_KEY_SERIES below is a same-shape duplicate of MTDS's
``FredAdapter.KEY_SERIES`` (28 curated series — the plan todo's "29-series"
description was a stale headcount; this file mirrors the real list). It is
duplicated rather than
imported because instruments-service MUST NOT depend on market-tick-data-service
(no service-to-service imports — codex/04-architecture/tier-and-import-architecture.md).
Keep the two lists in sync if either changes.
"""

import logging
from datetime import UTC, datetime

from unified_api_contracts import AssetClass, InstrumentType, build_instrument_id
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Matches MTDS's own FRED coverage-start declaration
# (unified_api_contracts.registry.market_data_categories.SOURCE_COVERAGE_START["FRED"]
# = date(1962, 1, 2), the DGS Treasury series depth — FRED has no per-series start-date
# axis there, so this reference adapter mirrors the same single floor).
_FRED_AVAILABLE_FROM = datetime(1962, 1, 2, tzinfo=UTC)

# Byte-identical to MTDS's FredAdapter.KEY_SERIES
# (market-tick-data-service/market_tick_data_service/market_interface/adapters/tradfi/fred_adapter.py).
_KEY_SERIES: list[dict[str, str]] = [
    # Volatility
    {"series_id": "VIXCLS", "name": "CBOE Volatility Index (VIX)", "frequency": "daily", "category": "volatility"},
    # US Treasury yields
    {"series_id": "DGS1MO", "name": "1-Month Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS3MO", "name": "3-Month Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS6MO", "name": "6-Month Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS1", "name": "1-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS2", "name": "2-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS5", "name": "5-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS7", "name": "7-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS10", "name": "10-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS20", "name": "20-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    {"series_id": "DGS30", "name": "30-Year Treasury", "frequency": "daily", "category": "yield_curve"},
    # TIPS (real yields)
    {"series_id": "DFII5", "name": "5-Year TIPS", "frequency": "daily", "category": "tips"},
    {"series_id": "DFII10", "name": "10-Year TIPS", "frequency": "daily", "category": "tips"},
    {"series_id": "DFII30", "name": "30-Year TIPS", "frequency": "daily", "category": "tips"},
    # Rates
    {"series_id": "FEDFUNDS", "name": "Federal Funds Rate", "frequency": "monthly", "category": "rates"},
    {"series_id": "DFF", "name": "Federal Funds Effective Rate", "frequency": "daily", "category": "rates"},
    {"series_id": "SOFR", "name": "Secured Overnight Financing Rate", "frequency": "daily", "category": "rates"},
    # Spreads
    {
        "series_id": "T10Y2Y",
        "name": "10-Year minus 2-Year Treasury Spread",
        "frequency": "daily",
        "category": "spreads",
    },
    {
        "series_id": "T10Y3M",
        "name": "10-Year minus 3-Month Treasury Spread",
        "frequency": "daily",
        "category": "spreads",
    },
    {"series_id": "BAMLH0A0HYM2", "name": "ICE BofA US High Yield OAS", "frequency": "daily", "category": "credit"},
    # Inflation
    {"series_id": "CPIAUCSL", "name": "CPI All Urban Consumers", "frequency": "monthly", "category": "inflation"},
    {"series_id": "T10YIE", "name": "10-Year Breakeven Inflation", "frequency": "daily", "category": "inflation"},
    # Growth
    {"series_id": "GDPC1", "name": "Real GDP", "frequency": "quarterly", "category": "macro"},
    {"series_id": "UNRATE", "name": "Unemployment Rate", "frequency": "monthly", "category": "macro"},
    {"series_id": "PAYEMS", "name": "Nonfarm Payrolls", "frequency": "monthly", "category": "macro"},
    {"series_id": "GDP", "name": "Nominal GDP", "frequency": "quarterly", "category": "macro"},
    {"series_id": "ICSA", "name": "Initial Jobless Claims", "frequency": "weekly", "category": "macro"},
    {"series_id": "PCEPI", "name": "PCE Price Index", "frequency": "monthly", "category": "inflation"},
]

# Category → UAC InstrumentType, byte-identical to MTDS's
# FredAdapter._FRED_CATEGORY_TO_UAC (fred_adapter.py) — the SSOT for how a
# KEY_SERIES category maps onto the canonical instrument-id TYPE segment.
_CATEGORY_TO_INSTRUMENT_TYPE: dict[str, InstrumentType] = {
    "volatility": InstrumentType.INDEX,
    "yield_curve": InstrumentType.BOND,
    "tips": InstrumentType.BOND,
    "rates": InstrumentType.BOND,
    "spreads": InstrumentType.BOND,
    "credit": InstrumentType.BOND,
    "inflation": InstrumentType.INDEX,
    "macro": InstrumentType.INDEX,
}

# Category → UAC AssetClass. BOND-classified categories (rate/yield/spread/credit
# series) are FIXED_INCOME; INDEX-classified categories follow the existing
# VX/VIX precedent (tradfi_instrument_universe.py: "VX/VIX is a volatility
# product ... asset_group=COMMODITY") since AssetClass has no dedicated
# macro-economic-index bucket.
_CATEGORY_TO_ASSET_CLASS: dict[str, AssetClass] = {
    "volatility": AssetClass.COMMODITY,
    "yield_curve": AssetClass.FIXED_INCOME,
    "tips": AssetClass.FIXED_INCOME,
    "rates": AssetClass.FIXED_INCOME,
    "spreads": AssetClass.FIXED_INCOME,
    "credit": AssetClass.FIXED_INCOME,
    "inflation": AssetClass.COMMODITY,
    "macro": AssetClass.COMMODITY,
}


class FredReferenceDataAdapter(BaseReferenceDataAdapter):
    """FRED reference data: the curated KEY_SERIES macro catalogue, static (no vendor call)."""

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "FRED"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return the curated FRED KEY_SERIES catalogue as BOND/INDEX instruments."""
        results: list[InstrumentRecord] = []
        for entry in _KEY_SERIES:
            series_id = entry["series_id"]
            category = entry["category"]
            entry_type = _CATEGORY_TO_INSTRUMENT_TYPE[category]
            if instrument_type is not None and entry_type != instrument_type:
                continue
            # Byte-identical construction to MTDS's own
            # derive_tradfi_row_instrument_id(..., venue="FRED", instrument_type=entry_type)
            # -- both resolve through build_instrument_id's TradFi-cash branch, which
            # appends the operator-decided -USD suffix uniformly for INDEX/BOND
            # (tradfi_consolidated_closeout_2026_07_18.md), giving
            # FRED:{BOND|INDEX}:{SERIES_ID}-USD.
            instrument_key = build_instrument_id("FRED", entry_type, series_id)
            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    canonical_instrument_id=instrument_key,
                    venue="FRED",
                    raw_symbol=series_id,
                    base_asset=series_id,
                    quote_asset="USD",
                    instrument_type=entry_type,
                    status=InstrumentStatus.ACTIVE,
                    asset_class=_CATEGORY_TO_ASSET_CLASS[category],
                    available_from_datetime=_FRED_AVAILABLE_FROM,
                )
            )

        logger.info("FRED: fetched %d static KEY_SERIES instruments", len(results))
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
        raise NotImplementedError("FRED series have no options chain")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("FRED series have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("FRED series have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv; not supported via reference data (see MTDS for FRED observation data)."""
        raise NotImplementedError("FRED OHLCV not supported via reference data")
