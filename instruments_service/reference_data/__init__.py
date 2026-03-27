"""unified-reference-data-interface: REST-only reference/static data for all venues.

Provides normalized access to:
- Instrument definitions (tick size, lot size, contract specs)
- Options chains (available strikes/expiries)
- Expiry/delivery calendars
- Funding rates (perpetuals)
- OHLCV bars
- Corporate actions (TradFi: IBKR/Bloomberg)

Usage:
    from instruments_service.reference_data import get_reference_adapter
    from instruments_service.reference_data import (
        InstrumentRecord, CanonicalOptionsChain,
        CanonicalExpiryCalendar, CanonicalCorporateAction,
        FundingRateRef, OHLCVRef,
    )

    adapter = get_reference_adapter("binance")
    instruments = await adapter.get_instruments(instrument_type="perp")
"""

from unified_api_contracts.internal import InstrumentRecord

from .adapters.sports.factory import (
    create_sports_reference_adapter as create_sports_reference_adapter,
)
from .base_adapter import BaseReferenceDataAdapter
from .factory import (
    ADAPTER_DATA_SOURCES,
    CANONICAL_VENUE_TO_ADAPTER,
    create_reference_data_adapter,
    get_adapter_for_canonical_venue,
)
from .router import ReferenceDataSourceConfig, create_reference_data_adapter_for_source
from .schemas import (
    CanonicalCorporateAction,
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

# Factory alias — preferred public API
get_reference_adapter = create_reference_data_adapter

# Router alias — preferred when data_source must be configurable
get_reference_adapter_for_source = create_reference_data_adapter_for_source

# Class alias — spec-compatible short name
BaseReferenceAdapter = BaseReferenceDataAdapter

__all__ = [
    "ADAPTER_DATA_SOURCES",
    "CANONICAL_VENUE_TO_ADAPTER",
    "BaseReferenceAdapter",
    "BaseReferenceDataAdapter",
    "CanonicalCorporateAction",
    "CanonicalExpiryCalendar",
    "CanonicalOptionsChain",
    "FundingRateRef",
    "InstrumentRecord",
    "OHLCVRef",
    "ReferenceDataSourceConfig",
    "create_reference_data_adapter",
    "create_reference_data_adapter_for_source",
    "create_sports_reference_adapter",
    "get_adapter_for_canonical_venue",
    "get_reference_adapter",
    "get_reference_adapter_for_source",
]
