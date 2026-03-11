"""URDI reference data provider for instruments-service.

Wraps unified-reference-data-interface to fetch canonical InstrumentRecord
objects for each venue. Used as a supplementary instrument discovery source
alongside Tardis/CCXT for venues that have URDI adapters.

Wiring decision (which venues route through URDI) lives here as the
URDI_SUPPORTED_VENUES constant — the canonical registry of venues for which
get_reference_adapter(venue).get_instruments() is available.
"""

import logging
from typing import Literal

from unified_reference_data_interface import InstrumentRecord, get_reference_adapter

logger = logging.getLogger(__name__)

# Venues with working URDI adapters (get_instruments() implemented).
# Kept here so instruments-service has a single place to expand when new
# URDI adapters land.
URDI_SUPPORTED_VENUES: frozenset[str] = frozenset(
    {
        "binance",
        "bybit",
        "okx",
        "deribit",
        "coinbase",
        "hyperliquid",
        "polymarket",
        "polygon",
        "tardis",
    }
)

InstrumentType = Literal["perp", "spot", "option", "future", "all"]


async def fetch_instruments_via_urdi(
    venue: str,
    instrument_type: InstrumentType = "perp",
) -> list[InstrumentRecord]:
    """Fetch canonical instrument records for a single venue via URDI.

    Args:
        venue: Venue name (must be in URDI_SUPPORTED_VENUES).
        instrument_type: One of perp / spot / option / future / all.

    Returns:
        List of InstrumentRecord objects, empty on error or unsupported venue.
    """
    if venue not in URDI_SUPPORTED_VENUES:
        logger.debug("urdi_reference_provider: venue %r not in URDI_SUPPORTED_VENUES — skipped", venue)
        return []

    try:
        adapter = get_reference_adapter(venue)
        instruments = await adapter.get_instruments(instrument_type=instrument_type)
        logger.info(
            "urdi_reference_provider: fetched %d %s instruments for venue=%s",
            len(instruments),
            instrument_type,
            venue,
        )
        return instruments
    except NotImplementedError:
        # Some adapters raise NotImplementedError for unsupported instrument_type
        logger.debug(
            "urdi_reference_provider: venue=%s does not support instrument_type=%r — skipped",
            venue,
            instrument_type,
        )
        return []
    except (OSError, ConnectionError, ValueError, RuntimeError) as exc:
        logger.warning(
            "urdi_reference_provider: failed to fetch instruments for venue=%s instrument_type=%s: %s",
            venue,
            instrument_type,
            exc,
        )
        return []


async def fetch_instruments_for_venues(
    venues: list[str],
    instrument_type: InstrumentType = "perp",
) -> dict[str, list[InstrumentRecord]]:
    """Fetch canonical instrument records for multiple venues concurrently.

    Skips venues not in URDI_SUPPORTED_VENUES. Failures per venue are logged
    and return an empty list — they do not propagate exceptions.

    Args:
        venues: List of venue names to fetch instruments for.
        instrument_type: One of perp / spot / option / future / all.

    Returns:
        Mapping of venue → list[InstrumentRecord]. Only venues with at least
        one record are included in the result dict.
    """
    import asyncio

    tasks = {v: fetch_instruments_via_urdi(v, instrument_type) for v in venues if v in URDI_SUPPORTED_VENUES}
    if not tasks:
        return {}

    results = await asyncio.gather(*tasks.values(), return_exceptions=False)
    return {venue: records for venue, records in zip(tasks.keys(), results, strict=True) if records}
