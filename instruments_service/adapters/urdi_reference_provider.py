"""URDI reference data provider — the ONLY external API path for instruments-service.

All naming translation (canonical venue → URDI adapter) and credential routing
(URDI adapter → data source → API key) is owned by URDI itself:
  - CANONICAL_VENUE_TO_ADAPTER in unified_reference_data_interface.factory
  - ADAPTER_DATA_SOURCES in unified_reference_data_interface.factory
  - get_adapter_for_canonical_venue() in unified_reference_data_interface

instruments-service maintains no local translation tables. This file contains
only orchestration logic: which venues to fetch, how to gather results, error policy.

ERROR HANDLING POLICY
---------------------
- Network errors (ConnectionError, TimeoutError): WARNING + return [] (retryable)
- NotImplementedError: adapter unsupported instrument_type — DEBUG + return []
- ValueError: no URDI adapter for this venue — ERROR + skip
- Programming errors (TypeError etc.): reraise to fail the shard
"""

from __future__ import annotations

import asyncio
import logging

from unified_internal_contracts import InstrumentRecord
from unified_reference_data_interface import (
    ADAPTER_DATA_SOURCES,
    CANONICAL_VENUE_TO_ADAPTER,
    get_adapter_for_canonical_venue,
)

logger = logging.getLogger(__name__)

# Covered canonical venues — services check membership without importing the full dict.
URDI_SUPPORTED_VENUES: frozenset[str] = frozenset(CANONICAL_VENUE_TO_ADAPTER.keys())


async def fetch_instruments_for_all_venues(
    venues: list[str],
    instrument_type: str | None = None,
    api_keys: dict[str, str] | None = None,
    date: str | None = None,
) -> list[InstrumentRecord]:
    """Fetch canonical InstrumentRecord[] for all configured venues via URDI.

    Args:
        venues: UAC canonical venue names (e.g. "UNISWAPV3-ETHEREUM").
        instrument_type: Optional URDI type filter. None = all.
        api_keys: {data_source: api_key} from UTL validate_api_keys_for_venues().
                  Injected into adapters at construction time.
        date: ISO date string (YYYY-MM-DD). Passed to date-aware adapters
              (e.g. API-Football) so they fetch only fixtures for that day.
              Adapters that don't need date filtering ignore this parameter.

    Returns:
        Flat list of InstrumentRecord in canonical format from URDI adapters.
    """
    if not venues:
        return []

    # Separate covered/uncovered; deduplicate by adapter key
    seen: set[str] = set()
    fetch_list: list[tuple[str, str]] = []  # (canonical, adapter_key)
    unsupported: list[str] = []

    for canonical in venues:
        adapter_key = CANONICAL_VENUE_TO_ADAPTER.get(canonical)
        if adapter_key is None:
            unsupported.append(canonical)
        elif adapter_key not in seen:
            seen.add(adapter_key)
            fetch_list.append((canonical, adapter_key))

    if unsupported:
        logger.warning(
            "No URDI adapter for %d venue(s) — add entry to CANONICAL_VENUE_TO_ADAPTER "
            "in unified-reference-data-interface/factory.py: %s",
            len(unsupported),
            unsupported,
        )

    if not fetch_list:
        return []

    async def _fetch_one(canonical: str, adapter_key: str) -> list[InstrumentRecord]:
        try:
            data_source = ADAPTER_DATA_SOURCES.get(adapter_key, "")
            api_key = (api_keys or {}).get(data_source) if data_source else None
            adapter = get_adapter_for_canonical_venue(
                canonical,
                api_key=api_key,
                date=date,
                extra_api_keys=api_keys,
            )
            records = await adapter.get_instruments(instrument_type=instrument_type)
            logger.info("URDI[%s]: fetched %d instruments", canonical, len(records))
            return records  # type already list[InstrumentRecord] from URDI
        except NotImplementedError:
            logger.debug("URDI[%s]: instrument_type=%r not supported", canonical, instrument_type)
            return []
        except (OSError, ConnectionError, TimeoutError) as exc:
            logger.warning("URDI[%s]: network error (retryable): %s", canonical, exc)
            return []
        except ValueError as exc:
            logger.error("URDI[%s]: adapter error: %s", canonical, exc)
            return []
        # Programming errors propagate — fail the shard

    results = await asyncio.gather(*[_fetch_one(c, k) for c, k in fetch_list])
    return [record for batch in results for record in batch]


async def fetch_instruments_via_urdi(
    venue: str,
    instrument_type: str | None = None,
    api_keys: dict[str, str] | None = None,
    date: str | None = None,
) -> list[InstrumentRecord]:
    """Single-venue fetch. Delegates to fetch_instruments_for_all_venues."""
    return await fetch_instruments_for_all_venues(
        [venue], instrument_type=instrument_type, api_keys=api_keys, date=date
    )
