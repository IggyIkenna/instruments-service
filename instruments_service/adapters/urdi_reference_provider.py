"""URDI reference data provider — the ONLY external API path for instruments-service.

All naming translation (canonical venue → URDI adapter) and credential routing
(URDI adapter → data source → API key) is owned by URDI itself:
  - CANONICAL_VENUE_TO_ADAPTER in instruments_service.reference_data.factory
  - ADAPTER_DATA_SOURCES in instruments_service.reference_data.factory
  - get_adapter_for_canonical_venue() in instruments_service.reference_data

instruments-service maintains no local translation tables. This file contains
only orchestration logic: which venues to fetch, how to gather results, error policy.

ERROR HANDLING POLICY
---------------------
- Network errors (ConnectionError, TimeoutError): WARNING + return [] (retryable)
- NotImplementedError: adapter unsupported instrument_type — DEBUG + return []
- ValueError: no URDI adapter for this venue — ERROR + skip
- Programming errors (TypeError etc.): reraise to fail the shard

Failed venues are tracked in `VenueFetchResult.failed_venues` with their error
classification (retryable/permanent) so the orchestrator can make retry decisions.
"""

from __future__ import annotations

import asyncio
import logging

from unified_api_contracts.internal import InstrumentRecord

from instruments_service.reference_data import (
    ADAPTER_DATA_SOURCES,
    CANONICAL_VENUE_TO_ADAPTER,
    get_adapter_for_canonical_venue,
)

logger = logging.getLogger(__name__)

# Covered canonical venues — services check membership without importing the full dict.
URDI_SUPPORTED_VENUES: frozenset[str] = frozenset(CANONICAL_VENUE_TO_ADAPTER.keys())


class VenueError:
    """Error classification for a failed venue fetch."""

    def __init__(self, venue: str, error_code: str, message: str, retryable: bool) -> None:
        self.venue = venue
        self.error_code = error_code  # RATE_LIMIT, NETWORK, TIMEOUT, PARSE_ERROR, ADAPTER_ERROR, UNSUPPORTED
        self.message = message
        self.retryable = retryable


class VenueFetchResult:
    """Result of fetching instruments for multiple venues.

    Separates records from error info so the orchestrator can decide
    which failed venues to retry based on error classification.
    """

    def __init__(
        self,
        records: list[InstrumentRecord] | None = None,
        failed_venues: list[VenueError] | None = None,
    ) -> None:
        self.records = records if records is not None else []
        self.failed_venues = failed_venues if failed_venues is not None else []

    @property
    def retryable_venues(self) -> list[str]:
        """Venues that failed with retryable errors (rate limit, network, timeout)."""
        return [v.venue for v in self.failed_venues if v.retryable]


async def fetch_instruments_for_all_venues(
    venues: list[str],
    instrument_type: str | None = None,
    api_keys: dict[str, str] | None = None,
    date: str | None = None,
    mode: str = "batch",
) -> VenueFetchResult:
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
        VenueFetchResult with records and per-venue error classifications.
    """
    if not venues:
        return VenueFetchResult()

    # Separate covered/uncovered; deduplicate by canonical venue name
    # (not adapter key — same adapter serves multiple chains)
    seen: set[str] = set()
    fetch_list: list[tuple[str, str]] = []  # (canonical, adapter_key)
    unsupported: list[str] = []

    for canonical in venues:
        adapter_key = CANONICAL_VENUE_TO_ADAPTER.get(canonical)
        if adapter_key is None:
            unsupported.append(canonical)
        elif canonical not in seen:
            seen.add(canonical)
            fetch_list.append((canonical, adapter_key))

    failed: list[VenueError] = []
    if unsupported:
        logger.warning(
            "No URDI adapter for %d venue(s) — add entry to CANONICAL_VENUE_TO_ADAPTER "
            "in unified-reference-data-interface/factory.py: %s",
            len(unsupported),
            unsupported,
        )
        for v in unsupported:
            failed.append(VenueError(v, "UNSUPPORTED", "No URDI adapter registered", retryable=False))

    if not fetch_list:
        return VenueFetchResult(failed_venues=failed)

    # Cap concurrent adapter calls at 4 to avoid overloading APIs
    sem = asyncio.Semaphore(4)

    async def _fetch_one(canonical: str, adapter_key: str) -> list[InstrumentRecord]:
        async with sem:
            try:
                data_source = ADAPTER_DATA_SOURCES.get(adapter_key, "")
                api_key = (api_keys or {}).get(data_source) if data_source else None
                adapter = get_adapter_for_canonical_venue(
                    canonical,
                    api_key=api_key,
                    date=date,
                    extra_api_keys=api_keys,
                    mode=mode,
                )
                # Use cached path — adapter pool ensures reuse, cache avoids redundant fetches
                records = await adapter.get_instruments_cached(instrument_type=instrument_type, date=date)
                logger.info("URDI[%s]: fetched %d instruments", canonical, len(records))
                # Venue-tag validation: every returned instrument must have a venue
                # matching the canonical venue we requested. Prevents adapters that
                # ignore their chain parameter from writing data under the wrong venue.
                mismatched = [r for r in records if getattr(r, "venue", "").upper() != canonical.upper()]
                if mismatched:
                    wrong_venues = {getattr(r, "venue", "") for r in mismatched}
                    logger.error(
                        "URDI[%s]: VENUE MISMATCH — adapter returned %d/%d instruments "
                        "with wrong venue tags %s (expected %s). "
                        "Adapter likely ignores chain parameter. Dropping mismatched.",
                        canonical,
                        len(mismatched),
                        len(records),
                        sorted(wrong_venues),
                        canonical,
                    )
                    records = [r for r in records if r not in mismatched]
                return records
            except NotImplementedError:
                logger.debug("URDI[%s]: instrument_type=%r not supported", canonical, instrument_type)
                failed.append(
                    VenueError(
                        canonical, "UNSUPPORTED", f"instrument_type={instrument_type!r} not supported", retryable=False
                    )
                )
                return []
            except TimeoutError as exc:
                logger.warning("URDI[%s]: TIMEOUT (retryable): %s", canonical, exc)
                failed.append(VenueError(canonical, "TIMEOUT", str(exc), retryable=True))
                return []
            except ConnectionError as exc:
                logger.warning("URDI[%s]: NETWORK error (retryable): %s", canonical, exc)
                failed.append(VenueError(canonical, "NETWORK", str(exc), retryable=True))
                return []
            except OSError as exc:
                # OSError covers network-level failures (socket errors, DNS, etc.)
                logger.warning("URDI[%s]: NETWORK error (retryable): %s", canonical, exc)
                failed.append(VenueError(canonical, "NETWORK", str(exc), retryable=True))
                return []
            except RuntimeError as exc:
                # _get_with_retry raises RuntimeError after exhausting retries.
                # The underlying cause is typically rate limiting or server errors.
                msg = str(exc).lower()
                if "429" in msg or "rate" in msg:
                    error_code = "RATE_LIMIT"
                elif "503" in msg or "502" in msg or "500" in msg:
                    error_code = "SERVER_ERROR"
                else:
                    error_code = "RETRY_EXHAUSTED"
                logger.warning("URDI[%s]: %s (retryable): %s", canonical, error_code, exc)
                failed.append(VenueError(canonical, error_code, str(exc), retryable=True))
                return []
            except ValueError as exc:
                # Pydantic ValidationError is multi-line; compress to single line for log visibility
                err_oneline = " | ".join(str(exc).splitlines()[:3])
                logger.error("URDI[%s]: ADAPTER_ERROR (permanent): %s", canonical, err_oneline)
                failed.append(VenueError(canonical, "ADAPTER_ERROR", err_oneline, retryable=False))
                return []
            except (AttributeError, KeyError, TypeError) as exc:
                logger.error("URDI[%s]: PARSE_ERROR (permanent): %s", canonical, exc)
                failed.append(VenueError(canonical, "PARSE_ERROR", str(exc), retryable=False))
                return []

    results = await asyncio.gather(*[_fetch_one(c, k) for c, k in fetch_list])
    all_records = [record for batch in results for record in batch]

    # Log summary of failures with classification
    if failed:
        retryable = [v for v in failed if v.retryable]
        permanent = [v for v in failed if not v.retryable]
        if retryable:
            logger.warning(
                "URDI fetch: %d venue(s) failed with RETRYABLE errors: %s",
                len(retryable),
                [(v.venue, v.error_code) for v in retryable],
            )
        if permanent:
            logger.error(
                "URDI fetch: %d venue(s) failed with PERMANENT errors: %s",
                len(permanent),
                [(v.venue, v.error_code) for v in permanent],
            )

    return VenueFetchResult(records=all_records, failed_venues=failed)


async def fetch_instruments_via_urdi(
    venue: str,
    instrument_type: str | None = None,
    api_keys: dict[str, str] | None = None,
    date: str | None = None,
) -> list[InstrumentRecord]:
    """Single-venue fetch. Delegates to fetch_instruments_for_all_venues."""
    result = await fetch_instruments_for_all_venues(
        [venue], instrument_type=instrument_type, api_keys=api_keys, date=date
    )
    return result.records
