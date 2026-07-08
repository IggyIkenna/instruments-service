"""URDI reference data provider — the ONLY external API path for instruments-service.

Naming translation and credential routing are owned upstream:
  - VENUE_TO_ADAPTER_KEY in unified_api_contracts.registry (venue → adapter key, UAC data)
  - ADAPTER_DATA_SOURCES in instruments_service.reference_data.factory (key → credential)
  - get_adapter_for_canonical_venue() in instruments_service.reference_data (key → class)

instruments-service maintains no local translation tables. This file contains
only orchestration logic: which venues to fetch, how to gather results, error policy.

ERROR HANDLING POLICY
---------------------
- Network errors (ConnectionError, TimeoutError): WARNING + return [] (retryable)
- NotImplementedError: adapter unsupported instrument_type — DEBUG + return []
- ValueError: no URDI adapter for this venue — ERROR + skip
- Programming errors (TypeError etc.): reraise to fail the shard
- Empty-after-venue-filter (fetched rows but 0 matched the venue tag, with none
  routed to a sibling): ADAPTER_ERROR (permanent) — a venue-tagging bug, NOT a
  silent honest-empty. See the empty-after-filter guard in `_fetch_one_venue`.

Failed venues are tracked in `VenueFetchResult.failed_venues` with their error
classification (retryable/permanent) so the orchestrator can make retry decisions.
"""

from __future__ import annotations

import asyncio
import logging

from unified_api_contracts import ErrorAction, VenueErrorClassification
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.registry import NO_ADAPTER_YET, VENUE_TO_ADAPTER_KEY, VENUES_WITH_REFERENCE_ADAPTER

from instruments_service.reference_data.factory import (
    ADAPTER_DATA_SOURCES,
    get_adapter_for_canonical_venue,
)

logger = logging.getLogger(__name__)

# Covered canonical venues — UAC-derived (venues with a real adapter key), not a
# frozen IS-side set. Kept as the IS-facing membership name.
URDI_SUPPORTED_VENUES: frozenset[str] = VENUES_WITH_REFERENCE_ADAPTER


class VenueFetchResult:
    """Result of fetching instruments for multiple venues.

    Separates records from error info so the orchestrator can decide
    which failed venues to retry based on error classification.

    ``failed_venues`` uses UAC ``VenueErrorClassification`` as the canonical
    error type — ``retry_safe`` drives retry decisions; ``description`` carries
    the human-readable message.
    """

    def __init__(
        self,
        records: list[InstrumentRecord] | None = None,
        failed_venues: list[VenueErrorClassification] | None = None,
    ) -> None:
        self.records = records if records is not None else []
        self.failed_venues = failed_venues if failed_venues is not None else []

    @property
    def retryable_venues(self) -> list[str]:
        """Venues that failed with retryable errors (rate limit, network, timeout)."""
        return [v.venue for v in self.failed_venues if v.retry_safe]


async def fetch_instruments_for_all_venues(
    venues: list[str],
    instrument_type: str | None = None,
    api_keys: dict[str, str] | None = None,
    date: str | None = None,
    mode: str = "batch",
    source: str | None = None,
) -> VenueFetchResult:
    """Fetch canonical InstrumentRecord[] for all configured venues via URDI.

    Args:
        venues: UAC canonical venue names (e.g. "UNISWAP_V3-ETHEREUM").
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
        adapter_key = VENUE_TO_ADAPTER_KEY.get(canonical)
        if adapter_key is None or adapter_key == NO_ADAPTER_YET:
            unsupported.append(canonical)
        elif canonical not in seen:
            seen.add(canonical)
            fetch_list.append((canonical, adapter_key))

    failed: list[VenueErrorClassification] = []
    if unsupported:
        logger.warning(
            "No URDI adapter for %d venue(s) — register a key (or NO_ADAPTER_YET sentinel) in "
            "unified_api_contracts/registry/venue_adapter_keys.py: %s",
            len(unsupported),
            unsupported,
        )
        for v in unsupported:
            failed.append(
                VenueErrorClassification(
                    venue=v,
                    error_code="UNSUPPORTED",
                    retry_safe=False,
                    reconnect=False,
                    action=ErrorAction.SKIP,
                    description="No URDI adapter registered",
                )
            )

    if not fetch_list:
        return VenueFetchResult(failed_venues=failed)

    # Cap concurrent adapter calls at 4 to avoid overloading APIs
    sem = asyncio.Semaphore(4)

    # Build set of all venues in this fetch batch so we can re-route
    # mismatched instruments to sibling venues instead of dropping them.
    # Example: DBEQ.BASIC returns both NYSE and NASDAQ instruments — if both
    # are requested, keep all and let each venue claim its own tagged records.
    batch_venues: set[str] = {c.upper() for c, _k in fetch_list}

    results = await asyncio.gather(
        *[
            _fetch_one_venue(
                c,
                k,
                sem=sem,
                instrument_type=instrument_type,
                api_keys=api_keys,
                date=date,
                mode=mode,
                source=source,
                batch_venues=batch_venues,
                failed=failed,
            )
            for c, k in fetch_list
        ]
    )
    all_records = [record for batch in results for record in batch]

    # Log summary of failures with classification
    if failed:
        retryable = [v for v in failed if v.retry_safe]
        permanent = [v for v in failed if not v.retry_safe]
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


def _filter_records_to_venue(
    records: list[InstrumentRecord],
    *,
    canonical: str,
    batch_venues: set[str],
    failed: list[VenueErrorClassification],
) -> list[InstrumentRecord]:
    """Filter fetched records down to those tagged for ``canonical``.

    Instruments tagged for sibling venues in the same batch are skipped here
    (the sibling's fetch will claim them); instruments tagged for venues NOT in
    the batch are dropped with a warning (a real adapter venue-tagging bug).

    Honest-absence guard: a fetch that returned rows but yields ZERO records for
    this venue after the venue-tag filter — with NONE of them legitimately
    routed to a sibling in this batch — is a silent exclusion, NOT an honest
    empty (every fetched row was tagged for a venue not in the batch). Record it
    as ``attempted_failed`` (ADAPTER_ERROR, permanent — a tagging bug, not
    transient) so the orchestrator flags the venue honestly rather than as a
    fetched-OK-empty. A genuine empty source response (``records == []``) stays
    an honest empty; a partially-claimed batch (``sibling_routed > 0``) is the
    legitimate sibling-routing path.
    """
    matched: list[InstrumentRecord] = []
    sibling_routed = 0
    unknown_venues: set[str] = set()
    for r in records:
        tag = getattr(r, "venue", "").upper()
        if tag == canonical.upper():
            matched.append(r)
        elif tag in batch_venues:
            sibling_routed += 1  # will be claimed by sibling fetch
        else:
            unknown_venues.add(tag)
    if sibling_routed:
        logger.debug(
            "URDI[%s]: %d instruments tagged for sibling venues (will be claimed by their fetch)",
            canonical,
            sibling_routed,
        )
    if unknown_venues:
        logger.warning(
            "URDI[%s]: dropping %d instruments tagged for unknown venues %s",
            canonical,
            len([r for r in records if getattr(r, "venue", "").upper() in unknown_venues]),
            sorted(unknown_venues),
        )
    if records and not matched and sibling_routed == 0:
        logger.error(
            "URDI[%s]: ADAPTER_ERROR (permanent) — fetched %d row(s) but 0 survived "
            "the venue-tag filter (all tagged for venue(s) not in batch: %s); "
            "recording attempted_failed, not a silent honest-empty",
            canonical,
            len(records),
            sorted(unknown_venues),
        )
        failed.append(
            VenueErrorClassification(
                venue=canonical,
                error_code="ADAPTER_ERROR",
                retry_safe=False,
                reconnect=False,
                action=ErrorAction.FAIL,
                description=(
                    f"fetched {len(records)} row(s) but 0 matched venue tag "
                    f"{canonical!r} (all tagged for {sorted(unknown_venues)})"
                ),
            )
        )
    return matched


async def _fetch_one_venue(
    canonical: str,
    adapter_key: str,
    *,
    sem: asyncio.Semaphore,
    instrument_type: str | None,
    api_keys: dict[str, str] | None,
    date: str | None,
    mode: str,
    source: str | None,
    batch_venues: set[str],
    failed: list[VenueErrorClassification],
) -> list[InstrumentRecord]:
    """Fetch one venue's instruments via its URDI adapter.

    Applies the module-docstring error-handling policy: retryable errors
    (timeout / network / rate-limit) and permanent errors (unsupported /
    adapter / parse) append a ``VenueErrorClassification`` to ``failed`` and
    return ``[]``; programming errors propagate to fail the shard.
    """
    async with sem:
        try:
            # Source-aware credential routing: when source="massive", a TradFi
            # venue that defaults to Databento needs the MASSIVE key, not the
            # Databento key — resolve the data_source against the effective source.
            effective_key = "massive" if (source == "massive" and adapter_key == "databento") else adapter_key
            data_source = ADAPTER_DATA_SOURCES.get(effective_key, "")
            api_key = (api_keys or {}).get(data_source) if data_source else None
            adapter = get_adapter_for_canonical_venue(
                canonical,
                api_key=api_key,
                date=date,
                extra_api_keys=api_keys,
                mode=mode,
                source=source,
            )
            # Use cached path — adapter pool ensures reuse, cache avoids redundant fetches
            records = await adapter.get_instruments_cached(instrument_type=instrument_type, date=date)
            logger.info("URDI[%s]: fetched %d instruments", canonical, len(records))
            return _filter_records_to_venue(
                records,
                canonical=canonical,
                batch_venues=batch_venues,
                failed=failed,
            )
        except NotImplementedError:
            logger.debug("URDI[%s]: instrument_type=%r not supported", canonical, instrument_type)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="UNSUPPORTED",
                    retry_safe=False,
                    reconnect=False,
                    action=ErrorAction.SKIP,
                    description=f"instrument_type={instrument_type!r} not supported",
                )
            )
            return []
        except TimeoutError as exc:
            logger.warning("URDI[%s]: TIMEOUT (retryable): %s", canonical, exc)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="TIMEOUT",
                    retry_safe=True,
                    reconnect=False,
                    action=ErrorAction.RETRY,
                    description=str(exc),
                )
            )
            return []
        except ConnectionError as exc:
            logger.warning("URDI[%s]: NETWORK error (retryable): %s", canonical, exc)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="NETWORK",
                    retry_safe=True,
                    reconnect=True,
                    action=ErrorAction.RECONNECT,
                    description=str(exc),
                )
            )
            return []
        except OSError as exc:
            # OSError covers network-level failures (socket errors, DNS, etc.)
            logger.warning("URDI[%s]: NETWORK error (retryable): %s", canonical, exc)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="NETWORK",
                    retry_safe=True,
                    reconnect=True,
                    action=ErrorAction.RECONNECT,
                    description=str(exc),
                )
            )
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
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code=error_code,
                    retry_safe=True,
                    reconnect=False,
                    action=ErrorAction.RETRY,
                    description=str(exc),
                )
            )
            return []
        except ValueError as exc:
            # Pydantic ValidationError is multi-line; compress to single line for log visibility
            err_oneline = " | ".join(str(exc).splitlines()[:3])
            logger.error("URDI[%s]: ADAPTER_ERROR (permanent): %s", canonical, err_oneline)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="ADAPTER_ERROR",
                    retry_safe=False,
                    reconnect=False,
                    action=ErrorAction.FAIL,
                    description=err_oneline,
                )
            )
            return []
        except (AttributeError, KeyError, TypeError) as exc:
            logger.error("URDI[%s]: PARSE_ERROR (permanent): %s", canonical, exc)
            failed.append(
                VenueErrorClassification(
                    venue=canonical,
                    error_code="PARSE_ERROR",
                    retry_safe=False,
                    reconnect=False,
                    action=ErrorAction.FAIL,
                    description=str(exc),
                )
            )
            return []
