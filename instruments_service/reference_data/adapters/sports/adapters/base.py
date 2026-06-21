"""BaseSportsReferenceAdapter: ABC for all sports reference data adapters.

Convention: interfaces are API-keyless. Services fetch credentials from
Secret Manager and inject them at runtime via constructor params.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from uuid import uuid4

import aiohttp
import aiohttp.resolver
from unified_api_contracts import classify_venue_error
from unified_api_contracts.sports import (
    CanonicalFixture,
    CanonicalLeague,
    CanonicalOdds,
    CanonicalTeam,
)
from unified_trading_library import log_event

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS: int = 10
_RETRY_BASE_DELAY: float = 3.0
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
# Bounded per-request HTTP timeouts. WITHOUT these, aiohttp falls back to its
# default ClientTimeout(total=300) with NO sock_connect / sock_read bound — a
# half-open connection or a provider that accepts the socket then never sends
# bytes hangs the single backfill worker INDEFINITELY (the event loop blocks on
# the open response). Root cause of the 2026-06-19 SFI backfill stall: the VM
# processed exactly one date then froze 3h+ on the second date's
# /matches/day/basic/ fetch with no error and no progress (heartbeat sidecar
# kept the VM "alive", masking the dead worker). Bounding sock_connect + sock_read
# converts a hung socket into a retryable aiohttp error the loop already handles.
_HTTP_SOCK_CONNECT_TIMEOUT: float = 15.0  # TCP+TLS establish
_HTTP_SOCK_READ_TIMEOUT: float = 60.0  # max idle gap between received chunks
_HTTP_TOTAL_TIMEOUT: float = 120.0  # absolute ceiling for one request
# Default throttle: 0.12s ≈ 8.3 req/sec ≈ 500 req/min PER VM. Two parallel
# backfill VMs ⇒ ~1000 req/min, a deliberate ~83% margin under the api_football
# Custom plan's 1200 req/min cap so the SELF-ENFORCED rate never trips a 429
# (avoiding the wasteful "sleep to next UTC minute" backoff). Subclasses override
# `_min_request_interval` when their plan is tighter (e.g. SFI = 4 req/sec).
_MIN_REQUEST_INTERVAL: float = 0.12


class BaseSportsReferenceAdapter(ABC):
    """Abstract base for sports reference data adapters.

    All adapters use REST only. Sports reference data is slow-moving;
    implementations should cache aggressively.

    Convention: interfaces MUST NOT fetch secrets directly. Services fetch
    credentials from Secret Manager and pass them in through the ``api_key``
    constructor parameter.
    """

    _last_request_time: float = 0.0
    # Per-class throttle override. Defaults to module-level _MIN_REQUEST_INTERVAL.
    # Subclasses set this to their RapidAPI plan's per-second floor.
    _min_request_interval: float = _MIN_REQUEST_INTERVAL
    # Concurrency-safe self-enforced rate limiter (per-subclass via cls-attr write).
    # ``_next_slot`` is the monotonic time the NEXT request may fire; the lock
    # serialises slot assignment so N concurrent per-fixture calls are spaced by
    # ``_min_request_interval`` instead of all firing at once (the burst that
    # instantly saturated the per-minute cap → 429 → minute-boundary sleep).
    _next_slot: float = 0.0
    _rate_lock: asyncio.Lock | None = None

    @classmethod
    def _get_rate_lock(cls) -> asyncio.Lock:
        """Lazily create a per-subclass asyncio.Lock (must run inside the loop)."""
        if cls.__dict__.get("_rate_lock") is None:
            cls._rate_lock = asyncio.Lock()
        return cls._rate_lock

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key

    @staticmethod
    def _make_session() -> aiohttp.ClientSession:
        """Create an aiohttp session with ThreadedResolver (OS DNS) + bounded timeouts.

        A bounded ``ClientTimeout`` is MANDATORY: without it aiohttp uses
        ``ClientTimeout(total=300)`` and leaves ``sock_connect``/``sock_read``
        unbounded, so a half-open or stalled provider connection hangs the
        single backfill worker forever (see ``_HTTP_*_TIMEOUT`` rationale above).
        With the bound, a stalled socket raises ``asyncio.TimeoutError`` (an
        ``aiohttp.ClientError`` subclass) that ``_get_with_retry`` already
        retries/escalates — progress is never silently frozen.
        """
        connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
        timeout = aiohttp.ClientTimeout(
            total=_HTTP_TOTAL_TIMEOUT,
            sock_connect=_HTTP_SOCK_CONNECT_TIMEOUT,
            sock_read=_HTTP_SOCK_READ_TIMEOUT,
        )
        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    @property
    @abstractmethod
    def venue(self) -> str:
        """Venue identifier e.g. 'api_football', 'transfermarkt'."""
        ...

    @abstractmethod
    async def get_fixtures(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[CanonicalFixture]:
        """Fetch fixtures for a given date.

        Args:
            date: Date string in YYYY-MM-DD format.
            league_ids: Optional list of league IDs to filter by.

        Returns:
            List of canonical fixtures.
        """
        ...

    async def get_fixtures_with_raw(
        self,
        date: str,
        league_ids: list[int] | None = None,
    ) -> list[tuple[CanonicalFixture, dict[str, object]]]:
        """Fetch fixtures paired with their RAW provider response item.

        The raw dict is what
        ``unified_trading_library.fixtures.extract_match_lifecycle`` consumes to
        populate the Q5/Q6 HT/ET/PEN phase-timestamp + score-distinction columns
        at FIXTURES write-time. Only the api-football adapter carries the raw
        api-football response shape ``extract_match_lifecycle`` expects, so it
        OVERRIDES this method. The base default pairs each canonical fixture with
        an EMPTY dict — a source that does not surface the api-football response
        shape contributes no lifecycle data (those columns take their honest
        defaults), but the writer interface stays uniform across adapters.

        Returns:
            ``[(canonical_fixture, raw_item_or_empty), …]``.
        """
        return [(fx, {}) for fx in await self.get_fixtures(date, league_ids=league_ids)]

    @abstractmethod
    async def get_leagues(self) -> list[CanonicalLeague]:
        """Fetch all available leagues.

        Returns:
            List of canonical leagues.
        """
        ...

    @abstractmethod
    async def get_teams(self, league_id: int | str, season: int | None = None) -> list[CanonicalTeam]:
        """Fetch teams for a given league.

        Args:
            league_id: League ID (numeric for API Football, string code for Transfermarkt).
            season: Optional season year. Defaults to current season.

        Returns:
            List of canonical teams.
        """
        ...

    @abstractmethod
    async def get_odds(
        self,
        sport: str,
        regions: str = "uk",
        markets: str = "h2h",
    ) -> list[CanonicalOdds]:
        """Fetch odds for a given sport.

        Args:
            sport: Sport key (e.g. 'soccer_epl').
            regions: Comma-separated region codes.
            markets: Comma-separated market keys.

        Returns:
            List of canonical odds.
        """
        ...

    async def get_standings(self, league_id: int | str, season: int | None = None) -> list[dict[str, object]]:
        """Fetch league standings/table for a given season.

        Returns:
            List of dicts with position, team, points, GD, form, etc.
        """
        return []

    async def get_injuries(self, date: str) -> list[dict[str, object]]:
        """Fetch player injuries for a given date.

        Returns:
            List of dicts with player, team, reason, status.
        """
        return []

    async def get_fixture_statistics(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch match statistics (shots, possession, etc.) for a completed fixture.

        Returns:
            List of dicts with team statistics.
        """
        return []

    async def get_fixture_events(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch match events (goals, cards, subs) for a fixture.

        Returns:
            List of dicts with event timeline.
        """
        return []

    async def get_fixture_lineups(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch starting lineups for a fixture.

        Returns:
            List of dicts with formation and starting XI.
        """
        return []

    async def get_fixture_player_stats(self, fixture_id: int) -> list[dict[str, object]]:
        """Fetch per-player per-match statistics for a fixture.

        Returns:
            List of dicts with player stats (33+ fields per player).
        """
        return []

    def _classify_error(self, exc: Exception, status: int | None = None) -> str:
        """Map an HTTP/network error to a UAC error code string.

        Subclasses may override for venue-specific classification.
        """
        msg = str(exc).lower()
        if status == 401 or "401" in msg or "authentication" in msg:
            return "INVALID_API_KEY"
        if status == 429 or "429" in msg or "rate" in msg:
            return "RATE_LIMIT_EXCEEDED"
        if "timeout" in msg:
            return "TIMEOUT_ERROR"
        if status == 403 or "403" in msg or "forbidden" in msg:
            return "FORBIDDEN"
        if status == 404 or "404" in msg:
            return "HTTP_NOT_FOUND"
        return "UNKNOWN"

    def _emit_fetch_failed(self, error_code: str, exc: Exception) -> None:
        """Emit ADAPTER_FETCH_FAILED event with UAC error classification."""
        classification = classify_venue_error(self.venue, error_code)
        correlation_id = str(uuid4())
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "error_code": error_code,
                "error_message": str(exc),
                "correlation_id": correlation_id,
                "classification": classification.action if classification else "UNKNOWN",
            },
        )
        logger.warning(
            "ADAPTER_FETCH_FAILED venue=%s error_code=%s: %s",
            self.venue,
            error_code,
            exc,
        )

    async def _throttle(self) -> None:
        """Self-enforced, concurrency-safe minimum interval between API requests.

        Uses per-class `_min_request_interval` so each adapter (SFI, api_football,
        transfermarkt, etc.) paces against its own RapidAPI plan. CRITICAL: slot
        assignment is serialised under a lock so that N concurrent per-fixture
        requests are spaced by ``interval`` rather than all passing the check at
        once. The old lock-free ``_last_request_time`` compare let concurrent
        async tasks read the same timestamp and burst together — instantly
        saturating the per-minute cap → 429 → 52s minute-boundary sleep →
        throughput collapsed to ~46/min vs the 1200/min cap.
        """
        import time

        cls = type(self)
        interval = cls._min_request_interval
        async with cls._get_rate_lock():
            now = time.monotonic()
            slot = cls._next_slot if cls._next_slot > now else now
            cls._next_slot = slot + interval
            cls._last_request_time = slot
        wait = slot - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)

    async def _get_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        max_retries: int | None = None,
    ) -> object:
        """GET request with rate-limit-aware retry on transient errors.

        Reads ``X-RateLimit-Remaining`` from API Football responses.
        When remaining hits 0, sleeps until the next minute boundary
        (rate limits reset per UTC minute).

        On 429, uses ``Retry-After`` header if present, otherwise sleeps
        to the next minute boundary.  Retries up to _RETRY_ATTEMPTS times
        on 429/5xx and connection errors.
        """
        last_exc: Exception | None = None
        _max = max_retries if max_retries is not None else _RETRY_ATTEMPTS
        for attempt in range(_max):
            await self._throttle()
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status in _RETRYABLE_STATUS_CODES:
                        if resp.status == 429:
                            # Rate limited — sleep to next minute boundary
                            delay = self._seconds_to_next_minute()
                            if delay < 3.0:
                                delay = 60.0  # Just missed boundary, wait full minute
                            logger.warning(
                                "Rate limited (429) from %s — sleeping %.0fs to next minute (attempt %d/%d)",
                                url,
                                delay,
                                attempt + 1,
                                _RETRY_ATTEMPTS,
                            )
                        else:
                            delay = _RETRY_BASE_DELAY * (1 << min(attempt, 4))
                            logger.warning(
                                "Retryable HTTP %d from %s (attempt %d/%d, backoff %.1fs)",
                                resp.status,
                                url,
                                attempt + 1,
                                _RETRY_ATTEMPTS,
                                delay,
                            )
                        if attempt < _RETRY_ATTEMPTS - 1:
                            await asyncio.sleep(delay)
                            continue
                        raise RuntimeError(f"HTTP {resp.status} from {url} after {_RETRY_ATTEMPTS} attempts")
                    resp.raise_for_status()

                    # Read rate limit headers — preemptively sleep if exhausted
                    remaining = resp.headers.get("X-RateLimit-Remaining")
                    if remaining is not None:
                        try:
                            rem = int(remaining)
                            if rem <= 1:
                                wait = self._seconds_to_next_minute()
                                logger.info(
                                    "Rate limit near-exhausted (remaining=%d) — sleeping %.0fs to next minute",
                                    rem,
                                    wait,
                                )
                                await asyncio.sleep(wait)
                        except ValueError:
                            pass

                    return await resp.json(content_type=None)
            except aiohttp.ClientError as exc:
                # 4xx client errors (except 429) are not transient.
                if (
                    isinstance(exc, aiohttp.ClientResponseError)
                    and exc.status is not None
                    and 400 <= exc.status < 500
                    and exc.status != 429
                ):
                    raise
                last_exc = exc
                delay = _RETRY_BASE_DELAY * (1 << min(attempt, 4))
                logger.warning(
                    "aiohttp error fetching %s (attempt %d/%d, backoff %.1fs): %s",
                    url,
                    attempt + 1,
                    _RETRY_ATTEMPTS,
                    delay,
                    exc,
                )
                if attempt < _RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(delay)
        raise RuntimeError(f"All {_RETRY_ATTEMPTS} attempts failed for {url}: {last_exc}") from last_exc

    @staticmethod
    def _seconds_to_next_minute() -> float:
        """Seconds until the next UTC minute boundary (rate limit reset)."""
        import time

        now = time.time()
        return 60.0 - (now % 60.0)
