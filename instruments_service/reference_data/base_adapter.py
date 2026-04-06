"""BaseReferenceDataAdapter: ABC for all venue reference data adapters.

Convention: interfaces are API-keyless. Services fetch credentials from
Secret Manager and inject them at runtime via the ``api_key`` parameter.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypeVar, cast
from uuid import uuid4

import aiohttp
from pydantic import BaseModel
from unified_api_contracts.internal import (
    EnhancedError,
    ErrorCategory,
    ErrorContext,
    ErrorRecoveryStrategy,
    ErrorSeverity,
    FeeScheduleEntry,
    InstrumentRecord,
)
from unified_trading_library import log_event

from .schemas import (
    CanonicalCorporateAction,
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Retry configuration constants
_RETRY_ATTEMPTS: int = 3
_RETRY_BASE_DELAY: float = 1.0  # seconds; doubles on each retry
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

T = TypeVar("T", bound=BaseModel)


class BaseReferenceDataAdapter(ABC):
    """Abstract base for all reference data adapters.

    All adapters use REST only — no WebSocket connections.
    Reference data is slow-moving; implementations should cache aggressively.

    Convention: interfaces MUST NOT fetch secrets directly. Services fetch
    credentials from Secret Manager (via ``get_secret_client``) and pass
    them in through the ``api_key`` constructor parameter.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
    ) -> None:
        """Initialize adapter.

        Args:
            project_id: Deprecated. Retained for call-site compatibility but
                no longer used for internal Secret Manager lookups. Will be
                removed in a future version.
            api_key: API key for the venue. The calling service MUST fetch
                this from Secret Manager and pass it in. If the adapter
                requires an API key and none is provided, it will raise
                ``ValueError`` at the point of use.
        """
        self._project_id = project_id
        self._api_key = api_key
        # In-memory instrument cache: {instrument_type_filter: (records, timestamp)}
        # Subclasses inherit this. TTL is configurable per adapter.
        self._instruments_cache: dict[str | None, tuple[list[InstrumentRecord], float]] = {}
        self._cache_ttl: float = 3600.0  # 1 hour default; subclasses can override

    def _optional_api_key(self) -> str | None:
        """Return the injected API key, or None if not provided.

        Services that need authenticated access MUST pass ``api_key`` to the
        adapter constructor. This method no longer performs Secret Manager
        lookups — interfaces are API-keyless by convention.
        """
        return self._api_key

    async def _handle_retryable_response(
        self,
        resp: aiohttp.ClientResponse,
        url: str,
        attempt: int,
        delay: float,
    ) -> object | None:
        """Handle a retryable HTTP status. Returns None to continue retrying, or parsed JSON."""
        if resp.status in _RETRYABLE_STATUS_CODES:
            logger.warning(
                "Retryable HTTP %d from %s (attempt %d/%d)",
                resp.status,
                url,
                attempt + 1,
                _RETRY_ATTEMPTS,
            )
            if attempt < _RETRY_ATTEMPTS - 1:
                await asyncio.sleep(delay)
                return None
            raise RuntimeError(f"HTTP {resp.status} from {url} after {_RETRY_ATTEMPTS} attempts")
        resp.raise_for_status()
        return cast(object, await resp.json())

    async def _get_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> object:
        """GET request with exponential backoff retry on transient errors.

        Retries up to _RETRY_ATTEMPTS times on HTTP 429/5xx and aiohttp
        connection errors.  On final failure raises the last exception.

        Args:
            session: Active aiohttp ClientSession.
            url: Fully-qualified URL to GET.
            params: Optional query parameters.
            headers: Optional request headers.

        Returns:
            Parsed JSON response body as a Python object.

        Raises:
            RuntimeError: If all retry attempts fail.
        """
        last_exc: Exception | None = None
        for attempt in range(_RETRY_ATTEMPTS):
            delay: float = _RETRY_BASE_DELAY * (1.0 * (1 << attempt))
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    result = await self._handle_retryable_response(resp, url, attempt, delay)
                    if result is not None:
                        return result
            except aiohttp.ClientError as exc:
                last_exc = exc
                logger.warning(
                    "aiohttp error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    _RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < _RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(delay)
        raise RuntimeError(f"All {_RETRY_ATTEMPTS} attempts failed for {url}: {last_exc}") from last_exc

    def _parse_raw(self, raw: dict[str, object], schema_class: type[T]) -> T:
        """Parse raw venue response into a validated schema instance.

        Subclasses MUST call this instead of constructing schemas directly.
        On validation failure, raises RuntimeError + logs INSTRUMENT_SCHEMA_VIOLATION event.

        Subclasses may override for venue-specific field mapping, then delegate:
            return super()._parse_raw(transformed_raw, schema_class)
        """
        try:
            return schema_class(**raw)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            correlation_id = str(uuid4())
            _err = EnhancedError(
                message=f"Schema validation failed for {schema_class.__name__}: {e}",
                category=ErrorCategory.VALIDATION_ERROR,
                severity=ErrorSeverity.HIGH,
                recovery_strategy=ErrorRecoveryStrategy.SKIP,
                correlation_id=correlation_id,
                context=ErrorContext(extra={"schema": schema_class.__name__, "exc_type": type(e).__name__}),
            )
            log_event(
                "INSTRUMENT_SCHEMA_VIOLATION",
                details={
                    "schema": schema_class.__name__,
                    "correlation_id": _err.correlation_id,
                    "error": _err.message,
                },
            )
            raise RuntimeError(_err.message) from e

    @property
    @abstractmethod
    def venue(self) -> str:
        """Venue identifier e.g. 'binance', 'bybit', 'deribit'."""
        ...

    async def get_instruments_cached(
        self,
        instrument_type: str | None = None,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        """Cache-aware wrapper around get_instruments().

        Returns cached results if within TTL, otherwise fetches fresh and caches.
        Adapters that need custom caching (e.g. Tardis fetches once then slices
        per-date) can override this method.
        """
        cache_key = (instrument_type, date)
        cached = self._instruments_cache.get(cache_key)
        if cached is not None:
            records, ts = cached
            if (time.monotonic() - ts) < self._cache_ttl:
                logger.debug(
                    "%s: cache hit for instrument_type=%s (%d records, age=%.0fs)",
                    self.venue,
                    instrument_type,
                    len(records),
                    time.monotonic() - ts,
                )
                return records

        # Pass date to adapters that support it (e.g. Polymarket for historical mode)
        import inspect

        sig = inspect.signature(self.get_instruments)
        if "date" in sig.parameters:
            records = await self.get_instruments(instrument_type=instrument_type, date=date)
        else:
            records = await self.get_instruments(instrument_type=instrument_type)
        self._instruments_cache[cache_key] = (records, time.monotonic())
        return records

    def clear_cache(self) -> None:
        """Clear the instrument cache. Call when API keys rotate or on explicit refresh."""
        self._instruments_cache.clear()

    @abstractmethod
    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all active instruments for this venue.

        Args:
            instrument_type: Filter by type (spot, perp, future, option). None = all.
        """
        ...

    @abstractmethod
    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by venue-native symbol. Returns None if not found."""
        ...

    @abstractmethod
    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Fetch options chain snapshot. If expiry is None, returns nearest expiry."""
        ...

    @abstractmethod
    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        """Fetch expiry/delivery schedule for futures or options."""
        ...

    @abstractmethod
    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch current funding rate for a perpetual contract symbol.

        Args:
            symbol: Venue-native symbol (e.g. 'BTCUSDT' on Binance).

        Returns:
            FundingRateRef with current rate and next funding time.
        """
        ...

    @abstractmethod
    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars for a symbol.

        Args:
            symbol: Venue-native symbol.
            interval: Bar interval (e.g. '1m', '5m', '1h', '1d').
            limit: Maximum number of bars to return.

        Returns:
            List of OHLCVRef bars, most recent last.
        """
        ...

    async def get_exchange_fee_schedule(
        self,
        client_id: str,
    ) -> FeeScheduleEntry | None:
        """Fetch the exchange fee schedule for a client account.

        Returns the active maker/taker bps for the API key associated with
        client_id. Credentials are resolved via UnifiedCloudConfig — never
        via direct environment variable reads.

        Default implementation returns None (fee schedule not supported for
        this venue). Override in venue adapters that expose a fee-tier
        endpoint (Bybit /v5/account/fee-rate, OKX /api/v5/account/trade-fee).

        Args:
            client_id: Account/fund identifier — used to look up API credentials
                       in UnifiedCloudConfig.

        Returns:
            FeeScheduleEntry for the exchange layer, or None if unsupported.
        """
        return None

    async def get_corporate_actions(
        self,
        symbol: str,
        from_dt: datetime,
        to_dt: datetime | None = None,
    ) -> list[CanonicalCorporateAction]:
        """Fetch corporate actions. Default: empty list (TradFi venues override)."""
        return []
