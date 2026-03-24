"""InstrumentsHandler — mode-agnostic handler for the instruments operation.

Preflight starts an ApiKeyReloader that fetches keys from Secret Manager
and periodically refreshes them (hot-reload on key rotation). Missing keys
fail the shard at startup — not after silently producing zero results.

In CLOUD_MOCK_MODE, key validation is skipped (no real Secret Manager available).
"""

from __future__ import annotations

import logging

from unified_trading_library import (
    ApiKeyReloader,
    BatchPayload,
    ServiceRuntime,
    UnifiedServiceHandler,
    get_bucket_name,
    validate_data_availability,
)

from instruments_service.engine import orchestrator as engine_orchestrator
from instruments_service.engine.orchestrator import get_venues_for_categories, is_venue_available

logger = logging.getLogger(__name__)

_INSTRUMENTS_PATH_PATTERN = "instrument_availability/by_date/day={date}/"


class InstrumentsHandler(UnifiedServiceHandler):
    """Process canonical instrument records via URDI for a date/category set.

    preflight():
      1. Starts ApiKeyReloader (fail-fast on missing keys, periodic refresh)
      2. Checks which dates already have complete data (skips those in process())

    process(payload): calls engine orchestrator for the given date+categories
    """

    def __init__(self, runtime: ServiceRuntime) -> None:
        super().__init__(runtime)
        self._completed_dates: set[str] = set()
        self._key_reloader: ApiKeyReloader | None = None
        self._venue_override: list[str] | None = None  # set in preflight() when --venues is used

    async def preflight(self) -> None:
        """Start API key reloader and check data availability."""
        categories: list[str] = [c.value for c in self.runtime.category] if self.runtime.category else ["ALL"]
        start_date: str = self.runtime.start_date
        all_venues = get_venues_for_categories(categories)

        # Apply --venues filter (for sharding: process one venue or shard at a time)
        if self.runtime.venue_filter:
            requested = {v.upper() for v in self.runtime.venue_filter}
            all_venues = [v for v in all_venues if v.upper() in requested]
            logger.info("Venue filter: processing only %s", self.runtime.venue_filter)
            self._venue_override = all_venues  # stored so process() can bypass category lookup

        active_venues = [v for v in all_venues if is_venue_available(v, start_date)] if start_date else all_venues

        # 1. Start API key reloader — fail-fast on missing keys, periodic refresh
        try:
            self._key_reloader = ApiKeyReloader(
                venues=active_venues,
                project_id=self.runtime.gcp_project_id or None,
            )
            self._key_reloader.start()
            keys = self._key_reloader.current_keys
            if keys:
                logger.info(
                    "API keys validated for %d data source(s): %s",
                    len(keys),
                    sorted(keys.keys()),
                )
        except Exception as exc:
            logger.error("API key validation failed: %s", exc)
            raise

        # 2. Check which dates already have data (skip logic)
        try:
            bucket = get_bucket_name("instruments")
            end_date: str = self.runtime.end_date
            if start_date and end_date:
                self._completed_dates = validate_data_availability(
                    service_name="instruments-service",
                    bucket=bucket,
                    path_pattern=_INSTRUMENTS_PATH_PATTERN,
                    start_date=start_date,
                    end_date=end_date,
                )
                logger.info(
                    "preflight: %d dates already complete (of %s → %s)",
                    len(self._completed_dates),
                    start_date,
                    end_date,
                )
        except Exception as exc:
            logger.warning("preflight data availability check failed (proceeding): %s", exc)

    async def process(self, payload: BatchPayload) -> object:
        """Process instruments for the date in the payload.

        Reads API keys from the hot-reloader (always fresh after rotation).
        Skips dates already present in storage (unless redo_all=True).
        """
        date = payload.date
        redo_all = bool(payload.extra.get("redo_all", False))

        if date in self._completed_dates and not redo_all:
            logger.debug("Skipping already-complete date=%s", date)
            return None

        categories: list[str] = list(payload.categories) if payload.categories else ["ALL"]
        api_keys = self._key_reloader.current_keys if self._key_reloader else {}
        return await engine_orchestrator.process_instruments(
            date=date,
            categories=categories,
            redo_all=redo_all,
            api_keys=api_keys,
            venue_override=self._venue_override,
        )
