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
    classify_and_emit_error,
    get_bucket_name,
    validate_data_availability,
)

from instruments_service.engine import orchestrator as engine_orchestrator
from instruments_service.engine.orchestrator import clear_defi_universe_cache, get_venues_for_categories

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
        """Start API key reloader. Date/category filtering happens in process()."""
        # Clear DeFi universe cache at the start of each batch run.
        # The cache is populated on the first DeFi date and reused for all
        # subsequent dates — one API call for the entire date range.
        clear_defi_universe_cache()

        # Wire --venues CLI override to the handler
        venues_arg: list[str] | None = getattr(self.args, "venues", None) if self.args else None
        if venues_arg:
            self._venue_override = venues_arg
            logger.info("Venue override from CLI: %s", venues_arg)

        # Preflight runs once before any date is processed. We don't know
        # the specific dates yet (BatchIO iterates them), so we validate keys
        # for ALL venues across ALL categories. Per-date filtering is in process().
        all_venues = get_venues_for_categories(["ALL"])
        self._start_key_reloader(all_venues)

    def _start_key_reloader(self, active_venues: list[str]) -> None:
        """Start API key reloader — fail-fast on missing keys, periodic refresh."""
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
        except Exception as _exc:
            classify_and_emit_error(
                _exc,
                service_name="instruments-service",
                operation="api_key_validation",
                reraise=True,
            )

    def _is_date_complete(self, date: str) -> bool:
        """Check if a single date already has data in storage."""
        try:
            bucket = get_bucket_name("instruments")
            completed = validate_data_availability(
                service_name="instruments-service",
                bucket=bucket,
                path_pattern=_INSTRUMENTS_PATH_PATTERN,
                start_date=date,
                end_date=date,
            )
            return date in completed
        except Exception as _exc:
            classify_and_emit_error(
                _exc,
                service_name="instruments-service",
                operation="check_date_completeness",
                shard=date,
            )
            return False

    async def process(self, payload: BatchPayload) -> object:
        """Process instruments for the date in the payload.

        Reads API keys from the hot-reloader (always fresh after rotation).
        Skips dates already present in storage (unless force=True).
        """
        date = payload.date
        redo_all = payload.force or bool(payload.extra.get("redo_all", False))

        if not redo_all and self._is_date_complete(date):
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
            mode=str(self.runtime.mode),
        )
