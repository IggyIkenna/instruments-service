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
    ManifestWriter,
    ServiceRuntime,
    UnifiedServiceHandler,
    classify_and_emit_error,
)

from instruments_service.engine import orchestrator as engine_orchestrator
from instruments_service.engine.orchestrator import (
    clear_defi_universe_cache,
    earliest_venue_date,
    get_venues_for_categories,
)

logger = logging.getLogger(__name__)


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
        self._sports_entity_filter: str | None = None  # set in preflight() when --sports-entity is used
        self._league_filter: list[str] | None = None  # set in preflight() when --league is used
        self._season_override: int | None = None  # set in preflight() when --season is used

    async def preflight(self) -> None:
        """Start API key reloader. Date/category filtering happens in process()."""
        # Resolve CLI --category (e.g. ["SPORTS"]) to scope preflight work
        cli_categories: list[str] | None = getattr(self.args, "category", None) if self.args else None
        categories = cli_categories or ["ALL"]

        # Only clear DeFi universe cache if DeFi categories are in scope
        is_all = any(c.upper() == "ALL" for c in categories)
        if is_all or any(c.upper() == "DEFI" for c in categories):
            clear_defi_universe_cache()

        # Wire --venues CLI override to the handler
        venues_arg: list[str] | None = getattr(self.args, "venues", None) if self.args else None
        if venues_arg:
            self._venue_override = venues_arg
            logger.info("Venue override from CLI: %s", venues_arg)
            earliest = earliest_venue_date(venues_arg)
            if earliest:
                logger.info("Earliest venue launch date: %s (dates before this will be skipped)", earliest)

        # Wire --sports-entity filter (entity-scoped VM: one VM per manifest entity type)
        sports_entity_arg: str | None = getattr(self.args, "sports_entity", None) if self.args else None
        if sports_entity_arg:
            self._sports_entity_filter = sports_entity_arg
            logger.info(
                "Sports entity filter from CLI: %s (only this entity will be checked/fetched)", sports_entity_arg
            )

        # Wire --league filter (league-scoped VM: only process specified leagues)
        league_arg: str | None = getattr(self.args, "league", None) if self.args else None
        if league_arg:
            self._league_filter = [lid.strip() for lid in league_arg.split(",") if lid.strip()]
            logger.info("League filter from CLI: %s", self._league_filter)

        # Wire --season override for historical Transfermarkt backfill
        season_arg: int | None = getattr(self.args, "season", None) if self.args else None
        if season_arg is not None:
            self._season_override = season_arg
            logger.info("Season override from CLI: %d", season_arg)

        # Scope key validation to the requested categories only.
        # --category SPORTS only validates sports API keys (not CeFi/DeFi/TradFi).
        active_venues = get_venues_for_categories(categories)
        self._start_key_reloader(active_venues)

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

    async def process(self, payload: BatchPayload) -> object:
        """Process instruments for the date in the payload.

        Reads API keys from the hot-reloader (always fresh after rotation).
        Skip-if-exists is handled by the orchestrator's check_shard_freshness()
        which uses the manifest with per-category buckets (correct bucket resolution).
        """
        date = str(payload.date) if not isinstance(payload.date, str) else payload.date
        # Normalize datetime to YYYY-MM-DD string (BatchIO yields datetime objects)
        if "T" in date or " " in date:
            date = date[:10]
        redo_all = payload.force or bool(payload.extra.get("redo_all", False))

        categories: list[str] = list(payload.categories) if payload.categories else ["ALL"]
        api_keys = self._key_reloader.current_keys if self._key_reloader else {}
        return await engine_orchestrator.process_instruments(
            date=date,
            categories=categories,
            redo_all=redo_all,
            api_keys=api_keys,
            venue_override=self._venue_override,
            mode=str(self.runtime.mode),
            sports_entity_filter=self._sports_entity_filter,
            league_filter=self._league_filter,
            season_override=self._season_override,
        )

    async def cleanup(self) -> None:
        """Flush any buffered manifest writes to GCS at end of batch."""
        from instruments_service.engine.orchestrator import _get_instruments_bucket

        flushed: list[str] = []
        for category in ("SPORTS", "CEFI", "DEFI", "TRADFI"):
            try:
                bucket = _get_instruments_bucket(category)
                if bucket:
                    writer = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket)
                    writer.flush()
                    flushed.append(bucket)
            except Exception as exc:
                logger.warning("ManifestWriter final flush failed for %s: %s", category, exc)
        if flushed:
            logger.info("ManifestWriter cleanup: flushed buffers for %s", flushed)
