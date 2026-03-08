"""
Instrument Handler

Generates instrument definitions using direct GCS existence checks.
No missing data report dependencies - pure force/skip logic.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import TypedDict, cast

from unified_events_interface import log_event
from unified_market_interface import VenueMapping
from unified_trading_library import get_date_range, parse_date

from instruments_service.app.core.cloud_data_provider import CloudDataProvider
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.instruments_service import InstrumentsService
from instruments_service.app.core.selective_validation import validate_required_api_keys
from instruments_service.cli.base_handler import HandlerResultValue, ModeHandler
from instruments_service.config import get_config

logger = logging.getLogger(__name__)


def _to_int(val: HandlerResultValue, default: int = 0) -> int:
    """Safely convert HandlerResultValue to int for result.get() values."""
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    return default


# Type definitions for handler arguments
class InstrumentHandlerConfig(TypedDict, total=False):
    """Configuration for InstrumentHandler."""

    project_id: str | None
    enable_ccxt_integration: bool
    enable_metadata_caching: bool


class InstrumentGenerationKwargs(TypedDict, total=False):
    """Keyword arguments for instrument generation."""

    venues: list[str] | None
    instrument_ids: list[str] | None
    cefi: bool
    tradfi: bool
    defi: bool
    sports: bool


class InstrumentHandler(ModeHandler):
    """Generate instruments with force mode and direct GCS checks only."""

    def __init__(self, config: InstrumentHandlerConfig) -> None:
        super().__init__(cast(dict[str, object], cast(object, config)))

        # Initialize services directly (no ServiceContainer)
        project_id = config.get("project_id") or get_config().gcp_project_id

        # Initialize InstrumentsService (orchestration wrapper)
        service_config: InstrumentHandlerConfig = {
            "project_id": project_id,
            "enable_ccxt_integration": True,
            "enable_metadata_caching": True,
        }
        self.instruments_service = InstrumentsService(cast(dict[str, object], cast(object, service_config)))

        # Initialize cloud storage (for CLI-specific operations)
        self.cloud_storage = CloudInstrumentStorage()

        # Venue mapping (for CLI-specific operations)
        self.venue_mapping = VenueMapping()

        logger.debug("✅ InstrumentHandler initialized")

    def _get_venues_to_process(
        self, requested_venues: list[str] | None, cefi: bool, tradfi: bool, defi: bool
    ) -> list[str]:
        """
        Get list of venues to process based on CLI args.

        Args:
            requested_venues: Explicit venues from --venues flag (optional)
            cefi: Whether to process CEFI venues
            tradfi: Whether to process TRADFI venues
            defi: Whether to process DEFI venues

        Returns:
            List of venue names to process
        """
        venues: list[str] = []

        # If explicit venues requested, use those (takes precedence)
        if requested_venues:
            return requested_venues

        # Otherwise, build from categories
        if cefi:
            venues.extend(cast(list[str], self.venue_mapping.all_tardis_exchanges))
        if tradfi:
            venues.extend(cast(list[str], self.venue_mapping.all_databento_venues))
        if defi:
            venues.extend(cast(list[str], self.venue_mapping.all_defi_venues))

        return venues

    def run(self, **kwargs: object) -> dict[str, HandlerResultValue]:
        """Execute instrument generation."""
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        if start_date is None or start_date == "":
            raise ValueError("start_date is required for instrument generation")
        if end_date is None or end_date == "":
            raise ValueError("end_date is required for instrument generation")
        force = bool(kwargs.get("force", False))
        # Remove already-extracted keys so they aren't passed twice to _execute_instrument_generation
        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ("start_date", "end_date", "force")}
        return self._execute_instrument_generation(start_date, end_date, force, **filtered_kwargs)

    def _execute_instrument_generation(
        self,
        start_date: str | datetime | object,
        end_date: str | datetime | object,
        force: bool = False,
        **kwargs: object,
    ) -> dict[str, HandlerResultValue]:
        """Generate instruments with direct GCS existence checks."""
        log_event("STARTED")
        log_event("VALIDATION_STARTED")
        try:
            # Parse dates
            if isinstance(start_date, str):
                start_date = parse_date(start_date)
            if isinstance(end_date, str):
                end_date = parse_date(end_date)

            # Generate date range (YYYY-MM-DD format required by get_date_range)
            start_str: str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else str(start_date)
            end_str: str = end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else str(end_date)
            date_range = get_date_range(start_str, end_str)

            # Get requested venues (from --venues CLI arg or default to all)
            requested_venues_raw = kwargs.get("venues")
            requested_venues: list[str] | None = (
                list(cast(list[str], requested_venues_raw)) if isinstance(requested_venues_raw, list) else None
            )

            # Determine market categories to process
            cefi: bool = bool(kwargs.get("cefi", False))
            tradfi: bool = bool(kwargs.get("tradfi", False))
            defi: bool = bool(kwargs.get("defi", False))
            sports: bool = bool(kwargs.get("sports", False))

            # Default: process ALL if no flags specified
            if not cefi and not tradfi and not defi and not sports:
                cefi = tradfi = defi = sports = True
                logger.info("🌍 Processing ALL market types: CEFI, TRADFI, DEFI, and SPORTS")
            else:
                market_types: list[str] = []
                if cefi:
                    market_types.append("CEFI")
                if tradfi:
                    market_types.append("TRADFI")
                if defi:
                    market_types.append("DEFI")
                if sports:
                    market_types.append("SPORTS")
                logger.info("🔍 Processing market types: %s", ", ".join(market_types))

            # Build venue list based on categories + explicit venues
            venues_to_process = self._get_venues_to_process(requested_venues, cefi, tradfi, defi)

            # Selective API key validation (only for requested venues)
            try:
                # Validate API keys (result not used yet, but validation ensures keys exist)
                _ = validate_required_api_keys(venues_to_process)
                logger.info("✅ Validated API keys for %s venues", len(venues_to_process))
            except ValueError as e:
                log_event("VALIDATION_FAILED", details={"message": f"API key validation: {e!s}"})
                raise

            log_event("VALIDATION_COMPLETED")
        except (ValueError, TypeError, KeyError) as e:
            log_event("VALIDATION_FAILED", details={"message": str(e)})
            log_event("FAILED", details={"message": f"Validation error: {e!s}"})
            raise

        # Observability metrics tracking
        total_generated = 0
        total_dates_processed = 0
        total_skipped = 0
        total_errors = 0
        total_processing_errors = 0  # Errors from processing (not date-level failures)
        total_processing_warnings = 0  # Warnings from processing
        today = datetime.now(UTC).date()

        # Set exchanges for CEFI processing (always use all exchanges - no filtering)
        # Note: --exchanges CLI arg was removed as it filtered within aggregated instruments.parquet
        exchanges_to_process: list[str] = cast(list[str], self.venue_mapping.all_tardis_exchanges) if cefi else []

        # Compute day-venue combinations for 3-level event counters
        total_combinations = len(date_range) * len(venues_to_process)
        total_dates = len(date_range)

        for date_idx, date in enumerate(date_range, start=1):
            # Skip future dates
            if date.date() > today:
                logger.warning("⚠️ Skipping future date: %s", date.strftime("%Y-%m-%d"))
                break

            try:
                # Check if file exists (using cloud service)
                if not force:
                    try:
                        # Build list of categories to check based on flags
                        categories_to_check: list[str] = []
                        if cefi:
                            categories_to_check.append("CEFI")
                        if tradfi:
                            categories_to_check.append("TRADFI")
                        if defi:
                            categories_to_check.append("DEFI")

                        # Get venues from kwargs (from --venues CLI arg)
                        venues_to_check_raw = kwargs.get("venues")
                        venues_to_check: list[str] | None = (
                            list(cast(list[str], venues_to_check_raw))
                            if isinstance(venues_to_check_raw, list)
                            else None
                        )

                        # Use cloud_data_provider to check existence for specific categories AND venues
                        # When venues specified, checks venue-level files (new structure)
                        # This enables granular skip logic matching category x venue x date sharding
                        data_provider = CloudDataProvider()
                        if data_provider.check_instruments_exist(
                            date, categories=categories_to_check, venues=venues_to_check
                        ):
                            date_str = date.strftime("%Y-%m-%d")

                            # Detect race condition: data appeared after shard was launched
                            # SHARD_LAUNCHED_AT is set by VM startup script - if present, this
                            # VM was launched because data was reported as MISSING, but now exists
                            config = get_config()
                            if config.shard_launched_at:
                                # Race condition detected: another deployment completed while we were launching
                                skip_msg = f"⚠️ RACE_CONDITION: Skipping {date_str} - data appeared after launch"
                                if venues_to_check:
                                    skip_msg += f" for {categories_to_check}/{venues_to_check}"
                                if config.deployment_id:
                                    skip_msg += (
                                        f" (deployment={config.deployment_id}, launched_at={config.shard_launched_at})"
                                    )
                                logger.warning(skip_msg)
                            else:
                                # Normal expected skip (resume scenario or data already exists)
                                skip_msg = f"⏭️ Skipping {date_str} - instruments exist"
                                if venues_to_check:
                                    skip_msg += f" for {categories_to_check}/{venues_to_check}"
                                else:
                                    skip_msg += f" for {categories_to_check}"
                                logger.info(skip_msg)

                            total_skipped += 1
                            continue
                    except (OSError, FileNotFoundError, RuntimeError, ValueError):
                        # File doesn't exist or check failed, proceed with generation
                        pass

                # Level 1: Day-venue combination counters (shows overall progress)
                # Calculate day-venue progress: this date processes ALL venues_to_process
                start_combination = (date_idx - 1) * len(venues_to_process) + 1
                end_combination = date_idx * len(venues_to_process)

                # Level 2: Daily progress (dates being processed)
                log_event(
                    "DATE_PROCESSING_STARTED",
                    details={"message": f"{date.strftime('%Y-%m-%d')} ({date_idx}/{total_dates})"},
                )

                # Level 3: Venues for this date (will process all in single call)
                venues_str = ", ".join(venues_to_process[:3])
                if len(venues_to_process) > 3:
                    venues_str += f" + {len(venues_to_process) - 3} more"
                log_event(
                    "VENUE_PROCESSING_STARTED",
                    details={
                        "message": f"{len(venues_to_process)} venues for {date.strftime('%Y-%m-%d')}: {venues_str}"
                    },
                )

                logger.info(
                    "📅 Processing %s (%s-%s/%s day-venue combinations)",
                    date.strftime("%Y-%m-%d"),
                    start_combination,
                    end_combination,
                    total_combinations,
                )

                log_event("DATA_INGESTION_STARTED")
                log_event("ADAPTER_FETCH_STARTED", "instruments")
                log_event("PROCESSING_STARTED")
                log_event("CLASSIFICATION_STARTED")

                # Delegate to InstrumentsService for orchestration
                venues_raw = kwargs.get("venues")
                venues_list: list[str] | None = (
                    list(cast(list[str], venues_raw)) if isinstance(venues_raw, list) else None
                )
                instrument_ids_raw = kwargs.get("instrument_ids")
                instrument_ids_list: list[str] | None = (
                    list(cast(list[str], instrument_ids_raw)) if isinstance(instrument_ids_raw, list) else None
                )

                result = asyncio.run(
                    self.instruments_service.generate_instruments_for_date(
                        date=date,
                        exchanges=exchanges_to_process if cefi else None,
                        force=force,
                        cefi=cefi,
                        tradfi=tradfi,
                        defi=defi,
                        venues=venues_list,
                        instrument_ids=instrument_ids_list,
                        tradfi_venues=None,  # No longer filtered - always use all TradFi venues
                    )
                )

                # Track error and warning counts from processing
                processing_errors = _to_int(cast(HandlerResultValue, result.get("error_count", 0)))
                processing_warnings = _to_int(cast(HandlerResultValue, result.get("warning_count", 0)))
                total_processing_errors += processing_errors
                total_processing_warnings += processing_warnings

                instruments_count = _to_int(cast(HandlerResultValue, result.get("instruments_generated", 0)))
                log_event("CLASSIFICATION_COMPLETED", details={"message": str(instruments_count)})
                log_event("PROCESSING_COMPLETED")
                log_event("ADAPTER_FETCH_COMPLETED", details={"message": str(instruments_count)})
                log_event("DATA_INGESTION_COMPLETED", details={"message": str(instruments_count)})
                log_event("DATA_BROADCAST")
                log_event("PERSISTENCE_STARTED")
                log_event("PERSISTENCE_COMPLETED")

                # Venue completion event
                log_event(
                    "VENUE_PROCESSING_COMPLETED",
                    details={"message": f"{len(venues_to_process)} venues for {date.strftime('%Y-%m-%d')}"},
                )

                # Date completion event
                log_event("DATE_PROCESSING_COMPLETED", details={"message": date.strftime("%Y-%m-%d")})
                if result.get("status") == "success":
                    total_generated += instruments_count
                    total_dates_processed += 1

                    # Log if there were processing errors/warnings even though overall status is success
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            "⚠️ Processing completed with %s errors and %s warnings for %s",
                            processing_errors,
                            processing_warnings,
                            date.strftime("%Y-%m-%d"),
                        )

                    # Note: CSV sampling is handled automatically by CloudInstrumentStorage
                    # when storing to GCS (via unified-trading-library SamplingService).
                    # No need to download and sample again here - that would be wasteful.
                elif result.get("status") == "warning":
                    logger.warning("⚠️ No instruments generated for %s", date.strftime("%Y-%m-%d"))
                    total_dates_processed += 1
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            "   (Processing had %s errors and %s warnings)", processing_errors, processing_warnings
                        )
                else:
                    logger.error(
                        "Failed to generate instruments for %s: %s",
                        date.strftime("%Y-%m-%d"),
                        result.get("message", "Unknown error"),
                    )
                    total_errors += 1
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            "   (Processing had %s errors and %s warnings)", processing_errors, processing_warnings
                        )

            except (OSError, ValueError, TypeError, RuntimeError, Exception) as e:
                # In-flight validation: catch any exception from service, log, continue with other dates
                logger.exception("Failed to process %s: %s", date.strftime("%Y-%m-%d"), e)
                total_errors += 1

        # Calculate success rate and provide comprehensive summary
        total_attempted = total_dates_processed + total_errors
        success_rate = (total_dates_processed / total_attempted * 100) if total_attempted > 0 else 0

        logger.info("📊 Instrument generation pipeline complete:")
        logger.info("   Generated: %s instruments", total_generated)
        logger.info(
            "   Dates processed: %s/%s successful (%..1f%)", total_dates_processed, total_attempted, success_rate
        )
        logger.info("   Skipped: %s (already existed)", total_skipped)
        logger.info("   Date-level errors: %s", total_errors)
        logger.info("   Processing errors: %s", total_processing_errors)
        logger.info("   Processing warnings: %s", total_processing_warnings)

        # Log completion status
        if total_errors == 0:
            log_event("STOPPED")
        else:
            log_event(
                "FAILED",
                details={"message": f"{total_errors} date-level errors, {total_processing_errors} processing errors"},
            )

        handler_result: dict[str, HandlerResultValue] = {
            "status": "success" if total_errors == 0 else "partial",
            "success": total_errors == 0,
            "instruments_generated": total_generated,
            "dates_processed": total_dates_processed,
            "dates_attempted": total_attempted,
            "dates_skipped": total_skipped,
            "dates_with_errors": total_errors,
            "processing_errors": total_processing_errors,
            "processing_warnings": total_processing_warnings,
            "success_rate_percent": int(success_rate),
            "pipeline_summary": {
                "total_instruments": total_generated,
                "processing_success_rate": int(success_rate),
                "skipped_existing": total_skipped,
                "date_level_errors": total_errors,
                "processing_errors": total_processing_errors,
                "processing_warnings": total_processing_warnings,
            },
        }
        return handler_result

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "instruments_service"):
            self.instruments_service.cleanup()
        super().cleanup()
