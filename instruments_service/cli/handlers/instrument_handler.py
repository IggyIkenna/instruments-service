"""
Instrument Handler

Generates instrument definitions using direct GCS existence checks.
No missing data report dependencies - pure force/skip logic.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from unified_cloud_services import VenueMapping, get_date_range, parse_date

try:
    from unified_cloud_services.observability import log_event
except ImportError:

    def log_event(event_name: str, details: str = "") -> None:
        pass  # noqa: ARG001


from instruments_service.app.core.cloud_data_provider import CloudDataProvider
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.instruments_service import InstrumentsService
from instruments_service.app.core.selective_validation import validate_required_api_keys
from instruments_service.cli.base_handler import ModeHandler
from instruments_service.config import instruments_config

logger = logging.getLogger(__name__)


class InstrumentHandler(ModeHandler):
    """Generate instruments with force mode and direct GCS checks only."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # Initialize services directly (no ServiceContainer)
        from instruments_service.config import get_config as get_service_config

        project_id = config.get("project_id") or get_service_config().gcp_project_id

        # Initialize InstrumentsService (orchestration wrapper)
        service_config = {
            "project_id": project_id,
            "enable_ccxt_integration": True,
            "enable_metadata_caching": True,
        }
        self.instruments_service = InstrumentsService(service_config)

        # Initialize cloud storage (for CLI-specific operations)
        self.cloud_storage = CloudInstrumentStorage()

        # Venue mapping (for CLI-specific operations)
        self.venue_mapping = VenueMapping()

        logger.debug("✅ InstrumentHandler initialized")

    def _get_venues_to_process(
        self, requested_venues: Optional[List[str]], cefi: bool, tradfi: bool, defi: bool
    ) -> List[str]:
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
        venues = []

        # If explicit venues requested, use those (takes precedence)
        if requested_venues:
            return requested_venues

        # Otherwise, build from categories
        if cefi:
            venues.extend(self.venue_mapping.all_tardis_exchanges)
        if tradfi:
            venues.extend(self.venue_mapping.all_databento_venues)
        if defi:
            venues.extend(self.venue_mapping.all_defi_venues)

        return venues

    def run(self, start_date, end_date, force=False, **kwargs) -> Dict[str, Any]:
        """Execute instrument generation."""
        return self._execute_instrument_generation(start_date, end_date, force, **kwargs)

    def _execute_instrument_generation(self, start_date, end_date, force=False, **kwargs):
        """Generate instruments with direct GCS existence checks."""
        log_event("STARTED")
        log_event("VALIDATION_STARTED")
        try:
            # Parse dates
            if isinstance(start_date, str):
                start_date = parse_date(start_date)
            if isinstance(end_date, str):
                end_date = parse_date(end_date)

            # Generate date range
            date_range = get_date_range(start_date, end_date)

            # Get requested venues (from --venues CLI arg or default to all)
            requested_venues = kwargs.get("venues")

            # Determine market categories to process
            cefi = kwargs.get("cefi", False)
            tradfi = kwargs.get("tradfi", False)
            defi = kwargs.get("defi", False)

            # Default: process ALL if no flags specified
            if not cefi and not tradfi and not defi:
                cefi = tradfi = defi = True

            # Build venue list based on categories + explicit venues
            venues_to_process = self._get_venues_to_process(requested_venues, cefi, tradfi, defi)

            # Selective API key validation (only for requested venues)
            try:
                # Validate API keys (result not used yet, but validation ensures keys exist)
                _ = validate_required_api_keys(venues_to_process)
                logger.info(f"✅ Validated API keys for {len(venues_to_process)} venues")
            except ValueError as e:
                log_event("VALIDATION_FAILED", f"API key validation: {str(e)}")
                raise

            log_event("VALIDATION_COMPLETED")
        except Exception as e:
            log_event("VALIDATION_FAILED", str(e))
            log_event("FAILED", f"Validation error: {str(e)}")
            raise

        # Observability metrics tracking
        total_generated = 0
        total_dates_processed = 0
        total_skipped = 0
        total_errors = 0
        total_processing_errors = 0  # Errors from processing (not date-level failures)
        total_processing_warnings = 0  # Warnings from processing
        today = datetime.now(timezone.utc).date()

        # Determine which market types to process based on flags
        cefi = kwargs.get("cefi", False)
        tradfi = kwargs.get("tradfi", False)
        defi = kwargs.get("defi", False)

        # Default behavior: If no flags specified, process ALL market types
        # If flags are specified, only process those market types
        if not cefi and not tradfi and not defi:
            # Default: Process ALL market types (CEFI, TRADFI, DEFI)
            cefi = True
            tradfi = True
            defi = True
            logger.info("🌍 Processing ALL market types: CEFI, TRADFI, and DEFI")
        else:
            # Log which market types will be processed based on flags
            market_types = []
            if cefi:
                market_types.append("CEFI")
            if tradfi:
                market_types.append("TRADFI")
            if defi:
                market_types.append("DEFI")
            logger.info(f"🔍 Processing market types: {', '.join(market_types)}")

        # Set exchanges for CEFI processing (always use all exchanges - no filtering)
        # Note: --exchanges CLI arg was removed as it filtered within aggregated instruments.parquet
        if cefi:
            exchanges_to_process = self.venue_mapping.all_tardis_exchanges
        else:
            exchanges_to_process = []

        # Compute day-venue combinations for 3-level event counters
        total_combinations = len(date_range) * len(venues_to_process)
        total_dates = len(date_range)

        for date_idx, date in enumerate(date_range, start=1):
            # Skip future dates
            if date.date() > today:
                logger.warning(f"⚠️ Skipping future date: {date.strftime('%Y-%m-%d')}")
                break

            try:
                # Check if file exists (using cloud service)
                if not force:
                    try:
                        # Build list of categories to check based on flags
                        categories_to_check = []
                        if cefi:
                            categories_to_check.append("CEFI")
                        if tradfi:
                            categories_to_check.append("TRADFI")
                        if defi:
                            categories_to_check.append("DEFI")

                        # Get venues from kwargs (from --venues CLI arg)
                        venues_to_check = kwargs.get("venues")

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
                            if instruments_config.shard_launched_at:
                                # Race condition detected: another deployment completed while we were launching
                                skip_msg = f"⚠️ RACE_CONDITION: Skipping {date_str} - data appeared after launch"
                                if venues_to_check:
                                    skip_msg += f" for {categories_to_check}/{venues_to_check}"
                                if instruments_config.deployment_id:
                                    skip_msg += f" (deployment={instruments_config.deployment_id}, launched_at={instruments_config.shard_launched_at})"
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
                    except Exception:
                        # File doesn't exist, proceed with generation
                        pass

                # Level 1: Day-venue combination counters (shows overall progress)
                # Calculate day-venue progress: this date processes ALL venues_to_process
                start_combination = (date_idx - 1) * len(venues_to_process) + 1
                end_combination = date_idx * len(venues_to_process)

                # Level 2: Daily progress (dates being processed)
                log_event("DATE_PROCESSING_STARTED", f"{date.strftime('%Y-%m-%d')} ({date_idx}/{total_dates})")

                # Level 3: Venues for this date (will process all in single call)
                venues_str = ", ".join(venues_to_process[:3])
                if len(venues_to_process) > 3:
                    venues_str += f" + {len(venues_to_process) - 3} more"
                log_event(
                    "VENUE_PROCESSING_STARTED",
                    f"{len(venues_to_process)} venues for {date.strftime('%Y-%m-%d')}: {venues_str}",
                )

                logger.info(
                    f"📅 Processing {date.strftime('%Y-%m-%d')} ({start_combination}-{end_combination}/{total_combinations} day-venue combinations)"
                )

                log_event("DATA_INGESTION_STARTED")
                log_event("ADAPTER_FETCH_STARTED", "instruments")
                log_event("PROCESSING_STARTED")
                log_event("CLASSIFICATION_STARTED")

                # Delegate to InstrumentsService for orchestration
                result = asyncio.run(
                    self.instruments_service.generate_instruments_for_date(
                        date=date,
                        exchanges=exchanges_to_process if cefi else None,
                        force=force,
                        cefi=cefi,
                        tradfi=tradfi,
                        defi=defi,
                        venues=kwargs.get("venues"),
                        instrument_ids=kwargs.get("instrument_ids"),
                        tradfi_venues=None,  # No longer filtered - always use all TradFi venues
                    )
                )

                # Track error and warning counts from processing
                processing_errors = result.get("error_count", 0)
                processing_warnings = result.get("warning_count", 0)
                total_processing_errors += processing_errors
                total_processing_warnings += processing_warnings

                log_event("CLASSIFICATION_COMPLETED", str(result.get("instruments_generated", 0)))
                log_event("PROCESSING_COMPLETED")
                log_event("ADAPTER_FETCH_COMPLETED", str(result.get("instruments_generated", 0)))
                log_event("DATA_INGESTION_COMPLETED", str(result.get("instruments_generated", 0)))
                log_event("UPLOAD_STARTED")
                log_event("UPLOAD_COMPLETED")

                # Venue completion event
                log_event(
                    "VENUE_PROCESSING_COMPLETED", f"{len(venues_to_process)} venues for {date.strftime('%Y-%m-%d')}"
                )

                # Date completion event
                log_event("DATE_PROCESSING_COMPLETED", date.strftime("%Y-%m-%d"))
                if result.get("status") == "success":
                    total_generated += result.get("instruments_generated", 0)
                    total_dates_processed += 1

                    # Log if there were processing errors/warnings even though overall status is success
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            f"⚠️ Processing completed with {processing_errors} errors and {processing_warnings} warnings "
                            f"for {date.strftime('%Y-%m-%d')}"
                        )

                    # Note: CSV sampling is handled automatically by CloudInstrumentStorage
                    # when storing to GCS (via unified-cloud-services SamplingService).
                    # No need to download and sample again here - that would be wasteful.
                elif result.get("status") == "warning":
                    logger.warning(f"⚠️ No instruments generated for {date.strftime('%Y-%m-%d')}")
                    total_dates_processed += 1
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            f"   (Processing had {processing_errors} errors and {processing_warnings} warnings)"
                        )
                else:
                    logger.error(
                        f"❌ Failed to generate instruments for {date.strftime('%Y-%m-%d')}: {result.get('message', 'Unknown error')}"
                    )
                    total_errors += 1
                    if processing_errors > 0 or processing_warnings > 0:
                        logger.info(
                            f"   (Processing had {processing_errors} errors and {processing_warnings} warnings)"
                        )

            except Exception as e:
                logger.error(
                    f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}",
                    exc_info=True,
                )
                total_errors += 1

        # Calculate success rate and provide comprehensive summary
        total_attempted = total_dates_processed + total_errors
        success_rate = (total_dates_processed / total_attempted * 100) if total_attempted > 0 else 0

        logger.info("📊 Instrument generation pipeline complete:")
        logger.info(f"   Generated: {total_generated} instruments")
        logger.info(f"   Dates processed: {total_dates_processed}/{total_attempted} successful ({success_rate:.1f}%)")
        logger.info(f"   Skipped: {total_skipped} (already existed)")
        logger.info(f"   Date-level errors: {total_errors}")
        logger.info(f"   Processing errors: {total_processing_errors}")
        logger.info(f"   Processing warnings: {total_processing_warnings}")

        # Log completion status
        if total_errors == 0:
            log_event("STOPPED")
        else:
            log_event("FAILED", f"{total_errors} date-level errors, {total_processing_errors} processing errors")

        return {
            "status": "success" if total_errors == 0 else "partial",
            "success": total_errors == 0,
            "instruments_generated": total_generated,
            "dates_processed": total_dates_processed,
            "dates_attempted": total_attempted,
            "dates_skipped": total_skipped,
            "dates_with_errors": total_errors,
            "processing_errors": total_processing_errors,
            "processing_warnings": total_processing_warnings,
            "success_rate_percent": success_rate,
            "pipeline_summary": {
                "total_instruments": total_generated,
                "processing_success_rate": success_rate,
                "skipped_existing": total_skipped,
                "date_level_errors": total_errors,
                "processing_errors": total_processing_errors,
                "processing_warnings": total_processing_warnings,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "instruments_service"):
            self.instruments_service.cleanup()
        super().cleanup()
