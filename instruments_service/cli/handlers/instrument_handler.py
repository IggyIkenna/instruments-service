"""
Instrument Handler

Generates instrument definitions using direct GCS existence checks.
No missing data report dependencies - pure force/skip logic.
"""

import logging
import asyncio
from typing import Dict, Any
from datetime import datetime, timezone

from instruments_service.cli.base_handler import ModeHandler
from instruments_service.app.core.instruments_service import InstrumentsService
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from unified_cloud_services import VenueMapping, parse_date, get_date_range
from instruments_service.app.core.cloud_data_provider import CloudDataProvider

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

    def run(self, start_date, end_date, force=False, **kwargs) -> Dict[str, Any]:
        """Execute instrument generation."""
        return self._execute_instrument_generation(start_date, end_date, force, **kwargs)

    def _execute_instrument_generation(self, start_date, end_date, force=False, **kwargs):
        """Generate instruments with direct GCS existence checks."""
        # Parse dates
        if isinstance(start_date, str):
            start_date = parse_date(start_date)
        if isinstance(end_date, str):
            end_date = parse_date(end_date)

        # Generate date range
        date_range = get_date_range(start_date, end_date)

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

        for date in date_range:
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
                            skip_msg = f"⏭️ Skipping {date.strftime('%Y-%m-%d')} - instruments exist"
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

                logger.info(f"📅 Processing {date.strftime('%Y-%m-%d')}")

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
        logger.info(
            f"   Dates processed: {total_dates_processed}/{total_attempted} successful ({success_rate:.1f}%)"
        )
        logger.info(f"   Skipped: {total_skipped} (already existed)")
        logger.info(f"   Date-level errors: {total_errors}")
        logger.info(f"   Processing errors: {total_processing_errors}")
        logger.info(f"   Processing warnings: {total_processing_warnings}")

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
