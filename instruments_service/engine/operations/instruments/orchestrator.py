"""
Instruments Orchestration Module

Coordinates instrument generation workflow across market types (CeFi, TradFi, DeFi).
Combines functionality from base, processors, and helper modules for cleaner code organization.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import cast

import pandas as pd
from unified_events_interface import ErrorWarningCounter

from instruments_service.engine.operations.instruments.orchestrator_base import OrchestratorBase
from instruments_service.engine.operations.instruments.orchestrator_helpers import (
    add_tradfi_placeholders,
    convert_to_dataframe,
    extract_venues_from_instrument_ids,
    filter_by_instrument_ids,
    handle_no_instruments,
    handle_utc_midnight_spanning,
    normalize_venues_filter,
    validate_venues,
)
from instruments_service.engine.operations.instruments.orchestrator_processors import (
    process_cefi,
    process_defi,
    process_tradfi,
)
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class InstrumentsOrchestrator(OrchestratorBase):
    """
    Orchestrates instrument generation workflow.

    Responsibilities:
    - Coordinate processing across market types (CeFi, TradFi, DeFi)
    - Handle venue filtering and validation
    - Manage parallel processing of exchanges/venues
    - Coordinate storage operations
    - Track errors and warnings

    Inherits base functionality from OrchestratorBase and delegates processing
    to specialized processor and helper modules.
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize the orchestrator.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - sink_bucket: Sink bucket name (optional, auto-detected)
                - analytics_dataset: Analytics dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        # Initialize base orchestrator
        super().__init__(config)

        logger.info("✅ InstrumentsOrchestrator initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: list[str] | None = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        skip_storage: bool = False,
        venues: list[str] | str | None = None,
        instrument_ids: list[str] | str | None = None,
    ) -> dict[str, object]:
        """
        Generate instrument definitions for a specific date.

        Args:
            date: Target date for instrument generation
            exchanges: Deprecated - use venues instead
            force: Force refresh of cached data
            cefi: Enable CeFi exchange processing
            tradfi: Enable TradFi exchange processing
            defi: Enable DeFi protocol processing
            skip_storage: Skip storing to cloud storage (for testing)
            venues: Specific venues to process (overrides category flags)
            instrument_ids: Specific instrument IDs to filter

        Returns:
            Dictionary with processing results and statistics
        """
        # Parameter handling
        if exchanges:
            logger.warning("⚠️ 'exchanges' parameter deprecated, use 'venues' instead")
            venues = exchanges

        # Default to all categories if none specified
        if not any([cefi, tradfi, defi]) and not venues and not instrument_ids:
            logger.info("🔍 No category specified, processing ALL market types")
            cefi = tradfi = defi = True

        # Normalize and extract venues
        venues_filter = normalize_venues_filter(venues)
        if instrument_ids:
            extracted_venues = extract_venues_from_instrument_ids(instrument_ids)
            if extracted_venues:
                if venues_filter:
                    logger.info("🔍 Merging extracted venues %s with filter %s", extracted_venues, venues_filter)
                    venues_filter = list(set(venues_filter) | set(extracted_venues))
                else:
                    logger.info("🔍 Using venues extracted from instrument_ids: %s", extracted_venues)
                    venues_filter = extracted_venues

        # Validate venues
        venues_filter, tradfi_venues = validate_venues(venues_filter, self.venue_mapping, cefi, tradfi, defi)

        # Setup error tracking
        error_warning_counter = ErrorWarningCounter()
        root_logger = logging.getLogger()
        root_logger.addHandler(error_warning_counter)

        date_str = date.strftime("%Y-%m-%d")
        logger.info("📅 Starting instrument generation for %s", date_str)

        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            # Process CeFi exchanges
            if cefi:
                cefi_instruments = await process_cefi(self, date, venues_filter)
                all_instruments.update(cefi_instruments)

            # Process TradFi exchanges
            if tradfi:
                tradfi_instruments = await process_tradfi(self, date, venues_filter, tradfi_venues)
                all_instruments.update(tradfi_instruments)

            # Process DeFi protocols
            if defi:
                defi_instruments = await process_defi(self, date, venues_filter)
                all_instruments.update(defi_instruments)

            # Filter by instrument_ids if specified
            if instrument_ids:
                all_instruments = filter_by_instrument_ids(all_instruments, instrument_ids)

            # Convert to DataFrame
            instruments_df = convert_to_dataframe(all_instruments, skip_storage)

            # Handle no instruments case
            if instruments_df.empty:
                error_response = handle_no_instruments(
                    date_str, tradfi, venues_filter, error_warning_counter, root_logger
                )
                if error_response:
                    return error_response

            # Add TradFi placeholders if needed
            if tradfi and venues_filter and not skip_storage:
                instruments_df = add_tradfi_placeholders(instruments_df, date, venues_filter, self.venue_mapping)

            # Store instruments if not skipping
            if not skip_storage:
                success = cast(
                    bool,
                    self.cloud_storage.store_instruments(
                        instruments_df=instruments_df, table_name="instruments", date=date
                    ),
                )
                if not success:
                    logger.error("Failed to store instruments for %s", date_str)
                    return {
                        "status": "error",
                        "date": date_str,
                        "instruments_generated": len(instruments_df),
                        "message": "Failed to store instruments to cloud storage",
                        "error_count": error_warning_counter.error_count,
                        "warning_count": error_warning_counter.warning_count,
                    }

                # Handle UTC midnight spanning instruments
                handle_utc_midnight_spanning(instruments_df, date, self.cloud_storage)

            # Get final counts
            error_count = error_warning_counter.error_count
            warning_count = error_warning_counter.warning_count
            root_logger.removeHandler(error_warning_counter)

            # Prepare response
            status = "success" if error_count == 0 else "completed_with_errors"

            response: dict[str, object] = {
                "status": status,
                "date": date_str,
                "instruments_generated": len(instruments_df),
                "instruments_by_venue": {},
                "instruments_by_type": {},
                "error_count": error_count,
                "warning_count": warning_count,
            }

            # Add venue breakdown if available
            if "venue" in instruments_df.columns and not instruments_df.empty:
                venue_counts = instruments_df["venue"].value_counts().to_dict()
                response["instruments_by_venue"] = {str(k): int(v) for k, v in venue_counts.items()}

            # Add type breakdown if available
            if "instrument_type" in instruments_df.columns and not instruments_df.empty:
                type_counts = instruments_df["instrument_type"].value_counts().to_dict()
                response["instruments_by_type"] = {str(k): int(v) for k, v in type_counts.items()}

            logger.info(
                "✅ Completed instrument generation for %s: %s instruments, %s errors, %s warnings",
                date_str,
                len(instruments_df),
                error_count,
                warning_count,
            )

            return response

        except (ValueError, TypeError, KeyError) as e:
            logger.exception("Critical error during instrument generation: %s", e)
            root_logger.removeHandler(error_warning_counter)
            return {
                "status": "error",
                "date": date_str,
                "instruments_generated": 0,
                "message": str(e),
                "error_count": error_warning_counter.error_count + 1,
                "warning_count": error_warning_counter.warning_count,
            }

    async def batch_generate(
        self,
        start_date: datetime,
        end_date: datetime,
        force: bool = False,
        cefi: bool = True,
        tradfi: bool = True,
        defi: bool = True,
        venues: list[str] | None = None,
        batch_size: int | None = None,
    ) -> dict[str, object]:
        """
        Batch generate instruments for a date range.

        Args:
            start_date: Start date for generation
            end_date: End date for generation (inclusive)
            force: Force refresh of cached data
            cefi: Enable CeFi exchange processing
            tradfi: Enable TradFi exchange processing
            defi: Enable DeFi protocol processing
            venues: Specific venues to process
            batch_size: Override default batch size

        Returns:
            Dictionary with batch processing results
        """
        return await self.batch_processor.batch_generate_instruments(
            orchestrator=self,
            start_date=start_date,
            end_date=end_date,
            force=force,
            cefi=cefi,
            tradfi=tradfi,
            defi=defi,
            venues=venues,
            batch_size=batch_size,
        )

    def query_instruments(
        self,
        venue: str | None = None,
        instrument_type: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
    ) -> pd.DataFrame:
        """
        Query stored instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            base_asset: Optional base asset filter
            quote_asset: Optional quote asset filter

        Returns:
            DataFrame with instruments
        """
        result: pd.DataFrame = self.cloud_storage.query_instruments(venue=venue, instrument_type=instrument_type)
        if base_asset is not None and not result.empty and "base_asset" in result.columns:
            result = result[result["base_asset"] == base_asset]
        if quote_asset is not None and not result.empty and "quote_asset" in result.columns:
            result = result[result["quote_asset"] == quote_asset]
        return result
