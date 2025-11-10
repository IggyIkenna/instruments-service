"""
Instrument Handler

Generates instrument definitions using direct GCS existence checks.
No missing data report dependencies - pure force/skip logic.
"""

import os
import logging
import pandas as pd
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ..base_handler import ModeHandler
from ...app.core.instrument_processing_service import InstrumentProcessingService
from ...app.core.cloud_instrument_storage import CloudInstrumentStorage
from ...config import VenueMapping, DatabentoInstrumentConfig

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string to timezone-aware datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(
            f"Invalid date format '{date_str}'. Use YYYY-MM-DD format."
        ) from e


def get_date_range(start_date_str: str, end_date_str: str) -> List[datetime]:
    """Generate date range from start/end date strings."""
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)

    if start_date > end_date:
        raise ValueError(
            f"Start date {start_date_str} must be <= end date {end_date_str}"
        )

    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date)
        current_date += timedelta(days=1)

    return date_range


class InstrumentHandler(ModeHandler):
    """Generate instruments with force mode and direct GCS checks only."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # Initialize services directly (no ServiceContainer)
        project_id = config.get("project_id", "central-element-323112")

        # Initialize instrument processing service
        processing_config = {
            "project_id": project_id,
            "enable_ccxt_integration": True,
            "enable_metadata_caching": True,
        }
        self.instrument_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()

        # Venue mapping
        self.venue_mapping = VenueMapping()
        
        # Databento instrument config
        self.databento_config = DatabentoInstrumentConfig()

        logger.debug("✅ InstrumentHandler initialized")

    def run(self, start_date, end_date, force=False, **kwargs) -> Dict[str, Any]:
        """Execute instrument generation."""
        return self._execute_instrument_generation(
            start_date, end_date, force, **kwargs
        )

    def _execute_instrument_generation(
        self, start_date, end_date, force=False, **kwargs
    ):
        """Generate instruments with direct GCS existence checks."""
        # Parse dates
        if isinstance(start_date, str):
            start_date = parse_date(start_date)
        if isinstance(end_date, str):
            end_date = parse_date(end_date)

        # Generate date range
        date_range = get_date_range(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        # Observability metrics tracking
        total_generated = 0
        total_dates_processed = 0
        total_skipped = 0
        total_errors = 0
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
        
        # Set exchanges for CEFI processing
        if cefi:
            exchanges_to_process = kwargs.get(
                "exchanges", self.venue_mapping.all_tardis_exchanges
            )
        else:
            exchanges_to_process = []

        for date in date_range:
            # Skip future dates
            if date.date() > today:
                logger.warning(f"⚠️ Skipping future date: {date.strftime('%Y-%m-%d')}")
                continue

            try:
                # Direct GCS existence check
                instrument_path = f"instrument_availability/by_date/day-{date.strftime('%Y-%m-%d')}/instruments.parquet"

                # Check if file exists (using cloud service)
                if not force:
                    try:
                        # Use cloud_data_provider to check existence
                        from ...app.core.cloud_data_provider import CloudDataProvider

                        data_provider = CloudDataProvider()
                        if data_provider.check_instruments_exist(date):
                            logger.info(
                                f"⏭️ Skipping {date.strftime('%Y-%m-%d')} - instruments exist"
                            )
                            total_skipped += 1
                            continue
                    except Exception:
                        # File doesn't exist, proceed with generation
                        pass

                logger.info(f"📅 Processing {date.strftime('%Y-%m-%d')}")

                # Generate instruments using service
                # Remove cefi/tradfi/defi from kwargs to avoid duplicate keyword arguments
                filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ['cefi', 'tradfi', 'defi']}
                instruments = self._generate_instruments_for_date(
                    date, force, exchanges_to_process, cefi=cefi, tradfi=tradfi, defi=defi, **filtered_kwargs
                )

                if instruments:
                    # Convert to DataFrame with proper field handling
                    instruments_list = []
                    for inst_key, inst_obj in instruments.items():
                        if hasattr(inst_obj, "model_dump"):
                            # Pydantic model - use model_dump
                            instruments_list.append(inst_obj.model_dump())
                        else:
                            # Fallback for other types
                            instruments_list.append(inst_obj)

                    instruments_df = pd.DataFrame(instruments_list)

                    # Store using CloudInstrumentStorage
                    logger.info(f"📤 Uploading {len(instruments_df)} instruments...")
                    success = self.cloud_storage.store_instruments(
                        instruments_df=instruments_df,
                        table_name="instruments",
                        date=date,
                    )

                    if success:
                        logger.info(
                            f"✅ Uploaded instruments for {date.strftime('%Y-%m-%d')}"
                        )
                        total_generated += len(instruments)
                        total_dates_processed += 1
                    else:
                        logger.error(
                            f"❌ Failed to upload instruments for {date.strftime('%Y-%m-%d')}"
                        )
                        total_errors += 1

                    # CSV sampling using centralized service
                    from unified_cloud_services import create_sampling_service

                    sampling_service = create_sampling_service()
                    sampling_service.generate_csv_sample(
                        df=instruments_df,
                        filename_prefix="instruments",
                        metadata={"date": date},
                    )
                else:
                    logger.warning(
                        f"⚠️ No instruments generated for {date.strftime('%Y-%m-%d')}"
                    )
                    total_dates_processed += 1

            except Exception as e:
                logger.error(
                    f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}",
                    exc_info=True,
                )
                total_errors += 1

        # Calculate success rate and provide comprehensive summary
        total_attempted = total_dates_processed + total_errors
        success_rate = (
            (total_dates_processed / total_attempted * 100)
            if total_attempted > 0
            else 0
        )

        logger.info(f"📊 Instrument generation pipeline complete:")
        logger.info(f"   Generated: {total_generated} instruments")
        logger.info(
            f"   Dates processed: {total_dates_processed}/{total_attempted} successful ({success_rate:.1f}%)"
        )
        logger.info(f"   Skipped: {total_skipped} (already existed)")
        logger.info(f"   Errors: {total_errors}")

        return {
            "status": "success" if total_errors == 0 else "partial",
            "success": total_errors == 0,
            "instruments_generated": total_generated,
            "dates_processed": total_dates_processed,
            "dates_attempted": total_attempted,
            "dates_skipped": total_skipped,
            "dates_with_errors": total_errors,
            "success_rate_percent": success_rate,
            "pipeline_summary": {
                "total_instruments": total_generated,
                "processing_success_rate": success_rate,
                "skipped_existing": total_skipped,
                "error_count": total_errors,
            },
        }

    def _generate_instruments_for_date(
        self, date, force=False, exchanges=None, cefi=False, tradfi=False, defi=False, **kwargs
    ):
        """Generate instruments using instrument processing service."""
        instruments = {}

        # Process CEFI (Tardis) exchanges
        if cefi:
            # Use specified exchanges or all Tardis exchanges
            if exchanges is None:
                exchanges = self.venue_mapping.all_tardis_exchanges

            for tardis_exchange in exchanges:
                try:
                    # Process exchange instruments via Tardis
                    exchange_instruments = asyncio.run(
                        self.instrument_service.process_exchange_instruments(
                            tardis_exchange, target_date=date, force=force
                        )
                    )
                    if exchange_instruments:
                        instruments.update(exchange_instruments)
                except Exception as e:
                    logger.error(
                        f"❌ Failed to process {tardis_exchange}: {e}", exc_info=True
                    )

        # Process TRADFI (Databento) exchanges
        if tradfi:
            try:
                # Common TradFi exchanges via Databento
                databento_exchanges = ["CME", "NASDAQ", "NYSE", "ICE"]
                
                # Track DBEQ.BASIC symbols to avoid duplicate fetches
                dbeq_symbols_fetched = False
                
                for exchange in databento_exchanges:
                    try:
                        # For NASDAQ/NYSE, fetch all DBEQ.BASIC symbols once
                        if exchange in ["NASDAQ", "NYSE"]:
                            if dbeq_symbols_fetched:
                                logger.info(f"⏭️ Skipping {exchange} (already fetched DBEQ.BASIC symbols via NASDAQ)")
                                continue
                            
                            # Get all equities from DBEQ.BASIC dataset (includes both NASDAQ and NYSE stocks)
                            symbols = self.databento_config._unified.get_symbols_for_dataset("DBEQ.BASIC")
                            dbeq_symbols_fetched = True
                            logger.info(f"📋 Fetching all DBEQ.BASIC symbols ({len(symbols)} symbols) for {exchange}")
                        else:
                            # Get symbols for this exchange from config
                            symbols = self._get_symbols_for_databento_exchange(exchange)
                        
                        if not symbols:
                            logger.warning(f"⚠️ No symbols configured for {exchange}, skipping")
                            continue
                        
                        # Fetch Databento instruments
                        databento_instruments = self.instrument_service.fetch_databento_instruments(
                            exchange=exchange,
                            symbols=symbols,
                            target_date=date,
                        )
                        if databento_instruments:
                            instruments.update(databento_instruments)
                            logger.info(f"✅ Processed {len(databento_instruments)} instruments from {exchange}")
                    except Exception as e:
                        logger.error(
                            f"❌ Failed to process Databento exchange {exchange}: {e}", exc_info=True
                        )
            except Exception as e:
                logger.error(f"❌ Failed to initialize Databento processing: {e}", exc_info=True)

        # Process DEFI protocols
        if defi:
            try:
                # Common DeFi protocols
                defi_protocols = [
                    ("uniswap_v3", "ETHEREUM"),
                    ("curve", "ETHEREUM"),
                    ("aave_v3", "ETHEREUM"),
                    ("etherfi", "ETHEREUM"),
                    ("lido", "ETHEREUM"),
                ]
                for protocol, chain in defi_protocols:
                    try:
                        # Fetch DeFi instruments
                        defi_instruments = self.instrument_service.fetch_defi_instruments(
                            protocol=protocol,
                            chain=chain,
                        )
                        if defi_instruments:
                            instruments.update(defi_instruments)
                            logger.info(f"✅ Processed {len(defi_instruments)} instruments from {protocol}")
                    except Exception as e:
                        logger.error(
                            f"❌ Failed to process DeFi protocol {protocol}: {e}", exc_info=True
                        )
            except Exception as e:
                logger.error(f"❌ Failed to initialize DeFi processing: {e}", exc_info=True)

        return instruments

    def _get_symbols_for_databento_exchange(self, exchange: str) -> List[str]:
        """
        Get symbols for a Databento exchange from unified config.
        
        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ', 'NYSE', 'ICE')
            
        Returns:
            List of symbols to fetch for this exchange
        """
        # For CME, ICE, etc., use venue-based lookup
        symbols = self.databento_config.get_symbols_for_venue(exchange.upper())
        
        if not symbols:
            logger.warning(f"⚠️ No symbols configured for {exchange}")
            return []
        
        logger.debug(f"📋 Found {len(symbols)} symbols for {exchange}: {symbols[:5]}..." if len(symbols) > 5 else f"📋 Found {len(symbols)} symbols for {exchange}: {symbols}")
        return symbols

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "instrument_service"):
            self.instrument_service.cleanup()
        super().cleanup()
