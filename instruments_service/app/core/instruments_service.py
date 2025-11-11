"""
Main Orchestration Service for Instruments Service

Coordinates instrument processing, storage, and batch operations.
Follows unified repository structure pattern.
"""

import logging
import pandas as pd
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from .instrument_processing_service import InstrumentProcessingService
from .cloud_instrument_storage import CloudInstrumentStorage
from .batch_processor import InstrumentBatchProcessor
from ...config import VenueMapping

logger = logging.getLogger(__name__)


class InstrumentsService:
    """
    Main orchestration service that coordinates:
    - Instrument processing (fetching from APIs, generating canonical keys)
    - Cloud storage (GCS and BigQuery)
    - Batch operations (date range processing, memory estimation)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the orchestration service.

        Args:
            config: Configuration dictionary with:
                - project_id: GCP project ID
                - gcs_bucket: GCS bucket name (optional, auto-detected)
                - bigquery_dataset: BigQuery dataset (optional, default: market_data_hft)
                - enable_ccxt_integration: Enable CCXT enrichment (default: True)
                - enable_metadata_caching: Enable metadata caching (default: True)
        """
        self.config = config
        project_id = config.get("project_id", "central-element-323112")

        # Initialize processing service
        processing_config = {
            "project_id": project_id,
            "enable_ccxt_integration": config.get("enable_ccxt_integration", True),
            "enable_metadata_caching": config.get("enable_metadata_caching", True),
        }
        self.processing_service = InstrumentProcessingService(processing_config)

        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()

        # Initialize batch processor
        batch_config = {
            "max_batch_size": config.get("max_batch_size", 1000),
            "lookback_days": config.get("lookback_days", 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)

        # Venue mapping
        self.venue_mapping = VenueMapping()

        logger.info("✅ InstrumentsService initialized")

    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate instruments for a specific date.

        Args:
            date: Target date for instrument generation
            exchanges: Optional list of exchanges to process (for CeFi mode)
            force: Force regeneration even if instruments exist
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (for DeFi mode)

        Returns:
            Dictionary with generation results
        """
        date_str = date.strftime("%Y-%m-%d")
        logger.info(
            f"📅 Generating instruments for {date_str} (CeFi={cefi}, TradFi={tradfi}, DeFi={defi})"
        )

        all_instruments = {}

        # Process CEFI (Tardis) exchanges
        if cefi:
            # Use specified exchanges or all Tardis exchanges
            if exchanges is None:
                exchanges = self.venue_mapping.all_tardis_exchanges

            for exchange in exchanges:
                try:
                    logger.info(f"🔍 Processing CeFi exchange {exchange}...")
                    exchange_instruments = (
                        await self.processing_service.process_exchange_instruments(
                            exchange=exchange, target_date=date, force=force
                        )
                    )
                    if exchange_instruments:
                        all_instruments.update(exchange_instruments)
                        logger.info(
                            f"✅ Processed {len(exchange_instruments)} instruments from {exchange}"
                        )
                except Exception as e:
                    logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)

        # Process TRADFI (Databento) exchanges
        if tradfi:
            try:
                from ...config import DatabentoInstrumentConfig

                databento_config = DatabentoInstrumentConfig()

                # Common TradFi exchanges via Databento
                databento_exchanges = ["CME", "NASDAQ", "NYSE", "ICE", "CBOE"]

                # Track DBEQ.BASIC symbols to avoid duplicate fetches
                dbeq_symbols_fetched = False

                for exchange in databento_exchanges:
                    try:
                        # For NASDAQ/NYSE, fetch all DBEQ.BASIC symbols once
                        if exchange in ["NASDAQ", "NYSE"]:
                            if dbeq_symbols_fetched:
                                logger.info(
                                    f"⏭️ Skipping {exchange} (already fetched DBEQ.BASIC symbols via NASDAQ)"
                                )
                                continue

                            # Get all equities from DBEQ.BASIC dataset
                            symbols = databento_config._unified.get_symbols_for_dataset(
                                "DBEQ.BASIC"
                            )
                            dbeq_symbols_fetched = True
                            logger.info(
                                f"📋 Fetching all DBEQ.BASIC symbols ({len(symbols)} symbols) for {exchange}"
                            )
                        elif exchange == "CBOE":
                            # Get options symbols for CBOE
                            symbols = databento_config._unified.get_symbols_for_dataset(
                                "OPRA.PILLAR"
                            )
                            logger.info(
                                f"📋 Fetching CBOE options symbols ({len(symbols)} symbols)"
                            )
                        else:
                            # Get symbols for this exchange from config (exchange names are venues)
                            symbols = databento_config.get_symbols_for_venue(exchange)

                        if not symbols:
                            logger.warning(
                                f"⚠️ No symbols configured for {exchange}, skipping"
                            )
                            continue

                        # Fetch Databento instruments
                        databento_instruments = (
                            self.processing_service.fetch_databento_instruments(
                                exchange=exchange,
                                symbols=symbols,
                                target_date=date,
                            )
                        )
                        if databento_instruments:
                            all_instruments.update(databento_instruments)
                            logger.info(
                                f"✅ Processed {len(databento_instruments)} instruments from {exchange}"
                            )
                    except Exception as e:
                        logger.error(
                            f"❌ Failed to process Databento exchange {exchange}: {e}",
                            exc_info=True,
                        )
            except Exception as e:
                logger.error(
                    f"❌ Failed to initialize Databento processing: {e}", exc_info=True
                )

        # Process DEFI protocols
        if defi:
            try:
                # Filter protocols by venue if specified
                venues_filter = venues or []
                if isinstance(venues_filter, str):
                    venues_filter = [venues_filter]

                # Map venues to protocols
                venue_to_protocol = {
                    "HYPERLIQUID": ("hyperliquid", None),
                    "ASTER": ("aster", None),
                }

                # Common DeFi protocols
                all_defi_protocols = [
                    ("uniswap_v2", "ETHEREUM"),
                    ("uniswap_v3", "ETHEREUM"),
                    ("uniswap_v4", "ETHEREUM"),
                    ("curve", "ETHEREUM"),
                    ("balancer", "ETHEREUM"),
                    ("aave_v3", "ETHEREUM"),
                    ("etherfi", "ETHEREUM"),
                    ("lido", "ETHEREUM"),
                    ("morpho", "ETHEREUM"),
                    ("euler_plasma", None),
                    ("fluid_plasma", None),
                    ("aave_plasma", None),
                    ("hyperliquid", None),
                    ("aster", None),
                    ("ethena", "ETHEREUM"),
                ]

                # Filter protocols if venues specified
                if venues_filter:
                    defi_protocols = []
                    for venue in venues_filter:
                        if venue.upper() in venue_to_protocol:
                            protocol, chain = venue_to_protocol[venue.upper()]
                            defi_protocols.append((protocol, chain))
                    if not defi_protocols:
                        logger.warning(
                            f"⚠️ No matching protocols found for venues: {venues_filter}"
                        )
                        defi_protocols = all_defi_protocols
                else:
                    defi_protocols = all_defi_protocols

                for protocol, chain in defi_protocols:
                    try:
                        # Fetch DeFi instruments
                        if chain:
                            defi_instruments = (
                                self.processing_service.fetch_defi_instruments(
                                    protocol=protocol,
                                    chain=chain,
                                    target_date=date,
                                )
                            )
                        else:
                            defi_instruments = (
                                self.processing_service.fetch_defi_instruments(
                                    protocol=protocol,
                                    target_date=date,
                                )
                            )
                        if defi_instruments:
                            all_instruments.update(defi_instruments)
                            logger.info(
                                f"✅ Processed {len(defi_instruments)} instruments from {protocol}"
                            )
                    except Exception as e:
                        logger.error(
                            f"❌ Failed to process {protocol}: {e}", exc_info=True
                        )
            except Exception as e:
                logger.error(
                    f"❌ Failed to initialize DeFi processing: {e}", exc_info=True
                )

        # If no mode flags specified, process all three modes
        if not cefi and not tradfi and not defi:
            logger.info(
                "📋 No mode flags specified, processing all modes (CeFi, TradFi, DeFi)"
            )
            return await self.generate_instruments_for_date(
                date=date,
                exchanges=exchanges,
                force=force,
                cefi=True,
                tradfi=True,
                defi=True,
                venues=venues,
            )

        if not all_instruments:
            logger.warning(f"⚠️ No instruments generated for {date_str}")
            return {
                "status": "warning",
                "date": date_str,
                "instruments_generated": 0,
                "message": "No instruments generated",
            }

        # Convert to DataFrame
        instruments_list = []
        for inst_key, inst_obj in all_instruments.items():
            if hasattr(inst_obj, "model_dump"):
                instruments_list.append(inst_obj.model_dump())
            else:
                instruments_list.append(inst_obj)

        instruments_df = pd.DataFrame(instruments_list)

        # Store to cloud
        logger.info(f"📤 Storing {len(instruments_df)} instruments to cloud...")
        success = self.cloud_storage.store_instruments(
            instruments_df=instruments_df, table_name="instruments", date=date
        )

        if success:
            logger.info(f"✅ Successfully stored instruments for {date_str}")
            return {
                "status": "success",
                "date": date_str,
                "instruments_generated": len(instruments_df),
                "exchanges_processed": len(exchanges) if exchanges else 0,
                "venues": (
                    instruments_df["venue"].unique().tolist()
                    if "venue" in instruments_df.columns
                    else []
                ),
            }
        else:
            logger.error(f"❌ Failed to store instruments for {date_str}")
            return {
                "status": "error",
                "date": date_str,
                "instruments_generated": len(instruments_df),
                "message": "Storage failed",
            }

    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False,
        cefi: bool = False,
        tradfi: bool = False,
        defi: bool = False,
        venues: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate instruments for a date range.

        Args:
            start_date: Start date
            end_date: End date
            exchanges: Optional list of exchanges to process
            force: Force regeneration
            cefi: Process CeFi (Tardis) exchanges
            tradfi: Process TradFi (Databento) exchanges
            defi: Process DeFi protocols
            venues: Optional venue filter (for DeFi mode)

        Returns:
            Dictionary with batch processing results
        """
        # Get date range from batch processor
        date_range = self.batch_processor.get_required_periods(start_date)

        # Filter to requested range
        date_range = [d for d in date_range if start_date <= d <= end_date]

        logger.info(
            f"📅 Processing {len(date_range)} dates from {start_date.date()} to {end_date.date()}"
        )

        results = []
        total_generated = 0
        total_errors = 0

        for date in date_range:
            try:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force,
                    cefi=cefi,
                    tradfi=tradfi,
                    defi=defi,
                    venues=venues,
                )
                results.append(result)
                if result.get("status") == "success":
                    total_generated += result.get("instruments_generated", 0)
                else:
                    total_errors += 1
            except Exception as e:
                logger.error(
                    f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}",
                    exc_info=True,
                )
                total_errors += 1
                results.append(
                    {
                        "status": "error",
                        "date": date.strftime("%Y-%m-%d"),
                        "error": str(e),
                    }
                )

        success_count = len([r for r in results if r.get("status") == "success"])
        success_rate = (success_count / len(date_range) * 100) if date_range else 0

        logger.info(
            f"📊 Batch processing complete: {success_count}/{len(date_range)} successful ({success_rate:.1f}%)"
        )
        logger.info(f"   Total instruments generated: {total_generated}")
        logger.info(f"   Errors: {total_errors}")

        return {
            "status": "success" if total_errors == 0 else "partial",
            "dates_processed": len(date_range),
            "dates_successful": success_count,
            "dates_failed": total_errors,
            "success_rate_percent": success_rate,
            "total_instruments_generated": total_generated,
            "results": results,
        }

    def query_instruments(
        self, venue: Optional[str] = None, instrument_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query stored instruments from BigQuery.

        Args:
            venue: Optional venue filter
            instrument_type: Optional instrument type filter

        Returns:
            DataFrame with instruments
        """
        return self.cloud_storage.query_instruments(
            venue=venue, instrument_type=instrument_type
        )

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            "processing_service": self.processing_service.get_processing_stats(),
            "batch_processor": {
                "max_batch_size": self.batch_processor.max_batch_size,
                "lookback_days": self.batch_processor.lookback_days,
            },
        }

    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, "processing_service"):
            self.processing_service.cleanup()
        logger.info("🧹 InstrumentsService cleanup completed")
