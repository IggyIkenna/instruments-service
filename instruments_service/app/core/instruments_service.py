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
        project_id = config.get('project_id', 'central-element-323112')
        
        # Initialize processing service
        processing_config = {
            'project_id': project_id,
            'enable_ccxt_integration': config.get('enable_ccxt_integration', True),
            'enable_metadata_caching': config.get('enable_metadata_caching', True),
        }
        self.processing_service = InstrumentProcessingService(processing_config)
        
        # Initialize cloud storage
        self.cloud_storage = CloudInstrumentStorage()
        
        # Initialize batch processor
        batch_config = {
            'max_batch_size': config.get('max_batch_size', 1000),
            'lookback_days': config.get('lookback_days', 0),
        }
        self.batch_processor = InstrumentBatchProcessor(batch_config)
        
        # Venue mapping
        self.venue_mapping = VenueMapping()
        
        logger.info("✅ InstrumentsService initialized")
    
    async def generate_instruments_for_date(
        self,
        date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Generate instruments for a specific date.
        
        Args:
            date: Target date for instrument generation
            exchanges: Optional list of exchanges to process (default: all)
            force: Force regeneration even if instruments exist
            
        Returns:
            Dictionary with generation results
        """
        date_str = date.strftime('%Y-%m-%d')
        logger.info(f"📅 Generating instruments for {date_str}")
        
        # Use specified exchanges or all
        if exchanges is None:
            exchanges = self.venue_mapping.all_tardis_exchanges
        
        # Generate instruments for all exchanges
        all_instruments = {}
        for exchange in exchanges:
            try:
                logger.info(f"🔍 Processing {exchange}...")
                exchange_instruments = await self.processing_service.process_exchange_instruments(
                    exchange=exchange,
                    target_date=date,
                    force=force
                )
                if exchange_instruments:
                    all_instruments.update(exchange_instruments)
                    logger.info(f"✅ Processed {len(exchange_instruments)} instruments from {exchange}")
            except Exception as e:
                logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
        
        if not all_instruments:
            logger.warning(f"⚠️ No instruments generated for {date_str}")
            return {
                'status': 'warning',
                'date': date_str,
                'instruments_generated': 0,
                'message': 'No instruments generated'
            }
        
        # Convert to DataFrame
        instruments_list = []
        for inst_key, inst_obj in all_instruments.items():
            if hasattr(inst_obj, 'model_dump'):
                instruments_list.append(inst_obj.model_dump())
            else:
                instruments_list.append(inst_obj)
        
        instruments_df = pd.DataFrame(instruments_list)
        
        # Store to cloud
        logger.info(f"📤 Storing {len(instruments_df)} instruments to cloud...")
        success = self.cloud_storage.store_instruments(
            instruments_df=instruments_df,
            table_name="instruments",
            date=date
        )
        
        if success:
            logger.info(f"✅ Successfully stored instruments for {date_str}")
            return {
                'status': 'success',
                'date': date_str,
                'instruments_generated': len(instruments_df),
                'exchanges_processed': len(exchanges),
                'venues': instruments_df['venue'].unique().tolist() if 'venue' in instruments_df.columns else []
            }
        else:
            logger.error(f"❌ Failed to store instruments for {date_str}")
            return {
                'status': 'error',
                'date': date_str,
                'instruments_generated': len(instruments_df),
                'message': 'Storage failed'
            }
    
    async def generate_instruments_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        exchanges: Optional[List[str]] = None,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Generate instruments for a date range.
        
        Args:
            start_date: Start date
            end_date: End date
            exchanges: Optional list of exchanges to process
            force: Force regeneration
            
        Returns:
            Dictionary with batch processing results
        """
        # Get date range from batch processor
        date_range = self.batch_processor.get_required_periods(start_date)
        
        # Filter to requested range
        date_range = [d for d in date_range if start_date <= d <= end_date]
        
        logger.info(f"📅 Processing {len(date_range)} dates from {start_date.date()} to {end_date.date()}")
        
        results = []
        total_generated = 0
        total_errors = 0
        
        for date in date_range:
            try:
                result = await self.generate_instruments_for_date(
                    date=date,
                    exchanges=exchanges,
                    force=force
                )
                results.append(result)
                if result.get('status') == 'success':
                    total_generated += result.get('instruments_generated', 0)
                else:
                    total_errors += 1
            except Exception as e:
                logger.error(f"❌ Failed to process {date.strftime('%Y-%m-%d')}: {e}", exc_info=True)
                total_errors += 1
                results.append({
                    'status': 'error',
                    'date': date.strftime('%Y-%m-%d'),
                    'error': str(e)
                })
        
        success_count = len([r for r in results if r.get('status') == 'success'])
        success_rate = (success_count / len(date_range) * 100) if date_range else 0
        
        logger.info(f"📊 Batch processing complete: {success_count}/{len(date_range)} successful ({success_rate:.1f}%)")
        logger.info(f"   Total instruments generated: {total_generated}")
        logger.info(f"   Errors: {total_errors}")
        
        return {
            'status': 'success' if total_errors == 0 else 'partial',
            'dates_processed': len(date_range),
            'dates_successful': success_count,
            'dates_failed': total_errors,
            'success_rate_percent': success_rate,
            'total_instruments_generated': total_generated,
            'results': results
        }
    
    def query_instruments(
        self,
        venue: Optional[str] = None,
        instrument_type: Optional[str] = None
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
            venue=venue,
            instrument_type=instrument_type
        )
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            'processing_service': self.processing_service.get_processing_stats(),
            'batch_processor': {
                'max_batch_size': self.batch_processor.max_batch_size,
                'lookback_days': self.batch_processor.lookback_days,
            }
        }
    
    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, 'processing_service'):
            self.processing_service.cleanup()
        logger.info("🧹 InstrumentsService cleanup completed")



