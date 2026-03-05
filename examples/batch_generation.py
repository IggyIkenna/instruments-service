#!/usr/bin/env python3
"""
Example: Batch Instrument Generation

Demonstrates how to run instruments-service in batch mode to generate instruments
for a date range.

This example shows:
- Batch processing for multiple dates
- Configuration setup
- Error handling
- Progress tracking

Note: For production batch processing, use the CLI instead:
    python -m instruments_service --mode instruments \
        --start-date 2023-05-23 --end-date 2023-05-24 \
        --CEFI --force
"""

import asyncio
import logging
from datetime import datetime, timedelta
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext
from unified_market_interface import VenueMapping

from instruments_service import (
    CloudInstrumentStorage,
    InstrumentBatchProcessor,
    InstrumentProcessingService,
)
from instruments_service.config import get_config

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def generate_instruments_batch(start_date: str, end_date: str, force: bool = False):
    """
    Generate instruments for a date range.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        force: Force regeneration even if instruments exist
    """
    # Parse dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Configuration for InstrumentProcessingService (uses config class)
    instruments_config = get_config()
    config: dict[str, object] = {
        "project_id": instruments_config.gcp_project_id,
        "max_batch_size": 1000,
        "lookback_days": 0,
    }

    # Initialize services
    logger.info("🚀 Initializing instruments-service...")
    processing_service = InstrumentProcessingService(config)
    storage_service = CloudInstrumentStorage()  # Uses default config from env
    InstrumentBatchProcessor(config)

    # Always process all exchanges (exchange filtering was removed from CLI)
    venue_mapping = VenueMapping()
    exchanges = venue_mapping.all_tardis_exchanges

    logger.info("📅 Processing date range: %s to %s", start_date, end_date)
    logger.info("🏦 Processing exchanges: %s", exchanges)

    # Process each date
    current_date = start_dt
    total_instruments = 0
    total_dates = 0
    errors = []

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        logger.info("\n%s", "=" * 60)
        logger.info("📅 Processing date: %s", date_str)
        logger.info("%s", "=" * 60)

        try:
            # Generate instruments for all exchanges
            all_instruments = {}
            for exchange in exchanges:
                try:
                    logger.info("  🔍 Processing %s...", exchange)
                    exchange_instruments = await processing_service.process_exchange_instruments(
                        exchange=exchange, target_date=current_date, force=force
                    )
                    if exchange_instruments:
                        all_instruments.update(exchange_instruments)
                        logger.info("  ✅ %s: %s instruments", exchange, len(exchange_instruments))
                except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.MEDIUM,
                        recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                    logger.error("  ❌ Failed to process %s: %s", exchange, e)
                    errors.append(f"{date_str}/{exchange}: {e}")
            if all_instruments:
                # Convert to DataFrame
                import pandas as pd

                instruments_list = []
                for _inst_key, inst_obj in all_instruments.items():
                    if hasattr(inst_obj, "model_dump"):
                        instruments_list.append(inst_obj.model_dump())
                    else:
                        instruments_list.append(inst_obj)

                instruments_df = pd.DataFrame(instruments_list)

                # Store to cloud
                logger.info("  📤 Uploading %s instruments...", len(instruments_df))
                success = storage_service.store_instruments(
                    instruments_df=instruments_df,
                    table_name="instruments",
                    date=current_date,
                )

                if success:
                    logger.info("  ✅ Successfully stored instruments for %s", date_str)
                    total_instruments += len(instruments_df)
                    total_dates += 1
                else:
                    logger.error("  ❌ Failed to store instruments for %s", date_str)
                    errors.append(f"{date_str}: Storage failed")
            else:
                logger.warning("  ⚠️ No instruments generated for %s", date_str)
                errors.append(f"{date_str}: No instruments")

        except (OSError, ValueError, RuntimeError, ConnectionError, TimeoutError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.exception("  ❌ Error processing %s: %s", date_str, e)
            errors.append(f"{date_str}: {e}")
        # Move to next date
        current_date += timedelta(days=1)

    # Summary
    logger.info("\n%s", "=" * 60)
    logger.info("📊 Batch Processing Summary")
    logger.info("%s", "=" * 60)
    logger.info("  Dates processed: %s", total_dates)
    logger.info("  Total instruments: %s", total_instruments)
    logger.info("  Errors: %s", len(errors))
    if errors:
        logger.warning("  Error details:")
        for error in errors[:10]:  # Show first 10 errors
            logger.warning("    - %s", error)

    return {
        "dates_processed": total_dates,
        "total_instruments": total_instruments,
        "errors": errors,
    }


def main():
    """Run batch generation example."""
    import argparse

    parser = argparse.ArgumentParser(description="Batch instrument generation")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Force regeneration")

    args = parser.parse_args()

    # Run async function (always processes all exchanges)
    result = asyncio.run(
        generate_instruments_batch(
            start_date=args.start_date,
            end_date=args.end_date,
            force=args.force,
        )
    )

    logger.info("\n✅ Batch generation complete!")
    return result


if __name__ == "__main__":
    main()
