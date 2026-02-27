"""
TradFi Instrument Processor

Handles TradFi-specific instrument processing:
- Databento API integration
- Traditional finance exchanges (CME, NASDAQ, etc.)
- Futures, options, and equity instruments
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext
from unified_market_interface import DatabentoAdapter

from instruments_service.engine.operations.instruments.processors.base_processor import BaseInstrumentProcessor
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class TradFiInstrumentProcessor(BaseInstrumentProcessor):
    """
    TradFi-specific instrument processor.

    Handles:
    - Databento API integration
    - Traditional finance exchanges
    - Futures, options, and equity instruments
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize TradFi processor.

        Args:
            config: Configuration with TradFi processing settings
        """
        super().__init__(config)

        logger.info("✅ TradFiInstrumentProcessor initialized")

    async def fetch_databento_instruments(
        self, exchange: str, symbols: list[str], target_date: datetime | None = None
    ) -> dict[str, InstrumentDefinition]:
        """
        Fetch TradFi instruments from Databento.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            symbols: List of symbols to fetch
            target_date: Target date for instrument definitions

        Returns:
            Dictionary mapping instrument_key to InstrumentDefinition

        Raises:
            Exception: If API fetch fails after retries
        """
        target_date_resolved = target_date or datetime.now(UTC)

        # Manual retry logic for type safety
        max_retries: int = self.processing_config.retry_max_attempts
        last_error: Exception | None = None

        for attempt in range(max_retries):
            try:
                adapter = DatabentoAdapter()

                raw_instruments = await asyncio.to_thread(
                    adapter.fetch_instrument_definitions,
                    exchange=exchange,
                    symbols=symbols,
                    date=target_date_resolved,
                )

                instruments: dict[str, InstrumentDefinition] = {}
                for inst_key, inst_data in raw_instruments.items():
                    try:
                        inst_def = InstrumentDefinition(**inst_data)  # type: ignore[reportAny]
                        instruments[inst_key] = inst_def
                    except Exception as e:
                        _err = EnhancedError(
                            message=str(e),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(e).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        logger.warning(f"Failed to create InstrumentDefinition for {inst_key}: {e}")
                        continue
                logger.info(f"✅ Fetched {len(instruments)} Databento instruments for {exchange}")
                return instruments

            except ImportError as e:
                logger.error("Databento adapter not available. Install: pip install databento")
                raise Exception("Databento adapter not available") from e
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    backoff_multiplier: int = cast(int, 2**attempt)
                    backoff: float = self.processing_config.retry_backoff_factor * float(backoff_multiplier)
                    logger.warning(f"⚠️ Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                else:
                    logger.error(f"❌ Failed to fetch Databento instruments after {max_retries} attempts: {e}")
                    raise Exception(f"Failed to fetch Databento instruments for {exchange}") from last_error
        raise Exception("Unexpected: retry loop completed without return or raise")

    async def process_tradfi_instruments(
        self,
        exchange: str,
        symbols: list[str],
        target_date: datetime | None = None,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process TradFi instruments for an exchange.

        Args:
            exchange: Exchange name (e.g., 'CME', 'NASDAQ')
            symbols: List of symbols to process
            target_date: Target date for processing

        Returns:
            Dictionary of processed instrument metadata keyed by canonical key
        """
        instruments = await self.fetch_databento_instruments(exchange, symbols, target_date)

        for inst_key, inst_def in instruments.items():
            self.cache_metadata(inst_key, inst_def)

        logger.info(f"📊 Processed {len(instruments)} TradFi instruments from {exchange}")
        return instruments

    def cleanup(self):
        """Cleanup resources and close connections"""
        super().cleanup()

        logger.info("🧹 TradFiInstrumentProcessor cleanup completed")
