"""
Databento instrument fetching.

Extracted from instrument_processing_service per file-splitting-guide §4.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


async def fetch_databento_instruments(
    exchange: str,
    symbols: List[str],
    target_date: Optional[datetime] = None,
    databento_adapter_factory=None,
) -> Dict[str, InstrumentDefinition]:
    """
    Fetch TradFi instruments from Databento.

    Args:
        exchange: Exchange name (e.g., 'CME', 'NASDAQ')
        symbols: List of symbols to fetch
        target_date: Target date for instrument definitions
        databento_adapter_factory: Callable that returns DatabentoAdapter (for DI)

    Returns:
        Dictionary mapping instrument_key to InstrumentDefinition
    """
    if databento_adapter_factory is None:
        from instruments_service.app.venues.databento import DatabentoAdapter

        def _default_factory():
            return DatabentoAdapter()

        databento_adapter_factory = _default_factory

    try:
        adapter = databento_adapter_factory()
        date = target_date or datetime.now(timezone.utc)

        raw_instruments = await asyncio.to_thread(
            adapter.fetch_instrument_definitions, exchange=exchange, symbols=symbols, date=date
        )

        instruments = {}
        for inst_key, inst_data in raw_instruments.items():
            try:
                inst_def = InstrumentDefinition(**inst_data)
                instruments[inst_key] = inst_def
            except Exception as e:
                logger.warning(f"Failed to create InstrumentDefinition for {inst_key}: {e}")
                continue

        logger.info(f"✅ Fetched {len(instruments)} Databento instruments for {exchange}")
        return instruments

    except ImportError:
        logger.error("Databento adapter not available. Install: pip install databento")
        return {}
    except Exception as e:
        logger.error(f"Failed to fetch Databento instruments: {e}")
        return {}
