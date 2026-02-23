"""
DeFi Instrument Processor

Handles DeFi-specific instrument processing:
- Subgraph integration (Uniswap, SushiSwap, etc.)
- On-chain instrument discovery
- DEX-specific logic
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, cast

from instruments_service.engine.operations.instruments.processors.base_processor import BaseInstrumentProcessor
from instruments_service.engine.processors.defi_processor import DefiServiceProtocol
from instruments_service.engine.processors.defi_processor import fetch_defi_instruments as _fetch_defi_instruments
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class DeFiInstrumentProcessor(BaseInstrumentProcessor):
    """
    DeFi-specific instrument processor.

    Handles:
    - Subgraph integration
    - On-chain instrument discovery
    - DEX-specific logic
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize DeFi processor.

        Args:
            config: Configuration with DeFi processing settings
        """
        super().__init__(config)

        logger.info("✅ DeFiInstrumentProcessor initialized")

    def fetch_defi_instruments(
        self,
        protocol: str,
        chain: str = "ETHEREUM",
        target_date: datetime | None = None,
        **kwargs: Any,  # type: ignore[reportAny]
    ) -> dict[str, InstrumentDefinition]:
        """
        Fetch DeFi instruments from various protocols.

        Args:
            protocol: Protocol name (e.g., 'uniswap-v3', 'sushiswap')
            chain: Blockchain name (default: 'ETHEREUM')
            target_date: Target date for instrument definitions
            **kwargs: Additional protocol-specific parameters

        Returns:
            Dictionary mapping instrument_key to InstrumentDefinition
        """
        return _fetch_defi_instruments(
            cast(DefiServiceProtocol, self),
            protocol,
            chain,
            target_date,
            **kwargs,  # type: ignore[reportAny]
        )

    async def process_defi_instruments(
        self,
        protocol: str,
        chain: str = "ETHEREUM",
        target_date: datetime | None = None,
        **kwargs: Any,  # type: ignore[reportAny]
    ) -> dict[str, InstrumentDefinition]:
        """
        Process DeFi instruments for a protocol.

        Args:
            protocol: Protocol name (e.g., 'uniswap-v3', 'sushiswap')
            chain: Blockchain name (default: 'ETHEREUM')
            target_date: Target date for processing
            **kwargs: Additional protocol-specific parameters

        Returns:
            Dictionary of processed instrument metadata keyed by canonical key
        """
        instruments = self.fetch_defi_instruments(protocol, chain, target_date, **kwargs)

        for inst_key, inst_def in instruments.items():
            self.cache_metadata(inst_key, inst_def)

        logger.info(f"📊 Processed {len(instruments)} DeFi instruments from {protocol} on {chain}")
        return instruments

    def cleanup(self):
        """Cleanup resources and close connections"""
        super().cleanup()

        logger.info("🧹 DeFiInstrumentProcessor cleanup completed")
