"""
DeFi Orchestration Module

Handles processing of DeFi protocols across different chains.
"""

import logging
from datetime import datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from unified_api_contracts import VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

from instruments_service.config import DEFI_PROTOCOLS, DEFI_VENUE_TO_PROTOCOL
from instruments_service.models import InstrumentDefinition

if TYPE_CHECKING:
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

logger = logging.getLogger(__name__)


class DeFiOrchestrator:
    """Orchestrates DeFi instrument processing."""

    def __init__(self, processing_service: "InstrumentProcessingService") -> None:
        """Initialize DeFi orchestrator."""
        self.processing_service = processing_service
        self.venue_mapping: VenueMapping = VenueMapping()

    async def process_defi(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """Process DeFi protocols."""
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            # Determine which protocols to process
            defi_protocols = self._determine_defi_protocols(venues_filter)

            if not defi_protocols:
                logger.info("⏭️ Skipping DEFI processing - no protocols to process")
                return all_instruments

            # Process protocols sequentially (they may have rate limits)
            for protocol, chain in defi_protocols:
                try:
                    defi_instruments = await self._process_defi_protocol(protocol, chain, date)
                    if defi_instruments:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], defi_instruments))
                        logger.info("✅ Processed %s instruments from %s", len(defi_instruments), protocol)
                except (ValueError, KeyError, TypeError, IndexError) as e:
                    _err = EnhancedError(
                        message=str(e),
                        category=ErrorCategory.SERVER_ERROR,
                        severity=ErrorSeverity.HIGH,
                        recovery_strategy=ErrorRecoveryStrategy.RETRY,
                        correlation_id=str(uuid4()),
                        context=ErrorContext(extra={"exc_type": type(e).__name__}),
                    )
                    logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
                    raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e
        except (OSError, ValueError, RuntimeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.HIGH,
                recovery_strategy=ErrorRecoveryStrategy.RETRY,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.error(_err.message, extra={"correlation_id": _err.correlation_id})
            raise RuntimeError(f"[{_err.correlation_id}] {_err.message}") from e
        return all_instruments

    def _determine_defi_protocols(self, venues_filter: list[str]) -> list[tuple[str, str | None]]:
        """Determine which DeFi protocols to process based on venue filter."""
        if venues_filter:
            defi_venues: list[str] = [
                v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_defi_venues)
            ]

            if defi_venues:
                defi_protocols: list[tuple[str, str | None]] = []
                for venue in defi_venues:
                    venue_key: str = venue.upper() if venue else ""
                    if venue_key in DEFI_VENUE_TO_PROTOCOL:
                        protocol, chain = DEFI_VENUE_TO_PROTOCOL[venue_key]
                        if (protocol, chain) not in defi_protocols:
                            defi_protocols.append((protocol, chain))

                if defi_protocols:
                    defi_protocol_names: list[str] = [p[0] for p in defi_protocols]
                    logger.info("🔍 Filtered DEFI protocols by venues %s: %s", defi_venues, defi_protocol_names)
                    return defi_protocols
                else:
                    logger.warning("⚠️ No matching DEFI protocols found for venues: %s", defi_venues)
                    return []
            else:
                logger.info("🔍 No DEFI venues in filter, skipping DEFI processing")
                return []
        else:
            logger.info("🔍 No venue filter specified, processing all DEFI protocols")
            return DEFI_PROTOCOLS

    async def _process_defi_protocol(
        self, protocol: str, chain: str | None, date: datetime
    ) -> dict[str, InstrumentDefinition]:
        """Process a single DeFi protocol."""
        if chain:
            return self.processing_service.fetch_defi_instruments(
                protocol=protocol,
                chain=chain,
                target_date=date,
            )
        else:
            return self.processing_service.fetch_defi_instruments(
                protocol=protocol,
                target_date=date,
            )
