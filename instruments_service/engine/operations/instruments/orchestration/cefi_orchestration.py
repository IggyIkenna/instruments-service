"""
CeFi Orchestration Module

Handles processing of CeFi (Tardis) exchanges and on-chain CLOB venues.
"""

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from unified_api_contracts import VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import TardisAdapter, get_adapter

from instruments_service.models import InstrumentDefinition

if TYPE_CHECKING:
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

logger = logging.getLogger(__name__)


class CeFiOrchestrator:
    """Orchestrates CeFi instrument processing."""

    def __init__(self, processing_service: "InstrumentProcessingService") -> None:
        """Initialize CeFi orchestrator."""
        self.processing_service = processing_service
        self.venue_mapping: VenueMapping = VenueMapping()

    async def process_cefi(
        self, date: datetime, exchanges: list[str] | None, force: bool, venues_filter: list[str]
    ) -> dict[str, InstrumentDefinition]:
        """Process CeFi (Tardis) exchanges."""
        cefi_exchanges: list[str] = (
            cast(list[str], exchanges)
            if exchanges is not None
            else cast(list[str], self.venue_mapping.all_tardis_exchanges)
        )

        # Apply venue filtering for CEFI
        cefi_exchanges = self._filter_cefi_venues(cefi_exchanges, venues_filter)

        if cefi_exchanges:
            # Pre-flight check: Validate venue access
            cefi_exchanges = await self._check_venue_access(cefi_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        if not cefi_exchanges:
            logger.info("⏭️ Skipping CEFI processing - no exchanges to process")
        else:
            # Process Tardis exchanges
            tardis_instruments = await self._process_tardis_exchanges(cefi_exchanges, date, force)
            all_instruments.update(tardis_instruments)

        # Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI
        clob_instruments = await self._process_onchain_clob_venues(venues_filter, date)
        all_instruments.update(clob_instruments)

        return all_instruments

    def _filter_cefi_venues(self, cefi_exchanges: list[str], venues_filter: list[str]) -> list[str]:
        """Filter CeFi exchanges based on venue filter."""
        if not venues_filter:
            logger.debug("🔍 No venue filter specified, processing exchanges: %s", cefi_exchanges)
            return cefi_exchanges

        tardis_venue_values: set[str] = set(cast(dict[str, str], self.venue_mapping.tardis_to_venue).values())
        cefi_venues: list[str] = [v for v in venues_filter if v in tardis_venue_values]

        if not cefi_venues:
            logger.info("🔍 No CEFI venues in filter, skipping CEFI processing")
            return []

        venue_to_exchanges: dict[str, list[str]] = cast(
            dict[str, list[str]],
            self.venue_mapping.get_venue_to_tardis_exchanges(),
        )

        filtered_exchanges: list[str] = []
        for canonical_venue in cefi_venues:
            if canonical_venue in venue_to_exchanges:
                raw_exchanges_for_venue: list[str] = venue_to_exchanges[canonical_venue]
                for raw_exchange in raw_exchanges_for_venue:
                    if raw_exchange in cefi_exchanges and raw_exchange not in filtered_exchanges:
                        filtered_exchanges.append(raw_exchange)
                        logger.debug("  Mapped %s -> raw exchange %s", canonical_venue, raw_exchange)

        if filtered_exchanges:
            logger.info("🔍 Filtered CEFI exchanges by canonical venues %s: %s", cefi_venues, filtered_exchanges)
            return filtered_exchanges
        else:
            logger.warning("⚠️ No matching CEFI exchanges found for canonical venues: %s", cefi_venues)
            return []

    async def _process_tardis_exchanges(
        self, cefi_exchanges: list[str], date: datetime, force: bool
    ) -> dict[str, InstrumentDefinition]:
        """Process Tardis exchanges in parallel."""
        logger.info("🚀 Processing %s CeFi exchanges in parallel...", len(cefi_exchanges))

        api_key = getattr(self.processing_service, "api_key", None)
        project_id = getattr(self.processing_service, "_tardis_project_id", None)

        async def process_single_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
            try:
                logger.info("🔍 Processing CeFi exchange %s...", exchange)
                raw_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
                adapter = cast(TardisAdapter, raw_adapter)
                instruments_raw: list[dict[str, object]] = cast(
                    list[dict[str, object]],
                    await adapter.fetch_instruments(  # pyright: ignore[reportUnknownMemberType]
                        exchange=exchange,
                        target_date=date,
                        force_refresh=force,
                        normalize=True,
                    ),
                )
                result: dict[str, InstrumentDefinition] = {}
                for d in instruments_raw:
                    try:
                        inst = InstrumentDefinition.model_validate(d)
                        key = cast(str, d.get("instrument_key") or inst.instrument_key)
                        result[key] = inst
                    except (ValueError, KeyError, TypeError, IndexError) as e:
                        _err = EnhancedError(
                            message=str(e),
                            category=ErrorCategory.SERVER_ERROR,
                            severity=ErrorSeverity.MEDIUM,
                            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                            correlation_id=str(uuid4()),
                            context=ErrorContext(extra={"exc_type": type(e).__name__}),
                        )
                        logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                        logger.warning(
                            "Failed to create InstrumentDefinition for %s: %s", d.get("instrument_key", "unknown"), e
                        )
                if result:
                    logger.info("✅ Processed %s instruments from %s", len(result), exchange)
                return result
            except (ValueError, KeyError, TypeError, IndexError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                logger.exception("Failed to process %s: %s", exchange, e)
                return {}

        results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
            *[process_single_exchange(ex) for ex in cefi_exchanges],
            return_exceptions=False,
        )

        all_instruments: dict[str, InstrumentDefinition] = {}
        for result in results:
            if result:
                all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        return all_instruments

    async def _process_onchain_clob_venues(
        self, venues_filter: list[str], date: datetime
    ) -> dict[str, InstrumentDefinition]:
        """Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI."""
        cefi_onchain_clob_venues: list[str] = cast(list[str], self.venue_mapping.all_cefi_onchain_clob_venues)
        cefi_clob_protocols: list[tuple[str, None]] = []

        if venues_filter:
            for venue in venues_filter:
                if venue.upper() in cefi_onchain_clob_venues:
                    protocol_name: str = venue.lower()
                    cefi_clob_protocols.append((protocol_name, None))
        else:
            for venue in cefi_onchain_clob_venues:
                cefi_clob_protocols.append((venue.lower(), None))

        all_instruments: dict[str, InstrumentDefinition] = {}

        if cefi_clob_protocols:
            protocol_names: list[str] = [p[0] for p in cefi_clob_protocols]
            logger.info("🚀 Processing %s on-chain CLOB venues (CEFI): %s", len(cefi_clob_protocols), protocol_names)

            for protocol, _ in cefi_clob_protocols:
                try:
                    clob_instruments = self.processing_service.fetch_defi_instruments(
                        protocol=protocol,
                        target_date=date,
                    )
                    if clob_instruments:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], clob_instruments))
                        logger.info(
                            "✅ Processed %s instruments from %s (CEFI on-chain CLOB)", len(clob_instruments), protocol
                        )
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
        return all_instruments

    async def _check_venue_access(self, cefi_exchanges: list[str]) -> list[str]:
        """Check venue access and filter out blocked venues."""
        tardis_adapter_checked: TardisAdapter | None = None
        try:
            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            raw_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
            tardis_adapter_checked = cast(TardisAdapter, raw_adapter)
        except (ValueError, Exception) as e:
            logger.warning("⚠️ Cannot check venue access: %s", e)

        if tardis_adapter_checked is not None:
            base_client = tardis_adapter_checked.base_client
            access_results: dict[str, tuple[bool, str]] = base_client.check_venues_access(cefi_exchanges)
            blocked: list[str] = [ex for ex, (ok, _) in access_results.items() if not ok]
            if blocked:
                for ex in blocked:
                    _, error_msg = access_results[ex]
                    logger.warning("⚠️ Blocking exchange %s: %s", ex, error_msg)
                accessible = [ex for ex in cefi_exchanges if ex not in blocked]
                logger.info("🔍 Venue access check: %s/%s exchanges accessible", len(accessible), len(cefi_exchanges))
                return accessible
            else:
                logger.info("✅ All %s exchanges are accessible", len(cefi_exchanges))

        return cefi_exchanges
