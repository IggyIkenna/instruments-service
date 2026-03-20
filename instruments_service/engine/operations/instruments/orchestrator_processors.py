"""
Orchestrator Market Processors

Contains processing logic for different market types (CeFi, TradFi, DeFi).
Split from orchestrator.py for better maintainability.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Protocol, cast, runtime_checkable
from uuid import uuid4

from unified_api_contracts import DEFI_PROTOCOLS, DEFI_VENUE_TO_PROTOCOL
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import get_adapter

from instruments_service.config import (
    UnifiedInstrumentConfig,
)
from instruments_service.engine.venues.special_instruments import (
    InstrumentDefDict,
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


@runtime_checkable
class ProcessorHost(Protocol):
    """Structural interface required by MarketProcessors methods."""

    venue_mapping: object
    processing_service: object


class MarketProcessors:
    """
    Market-specific processing logic for different market types.

    Handles CeFi, TradFi, and DeFi instrument processing.
    """

    async def process_cefi(
        self, date: datetime, exchanges: list[str] | None, force: bool, venues_filter: list[str]
    ) -> dict[str, InstrumentDefinition]:
        """
        Process CeFi (Tardis) exchanges.

        Args:
            date: Target date for instruments
            exchanges: List of exchanges to process
            force: Force refresh
            venues_filter: Venue filter list

        Returns:
            Dictionary of instrument definitions
        """
        cefi_exchanges: list[str] = (
            cast(list[str], exchanges)
            if exchanges is not None
            else cast(list[str], self.venue_mapping.all_tardis_exchanges)
        )

        # Apply venue filtering for CEFI
        if venues_filter:
            tardis_venue_values: set[str] = set(cast(dict[str, str], self.venue_mapping.tardis_to_venue).values())
            cefi_venues: list[str] = [v for v in venues_filter if v in tardis_venue_values]

            if cefi_venues:
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
                    cefi_exchanges = filtered_exchanges
                    logger.info("🔍 Filtered CEFI exchanges by canonical venues %s: %s", cefi_venues, cefi_exchanges)
                else:
                    logger.warning("⚠️ No matching CEFI exchanges found for canonical venues: %s", cefi_venues)
                    cefi_exchanges = []
            else:
                logger.info("🔍 No CEFI venues in filter, skipping CEFI processing")
                cefi_exchanges = []
        else:
            logger.debug("🔍 No venue filter specified, processing exchanges: %s", cefi_exchanges)

        if cefi_exchanges:
            # Pre-flight check: Validate venue access
            cefi_exchanges = await self._check_venue_access(cefi_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        if not cefi_exchanges:
            logger.info("⏭️ Skipping CEFI processing - no exchanges to process")
        else:
            logger.info("🚀 Processing %s CeFi exchanges in parallel...", len(cefi_exchanges))

            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)

            async def process_single_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
                try:
                    logger.info("🔍 Processing CeFi exchange %s...", exchange)
                    adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
                    instruments_raw = await adapter.fetch_instruments(
                        exchange=exchange,
                        target_date=date,
                        force_refresh=force,
                        normalize=True,
                    )
                    result: dict[str, InstrumentDefinition] = {}
                    for d in cast(list[dict[str, object]], instruments_raw):
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
                            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
                            logger.warning(
                                "Failed to create InstrumentDefinition for %s: %s",
                                d.get("instrument_key", "unknown"),
                                e,
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
                    logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
                    logger.exception("Failed to process %s: %s", exchange, e)
                    return {}

            results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
                *[process_single_exchange(ex) for ex in cefi_exchanges],
                return_exceptions=False,
            )

            for result in results:
                if result:
                    all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        # Process on-chain CLOB venues
        all_instruments.update(await self._process_onchain_clob(date, venues_filter))

        return all_instruments

    async def process_tradfi(
        self, date: datetime, venues_filter: list[str], tradfi_venues: list[str] | None
    ) -> dict[str, InstrumentDefinition]:
        """
        Process TradFi (Databento) exchanges.

        Args:
            date: Target date for instruments
            venues_filter: Venue filter list
            tradfi_venues: Specific TradFi venues to process

        Returns:
            Dictionary of instrument definitions
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            databento_config = UnifiedInstrumentConfig()
            all_databento_exchanges: list[str] = cast(list[str], self.venue_mapping.all_databento_venues)

            databento_exchanges: list[str]
            if tradfi_venues:
                filtered_tradfi_venues = [v for v in tradfi_venues if v in all_databento_exchanges]
                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info("🔍 Processing specified TradFi venues: %s", databento_exchanges)
                else:
                    logger.warning(
                        "⚠️ No valid TradFi venues in tradfi_venues=%s, skipping TRADFI processing", tradfi_venues
                    )
                    databento_exchanges = []
            elif venues_filter:
                filtered_tradfi_venues = [v for v in venues_filter if v in all_databento_exchanges]
                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info("🔍 Filtered TRADFI exchanges by venues: %s", databento_exchanges)
                else:
                    logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                    databento_exchanges = []
            else:
                databento_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
                logger.info("🔍 No venue filter, processing default TRADFI exchanges: %s", databento_exchanges)

            if not databento_exchanges:
                logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
            else:
                logger.info("🚀 Processing %s TradFi venues...", len(databento_exchanges))

                results = await asyncio.gather(
                    *[self._process_databento_exchange(ex, date, databento_config) for ex in databento_exchanges],
                    return_exceptions=False,
                )

                for result in results:
                    if result:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], result))

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

    async def process_defi(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """
        Process DeFi protocols.

        Args:
            date: Target date for instruments
            venues_filter: Venue filter list

        Returns:
            Dictionary of instrument definitions
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
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
                    else:
                        logger.warning("⚠️ No matching DEFI protocols found for venues: %s", defi_venues)
                        defi_protocols = []
                else:
                    logger.info("🔍 No DEFI venues in filter, skipping DEFI processing")
                    defi_protocols = []
            else:
                defi_protocols = DEFI_PROTOCOLS
                logger.info("🔍 No venue filter specified, processing all DEFI protocols")

            if not defi_protocols:
                logger.info("⏭️ Skipping DEFI processing - no protocols to process")
            else:
                for protocol, chain in defi_protocols:
                    try:
                        if chain:
                            defi_instruments = await self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                chain=chain,
                                target_date=date,
                            )
                        else:
                            defi_instruments = await self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                target_date=date,
                            )
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

    async def _check_venue_access(self, cefi_exchanges: list[str]) -> list[str]:
        """Check venue access and filter out blocked venues."""
        adapter: object | None = None
        try:
            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            tardis_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
            if hasattr(tardis_adapter, "base_client"):
                base_client = tardis_adapter.base_client
                if hasattr(base_client, "check_venues_access"):
                    adapter = tardis_adapter
        except (ValueError, Exception) as e:
            logger.warning("⚠️ Cannot check venue access: %s", e)

        if adapter is not None and hasattr(adapter, "base_client"):
            base_client = adapter.base_client
            if hasattr(base_client, "check_venues_access"):
                access_results: dict[str, tuple[bool, str]] = cast(
                    dict[str, tuple[bool, str]],
                    base_client.check_venues_access(cefi_exchanges),
                )
                blocked: list[str] = [ex for ex, (ok, _) in access_results.items() if not ok]
                if blocked:
                    for ex in blocked:
                        _, error_msg = access_results[ex]
                        logger.warning("⚠️ Venue access blocked: %s - %s", ex, error_msg)
                    accessible = [ex for ex in cefi_exchanges if ex not in blocked]
                    if not accessible:
                        logger.error("All %s CEFI venues blocked - skipping CEFI processing", len(cefi_exchanges))
                        return []
                    else:
                        logger.warning(
                            "⚠️ %s/%s venues blocked, continuing with %s accessible venues",
                            len(blocked),
                            len(cefi_exchanges),
                            len(accessible),
                        )
                        return accessible

        return cefi_exchanges

    async def _process_onchain_clob(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """Process on-chain CLOB venues (Hyperliquid, Aster)."""
        all_instruments: dict[str, InstrumentDefinition] = {}

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

        if cefi_clob_protocols:
            protocol_names: list[str] = [p[0] for p in cefi_clob_protocols]
            logger.info("🚀 Processing %s on-chain CLOB venues (CEFI): %s", len(cefi_clob_protocols), protocol_names)
            for protocol, _chain in cefi_clob_protocols:
                try:
                    clob_instruments = await self.processing_service.fetch_defi_instruments(
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

    async def _process_databento_exchange(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process a single Databento exchange."""
        try:
            if exchange == "CBOE":
                vix_def_dict: InstrumentDefDict = create_vix_instrument_definition(date)
                if vix_def_dict:
                    vix_def = InstrumentDefinition.model_validate(vix_def_dict)
                    logger.info("✅ Created VIX: %s", vix_def.instrument_key)
                    return {vix_def.instrument_key: vix_def}
                return {}
            elif exchange == "FX":
                krwusd_def_dict: InstrumentDefDict = create_krwusd_instrument_definition(date)
                if krwusd_def_dict:
                    krwusd_def = InstrumentDefinition.model_validate(krwusd_def_dict)
                    logger.info("✅ Created KRW/USD: %s", krwusd_def.instrument_key)
                    return {krwusd_def.instrument_key: krwusd_def}
                return {}
            elif exchange in ["NASDAQ", "NYSE"]:
                instruments: dict[str, InstrumentDefinition] = {}
                symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                bitcoin_etf_launch_date = datetime(2024, 1, 11, tzinfo=UTC)
                bitcoin_etf_tickers = ["IBIT", "FBTC", "ARKB"]

                if date >= bitcoin_etf_launch_date:
                    for ticker in bitcoin_etf_tickers:
                        if ticker in symbols:
                            etf_def_dict: InstrumentDefDict | None = create_bitcoin_etf_instrument_definition(
                                ticker, date, get_us_equity_trading_hours
                            )
                            if etf_def_dict:
                                etf_def = InstrumentDefinition.model_validate(etf_def_dict)
                                instruments[etf_def.instrument_key] = etf_def
                                logger.info("✅ Created Bitcoin ETF: %s", etf_def.instrument_key)

                non_btc_etf_symbols = [s for s in symbols if s not in bitcoin_etf_tickers]
                if non_btc_etf_symbols:
                    _raw_other: dict[str, InstrumentDefinition] = cast(
                        dict[str, InstrumentDefinition],
                        await self.processing_service.fetch_databento_instruments(
                            exchange=exchange,
                            symbols=non_btc_etf_symbols,
                            target_date=date,
                        )
                        or {},
                    )
                    if _raw_other:
                        instruments.update(_raw_other)
                        logger.info("✅ Processed %s additional instruments from %s", len(_raw_other), exchange)

                if instruments:
                    logger.info("✅ Processed %s total instruments from %s", len(instruments), exchange)
                return instruments
            else:
                # CME, ICE processing
                symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                if not symbols:
                    logger.warning("⚠️ No symbols configured for %s", exchange)
                    return {}

                instruments = cast(
                    dict[str, InstrumentDefinition],
                    await self.processing_service.fetch_databento_instruments(
                        exchange=exchange,
                        symbols=symbols,
                        target_date=date,
                    )
                    or {},
                )

                if instruments:
                    logger.info("✅ Processed %s instruments from %s", len(instruments), exchange)
                return instruments

        except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.warning("%s", _err.message, extra={"correlation_id": _err.correlation_id})
            logger.exception("Failed to process %s: %s", exchange, e)
            return {}


# ─── Module-level function adapters ──────────────────────────────────────────
# orchestrator.py imports and calls these as free functions with the orchestrator
# as the first positional argument. Because InstrumentsOrchestrator has the same
# attributes (venue_mapping, processing_service) that MarketProcessors methods
# access via self, we simply delegate via unbound-method call.


async def process_cefi(
    orchestrator: ProcessorHost,
    date: datetime,
    venues_filter: list[str],
) -> dict[str, InstrumentDefinition]:
    """Delegate to MarketProcessors.process_cefi using the orchestrator as receiver."""
    exchanges: list[str] | None = cast(list[str] | None, getattr(orchestrator, "_exchanges", None))
    force: bool = bool(getattr(orchestrator, "_force", False))
    return await MarketProcessors.process_cefi(
        cast(MarketProcessors, orchestrator), date, exchanges, force, venues_filter
    )


async def process_tradfi(
    orchestrator: ProcessorHost,
    date: datetime,
    venues_filter: list[str],
    tradfi_venues: list[str] | None,
) -> dict[str, InstrumentDefinition]:
    """Delegate to MarketProcessors.process_tradfi using the orchestrator as receiver."""
    return await MarketProcessors.process_tradfi(
        cast(MarketProcessors, orchestrator), date, venues_filter, tradfi_venues
    )


async def process_defi(
    orchestrator: ProcessorHost,
    date: datetime,
    venues_filter: list[str],
) -> dict[str, InstrumentDefinition]:
    """Delegate to MarketProcessors.process_defi using the orchestrator as receiver."""
    return await MarketProcessors.process_defi(cast(MarketProcessors, orchestrator), date, venues_filter)
