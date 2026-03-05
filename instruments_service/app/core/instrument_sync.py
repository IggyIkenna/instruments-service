"""
Instrument Synchronization — CEFI, TRADFI, and DEFI Venue Processing.

Extracted from instruments_service.py — contains the venue-specific instrument
generation logic for CeFi (Tardis), TradFi (Databento), and DeFi protocols.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from unified_config_interface import VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import TardisAdapter, get_adapter

if TYPE_CHECKING:
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService

from instruments_service.config import (
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    UnifiedInstrumentConfig,
)
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.special_instruments import (
    InstrumentDefDict,
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)

logger = logging.getLogger(__name__)


class InstrumentSyncMixin:
    """
    Mixin providing venue-specific instrument generation methods.

    Requires the host class to have:
        - self.venue_mapping: VenueMapping
        - self.processing_service: InstrumentProcessingService
    """

    # Attribute stubs provided by the concrete host class.
    # cast(T, cast(object, None)) satisfies reportUninitializedInstanceVariable.
    venue_mapping: VenueMapping = cast(VenueMapping, cast(object, None))
    processing_service: InstrumentProcessingService = cast("InstrumentProcessingService", cast(object, None))

    async def _process_cefi_exchanges(
        self,
        cefi_exchanges: list[str],
        venues_filter: list[str],
        date: datetime,
        force: bool,
    ) -> tuple[list[str], dict[str, InstrumentDefinition]]:
        """
        Process CeFi (Tardis) exchanges and on-chain CLOB venues.

        Returns:
            Tuple of (updated cefi_exchanges list, generated instruments dict)
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

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
            # Pre-flight check: Validate venue access before processing
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
                        logger.warning("⚠️ Venue access blocked: %s - %s", ex, error_msg)
                    accessible = [ex for ex in cefi_exchanges if ex not in blocked]
                    if not accessible:
                        logger.error("All %s CEFI venues blocked - skipping CEFI processing", len(cefi_exchanges))
                        cefi_exchanges = []
                    else:
                        logger.warning(
                            "⚠️ %s/%s venues blocked, continuing with %s accessible venues",
                            len(blocked),
                            len(cefi_exchanges),
                            len(accessible),
                        )
                        cefi_exchanges = accessible

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
                    instruments_raw = cast(
                        list[dict[str, object]],
                        await cast(TardisAdapter, adapter).fetch_instruments(  # pyright: ignore[reportUnknownMemberType]
                            exchange=exchange,
                            target_date=date,
                            force_refresh=force,
                            normalize=True,
                        ),
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
                            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
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
                    logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                    logger.exception("Failed to process %s: %s", exchange, e)
                    return {}

            results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
                *[process_single_exchange(ex) for ex in cefi_exchanges],
                return_exceptions=False,
            )

            for result in results:
                if result:
                    all_instruments.update(result)

        # Process on-chain CLOB venues (Hyperliquid, Aster) as part of CEFI
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

        return cefi_exchanges, all_instruments

    async def _process_tradfi_exchanges(
        self,
        venues_filter: list[str],
        tradfi_venues: list[str] | None,
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process TradFi (Databento) exchanges.

        Returns:
            Dictionary of generated instruments
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        databento_config = UnifiedInstrumentConfig()
        all_databento_exchanges: list[str] = cast(list[str], self.venue_mapping.all_databento_venues)

        # Apply venue filtering for TRADFI
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
            filtered_tradfi_venues_list: list[str] = [v for v in venues_filter if v in all_databento_exchanges]
            if filtered_tradfi_venues_list:
                databento_exchanges = filtered_tradfi_venues_list
                logger.info(
                    "🔍 Filtered TRADFI exchanges by venues %s: %s", filtered_tradfi_venues_list, databento_exchanges
                )
            else:
                logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                databento_exchanges = []
        else:
            databento_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
            logger.info("🔍 No venue filter specified, processing default TRADFI exchanges: %s", databento_exchanges)
        if not databento_exchanges:
            logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
            return all_instruments

        logger.info("🚀 Processing %s TradFi venues...", len(databento_exchanges))

        async def process_databento_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
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
                    else:
                        logger.debug(
                            "⏭️ Skipping Bitcoin ETFs - date %s is before launch (2024-01-11)", date.strftime("%Y-%m-%d")
                        )

                    non_btc_etf_symbols = [s for s in symbols if s not in bitcoin_etf_tickers]
                    if non_btc_etf_symbols:
                        _raw_other: dict[str, InstrumentDefinition] = cast(
                            dict[str, InstrumentDefinition],
                            await self.processing_service.fetch_databento_instruments(  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType,reportUnknownVariableType]
                                exchange=exchange,
                                symbols=non_btc_etf_symbols,
                                target_date=date,
                            )
                            or {},
                        )
                        other_instruments = _raw_other
                        if other_instruments:
                            instruments.update(other_instruments)
                            logger.info(
                                "✅ Processed %s additional instruments from %s", len(other_instruments), exchange
                            )
                    if instruments:
                        logger.info("✅ Processed %s total instruments from %s", len(instruments), exchange)
                    return instruments
                elif exchange == "ICE":
                    ice_us_launch = datetime(2018, 12, 23, tzinfo=UTC)
                    if date < ice_us_launch:
                        logger.info(
                            "⏭️ Skipping ICE - date %s is before ICE US dataset launch (2018-12-23)",
                            date.strftime("%Y-%m-%d"),
                        )
                        return {}
                    ice_symbols: list[str] = databento_config.get_symbols_for_venue(exchange)
                    if not ice_symbols:
                        logger.warning("⚠️ No symbols configured for %s", exchange)
                        return {}
                    ice_instruments = cast(
                        dict[str, InstrumentDefinition],
                        await self.processing_service.fetch_databento_instruments(  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType,reportUnknownVariableType]
                            exchange=exchange,
                            symbols=ice_symbols,
                            target_date=date,
                        )
                        or {},
                    )
                    if ice_instruments:
                        logger.info("✅ Processed %s ICE instruments", len(ice_instruments))
                    return ice_instruments
                else:
                    cme_symbols: list[str] = databento_config.get_symbols_for_venue(exchange)
                    if not cme_symbols:
                        logger.warning("⚠️ No symbols configured for %s", exchange)
                        return {}
                    cme_instruments = cast(
                        dict[str, InstrumentDefinition],
                        await self.processing_service.fetch_databento_instruments(  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType,reportUnknownVariableType]
                            exchange=exchange,
                            symbols=cme_symbols,
                            target_date=date,
                        )
                        or {},
                    )
                    if cme_instruments:
                        logger.info("✅ Processed %s instruments from %s", len(cme_instruments), exchange)
                    return cme_instruments
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
                logger.exception("Failed to process %s: %s", exchange, e)
                return {}

        results = await asyncio.gather(
            *[process_databento_exchange(ex) for ex in databento_exchanges],
            return_exceptions=False,
        )

        for result in results:
            if result:
                all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        return all_instruments

    async def _process_defi_protocols(
        self,
        venues_filter: list[str],
        date: datetime,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process DeFi protocols.

        Returns:
            Dictionary of generated instruments
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        # Filter protocols if venues specified
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
            return all_instruments

        for protocol, chain in defi_protocols:
            try:
                if chain:
                    defi_instruments = self.processing_service.fetch_defi_instruments(
                        protocol=protocol,
                        chain=chain,
                        target_date=date,
                    )
                else:
                    defi_instruments = self.processing_service.fetch_defi_instruments(
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

        return all_instruments
