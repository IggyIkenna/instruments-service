"""
TradFi Orchestration Module

Handles processing of TradFi (Databento) exchanges including special instruments like VIX, KRW/USD, and Bitcoin ETFs.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from unified_api_contracts import VenueMapping

from instruments_service.config import UnifiedInstrumentConfig
from instruments_service.models import InstrumentDefinition

if TYPE_CHECKING:
    from instruments_service.app.core.instrument_processing_service import InstrumentProcessingService
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

from instruments_service.engine.venues.special_instruments import (
    InstrumentDefDict,
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)

logger = logging.getLogger(__name__)


class TradFiOrchestrator:
    """Orchestrates TradFi instrument processing."""

    def __init__(self, processing_service: "InstrumentProcessingService") -> None:
        """Initialize TradFi orchestrator."""
        self.processing_service = processing_service
        self.venue_mapping: VenueMapping = VenueMapping()

    async def process_tradfi(
        self, date: datetime, venues_filter: list[str], tradfi_venues: list[str] | None
    ) -> dict[str, InstrumentDefinition]:
        """Process TradFi (Databento) exchanges."""
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            databento_config = UnifiedInstrumentConfig()
            all_databento_exchanges: list[str] = cast(list[str], self.venue_mapping.all_databento_venues)

            # Determine which exchanges to process
            databento_exchanges = self._determine_tradfi_exchanges(
                all_databento_exchanges, tradfi_venues, venues_filter
            )

            if not databento_exchanges:
                logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
                return all_instruments

            logger.info("🚀 Processing %s TradFi venues...", len(databento_exchanges))

            # Process exchanges in parallel
            exchange_tasks = [
                self._process_databento_exchange(exchange, date, databento_config) for exchange in databento_exchanges
            ]
            results = await asyncio.gather(*exchange_tasks, return_exceptions=False)

            # Aggregate results
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

    def _determine_tradfi_exchanges(
        self, all_databento_exchanges: list[str], tradfi_venues: list[str] | None, venues_filter: list[str]
    ) -> list[str]:
        """Determine which TradFi exchanges to process based on filters."""
        if tradfi_venues:
            filtered_tradfi_venues = [v for v in tradfi_venues if v in all_databento_exchanges]
            if filtered_tradfi_venues:
                logger.info("🔍 Processing specified TradFi venues: %s", filtered_tradfi_venues)
                return filtered_tradfi_venues
            else:
                logger.warning(
                    "⚠️ No valid TradFi venues in tradfi_venues=%s, skipping TRADFI processing", tradfi_venues
                )
                return []

        elif venues_filter:
            filtered_tradfi_venues = [v for v in venues_filter if v in all_databento_exchanges]
            if filtered_tradfi_venues:
                logger.info("🔍 Filtered TRADFI exchanges by venues filter: %s", filtered_tradfi_venues)
                return filtered_tradfi_venues
            else:
                logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                return []

        else:
            default_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
            logger.info("🔍 No venue filter specified, processing default TRADFI exchanges: %s", default_exchanges)
            return default_exchanges

    async def _process_databento_exchange(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process a single Databento exchange."""
        try:
            if exchange == "CBOE":
                return await self._process_cboe(date)
            elif exchange == "FX":
                return await self._process_fx(date)
            elif exchange in ["NASDAQ", "NYSE"]:
                return await self._process_us_equities(exchange, date, databento_config)
            elif exchange == "ICE":
                return await self._process_ice(exchange, date, databento_config)
            else:  # CME and others
                return await self._process_cme(exchange, date, databento_config)
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

    async def _process_cboe(self, date: datetime) -> dict[str, InstrumentDefinition]:
        """Process CBOE (VIX) instruments."""
        vix_def_dict: InstrumentDefDict = create_vix_instrument_definition(date)
        if vix_def_dict:
            vix_def = InstrumentDefinition.model_validate(vix_def_dict)
            logger.info("✅ Created VIX: %s", vix_def.instrument_key)
            return {vix_def.instrument_key: vix_def}
        return {}

    async def _process_fx(self, date: datetime) -> dict[str, InstrumentDefinition]:
        """Process FX (KRW/USD) instruments."""
        krwusd_def_dict: InstrumentDefDict = create_krwusd_instrument_definition(date)
        if krwusd_def_dict:
            krwusd_def = InstrumentDefinition.model_validate(krwusd_def_dict)
            logger.info("✅ Created KRW/USD: %s", krwusd_def.instrument_key)
            return {krwusd_def.instrument_key: krwusd_def}
        return {}

    async def _process_us_equities(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process US equity exchanges (NASDAQ, NYSE)."""
        instruments: dict[str, InstrumentDefinition] = {}
        symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

        # Handle Bitcoin ETFs
        instruments.update(await self._process_bitcoin_etfs(date, symbols))

        # Handle regular equity symbols
        non_btc_etf_symbols = [s for s in symbols if s not in ["IBIT", "FBTC", "ARKB"]]
        if non_btc_etf_symbols:
            other_instruments = await self._fetch_databento_instruments(exchange, non_btc_etf_symbols, date)
            if other_instruments:
                instruments.update(other_instruments)
                logger.info("✅ Processed %s additional instruments from %s", len(other_instruments), exchange)

        if instruments:
            logger.info("✅ Processed %s total instruments from %s", len(instruments), exchange)
        return instruments

    async def _process_bitcoin_etfs(self, date: datetime, symbols: list[str]) -> dict[str, InstrumentDefinition]:
        """Process Bitcoin ETF instruments."""
        instruments: dict[str, InstrumentDefinition] = {}
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
            logger.debug("⏭️ Skipping Bitcoin ETFs - date %s is before launch (2024-01-11)", date.strftime("%Y-%m-%d"))

        return instruments

    async def _process_ice(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process ICE exchange instruments."""
        ice_us_launch = datetime(2018, 12, 23, tzinfo=UTC)
        if date < ice_us_launch:
            logger.info(
                "⏭️ Skipping ICE - date %s is before ICE US dataset launch (2018-12-23)", date.strftime("%Y-%m-%d")
            )
            return {}

        ice_symbols: list[str] = databento_config.get_symbols_for_venue(exchange)
        if not ice_symbols:
            logger.warning("⚠️ No symbols configured for %s", exchange)
            return {}

        ice_instruments = await self._fetch_databento_instruments(exchange, ice_symbols, date)
        if ice_instruments:
            logger.info("✅ Processed %s ICE instruments", len(ice_instruments))
        return ice_instruments

    async def _process_cme(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process CME and other exchange instruments."""
        symbols: list[str] = databento_config.get_symbols_for_venue(exchange)
        if not symbols:
            logger.warning("⚠️ No symbols configured for %s", exchange)
            return {}

        instruments = await self._fetch_databento_instruments(exchange, symbols, date)
        if instruments:
            logger.info("✅ Processed %s instruments from %s", len(instruments), exchange)
        return instruments

    async def _fetch_databento_instruments(
        self, exchange: str, symbols: list[str], date: datetime
    ) -> dict[str, InstrumentDefinition]:
        """Fetch instruments from Databento."""
        # handle_api_errors decorator may lose async return type annotation in basedpyright;
        # call the method and ensure we await the coroutine result.
        maybe_coro = self.processing_service.fetch_databento_instruments(
            exchange=exchange,
            symbols=symbols,
            target_date=date,
        )
        result: object
        if asyncio.iscoroutine(maybe_coro):
            result = await maybe_coro
        else:
            result = maybe_coro
        return cast(dict[str, InstrumentDefinition], result or {})
