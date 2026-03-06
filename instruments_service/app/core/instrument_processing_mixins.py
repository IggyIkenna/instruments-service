"""
Instrument Processing Mixins

Provides specialized functionality for different data providers and processing tasks.
Separates concerns for better maintainability.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import unified_market_interface.clients.subgraph_service as sg_module
import unified_market_interface.clients.thegraph_base_client as tgc_module
from unified_config_interface import DataTypeConfig, ExchangeInstrumentConfig, VenueMapping
from unified_domain_client import DateFilterService
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import DatabentoAdapter, SubgraphService, TardisAdapter
from unified_market_interface import VenueMapping as UMI_VenueMapping
from unified_trading_library import get_secret_client, handle_api_errors

from instruments_service.app.core.instrument_processing_base import InstrumentProcessingConfig
from instruments_service.app.core.processors.canonical_key_generator import (
    CanonicalKeyServiceProtocol,
)
from instruments_service.app.core.processors.canonical_key_generator import (
    generate_canonical_key as _generate_canonical_key,
)
from instruments_service.app.core.processors.ccxt_manual_fallback import get_manual_ccxt_fallback
from instruments_service.app.core.processors.derived_fields_populator import (
    DerivedFieldsServiceProtocol,
    populate_derived_fields,
)
from instruments_service.app.core.processors.symbol_parser import SymbolParser
from instruments_service.config import instruments_config
from instruments_service.engine.processors.defi_processor import (
    DefiServiceProtocol,
)
from instruments_service.engine.processors.defi_processor import (
    fetch_defi_instruments as _fetch_defi_instruments,
)
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.ccxt_service import CCXTService

logger = logging.getLogger(__name__)


class TardisIntegrationMixin:
    """
    Mixin for Tardis API integration functionality.

    Handles CeFi exchange data fetching and processing.
    """

    tardis_adapter: TardisAdapter | None = None
    # Attributes provided by InstrumentProcessingBase (declared here so
    # basedpyright can resolve them when checking this mixin in isolation)
    api_key: str | None = None
    data_config: DataTypeConfig = cast(DataTypeConfig, cast(object, None))

    def _get_tardis_adapter(self) -> TardisAdapter:
        """
        Lazy-load Tardis adapter only when needed for CeFi instruments.

        Returns:
            TardisAdapter instance

        Raises:
            ValueError: If API key is not available when trying to fetch CeFi instruments
        """
        if not hasattr(self, "tardis_adapter") or self.tardis_adapter is None:
            if not self.api_key:
                raise ValueError(
                    "Tardis API key required for CeFi instruments. "
                    "Provide 'tardis_api_key' in config, or ensure Secret Manager access "
                    "to 'tardis-api-key' secret."
                )

            project_id = getattr(self, "_tardis_project_id", instruments_config.gcp_project_id)
            self.tardis_adapter = TardisAdapter(api_key=self.api_key, project_id=project_id)

        return self.tardis_adapter

    @handle_api_errors(max_retries=3)
    async def fetch_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> tuple[dict[str, dict[str, object]], int]:
        """
        Fetch instrument data from Tardis API for specific exchange.

        Args:
            exchange: Exchange name
            target_date: Target date for instrument availability
            force: If True, bypass date availability filtering

        Returns:
            Tuple of (instruments_data dict, date_filtered_count)
        """
        target_date = target_date or datetime.now(UTC)

        # Wrap synchronous Tardis API call in asyncio.to_thread
        available_symbols_list, date_filtered_count = await asyncio.to_thread(
            self._get_tardis_adapter().fetch_exchange_instruments,
            exchange=exchange,
            target_date=target_date,
            force_refresh=force,
        )

        # Convert list to dict keyed by symbol_id
        available_symbols: dict[str, dict[str, object]] = {
            (str(cast(str | None, symbol.get("id"))) or ""): symbol
            for symbol in available_symbols_list
            if symbol.get("id")
        }

        # Filter instruments available at target date AND exclude unwanted types
        instruments_data: dict[str, dict[str, object]] = {}
        for symbol_id, symbol in available_symbols.items():
            if not symbol_id:
                continue
            symbol_type = symbol.get("type") or ""

            # Filter excluded instrument types
            if symbol_type in self.data_config.excluded_instrument_types:
                logger.debug("Filtered excluded instrument type: %s (%s)", symbol_id, symbol_type)
                continue

            # Filter problematic Binance instruments
            if exchange in ["binance", "binance-futures"] and self._is_problematic_binance_instrument(symbol_id):
                logger.debug("Filtered problematic Binance instrument: %s", symbol_id)
                continue

            instruments_data[symbol_id] = symbol

        logger.info("✅ Retrieved %s instruments for %s", len(instruments_data), exchange)
        return instruments_data, date_filtered_count

    def _is_problematic_binance_instrument(self, symbol_id: str) -> bool:
        """
        Check if this is a problematic Binance instrument that should be filtered out.

        Args:
            symbol_id: Symbol identifier from Tardis

        Returns:
            bool: True if instrument should be filtered out
        """
        symbol_lower = symbol_id.lower()

        # Filter multiplier tokens
        multiplier_patterns = [
            "1000x",
            "1000sats",
            "1000cat",
            "1000cheems",
            "1mbabydoge",
            "1000pepe",
            "1000shib",
            "1000bonk",
            "1000floki",
            "1000rats",
            "1000btt",
            "1000lunc",
            "1000000mog",
            "1000why",
            "1000000bob",
            "1000000neiro",
            "1000wen",
            "1000usual",
            "1000turbo",
            "1000xec",
            "1000000babydoge",
        ]

        for pattern in multiplier_patterns:
            if symbol_lower.startswith(pattern):
                return True

        # General pattern for multiplier tokens
        if re.match(r"^(1000|1000000|1m)", symbol_lower):
            return True

        # Filter USDT as base asset (fiat pairs)
        usdt_base_patterns = [
            "usdttry",
            "usdtzar",
            "usdtuah",
            "usdtpln",
            "usdtars",
            "usdtmxn",
            "usdtcop",
            "usdtclp",
            "usdtpen",
            "usdtves",
            "usdtngn",
            "usdtbrl",
            "usdtgel",
            "usdtczk",
            "usdtron",
        ]

        if symbol_lower in usdt_base_patterns:
            return True

        # Filter tokens starting with numbers
        if symbol_lower.startswith(("1inch", "0g", "2z", "3p", "4p", "5p")):
            return True

        # Filter other problematic patterns
        problematic_patterns = ["nftusdt", "defiusdt", "bullusdt", "bearusdt"]

        return symbol_lower in problematic_patterns

    def _convert_to_tardis_symbol(self, symbol_id: str, exchange: str) -> str:
        """
        Convert symbol_id to proper Tardis API format.

        Args:
            symbol_id: Raw symbol from Tardis API
            exchange: Exchange name

        Returns:
            str: Tardis-formatted symbol
        """
        try:
            if exchange in ["binance", "binance-futures"]:
                return symbol_id.replace("-", "").lower()
            elif exchange == "deribit":
                return symbol_id.lower()
            elif exchange in ["upbit", "coinbase"]:
                return symbol_id.upper()
            else:
                return symbol_id.lower()
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
            logger.debug("Failed to convert symbol %s for %s: %s", symbol_id, exchange, e)
            return symbol_id.lower()


class DatabentoIntegrationMixin:
    """
    Mixin for Databento API integration functionality.

    Handles TradFi instrument data fetching.
    """

    @handle_api_errors(max_retries=3)
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
        """
        try:
            adapter = DatabentoAdapter()
            date = target_date or datetime.now(UTC)

            # Wrap synchronous Databento API call
            raw_instruments = await asyncio.to_thread(
                adapter.fetch_instrument_definitions, exchange=exchange, symbols=symbols, date=date
            )

            # Convert to InstrumentDefinition objects
            instruments: dict[str, InstrumentDefinition] = {}
            for inst_key, inst_data in raw_instruments.items():
                try:
                    inst_def = InstrumentDefinition(**inst_data)
                    instruments[inst_key] = inst_def
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
                    logger.warning("Failed to create InstrumentDefinition for %s: %s", inst_key, e)
                    continue
            logger.info("✅ Fetched %s Databento instruments for %s", len(instruments), exchange)
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
            logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
            logger.error("Failed to fetch Databento instruments: %s", e)
            return {}


class DefiIntegrationMixin:
    """
    Mixin for DeFi protocol integration functionality.

    Handles DeFi instrument data fetching from various protocols.
    """

    _graph_api_key: str | None = None
    # Class-level type annotations with cast(T, cast(object, None)) satisfy basedpyright's
    # reportUninitializedInstanceVariable without adding an __init__ that
    # would trigger reportUnsafeMultipleInheritance.
    subgraph_service: SubgraphService = cast(SubgraphService, cast(object, None))
    date_filter_service: DateFilterService = cast(DateFilterService, cast(object, None))

    def _init_defi_integration(self):
        """Initialize DeFi integration components."""
        # Initialize service instances (class-level annotations need runtime initialization here)
        self.subgraph_service = SubgraphService()
        self.date_filter_service = DateFilterService()

        # Retrieve Graph API key and cache it
        self._graph_api_key = None
        try:
            project_id_for_graph = instruments_config.gcp_project_id
            secret_name = instruments_config.graph_secret_name
            self._graph_api_key = get_secret_client(
                project_id=project_id_for_graph,
            ).get_secret(secret_name)
            if self._graph_api_key:
                self._graph_api_key = self._graph_api_key.strip()

                # Set in module-level cache
                tgc_module._API_KEY_CACHE = self._graph_api_key
                tgc_module._API_KEY_PROJECT_ID = project_id_for_graph
                sg_module._GRAPH_API_KEY_CACHE = self._graph_api_key
                sg_module._GRAPH_API_KEY_PROJECT_ID = project_id_for_graph
                logger.info("✅ Retrieved and cached Graph API key")
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
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

    def fetch_defi_instruments(
        self,
        protocol: str,
        chain: str = "ETHEREUM",
        target_date: datetime | None = None,
        **kwargs: str | int | bool | None | datetime,
    ) -> dict[str, InstrumentDefinition]:
        """Fetch DeFi instruments from various protocols."""
        return _fetch_defi_instruments(
            cast(DefiServiceProtocol, cast(object, self)),
            protocol,
            chain,
            target_date,
            **kwargs,
        )


class CCXTIntegrationMixin:
    """
    Mixin for CCXT integration functionality.

    Handles CCXT market data and enrichment.
    """

    ccxt_service: CCXTService | None = None
    # Attributes provided by InstrumentProcessingBase
    processing_config: InstrumentProcessingConfig = cast(InstrumentProcessingConfig, cast(object, None))
    venue_mapping: VenueMapping = cast(VenueMapping, cast(object, None))

    def _init_ccxt_integration(self, config: dict[str, object]):
        """Initialize CCXT integration components."""
        if self.processing_config.enable_ccxt_integration:
            self.ccxt_service = CCXTService(
                venue_mapping=cast(UMI_VenueMapping, self.venue_mapping),
                cache_ttl_hours=int(cast(int | float, config.get("cache_ttl_hours", 4))),
            )
            # Pre-load CCXT markets in parallel
            if config.get("preload_ccxt_markets", True):
                venues_to_preload = [
                    v
                    for v in self.venue_mapping.all_tardis_exchanges
                    if self.venue_mapping.venue_to_ccxt.get(v.upper())
                ]
                self.ccxt_service.preload_markets_parallel(venues=[v.upper() for v in venues_to_preload], max_workers=4)
        else:
            self.ccxt_service = None

    def get_manual_ccxt_fallback(self, venue: str, base_asset: str) -> dict[str, object]:
        """Manual fallback mappings when CCXT lookup fails."""
        result: dict[str, str | float | None] = get_manual_ccxt_fallback(venue, base_asset)
        return cast(dict[str, object], result)


class SymbolProcessingMixin:
    """
    Mixin for symbol parsing and processing functionality.

    Handles symbol parsing, canonical key generation, and derived fields.
    """

    # Class-level type annotations with cast(T, cast(object, None)) satisfy basedpyright's
    # reportUninitializedInstanceVariable without adding an __init__ that
    # would trigger reportUnsafeMultipleInheritance.
    _symbol_parser: SymbolParser = cast(SymbolParser, cast(object, None))
    # Attributes provided by InstrumentProcessingBase
    exchange_config: ExchangeInstrumentConfig = cast(ExchangeInstrumentConfig, cast(object, None))

    def _init_symbol_processing(self):
        """Initialize symbol processing components."""
        self._symbol_parser = SymbolParser(self.exchange_config)

    def generate_canonical_key(
        self,
        exchange: str,
        symbol_type: str,
        symbol_id: str,
        symbol_info: dict[str, object],
    ) -> str | None:
        """Generate canonical instrument key."""
        return _generate_canonical_key(
            cast(CanonicalKeyServiceProtocol, cast(object, self)), exchange, symbol_type, symbol_id, symbol_info
        )

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse base/quote assets from Tardis symbol ID."""
        return self._symbol_parser.parse_symbol_components(symbol_id, exchange)

    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, object]:
        """Parse option expiry, strike, and type."""
        return self._symbol_parser.parse_option_components(symbol_id, exchange)

    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None:
        """Parse expiry from symbol using exchange-specific patterns."""
        return self._symbol_parser.parse_expiry_from_symbol(symbol_id, exchange)

    def _parse_deribit_date(self, date_str: str) -> str:
        """Parse Deribit date format: 25DEC25 → 2025-12-25T08:00:00Z."""
        return self._symbol_parser.parse_deribit_date(date_str)

    async def _populate_all_derived_fields(
        self,
        canonical_key: str,
        venue: str,
        inst_type: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        exchange: str | None = None,
    ) -> dict[str, object]:
        """Populate ALL derived fields for instrument definition."""
        return await populate_derived_fields(
            cast(DerivedFieldsServiceProtocol, cast(object, self)),
            canonical_key,
            venue,
            inst_type,
            base_asset,
            quote_asset,
            symbol_id,
            exchange,
        )

    async def _parse_symbol_async(self, symbol_id: str, exchange: str) -> dict[str, str]:
        """Parse symbol components asynchronously for parallel processing."""
        try:
            parsed = self.parse_symbol_components(symbol_id, exchange)
            base: object = parsed.get("base_asset")
            quote: object = parsed.get("quote_asset")
            return {
                "base_asset": str(base) if base is not None else "",
                "quote_asset": str(quote) if quote is not None else "",
            }
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
            logger.debug("Error parsing symbol %s: %s", symbol_id, e)
            return {}
