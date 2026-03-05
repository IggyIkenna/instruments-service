"""
Base Instrument Processor

Provides shared functionality for all category-specific processors (CeFi, TradFi, DeFi).
Contains common utilities for symbol parsing, caching, and metadata management.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from unified_config_interface import DataTypeConfig, ExchangeInstrumentConfig, VenueMapping
from unified_domain_client import DateFilterService
from unified_market_interface import SubgraphService
from unified_market_interface import VenueMapping as UMI_VenueMapping

from instruments_service.config import instruments_config
from instruments_service.engine.processors.symbol_parser import OptionComponents, SymbolComponents, SymbolParser
from instruments_service.engine.venues.ccxt_service import CCXTService
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


@dataclass
class InstrumentProcessingConfig:
    """Configuration for instrument processing operations"""

    api_key: str
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 1.0
    enable_ccxt_integration: bool = True
    enable_metadata_caching: bool = True
    cache_ttl_hours: int = 24
    supported_exchanges: list[str] = field(
        default_factory=lambda: [
            "binance",
            "binance-futures",
            "deribit",
            "bybit",
            "bybit-spot",
            "okx",
            "okx-futures",
            "okx-swap",
        ]
    )


class BaseInstrumentProcessor:
    """
    Base processor for all instrument categories.

    Provides shared functionality:
    - Configuration management
    - Venue/instrument type mapping
    - Symbol parsing
    - Metadata caching
    - CCXT integration
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize base processor.

        Args:
            config: Configuration with instrument processing settings
        """
        self.config = config
        self.project_id: str = str(config.get("project_id") or instruments_config.gcp_project_id)

        # Use centralized configs from config.py (DRY principle)
        self.venue_mapping = VenueMapping()
        self.exchange_config = ExchangeInstrumentConfig()
        self.data_config = DataTypeConfig()
        self._symbol_parser = SymbolParser(self.exchange_config)

        retry_attempts: int = int(cast(int | float, config.get("retry_max_attempts", 3)))
        retry_backoff: float = float(cast(int | float, config.get("retry_backoff_factor", 1.0)))
        cache_ttl: int = int(cast(int | float, config.get("cache_ttl_hours", 24)))

        # API key is optional - only required for specific categories
        api_key: str | None = cast(str | None, config.get("tardis_api_key") or config.get("api_key"))

        self.processing_config = InstrumentProcessingConfig(
            api_key=api_key or "",
            retry_max_attempts=retry_attempts,
            retry_backoff_factor=retry_backoff,
            enable_ccxt_integration=cast(bool, config.get("enable_ccxt_integration", True)),
            enable_metadata_caching=cast(bool, config.get("enable_metadata_caching", True)),
            cache_ttl_hours=cache_ttl,
            supported_exchanges=self.venue_mapping.all_tardis_exchanges,
        )

        # Initialize metadata cache
        self._metadata_cache: dict[str, InstrumentDefinition] = {}
        self._cache_timestamps: dict[str, datetime] = {}

        # Initialize centralized services
        self.subgraph_service: SubgraphService = SubgraphService()
        self.date_filter_service = DateFilterService()

        # Initialize CCXT service if enabled
        if self.processing_config.enable_ccxt_integration:
            self.ccxt_service = CCXTService(
                venue_mapping=cast(UMI_VenueMapping, self.venue_mapping),
                cache_ttl_hours=int(cast(int | float, config.get("cache_ttl_hours", 4))),
            )
            if config.get("preload_ccxt_markets", True):
                venues_to_preload = [
                    v
                    for v in self.venue_mapping.all_tardis_exchanges
                    if self.venue_mapping.venue_to_ccxt.get(v.upper())
                ]
                self.ccxt_service.preload_markets_parallel(venues=[v.upper() for v in venues_to_preload], max_workers=4)
        else:
            self.ccxt_service = None

    def get_venue_mapping(self) -> dict[str, str]:
        """Get canonical venue mapping."""
        return self.venue_mapping.tardis_to_venue

    def get_instrument_type_mapping(self) -> dict[str, str]:
        """Get canonical instrument type mapping."""
        return {
            "spot": "SPOT_PAIR",
            "perpetual": "PERPETUAL",
            "future": "FUTURE",
            "option": "OPTION",
            "combo": "OPTION",
        }

    def normalize_venue(self, exchange: str) -> str | None:
        """
        Normalize exchange name to canonical venue.

        Args:
            exchange: Raw exchange name

        Returns:
            Canonical venue name or None if unknown
        """
        venue_mapping = self.get_venue_mapping()
        venue = venue_mapping.get(exchange.lower())

        if not venue:
            logger.warning("Unknown exchange: %s", exchange)

        return venue

    def normalize_instrument_type(self, symbol_type: str) -> str | None:
        """
        Normalize symbol type to canonical instrument type.

        Args:
            symbol_type: Raw symbol type from exchange

        Returns:
            Canonical instrument type or None if unknown
        """
        type_mapping = self.get_instrument_type_mapping()
        instrument_type = type_mapping.get(symbol_type.lower())

        if not instrument_type:
            logger.warning("Unknown symbol type: %s", symbol_type)

        return instrument_type

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> SymbolComponents:
        """Parse base/quote assets from symbol ID."""
        return self._symbol_parser.parse_symbol_components(symbol_id, exchange)

    def parse_option_components(self, symbol_id: str, exchange: str) -> OptionComponents:
        """Parse option expiry, strike, and type."""
        return self._symbol_parser.parse_option_components(symbol_id, exchange)

    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None:
        """Parse expiry from symbol using exchange-specific patterns."""
        return self._symbol_parser.parse_expiry_from_symbol(symbol_id, exchange)

    def cache_metadata(self, instrument_key: str, metadata: InstrumentDefinition):
        """
        Cache instrument metadata.

        Args:
            instrument_key: Canonical instrument key
            metadata: Metadata to cache
        """
        if self.processing_config.enable_metadata_caching:
            self._metadata_cache[instrument_key] = metadata
            self._cache_timestamps[instrument_key] = datetime.now(UTC)

    def get_processing_stats(self) -> dict[str, object]:
        """Get current processing statistics for monitoring"""
        return {
            "supported_exchanges": len(self.processing_config.supported_exchanges),
            "ccxt_integration_enabled": self.processing_config.enable_ccxt_integration,
            "caching_enabled": self.processing_config.enable_metadata_caching,
            "cached_instruments": len(self._metadata_cache),
            "cache_ttl_hours": self.processing_config.cache_ttl_hours,
            "retry_max_attempts": self.processing_config.retry_max_attempts,
        }

    def clear_cache(self):
        """Clear all cached metadata"""
        cache_count = len(self._metadata_cache)
        self._metadata_cache.clear()
        self._cache_timestamps.clear()
        logger.info("🧹 Cleared %s cached instruments", cache_count)

    def cleanup(self):
        """Cleanup resources and close connections"""
        if hasattr(self, "ccxt_service") and self.ccxt_service:
            self.ccxt_service.clear_cache()

        if hasattr(self, "subgraph_service") and self.subgraph_service:
            self.subgraph_service.clear_cache()

        self._metadata_cache.clear()
        self._cache_timestamps.clear()

        logger.info("🧹 BaseInstrumentProcessor cleanup completed")
