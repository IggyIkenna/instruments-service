"""
Base Instrument Processing Service

Core functionality for instrument processing without specific integrations.
Provides configuration, normalization, and basic processing capabilities.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from unified_api_contracts import DataTypeConfig, ExchangeInstrumentConfig, VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_trading_library import get_secret_client

from instruments_service.config import instruments_config
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


@dataclass
class InstrumentProcessingConfig:
    """Configuration for instrument processing operations"""

    api_key: str
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 1.0
    enable_ccxt_integration: bool = True
    enable_defi_integration: bool = True
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


class InstrumentProcessingBase:
    """
    Base instrument processing service with core functionality.

    Provides configuration, venue mapping, and normalization without
    specific API integrations.
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize base processing service.

        Args:
            config: Configuration with processing settings
        """
        self.config = config
        project_id: str = str(config.get("project_id") or instruments_config.gcp_project_id)

        # Try to get API key from config first
        self.api_key: str | None = cast(str | None, config.get("tardis_api_key") or config.get("api_key"))

        # If not in config, try Secret Manager
        if not self.api_key:
            try:
                secret_name = instruments_config.tardis_secret_name
                logger.debug(
                    "Attempting to retrieve Tardis API key from Secret Manager (secret: %s, project: %s)",
                    secret_name,
                    project_id,
                )
                self.api_key = get_secret_client(
                    project_id=project_id,
                ).get_secret(secret_name)
                if self.api_key:
                    self.api_key = self.api_key.strip()
                    logger.info("✅ Retrieved Tardis API key from Secret Manager")
                else:
                    logger.warning("⚠️ Tardis API key retrieval returned None (only needed for CeFi)")
            except (ConnectionError, TimeoutError, OSError, ValueError) as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.warning(_err.message, extra={"correlation_id": _err.correlation_id})
                logger.warning("⚠️ Tardis API key not available (only needed for CeFi): %s", e)
                self.api_key = None
        # Use centralized configs from config.py (DRY principle)
        self.venue_mapping = VenueMapping()
        self.exchange_config = ExchangeInstrumentConfig()
        self.data_config = DataTypeConfig()

        retry_attempts: int = int(cast(int | float, config.get("retry_max_attempts", 3)))
        retry_backoff: float = float(cast(int | float, config.get("retry_backoff_factor", 1.0)))
        cache_ttl: int = int(cast(int | float, config.get("cache_ttl_hours", 24)))
        self.processing_config = InstrumentProcessingConfig(
            api_key=self.api_key or "",
            retry_max_attempts=retry_attempts,
            retry_backoff_factor=retry_backoff,
            enable_ccxt_integration=cast(bool, config.get("enable_ccxt_integration", True)),
            enable_defi_integration=cast(bool, config.get("enable_defi_integration", True)),
            enable_metadata_caching=cast(bool, config.get("enable_metadata_caching", True)),
            cache_ttl_hours=cache_ttl,
            supported_exchanges=self.venue_mapping.all_tardis_exchanges,
        )

        # Initialize metadata cache
        self._metadata_cache: dict[str, InstrumentDefinition] = {}
        self._cache_timestamps: dict[str, datetime] = {}

        logger.info(
            "✅ InstrumentProcessingBase initialized: api_key=%s, ccxt_integration=%s, caching=%s",
            "*" * (len(self.api_key) - 4) + self.api_key[-4:] if self.api_key else "None",
            self.processing_config.enable_ccxt_integration,
            self.processing_config.enable_metadata_caching,
        )

    def get_venue_mapping(self) -> dict[str, str]:
        """
        Get canonical venue mapping.

        Replaces scattered venue mapping logic.
        """
        return self.venue_mapping.tardis_to_venue

    def get_instrument_type_mapping(self) -> dict[str, str]:
        """
        Get canonical instrument type mapping.

        Replaces scattered instrument type logic.
        """
        return {
            "spot": "SPOT_PAIR",
            "perpetual": "PERPETUAL",
            "future": "FUTURE",
            "option": "OPTION",
            "combo": "OPTION",  # Deribit combos are often options
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
