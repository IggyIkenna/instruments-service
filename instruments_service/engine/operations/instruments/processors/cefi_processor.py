"""
CeFi Instrument Processor

Handles CeFi (Centralized Finance) instrument processing:
- Tardis API integration for exchange data
- CCXT market data fetching
- Exchange-specific filtering and validation
- Canonical key generation
- Binance-specific filtering for problematic instruments
"""

from __future__ import annotations

import asyncio
import logging
import re
import warnings
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import TardisAdapter
from unified_trading_library import (
    determine_market_category,
    get_secret_client,
)

from instruments_service.config import instruments_config
from instruments_service.engine.operations.instruments.processors.base_processor import BaseInstrumentProcessor
from instruments_service.engine.processors.canonical_key_generator import (
    SymbolInfo,
)
from instruments_service.engine.processors.canonical_key_generator import (
    generate_canonical_key as _generate_canonical_key,
)
from instruments_service.engine.processors.derived_fields_populator import populate_derived_fields
from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class CeFiInstrumentProcessor(BaseInstrumentProcessor):
    """
    Processor for CeFi (Centralized Finance) instruments.

    Handles:
    - Tardis API integration for exchange data
    - CCXT market data enrichment
    - Exchange-specific filtering (Binance, Deribit, etc.)
    - Canonical key generation
    - Date-based instrument availability filtering
    """

    def __init__(self, config: dict[str, object]):
        """
        Initialize CeFi processor.

        Args:
            config: Configuration with CeFi processing settings
        """
        super().__init__(config)

        self.tardis_adapter: TardisAdapter | None = None
        self._tardis_project_id = self.project_id

        logger.info("✅ CeFiInstrumentProcessor initialized")

    @property
    def api_key(self) -> str | None:
        """Get Tardis API key."""
        return self.processing_config.api_key if self.processing_config.api_key else None

    def _get_tardis_adapter(self) -> TardisAdapter:
        """
        Lazy-load Tardis adapter only when needed for CeFi instruments.

        Returns:
            TardisAdapter instance

        Raises:
            ValueError: If API key is not available
        """
        if self.tardis_adapter is None:
            api_key = self.processing_config.api_key

            if not api_key:
                try:
                    secret_name = instruments_config.tardis_secret_name
                    logger.debug(
                        "Attempting to retrieve Tardis API key from Secret Manager (secret: %s, project: %s)",
                        secret_name,
                        self._tardis_project_id,
                    )
                    api_key = get_secret_client(
                        project_id=self._tardis_project_id,
                    ).get_secret(secret_name)
                    if api_key:
                        api_key = api_key.strip()
                        logger.info("✅ Retrieved Tardis API key from Secret Manager")
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
                    raise ValueError(
                        f"Tardis API key required for CeFi instruments. Error: {e}. "
                        "Provide 'tardis_api_key' in config or ensure Secret Manager access."
                    ) from e
            if not api_key:
                raise ValueError(
                    "Tardis API key required for CeFi instruments. "
                    "Provide 'tardis_api_key' in config or ensure Secret Manager access."
                )

            self.tardis_adapter = TardisAdapter(api_key=api_key, project_id=self._tardis_project_id)

        return self.tardis_adapter

    def generate_canonical_key(
        self,
        exchange: str,
        symbol_type: str,
        symbol_id: str,
        symbol_info: dict[str, object],
    ) -> str | None:
        """Generate canonical instrument key following INSTRUMENT_KEY.md specification."""
        return _generate_canonical_key(
            self, exchange, symbol_type, symbol_id, cast(SymbolInfo, cast(object, symbol_info))
        )

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
            force: If True, bypass date filtering and get ALL current instruments

        Returns:
            Tuple of (instruments_data dict, date_filtered_count)

        Raises:
            Exception: If API fetch fails after retries
        """
        target_date = target_date or datetime.now(UTC)
        date_str = target_date.strftime("%Y-%m-%d")

        # Manual retry logic for proper type safety (instead of decorator)
        max_retries: int = self.processing_config.retry_max_attempts
        last_error: Exception | None = None
        available_symbols_list: list[dict[str, object]] = []
        date_filtered_count: int = 0

        for attempt in range(max_retries):
            try:
                available_symbols_list, date_filtered_count = await asyncio.to_thread(
                    self._get_tardis_adapter().fetch_exchange_instruments,
                    exchange=exchange,
                    target_date=target_date,
                    force_refresh=force,
                )
                break
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
                last_error = e
                if attempt < max_retries - 1:
                    backoff_multiplier: int = cast(int, 2**attempt)
                    backoff: float = self.processing_config.retry_backoff_factor * float(backoff_multiplier)
                    logger.warning(
                        "⚠️ Attempt %s/%s failed for %s: %s. Retrying in %ss...",
                        attempt + 1,
                        max_retries,
                        exchange,
                        e,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error("❌ Failed to fetch %s after %s attempts: %s", exchange, max_retries, e)
                    raise Exception(
                        f"Failed to fetch instruments for {exchange} after {max_retries} retries"
                    ) from last_error
        available_symbols: dict[str, dict[str, object]] = {
            (str(cast(str | None, symbol.get("id"))) or ""): symbol
            for symbol in available_symbols_list
            if symbol.get("id")
        }

        instruments_data: dict[str, dict[str, object]] = {}
        for symbol_id, symbol in available_symbols.items():
            if not symbol_id:
                continue
            symbol_type = symbol.get("type") or ""

            if symbol_type in self.data_config.excluded_instrument_types:
                logger.debug("Filtered excluded instrument type: %s (%s)", symbol_id, symbol_type)
                continue

            if exchange in ["binance", "binance-futures"] and self._is_problematic_binance_instrument(symbol_id):
                logger.debug("Filtered problematic Binance instrument: %s", symbol_id)
                continue

            instruments_data[symbol_id] = symbol

        if target_date:
            logger.info("✅ Filtered to %s instruments available on %s", len(instruments_data), date_str)
        else:
            logger.info("✅ Retrieved %s currently active instruments", len(instruments_data))

        return instruments_data, date_filtered_count

    async def process_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process all instruments for an exchange and generate canonical keys.

        Args:
            exchange: Exchange name
            target_date: Target date for processing
            force: If True, force regeneration

        Returns:
            Dictionary of processed instruments keyed by canonical key
        """
        warnings.warn(
            "process_exchange_instruments is deprecated. CeFi path uses UMI get_adapter('tardis').fetch_instruments. "
            "Use InstrumentsService.generate_instruments_for_date(cefi=True) or UMI directly.",
            DeprecationWarning,
            stacklevel=2,
        )

        instruments_data: dict[str, dict[str, object]]
        date_filtered_count: int
        instruments_data, date_filtered_count = await self.fetch_exchange_instruments(exchange, target_date, force)

        canonical_venue = self.normalize_venue(exchange) or exchange
        valid_types_raw = self.exchange_config.exchange_instrument_types.get(canonical_venue)
        if valid_types_raw is None:
            raise ValueError(f"exchange_instrument_types must have entry for venue {canonical_venue}. Add to config.")
        if not isinstance(valid_types_raw, list):
            raise TypeError(
                f"exchange_instrument_types[{canonical_venue}] must be list, got {type(valid_types_raw).__name__}"
            )
        valid_types: list[str] = valid_types_raw

        valid_quotes_raw = self.exchange_config.valid_quote_currencies.get(canonical_venue)
        if valid_quotes_raw is None:
            raise ValueError(f"valid_quote_currencies must have entry for venue {canonical_venue}. Add to config.")
        if not isinstance(valid_quotes_raw, list):
            raise TypeError(
                f"valid_quote_currencies[{canonical_venue}] must be list, got {type(valid_quotes_raw).__name__}"
            )
        valid_quotes: list[str] = valid_quotes_raw

        logger.info(
            "🔍 Pre-filtering by exchange config: %s accepts types=%s, quotes=%s",
            canonical_venue,
            valid_types,
            valid_quotes,
        )

        excluded_bases_raw = self.exchange_config.excluded_base_currencies.get(canonical_venue)
        if excluded_bases_raw is None:
            excluded_bases = []
        else:
            if not isinstance(excluded_bases_raw, list):
                raise TypeError(
                    f"excluded_base_currencies[{canonical_venue}] must be list, got {type(excluded_bases_raw).__name__}"
                )
            excluded_bases = excluded_bases_raw

        excluded_patterns_raw = self.exchange_config.excluded_symbol_patterns.get(canonical_venue)
        if excluded_patterns_raw is None:
            excluded_patterns = []
        else:
            if not isinstance(excluded_patterns_raw, list):
                raise TypeError(
                    f"excluded_symbol_patterns[{canonical_venue}] must be list, "
                    f"got {type(excluded_patterns_raw).__name__}"
                )
            excluded_patterns = excluded_patterns_raw

        pre_filtered: dict[str, dict[str, object]] = {}
        for symbol_id, symbol_info in instruments_data.items():
            symbol_type: str = (cast(str | None, symbol_info.get("type")) or "").lower()
            normalized_type = self.normalize_instrument_type(symbol_type)

            if normalized_type not in valid_types:
                continue

            if excluded_patterns:
                symbol_upper: str = symbol_id.upper()
                excluded_by_pattern = False
                for pattern in excluded_patterns:
                    if pattern.upper() in symbol_upper:
                        logger.debug("🚫 Pre-filtered out %s: pattern '%s' excluded", symbol_id, pattern)
                        excluded_by_pattern = True
                        break
                if excluded_by_pattern:
                    continue

            parsed_components = self.parse_symbol_components(symbol_id, exchange)
            base_asset = (parsed_components.get("base_asset") or "").upper()
            quote_asset = (parsed_components.get("quote_asset") or "").upper()

            if base_asset and base_asset in excluded_bases:
                logger.debug("🚫 Pre-filtered out %s: base '%s' excluded", symbol_id, base_asset)
                continue

            if quote_asset and quote_asset not in valid_quotes:
                continue

            pre_filtered[symbol_id] = symbol_info

        logger.info("🔍 Exchange config filter: %s/%s instruments valid", len(pre_filtered), len(instruments_data))
        instruments_data = pre_filtered

        if canonical_venue in self.venue_mapping.spot_mvp_filtered_venues:
            mvp_base_assets = {b.upper() for b in self.venue_mapping.hyperliquid_aster_mvp_base_assets}
            mvp_filtered: dict[str, dict[str, object]] = {}
            for symbol_id, symbol_info in instruments_data.items():
                parsed_components = self.parse_symbol_components(symbol_id, exchange)
                base_asset = (parsed_components.get("base_asset") or "").upper()

                if base_asset in mvp_base_assets:
                    mvp_filtered[symbol_id] = symbol_info
                else:
                    logger.debug("🚫 MVP filter: %s excluded (base '%s' not in MVP list)", symbol_id, base_asset)

            logger.info("🔍 MVP filter: %s/%s instruments", len(mvp_filtered), len(instruments_data))
            instruments_data = mvp_filtered

        filter_stats: dict[str, int] = {
            "no_canonical_key": 0,
            "same_base_quote": 0,
            "date_filtered": date_filtered_count,
            "expired_filtered": 0,
            "expiry_filtered": 0,
            "processing_error": 0,
            "success": 0,
        }

        processed_instruments: dict[str, InstrumentDefinition] = {}
        for symbol_id, symbol_info in instruments_data.items():
            try:
                canonical_key = self.generate_canonical_key(
                    exchange=exchange,
                    symbol_type=cast(str, symbol_info.get("type") or ""),
                    symbol_id=symbol_id,
                    symbol_info=symbol_info,
                )

                if not canonical_key:
                    filter_stats["no_canonical_key"] += 1
                    if exchange == "deribit":
                        logger.debug("🚫 No canonical key for %s", symbol_id)
                    continue

                parsed_components = self.parse_symbol_components(symbol_id, exchange)
                base_asset = parsed_components.get("base_asset") or ""
                quote_asset = parsed_components.get("quote_asset") or ""

                clean_base = str(base_asset or "").upper() if base_asset is not None else ""
                clean_quote = str(quote_asset or "").upper() if quote_asset is not None else ""

                if clean_base == clean_quote and clean_base:
                    filter_stats["same_base_quote"] += 1
                    logger.debug("🚫 Filtered: %s (%s-%s - same asset)", symbol_id, clean_base, clean_quote)
                    continue

                normalized_instrument_type = self.normalize_instrument_type(cast(str, symbol_info.get("type") or ""))

                settle_asset = "USDT"
                canonical_venue_raw = self.normalize_venue(exchange)
                canonical_venue = canonical_venue_raw or exchange
                if canonical_venue == "DERIBIT":
                    deribit_quotes_raw = self.exchange_config.valid_quote_currencies.get("DERIBIT")
                    if deribit_quotes_raw is None:
                        raise ValueError("valid_quote_currencies must have entry for DERIBIT. Add to config.")
                    if not isinstance(deribit_quotes_raw, list):
                        raise TypeError(
                            f"valid_quote_currencies[DERIBIT] must be list, got {type(deribit_quotes_raw).__name__}"
                        )
                    deribit_quotes = deribit_quotes_raw
                    if clean_quote == "USD":
                        settle_asset = "USDC" if "USDC" in deribit_quotes else "USDT"
                    elif clean_quote in deribit_quotes:
                        settle_asset = clean_quote

                # Protocol variance: CeFiInstrumentProcessor has concrete types (VenueMapping, ExchangeInstrumentConfig)
                # but DerivedFieldsServiceProtocol expects structural subtype — cast through object to satisfy checker.
                from instruments_service.engine.processors.derived_fields_populator import (
                    DerivedFieldsServiceProtocol as _DFSProtocol,
                )

                enhanced_fields = await populate_derived_fields(
                    service=cast(_DFSProtocol, cast(object, self)),
                    canonical_key=canonical_key,
                    venue=canonical_venue,
                    inst_type=normalized_instrument_type or "SPOT_PAIR",
                    base_asset=clean_base,
                    quote_asset=clean_quote,
                    symbol_id=symbol_id,
                    exchange=exchange,
                )

                symbol = symbol_id
                normalized_instrument_type = normalized_instrument_type or "SPOT_PAIR"
                data_types_list = self.data_config.instrument_data_types.get(normalized_instrument_type, ["trades"])
                data_types_str = ",".join(data_types_list)

                # Use venue_to_data_provider from VenueMapping (not venue_to_data_provider_mapping)
                tardis_exchange = exchange.lower()  # Default to exchange name
                if hasattr(self.venue_mapping, "venue_to_data_provider"):
                    tardis_exchange = self.venue_mapping.venue_to_data_provider.get(canonical_venue, exchange.lower())

                available_to: str | None = cast(str | None, symbol_info.get("availableTo"))
                available_to_datetime = None

                if available_to:
                    try:
                        if isinstance(available_to, str):
                            if available_to.endswith("Z"):
                                available_to_datetime = available_to
                            else:
                                available_to_datetime = (
                                    available_to.replace("Z", "+00:00") if "+" not in available_to else available_to
                                )
                        else:
                            available_to_datetime = str(available_to)
                    except (ValueError, TypeError) as e:
                        logger.debug("⚠️ Could not parse availableTo '%s': %s", available_to, e)

                if (
                    not available_to_datetime
                    and normalized_instrument_type in ["FUTURE", "OPTION"]
                    and "expiry" in enhanced_fields
                ):
                    expiry_str: str = cast(str, enhanced_fields.get("expiry") or "")
                    if expiry_str:
                        try:
                            expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                            expiry_date = expiry_dt.date()
                            day_after_expiry = expiry_date + timedelta(days=1)
                            available_to_datetime = (
                                datetime.combine(day_after_expiry, datetime.min.time()).replace(tzinfo=UTC).isoformat()
                            )
                            logger.debug("✅ Set available_to for %s to midnight after expiry", symbol_id)
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
                if available_to_datetime:
                    try:
                        available_to_dt = datetime.fromisoformat(available_to_datetime.replace("Z", "+00:00"))
                        if available_to_dt.tzinfo is None:
                            available_to_dt = available_to_dt.replace(tzinfo=UTC)

                        comparison_date = target_date if target_date else datetime.now(UTC)
                        if comparison_date.tzinfo is None:
                            comparison_date = comparison_date.replace(tzinfo=UTC)

                        if comparison_date.date() > available_to_dt.date():
                            filter_stats["expired_filtered"] += 1
                            logger.debug("🚫 Filtered expired: %s", symbol_id)
                            continue
                    except (ValueError, TypeError) as e:
                        logger.debug("⚠️ Could not parse available_to '%s': %s", available_to_datetime, e)

                if target_date and normalized_instrument_type in ["FUTURE", "OPTION"] and "expiry" in enhanced_fields:
                    expiry_str_filter: str = cast(str, enhanced_fields.get("expiry") or "")
                    if expiry_str_filter:
                        try:
                            expiry_dt = datetime.fromisoformat(expiry_str_filter.replace("Z", "+00:00"))
                            target_date_only = target_date.date() if hasattr(target_date, "date") else target_date
                            expiry_date_only = expiry_dt.date()

                            if target_date_only > expiry_date_only:
                                filter_stats["expiry_filtered"] += 1
                                logger.debug("🚫 Filtered: %s - expiry < target_date", symbol_id)
                                continue
                        except (ValueError, TypeError) as e:
                            logger.debug("⚠️ Could not parse expiry '%s': %s", expiry_str_filter, e)

                instrument_dict: dict[str, object] = {
                    "databento_symbol": "",
                    "chain": "off-chain",
                }
                market_category = determine_market_category(instrument_dict)

                venue_str: str = canonical_venue
                inst_type_str: str = normalized_instrument_type or "SPOT_PAIR"
                base_fields: dict[str, object] = {
                    "instrument_key": canonical_key,
                    "venue": venue_str,
                    "instrument_type": inst_type_str,
                    "symbol": symbol,
                    "base_asset": clean_base,
                    "quote_asset": clean_quote,
                    "settle_asset": settle_asset,
                    "chain": "off-chain",
                    "market_category": market_category,
                    "exchange_raw_symbol": symbol_id,
                    "tardis_symbol": self._convert_to_tardis_symbol(symbol_id, exchange),
                    "tardis_exchange": tardis_exchange,
                    "available_from_datetime": symbol_info.get("availableSince") or "",
                    "available_to_datetime": available_to_datetime,
                    "data_types": data_types_str,
                }
                metadata = InstrumentDefinition.model_validate({**base_fields, **enhanced_fields})

                processed_instruments[canonical_key] = metadata
                filter_stats["success"] += 1
                self.cache_metadata(canonical_key, metadata)

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
                filter_stats["processing_error"] += 1
                logger.warning("⚠️ Failed to process %s: %s", symbol_id, e)
        total_filtered = sum(v for k, v in filter_stats.items() if k != "success")
        logger.info("📊 Processed %s instruments from %s", len(processed_instruments), exchange)
        if total_filtered > 0:
            logger.info(
                "🔍 Filtering: no_key=%s, same_base_quote=%s, date_filtered=%s, expired=%s, expiry=%s, errors=%s, success=%s",
                filter_stats["no_canonical_key"],
                filter_stats["same_base_quote"],
                filter_stats["date_filtered"],
                filter_stats.get("expired_filtered", 0),
                filter_stats["expiry_filtered"],
                filter_stats["processing_error"],
                filter_stats["success"],
            )

        return processed_instruments

    def filter_instruments_by_exchange_config(
        self, instruments: dict[str, dict[str, object]], exchange: str
    ) -> dict[str, dict[str, object]]:
        """
        Filter instruments by exchange-specific capabilities.

        Args:
            instruments: Dictionary of instruments to filter
            exchange: Exchange name

        Returns:
            Filtered instruments dictionary
        """
        canonical_venue: str = self.normalize_venue(exchange) or exchange
        valid_types_raw = self.exchange_config.exchange_instrument_types.get(canonical_venue)
        if valid_types_raw is None:
            raise ValueError(f"exchange_instrument_types must have entry for venue {canonical_venue}. Add to config.")
        if not isinstance(valid_types_raw, list):
            raise TypeError(
                f"exchange_instrument_types[{canonical_venue}] must be list, got {type(valid_types_raw).__name__}"
            )
        valid_types: list[str] = valid_types_raw

        valid_quotes_raw = self.exchange_config.valid_quote_currencies.get(canonical_venue)
        if valid_quotes_raw is None:
            raise ValueError(f"valid_quote_currencies must have entry for venue {canonical_venue}. Add to config.")
        if not isinstance(valid_quotes_raw, list):
            raise TypeError(
                f"valid_quote_currencies[{canonical_venue}] must be list, got {type(valid_quotes_raw).__name__}"
            )
        valid_quotes: list[str] = valid_quotes_raw

        is_derivative = canonical_venue in self.exchange_config.derivative_exchanges

        excluded_bases_raw = self.exchange_config.excluded_base_currencies.get(canonical_venue)
        if excluded_bases_raw is None:
            excluded_bases = []
        else:
            if not isinstance(excluded_bases_raw, list):
                raise TypeError(
                    f"excluded_base_currencies[{canonical_venue}] must be list, got {type(excluded_bases_raw).__name__}"
                )
            excluded_bases = excluded_bases_raw

        excluded_patterns_raw = self.exchange_config.excluded_symbol_patterns.get(canonical_venue)
        if excluded_patterns_raw is None:
            excluded_patterns = []
        else:
            if not isinstance(excluded_patterns_raw, list):
                raise TypeError(
                    f"excluded_symbol_patterns[{canonical_venue}] must be list, "
                    f"got {type(excluded_patterns_raw).__name__}"
                )
            excluded_patterns = excluded_patterns_raw

        filtered: dict[str, dict[str, object]] = {}

        for inst_key, inst_data in instruments.items():
            try:
                inst_type: str = cast(str, inst_data.get("instrument_type") or "")
                if inst_type not in valid_types:
                    logger.debug("❌ Filtered %s: %s not valid for %s", inst_key, inst_type, canonical_venue)
                    continue

                quote_asset: str = (cast(str | None, inst_data.get("quote_asset")) or "").upper()
                if quote_asset not in valid_quotes:
                    logger.debug("🚫 Filtered %s: quote '%s' not in %s", inst_key, quote_asset, valid_quotes)
                    continue

                base_asset: str = (cast(str | None, inst_data.get("base_asset")) or "").upper()
                if base_asset in excluded_bases:
                    logger.debug("🚫 Filtered %s: base '%s' excluded", inst_key, base_asset)
                    continue

                symbol: str = (cast(str | None, inst_data.get("symbol")) or "").upper()
                if excluded_patterns:
                    excluded_by_pattern = False
                    for pattern in excluded_patterns:
                        if pattern.upper() in symbol:
                            logger.debug("🚫 Filtered %s: pattern '%s' excluded", inst_key, pattern)
                            excluded_by_pattern = True
                            break
                    if excluded_by_pattern:
                        continue

                inst_data = self._populate_complete_instrument_data(inst_data, exchange, is_derivative)
                filtered[inst_key] = inst_data

            except (ConnectionError, TimeoutError, ValueError, KeyError, TypeError) as e:
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
        return filtered

    def _populate_complete_instrument_data(
        self, inst_data: dict[str, object], exchange: str, is_derivative: bool
    ) -> dict[str, object]:
        """Populate all InstrumentDefinition fields per models.py schema."""
        inst_data.setdefault("tardis_exchange", exchange.lower())
        inst_data.setdefault("data_provider", "tardis")
        inst_data.setdefault("asset_class", "crypto")

        if is_derivative and inst_data.get("instrument_type") in ["PERPETUAL", "FUTURE", "OPTION"]:
            inst_data["venue_type"] = "derivatives"
            base_asset: str = cast(str, inst_data.get("base_asset") or "")
            quote_asset: str = cast(str, inst_data.get("quote_asset") or "")
            if base_asset and quote_asset:
                inst_data["underlying"] = f"{base_asset}-{quote_asset}"
        else:
            inst_data["venue_type"] = "spot"
            inst_data["underlying"] = ""

        inst_type: str = cast(str, inst_data.get("instrument_type", "SPOT_PAIR"))
        inst_data["data_types"] = ",".join(
            self.data_config.instrument_data_types.get(inst_type, ["trades", "book_snapshot_5"])
        )

        return inst_data

    def _convert_to_tardis_symbol(self, symbol_id: str, exchange: str) -> str:
        """
        Convert symbol_id to proper Tardis API format.

        Args:
            symbol_id: Raw symbol from Tardis API
            exchange: Exchange name

        Returns:
            Tardis-formatted symbol
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

    def _is_problematic_binance_instrument(self, symbol_id: str) -> bool:
        """
        Check if this is a problematic Binance instrument that should be filtered out.

        Args:
            symbol_id: Symbol identifier from Tardis

        Returns:
            True if instrument should be filtered
        """
        symbol_lower = symbol_id.lower()

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

        if re.match(r"^(1000|1000000|1m)", symbol_lower):
            return True

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

        if symbol_lower.startswith(("1inch", "0g", "2z", "3p", "4p", "5p")):
            return True

        problematic_patterns = ["nftusdt", "defiusdt", "bullusdt", "bearusdt"]
        return symbol_lower in problematic_patterns

    def cleanup(self) -> None:
        """Cleanup CeFi processor resources."""
        super().cleanup()
        if self.tardis_adapter:
            logger.info("🧹 Cleaning up Tardis adapter")
