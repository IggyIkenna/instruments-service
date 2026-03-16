"""
Instrument Processing Handlers

High-level processing workflows for different market types and operations.
Provides complete processing pipelines.
"""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from uuid import uuid4

from unified_config_interface import DataTypeConfig, ExchangeInstrumentConfig, VenueMapping
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import SubgraphService, TardisAdapter
from unified_trading_library import determine_market_category  # noqa: domain-ucs — not yet in UDC

from instruments_service.app.core.instrument_processing_base import InstrumentProcessingConfig
from instruments_service.models import InstrumentDefinition
from instruments_service.utils.ccxt_service import CCXTService

logger = logging.getLogger(__name__)


class _ClearCacheable(Protocol):
    """Protocol for objects that expose a clear_cache() method."""

    def clear_cache(self) -> None: ...


class InstrumentProcessingHandlers:
    """
    High-level processing handlers for instrument workflows.

    Combines base functionality with mixins to provide complete
    processing pipelines for different market types.
    """

    # ---------------------------------------------------------------------------
    # Attribute declarations for attributes provided by InstrumentProcessingBase
    # and the integration mixins.  These declarations allow basedpyright to
    # resolve attribute access when checking this class in isolation, without
    # creating actual assignments that would interfere with the MRO.
    # ---------------------------------------------------------------------------
    exchange_config: ExchangeInstrumentConfig = cast(
        ExchangeInstrumentConfig, cast(object, None)
    )  # InstrumentProcessingBase
    venue_mapping: VenueMapping = cast(VenueMapping, cast(object, None))  # InstrumentProcessingBase
    processing_config: InstrumentProcessingConfig = cast(
        InstrumentProcessingConfig, cast(object, None)
    )  # InstrumentProcessingBase
    data_config: DataTypeConfig = cast(DataTypeConfig, cast(object, None))  # InstrumentProcessingBase
    tardis_adapter: TardisAdapter | None = None  # TardisIntegrationMixin
    ccxt_service: CCXTService | None = None  # CCXTIntegrationMixin
    subgraph_service: SubgraphService = cast(SubgraphService, cast(object, None))  # DefiIntegrationMixin
    _metadata_cache: dict[str, InstrumentDefinition] = cast(
        dict[str, InstrumentDefinition], cast(object, None)
    )  # InstrumentProcessingBase
    _cache_timestamps: dict[str, datetime] = cast(dict[str, datetime], cast(object, None))  # InstrumentProcessingBase

    # Method stubs for methods from other mixins/base.
    # Concrete implementations come via MRO in InstrumentProcessingService.
    async def fetch_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> tuple[dict[str, dict[str, object]], int] | None: ...  # type: ignore[override]  # UTL-DEC-02: decorated mixin returns Coroutine|None; stub must match callers

    def normalize_venue(self, exchange: str) -> str | None: ...

    def normalize_instrument_type(self, symbol_type: str) -> str | None: ...

    def parse_symbol_components(self, symbol_id: str, exchange: str) -> dict[str, object]: ...

    def parse_option_components(self, symbol_id: str, exchange: str) -> dict[str, object]: ...

    def generate_canonical_key(
        self,
        exchange: str,
        symbol_type: str,
        symbol_id: str,
        symbol_info: dict[str, object],
    ) -> str | None: ...

    async def _populate_all_derived_fields(
        self,
        canonical_key: str,
        venue: str,
        inst_type: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        exchange: str | None = None,
    ) -> dict[str, object]: ...

    def _convert_to_tardis_symbol(self, symbol_id: str, exchange: str) -> str: ...

    def cache_metadata(self, instrument_key: str, metadata: InstrumentDefinition) -> None: ...

    def _pre_filter_by_exchange_config(
        self,
        instruments_data: dict[str, dict[str, object]],
        exchange: str,
        canonical_venue: str,
    ) -> dict[str, dict[str, object]]:
        """Pre-filter raw instrument data by exchange config (types, quotes, exclusions)."""
        valid_types: list[str] = self.exchange_config.exchange_instrument_types.get(canonical_venue) or []
        valid_quotes: list[str] = self.exchange_config.valid_quote_currencies.get(canonical_venue) or ["USDT"]
        excluded_bases: list[str] = self.exchange_config.excluded_base_currencies.get(canonical_venue) or []
        excluded_patterns: list[str] = self.exchange_config.excluded_symbol_patterns.get(canonical_venue) or []

        logger.info(
            "Pre-filtering by exchange config: %s accepts types=%s, quotes=%s",
            canonical_venue,
            valid_types,
            valid_quotes,
        )

        pre_filtered: dict[str, dict[str, object]] = {}
        for symbol_id, symbol_info in instruments_data.items():
            symbol_type: str = (cast(str | None, symbol_info.get("type")) or "").lower()
            if self.normalize_instrument_type(symbol_type) not in valid_types:
                continue

            if excluded_patterns:
                symbol_upper: str = symbol_id.upper()
                if any(p.upper() in symbol_upper for p in excluded_patterns):
                    logger.debug("Pre-filtered out %s: excluded pattern match", symbol_id)
                    continue

            parsed = self.parse_symbol_components(symbol_id, exchange)
            if isinstance(parsed, dict):
                base_asset = (str(parsed.get("base_asset") or "")).upper()
                quote_asset = (str(parsed.get("quote_asset") or "")).upper()
            else:
                b, q = parsed if parsed else ("", "")
                base_asset = (b or "").upper()
                quote_asset = (q or "").upper()

            if base_asset and base_asset in excluded_bases:
                logger.debug("Pre-filtered out %s: base '%s' excluded", symbol_id, base_asset)
                continue
            if quote_asset and quote_asset not in valid_quotes:
                continue

            pre_filtered[symbol_id] = symbol_info

        logger.info(
            "Exchange config filter: %s/%s valid for %s",
            len(pre_filtered),
            len(instruments_data),
            canonical_venue,
        )
        return pre_filtered

    def _apply_mvp_filter(
        self,
        instruments_data: dict[str, dict[str, object]],
        exchange: str,
        canonical_venue: str,
    ) -> dict[str, dict[str, object]]:
        """Apply MVP base-asset allowlist filter for specific venues."""
        mvp_base_assets = {b.upper() for b in self.venue_mapping.hyperliquid_aster_mvp_base_assets}
        mvp_filtered: dict[str, dict[str, object]] = {}
        for symbol_id, symbol_info in instruments_data.items():
            parsed = self.parse_symbol_components(symbol_id, exchange)
            if isinstance(parsed, dict):
                base_asset = (parsed.get("base_asset") or "").upper()
            else:
                b, _ = parsed if parsed else ("", "")
                base_asset = (b or "").upper()
            if base_asset in mvp_base_assets:
                mvp_filtered[symbol_id] = symbol_info
            else:
                logger.debug("MVP filter: %s excluded (base '%s' not in MVP list)", symbol_id, base_asset)
        logger.info("MVP filter: %s/%s for %s", len(mvp_filtered), len(instruments_data), canonical_venue)
        return mvp_filtered

    def _infer_settle_asset(
        self,
        clean_base: str,
        clean_quote: str,
        canonical_venue: str,
    ) -> str:
        """Infer the settlement asset for a symbol at a given venue."""
        settle_asset = "USDT"
        if canonical_venue == "DERIBIT":
            deribit_quotes: list[str] = self.exchange_config.valid_quote_currencies.get("DERIBIT") or []
            if clean_quote == "USD":
                settle_asset = clean_base
            elif clean_quote in deribit_quotes and clean_quote != "USD":
                settle_asset = clean_quote
        return settle_asset

    def _resolve_available_to(
        self,
        symbol_info: dict[str, object],
        normalized_instrument_type: str | None,
        enhanced_fields: dict[str, object],
    ) -> str | None:
        """Resolve the available_to_datetime string from symbol info or expiry."""
        available_to: str | None = cast(str | None, symbol_info.get("availableTo"))
        available_to_datetime: str | None = None

        if available_to:
            try:
                if available_to.endswith("Z"):
                    available_to_datetime = available_to
                else:
                    available_to_datetime = (
                        available_to.replace("Z", "+00:00") if "+" not in available_to else available_to
                    )
            except (ValueError, TypeError) as e:
                logger.debug("Could not parse availableTo '%s': %s", available_to, e)

        if (
            not available_to_datetime
            and normalized_instrument_type in ["FUTURE", "OPTION"]
            and "expiry" in enhanced_fields
        ):
            expiry_str: str = cast(str, enhanced_fields.get("expiry") or "")
            if expiry_str:
                try:
                    expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                    day_after_expiry = expiry_dt.date() + timedelta(days=1)
                    available_to_datetime = (
                        datetime.combine(day_after_expiry, datetime.min.time()).replace(tzinfo=UTC).isoformat()
                    )
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

        return available_to_datetime

    def _is_expired(
        self,
        available_to_datetime: str | None,
        target_date: datetime | None,
    ) -> bool:
        """Return True if the instrument has expired relative to target_date."""
        if not available_to_datetime:
            return False
        try:
            available_to_dt = datetime.fromisoformat(available_to_datetime.replace("Z", "+00:00"))
            if available_to_dt.tzinfo is None:
                available_to_dt = available_to_dt.replace(tzinfo=UTC)
            comparison_date = target_date if target_date else datetime.now(UTC)
            if comparison_date.tzinfo is None:
                comparison_date = comparison_date.replace(tzinfo=UTC)
            return comparison_date.date() > available_to_dt.date()
        except (ValueError, TypeError) as e:
            logger.debug("Could not parse available_to_datetime '%s': %s", available_to_datetime, e)
            return False

    def _is_past_expiry(
        self,
        target_date: datetime | None,
        normalized_instrument_type: str | None,
        enhanced_fields: dict[str, object],
    ) -> bool:
        """Return True if the instrument's expiry date is before the target_date."""
        if not (target_date and normalized_instrument_type in ["FUTURE", "OPTION"] and "expiry" in enhanced_fields):
            return False
        expiry_str: str = cast(str, enhanced_fields.get("expiry") or "")
        if not expiry_str:
            return False
        try:
            expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
            target_date_only = target_date.date() if hasattr(target_date, "date") else target_date
            return target_date_only > expiry_dt.date()  # type: ignore[operator]
        except (ValueError, TypeError) as e:
            logger.debug("Could not parse expiry '%s': %s", expiry_str, e)
            return False

    async def _build_instrument_definition(
        self,
        symbol_id: str,
        symbol_info: dict[str, object],
        exchange: str,
        canonical_venue: str,
        target_date: datetime | None,
    ) -> InstrumentDefinition | None:
        """Build an InstrumentDefinition for a single symbol, or return None to skip it."""
        canonical_key = self.generate_canonical_key(
            exchange=exchange,
            symbol_type=cast(str, symbol_info.get("type") or ""),
            symbol_id=symbol_id,
            symbol_info=symbol_info,
        )
        if not canonical_key:
            return None

        parsed: dict[str, object] = self.parse_symbol_components(symbol_id, exchange)
        if isinstance(parsed, dict):
            base_asset = cast(str, parsed.get("base_asset") or "")
            quote_asset = cast(str, parsed.get("quote_asset") or "")
        else:
            base_asset, quote_asset = parsed if parsed else ("", "")

        clean_base = str(base_asset or "").upper() if base_asset is not None else ""
        clean_quote = str(quote_asset or "").upper() if quote_asset is not None else ""

        if clean_base == clean_quote and clean_base:
            return None  # nonsensical pair

        _raw_type: object = symbol_info.get("type")
        _type_str: str = str(_raw_type) if _raw_type else ""
        normalized_instrument_type = self.normalize_instrument_type(_type_str)
        norm_inst_type: str = normalized_instrument_type or ""

        enhanced_fields: dict[str, object] = await self._populate_all_derived_fields(
            canonical_key,
            canonical_venue,
            norm_inst_type,
            clean_base,
            clean_quote,
            symbol_id,
            exchange,
        )

        available_to_datetime = self._resolve_available_to(symbol_info, normalized_instrument_type, enhanced_fields)

        if self._is_expired(available_to_datetime, target_date):
            return None
        if self._is_past_expiry(target_date, normalized_instrument_type, enhanced_fields):
            return None

        settle_asset = self._infer_settle_asset(clean_base, clean_quote, canonical_venue)
        symbol: str = canonical_key.split(":", 2)[2] if len(canonical_key.split(":")) >= 3 else symbol_id

        inst_type_str: str = normalized_instrument_type or "SPOT_PAIR"
        config_data_types = self.data_config.instrument_data_types.get(inst_type_str, ["trades", "book_snapshot_5"])
        tardis_exchange: str = self.venue_mapping.venue_instrument_type_to_tardis.get(
            (canonical_venue, inst_type_str), exchange.lower()
        )

        market_category = determine_market_category({"databento_symbol": "", "chain": "off-chain"})

        inst_data: dict[str, object] = dict(enhanced_fields)
        inst_data.update(
            {
                "instrument_key": canonical_key,
                "venue": canonical_venue,
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
                "available_from_datetime": cast(str, symbol_info.get("availableSince") or ""),
                "available_to_datetime": available_to_datetime,
                "data_types": ",".join(config_data_types),
            }
        )
        return InstrumentDefinition.model_validate(inst_data)

    async def process_exchange_instruments(
        self,
        exchange: str,
        target_date: datetime | None = None,
        force: bool = False,
    ) -> dict[str, InstrumentDefinition]:
        """
        Process all instruments for an exchange and generate canonical keys.

        .. deprecated::
            CeFi path now uses UMI. This method will be removed in a future release.

        Args:
            exchange: Exchange name
            target_date: Target date for processing
            force: If True, force regeneration

        Returns:
            Dictionary of processed instrument metadata
        """
        warnings.warn(
            "process_exchange_instruments is deprecated. CeFi path uses UMI get_adapter('tardis').fetch_instruments.",
            DeprecationWarning,
            stacklevel=2,
        )

        fetch_result = await self.fetch_exchange_instruments(exchange, target_date, force)
        instruments_data, date_filtered_count = cast(tuple[dict[str, dict[str, object]], int], fetch_result)

        canonical_venue = self.normalize_venue(exchange) or exchange
        instruments_data = self._pre_filter_by_exchange_config(instruments_data, exchange, canonical_venue)

        if canonical_venue in self.venue_mapping.spot_mvp_filtered_venues:
            instruments_data = self._apply_mvp_filter(instruments_data, exchange, canonical_venue)

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
                metadata = await self._build_instrument_definition(
                    symbol_id, symbol_info, exchange, canonical_venue, target_date
                )
                if metadata is None:
                    filter_stats["no_canonical_key"] += 1
                    continue
                processed_instruments[metadata.instrument_key] = metadata
                filter_stats["success"] += 1
                self.cache_metadata(metadata.instrument_key, metadata)
            except (OSError, ValueError, RuntimeError) as e:
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
                logger.warning("Failed to process instrument %s: %s", symbol_id, e)

        total_filtered = sum(v for k, v in filter_stats.items() if k != "success")
        logger.info("Processed %s instruments from %s", len(processed_instruments), exchange)
        if total_filtered > 0:
            logger.info(
                "Filtering breakdown: no_key=%s, same_base_quote=%s, expired=%s, errors=%s",
                filter_stats["no_canonical_key"],
                filter_stats["same_base_quote"],
                filter_stats.get("expired_filtered", 0),
                filter_stats["processing_error"],
            )
        return processed_instruments

    async def generate_instruments_for_exchanges(
        self,
        exchanges: list[str],
        target_date: datetime | None = None,
        max_parallel: int | None = None,
        force: bool = False,
    ) -> dict[str, InstrumentDefinition]:
        """
        Generate instruments for multiple exchanges in parallel.

        Args:
            exchanges: List of exchange names
            target_date: Target date for instrument generation
            max_parallel: Maximum parallel exchange processing
            force: If True, force regeneration

        Returns:
            Combined dictionary of all processed instruments
        """
        target_date = target_date or datetime.now(UTC)

        # Filter supported exchanges
        supported_exchanges = [ex for ex in exchanges if ex.lower() in self.processing_config.supported_exchanges]

        if not supported_exchanges:
            logger.warning("No supported exchanges in: %s", exchanges)
            return {}

        logger.info("🚀 Processing %s exchanges: %s", len(supported_exchanges), supported_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        for exchange in supported_exchanges:
            try:
                exchange_instruments = await self.process_exchange_instruments(exchange, target_date, force)
                all_instruments.update(exchange_instruments)
                logger.info("✅ %s: %s instruments processed", exchange, len(exchange_instruments))
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
        logger.info("📊 Total: %s instruments across %s exchanges", len(all_instruments), len(supported_exchanges))
        return all_instruments

    def filter_instruments_by_exchange_config(
        self, instruments: dict[str, dict[str, object]], exchange: str
    ) -> dict[str, dict[str, object]]:
        """Filter instruments by exchange-specific capabilities."""
        canonical_venue: str = self.normalize_venue(exchange) or exchange

        valid_types_value = self.exchange_config.exchange_instrument_types.get(canonical_venue)
        if valid_types_value is None:
            valid_types_value = ["SPOT_PAIR"]
        valid_types: list[str] = valid_types_value

        valid_quotes_value = self.exchange_config.valid_quote_currencies.get(canonical_venue)
        if valid_quotes_value is None:
            valid_quotes_value = ["USDT"]
        valid_quotes: list[str] = valid_quotes_value

        is_derivative = canonical_venue in self.exchange_config.derivative_exchanges

        excluded_bases_value = self.exchange_config.excluded_base_currencies.get(canonical_venue)
        if excluded_bases_value is None:
            excluded_bases_value = []
        excluded_bases: list[str] = excluded_bases_value

        excluded_patterns_value = self.exchange_config.excluded_symbol_patterns.get(canonical_venue)
        if excluded_patterns_value is None:
            excluded_patterns_value = []
        excluded_patterns: list[str] = excluded_patterns_value

        filtered: dict[str, dict[str, object]] = {}

        for inst_key, inst_data in instruments.items():
            try:
                # Check instrument type
                inst_type: str = cast(str, inst_data.get("instrument_type") or "")
                if inst_type not in valid_types:
                    continue

                # Check quote currency
                quote_asset: str = (cast(str | None, inst_data.get("quote_asset")) or "").upper()
                if quote_asset not in valid_quotes:
                    continue

                # Check excluded base currencies
                base_asset: str = (cast(str | None, inst_data.get("base_asset")) or "").upper()
                if base_asset in excluded_bases:
                    continue

                # Check excluded symbol patterns
                symbol: str = (cast(str | None, inst_data.get("symbol")) or "").upper()
                if excluded_patterns:
                    excluded_by_pattern = False
                    for pattern in excluded_patterns:
                        if pattern.upper() in symbol:
                            excluded_by_pattern = True
                            break
                    if excluded_by_pattern:
                        continue

                # Populate complete fields
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
        """Populate all InstrumentDefinition fields per schema."""
        # Core fields
        inst_data.setdefault("tardis_exchange", exchange.lower())
        inst_data.setdefault("data_provider", "tardis")
        inst_data.setdefault("asset_class", "crypto")

        # Venue type classification
        if is_derivative and inst_data.get("instrument_type") in ["PERPETUAL", "FUTURE", "OPTION"]:
            inst_data["venue_type"] = "derivatives"
            base_asset: str = cast(str, inst_data.get("base_asset") or "")
            quote_asset: str = cast(str, inst_data.get("quote_asset") or "")
            if base_asset and quote_asset:
                inst_data["underlying"] = f"{base_asset}-{quote_asset}"
        else:
            inst_data["venue_type"] = "spot"
            inst_data["underlying"] = ""

        # Data types based on instrument type
        inst_type: str = cast(str, inst_data.get("instrument_type", "SPOT_PAIR"))
        inst_data["data_types"] = ",".join(
            self.data_config.instrument_data_types.get(inst_type, ["trades", "book_snapshot_5"])
        )

        return inst_data

    def cleanup(self):
        """Cleanup resources and close connections"""
        # Cleanup Tardis adapter
        adapter = self.tardis_adapter
        if adapter is not None:
            adapter.cleanup()

        # Cleanup CCXT service cache
        ccxt = self.ccxt_service
        if ccxt is not None:
            ccxt.clear_cache()

        # Cleanup subgraph service cache — access via hasattr to avoid AttributeError
        # if the mixin was not fully initialized
        if hasattr(self, "subgraph_service"):
            sg = self.subgraph_service
            if hasattr(sg, "clear_cache"):
                cast(_ClearCacheable, sg).clear_cache()

        # Clear metadata cache
        self._metadata_cache.clear()
        self._cache_timestamps.clear()

        logger.info("🧹 InstrumentProcessingHandlers cleanup completed")
