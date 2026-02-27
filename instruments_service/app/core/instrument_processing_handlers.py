"""
Instrument Processing Handlers

High-level processing workflows for different market types and operations.
Provides complete processing pipelines.
"""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from unified_cloud_services import determine_market_category
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext

from instruments_service.models import InstrumentDefinition

logger = logging.getLogger(__name__)


class InstrumentProcessingHandlers:
    """
    High-level processing handlers for instrument workflows.

    Combines base functionality with mixins to provide complete
    processing pipelines for different market types.
    """

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
        instruments_data, date_filtered_count = cast(tuple[dict[str, dict[str, Any]], int], fetch_result)

        # Apply exchange config filtering BEFORE expensive processing
        canonical_venue = self.normalize_venue(exchange) or exchange
        valid_types_value = self.exchange_config.exchange_instrument_types.get(canonical_venue)
        if valid_types_value is None:
            valid_types_value = []
        valid_types: list[str] = valid_types_value

        valid_quotes_value = self.exchange_config.valid_quote_currencies.get(canonical_venue)
        if valid_quotes_value is None:
            valid_quotes_value = ["USDT"]
        valid_quotes: list[str] = valid_quotes_value

        logger.info(
            f"🔍 Pre-filtering by exchange config: {canonical_venue} accepts types={valid_types}, quotes={valid_quotes}"
        )

        # Get excluded base currencies and symbol patterns
        excluded_bases_value = self.exchange_config.excluded_base_currencies.get(canonical_venue)
        if excluded_bases_value is None:
            excluded_bases_value = []
        excluded_bases: list[str] = excluded_bases_value

        excluded_patterns_value = self.exchange_config.excluded_symbol_patterns.get(canonical_venue)
        if excluded_patterns_value is None:
            excluded_patterns_value = []
        excluded_patterns: list[str] = excluded_patterns_value

        # Pre-filter by exchange config
        pre_filtered: dict[str, dict[str, Any]] = {}
        for symbol_id, symbol_info in instruments_data.items():
            symbol_type: str = (cast(str | None, symbol_info.get("type")) or "").lower()
            normalized_type = self.normalize_instrument_type(symbol_type)

            # Filter by valid instrument types
            if normalized_type not in valid_types:
                continue

            # Check excluded symbol patterns
            if excluded_patterns:
                symbol_upper: str = symbol_id.upper()
                excluded_by_pattern = False
                for pattern in excluded_patterns:
                    if pattern.upper() in symbol_upper:
                        logger.debug(f"🚫 Pre-filtered out {symbol_id}: pattern '{pattern}' excluded")
                        excluded_by_pattern = True
                        break
                if excluded_by_pattern:
                    continue

            # Quick parse to check validity
            parsed_components = self.parse_symbol_components(symbol_id, exchange)
            if isinstance(parsed_components, dict):
                base_asset = (parsed_components.get("base_asset") or "").upper()
                quote_asset = (parsed_components.get("quote_asset") or "").upper()
            else:
                base_asset, quote_asset = parsed_components if parsed_components else ("", "")
                base_asset = base_asset.upper() if base_asset else ""
                quote_asset = quote_asset.upper() if quote_asset else ""

            # Filter by excluded base currencies
            if base_asset and base_asset in excluded_bases:
                logger.debug(f"🚫 Pre-filtered out {symbol_id}: base '{base_asset}' excluded")
                continue

            # Filter by valid quote currencies
            if quote_asset and quote_asset not in valid_quotes:
                continue

            pre_filtered[symbol_id] = symbol_info

        logger.info(
            f"🔍 Exchange config filter: {len(pre_filtered)}/{len(instruments_data)} valid for {canonical_venue}"
        )
        instruments_data = pre_filtered

        # MVP base asset filtering for specific venues
        if canonical_venue in self.venue_mapping.spot_mvp_filtered_venues:
            mvp_base_assets = {b.upper() for b in self.venue_mapping.hyperliquid_aster_mvp_base_assets}
            mvp_filtered: dict[str, dict[str, Any]] = {}
            for symbol_id, symbol_info in instruments_data.items():
                parsed_components = self.parse_symbol_components(symbol_id, exchange)
                if isinstance(parsed_components, dict):
                    base_asset = (parsed_components.get("base_asset") or "").upper()
                else:
                    base_asset, _ = parsed_components if parsed_components else ("", "")
                    base_asset = base_asset.upper() if base_asset else ""

                if base_asset in mvp_base_assets:
                    mvp_filtered[symbol_id] = symbol_info
                else:
                    logger.debug(f"🚫 MVP filter: {symbol_id} excluded (base '{base_asset}' not in MVP list)")

            logger.info(f"🔍 MVP filter: {len(mvp_filtered)}/{len(instruments_data)} for {canonical_venue}")
            instruments_data = mvp_filtered

        # Process instruments
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
                    continue

                # Parse components
                parsed_components: dict[str, Any] = self.parse_symbol_components(symbol_id, exchange)
                if isinstance(parsed_components, dict):
                    base_asset = cast(str, parsed_components.get("base_asset") or "")
                    quote_asset = cast(str, parsed_components.get("quote_asset") or "")
                else:
                    base_asset, quote_asset = parsed_components if parsed_components else ("", "")

                # Clean and validate
                clean_base = str(base_asset or "").upper() if base_asset is not None else ""
                clean_quote = str(quote_asset or "").upper() if quote_asset is not None else ""

                # Filter nonsensical pairs
                if clean_base == clean_quote and clean_base:
                    filter_stats["same_base_quote"] += 1
                    continue

                # Infer settle_asset
                settle_asset = "USDT"
                canonical_venue_raw = self.normalize_venue(exchange)
                canonical_venue = canonical_venue_raw or exchange
                if canonical_venue == "DERIBIT":
                    deribit_quotes_value = self.exchange_config.valid_quote_currencies.get("DERIBIT")
                    if deribit_quotes_value is None:
                        deribit_quotes_value = []
                    deribit_quotes = deribit_quotes_value
                    if clean_quote == "USD":
                        settle_asset = clean_base
                    elif clean_quote in deribit_quotes and clean_quote != "USD":
                        settle_asset = clean_quote

                # Extract symbol from canonical key
                symbol: str = canonical_key.split(":", 2)[2] if len(canonical_key.split(":")) >= 3 else symbol_id

                # Populate derived fields
                norm_inst_type: str = self.normalize_instrument_type(symbol_info.get("type") or "") or ""
                enhanced_fields: dict[str, Any] = await self._populate_all_derived_fields(
                    canonical_key,
                    canonical_venue,
                    norm_inst_type,
                    clean_base,
                    clean_quote,
                    symbol_id,
                    exchange,
                )

                # Create metadata object
                normalized_instrument_type = self.normalize_instrument_type(symbol_info.get("type") or "")

                # Set data_types based on instrument_type
                config_data_types = self.data_config.instrument_data_types.get(
                    normalized_instrument_type or "SPOT_PAIR",
                    ["trades", "book_snapshot_5"],
                )
                data_types_str = ",".join(config_data_types)

                # Set tardis_exchange based on venue+instrument_type mapping
                mapping_key: tuple[str, str] = (
                    canonical_venue,
                    normalized_instrument_type or "SPOT_PAIR",
                )
                tardis_exchange: str = self.venue_mapping.venue_instrument_type_to_tardis.get(
                    mapping_key,
                    exchange.lower(),
                )

                # Get available_to from Tardis API
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
                        logger.debug(f"⚠️ Could not parse availableTo '{available_to}': {e}")

                # For derivatives, set available_to_datetime from expiry if missing
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
                        except Exception as e:
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
                # Filter expired instruments
                if available_to_datetime:
                    try:
                        available_to_dt = datetime.fromisoformat(available_to_datetime.replace("Z", "+00:00"))
                        if available_to_dt.tzinfo is None:
                            available_to_dt = available_to_dt.replace(tzinfo=UTC)

                        comparison_date = target_date if target_date else datetime.now(UTC)
                        if comparison_date.tzinfo is None:
                            comparison_date = comparison_date.replace(tzinfo=UTC)

                        if comparison_date.date() > available_to_dt.date():
                            filter_stats["expired_filtered"] = filter_stats.get("expired_filtered", 0) + 1
                            continue
                    except (ValueError, TypeError) as e:
                        logger.debug(f"⚠️ Could not parse available_to_datetime '{available_to_datetime}': {e}")

                # Check expiry for futures/options
                if target_date and normalized_instrument_type in ["FUTURE", "OPTION"] and "expiry" in enhanced_fields:
                    expiry_str_filter: str = cast(str, enhanced_fields.get("expiry") or "")
                    if expiry_str_filter:
                        try:
                            expiry_dt = datetime.fromisoformat(expiry_str_filter.replace("Z", "+00:00"))
                            target_date_only = target_date.date() if hasattr(target_date, "date") else target_date
                            expiry_date_only = expiry_dt.date()

                            if target_date_only > expiry_date_only:
                                filter_stats["expiry_filtered"] += 1
                                continue
                        except (ValueError, TypeError) as e:
                            logger.debug(f"⚠️ Could not parse expiry '{expiry_str_filter}': {e}")

                # Determine market category
                instrument_dict: dict[str, str | None] = {
                    "databento_symbol": "",
                    "chain": "off-chain",
                }
                market_category = determine_market_category(instrument_dict)

                venue_str: str = canonical_venue
                inst_type_str: str = normalized_instrument_type or "SPOT_PAIR"
                metadata = InstrumentDefinition(
                    instrument_key=canonical_key,
                    venue=venue_str,
                    instrument_type=inst_type_str,
                    symbol=symbol,
                    base_asset=clean_base,
                    quote_asset=clean_quote,
                    settle_asset=settle_asset,
                    chain="off-chain",
                    market_category=market_category,
                    exchange_raw_symbol=symbol_id,
                    tardis_symbol=self._convert_to_tardis_symbol(symbol_id, exchange),
                    tardis_exchange=tardis_exchange,
                    available_from_datetime=symbol_info.get("availableSince") or "",
                    available_to_datetime=available_to_datetime,
                    data_types=data_types_str,
                    **enhanced_fields,
                )

                processed_instruments[canonical_key] = metadata
                filter_stats["success"] += 1

                # Cache metadata
                self.cache_metadata(canonical_key, metadata)

            except Exception as e:
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
                logger.warning(f"⚠️ Failed to process instrument {symbol_id}: {e}")
        # Log statistics
        total_filtered = sum(v for k, v in filter_stats.items() if k != "success")
        logger.info(f"📊 Processed {len(processed_instruments)} instruments from {exchange}")
        if total_filtered > 0:
            logger.info(
                f"🔍 Filtering breakdown: "
                f"no_key={filter_stats['no_canonical_key']}, "
                f"same_base_quote={filter_stats['same_base_quote']}, "
                f"expired={filter_stats.get('expired_filtered', 0)}, "
                f"errors={filter_stats['processing_error']}"
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
            logger.warning(f"No supported exchanges in: {exchanges}")
            return {}

        logger.info(f"🚀 Processing {len(supported_exchanges)} exchanges: {supported_exchanges}")

        all_instruments: dict[str, InstrumentDefinition] = {}

        for exchange in supported_exchanges:
            try:
                exchange_instruments = await self.process_exchange_instruments(exchange, target_date, force)
                all_instruments.update(exchange_instruments)
                logger.info(f"✅ {exchange}: {len(exchange_instruments)} instruments processed")
            except Exception as e:
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
        logger.info(f"📊 Total: {len(all_instruments)} instruments across {len(supported_exchanges)} exchanges")
        return all_instruments

    def filter_instruments_by_exchange_config(
        self, instruments: dict[str, dict[str, Any]], exchange: str
    ) -> dict[str, dict[str, Any]]:
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

        filtered: dict[str, dict[str, Any]] = {}

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

            except Exception as e:
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
        self, inst_data: dict[str, Any], exchange: str, is_derivative: bool
    ) -> dict[str, Any]:
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
        if hasattr(self, "tardis_adapter") and self.tardis_adapter is not None:
            self.tardis_adapter.cleanup()

        # Cleanup CCXT service cache
        if hasattr(self, "ccxt_service") and self.ccxt_service:
            self.ccxt_service.clear_cache()

        # Cleanup subgraph service cache
        if hasattr(self, "subgraph_service") and self.subgraph_service:
            self.subgraph_service.clear_cache()

        # Clear metadata cache
        self._metadata_cache.clear()
        self._cache_timestamps.clear()

        logger.info("🧹 InstrumentProcessingHandlers cleanup completed")
