"""
Derived Fields Populator

Populates CCXT, leverage, and business-logic fields for instrument definitions.
Extracted from InstrumentProcessingService for file-size compliance (COD-SIZE).
"""

import logging
from typing import TYPE_CHECKING, Protocol, TypedDict, cast
from uuid import uuid4

from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity

if TYPE_CHECKING:
    from unified_config_interface import ExchangeInstrumentConfig

logger = logging.getLogger(__name__)


class OptionComponents(TypedDict):
    """Option parsing results with expiry, strike, and option type."""

    expiry_date: str
    strike_price: str
    option_type: str


class InstrumentProcessingConfig(Protocol):
    """Protocol for processing configuration."""

    enable_ccxt_integration: bool


class DerivedFields(TypedDict, total=False):
    """Derived fields populated for instrument definitions."""

    expiry: str
    strike: str
    option_type: str
    ccxt_symbol: str
    ccxt_exchange: str
    tick_size: str
    min_size: str
    contract_size: str
    max_position_size: float
    max_leverage: float
    initial_margin_rate: float
    maintenance_margin_rate: float
    inverse: bool
    underlying: str


class _CCXTServiceProtocol(Protocol):
    """Protocol for ccxt_service attribute."""

    def get_metadata(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: str | None = None,
        instrument_type: str | None = None,
    ) -> dict[str, str | float | int | None]: ...

    def get_leverage_limits(
        self,
        venue: str,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: str | None = None,
        instrument_type: str | None = None,
    ) -> dict[str, str | float | int | None]: ...


class _VenueMappingProtocol(Protocol):
    """Protocol for venue_mapping attribute."""

    tardis_to_venue: dict[str, str]
    venue_to_ccxt: dict[str, str]


class DerivedFieldsServiceProtocol(Protocol):
    """Protocol for InstrumentProcessingService used by populate_derived_fields."""

    venue_mapping: _VenueMappingProtocol
    exchange_config: "ExchangeInstrumentConfig"
    processing_config: InstrumentProcessingConfig
    ccxt_service: _CCXTServiceProtocol | None

    def parse_option_components(self, symbol_id: str, exchange: str) -> OptionComponents: ...
    def parse_expiry_from_symbol(self, symbol_id: str, exchange: str) -> str | None: ...


async def populate_derived_fields(
    service: DerivedFieldsServiceProtocol,
    canonical_key: str,
    venue: str,
    inst_type: str,
    base_asset: str,
    quote_asset: str,
    symbol_id: str,
    exchange: str | None = None,
) -> DerivedFields:
    """Populate ALL derived fields for instrument definition."""
    try:
        derived_fields: DerivedFields = {}

        if inst_type == "OPTION":
            tardis_exchange = exchange or venue.lower()
            option_components = service.parse_option_components(symbol_id, tardis_exchange)
            derived_fields.update(
                {
                    "expiry": cast(str, option_components.get("expiry_date", "2025-12-25T08:00:00Z")),
                    "strike": cast(str, option_components.get("strike_price") or ""),
                    "option_type": (cast(str, option_components.get("option_type") or "")).upper(),
                }
            )
        elif inst_type == "FUTURE":
            tardis_venue: str = service.venue_mapping.tardis_to_venue.get(venue, venue)
            expiry_date = service.parse_expiry_from_symbol(symbol_id, tardis_venue.lower())
            derived_fields["expiry"] = expiry_date or "2025-12-25T08:00:00Z"

        ccxt_exchange_id: str | None = service.venue_mapping.venue_to_ccxt.get(venue)
        default_ccxt_symbol = (
            f"{base_asset}/{quote_asset}:{quote_asset}"
            if quote_asset and inst_type in ["PERPETUAL", "FUTURE"]
            else f"{base_asset}/{quote_asset}"
            if base_asset and quote_asset
            else ""
        )

        enable_ccxt = getattr(service.processing_config, "enable_ccxt_integration", True)
        ccxt_service = service.ccxt_service

        if enable_ccxt and ccxt_service:
            ccxt_metadata = ccxt_service.get_metadata(
                venue=venue,
                base_asset=base_asset,
                quote_asset=quote_asset,
                symbol_id=symbol_id,
                tardis_symbol=None,
                instrument_type=inst_type,
            )
            if ccxt_metadata:
                derived_fields.update(
                    {
                        "ccxt_symbol": cast(str, ccxt_metadata.get("ccxt_symbol", default_ccxt_symbol)),
                        "ccxt_exchange": cast(str, ccxt_metadata.get("ccxt_exchange", ccxt_exchange_id or "")),
                        "tick_size": cast(str, ccxt_metadata.get("tick_size") or ""),
                        "min_size": cast(str, ccxt_metadata.get("min_size") or ""),
                        "contract_size": cast(str, ccxt_metadata.get("contract_size") or ""),
                    }
                )
            else:
                derived_fields["ccxt_symbol"] = default_ccxt_symbol
                derived_fields["ccxt_exchange"] = ccxt_exchange_id or ""

            ccxt_symbol_for_tiers = (
                cast(str, ccxt_metadata.get("ccxt_symbol") or "") if ccxt_metadata else default_ccxt_symbol
            )
            leverage_limits = ccxt_service.get_leverage_limits(
                venue=venue,
                symbol=ccxt_symbol_for_tiers,
                base_asset=base_asset,
                quote_asset=quote_asset,
                symbol_id=symbol_id,
                tardis_symbol=None,
                instrument_type=inst_type,
            )
            if leverage_limits and leverage_limits.get("max_position_size") is not None:
                _mps = cast(float, leverage_limits["max_position_size"])
                derived_fields["max_position_size"] = _mps
            if inst_type in ["PERPETUAL", "FUTURE"] and leverage_limits:
                if leverage_limits.get("max_leverage") is not None:
                    derived_fields["max_leverage"] = cast(float, leverage_limits["max_leverage"])
                if leverage_limits.get("initial_margin_rate") is not None:
                    derived_fields["initial_margin_rate"] = cast(float, leverage_limits["initial_margin_rate"])
                if leverage_limits.get("maintenance_margin_rate") is not None:
                    derived_fields["maintenance_margin_rate"] = cast(float, leverage_limits["maintenance_margin_rate"])
        else:
            derived_fields["ccxt_symbol"] = default_ccxt_symbol
            derived_fields["ccxt_exchange"] = ccxt_exchange_id or ""
            if ccxt_service:
                leverage_limits = ccxt_service.get_leverage_limits(
                    venue=venue,
                    symbol=default_ccxt_symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    symbol_id=symbol_id,
                    tardis_symbol=None,
                    instrument_type=inst_type,
                )
                if leverage_limits and leverage_limits.get("max_position_size") is not None:
                    derived_fields["max_position_size"] = cast(float, leverage_limits["max_position_size"])
                if inst_type in ["PERPETUAL", "FUTURE"] and leverage_limits:
                    if leverage_limits.get("max_leverage") is not None:
                        derived_fields["max_leverage"] = cast(float, leverage_limits["max_leverage"])
                    if leverage_limits.get("initial_margin_rate") is not None:
                        derived_fields["initial_margin_rate"] = cast(float, leverage_limits["initial_margin_rate"])
                    if leverage_limits.get("maintenance_margin_rate") is not None:
                        derived_fields["maintenance_margin_rate"] = cast(
                            float, leverage_limits["maintenance_margin_rate"]
                        )

        vqc_raw = getattr(service.exchange_config, "valid_quote_currencies", None)
        if vqc_raw is None:
            raise ValueError("exchange_config must have valid_quote_currencies")
        vqc: dict[str, object] = cast(dict[str, object], vqc_raw)
        deribit_raw: object = vqc.get("DERIBIT")
        if deribit_raw is None:
            raise ValueError("valid_quote_currencies must have entry for DERIBIT")
        if not isinstance(deribit_raw, list):
            raise TypeError(f"valid_quote_currencies[DERIBIT] must be list, got {type(deribit_raw).__name__}")
        deribit_quotes: list[str] = [str(item) for item in deribit_raw]
        if venue == "DERIBIT" and quote_asset == "USD":
            derived_fields["inverse"] = True
        elif venue == "DERIBIT" and quote_asset in deribit_quotes:
            derived_fields["inverse"] = False

        if inst_type in ["PERPETUAL", "FUTURE", "OPTION"] and base_asset and quote_asset:
            derived_fields["underlying"] = f"{base_asset}-{quote_asset}"

        return derived_fields

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
        logger.debug("⚠️ Error populating derived fields for %s: %s", canonical_key, e)
        return {}
