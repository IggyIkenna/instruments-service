"""
DeFi Instruments Processor

Fetches DeFi instruments from various protocols.
Extracted from InstrumentProcessingService.fetch_defi_instruments.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, cast
from uuid import uuid4

from unified_events_interface import VENUE_ZERO_INSTRUMENTS, log_event
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.reference.instrument import InstrumentRecord
from unified_reference_data_interface import create_reference_data_adapter

from instruments_service.models import InstrumentDefinition

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Protocol-specific kwargs (str, int, bool, datetime, etc.)
DefiProtocolKwargs = str | int | bool | None | datetime


class _VenueMappingProtocol(Protocol):
    """Protocol for venue_mapping attribute."""

    def get_defi_mvp_tokens(self) -> list[str]: ...

    hyperliquid_aster_mvp_base_assets: list[str]


class _DateFilterProtocol(Protocol):
    """Protocol for date_filter_service attribute."""

    def filter_instruments_by_date(
        self,
        instruments: dict[str, dict[str, str | None | object]],
        target_date: datetime,
        protocol: str | None = None,
    ) -> dict[str, dict[str, str | None | object]]: ...


class _CCXTServiceProtocol(Protocol):
    """Protocol for ccxt_service attribute."""

    def load_markets(self, venue: str, force_refresh: bool = False) -> dict[str, object] | None: ...

    def get_metadata(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        tardis_symbol: str | None = None,
        instrument_type: str | None = None,
    ) -> dict[str, str | float | int | None]: ...

    def generate_default_ccxt_symbol(
        self,
        venue: str,
        base_asset: str,
        quote_asset: str,
        symbol_id: str,
        instrument_type: str | None = None,
    ) -> str: ...


class DefiServiceProtocol(Protocol):
    """Protocol for InstrumentProcessingService used by fetch_defi_instruments."""

    venue_mapping: _VenueMappingProtocol
    date_filter_service: _DateFilterProtocol
    ccxt_service: _CCXTServiceProtocol

    def get_manual_ccxt_fallback(self, venue: str, base_asset: str) -> dict[str, object]: ...


def _records_to_raw_dict(records: list[InstrumentRecord]) -> dict[str, object]:
    """Convert a list of InstrumentRecord to a keyed dict of plain dicts.

    Applies the same None->empty, Decimal->str, datetime->isoformat conversion
    pattern used by the aster handler so downstream code receives uniform dicts.
    """
    result: dict[str, object] = {}
    for record in records:
        d: dict[str, object] = {}
        for k, v in record.model_dump().items():
            if v is None:
                d[k] = ""
            elif isinstance(v, Decimal):
                d[k] = str(v) if v.is_finite() else ""
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
            else:
                d[k] = v
        # Ensure downstream-required keys are present
        d["instrument_key"] = record.instrument_key
        d["venue"] = record.venue
        d["instrument_type"] = str(record.instrument_type)
        d["symbol"] = record.symbol or record.raw_symbol or ""
        d["base_asset"] = record.base_asset or ""
        d["quote_asset"] = record.quote_asset or ""
        d["exchange_raw_symbol"] = record.raw_symbol or ""
        d["available_from_datetime"] = (
            record.available_since.isoformat() if record.available_since else "2020-01-01T00:00:00Z"
        )
        result[record.instrument_key] = d
    return result


async def _fetch_raw_from_protocol(
    service: DefiServiceProtocol,
    protocol: str,
    chain: str,
    target_date: datetime | None,
    **kwargs: DefiProtocolKwargs,
) -> dict[str, object] | None:
    """Dispatch to the appropriate DeFi adapter and fetch raw instruments.

    Returns None if protocol is unrecognized.
    """
    graph_api_key: str | None = getattr(service, "_graph_api_key", None)

    proto = protocol.lower()

    # --- Protocols that use URDI's create_reference_data_adapter ---
    # Map protocol names used by instruments-service to URDI factory keys.
    _proto_to_urdi_key: dict[str, str] = {
        "uniswap_v3": "uniswap_v3",
        "uniswap_v2": "uniswap_v2",
        "uniswap_v4": "uniswap_v4",
        "curve": "curve",
        "aave_v3": "aave_v3",
        "morpho": "morpho",
        "euler_plasma": "euler",
        "fluid_plasma": "fluid",
        "ethena": "ethena",
        "balancer": "balancer",
        "lido": "lido",
        "etherfi": "etherfi",
        "aave_plasma": "aave_v3",
        "hyperliquid": "hyperliquid",
    }

    if proto == "aster":
        # Aster: URDI with additional field overrides for CLOB perpetuals
        aster_adapter = create_reference_data_adapter("aster")
        records = await aster_adapter.get_instruments()
        raw: dict[str, object] = _records_to_raw_dict(records)
        # Apply aster-specific overrides on top of the generic conversion
        for _inst_key, inst_data in raw.items():
            if isinstance(inst_data, dict):
                inst_data["venue"] = "ASTER"
                inst_data["instrument_type"] = "PERPETUAL"
                if not inst_data.get("available_from_datetime"):
                    inst_data["available_from_datetime"] = "2024-10-01T00:00:00Z"
                inst_data["tardis_exchange"] = ""
                inst_data["tardis_symbol"] = ""
                inst_data["data_provider"] = "aster"
                inst_data["market_category"] = "cefi"
                inst_data["data_types"] = "trades,book_snapshot_5,derivative_ticker"
        return raw

    urdi_key = _proto_to_urdi_key.get(proto)
    if urdi_key is None:
        return None

    adapter = create_reference_data_adapter(urdi_key, api_key=graph_api_key)
    records = await adapter.get_instruments()
    return _records_to_raw_dict(records)


def _enrich_hyperliquid_instrument(
    service: DefiServiceProtocol,
    inst_def: InstrumentDefinition,
) -> None:
    """Enrich a Hyperliquid instrument with CCXT metadata or manual fallback."""
    venue = inst_def.venue
    ccxt_metadata = service.ccxt_service.get_metadata(
        venue=venue,
        base_asset=inst_def.base_asset,
        quote_asset=inst_def.quote_asset,
        symbol_id=inst_def.exchange_raw_symbol or inst_def.symbol,
        instrument_type=inst_def.instrument_type,
    )
    if ccxt_metadata:
        if ccxt_metadata.get("ccxt_symbol"):
            inst_def.ccxt_symbol = str(ccxt_metadata["ccxt_symbol"])
        if ccxt_metadata.get("ccxt_exchange"):
            inst_def.ccxt_exchange = str(ccxt_metadata["ccxt_exchange"])
        if ccxt_metadata.get("tick_size"):
            inst_def.tick_size = str(ccxt_metadata["tick_size"])
        if ccxt_metadata.get("min_size"):
            inst_def.min_size = str(ccxt_metadata["min_size"])
        if ccxt_metadata.get("contract_size"):
            with contextlib.suppress(ValueError, TypeError):
                inst_def.contract_size = float(cast(str | int | float, ccxt_metadata["contract_size"]))
        return

    # Fallback: generate defaults + manual metadata
    if not inst_def.ccxt_symbol or not inst_def.ccxt_exchange:
        default_ccxt_symbol = service.ccxt_service.generate_default_ccxt_symbol(
            venue=venue,
            base_asset=inst_def.base_asset,
            quote_asset=inst_def.quote_asset,
            symbol_id=inst_def.exchange_raw_symbol or inst_def.symbol,
            instrument_type=inst_def.instrument_type,
        )
        ccxt_exchange_id = cast(dict[str, str], getattr(service.venue_mapping, "venue_to_ccxt", {})).get(venue) or ""
        if not inst_def.ccxt_symbol:
            inst_def.ccxt_symbol = default_ccxt_symbol
        if not inst_def.ccxt_exchange:
            inst_def.ccxt_exchange = ccxt_exchange_id

    manual_metadata = service.get_manual_ccxt_fallback(venue, inst_def.base_asset)
    if manual_metadata:
        if not inst_def.tick_size and manual_metadata.get("tick_size"):
            inst_def.tick_size = str(cast(str | int | float, manual_metadata["tick_size"]))
        if not inst_def.min_size and manual_metadata.get("min_size"):
            inst_def.min_size = str(cast(str | int | float, manual_metadata["min_size"]))
        if not inst_def.contract_size and manual_metadata.get("contract_size"):
            with contextlib.suppress(ValueError, TypeError):
                inst_def.contract_size = float(cast(str | int | float, manual_metadata["contract_size"]))


_WRAPPED_TOKEN_MAP: dict[str, str] = {
    "WETH": "ETH",
    "WSTETH": "ETH",
    "WEETH": "ETH",
    "STETH": "ETH",
}


def _passes_mvp_filter(
    asset: str,
    mvp_set: set[str],
) -> bool:
    """Check if asset or its unwrapped version is in MVP set."""
    if not asset:
        return True
    if asset in mvp_set:
        return True
    unwrapped = _WRAPPED_TOKEN_MAP.get(asset)
    return unwrapped is not None and unwrapped in mvp_set


async def fetch_defi_instruments(
    service: DefiServiceProtocol,
    protocol: str,
    chain: str = "ETHEREUM",
    target_date: datetime | None = None,
    **kwargs: DefiProtocolKwargs,
) -> dict[str, InstrumentDefinition]:
    """
    Fetch DeFi instruments from various protocols.

    Args:
        service: InstrumentProcessingService instance for delegation
        protocol: Protocol name
        chain: Chain identifier (default: 'ETHEREUM')
        target_date: Optional target date to filter instruments
        **kwargs: Additional protocol-specific arguments

    Returns:
        Dictionary mapping instrument_key to InstrumentDefinition
    """
    try:
        raw_instruments = await _fetch_raw_from_protocol(service, protocol, chain, target_date, **kwargs)
        if raw_instruments is None:
            logger.debug("DeFi protocol '%s' not yet implemented, skipping", protocol)
            return {}

        # Skip date filtering for on-chain CLOB perpetuals — they're always available
        if target_date and protocol.lower() not in ("aster", "hyperliquid"):
            # DateFilterService.filter_instruments_by_date expects dict values (it calls .get()
            # on each value). DeFi adapters return dict[str, dict[str, object]], but the cast
            # to dict[str, object] erases the inner dict type. Filter out non-dict values and
            # convert Pydantic models to dicts to prevent AttributeError on .get().
            filterable: dict[str, dict[str, str | None | object]] = {}
            for k, v in raw_instruments.items():
                if isinstance(v, dict):
                    filterable[k] = cast(dict[str, str | None | object], v)
                elif hasattr(v, "model_dump"):
                    filterable[k] = cast(dict[str, str | None | object], v.model_dump())  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
                else:
                    # Skip non-dict, non-model entries — they can't be date-filtered
                    continue
            raw_instruments = cast(
                dict[str, object],
                service.date_filter_service.filter_instruments_by_date(
                    instruments=filterable,
                    target_date=target_date,
                    protocol=protocol,
                ),
            )

        # Pre-load CCXT markets for Hyperliquid venues
        if protocol.lower() == "hyperliquid":
            venues_to_enrich: set[str] = set()
            for inst_data in raw_instruments.values():
                if isinstance(inst_data, dict):
                    venue_val = cast(str | None, cast(dict[str, object], inst_data).get("venue"))
                    if isinstance(venue_val, str):
                        venues_to_enrich.add(venue_val)
            for venue in venues_to_enrich:
                service.ccxt_service.load_markets(venue)

        # Determine MVP filter sets
        base_currency_list = service.venue_mapping.get_defi_mvp_tokens()
        if protocol.lower() in ["hyperliquid", "aster"]:
            mvp_bases = {str(b).upper() for b in service.venue_mapping.hyperliquid_aster_mvp_base_assets}
            mvp_quotes: set[str] = {"USDC", "USDT", "USD1"}
        elif protocol.lower() == "ethena":
            mvp_bases = {"USDE", "SUSDE"}
            mvp_quotes = {"USDE", "SUSDE", ""}
        else:
            mvp_quotes = {str(q).upper() for q in base_currency_list}
            mvp_bases = {str(b).upper() for b in base_currency_list}

        instruments: dict[str, InstrumentDefinition] = {}
        for inst_key, inst_data in raw_instruments.items():
            try:
                inst_data_dict: dict[str, object] = (
                    cast(dict[str, object], inst_data) if isinstance(inst_data, dict) else {}
                )
                base_asset: str = str(inst_data_dict.get("base_asset") or "").upper()
                if not _passes_mvp_filter(base_asset, mvp_bases):
                    continue
                quote_asset: str = str(inst_data_dict.get("quote_asset") or "").upper()
                if not _passes_mvp_filter(quote_asset, mvp_quotes):
                    continue

                inst_def = InstrumentDefinition.model_validate(inst_data_dict)
                if protocol.lower() == "hyperliquid":
                    _enrich_hyperliquid_instrument(service, inst_def)
                instruments[inst_key] = inst_def
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
                continue

        logger.info("Fetched %s %s instruments for %s", len(instruments), protocol, chain)
        if not instruments:
            log_event(
                VENUE_ZERO_INSTRUMENTS,
                details={
                    "venue": protocol,
                    "chain": chain,
                    "reason": "adapter returned 0 instruments after filtering",
                    "raw_count": len(raw_instruments) if raw_instruments else 0,
                },
            )
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
        logger.error("Failed to fetch %s instruments: %s", protocol, e)
        return {}
