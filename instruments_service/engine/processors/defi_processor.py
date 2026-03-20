"""
DeFi Instruments Processor

Fetches DeFi instruments from various protocols.
Extracted from InstrumentProcessingService.fetch_defi_instruments.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, cast
from uuid import uuid4

from unified_events_interface import VENUE_ZERO_INSTRUMENTS, log_event
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorContext, ErrorRecoveryStrategy, ErrorSeverity
from unified_market_interface import (
    AavePlasmaAdapter,
    AaveV3Adapter,
    BalancerAdapter,
    CurveAdapter,
    EthenaAdapter,
    EtherFiAdapter,
    EulerAdapter,
    FluidAdapter,
    HyperliquidAdapter,
    LidoAdapter,
    MorphoAdapter,
    UniswapV2Adapter,
    UniswapV3Adapter,
    UniswapV4Adapter,
)

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


def _list_to_dict(items: list[dict[str, object]]) -> dict[str, object]:
    """Convert a list of instrument dicts to a keyed dict.

    Some adapters (Ethena, Morpho) return ``list[dict]`` instead of
    ``dict[str, dict]``.  We key by ``instrument_key`` or ``instrument_id``
    when available, falling back to the list index.
    """
    result: dict[str, object] = {}
    for idx, item in enumerate(items):
        key = str(item.get("instrument_key") or item.get("instrument_id") or item.get("id") or f"__idx_{idx}")
        result[key] = item
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
    base_currency_list: list[str] = service.venue_mapping.get_defi_mvp_tokens()
    quote_currency_list: list[str] = service.venue_mapping.get_defi_mvp_tokens()
    graph_api_key: str | None = getattr(service, "_graph_api_key", None)

    bc = kwargs.get("base_currency")
    base_currency_kw: str | None = bc if isinstance(bc, str) else None
    ml = kwargs.get("min_liquidity")
    min_liquidity_kw: float | None = float(ml) if isinstance(ml, (int, float)) else None

    proto = protocol.lower()

    raw: dict[str, object] | list[dict[str, object]] | None = None

    if proto == "uniswap_v3":
        adapter = UniswapV3Adapter(chain=chain, api_key=graph_api_key)
        raw = cast(
            dict[str, object],
            await adapter.fetch_pools(
                base_currency_list=base_currency_list,
                quote_currency_list=quote_currency_list,
                base_currency=base_currency_kw,
                min_liquidity=min_liquidity_kw,
            ),
        )
    elif proto == "balancer":
        raw = cast(
            dict[str, object],
            BalancerAdapter(chain=chain).fetch_markets(
                base_currency_list=base_currency_list,
                quote_currency_list=quote_currency_list,
                base_currency=base_currency_kw,
                min_liquidity=min_liquidity_kw,
            ),
        )
    elif proto == "aave_v3":
        raw = cast(
            dict[str, object],
            await AaveV3Adapter(chain=chain, graph_api_key=graph_api_key).fetch_markets(target_date=target_date),
        )
    elif proto == "etherfi":
        raw = cast(dict[str, object], EtherFiAdapter(chain=chain).fetch_lst_instruments())
    elif proto == "lido":
        raw = cast(dict[str, object], LidoAdapter(chain=chain).fetch_lst_instruments())
    elif proto == "morpho":
        # MorphoAdapter.fetch_markets() returns list[dict], not dict
        raw = await MorphoAdapter(chain=chain).fetch_markets()
    elif proto == "hyperliquid":
        hl_bases: list[str] = service.venue_mapping.hyperliquid_aster_mvp_base_assets
        adapter = HyperliquidAdapter(base_currency_list=hl_bases)
        perpetuals = cast(dict[str, object], adapter.fetch_perpetuals(test_data_availability=False))
        spot_pairs = cast(dict[str, object], adapter.fetch_spot_pairs(test_data_availability=False))
        raw = {**perpetuals, **spot_pairs}
    elif proto == "aster":
        from unified_reference_data_interface import create_reference_data_adapter

        aster_adapter = create_reference_data_adapter("aster")
        records = await aster_adapter.get_instruments()
        raw = {}
        for record in records:
            # Convert InstrumentRecord → InstrumentDefinition-compatible dict
            # None → empty string, Decimal → str, datetime → isoformat
            from datetime import datetime as _dt
            from decimal import Decimal as _Decimal

            d: dict[str, object] = {}
            for k, v in record.model_dump().items():
                if v is None:
                    d[k] = ""
                elif isinstance(v, _Decimal):
                    d[k] = str(v) if v.is_finite() else ""
                elif isinstance(v, _dt):
                    d[k] = v.isoformat()
                else:
                    d[k] = v
            d["instrument_key"] = record.instrument_key
            d["venue"] = "ASTER"
            d["instrument_type"] = "PERPETUAL"
            d["symbol"] = record.symbol or record.raw_symbol or ""
            d["available_from_datetime"] = (
                record.available_since.isoformat() if record.available_since else "2024-10-01T00:00:00Z"
            )
            d["base_asset"] = record.base_asset or ""
            d["quote_asset"] = record.quote_asset or ""
            d["exchange_raw_symbol"] = record.raw_symbol or ""
            d["tardis_exchange"] = ""
            d["tardis_symbol"] = ""
            d["data_provider"] = "aster"
            d["market_category"] = "cefi"
            d["data_types"] = "trades,book_snapshot_5,derivative_ticker"
            raw[record.instrument_key] = d
        return raw
    elif proto == "uniswap_v2":
        raw = cast(
            dict[str, object],
            UniswapV2Adapter(chain=chain, api_key=graph_api_key).fetch_markets(
                base_currency_list=base_currency_list,
                quote_currency_list=quote_currency_list,
                base_currency=base_currency_kw,
                min_liquidity=min_liquidity_kw,
            ),
        )
    elif proto == "uniswap_v4":
        raw = cast(
            dict[str, object],
            UniswapV4Adapter(chain=chain, api_key=graph_api_key).fetch_markets(
                base_currency_list=base_currency_list,
                quote_currency_list=quote_currency_list,
                base_currency=base_currency_kw,
            ),
        )
    elif proto == "curve":
        raw = cast(
            dict[str, object], CurveAdapter(chain=chain if chain else "ETHEREUM").fetch_markets(target_date=target_date)
        )
    elif proto == "ethena":
        # EthenaAdapter.fetch_yield_bearing_instruments() returns list[dict], not dict
        raw = await EthenaAdapter(chain=chain).fetch_yield_bearing_instruments()
    elif proto == "euler_plasma":
        raw = cast(
            dict[str, object], EulerAdapter(chain=chain if chain else "ETHEREUM").fetch_markets(target_date=target_date)
        )
    elif proto == "fluid_plasma":
        raw = cast(
            dict[str, object], FluidAdapter(chain=chain if chain else "ETHEREUM").fetch_markets(target_date=target_date)
        )
    elif proto == "aave_plasma":
        raw = cast(
            dict[str, object],
            AavePlasmaAdapter(chain=chain if chain else "ETHEREUM", graph_api_key=graph_api_key).fetch_markets(
                target_date=target_date
            ),
        )
    else:
        return None

    # Normalize: some adapters return list[dict] instead of dict[str, dict].
    # Convert lists to keyed dicts so downstream code can call .items() safely.
    if isinstance(raw, list):
        return _list_to_dict(raw)
    return raw


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
