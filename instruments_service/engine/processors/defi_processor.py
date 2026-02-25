"""
DeFi Instruments Processor

Fetches DeFi instruments from various protocols.
Extracted from InstrumentProcessingService.fetch_defi_instruments.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

from unified_market_interface import (
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
        instruments: dict[str, Any],
        target_date: datetime,
        protocol: str | None = None,
    ) -> dict[str, Any]: ...


class _CCXTServiceProtocol(Protocol):
    """Protocol for ccxt_service attribute."""

    def load_markets(self, venue: str, force_refresh: bool = False) -> dict[str, Any] | None: ...

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

    def get_manual_ccxt_fallback(self, venue: str, base_asset: str) -> dict[str, Any]: ...


def fetch_defi_instruments(
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
        base_currency_list: list[str] = service.venue_mapping.get_defi_mvp_tokens()
        quote_currency_list: list[str] = service.venue_mapping.get_defi_mvp_tokens()
        graph_api_key: str | None = getattr(service, "_graph_api_key", None)

        # Extract fetch_markets/fetch_pools params from kwargs (exclude target_date)
        bc = kwargs.get("base_currency")
        base_currency_kw: str | None = bc if isinstance(bc, str) else None
        ml = kwargs.get("min_liquidity")
        min_liquidity_kw: float | None = float(ml) if isinstance(ml, (int, float)) else None

        raw_instruments: dict[str, Any] = {}

        if protocol.lower() == "uniswap_v3":
            adapter = UniswapV3Adapter(chain=chain, api_key=graph_api_key)
            raw_instruments = cast(
                dict[str, Any],
                asyncio.run(
                    adapter.fetch_pools(
                        base_currency_list=base_currency_list,
                        quote_currency_list=quote_currency_list,
                        base_currency=base_currency_kw,
                        min_liquidity=min_liquidity_kw,
                    )
                ),
            )
        elif protocol.lower() == "balancer":
            adapter = BalancerAdapter(chain=chain)
            raw_instruments = cast(
                dict[str, Any],
                adapter.fetch_markets(
                    base_currency_list=base_currency_list,
                    quote_currency_list=quote_currency_list,
                    base_currency=base_currency_kw,
                    min_liquidity=min_liquidity_kw,
                ),
            )
        elif protocol.lower() == "aave_v3":
            adapter = AaveV3Adapter(chain=chain, graph_api_key=graph_api_key)
            raw_instruments = cast(dict[str, Any], adapter.fetch_markets(target_date=target_date))
        elif protocol.lower() == "etherfi":
            adapter = EtherFiAdapter(chain=chain)
            raw_instruments = cast(dict[str, Any], adapter.fetch_lst_instruments())
        elif protocol.lower() == "lido":
            adapter = LidoAdapter(chain=chain)
            raw_instruments = cast(dict[str, Any], adapter.fetch_lst_instruments())
        elif protocol.lower() == "morpho":
            adapter = MorphoAdapter(chain=chain)
            raw_instruments = cast(dict[str, Any], adapter.fetch_markets())
        elif protocol.lower() == "hyperliquid":
            hyperliquid_base_assets: list[str] = service.venue_mapping.hyperliquid_aster_mvp_base_assets
            adapter = HyperliquidAdapter(base_currency_list=hyperliquid_base_assets)
            perpetuals = cast(dict[str, Any], adapter.fetch_perpetuals(test_data_availability=False))
            spot_pairs = cast(dict[str, Any], adapter.fetch_spot_pairs(test_data_availability=False))
            raw_instruments = {**perpetuals, **spot_pairs}
        elif protocol.lower() == "aster":
            raise NotImplementedError(
                "Aster adapter not available (AsterBaseClient removed from UCS). "
                "Use Hyperliquid or other on-chain perpetual venues."
            )
        elif protocol.lower() == "uniswap_v2":
            adapter = UniswapV2Adapter(chain=chain, api_key=graph_api_key)
            raw_instruments = cast(
                dict[str, Any],
                adapter.fetch_markets(
                    base_currency_list=base_currency_list,
                    quote_currency_list=quote_currency_list,
                    base_currency=base_currency_kw,
                    min_liquidity=min_liquidity_kw,
                ),
            )
        elif protocol.lower() == "uniswap_v4":
            adapter = UniswapV4Adapter(chain=chain, api_key=graph_api_key)
            raw_instruments = cast(
                dict[str, Any],
                adapter.fetch_markets(
                    base_currency_list=base_currency_list,
                    quote_currency_list=quote_currency_list,
                    base_currency=base_currency_kw,
                ),
            )
        elif protocol.lower() == "curve":
            adapter = CurveAdapter(chain=chain if chain else "ETHEREUM")
            raw_instruments = cast(dict[str, Any], adapter.fetch_markets(target_date=target_date))
        elif protocol.lower() == "ethena":
            adapter = EthenaAdapter(chain=chain)
            raw_instruments = cast(dict[str, Any], adapter.fetch_yield_bearing_instruments())
        elif protocol.lower() == "euler_plasma":
            adapter = EulerAdapter(chain=chain if chain else "ETHEREUM")
            raw_instruments = cast(dict[str, Any], adapter.fetch_markets(target_date=target_date))
        elif protocol.lower() == "fluid_plasma":
            adapter = FluidAdapter(chain=chain if chain else "ETHEREUM")
            raw_instruments = cast(dict[str, Any], adapter.fetch_markets(target_date=target_date))
        else:
            logger.debug(f"DeFi protocol '{protocol}' not yet implemented, skipping")
            return {}

        if target_date:
            raw_instruments = cast(
                dict[str, Any],
                service.date_filter_service.filter_instruments_by_date(
                    instruments=raw_instruments,
                    target_date=target_date,
                    protocol=protocol,
                ),
            )

        venues_to_enrich: set[str] = set()
        for inst_data in raw_instruments.values():  # type: ignore[reportAny]
            if protocol.lower() == "hyperliquid" and isinstance(inst_data, dict):
                inst_d: dict[str, Any] = cast(dict[str, Any], inst_data)
                venue_val: str | None = cast(str | None, inst_d.get("venue"))
                if isinstance(venue_val, str):
                    venues_to_enrich.add(venue_val)

        for venue in venues_to_enrich:
            logger.info(f"⚡ Pre-loading CCXT markets for {venue} to ensure enrichment works")
            ccxt_data = service.ccxt_service.load_markets(venue)
            if ccxt_data and ccxt_data.get("markets"):
                logger.info(
                    f"✅ CCXT markets loaded for {venue}: {len(cast(dict[str, Any], ccxt_data['markets']))} markets"
                )
            else:
                logger.warning(f"⚠️ Failed to load CCXT markets for {venue}")

        instruments: dict[str, InstrumentDefinition] = {}

        if protocol.lower() in ["hyperliquid", "aster"]:
            mvp_bases = {str(b).upper() for b in service.venue_mapping.hyperliquid_aster_mvp_base_assets}
            mvp_quotes: set[str] = {"USDC"}
        elif protocol.lower() == "ethena":
            mvp_bases = {"USDE", "SUSDE"}
            mvp_quotes = {"USDE", "SUSDE", ""}
        else:
            mvp_quotes = {str(q).upper() for q in quote_currency_list}
            mvp_bases = {str(b).upper() for b in base_currency_list}

        base_versions = {
            "WETH": "ETH",
            "WSTETH": "ETH",
            "WEETH": "ETH",
            "STETH": "ETH",
        }

        for inst_key, inst_data in raw_instruments.items():  # type: ignore[reportAny]
            try:
                inst_data_dict: dict[str, Any] = cast(dict[str, Any], inst_data) if isinstance(inst_data, dict) else {}
                base_asset: str = str(inst_data_dict.get("base_asset") or "").upper()
                if (
                    base_asset
                    and base_asset not in mvp_bases
                    and (base_asset not in base_versions or base_versions[base_asset] not in mvp_bases)
                ):
                    logger.debug(f"Skipping {inst_key}: base currency '{base_asset}' not in MVP list")
                    continue

                quote_asset: str = str(inst_data_dict.get("quote_asset") or "").upper()
                if quote_asset and quote_asset not in mvp_quotes:
                    quote_versions = {
                        "WETH": "ETH",
                        "WSTETH": "ETH",
                        "WEETH": "ETH",
                        "STETH": "ETH",
                    }
                    if quote_asset not in quote_versions or quote_versions[quote_asset] not in mvp_quotes:
                        logger.debug(f"Skipping {inst_key}: quote currency '{quote_asset}' not in MVP list")
                        continue

                inst_def = InstrumentDefinition(**inst_data_dict)  # type: ignore[reportAny]

                if protocol.lower() == "hyperliquid":
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
                    else:
                        if not inst_def.ccxt_symbol or not inst_def.ccxt_exchange:
                            default_ccxt_symbol = service.ccxt_service.generate_default_ccxt_symbol(
                                venue=venue,
                                base_asset=inst_def.base_asset,
                                quote_asset=inst_def.quote_asset,
                                symbol_id=inst_def.exchange_raw_symbol or inst_def.symbol,
                                instrument_type=inst_def.instrument_type,
                            )
                            ccxt_exchange_id = (
                                cast(dict[str, str], getattr(service.venue_mapping, "venue_to_ccxt", {})).get(venue)
                                or ""
                            )
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
                                    inst_def.contract_size = float(
                                        cast(str | int | float, manual_metadata["contract_size"])
                                    )
                            logger.debug(
                                f"✅ Used manual fallback for {inst_def.instrument_key}: "
                                f"tick_size={inst_def.tick_size}, "
                                f"min_size={inst_def.min_size}"
                            )
                        else:
                            logger.debug(
                                f"⚠️ CCXT enrichment failed for {inst_def.instrument_key}: "
                                f"venue={venue}, base={inst_def.base_asset}, "
                                f"quote={inst_def.quote_asset}"
                            )

                instruments[inst_key] = inst_def
            except Exception as e:
                logger.warning(f"Failed to create InstrumentDefinition for {inst_key}: {e}")
                continue

        logger.info(f"✅ Fetched {len(instruments)} {protocol} instruments for {chain}")
        return instruments

    except Exception as e:
        logger.error(f"Failed to fetch {protocol} instruments: {e}")
        return {}
