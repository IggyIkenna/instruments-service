"""Compound V3 (Comet) reference data adapter — instrument discovery via The Graph.

Discovers Compound V3 lending markets across Ethereum, Arbitrum, Base, Polygon,
Optimism, and Scroll. Each Comet market has a single base asset (e.g. USDC) that
can be supplied and borrowed, with multiple collateral assets.

Markets are returned as InstrumentRecord with instrument_type="LENDING".

Data source: The Graph (Messari Compound V3 subgraph).
Reference: https://docs.compound.finance/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import get_subgraph_id
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import classify_graph_error
from ...utils.evm_creation_resolver import block_to_timestamp, get_protocol_floor_date

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Per-chain deploy dates now in evm_creation_resolver.LENDING_PROTOCOL_DEPLOY_DATES

# Compound V3 uses Paperclip Labs community subgraph schema.
# Each "market" represents a Comet deployment (one base asset + collateral assets).
_MARKETS_QUERY = """
{
    markets(first: 100) {
        id
        cometProxy
        creationBlockNumber
        configuration {
            name
            symbol
            baseToken {
                token { id symbol name decimals }
                lastPriceUsd
            }
        }
        accounting {
            totalBaseSupplyUsd
            totalBaseBorrowUsd
            supplyApr
            borrowApr
        }
    }
}
"""


class CompoundV3ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Compound V3 (Comet) reference data: lending market discovery from The Graph.

    Each Compound V3 market produces two instruments:
    - SUPPLY: supply-side instrument (lend base asset to earn interest)
    - BORROW: borrow-side instrument (borrow base asset against collateral)
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
        date: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()
        self._date = date

    @property
    def venue(self) -> str:
        return "compound_v3"

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        if not api_key:
            logger.warning("CompoundV3: missing API key for The Graph")
            return None

        subgraph_id = get_subgraph_id("compound_v3", self._chain)
        if not subgraph_id:
            logger.warning(
                "CompoundV3: no subgraph ID for chain %s in UAC SUBGRAPH_IDS",
                self._chain,
            )
            return None

        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Compound V3 lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        try:
            async with (
                self._make_session() as session,
                session.post(
                    url,
                    json={"query": _MARKETS_QUERY},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            self._log_fetch_error(exc)
            raise ConnectionError(str(exc)) from exc

        raw_data = data.get("data") or {}
        markets: list[dict[str, object]] = raw_data.get("markets") or []
        results: list[InstrumentRecord] = []
        venue_tag = f"COMPOUNDV3-{self._chain}"

        # Resolve creationBlockNumber → timestamp for each market
        block_ts_map: dict[int, datetime] = {}
        block_numbers = [int(str(m.get("creationBlockNumber", 0))) for m in markets if m.get("creationBlockNumber")]
        for bn in set(block_numbers):
            ts = await block_to_timestamp(bn, self._chain)
            if ts:
                block_ts_map[bn] = ts

        for market in markets:
            creation_block = int(str(market.get("creationBlockNumber", 0))) if market.get("creationBlockNumber") else 0
            available_since = block_ts_map.get(creation_block) or get_protocol_floor_date("compound_v3", self._chain)
            results.extend(self._build_market_records(market, venue_tag, available_since))

        logger.info(
            "CompoundV3: fetched %d lending market instruments on %s",
            len(results),
            self._chain,
        )
        return results

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Compound V3."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error("compound_v3", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "CompoundV3 markets query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "compound_v3",
                "endpoint": "thegraph_markets",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    def _build_market_records(
        self,
        market: dict[str, object],
        venue_tag: str,
        available_since: datetime | None = None,
    ) -> list[InstrumentRecord]:
        """Build InstrumentRecord entries for a single Compound V3 market.

        Paperclip Labs schema: market.configuration.baseToken.token has symbol/name.
        """
        market_id = market.get("id") or market.get("cometProxy")
        config = market.get("configuration") or {}
        if not market_id or not isinstance(config, dict):
            return []

        base_token_obj = config.get("baseToken") or {}
        if not isinstance(base_token_obj, dict):
            return []
        token = base_token_obj.get("token") or {}
        if not isinstance(token, dict):
            return []

        symbol = str(token.get("symbol", ""))
        if not symbol:
            return []

        sym_upper = symbol.upper()

        if available_since is None:
            available_since = get_protocol_floor_date("compound_v3", self._chain)

        market_name = str(config.get("name", ""))

        # DeFi metadata: surface Comet proxy + base token ERC-20 info on
        # InstrumentRecord (Phase 2a of
        # instruments_service_metadata_refactor_2026_04_29). pool_address =
        # cometProxy (the deployment) so MTDS lending_indices handlers can
        # call getSupplyRate / getBorrowRate without re-querying The Graph.
        comet_proxy = market.get("cometProxy")
        pool_address: str | None = str(comet_proxy) if comet_proxy else str(market_id) if market_id else None
        underlying_addr_obj = token.get("id")
        underlying_addr: str | None = str(underlying_addr_obj) if underlying_addr_obj else None
        decimals_raw = token.get("decimals")
        decimals_int: int | None
        if isinstance(decimals_raw, int):
            decimals_int = decimals_raw
        elif isinstance(decimals_raw, str) and decimals_raw.isdigit():
            decimals_int = int(decimals_raw)
        else:
            decimals_int = None

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": market_name or str(market_id),
            "instrument_type": InstrumentType.LENDING,
            "base_asset": sym_upper,
            "quote_asset": "",
            "tick_size": Decimal("0.000001"),
            "min_size": Decimal("0.000001"),
            "contract_size": Decimal("1"),
            "expiry": None,
            "strike": None,
            "option_type": None,
            "status": InstrumentStatus.ACTIVE,
            "underlying": sym_upper,
            "available_from_datetime": available_since,
            # DeFi metadata
            "pool_address": pool_address,
            "base_asset_contract_address": underlying_addr,
            "base_asset_decimals": decimals_int,
            "base_asset_symbol_onchain": symbol or None,
        }

        # Supply instrument (lend base asset)
        supply_symbol = f"C{sym_upper}"
        results = [
            InstrumentRecord(
                instrument_key=f"{venue_tag}:SUPPLY:{supply_symbol}",
                **base_kwargs,
            )
        ]

        # Borrow instrument (borrow base asset against collateral)
        borrow_symbol = f"BORROW{sym_upper}"
        results.append(
            InstrumentRecord(
                instrument_key=f"{venue_tag}:BORROW:{borrow_symbol}",
                **base_kwargs,
            )
        )

        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Compound V3 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Compound V3 lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Compound V3 lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Compound V3 OHLCV not supported via reference data")
