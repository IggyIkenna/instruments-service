"""Aave V3 reference data adapter — instrument discovery via Aave subgraph.

Discovers Aave V3 lending markets (aToken and debtToken instruments) on Ethereum.
Markets are returned as InstrumentRecord with instrument_type="lending_market".

Data source: The Graph (Aave V3 subgraph).
Reference: https://aave.com/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import DEFI_MAJOR_ASSET_SYMBOLS, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.registry import get_subgraph_id
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ..utils import date_to_block
from ..utils.defi_utils import classify_graph_error

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Aave V3 Ethereum mainnet deployment date (2023-01-27).
# The Graph Aave V3 subgraph does not expose a per-reserve createdTimestamp
# field, so we use the protocol launch date as the available_since floor for
# all reserves on this chain.
_AAVE_V3_DEPLOY_DATE = datetime(2023, 1, 27, tzinfo=UTC)

# Query template — {block_clause} is replaced with '' or 'block: {number: N}, '
_RESERVES_QUERY_TEMPLATE = """
query GetReserves {{
    reserves({block_clause}first: 100, where: {{ isActive: true }}) {{
        id
        underlyingAsset
        symbol
        name
        decimals
        baseLTVasCollateral
        reserveLiquidationThreshold
        reserveLiquidationBonus
        reserveFactor
        usageAsCollateralEnabled
        borrowingEnabled
        isActive
        isFrozen
        isPaused
    }}
}}
"""


class AaveV3ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Aave V3 reference data: lending market discovery from The Graph subgraph.

    Each Aave V3 reserve produces two instruments:
    - A_TOKEN (aToken): supply/collateral instrument
    - DEBT_TOKEN (debtToken): borrow instrument
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
        return "aave_v3"

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        if not api_key:
            logger.warning("AaveV3: missing API key for The Graph")
            return None

        subgraph_id = get_subgraph_id("aave_v3", self._chain)
        if not subgraph_id:
            logger.warning("AaveV3: no subgraph ID for chain %s in UAC SUBGRAPH_IDS", self._chain)
            return None

        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Aave V3 lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f"block: {{number: {block_num}}}, " if block_num else ""
        query = _RESERVES_QUERY_TEMPLATE.format(block_clause=block_clause)

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    url,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            self._log_fetch_error(exc)
            return []

        reserves: list[dict[str, object]] = data.get("data", {}).get("reserves", [])
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        venue_tag = f"AAVEV3-{self._chain}"

        for reserve in reserves:
            results.extend(self._build_reserve_records(reserve, venue_tag, now))

        logger.info("AaveV3: fetched %d lending market instruments on %s", len(results), self._chain)
        return results

    async def _resolve_block_num(self) -> int | None:
        """Resolve the historical block number from self._date, if set."""
        if not self._date:
            return None
        block_num = await date_to_block(self._date, chain=self._chain)
        if block_num:
            logger.debug(
                "AaveV3: querying at historical block %d for date %s",
                block_num,
                self._date,
            )
        return block_num

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Aave V3."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error("aave_v3", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "AaveV3 reserves query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "aave_v3",
                "endpoint": "thegraph_reserves",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    def _build_reserve_records(
        self,
        reserve: dict[str, object],
        venue_tag: str,
        now: datetime,
    ) -> list[InstrumentRecord]:
        """Build InstrumentRecord entries for a single Aave reserve."""
        symbol = str(reserve.get("symbol", ""))
        underlying = str(reserve.get("underlyingAsset", ""))
        if not symbol or not underlying:
            return []

        # Filter: only major assets
        if symbol.upper() not in DEFI_MAJOR_ASSET_SYMBOLS:
            return []

        is_frozen = reserve.get("isFrozen", False)
        is_paused = reserve.get("isPaused", False)
        is_active = bool(reserve.get("isActive", True)) and not is_frozen and not is_paused
        sym_upper = symbol.upper()
        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": underlying,
            "instrument_type": "lending_market",
            "base_asset": sym_upper,
            "quote_asset": "",
            "tick_size": Decimal("0.000001"),
            "lot_size": Decimal("0.000001"),
            "min_order_size": Decimal("0"),
            "contract_size": Decimal("1"),
            "settlement_asset": sym_upper,
            "expiry": None,
            "strike": None,
            "option_type": None,
            "is_active": is_active,
            "updated_at": now,
            "underlying": sym_upper,
            "available_since": _AAVE_V3_DEPLOY_DATE,
        }

        a_symbol = f"A{sym_upper}"
        results = [
            InstrumentRecord(
                instrument_key=f"{venue_tag}:A_TOKEN:{a_symbol}",
                symbol=a_symbol,
                **base_kwargs,
            )
        ]

        if reserve.get("borrowingEnabled", False):
            debt_symbol = f"DEBT{sym_upper}"
            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:DEBT_TOKEN:{debt_symbol}",
                    symbol=debt_symbol,
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
        raise NotImplementedError("Aave V3 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Aave V3 lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Aave V3 lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Aave V3 OHLCV not supported via reference data")
