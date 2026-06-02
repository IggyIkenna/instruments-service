"""Spark reference data adapter — instrument discovery via Spark subgraph.

Spark is a MakerDAO-forked Aave V3-style lending market. The Spark subgraph
hosted on The Graph (id ``GbKdmBe4ycCYCQLQSjqGg6UHYoYfbyJyq5WrG35pv1si``)
follows the **Messari standardized lending schema** (entity ``Market``)
rather than the Aave-V3 ``Reserve`` schema. Field names and scaling differ
materially from ``aave_v3.py`` so Spark needs its own adapter.

Each Spark market produces two ``InstrumentRecord`` entries with
``instrument_type=InstrumentType.LENDING``:

- ``A_TOKEN`` (the spToken / supply position)
- ``DEBT_TOKEN`` (the variable-debt position) — only when borrowing is enabled

DeFi metadata fields are populated to mirror the aave_v3 adapter's Phase 2a
behaviour:

* ``pool_address``        — Messari ``market.id`` (the underlying reserve key)
* ``base_asset_contract_address`` — ``inputToken.id`` (underlying ERC-20)
* ``base_asset_decimals`` — ``inputToken.decimals``
* ``base_asset_symbol_onchain`` — ``inputToken.symbol``
* ``atoken_address``      — ``outputToken.id`` (the spToken)
* ``debt_token_address``  — ``_vToken.id`` (variable-debt receipt token)

Reserve risk parameters (``maximumLTV`` / ``liquidationThreshold`` /
``liquidationPenalty``) are surfaced via the existing record fields used by
downstream consumers — Spark expresses these as integer percentages
(e.g. ``"83"`` = 83%) which differ from Aave V3's reserve schema scaling.

Reference: https://spark.fi/
Subgraph schema: Messari ``lending``.
"""

from __future__ import annotations

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
from ...utils import date_to_block
from ...utils.defi_utils import assert_subgraph_payload, classify_graph_error
from ...utils.evm_creation_resolver import (
    batch_resolve_evm_creation_timestamps,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"
_PROTOCOL_SLUG = "spark"
_VENUE_PREFIX = "SPARK"

# Messari standardized lending schema query. Spark exposes Aave-V3-fork
# specific underscore-prefixed fields (`_vToken`, `_sToken`) that we use to
# surface variable-debt receipt token addresses on the InstrumentRecord.
_MARKETS_QUERY_TEMPLATE = """
query GetSparkMarkets {{
    markets({block_clause}first: 100, where: {{ isActive: true }}) {{
        id
        name
        isActive
        canBorrowFrom
        canUseAsCollateral
        maximumLTV
        liquidationThreshold
        liquidationPenalty
        reserveFactor
        createdTimestamp
        inputToken {{ id symbol name decimals }}
        outputToken {{ id symbol name decimals }}
        _vToken {{ id symbol }}
    }}
}}
"""


class SparkReferenceDataAdapter(BaseReferenceDataAdapter):
    """Spark reference data: lending market discovery via Messari subgraph.

    Each active Spark market yields a supply (``A_TOKEN``) instrument and,
    when ``canBorrowFrom`` is true, a borrow (``DEBT_TOKEN``) instrument.
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
        """Return the venue identifier."""
        return _PROTOCOL_SLUG

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        if not api_key:
            logger.warning("%s: missing API key for The Graph", _PROTOCOL_SLUG)
            return None

        subgraph_id = get_subgraph_id(_PROTOCOL_SLUG, self._chain)
        if not subgraph_id:
            logger.warning(
                "%s: no subgraph ID for chain %s in UAC SUBGRAPH_IDS",
                _PROTOCOL_SLUG,
                self._chain,
            )
            return None

        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Spark lending markets as instruments."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f"block: {{number: {block_num}}}, " if block_num else ""
        query = _MARKETS_QUERY_TEMPLATE.format(block_clause=block_clause)

        try:
            async with (
                self._make_session() as session,
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
            raise ConnectionError(str(exc)) from exc

        # A 200-with-``{"errors":[...]}`` / missing-``data`` response is a TRANSIENT FETCH FAILURE, not an
        # empty universe — raise so discovery records it ``attempted_failed`` rather than masking it as zero
        # markets (DeFi-plan A8 / operator 2026-05-07). ConnectionError → per-venue NETWORK/retryable.
        data_field = assert_subgraph_payload(data, venue=_VENUE_PREFIX, chain=self._chain)
        markets: list[dict[str, object]] = data_field.get("markets", [])
        venue_tag = f"{_VENUE_PREFIX}-{self._chain}"

        # Collect spToken (outputToken) addresses for creation timestamp
        # resolution. Spark deploys a fresh outputToken contract per market;
        # binary-searching its eth_getCode is the most accurate available_from
        # signal we can derive without a per-market on-chain RPC tour.
        sptoken_addresses: list[str] = []
        for market in markets:
            output_obj = market.get("outputToken")
            if isinstance(output_obj, dict):
                output_addr = str(output_obj.get("id", ""))
                if output_addr:
                    sptoken_addresses.append(output_addr)

        creation_ts_map: dict[str, datetime] = {}
        if sptoken_addresses:
            creation_ts_map = await batch_resolve_evm_creation_timestamps(
                sptoken_addresses,
                self._chain,
            )

        floor_date = get_protocol_floor_date(_PROTOCOL_SLUG, self._chain)
        results: list[InstrumentRecord] = []
        for market in markets:
            output_obj = market.get("outputToken")
            output_addr = str(output_obj.get("id", "")) if isinstance(output_obj, dict) else ""
            available_since = creation_ts_map.get(output_addr, floor_date)
            results.extend(self._build_market_records(market, venue_tag, available_since))

        logger.info("Spark: fetched %d lending market instruments on %s", len(results), self._chain)
        return results

    async def _resolve_block_num(self) -> int | None:
        """Resolve the historical block number from self._date, if set."""
        if not self._date:
            return None
        block_num = await date_to_block(self._date, chain=self._chain)
        if block_num:
            logger.debug(
                "Spark: querying at historical block %d for date %s",
                block_num,
                self._date,
            )
        return block_num

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Spark."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error(_PROTOCOL_SLUG, error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Spark markets query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": _PROTOCOL_SLUG,
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
        """Build InstrumentRecord entries for a single Spark market."""
        input_obj = market.get("inputToken")
        if not isinstance(input_obj, dict):
            return []
        symbol_raw = input_obj.get("symbol", "")
        underlying = input_obj.get("id", "")
        symbol = str(symbol_raw) if symbol_raw else ""
        underlying_str = str(underlying) if underlying else ""
        if not symbol or not underlying_str:
            return []

        if available_since is None:
            available_since = get_protocol_floor_date(_PROTOCOL_SLUG, self._chain)

        sym_upper = symbol.upper()

        # DeFi metadata: surface spToken / vToken / underlying ERC-20
        # information from the subgraph onto InstrumentRecord (mirrors
        # aave_v3.py Phase 2a). Spark's Messari schema places the supply
        # receipt token (``spToken``) under ``outputToken``, the underlying
        # ERC-20 under ``inputToken``, and the variable-debt receipt token
        # under the protocol-specific ``_vToken`` field.
        output_obj = market.get("outputToken")
        atoken_addr: str | None = None
        if isinstance(output_obj, dict):
            output_id = output_obj.get("id")
            if output_id:
                atoken_addr = str(output_id)

        decimals_raw = input_obj.get("decimals")
        decimals_int: int | None
        if isinstance(decimals_raw, int):
            decimals_int = decimals_raw
        elif isinstance(decimals_raw, str) and decimals_raw.isdigit():
            decimals_int = int(decimals_raw)
        else:
            logger.warning("Spark: skipping market %s — invalid/missing decimals %r", market.get("id"), decimals_raw)
            return []

        market_id = str(market.get("id", "")) or None

        v_token_obj = market.get("_vToken")
        debt_token_addr: str | None = None
        if isinstance(v_token_obj, dict):
            v_token_id = v_token_obj.get("id")
            if v_token_id:
                debt_token_addr = str(v_token_id)

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": underlying_str,
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
            "pool_address": market_id,
            "base_asset_contract_address": underlying_str,
            "base_asset_decimals": decimals_int,
            "base_asset_symbol_onchain": symbol or None,
            "atoken_address": atoken_addr,
            "debt_token_address": debt_token_addr,
        }

        a_symbol = f"A{sym_upper}"
        results: list[InstrumentRecord] = [
            InstrumentRecord(
                instrument_key=f"{venue_tag}:A_TOKEN:{a_symbol}",
                **base_kwargs,
            )
        ]

        if bool(market.get("canBorrowFrom", False)):
            debt_symbol = f"DEBT{sym_upper}"
            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:DEBT_TOKEN:{debt_symbol}",
                    **base_kwargs,
                )
            )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key.endswith(f":{symbol}"):
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Spark does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Spark lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Spark lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Spark OHLCV not supported via reference data")
