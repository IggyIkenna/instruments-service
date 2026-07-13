"""Aave V3 reference data adapter — instrument discovery via Aave subgraph.

Discovers Aave V3 lending markets (aToken and debtToken instruments) on Ethereum.
Markets are returned as two InstrumentRecords per reserve: instrument_type=A_TOKEN
(supply side) and instrument_type=DEBT_TOKEN (borrow side, when borrowingEnabled).

Data source: The Graph (Aave V3 subgraph).
Reference: https://aave.com/
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
from ...utils import date_to_block
from ...utils.defi_utils import assert_subgraph_payload, classify_graph_error
from ...utils.evm_creation_resolver import (
    batch_resolve_evm_creation_timestamps,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"

# Per-chain deploy dates now in evm_creation_resolver.LENDING_PROTOCOL_DEPLOY_DATES.
# Per-reserve creation is resolved dynamically via binary search on the
# aToken contract address (eth_getCode).

# OPTIMISM: Aave abandoned their subgraph deployment (republished empty v0.0.5).
# Canonical source for IS instrument discovery is the static registry below
# (same 7 reserves that MTDS fetches via RPC fallback daily).
# Underlying asset + aToken addresses: https://app.aave.com/reserve-overview/
#   USDC  : proto_optimism_v3 PoolAddressesProvider 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb
# Updated 2026-06-19 from Aave V3 Optimism public deployment.
_AAVE_V3_OPTIMISM_STATIC_RESERVES: list[dict[str, object]] = [
    {
        "id": "0x625e7708f30ca75bfd92586e17077590c60eb4cd0xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "USDC",
        "name": "USD Coin",
        "underlyingAsset": "0x7f5c764cbc14f9669b88837ca1490cca17c31607",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 6,
        "borrowingEnabled": True,
        "baseLTVasCollateral": "7500",
        "reserveLiquidationThreshold": "7800",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x625e7708f30ca75bfd92586e17077590c60eb4cd"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0x82e64f49ed5ec1bc6e43dad4fc8af9bb3a2312ee0xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "DAI",
        "name": "Dai Stablecoin",
        "underlyingAsset": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 18,
        "borrowingEnabled": True,
        "baseLTVasCollateral": "7500",
        "reserveLiquidationThreshold": "8000",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x82e64f49ed5ec1bc6e43dad4fc8af9bb3a2312ee"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0x6ab707aca953edaefbc4fd23ba73294241490620a97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "USDT",
        "name": "Tether USD",
        "underlyingAsset": "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 6,
        "borrowingEnabled": True,
        "baseLTVasCollateral": "7500",
        "reserveLiquidationThreshold": "7800",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x6ab707aca953edaefbc4fd23ba73294241490620"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c80xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "WETH",
        "name": "Wrapped Ether",
        "underlyingAsset": "0x4200000000000000000000000000000000000006",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 18,
        "borrowingEnabled": True,
        "baseLTVasCollateral": "8000",
        "reserveLiquidationThreshold": "8300",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0x078f358208685046a11c85e8ad32895ded33a2490xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "WBTC",
        "name": "Wrapped BTC",
        "underlyingAsset": "0x68f180fcce6836688e9084f035309e29bf0a2095",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 8,
        "borrowingEnabled": True,
        "baseLTVasCollateral": "7000",
        "reserveLiquidationThreshold": "7500",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x078f358208685046a11c85e8ad32895ded33a249"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff0xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "WSTETH",
        "name": "Wrapped liquid staked Ether 2.0",
        "underlyingAsset": "0x1f32b1c2345538c0c6f582fcb022739c4a194ebb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 18,
        "borrowingEnabled": False,
        "baseLTVasCollateral": "7850",
        "reserveLiquidationThreshold": "8100",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
    {
        "id": "0x724dc807b04555b71ed48a6896b6f41593b8c6700xa97684ead0e402dc232d5a977953df7ecbab3cdb",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "symbol": "RETH",
        "name": "Rocket Pool ETH",
        "underlyingAsset": "0x9bcef72be871e61ed4fbbc7630889bee758eb81d",  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        "decimals": 18,
        "borrowingEnabled": False,
        "baseLTVasCollateral": "7450",
        "reserveLiquidationThreshold": "7700",
        "isActive": True,
        "isFrozen": False,
        "isPaused": False,
        "aToken": {
            "id": "0x724dc807b04555b71ed48a6896b6f41593b8c670"  # DERIVED 2026-06-19 from optimism aave-v3 app.aave.com/reserve-overview
        },
    },
]

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
        aToken {{ id }}
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
        protocol_slug: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()
        self._date = date
        self._protocol_slug = protocol_slug or "aave_v3"
        # KEEP the underscore — canonical defi venue names are the underscore form (UAC
        # registry/defi_venues.py; e.g. "aave_v3" → "AAVE_V3-<CHAIN>"). Stripping it made the
        # URDI venue filter drop every fetched record (R4-IS-freeze finding 2026-06-11).
        self._venue_prefix = self._protocol_slug.upper()

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._protocol_slug

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        if not api_key:
            logger.warning("%s: missing API key for The Graph", self._protocol_slug)
            return None

        subgraph_id = get_subgraph_id(self._protocol_slug, self._chain)
        if not subgraph_id:
            logger.warning("%s: no subgraph ID for chain %s in UAC SUBGRAPH_IDS", self._protocol_slug, self._chain)
            return None

        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    def _get_optimism_reserves_static(self) -> list[InstrumentRecord]:
        """Return static lending instruments for OPTIMISM (abandoned subgraph fallback)."""
        floor_date = get_protocol_floor_date("aave_v3", self._chain)
        venue_tag = f"{self._venue_prefix}-{self._chain}"
        results: list[InstrumentRecord] = []
        for reserve in _AAVE_V3_OPTIMISM_STATIC_RESERVES:
            results.extend(self._build_reserve_records(reserve, venue_tag, floor_date))
        logger.info(
            "AaveV3: OPTIMISM static fallback — returned %d lending instruments",
            len(results),
        )
        return results

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Aave V3 lending markets as instruments.

        For OPTIMISM: routes to static reserve registry (abandoned subgraph).
        """
        if instrument_type not in (None, InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN):
            return []

        if self._chain == "OPTIMISM":
            return self._get_optimism_reserves_static()

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f"block: {{number: {block_num}}}, " if block_num else ""
        query = _RESERVES_QUERY_TEMPLATE.format(block_clause=block_clause)

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
        # empty universe — raise so the discovery caller records it ``attempted_failed`` (honest gap) rather
        # than silently masking it as zero reserves (DeFi-plan A8 / operator 2026-05-07). ConnectionError is
        # caught per-venue in urdi_reference_provider → classified NETWORK/retryable.
        data_field = assert_subgraph_payload(data, venue=self._venue_prefix, chain=self._chain)
        reserves: list[dict[str, object]] = data_field.get("reserves", [])
        venue_tag = f"{self._venue_prefix}-{self._chain}"

        # Collect aToken addresses for creation timestamp resolution
        atoken_addresses: list[str] = []
        atoken_to_reserve_idx: dict[str, int] = {}
        for i, reserve in enumerate(reserves):
            atoken_obj = reserve.get("aToken")
            if isinstance(atoken_obj, dict):
                atoken_addr = str(atoken_obj.get("id", ""))
                if atoken_addr:
                    atoken_addresses.append(atoken_addr)
                    atoken_to_reserve_idx[atoken_addr] = i

        # Batch-resolve aToken creation timestamps (cached — only RPC on first run)
        creation_ts_map: dict[str, datetime] = {}
        if atoken_addresses:
            creation_ts_map = await batch_resolve_evm_creation_timestamps(
                atoken_addresses,
                self._chain,
            )

        floor_date = get_protocol_floor_date("aave_v3", self._chain)
        results: list[InstrumentRecord] = []
        for _i, reserve in enumerate(reserves):
            # Find the creation timestamp for this reserve's aToken
            atoken_obj = reserve.get("aToken")
            atoken_addr = str(atoken_obj.get("id", "")) if isinstance(atoken_obj, dict) else ""
            available_since = creation_ts_map.get(atoken_addr, floor_date)
            results.extend(self._build_reserve_records(reserve, venue_tag, available_since))

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
        available_since: datetime | None = None,
    ) -> list[InstrumentRecord]:
        """Build InstrumentRecord entries for a single Aave reserve."""
        symbol = str(reserve.get("symbol", ""))
        underlying = str(reserve.get("underlyingAsset", ""))
        if not symbol or not underlying:
            return []

        if available_since is None:
            available_since = get_protocol_floor_date("aave_v3", self._chain)

        sym_upper = symbol.upper()

        # DeFi metadata: surface aToken / debtToken / underlying ERC-20
        # information from the subgraph onto InstrumentRecord (Phase 2a of
        # instruments_service_metadata_refactor_2026_04_29). Aave V3 reserves
        # don't expose a separate poolAddress on the subgraph schema we query
        # — the reserve.id IS the canonical reserve identifier (lower-cased
        # encoded address pair); we set pool_address = reserve.id so MTDS
        # liquidations / lending_indices handlers don't have to re-query.
        atoken_obj = reserve.get("aToken")
        atoken_addr = str(atoken_obj.get("id", "")) if isinstance(atoken_obj, dict) and atoken_obj.get("id") else None
        decimals_raw = reserve.get("decimals")
        decimals_int: int | None
        if isinstance(decimals_raw, int):
            decimals_int = decimals_raw
        elif isinstance(decimals_raw, str) and decimals_raw.isdigit():
            decimals_int = int(decimals_raw)
        else:
            decimals_int = None
        reserve_id = str(reserve.get("id", "")) or None
        # debt token address is not currently returned by the reserves query;
        # set None for now (Phase 2a leaves room for future query expansion).
        debt_token_addr: str | None = None

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": underlying,
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
            "pool_address": reserve_id,
            "base_asset_contract_address": underlying,
            "base_asset_decimals": decimals_int,
            "base_asset_symbol_onchain": symbol or None,
            "atoken_address": atoken_addr,
            "debt_token_address": debt_token_addr,
        }

        a_symbol = f"A{sym_upper}"
        results = [
            InstrumentRecord(
                instrument_key=f"{venue_tag}:A_TOKEN:{a_symbol}",
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            )
        ]

        if reserve.get("borrowingEnabled", False):
            debt_symbol = f"DEBT{sym_upper}"
            results.append(
                InstrumentRecord(
                    instrument_key=f"{venue_tag}:DEBT_TOKEN:{debt_symbol}",
                    instrument_type=InstrumentType.DEBT_TOKEN,
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
        raise NotImplementedError("Aave V3 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Aave V3 lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Aave V3 lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Aave V3 OHLCV not supported via reference data")
