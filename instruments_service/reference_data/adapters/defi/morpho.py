"""Morpho Blue reference data adapter — instrument discovery via Morpho API.

Discovers Morpho Blue isolated lending markets on Ethereum.
Markets are returned as InstrumentRecord with instrument_type="LENDING".

Data source: Morpho Blue GraphQL API (blue-api.morpho.org).
Reference: https://morpho.org/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import assert_subgraph_payload, classify_graph_error
from ...utils.evm_creation_resolver import get_protocol_floor_date

logger = logging.getLogger(__name__)

_MORPHO_API_URL = "https://blue-api.morpho.org/graphql"
_DEFAULT_CHAIN = "ETHEREUM"

# Per-chain deploy dates now in evm_creation_resolver.LENDING_PROTOCOL_DEPLOY_DATES.
# Morpho uniqueKey is bytes32 (not a contract address), so binary-search eth_getCode
# won't work. We use protocol floor dates per chain as the available_since floor.

# Chain name → EVM chain ID for Morpho Blue API filtering.
# Morpho supports these chains via blue-api.morpho.org (chainId_in filter).
_MORPHO_CHAIN_IDS: dict[str, int] = {
    "ETHEREUM": 1,
    "BASE": 8453,
    "ARBITRUM": 42161,
    "OPTIMISM": 10,
    "POLYGON": 137,
}

_MARKETS_QUERY_TEMPLATE = """
query {{
    markets(first: 1000, orderBy: SupplyAssets, orderDirection: Desc, where: {{ chainId_in: [{chain_id}] }}) {{
        items {{
            uniqueKey
            loanAsset {{ address symbol name decimals }}
            collateralAsset {{ address symbol name decimals }}
            lltv
            state {{
                supplyAssets
                borrowAssets
                supplyApy
                borrowApy
            }}
        }}
    }}
}}
"""


class MorphoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Morpho Blue reference data: isolated lending market discovery via Morpho API."""

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "morpho"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Morpho Blue lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        chain_id = _MORPHO_CHAIN_IDS.get(self._chain)
        if chain_id is None:
            logger.warning("Morpho: unsupported chain %s (supported: %s)", self._chain, list(_MORPHO_CHAIN_IDS))
            return []

        query = _MARKETS_QUERY_TEMPLATE.format(chain_id=chain_id)

        try:
            async with (
                self._make_session() as session,
                session.post(
                    _MORPHO_API_URL,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data: dict[str, object] = dict(await resp.json())
                gql_errors = data.get("errors")
                if gql_errors:
                    logger.warning("Morpho GraphQL errors on %s: %s", self._chain, gql_errors)
        except aiohttp.ClientError as exc:
            error_code = classify_graph_error(exc)
            classification = classify_venue_error("morpho", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Morpho API request failed on %s: %s (classified: %s, action: %s, retry_safe: %s)",
                self._chain,
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "morpho",
                    "chain": self._chain,
                    "endpoint": "morpho_api_markets",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            raise ConnectionError(str(exc)) from exc

        # A 200-with-``{"errors":[...]}`` / missing-``data`` response is a TRANSIENT FETCH FAILURE, not an
        # empty universe — raise so discovery records it ``attempted_failed`` rather than masking it as zero
        # markets (DeFi-plan A8 / operator 2026-05-07). ConnectionError → per-venue NETWORK/retryable.
        response_dict = assert_subgraph_payload(data, venue="MORPHO", chain=self._chain)
        markets_data_val: object = response_dict.get("markets")
        if markets_data_val is None:
            raise ConnectionError(
                f"MORPHO-{self._chain}: subgraph 'data' missing 'markets' key — transient fetch failure"
            )

        markets_dict: dict[str, object] = markets_data_val if isinstance(markets_data_val, dict) else {}
        markets_items: object = markets_dict.get("items", [])
        markets: list[dict[str, object]] = markets_items if isinstance(markets_items, list) else []
        results: list[InstrumentRecord] = []
        venue_tag = f"MORPHO-{self._chain}"

        floor_date = get_protocol_floor_date("morpho", self._chain)
        for market in markets:
            record = self._market_to_record(market, venue_tag, floor_date)
            if record is not None:
                results.append(record)

        logger.info(
            "Morpho %s: fetched %d lending markets from %d API results", self._chain, len(results), len(markets)
        )
        return results

    @staticmethod
    def _market_to_record(
        market: dict[str, object],
        venue_tag: str,
        available_since: datetime | None = None,
    ) -> InstrumentRecord | None:
        """Convert a raw Morpho market dict to an InstrumentRecord, or None if filtered."""
        loan_asset = market.get("loanAsset")
        collateral_asset = market.get("collateralAsset")
        if not isinstance(loan_asset, dict) or not isinstance(collateral_asset, dict):
            return None

        loan_symbol = str(loan_asset.get("symbol", ""))
        collateral_symbol = str(collateral_asset.get("symbol", ""))
        market_key = str(market.get("uniqueKey", ""))

        if not collateral_symbol or not market_key:
            return None

        symbol = f"{collateral_symbol}-{loan_symbol}"
        instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}:{market_key[:8]}"

        # DeFi metadata: Morpho Blue uses the bytes32 uniqueKey as the
        # canonical market identifier; surface that as pool_address along
        # with collateral (base) and loan (quote) ERC-20 contract info.
        # Phase 2a of instruments_service_metadata_refactor_2026_04_29.
        coll_addr_obj = collateral_asset.get("address")
        coll_addr: str | None = str(coll_addr_obj) if coll_addr_obj else None
        loan_addr_obj = loan_asset.get("address")
        loan_addr: str | None = str(loan_addr_obj) if loan_addr_obj else None
        coll_decimals_raw = collateral_asset.get("decimals")
        coll_decimals: int | None
        if isinstance(coll_decimals_raw, int):
            coll_decimals = coll_decimals_raw
        elif isinstance(coll_decimals_raw, str) and coll_decimals_raw.isdigit():
            coll_decimals = int(coll_decimals_raw)
        else:
            coll_decimals = None
        loan_decimals_raw = loan_asset.get("decimals")
        loan_decimals: int | None
        if isinstance(loan_decimals_raw, int):
            loan_decimals = loan_decimals_raw
        elif isinstance(loan_decimals_raw, str) and loan_decimals_raw.isdigit():
            loan_decimals = int(loan_decimals_raw)
        else:
            loan_decimals = None

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=market_key,
            instrument_type=InstrumentType.LENDING,
            base_asset=collateral_symbol,
            quote_asset=loan_symbol,
            tick_size=Decimal("0.000001"),
            min_size=Decimal("0.000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=available_since or get_protocol_floor_date("morpho", "ETHEREUM"),
            # DeFi metadata
            pool_address=market_key,
            base_asset_contract_address=coll_addr,
            base_asset_decimals=coll_decimals,
            base_asset_symbol_onchain=collateral_symbol or None,
            quote_asset_contract_address=loan_addr,
            quote_asset_decimals=loan_decimals,
            quote_asset_symbol_onchain=loan_symbol or None,
        )

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
        raise NotImplementedError("Morpho does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Morpho lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Morpho lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Morpho OHLCV not supported via reference data")
