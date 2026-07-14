"""Morpho Blue reference data adapter — instrument discovery via Morpho API.

Discovers Morpho Blue isolated lending markets on Ethereum. Each market emits
a supply-side ``A_TOKEN`` instrument (collateral deposited into the market)
and a borrow-side ``DEBT_TOKEN`` instrument (loan asset borrowed against that
collateral) — the same A_TOKEN/DEBT_TOKEN split ``aave_v3.py`` uses per
reserve (defi_lending_atoken_debttoken_instrument_split_2026_07_07.md).

Data source: Morpho Blue GraphQL API (blue-api.morpho.org).
Reference: https://morpho.org/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import AssetGroup, build_canonical_instrument_id, classify_venue_error
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
            marketId
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
        if instrument_type not in (None, InstrumentType.A_TOKEN, InstrumentType.DEBT_TOKEN):
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
            results.extend(self._market_to_records(market, venue_tag, self._chain, floor_date))

        logger.info(
            "Morpho %s: fetched %d lending/debt instruments from %d API results",
            self._chain,
            len(results),
            len(markets),
        )
        return results

    @staticmethod
    def _market_to_records(
        market: dict[str, object],
        venue_tag: str,
        chain: str,
        available_since: datetime | None = None,
    ) -> list[InstrumentRecord]:
        """Convert a raw Morpho market dict into its A_TOKEN + DEBT_TOKEN pair.

        Each isolated Morpho market produces two instruments — a supply-side
        ``A_TOKEN`` (collateral deposited into the market, earns yield) and a
        borrow-side ``DEBT_TOKEN`` (loan asset borrowed against that
        collateral, accrues interest) — mirroring ``aave_v3.py``'s
        ``_build_reserve_records`` split. Returns ``[]`` if the market is
        filtered (malformed payload / missing identity fields).
        """
        loan_asset = market.get("loanAsset")
        collateral_asset = market.get("collateralAsset")
        if not isinstance(loan_asset, dict) or not isinstance(collateral_asset, dict):
            return []

        loan_symbol = str(loan_asset.get("symbol", ""))
        collateral_symbol = str(collateral_asset.get("symbol", ""))
        # Morpho renamed the canonical bytes32 market id field uniqueKey → marketId
        # (verified live 2026-06-18: the old query 400'd with "Cannot query field
        # uniqueKey on type Market"; the venue had been attempted_failed since ~2026-05).
        market_key = str(market.get("marketId", ""))

        if not collateral_symbol or not market_key:
            return []

        # Dash-separate the market-key disambiguator, not colon — colon is the
        # reserved top-level VENUE:TYPE:SYMBOL delimiter, so a 3rd colon inside the
        # symbol segment is ambiguous to any naive split(":") parser (operator
        # decision 2026-07-08, instrument_id_format_canonicalization finding 6).
        pair_key = f"{collateral_symbol}-{loan_symbol}-{market_key[:8]}"

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

        resolved_since = available_since or get_protocol_floor_date("morpho", "ETHEREUM")

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": market_key,
            "base_asset": collateral_symbol,
            "quote_asset": loan_symbol,
            "tick_size": Decimal("0.000001"),
            "min_size": Decimal("0.000001"),
            "contract_size": Decimal("1"),
            "expiry": None,
            "strike": None,
            "option_type": None,
            "status": InstrumentStatus.ACTIVE,
            "available_from_datetime": resolved_since,
            # DeFi metadata
            "pool_address": market_key,
            "base_asset_contract_address": coll_addr,
            "base_asset_decimals": coll_decimals,
            "base_asset_symbol_onchain": collateral_symbol or None,
            "quote_asset_contract_address": loan_addr,
            "quote_asset_decimals": loan_decimals,
            "quote_asset_symbol_onchain": loan_symbol or None,
        }

        a_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "morpho",
            InstrumentType.A_TOKEN,
            f"A{pair_key}",
            chain=chain,
            passthrough=True,
        )
        debt_token_instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI,
            "morpho",
            InstrumentType.DEBT_TOKEN,
            f"DEBT{pair_key}",
            chain=chain,
            passthrough=True,
        )
        return [
            InstrumentRecord(
                instrument_key=a_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=a_token_instrument_key,
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=debt_token_instrument_key,
                # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
                # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
                canonical_instrument_id=debt_token_instrument_key,
                instrument_type=InstrumentType.DEBT_TOKEN,
                **base_kwargs,
            ),
        ]

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
