"""Morpho Blue reference data adapter — instrument discovery via Morpho API.

Discovers Morpho Blue isolated lending markets on Ethereum.
Markets are returned as InstrumentRecord with instrument_type="lending_market".

Data source: Morpho Blue GraphQL API (blue-api.morpho.org).
Reference: https://morpho.org/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import DEFI_MAJOR_ASSET_SYMBOLS, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_MORPHO_API_URL = "https://blue-api.morpho.org/graphql"
_DEFAULT_CHAIN = "ETHEREUM"

# Morpho Blue Ethereum mainnet deployment date (2024-01-08).
# The Morpho GraphQL API does not expose per-market creation timestamps,
# so we use the protocol launch date as the available_since floor for all markets.
_MORPHO_DEPLOY_DATE = datetime(2024, 1, 8, tzinfo=UTC)

_MARKETS_QUERY = """
query {
    markets(first: 400, orderBy: SupplyAssets, orderDirection: Desc) {
        items {
            uniqueKey
            loanAsset { address symbol name decimals }
            collateralAsset { address symbol name decimals }
            lltv
            state {
                supplyAssets
                borrowAssets
                supplyApy
                borrowApy
            }
        }
    }
}
"""


def _classify_error(exc: Exception, status: int | None = None) -> str:
    if status == 429:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "503" in msg or "unavailable" in msg:
        return "503"
    return "UNKNOWN"


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
        return "morpho"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Morpho Blue lending markets as instruments."""
        if instrument_type not in (None, "lending_market"):
            return []

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    _MORPHO_API_URL,
                    json={"query": _MARKETS_QUERY},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
                gql_errors = data.get("errors")
                if gql_errors:
                    logger.warning("Morpho GraphQL errors: %s", gql_errors)
        except aiohttp.ClientError as exc:
            error_code = _classify_error(exc)
            classification = classify_venue_error("morpho", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Morpho API request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "morpho",
                    "endpoint": "morpho_api_markets",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        markets_data = data.get("data", {}).get("markets", {})
        markets: list[dict[str, object]] = (
            markets_data.get("items", []) if isinstance(markets_data, dict) else markets_data
        )
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        venue_tag = f"MORPHO-{self._chain}"

        for market in markets:
            record = self._market_to_record(market, venue_tag, now)
            if record is not None:
                results.append(record)

        logger.info("Morpho: fetched %d lending market instruments on %s", len(results), self._chain)
        return results

    @staticmethod
    def _market_to_record(market: dict[str, object], venue_tag: str, now: datetime) -> InstrumentRecord | None:
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

        # Filter: both collateral and loan asset must be major
        if collateral_symbol.upper() not in DEFI_MAJOR_ASSET_SYMBOLS:
            return None
        if loan_symbol.upper() not in DEFI_MAJOR_ASSET_SYMBOLS:
            return None

        symbol = f"{collateral_symbol}-{loan_symbol}"
        instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}:{market_key[:8]}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            symbol=symbol,
            raw_symbol=market_key,
            instrument_type="lending_market",
            base_asset=collateral_symbol,
            quote_asset=loan_symbol,
            tick_size=Decimal("0.000001"),
            lot_size=Decimal("0.000001"),
            min_order_size=Decimal("0"),
            contract_size=Decimal("1"),
            settlement_asset=loan_symbol,
            expiry=None,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
            available_since=_MORPHO_DEPLOY_DATE,
        )

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
        raise NotImplementedError("Morpho does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Morpho lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Morpho lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Morpho OHLCV not supported via reference data")
