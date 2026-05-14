"""Kamino reference data adapter -- instrument discovery via REST API.

Discovers Kamino liquidity vaults on Solana. Each vault is an automated
LP position on Raydium/Orca CLMM pools, returned as instrument_type="POOL".

Data source: Kamino REST API (https://api.kamino.finance/strategies).
Reference: https://docs.kamino.finance/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import (
    get_solana_protocol_url,
    resolve_solana_mint,
)
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_BASE_URL = get_solana_protocol_url("kamino") or "https://api.kamino.finance"
_DEFAULT_CHAIN = "SOLANA"
_KAMINO_DEPLOY_DATE = get_protocol_floor_date("kamino")


def _classify_kamino_error(exc: Exception, status: int | None = None) -> str:
    """Map a Kamino HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class KaminoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Kamino reference data: liquidity vault discovery from REST API.

    Discovers Kamino automated LP vaults (CLMM positions on Raydium/Orca).
    Each vault produces one instrument with instrument_type="POOL".
    Only LIVE vaults with at least one major-asset token are included.
    """

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
        return f"KAMINO-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Kamino."""
        error_code = _classify_kamino_error(exc)
        classification = classify_venue_error("kamino", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Kamino strategies request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": "strategies",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Kamino liquidity vaults as instruments."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = f"{_BASE_URL}/strategies"
        params = {"status": "LIVE"}
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url, params=params)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Kamino strategies request failed after retries: %s", exc)
            raise

        strategies: list[dict[str, object]] = data if isinstance(data, list) else []
        results: list[InstrumentRecord] = []
        venue_tag = self.venue

        for strategy in strategies:
            record = self._build_vault_record(strategy, venue_tag)
            if record:
                results.append(record)

        logger.info("Kamino: fetched %d vault instruments on %s", len(results), self._chain)

        # Resolve creation timestamps via Solana RPC
        if results:
            from ._solana_utils import (
                batch_resolve_creation_timestamps,
            )

            addresses = [r.raw_symbol for r in results if r.raw_symbol]
            ts_map = await batch_resolve_creation_timestamps(addresses)
            for record in results:
                if record.raw_symbol in ts_map:
                    rpc_ts = ts_map[record.raw_symbol]
                    if record.available_from_datetime is None or rpc_ts < record.available_from_datetime:
                        record.available_from_datetime = rpc_ts

        return results

    def _resolve_symbol(self, mint: str) -> str:
        """Resolve a Solana token mint address to its symbol."""
        return resolve_solana_mint(mint) or ""

    def _build_vault_record(
        self,
        strategy: dict[str, object],
        venue_tag: str,
    ) -> InstrumentRecord | None:
        """Build InstrumentRecord from a Kamino strategy/vault entry."""
        address = str(strategy.get("address", ""))
        status = str(strategy.get("status", ""))
        if not address or status != "LIVE":
            return None

        mint_a = str(strategy.get("tokenAMint", ""))
        mint_b = str(strategy.get("tokenBMint", ""))
        sym_a = self._resolve_symbol(mint_a)
        sym_b = self._resolve_symbol(mint_b)

        # Skip if we can't resolve either symbol
        if not sym_a or not sym_b:
            return None

        instrument_key = f"{venue_tag}:VAULT:{sym_a}-{sym_b}:{address[:8]}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=address,
            pool_address=address,
            instrument_type=InstrumentType.POOL,
            base_asset=sym_a,
            quote_asset=sym_b,
            tick_size=Decimal("0.000001"),
            min_size=Decimal("0.000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_KAMINO_DEPLOY_DATE,
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
        raise NotImplementedError("Kamino does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Kamino vaults have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Kamino vaults have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Kamino OHLCV not supported via reference data")
