"""Orca Whirlpool reference data adapter -- instrument discovery via REST API.

Discovers Orca concentrated liquidity (Whirlpool) pools on Solana.
Pools are returned as InstrumentRecord with instrument_type="POOL".

Data source: Orca Whirlpool API (https://api.mainnet.orca.so).
Reference: https://docs.orca.so/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import get_solana_protocol_url
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import order_base_quote
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_BASE_URL = get_solana_protocol_url("orca") or "https://api.mainnet.orca.so"
_DEFAULT_CHAIN = "SOLANA"
_ORCA_DEPLOY_DATE = get_protocol_floor_date("orca")


def _classify_orca_error(exc: Exception, status: int | None = None) -> str:
    """Map an Orca HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class OrcaReferenceDataAdapter(BaseReferenceDataAdapter):
    """Orca Whirlpool reference data: concentrated liquidity pool discovery.

    Each Orca Whirlpool produces one instrument with instrument_type="POOL"
    and symbol=f"{tokenA}/{tokenB}".
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
        """Return the venue identifier."""
        return f"ORCA-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Orca."""
        error_code = _classify_orca_error(exc)
        classification = classify_venue_error("orca", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Orca whirlpools request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": "whirlpools",
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
        """Fetch active Orca Whirlpool pools as instruments."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = f"{_BASE_URL}/v1/whirlpool/list"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Orca whirlpools request failed after retries: %s", exc)
            raise

        # Orca API returns { "whirlpools": [...] }
        raw_data: dict[str, object] = data if isinstance(data, dict) else {}
        pools: list[dict[str, object]] = raw_data.get("whirlpools") or []
        if not isinstance(pools, list):
            pools = []

        results: list[InstrumentRecord] = []

        for pool in pools:
            if not isinstance(pool, dict):
                continue
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("Orca: fetched %d whirlpool instruments on %s", len(results), self._chain)

        # Resolve creation timestamps via Solana RPC (getSignaturesForAddress)
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

    def _build_pool_record(
        self,
        pool: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a single Orca Whirlpool, or None."""
        address = pool.get("address")
        if not address:
            return None

        # Extract token symbols and decimals from tokenA/tokenB
        token_a = pool.get("tokenA") or {}
        token_b = pool.get("tokenB") or {}
        sym_a = str(token_a.get("symbol", "")).upper() if isinstance(token_a, dict) else ""
        sym_b = str(token_b.get("symbol", "")).upper() if isinstance(token_b, dict) else ""
        if not sym_a or not sym_b:
            return None

        base, quote = order_base_quote(sym_a, sym_b)

        # TVL minimum: skip dust pools (Orca returns thousands of low-liquidity pools)
        tvl_raw = pool.get("tvl") or pool.get("liquidity") or 0
        tvl = float(str(tvl_raw)) if tvl_raw else 0
        if tvl < 10_000:  # $10k minimum TVL
            return None

        def _parse_dec(t: object) -> int | None:
            if not isinstance(t, dict):
                return None
            raw = t.get("decimals")
            if isinstance(raw, int):
                return raw
            if isinstance(raw, str) and raw.isdigit():
                return int(raw)
            return None

        if base == sym_a:
            base_decimals = _parse_dec(token_a)
            quote_decimals = _parse_dec(token_b)
        else:
            base_decimals = _parse_dec(token_b)
            quote_decimals = _parse_dec(token_a)

        if base_decimals is None or quote_decimals is None:
            return None

        venue_tag = self.venue
        tick_spacing = pool.get("tickSpacing", "")
        instrument_key = f"{venue_tag}:POOL:{base}-{quote}:WP{tick_spacing}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=str(address),
            pool_address=str(address),
            instrument_type=InstrumentType.POOL,
            base_asset=base,
            quote_asset=quote,
            tick_size=Decimal("0.000001"),
            min_size=Decimal("0.000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_ORCA_DEPLOY_DATE,
            base_asset_decimals=base_decimals,
            quote_asset_decimals=quote_decimals,
            source_archive_url_template=None,
            source_record_types=None,
            source_coverage_start=None,
            source_coverage_end=None,
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Orca does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Orca pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Orca pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Orca OHLCV not supported via reference data")
