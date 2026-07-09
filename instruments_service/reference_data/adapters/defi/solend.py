"""Solend reference data adapter — instrument discovery via Solend public REST API.

Discovers Solend lending-market reserves on Solana. Each reserve emits a
supply-side ``A_TOKEN`` instrument (collateral deposited into the reserve)
and a borrow-side ``DEBT_TOKEN`` instrument (asset borrowed against the
pooled liquidity) — the same A_TOKEN/DEBT_TOKEN split ``aave_v3.py`` /
``morpho.py`` use per reserve/market
(defi_lending_atoken_debttoken_instrument_split_2026_07_07.md). Unlike Aave
(which flags borrowing per-reserve), every listed Solend reserve is both
depositable and borrowable by protocol design (isolated-pool cross-collateral
lending) — the public markets-config endpoint carries no per-reserve
"borrow disabled" flag, so both legs are always emitted.

Data source: Solend public REST API (https://api.solend.fi/v1/markets/configs).
Scope is restricted to the curated "solend" scope (the 21 markets shipped by
Solend Labs itself, excluding permissionless community-created pools) and,
within that, the primary/flagship market (``isPrimary=true``) — the same
curation discipline Kamino (status=LIVE) and Solend's own UI apply, so the
adapter surfaces the real, actively-used reserve set rather than every
long-tail permissionless pool.

NOTE: Solend Labs rebranded its consumer product to "Save" in 2025; the
on-chain program, canonical venue name (``SOLEND-SOLANA``), and this public
API host (api.solend.fi) are unchanged, so the legacy "solend" name stays
the canonical adapter/venue key.

Reference: https://dev.solend.fi/docs/api/ ; https://docs.solend.fi/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import AssetGroup, build_canonical_instrument_id, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._solana_utils import batch_resolve_creation_timestamps, get_protocol_floor_date

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.solend.fi"
_MARKETS_CONFIG_PATH = "/v1/markets/configs"
_DEFAULT_CHAIN = "SOLANA"
_SOLEND_DEPLOY_DATE = get_protocol_floor_date("solend")


def _classify_solend_error(exc: Exception, status: int | None = None) -> str:
    """Map a Solend HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


def _sanitize_symbol(symbol: str) -> str:
    """Strip whitespace from a raw reserve symbol for safe instrument_key embedding.

    A handful of Solend LP-style reserves carry a human-readable symbol with an
    embedded space (e.g. "SOL-bSOL MLP") — not valid inside the colon-delimited
    canonical instrument_key grammar's SYMBOL segment. ``base_asset`` keeps the
    original (unsanitized) symbol for readability; only the key-embedded copy
    is stripped.
    """
    return symbol.replace(" ", "")


class SolendReferenceDataAdapter(BaseReferenceDataAdapter):
    """Solend reference data: lending-market reserve discovery from the public REST API.

    Fetches the primary Solend market's reserve list and emits one A_TOKEN +
    one DEBT_TOKEN instrument per reserve (supply/borrow split).
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
        return f"SOLEND-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Solend."""
        error_code = _classify_solend_error(exc)
        classification = classify_venue_error("solend", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Solend markets/configs request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch the primary Solend market's reserves as supply/debt instruments."""
        if instrument_type not in (None, InstrumentType.LENDING):
            return []

        url = f"{_BASE_URL}{_MARKETS_CONFIG_PATH}"
        params = {"scope": "solend", "deployment": "production"}
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url, params=params)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Solend markets/configs request failed after retries: %s", exc)
            raise

        markets: list[dict[str, object]] = data if isinstance(data, list) else []
        primary_market = next((m for m in markets if m.get("isPrimary")), None)
        if primary_market is None:
            logger.warning("Solend: no primary market found in %d markets returned", len(markets))
            return []

        reserves_raw = primary_market.get("reserves")
        reserves: list[dict[str, object]] = reserves_raw if isinstance(reserves_raw, list) else []

        # Resolve reserve creation timestamps via Solana RPC (cached — only
        # RPC on first run, mirrors kamino.py's timestamp-resolution pass).
        addresses = [str(r.get("address", "")) for r in reserves if r.get("address")]
        ts_map = await batch_resolve_creation_timestamps(addresses) if addresses else {}

        venue_tag = self.venue
        results: list[InstrumentRecord] = []
        for reserve in reserves:
            available_since = ts_map.get(str(reserve.get("address", "")), _SOLEND_DEPLOY_DATE)
            results.extend(self._build_reserve_records(reserve, venue_tag, available_since))

        logger.info(
            "Solend: fetched %d supply/debt instruments from %d reserves in the primary market",
            len(results),
            len(reserves),
        )
        return results

    @staticmethod
    def _build_reserve_records(
        reserve: dict[str, object],
        venue_tag: str,
        available_since: datetime,
    ) -> list[InstrumentRecord]:
        """Build the A_TOKEN (supply) + DEBT_TOKEN (borrow) pair for one Solend reserve."""
        liquidity_token_raw = reserve.get("liquidityToken")
        liquidity_token: dict[str, object] = liquidity_token_raw if isinstance(liquidity_token_raw, dict) else {}
        mint = str(liquidity_token.get("mint", ""))
        raw_symbol_field = str(liquidity_token.get("symbol", ""))
        reserve_address = str(reserve.get("address", ""))
        if not mint or not raw_symbol_field or not reserve_address:
            return []

        # base_asset_decimals is REQUIRED (non-null) for DeFi on-chain
        # instrument types (workspace rule: null decimals is a silent
        # price-normalisation bug). The real Solend endpoint always carries
        # ``liquidityToken.decimals`` (verified against all 89 real "main"
        # market reserves, 2026-07-09) — this guard is defense-in-depth, not
        # the expected path, and skips honestly rather than fabricating.
        decimals_raw = liquidity_token.get("decimals")
        decimals_int = decimals_raw if isinstance(decimals_raw, int) else None
        if decimals_int is None:
            logger.warning(
                "Solend: skipping reserve %s (%s) — liquidityToken.decimals missing",
                reserve_address,
                raw_symbol_field,
            )
            return []

        symbol = _sanitize_symbol(raw_symbol_field)

        base_kwargs = {
            "venue": venue_tag,
            "raw_symbol": reserve_address,
            "base_asset": raw_symbol_field,
            "quote_asset": "",
            "tick_size": Decimal("0.000001"),
            "min_size": Decimal("0.000001"),
            "contract_size": Decimal("1"),
            "expiry": None,
            "strike": None,
            "option_type": None,
            "status": InstrumentStatus.ACTIVE,
            "underlying": raw_symbol_field,
            "available_from_datetime": available_since,
            # DeFi metadata
            "pool_address": reserve_address,
            "base_asset_contract_address": mint,
            "base_asset_decimals": decimals_int,
            "base_asset_symbol_onchain": raw_symbol_field,
        }

        return [
            InstrumentRecord(
                instrument_key=build_canonical_instrument_id(
                    AssetGroup.DEFI,
                    "solend",
                    InstrumentType.A_TOKEN,
                    f"A{symbol}",
                    chain="SOLANA",
                    passthrough=True,
                ),
                instrument_type=InstrumentType.A_TOKEN,
                **base_kwargs,
            ),
            InstrumentRecord(
                instrument_key=build_canonical_instrument_id(
                    AssetGroup.DEFI,
                    "solend",
                    InstrumentType.DEBT_TOKEN,
                    f"DEBT{symbol}",
                    chain="SOLANA",
                    passthrough=True,
                ),
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
        raise NotImplementedError("Solend does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Solend lending markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Solend lending markets have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Solend OHLCV not supported via reference data")
