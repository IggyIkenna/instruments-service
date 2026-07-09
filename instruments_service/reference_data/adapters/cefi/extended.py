"""Extended reference data adapter — instrument discovery via /info/markets.

Extended is a StarkNet perp DEX (Extended.exchange, formerly X10). Public REST
at api.starknet.extended.exchange — **no auth / API key required** for market
discovery, candles, or funding (verified live 2026-06-22 against the public
docs at api.docs.extended.exchange). Trading keys are needed ONLY for order
placement, never for read-only market data. Returns all active perpetual
markets as InstrumentRecord objects.

Per-instrument genesis (2026-06-22 audit, slot-3-ikenna):
`/info/markets` `createdAt` is the StarkNet-mainnet record-creation timestamp
(50 of 103 markets share the 2025-07-18 bulk-migration stamp), but historical
OHLCV pre-dates it — e.g. BTC-USD / ETH-USD candles begin 2024-07-26 (testnet
/ x10 era). The honest `available_from` is therefore the **earliest actual
daily candle per market** (resolved via a one-shot P1D `/info/candles` probe),
NOT a single venue-wide constant and NOT `createdAt`. Audited genesis spans
2024-07-26 → 2026-05-22 across all 103 active markets.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_EXTENDED_API_BASE = "https://api.starknet.extended.exchange/api/v1"
# Earliest possible Extended data (StarkNet/x10 testnet launch). Used only as a
# fallback floor for available_from when the per-market genesis probe fails.
_EXTENDED_DEPLOY_DATE = datetime(2024, 7, 26, tzinfo=UTC)
# Lower bound for the per-market genesis probe (before any Extended history).
_EXTENDED_GENESIS_PROBE_START_MS = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1000)
# Bounded concurrency for the per-market genesis probe.
_EXTENDED_GENESIS_PROBE_CONCURRENCY = 12

# Fallback market list used when /info/markets is unreachable.
# Mirrors MTDS _EXTENDED_FALLBACK_SYMBOLS (umi_tick_provider.py:1086-1092).
_EXTENDED_FALLBACK_MARKETS: tuple[str, ...] = (
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "SUI-USD",
)


def _classify_extended_error(exc: Exception, status: int | None = None) -> str:
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "500"
    return "UNKNOWN"


class ExtendedReferenceDataAdapter(BaseReferenceDataAdapter):
    """Extended reference data: perp market discovery via /info/markets.

    Fetches ACTIVE markets from the public /info/markets endpoint.
    Each active market with status=ACTIVE becomes one InstrumentRecord with
    instrument_type=PERPETUAL, settle_asset=USDC.
    Falls back to the curated list on network error.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "EXTENDED-STARKNET"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Extended perp markets from /info/markets."""
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        symbols: list[str] = []
        genesis_map: dict[str, datetime] = {}
        async with self._make_session() as session:
            try:
                raw = await self._get_with_retry(session, f"{_EXTENDED_API_BASE}/info/markets")
                payload: dict[str, object] = cast("dict[str, object]", raw) if isinstance(raw, dict) else {}
                mkt_list = cast("list[dict[str, object]]", payload.get("data") or [])
                symbols = [str(m["name"]) for m in mkt_list if m.get("active") and str(m.get("status", "")) == "ACTIVE"]
                if not symbols:
                    logger.warning("Extended /info/markets returned no ACTIVE markets — using fallback")
                    symbols = list(_EXTENDED_FALLBACK_MARKETS)
            except (aiohttp.ClientError, RuntimeError) as exc:
                if isinstance(exc, aiohttp.ClientError):
                    error_code = _classify_extended_error(exc)
                    classification = classify_venue_error("extended", error_code)
                    action = classification.action.value if classification else "fail"
                    logger.warning(
                        "Extended /info/markets failed: %s (action=%s) — using fallback",
                        exc,
                        action,
                    )
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": self.venue,
                            "endpoint": "info/markets",
                            "error": str(exc),
                            "action": action,
                        },
                    )
                else:
                    logger.warning("Extended /info/markets failed after retries: %s — using fallback", exc)
                symbols = list(_EXTENDED_FALLBACK_MARKETS)

            # Resolve the honest per-market available_from = earliest daily candle.
            # createdAt reflects the StarkNet-mainnet record migration, not first
            # tradability (OHLCV pre-dates it for ~half the universe), so the
            # candle history is the SSOT for genesis. Probe concurrently.
            genesis_map = await self._probe_genesis_map(session, symbols)

        results: list[InstrumentRecord] = []
        for sym in symbols:
            base_asset = sym.split("-")[0].upper() if "-" in sym else sym.upper()
            results.append(
                InstrumentRecord(
                    # Canonical instrument_id: VENUE:PERPETUAL:BASE-QUOTE (2026-07-08
                    # canonicalization — dropped the PERP shorthand only; this venue
                    # was already dash-normalized with a real settlement currency
                    # (``sym`` is e.g. "BTC-USD", confirmed live:
                    # collateralAssetName="USD" uniformly across markets). SSOT:
                    # plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
                    # finding 3+4;
                    # plans/active/canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md.
                    # Routed through the shared UAC builder (2026-07-09 retrofit,
                    # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 4) —
                    # pure DRY, output is behavior-identical to the prior f-string.
                    instrument_key=build_instrument_id("EXTENDED-STARKNET", InstrumentType.PERPETUAL, sym),
                    venue=self.venue,
                    raw_symbol=sym,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=base_asset,
                    quote_asset="USD",
                    settle_asset="USDC",
                    margin_type=MarginType.LINEAR,
                    tick_size=Decimal("0.0001"),
                    min_size=Decimal("0.001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=genesis_map.get(sym, _EXTENDED_DEPLOY_DATE),
                    timezone="UTC",
                )
            )

        logger.info("Extended: fetched %d perp instruments", len(results))
        return results

    async def _probe_genesis_map(
        self,
        session: aiohttp.ClientSession,
        symbols: list[str],
    ) -> dict[str, datetime]:
        """Resolve earliest-tradable date per market via a one-shot P1D candle probe.

        Returns ``{symbol: available_from_datetime}``; markets whose probe fails
        are omitted and fall back to ``_EXTENDED_DEPLOY_DATE`` at the call site.
        """
        semaphore = asyncio.Semaphore(_EXTENDED_GENESIS_PROBE_CONCURRENCY)
        end_ms = int(datetime.now(UTC).timestamp() * 1000)

        async def _one(sym: str) -> tuple[str, datetime | None]:
            async with semaphore:
                return sym, await self._probe_market_genesis(session, sym, end_ms)

        pairs = await asyncio.gather(*(_one(s) for s in symbols))
        return {sym: dt for sym, dt in pairs if dt is not None}

    async def _probe_market_genesis(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        end_ms: int,
    ) -> datetime | None:
        """Earliest daily candle timestamp for ``symbol`` (the honest genesis)."""
        url = (
            f"{_EXTENDED_API_BASE}/info/candles/{symbol}/trades"
            f"?interval=P1D&startTime={_EXTENDED_GENESIS_PROBE_START_MS}"
            f"&endTime={end_ms}&limit=1000"
        )
        try:
            raw = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            logger.warning("Extended genesis probe failed for %s: %s — using deploy-date floor", symbol, exc)
            return None
        payload: dict[str, object] = cast("dict[str, object]", raw) if isinstance(raw, dict) else {}
        candles = cast("list[dict[str, object]]", payload.get("data") or [])
        stamps = [int(cast("int", c["T"])) for c in candles if c.get("T") is not None]
        if not stamps:
            return None
        return datetime.fromtimestamp(min(stamps) / 1000, UTC)

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        for inst in await self.get_instruments():
            if inst.raw_symbol == symbol or inst.raw_symbol == symbol.upper():
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Extended does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Extended perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Extended funding rate not supported via reference data adapter")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Extended OHLCV not supported via reference data adapter")
