"""Hyperliquid reference data adapter — REST only, no WebSocket."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    HyperliquidAssetInfo,
    HyperliquidMeta,
    UnsupportedCapabilityError,
    VenueMapping,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BASE = "https://api.hyperliquid.xyz"
# SSOT: UAC VenueMapping.get_instrument_discovery_start("HYPERLIQUID"). Pre-2026-05-05
# this was hardcoded as datetime(2023, 11, 1) and silently diverged from UAC's
# venue_start_dates (2023-04-15) — the orchestrator's expected-window calc used
# UAC's market-data start, the adapter wrote 2023-11-01 onto each instrument's
# available_from_datetime. Result: 200 phantom attempted_failed rows per the
# 2026-05-05 discovery. Now both consume the same UAC field.
_HYPERLIQUID_LAUNCH_DATE = datetime.fromisoformat(
    cast(str, VenueMapping().get_instrument_discovery_start("HYPERLIQUID"))
).replace(tzinfo=UTC)

# BUG #4 (B) — per-instrument earliest-funding-date probe. The catalogue rollup +
# the MTDS catalogue-driven backfill key the historical universe off each
# instrument's ``available_from``. Without a per-instrument genesis date every perp
# would inherit the coarse venue launch date (over-attempts pre-listing history) or
# the single observed day (under-attempts history → small-coin funding NEVER
# captured, the exact BUG #4 gap). HL ``fundingHistory`` from ``startTime=0`` returns
# the instrument's genesis funding entry, so its timestamp is the true listing date.
# One-off at enumeration time; bounded concurrency; falls back to the venue launch
# date on any probe failure (never blocks the universe). SSOT:
# plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4 (B).
# HL /info is aggressively rate-limited (429 under burst) → low concurrency + a
# longer backoff. An UNRESOLVED probe falls back to the venue launch date, which is
# the SAFE direction (over-attempt history → honest-absence on pre-listing dates,
# never lost data) — so the probe is an efficiency optimisation, not a correctness
# dependency.
_FUNDING_PROBE_CONCURRENCY = 2
_FUNDING_PROBE_TIMEOUT_S = 20.0
_FUNDING_PROBE_RETRIES = 4
_FUNDING_PROBE_BACKOFF_BASE_S = 1.0


def _classify_hyperliquid_error(exc: Exception, status: int | None = None) -> str:
    """Map a Hyperliquid HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    if "margin" in msg:
        return "INSUFFICIENT_MARGIN"
    return "UNKNOWN"


class HyperliquidReferenceDataAdapter(BaseReferenceDataAdapter):
    """Hyperliquid reference data: perpetuals from meta REST endpoint."""

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "HYPERLIQUID"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue."""
        # OPTIONS: not supported — venue does not offer listed options contracts
        # FUTURE: not supported — Hyperliquid only offers perpetual futures (no expiry)
        if instrument_type in (InstrumentType.OPTION, InstrumentType.FUTURE):
            raise UnsupportedCapabilityError(
                venue="HYPERLIQUID",
                capability=str(instrument_type),
                message=(f"HYPERLIQUID does not support {instrument_type} instruments. Only PERPETUAL is available."),
            )
        if instrument_type not in (None, InstrumentType.PERPETUAL):
            return []
        url = f"{_BASE}/info"
        payload = {"type": "meta"}
        try:
            async with self._make_session() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = HyperliquidMeta.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid meta request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "meta",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # CF-11: a genuine fetch error must thread STATE so the venue lands in
            # urdi_reference_provider._fetch_one's failed[] (→ attempted_failed),
            # NOT return [] (read as a clean empty → silent universe shrink /
            # A8 false-complete). Single-call adapter: this error means the WHOLE
            # Hyperliquid universe is unavailable. Re-raise after classify+emit
            # (mirrors the tradfi databento fix).
            raise RuntimeError(
                f"Hyperliquid meta fetch failed (error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc
        universe: list[HyperliquidAssetInfo] = data.universe or []
        # BUG #4 (2026-06-22): the base-asset majors whitelist capped HL at ~33
        # instruments. The operator wants the FULL active perp universe enumerated
        # (funding rates valuable even for small/illiquid coins), so the whitelist
        # is REMOVED — every non-delisted perp in the /info meta universe is
        # catalogued. Delisted perps are skipped (stale meta entries).
        # SSOT: plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4.
        active = [a for a in universe if a.name and not a.isDelisted]
        # BUG #4 (B): probe each instrument's genesis funding date concurrently so its
        # available_from reflects the true listing date (history-accurate universe).
        listing_dates = await self._probe_earliest_funding_dates([cast(str, a.name) for a in active])
        results: list[InstrumentRecord] = []
        for asset in active:
            name = cast(str, asset.name)
            sz_decimals: int = asset.szDecimals or 3
            lot = Decimal(10) ** -sz_decimals
            results.append(
                InstrumentRecord(
                    instrument_key=f"HYPERLIQUID:PERP:{name}",
                    venue=self.venue,
                    raw_symbol=name,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=name,
                    quote_asset="USD",
                    settle_asset="USDC",
                    margin_type=MarginType.LINEAR,
                    tick_size=Decimal("0.001"),
                    min_size=lot,
                    contract_size=Decimal("1"),
                    available_from_datetime=listing_dates.get(name, _HYPERLIQUID_LAUNCH_DATE),
                    status=InstrumentStatus.ACTIVE,
                    timezone="UTC",
                )
            )
        return results

    async def _probe_earliest_funding_dates(self, coins: list[str]) -> dict[str, datetime]:
        """Probe each coin's genesis funding date (HL fundingHistory startTime=0).

        Bounded-concurrent; per-coin timeout; any failure falls back (the coin is
        simply omitted → caller uses the venue launch date). BUG #4 (B).
        """
        sem = asyncio.Semaphore(_FUNDING_PROBE_CONCURRENCY)

        async def _one(coin: str) -> tuple[str, datetime | None]:
            async with sem:
                last_exc: Exception | None = None
                for attempt in range(_FUNDING_PROBE_RETRIES):
                    try:
                        return coin, await asyncio.wait_for(
                            self._fetch_earliest_funding_date(coin), timeout=_FUNDING_PROBE_TIMEOUT_S
                        )
                    except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, TypeError) as exc:
                        last_exc = exc
                        await asyncio.sleep(_FUNDING_PROBE_BACKOFF_BASE_S * (2.0**attempt))  # backoff on 429
                logger.warning("HL earliest-funding probe failed for %s: %s — using launch date", coin, last_exc)
                return coin, None

        out: dict[str, datetime] = {}
        for coin, dt in await asyncio.gather(*(_one(c) for c in coins)):
            if dt is not None:
                out[coin] = dt
        logger.info("HL earliest-funding probe: resolved %d/%d genesis dates", len(out), len(coins))
        return out

    async def _fetch_earliest_funding_date(self, coin: str) -> datetime | None:
        """Return the datetime of the coin's first-ever funding entry, or None."""
        payload = {"type": "fundingHistory", "coin": coin, "startTime": 0}
        async with self._make_session() as session, session.post(f"{_BASE}/info", json=payload) as resp:
            resp.raise_for_status()
            data = cast(object, await resp.json())
        if not isinstance(data, list) or not data:
            return None
        first = cast(dict[str, object], data[0])
        ts_ms = first.get("time")
        if not isinstance(ts_ms, (int, float)):
            return None
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC)

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Hyperliquid does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Hyperliquid perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Hyperliquid /info endpoint (fundingHistory type).

        POST /info with type=fundingHistory. Returns most recent rate.
        """
        url = f"{_BASE}/info"
        now = datetime.now(UTC)
        payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": int((now.timestamp() - 8 * 3600) * 1000),
        }
        try:
            async with self._make_session() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid fundingHistory request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "fundingHistory",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return FundingRateRef(
                venue=self.venue,
                symbol=symbol,
                rate=Decimal("0"),
                next_funding_time=now,
                mark_price=None,
                updated_at=now,
            )
        entries: list[object] = raw if isinstance(raw, list) else []
        entry_obj: object = entries[-1] if entries else {}
        entry: dict[str, object] = entry_obj if isinstance(entry_obj, dict) else {}
        rate_raw = entry.get("fundingRate", "0") or "0"
        ts_raw = entry.get("time", int(now.timestamp() * 1000))
        next_funding_time = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    _INTERVAL_SECONDS: ClassVar[dict[str, int]] = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }

    def _resolve_hl_interval(self, interval: str) -> tuple[str, int]:
        """Return (hl_interval, bar_seconds) for a given interval string."""
        hl = interval if interval in self._INTERVAL_SECONDS else "1d"
        return hl, self._INTERVAL_SECONDS.get(hl, 86400)

    def _parse_hl_candle(self, candle_raw: object, symbol: str, hl_interval: str) -> OHLCVRef | None:
        """Parse a single Hyperliquid candle dict into an OHLCVRef. Returns None if invalid."""
        if not isinstance(candle_raw, dict):
            return None
        candle: dict[str, object] = candle_raw
        # T = close-time ms (right/close edge of bar); t = open-time ms (left edge).
        # Always stamp the close edge to avoid look-ahead leakage.
        ts_raw = candle.get("T") or candle.get("t") or 0
        ts = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(candle.get("o", "0") or "0")),
            high=Decimal(str(candle.get("h", "0") or "0")),
            low=Decimal(str(candle.get("l", "0") or "0")),
            close=Decimal(str(candle.get("c", "0") or "0")),
            volume=Decimal(str(candle.get("v", "0") or "0")),
            interval=hl_interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Hyperliquid /info candleSnapshot endpoint.

        POST /info with type=candleSnapshot. Supported intervals: 1m, 5m, 15m, 30m, 1h, 4h, 1d.
        """
        hl_interval, bar_seconds = self._resolve_hl_interval(interval)
        now = datetime.now(UTC)
        end_ts = int(now.timestamp() * 1000)
        start_ts = end_ts - limit * bar_seconds * 1000
        url = f"{_BASE}/info"
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": hl_interval,
                "startTime": start_ts,
                "endTime": end_ts,
            },
        }
        try:
            async with self._make_session() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                raw_ohlcv: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid candleSnapshot request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "candleSnapshot",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        candles: list[object] = raw_ohlcv if isinstance(raw_ohlcv, list) else []
        return [
            bar for candle_raw in candles if (bar := self._parse_hl_candle(candle_raw, symbol, hl_interval)) is not None
        ]
