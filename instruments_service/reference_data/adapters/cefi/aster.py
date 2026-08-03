"""Aster reference data adapter — REST only, no WebSocket.

Aster is an on-chain perpetual futures exchange (CLOB model, Binance Futures-compatible API).
Base URL: https://www.aster.exchange (fapi subdomain deprecated)
API docs: https://github.com/asterdex/api-docs
"""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    CEFI_ACCEPTED_QUOTE_ASSETS,
    AsterExchangeInfo,
    UnsupportedCapabilityError,
    VenueMapping,
    classify_venue_error,
)
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

# Canonical Aster Finance futures API base per asterdex/api-docs.
# Fixed 2026-05-14: prior "www.aster.exchange" caused 0% capture since 2024-10-01
# (Aster rebranded from Astherus → Aster Finance; domain moved to asterdex.com).
# Additional context (slot-3-ikenna 2026-05-14): All 17,681 manifest rows were
# attempted_failed/0% because www.aster.exchange/fapi/v1/* returns 404.
# Verified live endpoint: https://fapi.asterdex.com (confirmed by MTDS
# umi_tick_provider.py:545 + UAC SCHEMA_VERSIONS.md wss://fstream.asterdex.com).
# Issue: plans/active/issues/emerging_perp_venue_adapters_broken_2026_05_13.md
_BASE = "https://fapi.asterdex.com"
# SSOT: UAC VenueMapping.get_instrument_discovery_start("ASTER") = 2023-07-22
# (operator-confirmed 2026-06-17 = the Astherus pre-rebrand genesis; matches the
# venue_launch_dates ASTER floor + the perp_funding_handler funding genesis).
# IMPORTANT — pre-2024 Aster funding is BINANCE-PROXIED (Astherus pre-rebrand
# mirrored Binance funding); it is imported, NOT Aster-native — label source
# honestly downstream. SSOT:
# perp_funding_data_semantics_and_cadence_2026_06_16.md §GAP 2. The adapter
# consumes UAC's value directly (no venue_instrument_discovery_overrides entry —
# no second timeline like HYPERLIQUID's market-data vs discovery split).
_ASTER_LAUNCH_DATE = datetime.fromisoformat(cast(str, VenueMapping().get_instrument_discovery_start("ASTER"))).replace(
    tzinfo=UTC
)

# BUG #4 (B) — per-instrument earliest-funding-date probe (Binance-compat
# /fapi/v1/fundingRate?startTime=0&limit=1 returns the genesis funding entry). Sets
# each perp's available_from to its true listing date so the catalogue-driven
# historical backfill attempts each instrument from the right date forward (small-coin
# funding history captured, not lost). One-off at enumeration; bounded concurrency;
# falls back to the venue launch date on probe failure. SSOT:
# plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4 (B).
_FUNDING_PROBE_CONCURRENCY = 4
_FUNDING_PROBE_TIMEOUT_S = 20.0
_FUNDING_PROBE_RETRIES = 3
_FUNDING_PROBE_BACKOFF_BASE_S = 0.5

# Real margin type (2026-07-09, instrument_id_format_canonicalization_2026_07_08.md
# finding 1's PERPETUAL scope-expansion) — confirmed live: docs.asterdex.com states
# "Aster Perpetuals supports perpetual futures trading fully settled in USDT", and
# the real live exchangeInfo pull already done 2026-07-08 (fapi.asterdex.com/fapi/v1/
# exchangeInfo, 509 perp symbols) shows 100% stablecoin quoteAsset distribution
# (504 USDT / 3 USD1 / 2 bare "U") — zero coin-margined (inverse) pairs. Derives the
# @LIN/@INV instrument_id marker FROM this field so the two can never drift.
_MARGIN_TYPE = MarginType.LINEAR
_MARGIN_MARKER = _MARGIN_TYPE.value[:3].upper()


def _classify_aster_error(exc: Exception, status: int | None = None) -> str:
    """Map an Aster HTTP/network error to a UAC error code for classification."""
    if status == 429:
        return "429"
    if status == 503:
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg or "-1003" in msg:
        return "-1003"
    if "503" in msg or "unavailable" in msg:
        return "503"
    if "429" in msg:
        return "429"
    return "UNKNOWN"


def _extract_filter_value(filters: list[object], filter_type: str, field: str) -> str:
    """Extract a value from Binance-style symbol filters list."""
    for f in filters:
        if isinstance(f, dict) and f.get("filterType") == filter_type:
            val = f.get(field)
            if val is not None:
                return str(val)
    return ""


class AsterReferenceDataAdapter(BaseReferenceDataAdapter):
    """Aster reference data: perpetual futures from exchangeInfo REST endpoint.

    Aster uses a Binance Futures-compatible REST API.  Instruments are
    fetched from GET /fapi/v1/exchangeInfo and filtered to PERPETUAL contracts
    with status TRADING.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "aster"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active perpetual instruments from Aster exchangeInfo."""
        # OPTIONS: not supported — venue does not offer listed options contracts
        # FUTURE: not supported — Aster only offers perpetual futures (CLOB, no expiry)
        if instrument_type in (InstrumentType.OPTION, InstrumentType.FUTURE):
            raise UnsupportedCapabilityError(
                venue="ASTER",
                capability=str(instrument_type),
                message=(f"ASTER does not support {instrument_type} instruments. Only PERPETUAL is available."),
            )
        if instrument_type not in (None, InstrumentType.PERPETUAL):
            return []

        url = f"{_BASE}/fapi/v1/exchangeInfo"
        try:
            async with self._make_session() as session, session.get(url) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
                    "endpoint": "exchangeInfo",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # CF-11: re-raise so the venue threads STATE to _fetch_one's failed[]
            # (→ attempted_failed) instead of returning [] (clean empty → silent
            # universe shrink / A8 false-complete). Single-call adapter: this error
            # means the WHOLE Aster universe is unavailable. Mirrors the tradfi fix.
            raise RuntimeError(
                f"Aster exchangeInfo fetch failed (error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc

        data = AsterExchangeInfo.model_validate(raw)
        symbols: list[object] = data.symbols or []
        results: list[InstrumentRecord] = []

        # Pass 1 — collect the eligible TRADING perps. BUG #4 (2026-06-22): the
        # base-asset majors whitelist capped ASTER at ~33; it is REMOVED so every
        # TRADING perp on a canonical quote asset is catalogued (funding valuable for
        # small coins). The quote-asset filter stays (USDT/USDC/USD only).
        # SSOT: plans/active/issues/cefi_hl_aster_batch_data_gaps_2026_06_22.md BUG #4.
        eligible: list[tuple[str, str, str, Decimal, Decimal]] = []
        for sym_raw in symbols:
            if not isinstance(sym_raw, dict):
                continue
            contract_type = sym_raw.get("contractType")
            status = sym_raw.get("status")
            if contract_type != "PERPETUAL" or status != "TRADING":
                continue
            base_asset: str = str(sym_raw.get("baseAsset", ""))
            quote_asset: str = str(sym_raw.get("quoteAsset", "USDC"))
            raw_symbol: str = str(sym_raw.get("symbol", ""))
            if not base_asset or not raw_symbol:
                continue
            if quote_asset.upper() not in CEFI_ACCEPTED_QUOTE_ASSETS:
                continue
            filters: list[object] = sym_raw.get("filters", [])
            tick_str = _extract_filter_value(filters, "PRICE_FILTER", "tickSize")
            lot_str = _extract_filter_value(filters, "LOT_SIZE", "stepSize")
            tick_size = Decimal(tick_str) if tick_str else Decimal("0.01")
            lot_size = Decimal(lot_str) if lot_str else Decimal("0.001")
            eligible.append((raw_symbol, base_asset, quote_asset, tick_size, lot_size))

        # Pass 2 — BUG #4 (B): probe each perp's genesis funding date concurrently so
        # available_from is the true listing date (history-accurate universe).
        listing_dates = await self._probe_earliest_funding_dates([e[0] for e in eligible])
        for raw_symbol, base_asset, quote_asset, tick_size, lot_size in eligible:
            aster_instrument_key = build_instrument_id(
                "ASTER", InstrumentType.PERPETUAL, f"{base_asset}-{quote_asset}@{_MARGIN_MARKER}"
            )
            results.append(
                InstrumentRecord(
                    # Canonical instrument_id: VENUE:PERPETUAL:BASE-QUOTE@LIN|@INV
                    # (2026-07-08 canonicalization — dropped the PERP shorthand + the
                    # raw concatenated exchange symbol in favour of the real
                    # per-instrument settlement currency already parsed above
                    # (confirmed live: 504/509 real ASTER perps quote USDT,
                    # spot-checked via fapi.asterdex.com/fapi/v1/exchangeInfo
                    # 2026-07-08. 2026-07-09 scope-expansion — added the real @LIN
                    # margin marker, see _MARGIN_TYPE above for the verification
                    # method). SSOT:
                    # plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
                    # finding 1 (2026-07-09 PERPETUAL scope-expansion) + finding 3+4;
                    # plans/active/canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md.
                    # Routed through the shared UAC builder (2026-07-09 retrofit,
                    # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 4) — the
                    # marker is embedded in the symbol passed to the builder (PERPETUAL's
                    # ``_build_cefi_simple`` upper-cases the symbol verbatim, same
                    # convention DeFi POOL fee-tiers already use).
                    instrument_key=aster_instrument_key,
                    # No CeFi raw-code-to-human-name translation gap (see other CeFi
                    # adapters' identical comment) — canonical_instrument_id mirrors
                    # instrument_key.
                    canonical_instrument_id=aster_instrument_key,
                    venue="ASTER",
                    raw_symbol=raw_symbol,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    settle_asset=quote_asset,
                    margin_type=_MARGIN_TYPE,
                    tick_size=tick_size,
                    min_size=lot_size,
                    contract_size=Decimal("1"),
                    available_from_datetime=listing_dates.get(raw_symbol, _ASTER_LAUNCH_DATE),
                    status=InstrumentStatus.ACTIVE,
                    timezone="UTC",
                )
            )

        logger.info("Aster: fetched %d perpetual instruments", len(results))
        return results

    async def _probe_earliest_funding_dates(self, symbols: list[str]) -> dict[str, datetime]:
        """Probe each symbol's genesis funding date (fundingRate startTime=0&limit=1).

        Bounded-concurrent; per-symbol timeout; any failure is omitted → caller uses
        the venue launch date. BUG #4 (B).
        """
        sem = asyncio.Semaphore(_FUNDING_PROBE_CONCURRENCY)

        async def _one(symbol: str) -> tuple[str, datetime | None]:
            async with sem:
                last_exc: Exception | None = None
                for attempt in range(_FUNDING_PROBE_RETRIES):
                    try:
                        return symbol, await asyncio.wait_for(
                            self._fetch_earliest_funding_date(symbol), timeout=_FUNDING_PROBE_TIMEOUT_S
                        )
                    except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, TypeError) as exc:
                        last_exc = exc
                        await asyncio.sleep(_FUNDING_PROBE_BACKOFF_BASE_S * (2.0**attempt))  # backoff on 429
                logger.warning("Aster earliest-funding probe failed for %s: %s — using launch date", symbol, last_exc)
                return symbol, None

        out: dict[str, datetime] = {}
        for symbol, dt in await asyncio.gather(*(_one(s) for s in symbols)):
            if dt is not None:
                out[symbol] = dt
        logger.info("Aster earliest-funding probe: resolved %d/%d genesis dates", len(out), len(symbols))
        return out

    async def _fetch_earliest_funding_date(self, symbol: str) -> datetime | None:
        """Return the datetime of the symbol's first-ever funding entry, or None.

        Binance-compat ``fundingRate`` clamps ``startTime=0`` to "recent" (returns
        today). Passing an explicit early ``startTime`` (before any possible listing)
        + ``limit=1`` returns the EARLIEST entry at/after it ascending → the genesis
        funding date. Use a pre-history floor well before the ASTER launch.
        """
        url = f"{_BASE}/fapi/v1/fundingRate"
        early_ms = int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1000)
        params = {"symbol": symbol, "startTime": str(early_ms), "limit": "1"}
        async with self._make_session() as session, session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = cast(object, await resp.json())
        if not isinstance(data, list) or not data:
            return None
        first = cast(dict[str, object], data[0])
        ts_ms = first.get("fundingTime")
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
        raise NotImplementedError("Aster does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Aster perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Aster premiumIndex endpoint.

        GET /fapi/v1/premiumIndex?symbol=<symbol>
        Returns lastFundingRate and nextFundingTime.
        """
        url = f"{_BASE}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        now = datetime.now(UTC)

        try:
            async with self._make_session() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster premiumIndex request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
                    "endpoint": "premiumIndex",
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

        entry: dict[str, object] = raw if isinstance(raw, dict) else {}
        rate_raw = entry.get("lastFundingRate", "0") or "0"
        next_time_raw = entry.get("nextFundingTime", 0)
        mark_raw = entry.get("markPrice", "0") or "0"

        next_funding_time = (
            datetime.fromtimestamp(int(str(next_time_raw)) / 1000, tz=UTC) if int(str(next_time_raw)) > 0 else now
        )

        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=Decimal(str(mark_raw)) if mark_raw != "0" else None,
            updated_at=now,
        )

    _INTERVAL_SECONDS: ClassVar[dict[str, int]] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
        "1w": 604800,
    }

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Aster klines endpoint (Binance Futures-compatible).

        GET /fapi/v1/klines?symbol=<symbol>&interval=<interval>&limit=<limit>
        """
        aster_interval = interval if interval in self._INTERVAL_SECONDS else "1d"
        bar_seconds = self._INTERVAL_SECONDS.get(aster_interval, 86400)

        now = datetime.now(UTC)
        end_ts = int(now.timestamp() * 1000)
        start_ts = end_ts - limit * bar_seconds * 1000

        url = f"{_BASE}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": aster_interval,
            "startTime": str(start_ts),
            "endTime": str(end_ts),
            "limit": str(limit),
        }

        try:
            async with self._make_session() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_ohlcv: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster klines request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
                    "endpoint": "klines",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        candles: list[object] = raw_ohlcv if isinstance(raw_ohlcv, list) else []
        results: list[OHLCVRef] = []

        for candle_raw in candles:
            if not isinstance(candle_raw, list) or len(candle_raw) < 7:
                continue
            # Binance kline format: [openTime, open, high, low, close, volume, closeTime, ...]
            # Use closeTime (index 6) — the right/close edge — to avoid look-ahead leakage.
            ts = datetime.fromtimestamp(int(str(candle_raw[6])) / 1000, tz=UTC)
            results.append(
                OHLCVRef(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(candle_raw[1])),
                    high=Decimal(str(candle_raw[2])),
                    low=Decimal(str(candle_raw[3])),
                    close=Decimal(str(candle_raw[4])),
                    volume=Decimal(str(candle_raw[5])),
                    interval=aster_interval,
                )
            )

        return results
