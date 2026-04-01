"""Tardis reference data adapter — historical tick data provider.

Tardis provides historical trades/orderbook data for crypto derivatives.
This adapter retrieves instrument metadata via the public exchanges REST endpoint.
API key optional for public instrument listing; required for higher rate limits.

API key: store in Secret Manager as TARDIS_API_KEY.
Auth: Authorization: Bearer {api_key} (header).
Base URL: https://api.tardis.dev/v1

Supported exchanges (configurable):
  binance-futures, bybit, okex, deribit

Not applicable: crypto funding rates (historical; use tardis-client library),
OHLCV bars (requires /replay endpoint — out of scope for URDI REST adapter).
"""

import asyncio
import contextlib
import json
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import (
    CEFI_ACCEPTED_QUOTE_ASSETS,
    CEFI_BASE_ASSET_UNIVERSE,
    CEFI_OPTIONS_UNDERLYINGS,
    TardisExchangeDetail,
    TardisInstrumentDetail,
    VenueMapping,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentLeg, InstrumentRecord, InstrumentType, OptionType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_TARDIS_BASE = "https://api.tardis.dev/v1"


def _normalize_option_type(raw: str | None) -> OptionType | None:
    """Normalize option type to OptionType enum, or None if unrecognised."""
    if not raw:
        return None
    upper = raw.strip().upper()
    if upper in ("CALL", "C"):
        return OptionType.CALL
    if upper in ("PUT", "P"):
        return OptionType.PUT
    return None


# Retry configuration for Tardis instrument listing requests
_TARDIS_RETRY_ATTEMPTS: int = 3
_TARDIS_RETRY_BASE_DELAY: float = 2.0  # seconds; doubles on each retry
_TARDIS_RETRYABLE_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

_DEFAULT_EXCHANGES: list[str] = [
    # Tier 1 — primary CeFi venues
    "binance",  # Binance spot
    "binance-futures",  # Binance USDT-M + COIN-M futures
    "bybit",  # Bybit spot + derivatives
    "okex",  # OKX spot + derivatives
    "deribit",  # Deribit options + futures (BTC/ETH)
    "coinbase",  # Coinbase spot (coinbase premium)
    # Tier 2
    "upbit",  # Upbit spot (kimchi premium)
]

# Tardis instrument type → canonical InstrumentType
_TYPE_MAP: dict[str, InstrumentType] = {
    "perpetual": InstrumentType.PERPETUAL,
    "future": InstrumentType.FUTURE,
    "option": InstrumentType.OPTION,
    "spot": InstrumentType.SPOT_PAIR,
    "combo": InstrumentType.COMBO,
}

# Quote currencies for symbol splitting (longest first for correct prefix matching)
# Extended from instruments-service/engine/processors/symbol_parser.py _ALL_QUOTE_SUFFIXES
_QUOTE_CURRENCIES: list[str] = [
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "USDE",
    "BRZ",
    "IDRT",
    "BIDR",
    "USD",
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "TRX",
    "ADA",
    "SOL",
    "LTC",
    "DOT",
    "LINK",
    "UNI",
    "AVAX",
    "DOGE",
    "SHIB",
    "PEPE",
    "FLOKI",
    "WIF",
    "BONK",
    "MEME",
    "KRW",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "TRY",
    "BRL",
    "RUB",
    "NGN",
    "ZAR",
    "DAI",
    "VAI",
    "GEL",
    "CZK",
    "MXN",
    "ARS",
    "COP",
    "CLP",
    "PEN",
    "VES",
    "UAH",
    "PLN",
    "RON",
    "CNY",
    "HKD",
]

# Set version for O(1) lookup in _split_symbol
_QUOTE_CURRENCIES_SET: frozenset[str] = frozenset(_QUOTE_CURRENCIES)

# Singleton venue mapping for exchange→canonical venue resolution
_VENUE_MAPPING = VenueMapping()


def _classify_tardis_error(exc: Exception, status: int | None = None) -> str:
    """Map a Tardis HTTP/network error to a UAC error code for classification."""
    if status == 429:
        return "429"
    if status == 401:
        return "401"
    if status is not None and status >= 500:
        return "500"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "429"
    if "401" in msg or "auth" in msg or "unauthorized" in msg:
        return "401"
    if "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


def _parse_expiry(s: str | None) -> datetime | None:
    """Parse ISO date/datetime string (with optional Z suffix) to UTC datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


_DERIBIT_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _parse_deribit_symbol_expiry(raw_id: str) -> datetime | None:
    """Parse expiry from Deribit symbol format: BTC-27MAR26-190000-C.

    The second segment is DDMMMYY (e.g. 27MAR26 = 2026-03-27).
    Also handles futures like BTC-27MAR26 (no strike/option segments).
    """
    parts = raw_id.split("-")
    if len(parts) < 2 or len(parts[1]) < 5:
        return None
    return _parse_ddmmmyy(parts[1])


def _parse_ddmmmyy(date_part: str) -> datetime | None:
    """Parse DDMMMYY string (e.g. '27MAR26') into a UTC datetime."""
    try:
        m = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", date_part.upper())
        if not m:
            return None
        day, month_str, year_str = int(m.group(1)), m.group(2), int(m.group(3))
        month = _DERIBIT_MONTHS.get(month_str)
        if not month:
            return None
        return datetime(2000 + year_str, month, day, tzinfo=UTC)
    except (ValueError, IndexError):
        return None


def _parse_yymmdd_symbol_expiry(raw_id: str) -> datetime | None:
    """Parse expiry from OKX-style symbol: BASE-QUOTE-YYMMDD (e.g. BTC-USD-260626).

    The last segment is a 6-digit YYMMDD date string.  Also handles the
    shorter BASE-YYMMDD variant used by some exchanges.
    """
    parts = raw_id.split("-")
    # Try last segment first (covers both BASE-QUOTE-YYMMDD and BASE-YYMMDD)
    for idx in (-1, -2):
        if abs(idx) > len(parts):
            continue
        seg = parts[idx]
        if len(seg) == 6 and seg.isdigit():
            try:
                yy, mm, dd = int(seg[:2]), int(seg[2:4]), int(seg[4:6])
                return datetime(2000 + yy, mm, dd, tzinfo=UTC)
            except ValueError:
                continue
    return None


def _resolve_base_quote(item: TardisInstrumentDetail, raw_id: str, exchange: str) -> tuple[str, str]:
    """Parse base/quote from Tardis metadata or fall back to symbol splitting.

    For derivatives without an explicit quote suffix (e.g. BTC-PERPETUAL,
    BTC-27MAR26-190000-C), the settlement currency is inferred from the
    exchange context:
      - deribit: USD (inverse) or USDC (linear)
      - okex coin-margined (-USD_UM-): USD
      - binance-futures COIN-M: USD
      - all others: USD as safe default for derivatives
    """
    base = item.baseCurrency or ""
    quote = item.quoteCurrency or ""
    if base and quote:
        return base, quote

    upper_id = raw_id.upper()
    if "-" in upper_id:
        parts = upper_id.split("-")
        # UPBIT uses QUOTE-BASE format: KRW-BTC, BTC-DOT, USDT-SOL
        if exchange == "upbit" and len(parts) >= 2:
            return parts[1], parts[0]
        if len(parts) >= 2 and parts[1] in (
            "USDT",
            "USDC",
            "USD",
            "BUSD",
            "EUR",
            "GBP",
            "KRW",
        ):
            return parts[0], parts[1]
        # Derivatives without explicit quote suffix — infer settlement currency.
        # Deribit USDC-denominated instruments contain "USDC" in the symbol.
        derived_base = parts[0]
        derived_quote = _infer_derivative_quote(upper_id, exchange)
        # Handle underscore-separated base: BTC_USDC → base=BTC, quote=USDC
        # Deribit linear instruments use BASE_QUOTE format (BTC_USDC-PERPETUAL)
        if "_" in derived_base:
            sub_parts = derived_base.split("_", 1)
            if sub_parts[1] in ("USDC", "USDT", "USD", "BUSD"):
                derived_base = sub_parts[0]
                derived_quote = sub_parts[1]
        return derived_base, derived_quote

    # Concatenated: BTCUSDT, ETHUSDT → split by known quote suffix
    return _split_symbol(upper_id)


def _infer_derivative_quote(upper_id: str, exchange: str) -> str:
    """Infer the quote/settlement currency for a derivative without explicit quote suffix.

    Returns the most specific match based on symbol patterns and exchange conventions.
    """
    # USDC-denominated: symbol contains USDC (e.g. BTC_USDC-PERPETUAL on deribit)
    if "USDC" in upper_id:
        return "USDC"
    # USDT-denominated: symbol contains USDT (e.g. BTCUSDT_PERP on some exchanges)
    if "USDT" in upper_id:
        return "USDT"
    # OKX coin-margined: BTC-USD_UM-SWAP, BTC-USD_UM-260626
    if "USD_UM" in upper_id or "USD_CM" in upper_id:
        return "USD"
    # Everything else: crypto derivatives default to USD settlement
    # (deribit inverse, binance COIN-M, generic perpetuals/futures)
    return "USD"


def _passes_asset_filter(base: str, quote: str, instrument_type: str) -> bool:
    """Check if the instrument passes the curated asset universe filter."""
    base_upper = base.upper()
    quote_upper = quote.upper() if quote else ""
    if base_upper not in CEFI_BASE_ASSET_UNIVERSE:
        return False
    # Derivatives (perps, futures, options) have no quote — allow them through
    if quote_upper and quote_upper not in CEFI_ACCEPTED_QUOTE_ASSETS:
        return False
    # Options: only BTC and ETH underlyings (too much data otherwise)
    return not (instrument_type == "OPTION" and base_upper not in CEFI_OPTIONS_UNDERLYINGS)


def _resolve_option_fields(
    item: TardisInstrumentDetail, instrument_type: str, raw_id: str
) -> tuple[Decimal | None, str | None]:
    """Extract strike price and option type from Tardis item metadata.

    Falls back to parsing Deribit-style option symbol names
    (e.g. BTC-27MAR26-190000-C).
    """
    strike_raw = item.strikePrice
    strike = Decimal(str(strike_raw)) if strike_raw is not None else None
    opt_type_raw = item.optionType
    opt_type = _normalize_option_type(opt_type_raw)

    if instrument_type.upper() == "OPTION" and (strike is None or opt_type is None):
        parts = raw_id.split("-")
        if len(parts) >= 4:
            if strike is None:
                with contextlib.suppress(Exception):
                    strike = Decimal(parts[-2])
            if opt_type is None and parts[-1] in ("C", "P"):
                opt_type = _normalize_option_type(parts[-1])

    return strike, opt_type


class TardisReferenceDataAdapter(BaseReferenceDataAdapter):
    """Tardis reference data adapter.

    Fetches instrument metadata from the Tardis exchanges REST endpoint.
    Exchanges iterated by default: binance-futures, bybit, okex, deribit.
    Pass exchanges=[...] to the constructor to override.

    Not applicable: live funding rates, OHLCV bars (requires /replay endpoint).
    """

    def __init__(
        self,
        project_id: str | None = None,
        exchanges: list[str] | None = None,
        api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._exchanges: list[str] = exchanges if exchanges is not None else _DEFAULT_EXCHANGES

    @property
    def venue(self) -> str:
        return "tardis"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        api_key = self._optional_api_key()
        results: list[InstrumentRecord] = []
        async with aiohttp.ClientSession() as session:
            for exchange in self._exchanges:
                batch = await self._fetch_exchange_instruments(session, api_key, exchange)
                if instrument_type is not None:
                    batch = [r for r in batch if r.instrument_type == instrument_type]
                results.extend(batch)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
        instruments = await self.get_instruments(instrument_type="OPTION")
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            base = inst.base_asset or ""
            if underlying.upper() not in base.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            if inst.option_type == OptionType.CALL:
                calls.append(inst)
            else:
                puts.append(inst)
        now = datetime.now(UTC)
        target_expiry = expiry or (calls[0].expiry if calls else now)
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=target_expiry or now,
            strikes=sorted(strikes),
            calls=calls,
            puts=puts,
            fetched_at=now,
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        instruments = await self.get_instruments(instrument_type=instrument_type)
        expiry_set: set[datetime] = set()
        for inst in instruments:
            base = inst.base_asset or ""
            if underlying.upper() in base.upper() and inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch recent funding rate via Tardis data-feeds endpoint.

        Queries /v1/data-feeds for derivative_ticker channel data.
        Requires API key from Secret Manager as TARDIS_API_KEY.
        """
        now = datetime.now(UTC)
        from_ts = int((now.timestamp() - 8 * 3600) * 1000)
        to_ts = int(now.timestamp() * 1000)
        filter_json = json.dumps([{"channel": "derivative_ticker", "symbols": [symbol]}])
        headers = self._build_datafeed_headers()
        last_rate, last_exchange = await self._scan_exchanges_for_funding_rate(
            symbol, from_ts, to_ts, filter_json, headers
        )
        if last_rate is None:
            raise RuntimeError(f"No funding rate for '{symbol}' in Tardis exchanges: {self._exchanges}.")
        return self._make_funding_rate_ref(symbol, last_rate, last_exchange, now)

    async def _scan_exchanges_for_funding_rate(
        self,
        symbol: str,
        from_ts: int,
        to_ts: int,
        filter_json: str,
        headers: dict[str, str],
    ) -> tuple[dict[str, object] | None, str]:
        """Scan configured exchanges for most recent derivative_ticker funding rate."""
        last_exchange: str = self._exchanges[0] if self._exchanges else "deribit"
        async with aiohttp.ClientSession() as session:
            for exchange in self._exchanges:
                rate = await self._find_funding_rate(session, exchange, from_ts, to_ts, filter_json, headers, None)
                if rate is not None:
                    return rate, exchange
        return None, last_exchange

    def _make_funding_rate_ref(
        self,
        symbol: str,
        last_rate: dict[str, object],
        last_exchange: str,
        now: datetime,
    ) -> FundingRateRef:
        """Build FundingRateRef from raw Tardis message dict."""
        rate_raw = last_rate.get("fundingRate", "0") or "0"
        ts_raw = last_rate.get("timestamp")
        next_funding_time = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC) if ts_raw else now
        logger.debug("Tardis funding rate %s/%s: %s", last_exchange, symbol, rate_raw)
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars via Tardis data-feeds trade_bar channel.

        Queries /v1/data-feeds for trade_bar_{interval} channel data.
        Supported intervals: 1m, 1h, 1d. Others fall back to 1d.
        API key required; fetched from Secret Manager as TARDIS_API_KEY.
        """
        bar_seconds, channel = self._resolve_bar_type(interval)
        headers = self._build_datafeed_headers()
        now = datetime.now(UTC)
        from_ts = int((now.timestamp() - limit * bar_seconds) * 1000)
        to_ts = int(now.timestamp() * 1000)
        filter_json = json.dumps([{"channel": channel, "symbols": [symbol]}])
        results = await self._collect_ohlcv_from_exchanges(symbol, interval, from_ts, to_ts, filter_json, headers)
        return results[-limit:] if len(results) > limit else results

    @staticmethod
    def _resolve_bar_type(interval: str) -> tuple[int, str]:
        """Return (bar_seconds, channel_name) for the given interval string."""
        _seconds: dict[str, int] = {"1m": 60, "1h": 3600, "1d": 86400}
        _channels: dict[str, str] = {
            "1m": "trade_bar_1m",
            "1h": "trade_bar_1h",
            "1d": "trade_bar_1d",
        }
        return _seconds.get(interval, 86400), _channels.get(interval, "trade_bar_1d")

    async def _collect_ohlcv_from_exchanges(
        self,
        symbol: str,
        interval: str,
        from_ts: int,
        to_ts: int,
        filter_json: str,
        headers: dict[str, str],
    ) -> list[OHLCVRef]:
        """Collect OHLCV bars from the first exchange that returns data."""
        results: list[OHLCVRef] = []
        async with aiohttp.ClientSession() as session:
            for exchange in self._exchanges:
                if results:
                    break
                batch = await self._fetch_ohlcv_from_exchange(
                    session, exchange, from_ts, to_ts, filter_json, headers, symbol, interval
                )
                results.extend(batch)
        return results

    async def _find_funding_rate(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        from_ts: int,
        to_ts: int,
        filter_json: str,
        headers: dict[str, str],
        json_mod: object,
    ) -> dict[str, object] | None:
        """Scan NDJSON from one exchange for most recent derivative_ticker with fundingRate."""
        try:
            text = await self._fetch_datafeed_text(session, exchange, from_ts, to_ts, filter_json, headers)
        except aiohttp.ClientError as exc:
            error_code = _classify_tardis_error(exc)
            classification = classify_venue_error("tardis", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.warning(
                "Tardis funding fetch failed for %s: %s (classified: %s, action: %s)",
                exchange,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "tardis",
                    "exchange": exchange,
                    "endpoint": "funding_rate",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return None
        if text is None:
            return None
        for line in reversed(text.strip().splitlines()):
            if not line.strip():
                continue
            msg: dict[str, object] = cast(dict[str, object], json.loads(line))
            if msg.get("fundingRate") is not None:
                return msg
        return None

    async def _fetch_ohlcv_from_exchange(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        from_ts: int,
        to_ts: int,
        filter_json: str,
        headers: dict[str, str],
        symbol: str,
        interval: str,
    ) -> list[OHLCVRef]:
        """Fetch and parse OHLCV bars from a single Tardis exchange."""
        try:
            text = await self._fetch_datafeed_text(session, exchange, from_ts, to_ts, filter_json, headers)
        except aiohttp.ClientError as exc:
            error_code = _classify_tardis_error(exc)
            classification = classify_venue_error("tardis", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.warning(
                "Tardis OHLCV fetch failed for %s: %s (classified: %s, action: %s)",
                exchange,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "tardis",
                    "exchange": exchange,
                    "endpoint": "ohlcv",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        if text is None:
            return []
        results: list[OHLCVRef] = []
        for line in text.strip().splitlines():
            if not line.strip():
                continue
            msg: dict[str, object] = cast(dict[str, object], json.loads(line))
            bar = self._parse_ohlcv_line(msg, symbol, interval)
            if bar is not None:
                results.append(bar)
        return results

    def _build_datafeed_headers(self) -> dict[str, str]:
        """Build auth headers for Tardis data-feeds endpoint."""
        api_key = self._optional_api_key()
        if api_key:
            return {"Authorization": f"Bearer {api_key}"}
        return {}

    async def _fetch_datafeed_text(
        self,
        session: aiohttp.ClientSession,
        exchange: str,
        from_ts: int,
        to_ts: int,
        filter_json: str,
        headers: dict[str, str],
    ) -> str | None:
        """Fetch raw NDJSON text from Tardis data-feeds for one exchange.

        Returns None if exchange responds with 404/422 (not found / invalid).
        Raises RuntimeError on 401 auth failure.
        """
        url = f"{_TARDIS_BASE}/data-feeds"
        params: dict[str, str] = {
            "exchange": exchange,
            "from": str(from_ts),
            "to": str(to_ts),
            "filters": filter_json,
        }
        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status in (404, 422):
                return None
            if resp.status == 401:
                raise RuntimeError(
                    "Tardis API key required for data-feeds endpoint. "
                    "Service must fetch TARDIS_API_KEY from Secret Manager "
                    "and pass it via api_key= constructor parameter."
                )
            resp.raise_for_status()
            return await resp.text()

    def _parse_ohlcv_line(self, msg: dict[str, object], symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a Tardis trade_bar NDJSON message into an OHLCVRef.

        Returns None if message is missing required open or timestamp fields.
        """
        open_val = msg.get("open")
        if open_val is None:
            return None
        ts_raw = msg.get("timestamp")
        if ts_raw is None:
            return None
        ts = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(open_val)),
            high=Decimal(str(msg.get("high", open_val) or open_val)),
            low=Decimal(str(msg.get("low", open_val) or open_val)),
            close=Decimal(str(msg.get("close", open_val) or open_val)),
            volume=Decimal(str(msg.get("volume", "0") or "0")),
            interval=interval,
        )

    async def _fetch_exchange_instruments(
        self,
        session: aiohttp.ClientSession,
        api_key: str | None,
        exchange: str,
    ) -> list[InstrumentRecord]:
        # Primary: /v1/instruments/{exchange} — full metadata (priceIncrement, contractMultiplier).
        # Requires Tardis pro/business subscription. Falls back to /v1/exchanges/{exchange}
        # (free tier, basic availability only — tick sizes use exchange-specific defaults).
        instruments_list: list[TardisInstrumentDetail] | None = None
        last_exc: Exception | None = None
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        # Try /v1/instruments first (rich metadata)
        url = f"{_TARDIS_BASE}/instruments/{exchange}"
        used_instruments_api = False

        for attempt in range(_TARDIS_RETRY_ATTEMPTS):
            delay = _TARDIS_RETRY_BASE_DELAY * (1 << attempt)
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 401:
                        # Pro subscription required — fall back to /v1/exchanges
                        logger.debug(
                            "Tardis %r: /v1/instruments requires pro tier, falling back to /v1/exchanges", exchange
                        )
                        break
                    if resp.status == 404:
                        logger.warning("Tardis exchange %r not found — skipping", exchange)
                        return []
                    if resp.status in _TARDIS_RETRYABLE_CODES:
                        logger.warning(
                            "Tardis %r: HTTP %d (attempt %d/%d, retry in %.0fs)",
                            exchange,
                            resp.status,
                            attempt + 1,
                            _TARDIS_RETRY_ATTEMPTS,
                            delay,
                        )
                        if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                            await asyncio.sleep(delay)
                            continue
                        resp.raise_for_status()
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
                    if isinstance(raw_json, list):
                        instruments_list = [TardisInstrumentDetail.model_validate(item) for item in raw_json]
                        used_instruments_api = True
                    break
            except aiohttp.ClientError as exc:
                last_exc = exc
                if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                    logger.warning(
                        "Tardis %r: %s (attempt %d/%d, retry in %.0fs)",
                        exchange,
                        exc,
                        attempt + 1,
                        _TARDIS_RETRY_ATTEMPTS,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                # Final attempt failed — fall back to /v1/exchanges
                logger.debug("Tardis %r: /v1/instruments failed, falling back to /v1/exchanges: %s", exchange, exc)
                break

        # Fallback: /v1/exchanges/{exchange} (free tier — basic availability, no tick sizes)
        if instruments_list is None:
            url = f"{_TARDIS_BASE}/exchanges/{exchange}"
            for attempt in range(_TARDIS_RETRY_ATTEMPTS):
                delay = _TARDIS_RETRY_BASE_DELAY * (1 << attempt)
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 404:
                            logger.warning("Tardis exchange %r not found — skipping", exchange)
                            return []
                        if resp.status in _TARDIS_RETRYABLE_CODES:
                            if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                                await asyncio.sleep(delay)
                                continue
                            resp.raise_for_status()
                        resp.raise_for_status()
                        raw_json = cast(object, await resp.json())
                        exchange_detail = TardisExchangeDetail.model_validate(raw_json)
                        instruments_list = exchange_detail.instruments
                        break
                except aiohttp.ClientError as exc:
                    last_exc = exc
                    if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                        await asyncio.sleep(delay)
                        continue
                    error_code = _classify_tardis_error(exc)
                    classification = classify_venue_error("tardis", error_code)
                    action = classification.action.value if classification else "fail"
                    retry_safe = classification.retry_safe if classification else False
                    logger.error(
                        "Tardis request failed for exchange %r after %d attempts: %s "
                        "(classified: %s, action: %s, retry=%s)",
                        exchange,
                        _TARDIS_RETRY_ATTEMPTS,
                        exc,
                        error_code,
                        action,
                        retry_safe,
                    )
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "tardis",
                            "exchange": exchange,
                            "error": str(exc),
                            "error_code": error_code,
                            "action": action,
                            "retry_safe": retry_safe,
                        },
                    )
                    return []

        if instruments_list is None:
            logger.error(
                "Tardis %r: no response after %d attempts (last error: %s)",
                exchange,
                _TARDIS_RETRY_ATTEMPTS,
                last_exc,
            )
            return []

        # Parse instruments with diagnostic logging
        api_count = len(instruments_list)
        results: list[InstrumentRecord] = []
        filtered_count = 0
        for item in instruments_list:
            record = self._parse_tardis_instrument(item, exchange)
            if record is not None:
                results.append(record)
            else:
                filtered_count += 1
        api_label = "instruments" if used_instruments_api else "exchanges"
        logger.info(
            "Tardis %s: API /%s returned %d symbols, parsed %d instruments, filtered %d",
            exchange,
            api_label,
            api_count,
            len(results),
            filtered_count,
        )
        return results

    def _parse_tardis_instrument(
        self,
        item: TardisInstrumentDetail,
        exchange: str,
    ) -> InstrumentRecord | None:
        raw_id = item.id
        if not raw_id:
            return None
        tardis_type = item.type or "spot"
        instrument_type: InstrumentType = _TYPE_MAP.get(tardis_type, InstrumentType.SPOT_PAIR)

        # Resolve canonical venue name from UAC VenueMapping (e.g. "binance" → "BINANCE-SPOT")
        canonical_venue = _VENUE_MAPPING.tardis_to_venue.get(exchange, exchange.upper())

        # Parse base/quote — prefer Tardis metadata, fall back to symbol splitting
        base, quote = _resolve_base_quote(item, raw_id, exchange)

        # Filter: base must be in curated universe, quote (if present) must be accepted
        if not _passes_asset_filter(base, quote, instrument_type):
            return None

        # Canonical symbol: BASE-QUOTE for spot/perp (one per pair),
        # raw_id for derivatives where expiry/strike/structure matter for uniqueness.
        if instrument_type in (InstrumentType.SPOT_PAIR, InstrumentType.PERPETUAL):
            symbol = f"{base}-{quote}" if base and quote else raw_id.upper()
        else:
            symbol = raw_id.upper()

        available_since = item.availableSince
        available_to = item.availableTo
        is_active = available_to is None
        available_since_dt = _parse_expiry(available_since) if available_since else None
        available_to_dt = _parse_expiry(available_to) if available_to else None
        expiry = _parse_expiry(item.expiry)
        if expiry is None and not is_active and available_to:
            expiry = _parse_expiry(available_to)
        # Deribit symbol format: BTC-27MAR26-190000-C → parse expiry from 2nd segment
        if expiry is None and "-" in raw_id:
            expiry = _parse_deribit_symbol_expiry(raw_id)
        # OKX symbol format: BTC-USD-260626 → parse YYMMDD from last segment
        if expiry is None and "-" in raw_id:
            expiry = _parse_yymmdd_symbol_expiry(raw_id)

        strike, opt_type = _resolve_option_fields(item, instrument_type, raw_id)

        # underlying: required for CeFi derivatives (FUTURE/OPTION), not for COMBO
        underlying: str | None = None
        if instrument_type in (InstrumentType.FUTURE, InstrumentType.OPTION, InstrumentType.PERPETUAL) and base:
            underlying = base

        # Use Tardis instrument spec fields when available (from /v1/instruments endpoint),
        # otherwise fall back to sensible defaults.
        tick_size = (
            Decimal(str(item.priceIncrement)) if item.priceIncrement and item.priceIncrement > 0 else Decimal("0.01")
        )
        min_size = (
            Decimal(str(item.minTradeAmount))
            if item.minTradeAmount and item.minTradeAmount > 0
            else (
                Decimal(str(item.amountIncrement))
                if item.amountIncrement and item.amountIncrement > 0
                else Decimal("0.001")  # conservative fallback in base asset units (e.g. 0.001 BTC)
            )
        )
        contract_size = (
            Decimal(str(item.contractMultiplier))
            if item.contractMultiplier and item.contractMultiplier > 0
            else Decimal("1")
        )

        # Canonical instrument_key: VENUE:INSTRUMENT_TYPE:BASE-QUOTE
        instrument_key = f"{canonical_venue}:{instrument_type}:{symbol}"
        is_combo = instrument_type == InstrumentType.COMBO

        # COMBO instruments: parse real legs from Deribit symbol encoding.
        # Skip combos where legs can't be resolved — no placeholders.
        legs: list[InstrumentLeg] | None = None
        if is_combo:
            legs = _parse_deribit_combo_legs(raw_id, canonical_venue)
            if not legs:
                return None

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=canonical_venue,
            raw_symbol=raw_id,
            instrument_type=instrument_type,
            base_asset=base,
            quote_asset=quote,
            tick_size=tick_size if not is_combo else None,
            min_size=min_size if not is_combo else None,
            contract_size=contract_size if not is_combo else None,
            expiry=expiry,
            strike=strike if not is_combo else None,
            option_type=opt_type if not is_combo else None,
            underlying=underlying,
            legs=legs,
            available_from_datetime=available_since_dt,
            available_to_datetime=available_to_dt,
            timezone="UTC",
        )


# ---------------------------------------------------------------------------
# Deribit combo/spread symbol parser
# ---------------------------------------------------------------------------
# Deribit encodes legs deterministically in the symbol name.
# Format: BASE-CODE-EXPIRY-STRIKES  (single-expiry)
#         BASE-CODE-EXP1_EXP2-STRIKES  (calendar/diagonal)
# Where CODE is one of 33 structure codes.  Strikes separated by _.
# PERP in expiry position → BASE-PERPETUAL instrument name.

# Maps structure code → list of (option_type, side, ratio) per leg position.
# option_type: "C", "P", or None (future/perp leg).
# side: "BUY" or "SELL".  ratio: int multiplier.
_DERIBIT_COMBO_STRUCTURES: dict[str, list[tuple[str | None, str, int]]] = {
    # --- Future spreads (2 legs) ---
    "FS": [(None, "BUY", 1), (None, "SELL", 1)],
    # --- Vanilla option spreads (2 legs, same expiry) ---
    "CS": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PS": [("P", "BUY", 1), ("P", "SELL", 1)],
    "STRD": [("C", "BUY", 1), ("P", "BUY", 1)],
    "STRG": [("P", "BUY", 1), ("C", "BUY", 1)],
    "RR": [("P", "BUY", 1), ("C", "SELL", 1)],
    "RRITM": [("P", "BUY", 1), ("C", "SELL", 1)],
    "GUTS": [("C", "BUY", 1), ("P", "BUY", 1)],
    "REV": [("C", "BUY", 1), ("P", "SELL", 1)],
    # --- 3-leg (butterflies, ladders) ---
    "CBUT": [("C", "BUY", 1), ("C", "SELL", 2), ("C", "BUY", 1)],
    "PBUT": [("P", "BUY", 1), ("P", "SELL", 2), ("P", "BUY", 1)],
    "CBUT111": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "PBUT111": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "BUY", 1)],
    "CLAD": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "SELL", 1)],
    "PLAD": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "SELL", 1)],
    # --- 4-leg (condors, iron butterflies, boxes) ---
    "IBUT": [("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1), ("C", "BUY", 1)],
    "ICOND": [("P", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "CCOND": [("C", "BUY", 1), ("C", "SELL", 1), ("C", "SELL", 1), ("C", "BUY", 1)],
    "PCOND": [("P", "BUY", 1), ("P", "SELL", 1), ("P", "SELL", 1), ("P", "BUY", 1)],
    "BOX": [("C", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("P", "BUY", 1)],
    # --- Calendar / diagonal (2 expiries) ---
    "CCAL": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PCAL": [("P", "BUY", 1), ("P", "SELL", 1)],
    "CDIAG": [("C", "BUY", 1), ("C", "SELL", 1)],
    "PDIAG": [("P", "BUY", 1), ("P", "SELL", 1)],
    "STDC": [("C", "BUY", 1), ("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1)],
    "DSTDC": [("C", "BUY", 1), ("P", "BUY", 1), ("C", "SELL", 1), ("P", "SELL", 1)],
    # --- Ratio spreads (2 legs, unequal ratios) ---
    "CSR12": [("C", "BUY", 1), ("C", "SELL", 2)],
    "CSR13": [("C", "BUY", 1), ("C", "SELL", 3)],
    "CSR23": [("C", "BUY", 2), ("C", "SELL", 3)],
    "PSR12": [("P", "BUY", 1), ("P", "SELL", 2)],
    "PSR13": [("P", "BUY", 1), ("P", "SELL", 3)],
    "PSR23": [("P", "BUY", 2), ("P", "SELL", 3)],
    # --- Jelly roll (4 legs, 2 expiries) ---
    "JR": [("C", "BUY", 1), ("P", "SELL", 1), ("C", "SELL", 1), ("P", "BUY", 1)],
}

# Codes that use two expiries (calendar/diagonal families)
_DERIBIT_DUAL_EXPIRY_CODES = frozenset(
    {
        "CCAL",
        "PCAL",
        "CDIAG",
        "PDIAG",
        "STDC",
        "DSTDC",
        "JR",
    }
)


def _parse_deribit_combo_legs(raw_id: str, venue: str) -> list[InstrumentLeg]:
    """Parse Deribit combo symbol into InstrumentLeg list.

    Symbol format examples:
        BTC-FS-25APR26_PERP          → future spread
        BTC-CS-25APR26-90000_100000  → call spread
        BTC-CBUT-25APR26-80000_90000_100000  → call butterfly
        ETH_USDC-FS-25APR26_PERP    → linear future spread
        BTC-CCAL-25APR26_3APR26-90000 → call calendar

    Returns empty list if symbol can't be parsed (caller skips the combo).
    """
    # Split: BASE-CODE-REST
    # Base may contain underscore (BTC_USDC), so find the structure code.
    parts = raw_id.split("-")
    if len(parts) < 3:
        return []

    # Find the structure code — it's the first part that matches a known code.
    code_idx = -1
    code = ""
    for i, p in enumerate(parts):
        if p in _DERIBIT_COMBO_STRUCTURES:
            code_idx = i
            code = p
            break

    if code_idx < 0:
        return []

    base = "-".join(parts[:code_idx])  # e.g. "BTC" or "BTC_USDC" or "ETH"
    rest = parts[code_idx + 1 :]  # everything after the code
    structure = _DERIBIT_COMBO_STRUCTURES[code]
    is_dual_expiry = code in _DERIBIT_DUAL_EXPIRY_CODES

    # --- Future spread (FS): BASE-FS-EXP1_EXP2 ---
    if code == "FS":
        if not rest:
            return []
        expiry_part = rest[0]  # e.g. "25APR26_PERP" or "25APR26_3APR26"
        expiries = expiry_part.split("_")
        legs: list[InstrumentLeg] = []
        for i, (_, side, ratio) in enumerate(structure):
            if i >= len(expiries):
                break
            exp = expiries[i]
            # PERP → BASE-PERPETUAL, else BASE-EXPIRY (Deribit future format)
            if exp == "PERP":
                leg_name = f"{base}-PERPETUAL"
                leg_type = InstrumentType.PERPETUAL
            else:
                leg_name = f"{base}-{exp}"
                leg_type = InstrumentType.FUTURE
            legs.append(
                InstrumentLeg(
                    instrument_key=f"{venue}:{leg_type}:{leg_name}",
                    side=side,
                    ratio=ratio,
                )
            )
        return legs

    # --- Calendar/diagonal (dual expiry): BASE-CODE-EXP1_EXP2-STRIKES ---
    if is_dual_expiry:
        if not rest:
            return []
        expiry_part = rest[0]
        expiries = expiry_part.split("_")
        if len(expiries) < 2:
            return []
        exp_far, exp_near = expiries[0], expiries[1]
        strikes_part = rest[1] if len(rest) > 1 else ""
        strikes = strikes_part.split("_") if strikes_part else []

        legs = []
        half = len(structure) // 2
        for leg_idx, (opt_type, side, ratio) in enumerate(structure):
            # Alternate expiries: first half = far, second half = near
            exp = exp_far if leg_idx < half else exp_near
            strike = strikes[leg_idx % len(strikes)] if strikes else ""
            suffix = f"-{strike}-{opt_type}" if opt_type and strike else ""
            leg_name = f"{base}-{exp}{suffix}"
            legs.append(
                InstrumentLeg(
                    instrument_key=f"{venue}:OPTION:{leg_name}" if opt_type else f"{venue}:FUTURE:{leg_name}",
                    side=side,
                    ratio=ratio,
                )
            )
        return legs

    # --- Single-expiry option combos: BASE-CODE-EXPIRY-K1_K2[_K3[_K4]] ---
    if not rest:
        return []

    expiry = rest[0]
    strikes_part = rest[1] if len(rest) > 1 else ""
    strikes = strikes_part.split("_") if strikes_part else []

    # For STRD (straddle): single strike, two legs (C + P at same strike)
    legs = []
    for i, (opt_type, side, ratio) in enumerate(structure):
        # Map strike index: for structures with fewer strikes than legs,
        # reuse strikes (e.g. STRD has 1 strike, 2 legs).
        strike = strikes[min(i, len(strikes) - 1)] if strikes else ""
        suffix = f"-{strike}-{opt_type}" if opt_type and strike else ""
        leg_name = f"{base}-{expiry}{suffix}"
        legs.append(
            InstrumentLeg(
                instrument_key=f"{venue}:OPTION:{leg_name}" if opt_type else f"{venue}:FUTURE:{leg_name}",
                side=side,
                ratio=ratio,
            )
        )
    return legs


def _split_symbol(symbol: str) -> tuple[str, str]:
    """Split a concatenated symbol like BNBBTC into (BNB, BTC) using known quote currencies.

    Also handles underscore-separated symbols: BTC_USDC → (BTC, USDC).
    """
    # Underscore-separated: BTC_USDC, ETH_USDT, STETH_ETH
    if "_" in symbol:
        sub_parts = symbol.split("_", 1)
        if sub_parts[1] in _QUOTE_CURRENCIES_SET:
            return sub_parts[0], sub_parts[1]
    # Concatenated suffix match: BTCUSDT → (BTC, USDT)
    for quote in _QUOTE_CURRENCIES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""
