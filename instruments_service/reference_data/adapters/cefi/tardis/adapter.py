"""Tardis REST adapter class + venue universe constants.

Cohesion module of the ``adapters.cefi.tardis`` package (split from the former
monolithic ``adapters/cefi/tardis.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators (``log_event``, ``classify_venue_error``, the parsing /
combo helpers and the Tardis pydantic detail models) resolve through
``_tardis`` — the live package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.cefi.tardis.<name>")`` targets and
cross-module monkeypatching behave exactly as they did before the split.
"""

# Package-internal access: the tardis package is ONE logical namespace split
# across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, cast

import aiohttp
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import InstrumentType, OptionType

from ....base_adapter import BaseReferenceDataAdapter
from ....schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

if TYPE_CHECKING:
    from unified_api_contracts import TardisInstrumentDetail
    from unified_api_contracts.internal import InstrumentLeg, InstrumentRecord, MarginType

    from instruments_service.reference_data.adapters.cefi import tardis as _tardis
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.cefi.tardis._pkg_ref import tardis_namespace as _tardis

__all__ = [
    "_DEFAULT_EXCHANGES",
    "_DERIVATIVES_ONLY_EXCHANGES",
    "_TARDIS_BASE",
    "_TARDIS_RETRYABLE_CODES",
    "_TARDIS_RETRY_ATTEMPTS",
    "_TARDIS_RETRY_BASE_DELAY",
    "_TYPE_MAP",
    "_VENUE_MAPPING",
    "TardisReferenceDataAdapter",
    "_classify_tardis_error",
]

_TARDIS_BASE = "https://api.tardis.dev/v1"

# Retry configuration for Tardis instrument listing requests
_TARDIS_RETRY_ATTEMPTS: int = 3
_TARDIS_RETRY_BASE_DELAY: float = 2.0  # seconds; doubles on each retry
_TARDIS_RETRYABLE_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

# Default exchange universe = the canonical SSOT `VenueMapping.all_tardis_exchanges`
# (NOT a local subset). The prior hand-maintained 8-exchange list silently DRIFTED
# below the SSOT: it omitted kraken / cryptofacilities (=KRAKEN-FUTURES) / bitfinex /
# bitget / lighter-zksync etc., so the IS reference universe under-covered venues MTDS
# actively captures via Tardis replay → the catalogue was NOT ⊇ the captured present-set
# → falsely-high coverage (slot-3 pre-apply audit 2026-06-08, CF-14: KRAKEN-SPOT 75,714 +
# KRAKEN-FUTURES 31,582 captured rows had no could-exist denominator). Deriving from the
# SSOT makes the IS reference universe track the canonical Tardis venue set automatically
# (no future drift). `cefi_manifest_canonicalisation_2026_06_01.md` § "PRE-APPLY AUDIT" ⑧.
_DEFAULT_EXCHANGES: list[str] = list(VenueMapping().all_tardis_exchanges)

# Tardis instrument type → canonical InstrumentType
_TYPE_MAP: dict[str, InstrumentType] = {
    "perpetual": InstrumentType.PERPETUAL,
    "future": InstrumentType.FUTURE,
    "option": InstrumentType.OPTION,
    "spot": InstrumentType.SPOT_PAIR,
    "combo": InstrumentType.COMBO,
}

# Exchanges that are derivatives-only (no spot trading).
# Instruments from these venues with unknown/None type must NOT default to
# SPOT_PAIR — that causes HTTP 400s when MTDS requests spot symbols from a
# venue that carries only futures/options/perps. Skip unknowns instead.
# Extended 2026-06-08 (slot-3 ⑧ fix): now that _DEFAULT_EXCHANGES tracks the full
# SSOT, classify the newly-included derivatives-only Tardis exchange ids (unambiguous
# -futures / -swap / -derivatives / -dm / cryptofacilities-=-Kraken-Futures suffixes)
# so their unknown-type instruments are skipped, not mis-defaulted to SPOT_PAIR.
# NOTE 2026-06-16 (operator correction): Deribit DOES list spot instruments
# (BTC_USDC / ETH_USDC / … spot launched ~2023) — it is REMOVED from this set so
# its SPOT_PAIR instruments enumerate and pass the normal CEFI_BASE_ASSET_UNIVERSE
# base-asset filter like any other venue. The Tardis `deribit` exchange id covers
# both the derivatives (perp/future/option, carrying explicit `type`) and the spot
# pairs, so unknown-type defaulting is not a concern here.
_DERIVATIVES_ONLY_EXCHANGES: frozenset[str] = frozenset(
    {
        "binance-futures",
        "okex-futures",
        "okex-swap",
        "huobi-dm",
        "bitfinex-derivatives",
        "bitget-futures",
        "cryptofacilities",  # Kraken Futures (Tardis legacy id) — derivatives only
    }
)

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


class TardisReferenceDataAdapter(BaseReferenceDataAdapter):
    """Tardis reference data adapter.

    Fetches the instrument universe from the FREE, NO-AUTH Tardis metadata
    endpoint ``GET /v1/exchanges/{exchange}`` (symbol list + per-instrument
    availability window). No API key is sent or consumed for enumeration —
    instruments-service does not need Tardis auth for reference data, and must
    not burn the academic-unlimited key's limits (operator 2026-06-23).

    Exchanges iterated by default: the full canonical ``VenueMapping`` Tardis
    set. Pass exchanges=[...] to the constructor to override.

    Not applicable: live funding rates, OHLCV bars (those hit the authenticated
    ``datasets.tardis.dev`` / ``/data-feeds`` tick-data path — MTDS's job, not
    IS reference data).
    """

    def __init__(
        self,
        project_id: str | None = None,
        exchanges: list[str] | None = None,
        api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._exchanges: list[str] = exchanges if exchanges is not None else _DEFAULT_EXCHANGES
        # Tardis returns the full historical universe in one REST call —
        # the result is date-independent. Bump cache TTL well past the longest
        # plausible single-process backfill window (default 1h is too short for
        # multi-year sweeps that take several hours). 24h cache keeps RSS flat
        # for any realistic single-run.
        self._cache_ttl = 86400.0

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "tardis"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch all instruments from the venue.

        Instrument ENUMERATION uses the FREE, NO-AUTH Tardis metadata endpoint
        ``GET /v1/exchanges/{exchange}`` (returns ``availableSymbols`` with
        id / type / availableSince / availableTo). No API key is sent or
        consumed — instruments-service does NOT need Tardis auth for the
        reference-data universe (the authenticated ``datasets.tardis.dev`` /
        ``/data-feeds`` tick-data path is MTDS's job, not IS). Operator decision
        2026-06-23: "IS doesn't need auth for tardis; don't waste API limits".
        """
        results: list[InstrumentRecord] = []
        failures: list[str] = []
        async with self._make_session() as session:
            for exchange in self._exchanges:
                try:
                    batch = await self._fetch_exchange_instruments(session, exchange)
                except RuntimeError as exc:
                    # CF-11: isolate a per-exchange failure (don't abort sibling
                    # exchanges) but TRACK it. A genuine fetch failure must not be
                    # silently dropped — if the venue ends up empty BECAUSE of
                    # failures, re-raise below so urdi_reference_provider._fetch_one
                    # routes tardis into failed[] (→ attempted_failed) instead of a
                    # clean-empty silent universe shrink (A8 false-complete).
                    _tardis.logger.error("Tardis exchange %r failed: %s", exchange, exc)
                    failures.append(exchange)
                    continue
                if instrument_type is not None:
                    batch = [r for r in batch if r.instrument_type == instrument_type]
                results.extend(batch)
        if not results and failures:
            raise RuntimeError(f"Tardis: all requested exchange(s) failed ({failures}); no instruments fetched")
        return results

    async def get_instruments_cached(
        self,
        instrument_type: str | None = None,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        """Tardis-specific cache: keyed on instrument_type only.

        Tardis returns the full historical instrument universe in a single
        REST call regardless of date — date filtering happens downstream in
        URDI/orchestrator. Including date in the cache key would force a fresh
        ~200K-instrument fetch per backfill date and accumulate ~40 GB of
        pydantic objects across a multi-year sweep (OOM-killed cefi-instr-deribit
        on 2026-05-04 at 25 dates in). Keying only on instrument_type means
        the second date onward hits cache, RSS plateaus after the first fetch.
        """
        cache_key = instrument_type
        cached = self._instruments_cache.get(cache_key)
        if cached is not None:
            records, ts = cached
            if (time.monotonic() - ts) < self._cache_ttl:
                return records
        records = await self.get_instruments(instrument_type=instrument_type)
        self._instruments_cache[cache_key] = (records, time.monotonic())
        return records

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
        """Return expiry calendar; not supported for this venue."""
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
        async with self._make_session() as session:
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
        _tardis.logger.debug("Tardis funding rate %s/%s: %s", last_exchange, symbol, rate_raw)
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
        async with self._make_session() as session:
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
            error_code = _tardis._classify_tardis_error(exc)
            classification = _tardis.classify_venue_error("tardis", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            _tardis.logger.warning(
                "Tardis funding fetch failed for %s: %s (classified: %s, action: %s)",
                exchange,
                exc,
                error_code,
                action,
            )
            _tardis.log_event(
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
            msg: dict[str, object] = cast("dict[str, object]", json.loads(line))
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
            error_code = _tardis._classify_tardis_error(exc)
            classification = _tardis.classify_venue_error("tardis", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            _tardis.logger.warning(
                "Tardis OHLCV fetch failed for %s: %s (classified: %s, action: %s)",
                exchange,
                exc,
                error_code,
                action,
            )
            _tardis.log_event(
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
            msg: dict[str, object] = cast("dict[str, object]", json.loads(line))
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
        exchange: str,
    ) -> list[InstrumentRecord]:
        # Instrument enumeration uses the FREE, NO-AUTH metadata endpoint
        # /v1/exchanges/{exchange} ONLY — returns availableSymbols (id / type /
        # availableSince / availableTo). NO Authorization header, NO API key
        # consumed. The authenticated /v1/instruments/{exchange} (pro-tier rich
        # tick-size metadata) is deliberately NOT called: instruments-service
        # only needs the symbol universe + lifecycle window, and the operator
        # mandate (2026-06-23) is that IS must not burn Tardis API limits on
        # the academic-unlimited key. Tick sizes use exchange-specific defaults
        # in _parse_tardis_instrument (priceIncrement is None from this endpoint).
        instruments_list: list[TardisInstrumentDetail] | None = None
        last_exc: Exception | None = None

        url = f"{_TARDIS_BASE}/exchanges/{exchange}"
        for attempt in range(_TARDIS_RETRY_ATTEMPTS):
            delay = _TARDIS_RETRY_BASE_DELAY * (1 << attempt)
            try:
                # No headers: the metadata endpoint is public (no Bearer token).
                async with session.get(url) as resp:
                    if resp.status == 404:
                        _tardis.logger.warning("Tardis exchange %r not found — skipping", exchange)
                        return []
                    if resp.status in _TARDIS_RETRYABLE_CODES:
                        if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                            await asyncio.sleep(delay)
                            continue
                        resp.raise_for_status()
                    resp.raise_for_status()
                    raw_json: object = cast("object", await resp.json())
                    exchange_detail = _tardis.TardisExchangeDetail.model_validate(raw_json)
                    instruments_list = exchange_detail.instruments
                    break
            except aiohttp.ClientError as exc:
                last_exc = exc
                if attempt < _TARDIS_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(delay)
                    continue
                error_code = _tardis._classify_tardis_error(exc)
                classification = _tardis.classify_venue_error("tardis", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                _tardis.logger.error(
                    "Tardis request failed for exchange %r after %d attempts: %s (classified: %s, action: %s, retry=%s)",
                    exchange,
                    _TARDIS_RETRY_ATTEMPTS,
                    exc,
                    error_code,
                    action,
                    retry_safe,
                )
                _tardis.log_event(
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
                # CF-11: re-raise so get_instruments tracks this exchange as
                # failed (→ venue failed[] if it leaves the universe empty),
                # not a silent clean-empty. Mirrors the tradfi databento fix.
                raise RuntimeError(
                    f"Tardis {exchange!r}: all {_TARDIS_RETRY_ATTEMPTS} attempts failed (error_code={error_code}): {exc}"
                ) from exc

        if instruments_list is None:
            _tardis.logger.error(
                "Tardis %r: no response after %d attempts (last error: %s)",
                exchange,
                _TARDIS_RETRY_ATTEMPTS,
                last_exc,
            )
            # CF-11: no parseable response is a genuine fetch failure —
            # raise (not return []) so the venue threads state.
            raise RuntimeError(
                f"Tardis {exchange!r}: no instruments after {_TARDIS_RETRY_ATTEMPTS} attempts (last={last_exc})"
            )

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
        _tardis.logger.info(
            "Tardis %s: API /exchanges (no-auth) returned %d symbols, parsed %d instruments, filtered %d",
            exchange,
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
        tardis_type = item.type
        if tardis_type is None:
            if exchange in _DERIVATIVES_ONLY_EXCHANGES:
                # Derivatives-only venue: skip instruments with unknown type rather
                # than defaulting to SPOT_PAIR (which causes HTTP 400 from MTDS when
                # it requests spot symbols from a venue carrying only derivatives).
                # NOTE: Deribit is NOT in this set — it lists spot (BTC_USDC/…) since
                # ~2023, so its spot pairs enumerate normally.
                return None
            tardis_type = "spot"
        instrument_type: InstrumentType = _TYPE_MAP.get(tardis_type, InstrumentType.SPOT_PAIR)
        if instrument_type == InstrumentType.SPOT_PAIR and exchange in _DERIVATIVES_ONLY_EXCHANGES:
            # Guard: recognised-as-spot on a derivatives-only venue is also invalid.
            return None

        # Resolve canonical venue name from UAC VenueMapping (e.g. "binance" → "BINANCE-SPOT")
        canonical_venue = _VENUE_MAPPING.tardis_to_venue.get(exchange, exchange.upper())

        # Parse base/quote — prefer Tardis metadata, fall back to symbol splitting
        base, quote = _tardis._resolve_base_quote(item, raw_id, exchange)

        # Filter: quote (if present) must be accepted (venue-aware — KRW for UPBIT).
        if not _tardis._passes_asset_filter(base, quote, instrument_type, canonical_venue):
            return None

        # Per-instrument SKIP (never raise) when the quote can't be resolved for a
        # pair-identity instrument: InstrumentRecord REQUIRES a non-empty quote_asset
        # for SPOT_PAIR/FUTURE/PERPETUAL (hard_schema_enforcement Phase 1). A single
        # unparseable symbol must NOT raise inside the per-venue parse loop (CF-11
        # re-raises → the WHOLE venue is dropped to 0 rows — the full-universe
        # enumeration exposed binance-futures dated quarterlies / odd suffixes here).
        # Shard-level isolation: skip the one symbol, keep the venue's universe.
        if not quote and instrument_type in (
            InstrumentType.SPOT_PAIR,
            InstrumentType.FUTURE,
            InstrumentType.PERPETUAL,
        ):
            _tardis.logger.debug(
                "Tardis %s: skipping %r — unresolved quote for %s (pair-identity required)",
                exchange,
                raw_id,
                instrument_type,
            )
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
        available_since_dt = _tardis._parse_expiry(available_since) if available_since else None
        available_to_dt = _tardis._parse_expiry(available_to) if available_to else None
        expiry = _tardis._parse_expiry(item.expiry)
        if expiry is None and not is_active and available_to:
            expiry = _tardis._parse_expiry(available_to)
        # Deribit symbol format: BTC-27MAR26-190000-C → parse expiry from 2nd segment
        if expiry is None and "-" in raw_id:
            expiry = _tardis._parse_deribit_symbol_expiry(raw_id)
        # OKX symbol format: BTC-USD-260626 → parse YYMMDD from last segment
        if expiry is None and "-" in raw_id:
            expiry = _tardis._parse_yymmdd_symbol_expiry(raw_id)
        # Kraken Futures symbol format: FI_XBTUSD_240329 → parse YYMMDD from last underscore segment
        if expiry is None and "_" in raw_id:
            expiry = _tardis._parse_underscore_yymmdd_symbol_expiry(raw_id)

        strike, opt_type = _tardis._resolve_option_fields(item, instrument_type, raw_id)

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
            legs = _tardis._parse_deribit_combo_legs(raw_id, canonical_venue)
            if not legs:
                return None

        margin_type: MarginType | None = _tardis._infer_margin_type(instrument_type, quote, raw_id, exchange)

        return _tardis.InstrumentRecord(
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
            margin_type=margin_type,
            available_from_datetime=available_since_dt,
            available_to_datetime=available_to_dt,
            timezone="UTC",
        )
