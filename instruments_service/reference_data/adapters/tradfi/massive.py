"""Massive reference data adapter — TradFi equities / ETFs / indices / FX / futures.

Massive (rebranded from Polygon.io 2025-10-30) serves the same REST API on
``https://api.polygon.io``. This is the alternate TradFi reference-data source to
Databento (Databento re-runs are billing-blocked as of 2026-06; Massive feeds
``instrument_availability/by_date/`` for the gap from 2026-05-05 onward).

THIN ADAPTER: all raw→canonical normalisation lives in UAC
(:mod:`unified_api_contracts.external.massive`) so the canonical
:class:`InstrumentRecord` schema is identical regardless of source. This adapter
only does HTTP + pagination + venue filtering + session enrichment, then calls
the UAC normalisers. Every fetched record is stamped ``source="massive"`` by the
orchestrator at ``record_captured`` time.

Endpoint paths (verified 2026-06-07 against the active Massive subscription —
Stocks Starter / Options Developer / Indices Basic / Futures Developer /
Currencies Basic):

* equities/ETF : ``GET /v3/reference/tickers?market=stocks``
* indices      : ``GET /v3/reference/tickers?market=indices``
* FX           : ``GET /v3/reference/tickers?market=fx``
* futures      : ``GET /futures/vX/contracts`` + ``GET /futures/vX/products``
  (NOT ``/v3/reference/futures/*`` — that path 404s)

Auth: ``Authorization: Bearer {api_key}``; the service fetches ``MASSIVE_API_KEY``
from Secret Manager and injects it via the ``api_key`` constructor parameter.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import cast

import aiohttp
from unified_api_contracts import (
    FX_SPOT_PAIRS,
    TRADFI_DATABENTO_INSTRUMENTS,
    TRADFI_TICKER_UNIVERSE,
    MassiveFuturesContractsResponse,
    MassiveFuturesProduct,
    MassiveFuturesProductsResponse,
    MassiveOptionContractsResponse,
    MassiveTickersResponse,
    classify_venue_error,
    normalize_massive_equity,
    normalize_massive_futures,
    normalize_massive_fx,
    normalize_massive_index,
    normalize_massive_option,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_api_contracts.registry import YAHOO_INDICES
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from .databento import EXCHANGE_HOURS, get_session_metadata

logger = logging.getLogger(__name__)

#: Massive source tag stamped on the manifest row (UAC SOURCE_PRIORITY value).
MASSIVE_SOURCE = "massive"

_MASSIVE_BASE = "https://api.polygon.io"
_PAGE_LIMIT = 1000
_MAX_PAGES = 30  # cap at 30k rows per surface — bounds runaway pagination

_EQUITY_VENUES = frozenset({"NASDAQ", "NYSE"})
_FUTURES_VENUES = frozenset({"CME", "ICE"})

#: Cash-index OPRA option underlyings to capture, tagged to their canonical
#: venue. SPX + VIX are the CBOE index-vol complex (Massive has no CME
#: futures-options, so these OPRA index options are the relevant options layer).
_INDEX_OPTION_UNDERLYINGS: dict[str, str] = {"SPX": "CBOE", "VIX": "CBOE"}


def _meta_str(meta: dict[str, str | bool | None], key: str) -> str | None:
    """Read a string-valued session-metadata field, narrowing the union to str|None."""
    value = meta.get(key)
    return value if isinstance(value, str) else None


def _classify_massive_error(exc: Exception) -> str:
    """Map a transport/HTTP exception to a venue-error code string for classification."""
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "401" in msg or "403" in msg or "unauthor" in msg or "forbidden" in msg:
        return "AUTH_ERROR"
    if "500" in msg or "502" in msg or "503" in msg or "504" in msg:
        return "SERVER_ERROR"
    if isinstance(exc, (OSError, aiohttp.ClientError)):
        return "NETWORK"
    return "MASSIVE_FETCH_FAILED"


def _curated_equity_symbols() -> set[str]:
    """Curated equity/ETF universe (same set Databento fetches)."""
    sp500 = TRADFI_TICKER_UNIVERSE.get("sp500_tickers", [])
    etfs = TRADFI_TICKER_UNIVERSE.get("etf_tickers", [])
    return {*sp500, *etfs}


def _futures_roots_for_venue(venue: str) -> list[str]:
    """Curated futures product-code roots for a venue (from the UAC registry)."""
    seen: set[str] = set()
    roots: list[str] = []
    for d in TRADFI_DATABENTO_INSTRUMENTS:
        if d.instrument_type == "FUTURE" and d.venue == venue and d.exchange_code and d.exchange_code not in seen:
            seen.add(d.exchange_code)
            roots.append(d.exchange_code)
    return roots


class MassiveReferenceDataAdapter(BaseReferenceDataAdapter):
    """Massive TradFi reference data adapter (equities/ETF/index/FX/futures).

    Mirrors :class:`DatabentoReferenceDataAdapter`'s public contract + venue
    semantics so it is a drop-in alternate source: same canonical
    ``InstrumentRecord`` output, same venue tagging, same session enrichment.
    """

    def __init__(
        self,
        project_id: str | None = None,
        target_date: date | None = None,
        api_key: str | None = None,
        venue_filter: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._target_date: date = target_date or datetime.now(UTC).date()
        self._venue_filter: str | None = venue_filter

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._venue_filter or "massive"

    def _require_api_key(self) -> str:
        key = self._optional_api_key()
        if key is None:
            raise ValueError(
                "api_key required — service must fetch MASSIVE_API_KEY from Secret Manager and pass it via api_key=."
            )
        return key

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch canonical InstrumentRecord[] for the configured venue from Massive."""
        api_key = self._require_api_key()
        headers = {"Authorization": f"Bearer {api_key}"}
        vf = self._venue_filter
        results: list[InstrumentRecord] = []

        async with self._make_session() as session:
            if vf in (None, "NASDAQ", "NYSE"):
                results.extend(await self._fetch_equities(session, headers, vf))
            if vf in (None, "CBOE"):
                results.extend(await self._fetch_indices(session, headers))
                results.extend(await self._fetch_index_options(session, headers))
            if vf in (None, "FX"):
                results.extend(await self._fetch_fx(session, headers))
            if vf in (None, "CME", "ICE"):
                results.extend(await self._fetch_futures(session, headers, vf))

        self._enrich_session_metadata(results)
        if instrument_type is not None:
            results = [r for r in results if r.instrument_type == instrument_type]
        logger.info("Massive [%s]: %d canonical instruments", vf or "ALL", len(results))
        return results

    def _enrich_session_metadata(self, results: list[InstrumentRecord]) -> None:
        """Apply venue session metadata (trading hours/holidays) — reuses the Databento path."""
        cache: dict[str, dict[str, str | bool | None]] = {}
        for record in results:
            v = record.venue
            if v not in cache:
                cache[v] = get_session_metadata(v, self._target_date) if v in EXCHANGE_HOURS else {}
            meta = cache[v]
            if meta:
                _is_trading = meta.get("is_trading_day")
                record.is_trading_day = _is_trading if isinstance(_is_trading, bool) else None
                record.regular_open_utc = _meta_str(meta, "regular_open_utc")
                record.regular_close_utc = _meta_str(meta, "regular_close_utc")
                record.early_close_utc = _meta_str(meta, "early_close_utc")
                record.holiday_calendar = _meta_str(meta, "holiday_calendar")
                venue_hours = EXCHANGE_HOURS.get(v)
                tz = venue_hours.get("tz") if venue_hours else None
                if isinstance(tz, str):
                    record.timezone = tz

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict[str, str],
        params: dict[str, str] | None,
        surface: str,
    ) -> dict[str, object] | None:
        """GET + parse one page. Classifies + logs failures, returns None (shard isolation — no raise)."""
        try:
            raw = await self._get_with_retry(session, url, params=params, headers=headers)
            return cast(dict[str, object], raw)
        except (aiohttp.ClientError, RuntimeError, OSError) as exc:
            error_code = _classify_massive_error(exc)
            classification = classify_venue_error("MASSIVE", error_code)
            action = classification.action.value if classification else "fail"
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": self._venue_filter or "ALL",
                    "source": MASSIVE_SOURCE,
                    "surface": surface,
                    "error_code": error_code,
                    "error_action": action,
                    "error": str(exc)[:200],
                },
            )
            logger.warning("Massive %s fetch failed [%s/%s]: %s", surface, error_code, action, exc)
            return None

    async def _fetch_equities(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        vf: str | None,
    ) -> list[InstrumentRecord]:
        """Paginate /v3/reference/tickers?market=stocks, filter to curated universe + venue."""
        curated = _curated_equity_symbols()
        out: list[InstrumentRecord] = []
        url: str | None = f"{_MASSIVE_BASE}/v3/reference/tickers"
        params: dict[str, str] | None = {"market": "stocks", "active": "true", "limit": str(_PAGE_LIMIT)}
        pages = 0
        while url and pages < _MAX_PAGES:
            raw = await self._get_json(session, url, headers, params, "equities")
            if raw is None:
                break
            resp = MassiveTickersResponse.model_validate(raw)
            for ticker in resp.results or []:
                if (ticker.ticker or "") not in curated:
                    continue
                rec = normalize_massive_equity(ticker)
                if rec is not None and (vf is None or rec.venue == vf):
                    out.append(rec)
            url = resp.next_url
            params = None
            pages += 1
        return out

    async def _fetch_indices(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ) -> list[InstrumentRecord]:
        """Fetch the curated index universe (VIX etc.) from market=indices, tagged CBOE."""
        out: list[InstrumentRecord] = []
        for idx in YAHOO_INDICES:
            if idx.venue != "CBOE":
                continue
            params = {"market": "indices", "ticker": f"I:{idx.symbol}", "limit": "1"}
            raw = await self._get_json(session, f"{_MASSIVE_BASE}/v3/reference/tickers", headers, params, "indices")
            if raw is None:
                continue
            resp = MassiveTickersResponse.model_validate(raw)
            for ticker in resp.results or []:
                rec = normalize_massive_index(ticker, venue="CBOE")
                if rec is not None:
                    out.append(rec)
        return out

    async def _fetch_index_options(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ) -> list[InstrumentRecord]:
        """Fetch OPRA cash-index option chains (SPX/VIX) tagged to their venue.

        Massive has no CME futures-options; these CBOE index options are the
        relevant options layer (SPX + VIX vol complex). Paginated per underlying.
        """
        out: list[InstrumentRecord] = []
        for underlying, venue in _INDEX_OPTION_UNDERLYINGS.items():
            url: str | None = f"{_MASSIVE_BASE}/v3/reference/options/contracts"
            params: dict[str, str] | None = {"underlying_ticker": underlying, "limit": str(_PAGE_LIMIT)}
            pages = 0
            while url and pages < _MAX_PAGES:
                raw = await self._get_json(session, url, headers, params, "index_options")
                if raw is None:
                    break
                resp = MassiveOptionContractsResponse.model_validate(raw)
                for contract in resp.results or []:
                    rec = normalize_massive_option(contract, venue=venue)
                    if rec is not None:
                        out.append(rec)
                url = resp.next_url
                params = None
                pages += 1
        return out

    async def _fetch_fx(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ) -> list[InstrumentRecord]:
        """Fetch the curated FX pairs from market=fx, tagged FX."""
        out: list[InstrumentRecord] = []
        for fx in FX_SPOT_PAIRS:
            params = {"market": "fx", "ticker": f"C:{fx.base}{fx.quote}", "limit": "1"}
            raw = await self._get_json(session, f"{_MASSIVE_BASE}/v3/reference/tickers", headers, params, "fx")
            if raw is None:
                continue
            resp = MassiveTickersResponse.model_validate(raw)
            for ticker in resp.results or []:
                rec = normalize_massive_fx(ticker)
                if rec is not None:
                    out.append(rec)
        return out

    async def _fetch_futures(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        vf: str | None,
    ) -> list[InstrumentRecord]:
        """Fetch curated futures roots from /futures/vX/, alive on the target date."""
        venues = [vf] if vf in _FUTURES_VENUES else sorted(_FUTURES_VENUES)
        out: list[InstrumentRecord] = []
        seen: set[str] = set()  # cross-root dedup by canonical instrument_key
        target_iso = self._target_date.isoformat()
        for venue in venues:
            for root in _futures_roots_for_venue(venue):
                product = await self._fetch_futures_product(session, headers, root)
                for rec in await self._fetch_futures_contracts(session, headers, root, product, target_iso):
                    if rec.instrument_key in seen:
                        continue
                    seen.add(rec.instrument_key)
                    out.append(rec)
        return out

    async def _fetch_futures_product(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        root: str,
    ) -> MassiveFuturesProduct | None:
        """Fetch product-level metadata (contract size / currency / sub-class) for a root."""
        params = {"product_code": root, "limit": "1"}
        raw = await self._get_json(session, f"{_MASSIVE_BASE}/futures/vX/products", headers, params, "futures_products")
        if raw is None:
            return None
        resp = MassiveFuturesProductsResponse.model_validate(raw)
        return (resp.results or [None])[0]

    async def _fetch_futures_contracts(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        root: str,
        product: MassiveFuturesProduct | None,
        target_iso: str,
    ) -> list[InstrumentRecord]:
        """Fetch the point-in-time contract snapshot for a root on the target date."""
        out: list[InstrumentRecord] = []
        seen: set[str] = set()
        url: str | None = f"{_MASSIVE_BASE}/futures/vX/contracts"
        # ``date=`` returns ONE row per contract ALIVE on the target date, NOT the
        # full per-trading-day history — without it the endpoint returns one row
        # per (contract, day) and bloats the snapshot ~230x (e.g. a single ES
        # contract appears once for every day of its life).
        params: dict[str, str] | None = {
            "product_code": root,
            "date": target_iso,
            "limit": str(_PAGE_LIMIT),
        }
        pages = 0
        while url and pages < _MAX_PAGES:
            raw = await self._get_json(session, url, headers, params, "futures_contracts")
            if raw is None:
                break
            resp = MassiveFuturesContractsResponse.model_validate(raw)
            for contract in resp.results or []:
                ticker = contract.ticker or ""
                # Skip calendar / inter-commodity spreads (combo legs, e.g.
                # ``ESH7-ESM7``) — not outright futures; dedup by ticker as a
                # safety net against any pagination overlap.
                if "-" in ticker or ticker in seen:
                    continue
                rec = normalize_massive_futures(contract, product)
                if rec is not None:
                    seen.add(ticker)
                    out.append(rec)
            url = resp.next_url
            params = None
            pages += 1
        return out

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by raw symbol."""
        for inst in await self.get_instruments():
            if inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Options chain — not served via this reference adapter (see plan follow-up)."""
        raise NotImplementedError("Massive options-chain reference fetch is a tracked follow-up (see tradfi plan).")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Expiry calendar derived from fetched futures instruments."""
        instruments = await self.get_instruments(instrument_type=InstrumentType.FUTURE)
        expiry_set: set[datetime] = set()
        for inst in instruments:
            und = inst.underlying or inst.base_asset or ""
            if underlying.upper() not in und.upper():
                continue
            if inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported (equities/futures only)."""
        raise NotImplementedError("Massive covers equities/futures reference — no funding rates.")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """OHLCV is MTDS's responsibility — not served by this reference adapter."""
        raise NotImplementedError("Massive OHLCV is fetched by MTDS, not the reference-data adapter.")
