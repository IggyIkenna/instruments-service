"""Kalshi reference data adapter — prediction market instrument listing.

Kalshi's REST API provides market metadata for prediction markets.
Each active market is returned as an InstrumentRecord so services can treat
prediction markets as tradeable instruments.

Base URL: https://api.elections.kalshi.com/trade-api/v2
Auth: API key passed as header (RSA key signing for production).
"""

import base64
import json
import logging
import time
from datetime import UTC, datetime
from datetime import date as date_cls
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

import aiohttp
from unified_api_contracts import (
    KalshiMarket,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_api_contracts.predictions import (
    CANONICAL_GROUP_METADATA,
    CanonicalQuestionGroup,
    MarketLifecycle,
    classify_kalshi_to_canonical_group,
)
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
_KALSHI_API_PREFIX = "/trade-api/v2"  # path prefix included in the RSA-PSS signed message
_PAGE_LIMIT = 200
_MAX_PAGES = 10  # cap at 2000 markets per fetch (live snapshot)
# Historical (pre-cutoff) enumeration paginates `/historical/markets` newest-first
# from the cutoff backward; cap pages so a deep target date (whose markets are far
# down the cursor) degrades to honest-absence rather than an unbounded walk — deep
# history is seeded from the bulk corpus, NOT this per-date API path. SSOT:
# plans/active/prediction_venue_perps_and_live_clob_depth_2026_06_20.md (PM-2 entry).
_MAX_HISTORICAL_PAGES = 40
# Only attempt live↔historical gap-edge pagination within this many days of the
# cutoff; deeper dates are honest-absence (served by the bulk corpus seed).
_HISTORICAL_GAP_EDGE_DAYS = 3
# Series-scoped capture: fetches `/series?category=X` then `/markets?status=open&series_ticker=Y`
# for each non-OTHER series — sidesteps the KXMVE multivariate flood that consumes all
# 2000 cap slots in the plain `/markets?status=open` snapshot. LIVE path only.
_SERIES_CATEGORIES: tuple[str, ...] = ("Crypto", "Economics", "Financials")
_MAX_SERIES_PAGES = 5  # per-series page budget (≤1000 markets per series)
_MAX_SERIES_TOTAL = 200  # ceiling on total series fetched across all categories


_STATUS_MAP: dict[int, str] = {429: "429", 401: "401", 403: "403", 400: "400"}
_MSG_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("429", "rate"), "429"),
    (("401", "unauthorized"), "401"),
    (("403", "forbidden"), "403"),
    (("400", "bad request"), "400"),
    (("500", "internal", "server"), "500"),
)


def _parse_iso_date(value: str | None) -> date_cls | None:
    """Parse a ``YYYY-MM-DD`` backfill date string to a ``date`` (None if blank/bad)."""
    if not value:
        return None
    try:
        return date_cls.fromisoformat(value[:10])
    except (ValueError, TypeError):
        return None


def _market_date(value: object) -> date_cls | None:
    """Parse a Kalshi market ISO timestamp (e.g. ``2026-04-20T23:00:00Z``) to a date."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return None


def _classify_kalshi_error(exc: Exception, status: int | None = None) -> str:
    """Map a Kalshi HTTP/network error to a UAC error code for classification."""
    if status is not None:
        if status in _STATUS_MAP:
            return _STATUS_MAP[status]
        if status >= 500:
            return "500"
    msg = str(exc).lower()
    for keywords, code in _MSG_PATTERNS:
        if any(kw in msg for kw in keywords):
            return code
    return "UNKNOWN"


class KalshiReferenceDataAdapter(BaseReferenceDataAdapter):
    """Kalshi reference data adapter.

    Returns active prediction markets as InstrumentRecord instances.
    Each market becomes one InstrumentRecord:
      instrument_key  = market ticker (e.g. "KXBTC-21MAR26-T95000")
      venue           = "kalshi"
      instrument_type = "prediction_market"
      raw_symbol      = event_ticker
      base_asset      = series_ticker
      quote_asset     = "USD"
      is_active       = status == "active"

    Auth: service must fetch kalshi-api-key from Secret Manager and pass
    it via ``api_key`` constructor parameter.
    """

    def __init__(
        self,
        api_key: str | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        # Captured during get_instruments() — available via
        # get_market_lifecycles() so the orchestrator can emit
        # MARKET_LIFECYCLE rows alongside the InstrumentRecord shard
        # without re-fetching from the Kalshi API.
        self._last_markets: list[KalshiMarket] = []
        # RSA-PSS credentials parsed from the `kalshi-api-credentials` JSON blob
        # ({"api_key_id"|"key_id", "private_key"}). Kalshi authenticates with
        # RSA-PSS request signing — a plain `Authorization: Bearer` is rejected.
        # The LIVE `/markets?status=open` snapshot is reachable UNAUTHENTICATED;
        # the `/historical/*` tier (pre-cutoff markets/trades) REQUIRES signing.
        self._kalshi_key_id, self._kalshi_private_key_pem = self._parse_kalshi_creds(api_key)
        self._loaded_private_key: RSAPrivateKey | None = None  # lazily loaded crypto key
        self._historical_cutoff: date_cls | None = None  # lazily resolved

    @property
    def venue(self) -> str:
        """Return the canonical venue token (UPPERCASE).

        Must be ``KALSHI`` (uppercase) — the canonical prediction venue per UAC
        ``partition_paths`` ("POLYMARKET / KALSHI"), mirroring the Polymarket
        adapter's ``"POLYMARKET"``. This governs the instrument-parquet partition
        ``venue=KALSHI`` AND must match what the MTDS live runner looks up
        (``venue={venue}.upper()/instruments.parquet``). Returning the lowercase
        SOURCE name ``"kalshi"`` (fixed 2026-06-22) wrote the universe to
        ``venue=kalshi`` while the live reader searched ``venue=KALSHI`` → the
        Kalshi universe was silently never found (venue ≠ source: venue=KALSHI,
        source=kalshi are distinct axes).
        """
        return "KALSHI"

    @staticmethod
    def _parse_kalshi_creds(api_key: str | None) -> tuple[str | None, str | None]:
        """Extract (api_key_id, private_key_pem) from the injected credential blob.

        The service injects the `kalshi-api-credentials` Secret Manager JSON via the
        ``api_key`` constructor param. Returns (None, None) for a missing/legacy
        non-JSON credential — the adapter then runs unauthenticated (live-only).
        """
        if not api_key:
            return None, None
        try:
            blob = cast(dict[str, object], json.loads(api_key))
        except (json.JSONDecodeError, TypeError):
            return None, None
        key_id = blob.get("api_key_id") or blob.get("key_id")
        priv = blob.get("private_key")
        return (
            str(key_id) if isinstance(key_id, str) else None,
            str(priv) if isinstance(priv, str) else None,
        )

    @property
    def _can_sign(self) -> bool:
        return bool(self._kalshi_key_id and self._kalshi_private_key_pem)

    def _signed_headers(self, method: str, path: str) -> dict[str, str]:
        """Build request headers, RSA-PSS-signing when credentials are present.

        ``path`` is the API path the signature covers, e.g.
        ``/trade-api/v2/historical/markets`` (no query string). Falls back to plain
        headers (unauthenticated) when no RSA credentials were injected — sufficient
        for the live ``/markets?status=open`` snapshot.
        """
        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if not self._can_sign:
            return headers
        # Lazy import: cryptography is heavy + only needed for the signed
        # `/historical/*` tier (live `status=open` runs without it).
        if self._loaded_private_key is None:
            from cryptography.hazmat.primitives import serialization

            self._loaded_private_key = cast(
                "RSAPrivateKey",
                serialization.load_pem_private_key(cast(str, self._kalshi_private_key_pem).encode(), password=None),
            )
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        ts = str(int(time.time() * 1000))
        signature: bytes = self._loaded_private_key.sign(
            (ts + method + path).encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        headers["KALSHI-ACCESS-KEY"] = cast(str, self._kalshi_key_id)
        headers["KALSHI-ACCESS-SIGNATURE"] = base64.b64encode(signature).decode()
        headers["KALSHI-ACCESS-TIMESTAMP"] = ts
        return headers

    async def _resolve_cutoff(self, session: aiohttp.ClientSession) -> date_cls:
        """Resolve the live↔historical boundary via ``/historical/cutoff`` (cached).

        Markets settled on/after the cutoff live on ``/markets`` (the rolling live
        snapshot); older markets are served by ``/historical/*``. On any failure we
        return ``date.min`` so EVERY date routes LIVE (safe default — live works
        unauthenticated; historical is opt-in only when the cutoff is known).
        """
        if self._historical_cutoff is not None:
            return self._historical_cutoff
        sign_path = f"{_KALSHI_API_PREFIX}/historical/cutoff"
        try:
            async with session.get(
                f"{_KALSHI_BASE_URL}/historical/cutoff", headers=self._signed_headers("GET", sign_path)
            ) as resp:
                resp.raise_for_status()
                body = cast(dict[str, object], await resp.json())
            raw = body.get("market_settled_ts")
            self._historical_cutoff = (
                datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date() if raw else date_cls.min
            )
        except (aiohttp.ClientError, ValueError, TypeError):
            self._historical_cutoff = date_cls.min
        return self._historical_cutoff

    async def get_instruments(
        self,
        instrument_type: str | None = None,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch Kalshi markets as InstrumentRecord list.

        ``date`` (``YYYY-MM-DD``) selects the enumeration mode — this is what makes
        batch (historical) and live pipeline modes share one canonical path:
          * ``None`` / on-or-after the live cutoff → LIVE snapshot
            (``/markets?status=open``, unauthenticated-OK) = currently-tradeable
            markets. Used by the daily/live cron + forward batch.
          * before the live cutoff → HISTORICAL tier (``/historical/markets``,
            RSA-PSS signed), cursor-paginated newest-first from the cutoff and
            filtered to markets whose lifecycle spans ``date``. Bounded by
            ``_MAX_HISTORICAL_PAGES``; deep dates beyond the cap return empty
            (honest-absence) — deep history is seeded from the bulk corpus.

        instrument_type filter: pass "PREDICTION_MARKET" or None (all). Other values
        return an empty list since Kalshi only exposes prediction markets.
        """
        if instrument_type is not None and instrument_type != "PREDICTION_MARKET":
            return []
        self._last_markets = []
        target = _parse_iso_date(date)
        async with self._make_session() as session:
            # LIVE (date None) needs no cutoff lookup — it is always the current
            # snapshot. Only a dated request resolves the cutoff to decide routing.
            historical = False
            if target is not None:
                cutoff = await self._resolve_cutoff(session)
                historical = target < cutoff
                if historical and (cutoff - target).days > _HISTORICAL_GAP_EDGE_DAYS:
                    # Deep pre-cutoff date: flat `/historical/markets` pagination
                    # cannot reach it (Kalshi settles ~12k markets/day; the page
                    # budget covers ~1 day from the cutoff). Honest-absence — this
                    # date's universe comes from the BULK corpus seed (+ the
                    # series-scoped enumerator), not this API path.
                    logger.info(
                        "KalshiAdapter: %s is >%dd before cutoff %s — honest-absence "
                        "(deep history served by bulk seed, not the live/historical API path)",
                        target,
                        _HISTORICAL_GAP_EDGE_DAYS,
                        cutoff,
                    )
                    return []
            max_pages = _MAX_HISTORICAL_PAGES if historical else _MAX_PAGES
            now = datetime.now(UTC)
            results: list[InstrumentRecord] = []
            cursor: str | None = None
            fetch_failed = False
            for _page in range(max_pages):
                try:
                    batch, cursor = await self._fetch_markets_page(
                        session, cursor, now, target=(target if historical else None)
                    )
                except RuntimeError:
                    # Page fetch failed — ADAPTER_FETCH_FAILED already emitted in
                    # _fetch_markets_page. Per shard-isolation, keep pages already
                    # fetched; the all-failed case re-raises below so the venue is
                    # recorded attempted_failed (NOT a silent honest-empty / CF-11).
                    fetch_failed = True
                    break
                results.extend(batch)
                if cursor is None or (not historical and len(batch) < _PAGE_LIMIT):
                    break
        if fetch_failed and not results:
            # All pages failed with zero records → raise so
            # urdi_reference_provider._fetch_one records this venue as
            # attempted_failed instead of dropping it as fetched-OK-empty.
            raise RuntimeError(
                "Kalshi get_instruments: market fetch failed with no records "
                "(see ADAPTER_FETCH_FAILED) — recording attempted_failed, not empty"
            )
        # Series-scoped capture (LIVE path only): supplement the snapshot with
        # markets from cross-venue-relevant series (Crypto/Economics/Financials)
        # whose tickers classify as non-OTHER canonical groups.  The plain
        # /markets?status=open snapshot is dominated by KXMVE* multivariate parlay
        # markets which consume all 2000 cap slots → KXBTCD/KXETHD/KXCPI/etc. are
        # never reached.  Fetching per-series sidesteps the flood.
        if target is None:
            async with self._make_session() as session:
                series_records = await self._fetch_series_scoped_batch(session)
            if series_records:
                existing_tickers = {r.instrument_key for r in results}
                for rec in series_records:
                    if rec.instrument_key not in existing_tickers:
                        results.append(rec)
                        existing_tickers.add(rec.instrument_key)
        return results

    async def _fetch_markets_page(
        self,
        session: aiohttp.ClientSession,
        cursor: str | None,
        now: datetime,
        *,
        target: date_cls | None = None,
    ) -> tuple[list[InstrumentRecord], str | None]:
        """Fetch one page of markets from Kalshi API.

        ``target`` None → LIVE ``/markets?status=open`` snapshot. ``target`` set →
        HISTORICAL ``/historical/markets`` (signed), filtered to markets whose
        lifecycle spans ``target``; ``next_cursor`` is forced to None once a page
        holds no market still open on/after ``target`` (newest-first → we've walked
        past the target going backward).

        Returns (records, next_cursor). next_cursor is None when no more pages.
        """
        base_path = "/historical/markets" if target is not None else "/markets"
        url = f"{_KALSHI_BASE_URL}{base_path}"
        sign_path = f"{_KALSHI_API_PREFIX}{base_path}"
        # Kalshi's ``status`` query param is a LIFECYCLE filter whose valid
        # values are ``unopened`` / ``open`` / ``closed`` / ``settled`` —
        # ``status=active`` is rejected with HTTP 400. For the live snapshot the
        # request filter for currently-tradeable markets is ``status=open`` (each
        # carries market-level ``status=active``, which ``_parse_market`` checks).
        # The ``/historical/markets`` tier is unfiltered (returns settled markets
        # newest-first from the cutoff); we date-filter client-side below.
        params: dict[str, str] = {"limit": str(_PAGE_LIMIT)}
        if target is None:
            params["status"] = "open"
        if cursor is not None:
            params["cursor"] = cursor
        headers = self._signed_headers("GET", sign_path)
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    logger.error("Kalshi authentication failed (HTTP 401)")
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "kalshi",
                            "endpoint": "markets",
                            "error": "HTTP 401 Unauthorized",
                            "error_code": "401",
                            "action": "fail",
                            "retry_safe": False,
                        },
                    )
                    raise RuntimeError("Kalshi markets fetch failed: HTTP 401 Unauthorized")
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_kalshi_error(exc)
            classification = classify_venue_error("kalshi", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Kalshi markets request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "kalshi",
                    "endpoint": "markets",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # CF-11: surface the fetch failure (don't swallow into []) so
            # get_instruments re-raises on all-failed → venue records
            # attempted_failed, not a silent honest-empty. RuntimeError is in
            # urdi_reference_provider._fetch_one's catchable set (a bare
            # aiohttp.ClientError would escape it and crash the gather).
            raise RuntimeError(f"Kalshi markets fetch failed: {exc}") from exc

        raw_dict = cast(dict[str, object], raw_json)
        markets_raw = raw_dict.get("markets")
        next_cursor_raw = raw_dict.get("cursor")
        next_cursor = str(next_cursor_raw) if next_cursor_raw else None

        if not isinstance(markets_raw, list):
            return [], None

        records: list[InstrumentRecord] = []
        page_has_market_active_through_target = False
        for raw_item in markets_raw:
            market = cast(dict[str, object], raw_item)
            if target is not None:
                open_d = _market_date(market.get("open_time"))
                close_d = _market_date(market.get("close_time"))
                if close_d is not None and close_d >= target:
                    page_has_market_active_through_target = True
                # Keep only markets whose lifecycle [open, close] spans the target
                # date — i.e. the markets actually tradeable on that historical day.
                if not (open_d is not None and close_d is not None and open_d <= target <= close_d):
                    continue
            record = self._parse_market(market, now)
            if record is not None:
                records.append(record)
        if target is not None and not page_has_market_active_through_target:
            # Newest-first historical pagination has walked entirely PAST the target
            # (every market on this page already closed before it) → stop.
            next_cursor = None
        return records, next_cursor

    async def _fetch_series_for_category(
        self,
        session: aiohttp.ClientSession,
        category: str,
    ) -> list[str]:
        """Fetch series tickers for a Kalshi category via ``/series?category=X``.

        NO trailing slash (Kalshi 301-redirects it). Returns empty list on
        failure — per-category failure is shard-isolated.
        """
        url = f"{_KALSHI_BASE_URL}/series"
        sign_path = f"{_KALSHI_API_PREFIX}/series"
        params: dict[str, str] = {"category": category}
        headers = self._signed_headers("GET", sign_path)
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_kalshi_error(exc)
            classification = classify_venue_error("kalshi", error_code)
            action = classification.action.value if classification else "fail"
            logger.warning(
                "Kalshi series fetch failed for category=%s: %s (action: %s)",
                category,
                exc,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "kalshi",
                    "endpoint": "series",
                    "category": category,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": classification.retry_safe if classification else False,
                },
            )
            return []
        raw_dict = cast(dict[str, object], raw_json)
        series_list = raw_dict.get("series")
        if not isinstance(series_list, list):
            return []
        tickers: list[str] = []
        for item in series_list:
            if isinstance(item, dict):
                ticker_val = item.get("ticker")
                if isinstance(ticker_val, str) and ticker_val:
                    tickers.append(ticker_val)
        return tickers

    async def _fetch_series_scoped_markets(
        self,
        session: aiohttp.ClientSession,
        series_ticker: str,
        now: datetime,
    ) -> list[InstrumentRecord]:
        """Fetch ``/markets?status=open&series_ticker=<ticker>`` up to ``_MAX_SERIES_PAGES`` pages.

        Reuses ``_parse_market()`` — no logic duplication. Failures are
        caught + logged (shard-isolated); caller skips failing series.
        """
        url = f"{_KALSHI_BASE_URL}/markets"
        sign_path = f"{_KALSHI_API_PREFIX}/markets"
        records: list[InstrumentRecord] = []
        cursor: str | None = None
        for _page in range(_MAX_SERIES_PAGES):
            params: dict[str, str] = {
                "limit": str(_PAGE_LIMIT),
                "status": "open",
                "series_ticker": series_ticker,
            }
            if cursor is not None:
                params["cursor"] = cursor
            headers = self._signed_headers("GET", sign_path)
            try:
                async with session.get(url, params=params, headers=headers) as resp:
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_kalshi_error(exc)
                classification = classify_venue_error("kalshi", error_code)
                action = classification.action.value if classification else "fail"
                logger.warning(
                    "Kalshi series-scoped market fetch failed for series=%s page=%d: %s (action: %s)",
                    series_ticker,
                    _page,
                    exc,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "kalshi",
                        "endpoint": "markets",
                        "series_ticker": series_ticker,
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": classification.retry_safe if classification else False,
                    },
                )
                break
            raw_dict = cast(dict[str, object], raw_json)
            markets_raw = raw_dict.get("markets")
            next_cursor_raw = raw_dict.get("cursor")
            cursor = str(next_cursor_raw) if next_cursor_raw else None
            if not isinstance(markets_raw, list):
                break
            for raw_item in markets_raw:
                record = self._parse_market(cast(dict[str, object], raw_item), now)
                if record is not None:
                    records.append(record)
            if cursor is None or len(records) < _PAGE_LIMIT:
                break
        return records

    async def _fetch_series_scoped_batch(
        self,
        session: aiohttp.ClientSession,
    ) -> list[InstrumentRecord]:
        """Fetch markets for non-OTHER series across ``_SERIES_CATEGORIES``.

        For each category: fetch ``/series?category=X``, classify each
        series ticker, keep non-OTHER groups, then fetch per-series markets.
        Returns deduplicated InstrumentRecord list (by instrument_key).
        """
        now = datetime.now(UTC)
        all_records: list[InstrumentRecord] = []
        seen_tickers: set[str] = set()
        seen_series: set[str] = set()
        series_count = 0
        all_categories_failed = True

        for category in _SERIES_CATEGORIES:
            tickers = await self._fetch_series_for_category(session, category)
            if tickers:
                all_categories_failed = False
            for series_ticker in tickers:
                if series_ticker in seen_series:
                    continue
                seen_series.add(series_ticker)
                if series_count >= _MAX_SERIES_TOTAL:
                    logger.info(
                        "KalshiAdapter series-scoped: reached _MAX_SERIES_TOTAL=%d; stopping",
                        _MAX_SERIES_TOTAL,
                    )
                    break
                group = classify_kalshi_to_canonical_group(ticker=series_ticker)
                if group is None or group == CanonicalQuestionGroup.OTHER:
                    continue
                series_count += 1
                records = await self._fetch_series_scoped_markets(session, series_ticker, now)
                for rec in records:
                    if rec.instrument_key not in seen_tickers:
                        all_records.append(rec)
                        seen_tickers.add(rec.instrument_key)
            else:
                # Inner loop completed without break → proceed to next category.
                continue
            # Inner loop broke (hit _MAX_SERIES_TOTAL) → stop outer loop too.
            break

        logger.info(
            "KalshiAdapter series-scoped: %d series across categories %s → %d records",
            series_count,
            _SERIES_CATEGORIES,
            len(all_records),
        )
        if all_categories_failed and not all_records:
            logger.warning(
                "KalshiAdapter series-scoped: all category fetches failed — falling back to snapshot-only results"
            )
        return all_records

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch single market by ticker."""
        url = f"{_KALSHI_BASE_URL}/markets/{symbol}"
        headers = self._signed_headers("GET", f"{_KALSHI_API_PREFIX}/markets/{symbol}")
        now = datetime.now(UTC)
        async with self._make_session() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_kalshi_error(exc)
                classification = classify_venue_error("kalshi", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Kalshi get_instrument failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "kalshi",
                        "endpoint": f"markets/{symbol}",
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                return None

        raw_dict = cast(dict[str, object], raw_json)
        market_raw = raw_dict.get("market")
        if not isinstance(market_raw, dict):
            return None
        return self._parse_market(cast(dict[str, object], market_raw), now)

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError(
            "Kalshi does not provide options chains. Use get_instruments() to list available prediction markets."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError(
            "Kalshi does not provide expiry calendars. Market close times are in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Kalshi does not have perpetual funding rates.")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Kalshi OHLCV is not available via reference data API.")

    def _parse_market(
        self,
        raw: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a Kalshi market dict to an InstrumentRecord."""
        market = KalshiMarket.model_validate(raw)
        ticker = market.ticker
        if not ticker:
            return None
        # Cache for get_market_lifecycles() — populated even when the
        # InstrumentRecord doesn't ship (predictions without resolvable
        # close_time still have lifecycle metadata downstream).
        self._last_markets.append(market)
        event_ticker = market.event_ticker or ticker
        series_ticker = getattr(market, "series_ticker", None) or event_ticker
        title = getattr(market, "title", None) or getattr(market, "subtitle", None) or ticker
        base_asset = str(series_ticker)[:50] if series_ticker else ticker[:50]
        expiry = self._parse_close_time(market.close_time)
        status_raw = getattr(market, "status", None)
        is_active = str(status_raw).lower() == "active" if status_raw else True
        tick_raw = getattr(market, "tick_size", None)
        tick_size = Decimal(str(tick_raw)) if tick_raw else Decimal("0.01")
        min_order_raw = getattr(market, "min_order_size", None)
        min_order = Decimal(str(min_order_raw)) if min_order_raw else Decimal("1")
        # Per CLAUDE.md "Prediction market lifecycle timing": carry
        # market_created_at + settlement_time on the InstrumentRecord so
        # MTDS CLOB capture + features-* compute can gate ticks. Full
        # lifecycle row (with canonical_question_group + current_status)
        # rides the MARKET_LIFECYCLE data_type alongside.
        lifecycle = self.classify_lifecycle(market)
        # Universe-membership floor (fixes silent-empty KALSHI universe, 2026-06-22):
        # Kalshi's live `/markets?status=open` snapshot stamps `open_time` as an
        # intraday timestamp on the CURRENT day (e.g. 2026-06-22T13:21Z). The IS
        # date filter (`filter_instruments_by_date`) compares against the
        # enumeration day's MIDNIGHT (`date_dt = fromisoformat("2026-06-22")` →
        # 00:00Z), so a market opening at 13:21 today fails `available_from <= date_dt`
        # and EVERY open Kalshi market is dropped on every day → zero universe written.
        # A market opening any time on day D belongs to day D's universe, so floor
        # `available_from_datetime` to the open DATE (midnight). The PRECISE
        # market_created_at is still carried on the MarketLifecycle for MTDS
        # tick-gating; this floor only governs day-grain universe membership.
        _created = lifecycle.market_created_at if lifecycle else None
        _afd = _created.replace(hour=0, minute=0, second=0, microsecond=0) if _created else None
        return InstrumentRecord(
            instrument_key=ticker,
            venue=self.venue,
            symbol=str(title)[:100],
            raw_symbol=event_ticker,
            instrument_type=InstrumentType.PREDICTION_MARKET,
            base_asset=base_asset,
            quote_asset="USD",
            tick_size=tick_size,
            min_size=Decimal("1"),
            min_order_size=min_order,
            contract_size=Decimal("1"),
            settle_asset="USD",
            expiry=expiry,
            strike=None,
            option_type=None,
            is_active=is_active,
            updated_at=now,
            available_from_datetime=_afd,
            available_to_datetime=lifecycle.settlement_time if lifecycle else None,
        )

    def _parse_close_time(self, close_time_raw: str | None) -> datetime | None:
        """Parse Kalshi close_time ISO string to UTC datetime."""
        if not close_time_raw:
            return None
        try:
            return datetime.fromisoformat(close_time_raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    @staticmethod
    def _kalshi_status_to_lifecycle_status(
        status_raw: object,
    ) -> Literal["created", "active", "resolved", "settled"]:
        """Map Kalshi market.status enum to MarketLifecycle.current_status.

        Kalshi statuses:
          ``initialized``  → ``"created"``
          ``inactive``     → ``"created"``
          ``active``       → ``"active"``
          ``closed``       → ``"resolved"`` (trading stopped, outcome pending)
          ``determined``   → ``"resolved"``
          ``disputed``     → ``"resolved"``
          ``amended``      → ``"resolved"``
          ``finalized``    → ``"settled"``
          (anything else)  → ``"created"``
        """
        if not isinstance(status_raw, str):
            return "created"
        status = status_raw.lower()
        if status == "active":
            return "active"
        if status in ("closed", "determined", "disputed", "amended"):
            return "resolved"
        if status == "finalized":
            return "settled"
        return "created"

    def classify_lifecycle(self, market: KalshiMarket) -> MarketLifecycle | None:
        """Build a :class:`MarketLifecycle` for a Kalshi market.

        Returns ``None`` when ``ticker`` is missing or ``close_time`` is
        unparseable — without a resolution timestamp the cluster gate
        can't reason about whether the market overlaps a given day.

        Lifecycle field derivation:

        * ``market_created_at``: ``open_time`` (when trading opened);
          falls back to ``expected_expiration_time - 30d`` when missing
          (rare; older markets with no ``open_time`` field).
        * ``resolution_time``: ``close_time`` (when trading stops and
          UMA-equivalent resolution begins).
        * ``settlement_time``: ``resolution_time + canonical_group.settlement_lag``
          per UAC :data:`CANONICAL_GROUP_METADATA`. Kalshi finalisation
          typically adds 1-24h to the close time.
        * ``current_status``: derived from ``status`` enum via
          :meth:`_kalshi_status_to_lifecycle_status`.

        The Kalshi canonical-group classifier is the full three-tier
        :func:`unified_api_contracts.predictions.classify_kalshi_to_canonical_group`
        (override dict → ticker-prefix rules → OTHER fallback): crypto-daily,
        macro, equity-index and FX series map to the SAME
        :class:`CanonicalQuestionGroup` values as their Polymarket counterparts
        (enabling cross-venue dispersion); only genuinely venue-unique markets
        (FDV / token-launch / airdrop / single-coin niche) fall to
        :class:`CanonicalQuestionGroup.OTHER`. (Prior docstring said
        "override-only" — STALE since the prefix classifier landed
        unified-api-contracts@c3bf51d1; the call below has always invoked it.)
        """
        ticker = market.ticker
        if not ticker:
            return None

        resolution_time = self._parse_close_time(market.close_time)
        if resolution_time is None:
            return None

        market_created_at = self._parse_close_time(market.open_time)
        if market_created_at is None:
            # Fallback: assume markets open 30 days before they resolve.
            # Conservative — over-broadens the lifecycle window so ticks
            # are never gated out for a missing open_time, but still
            # gives feature compute a per-market created_at to enforce.
            from datetime import timedelta as _td  # noqa: qg-inside-import

            market_created_at = resolution_time - _td(days=30)

        group = classify_kalshi_to_canonical_group(ticker=ticker) or CanonicalQuestionGroup.OTHER
        settlement_lag = CANONICAL_GROUP_METADATA[group].settlement_lag
        settlement_time = resolution_time + settlement_lag

        current_status = self._kalshi_status_to_lifecycle_status(market.status)

        return MarketLifecycle(
            market_id=ticker,
            venue=self.venue,
            canonical_group=group,
            market_created_at=market_created_at,
            resolution_time=resolution_time,
            settlement_time=settlement_time,
            current_status=current_status,
        )

    def get_market_lifecycles(self) -> list[MarketLifecycle]:
        """Return :class:`MarketLifecycle` rows for the markets captured by
        the most-recent :meth:`get_instruments` call.

        Used by the orchestrator's prediction writer path to emit the
        ``MARKET_LIFECYCLE`` data_type parquet alongside per-instrument
        records (per the
        ``predictions_master.plan.md`` Phase 1 critical path).

        Markets that fail :meth:`classify_lifecycle` (missing ticker or
        unparseable close_time) are silently dropped — they're already
        excluded from the InstrumentRecord output for the same reasons.
        Returns an empty list before any ``get_instruments()`` call.
        """
        out: list[MarketLifecycle] = []
        for market in self._last_markets:
            lifecycle = self.classify_lifecycle(market)
            if lifecycle is not None:
                out.append(lifecycle)
        return out
