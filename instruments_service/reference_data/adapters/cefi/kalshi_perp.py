"""Kalshi crypto-perp reference data adapter — CFTC-regulated perpetual futures (KALSHI-PERP).

Distinct from the prediction-market KalshiReferenceDataAdapter in
``adapters/prediction/kalshi.py``. KALSHI-PERP is a real, intended cefi venue —
Kalshi launched CFTC-regulated crypto perpetual futures on 2026-05-29.

⚠️  ENUMERATION DISABLED PENDING REPOINT (``_REPOINT_PENDING = True``). SSOT:
``plans/active/prediction_capture_incident_remediation_2026_07_06.md`` Workstream B
(issue ``plans/active/issues/prediction_universe_capture_dead_since_07_01_2026_07_06.md``).

WHY: this adapter was pointed at the Kalshi *events* host
``https://api.elections.kalshi.com/trade-api/v2/markets`` — which serves ONLY
binary event contracts. A live probe of 3,000 crypto-series markets returned
100% ``market_type=binary``, 0 perpetuals; every market carries ``category: null``.
The ``category=Crypto`` request param is ignored by that endpoint and the
client-side filter treated empty-category rows as a PASS, so the adapter emitted
the entire binary event universe (``KXMVESPORTSMULTIGAMEEXTENDED`` /
``KXMVECROSSCATEGORY`` …) as fake ``PERPETUAL`` rows — contaminating the cefi
catalogue with 25,473 rows.

Kalshi's real perpetual futures live on a SEPARATE, authenticated host +
namespace: ``https://external-api.kalshi.com/trade-api/v2/margin/`` (demo
``external-api.demo.kalshi.co``), tickers like ``BTC-PERPETUAL``, funding via
``/margin/funding_rates/*`` — RSA-PSS auth required, rolling out member by member.
Until this adapter is repointed there (plan Phase 2), enumeration returns an
honest-empty result: the venue stays declared but carries no reference-data feed.
Do NOT re-enable against the events host.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
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
_PAGE_LIMIT = 200
_MAX_PAGES = 10  # cap at 2000 contracts per fetch

# Enumeration is DISABLED until the adapter is repointed from the events host to
# the auth'd margin/perps API (plan Workstream B, Phase 2). While True,
# get_instruments()/get_instrument() return an honest-empty result BEFORE any
# network call — the events host has 0 perpetuals, so emitting anything from it
# is contamination. The venue declaration stays; only the feed is empty.
_REPOINT_PENDING = True

_STATUS_MAP: dict[int, str] = {429: "429", 401: "401", 403: "403", 400: "400"}
_MSG_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("429", "rate"), "429"),
    (("401", "unauthorized"), "401"),
    (("403", "forbidden"), "403"),
    (("400", "bad request"), "400"),
    (("500", "internal", "server"), "500"),
)


def _classify_kalshi_perp_error(exc: Exception, status: int | None = None) -> str:
    """Map a Kalshi HTTP/network error to a UAC error code."""
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


class KalshiPerpReferenceDataAdapter(BaseReferenceDataAdapter):
    """Kalshi crypto-perp reference data adapter (KALSHI-PERP).

    Returns active CFTC-regulated crypto perpetual futures as InstrumentRecord
    instances with ``instrument_type=PERPETUAL``.

    Field mapping:
      instrument_key  = market ticker (e.g. "KXBTCUSD-PERP")
      venue           = "KALSHI-PERP"  (canonical UAC venue; matches VENUES_BY_ASSET_GROUP["cefi"])
      instrument_type = InstrumentType.PERPETUAL
      raw_symbol      = market ticker
      base_asset      = underlying crypto symbol (e.g. "BTC")
      quote_asset     = "USD"
      status          = InstrumentStatus.ACTIVE when market.status == "active", else DELISTED

    Public read — no API key required for contract enumeration.
    Trades/funding/orderbook require authentication (separate data pipeline).
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier (canonical UAC casing)."""
        return "KALSHI-PERP"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Kalshi crypto-perp contracts as InstrumentRecord list.

        Filters to ``category=Crypto`` and ``status=open`` — the correct
        API-level lifecycle filter for currently-tradeable contracts
        (per-market ``status=active`` is the tradeable state; ``open`` is the
        API request filter that returns those markets).

        instrument_type filter: pass ``InstrumentType.PERPETUAL`` or None (all).
        Other values return an empty list (venue only has PERPETUAL).
        """
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        if _REPOINT_PENDING:
            # DISABLED pending repoint to the margin/perps API (plan Phase 2).
            # The events host serves 0 perpetuals; return honest-empty rather
            # than emitting binary event contracts as fake PERPETUAL. No network
            # call is made. See module docstring.
            log_event(
                "ADAPTER_DISABLED_PENDING_REPOINT",
                details={
                    "venue": self.venue,
                    "reason": "events-host serves 0 perpetuals; awaiting margin-API repoint (Phase 2)",
                },
            )
            return []

        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        cursor: str | None = None
        fetch_failed = False

        async with self._make_session() as session:
            for _page in range(_MAX_PAGES):
                try:
                    batch, cursor = await self._fetch_markets_page(session, cursor, now)
                except RuntimeError:
                    # Page fetch failed — ADAPTER_FETCH_FAILED already emitted in
                    # _fetch_markets_page. Per shard-isolation, keep pages already
                    # fetched; the all-failed case re-raises below.
                    fetch_failed = True
                    break
                results.extend(batch)
                if cursor is None or len(batch) < _PAGE_LIMIT:
                    break

        if fetch_failed and not results:
            # All pages failed with zero records → raise so the caller records
            # this venue as attempted_failed (not a silent honest-empty).
            raise RuntimeError(
                "KalshiPerp get_instruments: market fetch failed with no records "
                "(see ADAPTER_FETCH_FAILED) — recording attempted_failed, not empty"
            )
        return results

    async def _fetch_markets_page(
        self,
        session: aiohttp.ClientSession,
        cursor: str | None,
        now: datetime,
    ) -> tuple[list[InstrumentRecord], str | None]:
        """Fetch one page of Kalshi crypto-perp markets.

        Uses category=Crypto + status=open to select CFTC crypto perp contracts.
        Returns (records, next_cursor); next_cursor is None when no more pages.
        """
        url = f"{_KALSHI_BASE_URL}/markets"
        params: dict[str, str] = {
            "limit": str(_PAGE_LIMIT),
            "status": "open",
            "category": "Crypto",
        }
        if cursor is not None:
            params["cursor"] = cursor

        headers = {"Accept": "application/json"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    logger.error("Kalshi-perp authentication failed (HTTP 401)")
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": self.venue,
                            "endpoint": "markets",
                            "error": "HTTP 401 Unauthorized",
                            "error_code": "401",
                            "action": "fail",
                            "retry_safe": False,
                        },
                    )
                    raise RuntimeError("Kalshi-perp markets fetch failed: HTTP 401 Unauthorized")
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_kalshi_perp_error(exc)
            classification = classify_venue_error("kalshi", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Kalshi-perp markets request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": self.venue,
                    "endpoint": "markets",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            raise RuntimeError(f"Kalshi-perp markets fetch failed: {exc}") from exc

        raw_dict = cast(dict[str, object], raw_json)
        markets_raw = raw_dict.get("markets")
        next_cursor_raw = raw_dict.get("cursor")
        next_cursor = str(next_cursor_raw) if next_cursor_raw else None

        if not isinstance(markets_raw, list):
            return [], None

        records: list[InstrumentRecord] = []
        for raw_item in markets_raw:
            market = cast(dict[str, object], raw_item)
            record = self._parse_market(market, now)
            if record is not None:
                records.append(record)

        return records, next_cursor

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single Kalshi crypto-perp contract by ticker."""
        if _REPOINT_PENDING:
            # DISABLED pending margin-API repoint (plan Phase 2) — see get_instruments.
            return None
        url = f"{_KALSHI_BASE_URL}/markets/{symbol}"
        headers = {"Accept": "application/json"}
        now = datetime.now(UTC)
        async with self._make_session() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_kalshi_perp_error(exc)
                classification = classify_venue_error("kalshi", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Kalshi-perp get_instrument failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": self.venue,
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

    def _parse_market(
        self,
        raw: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a Kalshi market dict to an InstrumentRecord (PERPETUAL type).

        Crypto-perp contracts are identified by category=Crypto and no
        settlement date (they are perpetual with rolling funding). The
        base_asset is extracted from the underlying field or the ticker prefix.
        """
        ticker = raw.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            return None

        # Reject anything not EXPLICITLY tagged as a crypto perp. The events host
        # returns category: null → "" for every market; the earlier code treated
        # "" as a PASS, which let the entire binary event universe through as fake
        # PERPETUAL (the 25,473-row cefi contamination). Enumeration is disabled
        # above (_REPOINT_PENDING); this keeps the parser honest if ever called
        # directly and documents the real bug.
        category = raw.get("category") or raw.get("series_category") or ""
        if not isinstance(category, str) or category.lower() not in ("crypto", "cryptocurrency"):
            return None

        # Derive base_asset from the underlying field, series_ticker, or ticker.
        # E.g. ticker "KXBTCUSD" → base "BTC"; "BTCPERP" → "BTC".
        underlying = raw.get("underlying") or raw.get("series_ticker") or ""
        base_asset = _extract_base_asset(str(underlying) if underlying else ticker)

        status_raw = raw.get("status")
        is_active_bool = str(status_raw).lower() == "active" if status_raw else True
        instrument_status = InstrumentStatus.ACTIVE if is_active_bool else InstrumentStatus.DELISTED

        # Perpetual contracts: no expiry date (None signals continuous contract).
        # Kalshi perps may have a close_time for the current funding period;
        # we do NOT treat that as an instrument expiry.
        return InstrumentRecord(
            instrument_key=ticker,
            # No CeFi raw-code-to-human-name translation gap (see other CeFi adapters'
            # identical comment) — canonical_instrument_id mirrors instrument_key.
            canonical_instrument_id=ticker,
            venue=self.venue,
            raw_symbol=ticker,
            instrument_type=InstrumentType.PERPETUAL,
            base_asset=base_asset[:50],
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            min_size=Decimal("1"),
            contract_size=Decimal("1"),
            settle_asset="USD",
            expiry=None,
            strike=None,
            option_type=None,
            status=instrument_status,
            available_from_datetime=None,
            available_to_datetime=None,
        )

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Not supported for perpetual contracts."""
        raise NotImplementedError(
            "Kalshi-perp does not support options chains. Use get_instruments() to list available perpetual contracts."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "PERPETUAL",
    ) -> CanonicalExpiryCalendar:
        """Not supported — perpetual contracts have no expiry calendar."""
        raise NotImplementedError("Kalshi-perp does not have an expiry calendar — contracts are perpetual.")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch current funding rate for a Kalshi perp contract.

        TODO: wire GET /markets/{ticker}/funding_rates once MTDS funding
        data pipeline is set up (BLOCKED-CREDENTIALS: kalshi-perp-api-key
        required for trading-data tier). Instrument enumeration (this adapter)
        is public-read only.
        """
        raise NotImplementedError(
            "Kalshi-perp funding rate requires authentication. "
            "Implement via MTDS funding handler once kalshi-perp-api-key is provisioned."
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """OHLCV not available via reference data API."""
        raise NotImplementedError("Kalshi-perp OHLCV is not available via the reference data API.")


def _extract_base_asset(ticker_or_underlying: str) -> str:
    """Extract the base crypto asset symbol from a Kalshi perp ticker or underlying.

    Examples:
      "KXBTCUSD" → "BTC"
      "KXETHUSD" → "ETH"
      "BTCUSD"   → "BTC"
      "BTC"      → "BTC"
      "KXBTC"    → "BTC"

    Strategy: strip common prefixes (KX, KX...) and suffixes (USD, USDT, USDC,
    PERP), then return what remains (up to 8 chars for safety).
    """
    s = ticker_or_underlying.upper().strip()
    # Strip leading "KX" prefix used by Kalshi market tickers
    if s.startswith("KX"):
        s = s[2:]
    # Strip trailing quote-asset and perp suffixes
    for suffix in ("USDT", "USDC", "USD", "PERP"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[: -len(suffix)]
            break
    return s[:8] if s else ticker_or_underlying[:8]
