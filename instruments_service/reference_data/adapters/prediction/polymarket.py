"""Polymarket reference data adapter — prediction market instrument listing.

Polymarket Gamma API provides public market metadata without authentication.
Each active market is returned as an InstrumentRecord so services can treat
prediction markets as tradeable instruments alongside crypto and sports.

Base URL: https://gamma-api.polymarket.com
No auth required for market listing.

Supported endpoint:
  GET /markets?closed=false&active=true&limit=100&offset={n}
"""

import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    PolymarketGammaMarket,
    classify_venue_error,
)
from unified_api_contracts.canonical.domain.prediction import (
    PredictionMarketMapper,
)
from unified_api_contracts.canonical.domain.sports import (
    build_crypto_prediction_id,
    build_macro_prediction_id,
    build_prediction_instrument_id,
)
from unified_api_contracts.canonical.domain.sports.canonical_ids import (
    POLYMARKET_MARKET_TO_CANONICAL,
    _slug,
)
from unified_api_contracts.external.polymarket import (
    POLYMARKET_PREDICTION_LEAGUES,
    get_canonical_league_for_polymarket_series,
    get_canonical_team_for_polymarket,
)
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_CRYPTO_KEYWORDS: dict[str, str] = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
    "xrp": "XRP",
    "dogecoin": "DOGE",
    "doge": "DOGE",
    "bnb": "BNB",
    "hyperliquid": "HYPE",
    "hype": "HYPE",
}

_MACRO_KEYWORDS: dict[str, str] = {
    "s&p 500": "SPX",
    "(spx)": "SPX",
    "nasdaq": "NDX",
    "dow jones": "DJIA",
    "crude oil": "CRUDE_OIL",
    "gold (gc)": "GOLD",
    "silver (si)": "SILVER",
}


def _match_crypto_asset(q_lower: str) -> str | None:
    for kw, canonical in _CRYPTO_KEYWORDS.items():
        if kw in q_lower:
            return canonical
    return None


def _match_macro_index(q_lower: str) -> str | None:
    for kw, canonical in _MACRO_KEYWORDS.items():
        if kw in q_lower:
            return canonical
    return None


_GENERIC_OUTCOMES: frozenset[str] = frozenset({"yes", "no", "over", "under", "odd", "even"})

_VS_PATTERN = re.compile(r"^(.+?)\s+vs\.?\s+(.+?)$", re.IGNORECASE)
_SINGLE_TEAM_PATTERN = re.compile(
    r"^Will\s+(.+?)\s+(?:win|lose|draw)\b",
    re.IGNORECASE,
)
# Suffixes to strip from "away" capture in question-derived vs parses
_QUESTION_SUFFIX = re.compile(
    r"\s+(?:end in a draw|win|lose)\b.*$",
    re.IGNORECASE,
)


def _parse_vs_string(text: str) -> tuple[str, str] | None:
    """Parse 'Home vs. Away' or 'Home vs Away' into a (home, away) tuple.

    Handles:
    - "Arsenal vs. Chelsea"
    - "Arsenal vs. Chelsea - More Markets" (Polymarket event title suffix)
    - "Will Arsenal vs. Chelsea end in a draw?" (question format)
    - "Super Rugby Pacific: Blues vs Moana Pasifika" (prefixed titles)
    """
    cleaned = text.strip()
    # Strip " - More Markets" suffix from event titles
    cleaned = re.sub(r"\s+-\s+More Markets$", "", cleaned)
    # Strip leading "Will " for question-style strings
    if cleaned.lower().startswith("will "):
        cleaned = cleaned[5:]
    # Strip prefixed league names like "Super Rugby Pacific: "
    if ": " in cleaned and " vs" in cleaned.split(": ", 1)[1].lower():
        cleaned = cleaned.split(": ", 1)[1]
    m = _VS_PATTERN.match(cleaned)
    if m:
        home = m.group(1).strip()
        away = m.group(2).strip()
        # Strip question suffixes from away team (e.g. "end in a draw?")
        away = _QUESTION_SUFFIX.sub("", away).strip().rstrip("?")
        return home, away
    return None


def _parse_single_team_question(question: str) -> str | None:
    """Parse 'Will Arsenal win on 2026-03-22?' into team name."""
    m = _SINGLE_TEAM_PATTERN.match(question.strip())
    if m:
        team = m.group(1).strip()
        # Strip trailing date patterns like "on 2026-03-22"
        team = re.sub(r"\s+on\s+\d{4}-\d{2}-\d{2}$", "", team)
        return team if team else None
    return None


def _normalize_team_pair(raw_home: str, raw_away: str) -> tuple[str, str]:
    """Normalize a pair of team names to canonical IDs."""
    home_canonical = get_canonical_team_for_polymarket(raw_home)
    away_canonical = get_canonical_team_for_polymarket(raw_away)
    home = home_canonical if home_canonical else _slug(raw_home)[:30]
    away = away_canonical if away_canonical else _slug(raw_away)[:30]
    return home, away


def _selection_from_outcomes(outcomes: list[str], market_type: str) -> str:
    """Determine the selection label from outcomes and market type."""
    if not outcomes:
        return "HOME"
    first = outcomes[0].lower()
    if first in ("over",):
        return "OVER"
    if first in ("under",):
        return "UNDER"
    if market_type in ("totals", "both_teams_to_score"):
        return "OVER" if first == "yes" else "HOME"
    # For moneyline: "Yes" on "Will X win?" → HOME for first team
    # For spreads: first outcome is typically the spread team
    return "HOME"


def _get_first_event(raw_item: object) -> dict[str, object] | None:
    """Return the first event dict from a raw Gamma API market, or None."""
    if not isinstance(raw_item, dict):
        return None
    events = raw_item.get("events")
    if not isinstance(events, list) or not events:
        return None
    event = events[0]
    return event if isinstance(event, dict) else None


def _enrich_raw_event_fields(raw_item: object) -> None:
    """Extract nested event fields into top-level dict keys before Pydantic validation.

    Populates series_slug, event_title, event_slug from events[0] if not already set.
    """
    event = _get_first_event(raw_item)
    if event is None or not isinstance(raw_item, dict):
        return
    raw_item.setdefault("series_slug", _extract_series_slug(raw_item))
    raw_item.setdefault("event_title", event.get("title"))
    raw_item.setdefault("event_slug", event.get("slug"))


def _extract_series_slug(raw: dict[str, object]) -> str | None:
    """Extract series slug from nested events[].seriesSlug or events[].series[].slug."""
    events = raw.get("events")
    if not isinstance(events, list) or not events:
        return None
    event = events[0]
    if not isinstance(event, dict):
        return None
    # Try event.seriesSlug first (flat)
    series_slug = event.get("seriesSlug")
    if isinstance(series_slug, str) and series_slug:
        return series_slug
    # Try event.series[].slug (nested)
    series_list = event.get("series")
    if isinstance(series_list, list) and series_list:
        first = series_list[0]
        if isinstance(first, dict):
            slug = first.get("slug")
            if isinstance(slug, str) and slug:
                return slug
    return None


_GAMMA_BASE = "https://gamma-api.polymarket.com"
_PAGE_LIMIT = 100
_MAX_PAGES_ACTIVE = 20  # cap for active-only mode (live)
_MAX_PAGES_HISTORICAL = 200  # cap for per-date historical mode (batch) — ~20k markets
_MAPPER = PredictionMarketMapper()


def _classify_polymarket_error(exc: Exception, status: int | None = None) -> str:
    """Map a Polymarket HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "429"
    if status == 401 or "401" in msg or "unauthorized" in msg:
        return "401"
    if status == 400 or "400" in msg or "bad request" in msg:
        return "400"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class PolymarketReferenceDataAdapter(BaseReferenceDataAdapter):
    """Polymarket reference data adapter.

    Returns active prediction markets as InstrumentRecord instances.
    For sports fixtures, cross-references with API-Football to get canonical
    fixture_ids (requires ``api_football_api_key``).

    No auth required for Polymarket Gamma API — it's public.
    The optional ``api_football_api_key`` enables fixture cross-referencing
    for venue-swappable instrument IDs.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        api_football_api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._api_football_key = api_football_api_key
        self._fixture_cache: dict[str, str] = {}  # "LEAGUE:HOME:AWAY:DATE" → fixture_id
        self._last_raw_page_size: int = 0

    @property
    def venue(self) -> str:
        return "POLYMARKET"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch Polymarket markets as InstrumentRecord list.

        Args:
            instrument_type: Filter by type. Pass "PREDICTION_MARKET" or None.
            date: If provided (YYYY-MM-DD), fetch ALL markets that ended on this date
                  (including closed/resolved). For batch/backtest mode.
                  If None, fetch currently active markets only (live mode).
        """
        if instrument_type is not None and instrument_type != "PREDICTION_MARKET":
            return []
        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        max_pages = _MAX_PAGES_HISTORICAL if date else _MAX_PAGES_ACTIVE
        self._last_raw_page_size = 0
        async with aiohttp.ClientSession() as session:
            for page in range(max_pages):
                offset = page * _PAGE_LIMIT
                batch = await self._fetch_page(session, offset, now, date=date)
                results.extend(batch)
                # Use raw API page size for pagination (not filtered count)
                if self._last_raw_page_size < _PAGE_LIMIT:
                    break
                if page > 0 and page % 20 == 0:
                    logger.info(
                        "Polymarket fetch: %d instruments so far (offset=%d, date=%s)",
                        len(results),
                        offset,
                        date,
                    )
        logger.info("Polymarket: fetched %d instruments (date=%s)", len(results), date or "active")
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch single market by condition_id or market_slug."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.instrument_key == symbol or inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError(
            "Polymarket does not provide options chains. Use get_instruments() to list available prediction markets."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError(
            "Polymarket does not provide expiry calendars. Market resolution dates are in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Polymarket does not have perpetual funding rates.")

    _CLOB_INTERVAL_MAP: ClassVar[dict[str, str]] = {
        "1m": "1m",
        "5m": "1m",
        "15m": "1m",
        "30m": "1m",
        "1h": "1h",
        "4h": "6h",
        "6h": "6h",
        "12h": "6h",
        "1d": "1d",
        "1w": "1w",
        "1M": "1M",
    }

    def _parse_clob_point(self, point_raw: object, symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a Polymarket CLOB price point into a synthetic OHLCVRef."""
        if not isinstance(point_raw, dict):
            return None
        point: dict[str, object] = point_raw
        ts_raw = point.get("t")
        price_raw = point.get("p")
        if ts_raw is None or price_raw is None:
            return None
        ts = datetime.fromtimestamp(int(str(ts_raw)), tz=UTC)
        price = Decimal(str(price_raw))
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=Decimal("0"),
            interval=interval,
        )

    async def _fetch_clob_history(self, symbol: str, clob_interval: str) -> list[object]:
        """Fetch raw price history from Polymarket CLOB API. Returns [] on error."""
        url = "https://clob.polymarket.com/prices-history"
        params = {"market": symbol, "interval": clob_interval, "fidelity": "1"}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    raw: dict[str, object] = cast("dict[str, object]", await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_polymarket_error(exc)
                classification = classify_venue_error("POLYMARKET", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Polymarket CLOB prices-history failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "POLYMARKET",
                        "endpoint": "clob/prices-history",
                        "symbol": symbol,
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                return []
        history_obj: object = raw.get("history")
        return history_obj if isinstance(history_obj, list) else []

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch price history from Polymarket CLOB API (prices-history endpoint).

        Uses GET https://clob.polymarket.com/prices-history?market={token_id}&interval={interval}.
        The symbol should be the Polymarket token_id (CLOB market identifier).
        Returns synthetic OHLCV (open=first, close=last, high=max, low=min of period).
        """
        clob_interval = self._CLOB_INTERVAL_MAP.get(interval, "1d")
        history_raw = await self._fetch_clob_history(symbol, clob_interval)
        return [
            ref
            for point_raw in history_raw[-limit:]
            if (ref := self._parse_clob_point(point_raw, symbol, interval)) is not None
        ]

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        offset: int,
        now: datetime,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        if date:
            # Historical mode: fetch ALL markets ending on this UTC date
            params: dict[str, str] = {
                "limit": str(_PAGE_LIMIT),
                "offset": str(offset),
                "end_date_min": f"{date}T00:00:00Z",
                "end_date_max": f"{date}T23:59:59Z",
            }
        else:
            # Live mode: active markets sorted by volume
            params = {
                "closed": "false",
                "active": "true",
                "limit": str(_PAGE_LIMIT),
                "offset": str(offset),
                "order": "volume24hr",
                "ascending": "false",
            }
        url = f"{_GAMMA_BASE}/markets"
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_polymarket_error(exc)
            classification = classify_venue_error("POLYMARKET", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Polymarket markets failed (offset=%d): %s (classified: %s, action: %s)",
                offset,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "POLYMARKET",
                    "endpoint": "gamma/markets",
                    "offset": offset,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        raw_list = cast(list[object], raw_json)
        results: list[InstrumentRecord] = []
        for raw_item in raw_list:
            _enrich_raw_event_fields(raw_item)
            try:
                market = PolymarketGammaMarket.model_validate(raw_item)
            except (ValueError, TypeError) as exc:
                # Skip individual markets that fail validation — don't kill the whole page
                slug = raw_item.get("market_slug", "?") if isinstance(raw_item, dict) else "?"
                logger.debug("Polymarket: skipping market %s — validation error: %s", slug, exc)
                continue
            record = self._parse_market(market, now)
            if record is not None:
                results.append(record)
        # Store raw page size so caller can paginate correctly even when
        # many markets are filtered out (e.g. non-prediction-league sports).
        self._last_raw_page_size = len(raw_list)
        return results

    def _parse_market(
        self,
        market: PolymarketGammaMarket,
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a PolymarketGammaMarket to an InstrumentRecord.

        Generates canonical instrument IDs following the system convention:
        - Sports: ``POLYMARKET::{LEAGUE}:{HOME}-v-{AWAY}:{DATE}:{MARKET_TYPE}``
        - Crypto/Macro: ``POLYMARKET::{CATEGORY}:{QUESTION_HASH}``
        - Other: ``POLYMARKET::{CATEGORY}:{QUESTION_HASH}``

        Team names normalized via ``get_canonical_team_for_polymarket()``.
        Category from ``PredictionMarketMapper``.
        """
        condition_id = market.condition_id
        if not condition_id:
            return None
        slug = market.market_slug or condition_id
        question = market.question or ""
        expiry = self._parse_end_date(market.end_date_iso)
        is_active = bool(market.active) and not bool(market.closed)
        tick_raw = market.minimum_tick_size
        tick_size = Decimal(str(tick_raw)) if tick_raw else Decimal("0.01")
        min_order_raw = market.minimum_order_size
        min_order = Decimal(str(min_order_raw)) if min_order_raw else Decimal("1")

        # Classify via PredictionMarketMapper
        mapped = _MAPPER.map_market(
            venue="POLYMARKET",
            market_id=condition_id,
            question=question,
            resolution_date=expiry,
            outcomes=tuple(market.outcomes) if market.outcomes else ("Yes", "No"),
        )
        category = mapped.category.value  # crypto, financial, sports, politics, etc.

        # Build instrument_type and base_asset based on category
        result = self._build_instrument_id(
            market,
            category,
            question,
            slug,
            expiry,
        )
        if result is None:
            return None  # League not in prediction registry — skip
        _sub_category, base_asset = result

        return InstrumentRecord(
            instrument_key=condition_id,
            venue=self.venue,
            symbol=slug,
            raw_symbol=slug,
            instrument_type="PREDICTION_MARKET",
            base_asset=base_asset,
            quote_asset="USDC",
            tick_size=tick_size,
            min_size=Decimal("1"),
            min_order_size=min_order,
            contract_size=Decimal("1"),
            settle_asset="USDC",
            expiry=expiry,
            strike=None,
            option_type=None,
            is_active=is_active,
            updated_at=now,
        )

    def _build_instrument_id(
        self,
        market: PolymarketGammaMarket,
        category: str,
        question: str,
        slug: str,
        expiry: datetime | None,
    ) -> tuple[str, str] | None:
        """Build (instrument_type, base_asset) with canonical naming.

        Returns:
            instrument_type: e.g. "prediction::crypto", "prediction::sports::EPL"
            base_asset: human-readable canonical e.g. "BTC:UP_DOWN:2026-03-25"
                        or "EPL:ARSENAL-v-CHELSEA:2026-03-25:MONEYLINE"
            None if the market should be skipped (e.g. league not in prediction registry).
        """
        date_str = expiry.strftime("%Y-%m-%d") if expiry else "unknown"

        # Sports markets: use team names + league + market type
        if market.sports_market_type and market.outcomes:
            return self._build_sports_id(market, date_str)

        q_lower = question.lower()
        crypto_match = _match_crypto_asset(q_lower)
        if crypto_match:
            canonical_id = build_crypto_prediction_id("polymarket", crypto_match, "1D", date_str)
            return "prediction::crypto", canonical_id

        macro_match = _match_macro_index(q_lower)
        if macro_match:
            canonical_id = build_macro_prediction_id("polymarket", macro_match, "1D", date_str)
            return "prediction::macro", canonical_id

        # Sports classified by PredictionMarketMapper but without sportsMarketType
        # (e.g. F1, UFC, NBA props) — reclassify as "other" since they're not
        # structured sports markets we can normalize.
        label = "other" if category == "sports" else category
        return f"prediction::{label}", question[:50] if question else slug[:50]

    def _build_sports_id(
        self,
        market: PolymarketGammaMarket,
        date_str: str,
    ) -> tuple[str, str] | None:
        """Build canonical sports instrument ID using the system-wide format.

        Uses ``build_prediction_instrument_id()`` from UAC canonical_ids so that
        Polymarket sports instruments have the SAME format as Betfair/Odds API:
        ``FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME``

        Team names are extracted from (in priority order):
        1. ``event_title`` — e.g. "Arsenal vs. Chelsea" (most reliable)
        2. ``outcomes`` — for spreads markets where outcomes ARE team names
        3. ``question`` — e.g. "Will Arsenal win on 2026-03-22?"

        Returns None if the league is not in the prediction leagues registry
        (esports, cricket, rugby, etc. are dropped).
        """
        # Resolve league from series_slug — skip if not in prediction registry
        series_slug = market.series_slug or ""
        league_id = get_canonical_league_for_polymarket_series(series_slug) if series_slug else None
        if not league_id or league_id not in POLYMARKET_PREDICTION_LEAGUES:
            return None

        outcomes = market.outcomes or []
        pm_market_type = market.sports_market_type or "moneyline"
        canonical_market = POLYMARKET_MARKET_TO_CANONICAL.get(pm_market_type, _slug(pm_market_type))

        # Extract team names — priority: event_title > outcomes > question
        home, away = self._extract_teams(market, outcomes)

        # Derive season from date
        date_digits = date_str.replace("-", "")[:8]
        year = int(date_digits[:4]) if len(date_digits) >= 4 else 2026
        month = int(date_digits[4:6]) if len(date_digits) >= 6 else 1
        season_start = year if month >= 8 else year - 1
        season = f"{season_start}-{(season_start + 1) % 100:02d}"

        # Determine selection from outcomes
        selection = _selection_from_outcomes(outcomes, pm_market_type)

        instrument_id = build_prediction_instrument_id(
            venue="POLYMARKET",
            market_type=canonical_market,
            league_id=league_id,
            season=season,
            home_team_id=home,
            away_team_id=away,
            selection=selection,
            point=market.line,
        )

        instrument_type = f"prediction::sports::{league_id}"
        return instrument_type, instrument_id

    def _extract_teams(
        self,
        market: PolymarketGammaMarket,
        outcomes: list[str],
    ) -> tuple[str, str]:
        """Extract canonical home/away team names from market metadata.

        Priority:
        1. event_title "Home vs. Away" — always has both teams
        2. outcomes (for spreads where outcomes = team names, not Yes/No)
        3. question text "Will Home win?" or "Home vs. Away: O/U 2.5"
        """
        # 1. Try event_title — most reliable, always "Home vs. Away"
        event_title = market.event_title or ""
        teams = _parse_vs_string(event_title)
        if teams:
            return _normalize_team_pair(teams[0], teams[1])

        # 2. Try outcomes — works for spreads (e.g. ["Austin FC", "Real Salt Lake"])
        #    but NOT for moneyline/totals/btts where outcomes are Yes/No/Over/Under
        non_generic = [o for o in outcomes[:2] if o.lower() not in _GENERIC_OUTCOMES]
        if len(non_generic) == 2:
            return _normalize_team_pair(non_generic[0], non_generic[1])

        # 3. Try question text — "Will Arsenal win?", "Arsenal vs. Chelsea: O/U 2.5"
        question = market.question or ""
        teams = _parse_vs_string(question.split(":")[0])  # strip ": O/U 2.5" suffix
        if teams:
            return _normalize_team_pair(teams[0], teams[1])

        # 4. Single-team question: "Will Arsenal win on 2026-03-22?"
        single = _parse_single_team_question(question)
        if single:
            canonical = get_canonical_team_for_polymarket(single)
            team_id = canonical if canonical else _slug(single)[:30]
            return team_id, "UNKNOWN"

        return "UNKNOWN", "UNKNOWN"

    async def _cross_reference_fixture(
        self,
        league_id: str,
        home_team: str,
        away_team: str,
        date_str: str,
    ) -> str | None:
        """Look up API-Football fixture_id for a Polymarket sports fixture.

        Uses USRI api_football adapter with cached results. Returns the numeric
        API-Football fixture_id as string, or None if no match found.
        """
        if not self._api_football_key:
            return None

        cache_key = f"{league_id}:{home_team}:{away_team}:{date_str}"
        if cache_key in self._fixture_cache:
            return self._fixture_cache[cache_key]

        from unified_api_contracts.canonical.domain.sports.league_data import (
            get_league,
        )

        league_def = get_league(league_id)
        if not league_def or not league_def.api_football_id:
            return None

        from ..sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key=self._api_football_key)
        try:
            fixtures = await adapter.get_fixtures(
                date=date_str,
                league_ids=[league_def.api_football_id],
            )
        except Exception as exc:
            logger.warning("API-Football fixture lookup failed for %s: %s", cache_key, exc)
            return None

        # Match by team names (either home or away matches either side)
        ht = home_team.upper()
        at = away_team.upper()
        for fixture in fixtures:
            h = fixture.home_team.team_id.upper()
            a = fixture.away_team.team_id.upper()
            if (ht in h or h in ht or at in a or a in at) and (ht in h or ht in a or h in ht or a in ht):
                fid = fixture.fixture_id
                self._fixture_cache[cache_key] = fid
                logger.info("Cross-ref match: %s %s-v-%s → %s", league_id, home_team, away_team, fid)
                return fid

        return None

    def _parse_end_date(self, end_date_raw: str | None) -> datetime | None:
        """Parse ISO end date string to UTC datetime."""
        if not end_date_raw:
            return None
        try:
            return datetime.fromisoformat(end_date_raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
