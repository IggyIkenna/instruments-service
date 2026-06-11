"""Polymarket question/team/keyword classification helpers + Gamma constants.

Cohesion module of the ``adapters.prediction.polymarket`` package (split from
the former monolithic ``adapters/prediction/polymarket.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators resolve through ``_pm`` — the live package namespace — so
``unittest.mock.patch(
"instruments_service.reference_data.adapters.prediction.polymarket.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split.
"""

# Package-internal access: the polymarket package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from unified_api_contracts.prediction import PredictionMarketMapper

if TYPE_CHECKING:
    from instruments_service.reference_data.adapters.prediction import polymarket as _pm
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.prediction.polymarket._pkg_ref import polymarket_namespace as _pm

__all__ = [
    "_CRYPTO_KEYWORDS",
    "_CRYPTO_LONG_FORMS",
    "_CRYPTO_SHORT_PATTERNS",
    "_CRYPTO_SHORT_TICKERS",
    "_GAMMA_BASE",
    "_GENERIC_OUTCOMES",
    "_MACRO_KEYWORDS",
    "_MAPPER",
    "_MAX_PAGES_ACTIVE",
    "_PAGE_LIMIT",
    "_QUESTION_SUFFIX",
    "_SINGLE_TEAM_PATTERN",
    "_VS_PATTERN",
    "_classify_polymarket_error",
    "_enrich_raw_event_fields",
    "_extract_series_slug",
    "_get_first_event",
    "_match_crypto_asset",
    "_match_macro_index",
    "_normalize_team_pair",
    "_parse_single_team_question",
    "_parse_vs_string",
    "_selection_from_outcomes",
]

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


# Crypto-ticker matching has two flavours:
#
#   1. Long-form names (bitcoin, ethereum, solana, dogecoin, hyperliquid):
#      these are unambiguous — substring match catches "archBitcoin",
#      "BitcoinPriceTomorrow", "Bitcoin?", etc. all correctly.
#
#   2. Short tickers (btc, eth, sol, xrp, doge, bnb, hype): these are
#      embedded in many unrelated words ("abnb" Airbnb, "solar", "etheriza",
#      "hyped"...). They need word-boundary matching to avoid false
#      positives. Pattern: `(?<![a-z])TICKER(?![a-z])` — preceding char
#      must not be a letter, following char must not be a letter, but
#      digits/punctuation/whitespace/start-end are fine.
#
# 2026-05-05 audit:
#   - 30 of 78 BNB-tagged dates were Airbnb (ABNB) noise (substring match
#     of "bnb" in "abnb"). New short-ticker rule rejects these.
#   - 172 BTC / 36 ETH / 41 SOL / 25 XRP / 109 HYPE entries had questions
#     with a generator-prefixed long-form ("archBitcoin", "archEthereum",
#     etc.). These are LEGITIMATE markets that need to keep matching, so
#     long-form names use plain substring.
_CRYPTO_LONG_FORMS: dict[str, str] = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "dogecoin": "DOGE",
    "hyperliquid": "HYPE",
    # XRP has no long form — 3-letter ticker is the canonical word. The short
    # tickers list catches "XRP" / "archXRP" / "xrp-something" via substring
    # because no English word contains the trigram "xrp".
    "xrp": "XRP",
}

_CRYPTO_SHORT_TICKERS: dict[str, str] = {
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
    "doge": "DOGE",
    "bnb": "BNB",
    "hype": "HYPE",
}

_CRYPTO_SHORT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(rf"(?<![a-z]){re.escape(kw)}(?![a-z])"), canonical) for kw, canonical in _CRYPTO_SHORT_TICKERS.items()
]


def _match_crypto_asset(q_lower: str) -> str | None:
    # Long forms first — unambiguous substring match
    for kw, canonical in _CRYPTO_LONG_FORMS.items():
        if kw in q_lower:
            return canonical
    # Short tickers — require non-letter boundaries to reject ABNB/Solar/Hyped/etc.
    for pattern, canonical in _CRYPTO_SHORT_PATTERNS:
        if pattern.search(q_lower):
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
    home_canonical = _pm.get_canonical_team_for_polymarket(raw_home)
    away_canonical = _pm.get_canonical_team_for_polymarket(raw_away)
    home = home_canonical if home_canonical else _pm.slugify_canonical_name(raw_home)[:30]
    away = away_canonical if away_canonical else _pm.slugify_canonical_name(raw_away)[:30]
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
    event = _pm._get_first_event(raw_item)
    if event is None or not isinstance(raw_item, dict):
        return
    raw_item.setdefault("series_slug", _pm._extract_series_slug(raw_item))
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
