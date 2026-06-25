"""TradFi exchange calendar / trading-hours session enrichment.

Cohesion module of the ``adapters.tradfi.databento`` package (split from the
former monolithic ``adapters/tradfi/databento.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Single SSOT for TradFi session metadata (trading hours / holidays / early
closes) across reference-data sources — the Massive adapter reuses
``EXCHANGE_HOURS`` + ``get_session_metadata`` from here via the package facade.

Shared collaborators (the ``xcals`` module alias, the calendar cache and
sibling functions) resolve through ``_db`` — the live package namespace — so
``unittest.mock.patch("instruments_service.reference_data.adapters.tradfi.databento.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split.
"""

# Package-internal access: the databento package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd

if TYPE_CHECKING:
    from datetime import date

    from instruments_service.reference_data.adapters.tradfi import databento as _db
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.tradfi.databento._pkg_ref import databento_namespace as _db

__all__ = [
    "EXCHANGE_HOURS",
    "_EXCHANGE_HOURS",
    "_FX_VENUES_24_7",
    "_XCAL_CACHE",
    "_XCAL_MAPPING",
    "UndeclaredTradfiVenueError",
    "_apply_early_close",
    "_compute_utc_hours",
    "_get_session_metadata",
    "_get_xcal",
    "_is_trading_holiday",
    "_non_trading_result",
    "_resolve_trading_status",
    "get_session_metadata",
    "is_non_trading_day",
    "non_trading_day_reason",
]

_XCAL_MAPPING: dict[str, str] = {
    "NASDAQ": "XNAS",
    "NYSE": "XNYS",
    "CME": "CMES",
    "CBOE": "XNYS",  # CBOE equity/VX products follow NYSE calendar
    "KRX": "XKRX",  # Korea Exchange — Korean holiday calendar (Seollal/Chuseok), KST (G1.e 2026-06-25)
    # ICE stays DECLARED here pending the ICE whole-venue retirement unit (databento
    # billing-blocked → purge sessions+registry+EU+GCS together; operator 2026-06-25). Keeping
    # it declared now means is_non_trading_day("ICE") resolves (no spurious fail-closed raise)
    # while the curated enumeration already drops ICE instruments.
    "ICE": "XNYS",  # ICE US follows NYSE calendar (pending ICE retirement)
}

# Exchange-specific trading hours (local timezone, static per venue)
_EXCHANGE_HOURS: dict[str, dict[str, str | None]] = {
    "CME": {
        "open": "17:00:00",
        "close": "16:00:00",  # 5pm-4pm CT (spans midnight)
        "tz": "America/Chicago",
        "calendar": "CME",
        "auction_open": None,
        "auction_close": None,
    },
    "KRX": {
        # Korea Exchange regular session 09:00-15:30 KST (G1.e 2026-06-25). The XKRX
        # exchange_calendars feed supplies the Korean holiday set (Seollal / Chuseok / etc.)
        # so a Korean market holiday is recorded EXPECTED_HOLIDAY, not a false attempted_failed.
        "open": "09:00:00",
        "close": "15:30:00",
        "tz": "Asia/Seoul",
        "calendar": "KRX",
        "auction_open": "08:30:00",
        "auction_close": "15:20:00",
    },
    # ICE: declared pending the whole-venue retirement (databento billing-blocked, curated
    # enumeration already drops ICE instruments; sessions+registry+EU+GCS purge together).
    "ICE": {
        "open": "20:00:00",
        "close": "17:00:00",  # 8pm-5pm ET (spans midnight)
        "tz": "America/New_York",
        "calendar": "ICE",
        "auction_open": None,
        "auction_close": None,
    },
    "CBOE": {
        "open": "09:30:00",
        "close": "16:15:00",
        "tz": "America/New_York",
        "calendar": "CBOE",
        "auction_open": "09:28:00",
        "auction_close": "16:00:00",
    },
    "NASDAQ": {
        "open": "09:30:00",
        "close": "16:00:00",
        "tz": "America/New_York",
        "calendar": "NASDAQ",
        "auction_open": "09:28:00",
        "auction_close": "15:50:00",
        "pre_market_open": "04:00:00",
        "post_market_close": "20:00:00",
    },
    "NYSE": {
        "open": "09:30:00",
        "close": "16:00:00",
        "tz": "America/New_York",
        "calendar": "NYSE",
        "auction_open": "09:28:00",
        "auction_close": "15:50:00",
        "pre_market_open": "04:00:00",
        "post_market_close": "20:00:00",
    },
}

_XCAL_CACHE: dict[str, object] = {}

# FX is the DECLARED 24/7 exception (operator 2026-06-25): the Yahoo-sourced currency /
# conversion series (KRWUSD=X, DXY) have no exchange calendar — every day is an available
# day. Declared EXPLICITLY here, never reached by accident, so the fail-closed guard below
# can distinguish "legit 24/7" from "undeclared venue config error".
_FX_VENUES_24_7: frozenset[str] = frozenset({"FX"})


class UndeclaredTradfiVenueError(ValueError):
    """A tradfi venue was queried that is not declared in the session/calendar SSOT.

    Fail-CLOSED (G1.e, 2026-06-25): the old behaviour silently defaulted an undeclared venue
    to 24/7-trading (so KRX was treated 24/7 and its Korean holidays mis-handled). An
    undeclared tradfi venue is a G1 CONFIG ERROR — declare it in ``_EXCHANGE_HOURS`` +
    ``_XCAL_MAPPING`` (or as a 24/7 FX venue), never silently assume it trades every day.
    """


def _get_xcal(calendar_name: str) -> object | None:
    """Get exchange calendar instance (cached)."""
    if calendar_name in _db._XCAL_CACHE:
        return _db._XCAL_CACHE[calendar_name]
    xcal_code = _db._XCAL_MAPPING.get(calendar_name)
    if not xcal_code:
        return None
    try:
        cal = _db.xcals.get_calendar(xcal_code)
        _db._XCAL_CACHE[calendar_name] = cal
        return cal
    except Exception as _exc:
        return None


def _is_trading_holiday(target: date, calendar_name: str) -> bool:
    """Check if a weekday is a market holiday using exchange_calendars.

    Only checks holidays for Mon-Fri. Weekends are handled separately
    by the caller (CME/ICE open Sunday evening, equities closed all weekend).
    """
    if target.weekday() >= 5:  # Weekend — not a "holiday", handled by caller
        return False
    cal = _db._get_xcal(calendar_name)
    if cal is None:
        return False
    try:
        ts = pd.Timestamp(target)
        return not cal.is_session(ts)
    except Exception as _exc:
        return False


def _resolve_trading_status(venue: str, target_date: date, is_holiday: bool) -> tuple[bool, str]:
    """Determine if a date is a trading day and its session label.

    Returns (is_trading, session_label).
    """
    weekday = target_date.weekday()  # 0=Mon, 5=Sat, 6=Sun
    is_saturday = weekday == 5
    is_sunday = weekday == 6
    # Sunday-evening-open futures venues. CBOE/VX + KRX equities are closed weekends;
    # FX is handled 24/7 upstream in is_non_trading_day. (ICE pending whole-venue retirement.)
    futures_venues = {"CME", "ICE"}

    if is_saturday:
        is_trading = False
    elif is_sunday:
        is_trading = venue in futures_venues and not is_holiday
    else:
        is_trading = not is_holiday

    if is_holiday:
        session_label = "holiday"
    elif is_saturday:
        session_label = "weekend"
    elif is_sunday and venue in futures_venues:
        session_label = "sunday_open"
    elif is_sunday:
        session_label = "weekend"
    else:
        session_label = "regular"

    return is_trading, session_label


def _non_trading_result(session_label: str, calendar_name: str) -> dict[str, str | bool | None]:
    """Build session metadata dict for a non-trading day."""
    return {
        "trading_session": session_label,
        "is_trading_day": False,
        "holiday_calendar": calendar_name,
        "trading_hours_open": None,
        "trading_hours_close": None,
        "regular_open_utc": None,
        "regular_close_utc": None,
        "auction_open_utc": None,
        "auction_close_utc": None,
        "early_close_utc": None,
    }


def non_trading_day_reason(venue: str, target_date: date) -> str | None:
    """Return the EXPECTED_* reason for a non-trading day, or None if trading.

    Discriminates ``EXPECTED_WEEKEND`` (Sat/Sun for closed-on-weekends venues,
    plus Sat for everyone) from ``EXPECTED_HOLIDAY`` (weekday session marked
    closed by the venue's exchange_calendars). Sunday for CME/ICE futures is a
    trading day (Sunday-evening open) and returns ``None``.

    Used by orchestrator pre-skip sites to feed
    ``ManifestWriter.record_expected_empty(reason=...)`` per writegate Phase
    2.E.2 so the manifest carries an EXPECTED_* row for every (shard_key, day)
    in the expected universe instead of a bare "no row at all."
    """
    if not _db.is_non_trading_day(venue, target_date):
        return None
    if target_date.weekday() >= 5:  # Sat/Sun (Sunday for futures already filtered above)
        return "EXPECTED_WEEKEND"
    return "EXPECTED_HOLIDAY"


def is_non_trading_day(venue: str, target_date: date) -> bool:
    """Check whether the given date is a non-trading day for a TradFi venue.

    Uses exchange_calendars for holiday detection and venue-specific weekend
    rules (CME/ICE open Sunday evening; equities closed all weekend).

    This is the public interface used by the orchestrator to decide whether
    zero instruments from Databento is expected (non-trading day) vs an error.

    FAIL-CLOSED (G1.e, 2026-06-25): FX is the declared 24/7 exception (always trading);
    any other venue NOT declared in the calendar SSOT raises ``UndeclaredTradfiVenueError``
    instead of silently defaulting to 24/7-trading (the old ``return False`` mis-handled KRX).
    """
    if venue in _db._FX_VENUES_24_7:
        return False  # FX: declared 24/7 — every day is an available (trading) day.
    cfg = _db._EXCHANGE_HOURS.get(venue)
    if cfg is None:
        raise _db.UndeclaredTradfiVenueError(
            f"is_non_trading_day: tradfi venue {venue!r} is not declared in the session/calendar "
            f"SSOT (_EXCHANGE_HOURS / _XCAL_MAPPING). Declare it (or as a 24/7 FX venue) — an "
            f"undeclared tradfi venue must never silently default to 24/7-trading."
        )
    calendar_name = cfg.get("calendar", venue)
    is_holiday = _db._is_trading_holiday(target_date, calendar_name)
    is_trading, _label = _db._resolve_trading_status(venue, target_date, is_holiday)
    return not is_trading


def _compute_utc_hours(
    cfg: dict[str, str | None],
    target_date: date,
    calendar_name: str,
    venue: str,
    result: dict[str, str | bool | None],
) -> None:
    """Convert local trading hours to UTC and populate result dict in-place."""
    try:
        tz = ZoneInfo(cfg["tz"])
        open_parts = [int(x) for x in cfg["open"].split(":")]
        close_parts = [int(x) for x in cfg["close"].split(":")]

        open_seconds = open_parts[0] * 3600 + open_parts[1] * 60 + open_parts[2]
        close_seconds = close_parts[0] * 3600 + close_parts[1] * 60 + close_parts[2]

        # If open > close, session starts previous calendar day (CME, ICE)
        open_date = target_date - timedelta(days=1) if open_seconds > close_seconds else target_date

        open_local = datetime(
            open_date.year,
            open_date.month,
            open_date.day,
            open_parts[0],
            open_parts[1],
            open_parts[2],
            tzinfo=tz,
        )
        close_local = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            close_parts[0],
            close_parts[1],
            close_parts[2],
            tzinfo=tz,
        )

        open_utc = open_local.astimezone(UTC)
        close_utc = close_local.astimezone(UTC)

        result["trading_hours_open"] = open_utc.strftime("%H:%M:%S+00:00")
        result["trading_hours_close"] = close_utc.strftime("%H:%M:%S+00:00")
        result["regular_open_utc"] = open_utc.isoformat()
        result["regular_close_utc"] = close_utc.isoformat()

        # Auction times + pre/post market
        for field, cfg_key in [
            ("auction_open_utc", "auction_open"),
            ("auction_close_utc", "auction_close"),
            ("pre_market_open_utc", "pre_market_open"),
            ("post_market_close_utc", "post_market_close"),
        ]:
            local_str = cfg.get(cfg_key)
            if local_str:
                parts = [int(x) for x in local_str.split(":")]
                local_dt = datetime(
                    target_date.year,
                    target_date.month,
                    target_date.day,
                    parts[0],
                    parts[1],
                    parts[2],
                    tzinfo=tz,
                )
                result[field] = local_dt.astimezone(UTC).isoformat()

        _db._apply_early_close(calendar_name, target_date, venue, result)

    except Exception as _exc:
        _db.logger.warning("Failed to compute session hours for %s on %s: %s", venue, target_date, _exc)


def _apply_early_close(
    calendar_name: str,
    target_date: date,
    venue: str,
    result: dict[str, str | bool | None],
) -> None:
    """Check for early close via exchange_calendars and update result in-place."""
    cal = _db._get_xcal(calendar_name)
    if cal is None:
        return
    try:
        ts = pd.Timestamp(target_date)
        if hasattr(cal, "early_closes") and ts in cal.early_closes and ts in cal.schedule.index:
            actual_close = cal.schedule.loc[ts, "close"]
            if pd.notna(actual_close):
                early_dt = actual_close.to_pydatetime()
                early_dt = early_dt.replace(tzinfo=UTC) if early_dt.tzinfo is None else early_dt.astimezone(UTC)
                result["early_close_utc"] = early_dt.isoformat()
                result["regular_close_utc"] = early_dt.isoformat()
                result["trading_hours_close"] = early_dt.strftime("%H:%M:%S+00:00")
    except Exception as _exc:
        _db.logger.debug("Early close check failed for %s on %s: %s", venue, target_date, _exc)


def _get_session_metadata(venue: str, target_date: date) -> dict[str, str | bool | None]:
    """Compute DST-aware trading hours for a TradFi venue on a specific date.

    Returns a dict with keys matching InstrumentRecord session fields.
    """
    # FX: declared 24/7 (operator 2026-06-25) — currency/conversion series have no exchange
    # session, so mark every day a regular trading day (no holiday/early-close enrichment).
    if venue in _db._FX_VENUES_24_7:
        return {"trading_session": "regular", "is_trading_day": True, "holiday_calendar": "FX"}
    cfg = _db._EXCHANGE_HOURS.get(venue)
    if cfg is None:
        return {}

    calendar_name = cfg.get("calendar", venue)
    is_holiday = _db._is_trading_holiday(target_date, calendar_name)
    is_trading, session_label = _db._resolve_trading_status(venue, target_date, is_holiday)

    if not is_trading:
        return _db._non_trading_result(session_label, calendar_name)

    result: dict[str, str | bool | None] = {
        "trading_session": session_label,
        "is_trading_day": is_trading,
        "holiday_calendar": calendar_name,
    }

    _db._compute_utc_hours(cfg, target_date, calendar_name, venue, result)

    return result


# Public aliases so sibling TradFi adapters (e.g. Massive) reuse the SAME
# trading-hours/holiday session-metadata logic instead of duplicating it —
# single SSOT for TradFi session enrichment across reference-data sources.
EXCHANGE_HOURS = _EXCHANGE_HOURS
get_session_metadata = _get_session_metadata
