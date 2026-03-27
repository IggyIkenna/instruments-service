"""Databento reference data adapter — institutional market data provider.

Databento provides normalized historical and reference data for equities, futures, options.
API key required: store in Secret Manager as databento-api-key.

Fetches ONLY the curated instruments declared in UAC's TRADFI_DATABENTO_INSTRUMENTS
registry (futures/options via ``stype_in=parent``) plus S&P 500 / ETF equities from
TRADFI_TICKER_UNIVERSE (via ``stype_in=raw_symbol``).  This avoids dumping the entire
Databento dataset (millions of rows) and returns only the ~600 instruments the system
actually trades.

FX spot pairs (KRW/USD etc.) are created as static InstrumentRecords from
UAC's FX_SPOT_PAIRS — they don't come from Databento.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import databento as db
import exchange_calendars as xcals
import pandas as pd
from unified_api_contracts import (
    FX_SPOT_PAIRS,
    KNOWN_ETFS,
    TRADFI_DATABENTO_INSTRUMENTS,
    TRADFI_TICKER_UNIVERSE,
    VenueMapping,
    classify_venue_error,
)
from unified_api_contracts.internal import AssetClass, InstrumentRecord
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Databento instrument_class → URDI instrument_type
_CLASS_TO_TYPE: dict[str, str] = {
    "B": "spot",  # Bond
    "C": "option",  # Call option (CME/ICE)
    "E": "spot",  # Equity
    "F": "future",  # Future
    "K": "spot",  # Forex spot
    "M": "future",  # Monthly future (CME)
    "N": "spot",  # ETF / Fund / Index
    "O": "option",  # Option (generic)
    "P": "option",  # Put option (CME/ICE)
    "S": "spot",  # FX Spot / Equity spot
    "T": "spread",  # Spread / Calendar spread
    "X": "spot",  # Index
}

# Dataset → canonical venue mapping
_DATASET_TO_VENUE: dict[str, str] = {
    "GLBX.MDP3": "CME",
    "XNAS.ITCH": "NASDAQ",
    "XNAS.BASIC": "NASDAQ",
    "XNYS.PILLAR": "NYSE",
    "DBEQ.BASIC": "NYSE",
    "IFEU.IMPACT": "ICE",
    "IFUS.IMPACT": "ICE",
    "OPRA.PILLAR": "CBOE",
    "XCBF.PITCH": "CBOE",
}

# Dataset → fallback asset class (used when no per-instrument match exists)
_DATASET_TO_ASSET_CLASS: dict[str, AssetClass] = {
    "GLBX.MDP3": AssetClass.COMMODITY,
    "XNAS.ITCH": AssetClass.EQUITY,
    "XNAS.BASIC": AssetClass.EQUITY,
    "XNYS.PILLAR": AssetClass.EQUITY,
    "DBEQ.BASIC": AssetClass.EQUITY,
    "IFEU.IMPACT": AssetClass.COMMODITY,
    "IFUS.IMPACT": AssetClass.COMMODITY,
    "OPRA.PILLAR": AssetClass.EQUITY,
    "XCBF.PITCH": AssetClass.EQUITY,
}

# Per-instrument asset class from UAC registry.
# Maps exchange_code → asset_class (e.g. "ES" → equity, "CL" → commodity).
_EXCHANGE_CODE_ASSET_CLASS: dict[str, str] = {
    inst.exchange_code: inst.asset_class for inst in TRADFI_DATABENTO_INSTRUMENTS if inst.exchange_code
}

_VENUE_MAPPING = VenueMapping()

# ---------------------------------------------------------------------------
# Exchange calendar / trading hours enrichment
# ---------------------------------------------------------------------------
_XCAL_MAPPING: dict[str, str] = {
    "NASDAQ": "XNAS",
    "NYSE": "XNYS",
    "CME": "CMES",
    "CBOE": "XNYS",  # CBOE equity products follow NYSE calendar
    "ICE": "XNYS",  # ICE US follows NYSE calendar
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
    },
    "NYSE": {
        "open": "09:30:00",
        "close": "16:00:00",
        "tz": "America/New_York",
        "calendar": "NYSE",
        "auction_open": "09:28:00",
        "auction_close": "15:50:00",
    },
}

_XCAL_CACHE: dict[str, object] = {}


def _get_xcal(calendar_name: str) -> object | None:
    """Get exchange calendar instance (cached)."""
    if calendar_name in _XCAL_CACHE:
        return _XCAL_CACHE[calendar_name]
    xcal_code = _XCAL_MAPPING.get(calendar_name)
    if not xcal_code:
        return None
    try:
        cal = xcals.get_calendar(xcal_code)
        _XCAL_CACHE[calendar_name] = cal
        return cal
    except Exception:
        return None


def _is_trading_holiday(target: date, calendar_name: str) -> bool:
    """Check if a weekday is a market holiday using exchange_calendars.

    Only checks holidays for Mon-Fri. Weekends are handled separately
    by the caller (CME/ICE open Sunday evening, equities closed all weekend).
    """
    if target.weekday() >= 5:  # Weekend — not a "holiday", handled by caller
        return False
    cal = _get_xcal(calendar_name)
    if cal is None:
        return False
    try:
        ts = pd.Timestamp(target)
        return not cal.is_session(ts)
    except Exception:
        return False


def _resolve_trading_status(venue: str, target_date: date, is_holiday: bool) -> tuple[bool, str]:
    """Determine if a date is a trading day and its session label.

    Returns (is_trading, session_label).
    """
    weekday = target_date.weekday()  # 0=Mon, 5=Sat, 6=Sun
    is_saturday = weekday == 5
    is_sunday = weekday == 6
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

        # Auction times
        for field, cfg_key in [
            ("auction_open_utc", "auction_open"),
            ("auction_close_utc", "auction_close"),
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

        _apply_early_close(calendar_name, target_date, venue, result)

    except Exception as exc:
        logger.warning("Failed to compute session hours for %s on %s: %s", venue, target_date, exc)


def _apply_early_close(
    calendar_name: str,
    target_date: date,
    venue: str,
    result: dict[str, str | bool | None],
) -> None:
    """Check for early close via exchange_calendars and update result in-place."""
    cal = _get_xcal(calendar_name)
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
    except Exception as exc:
        logger.debug("Early close check failed for %s on %s: %s", venue, target_date, exc)


def _get_session_metadata(venue: str, target_date: date) -> dict[str, str | bool | None]:
    """Compute DST-aware trading hours for a TradFi venue on a specific date.

    Returns a dict with keys matching InstrumentRecord session fields.
    """
    cfg = _EXCHANGE_HOURS.get(venue)
    if cfg is None:
        return {}

    calendar_name = cfg.get("calendar", venue)
    is_holiday = _is_trading_holiday(target_date, calendar_name)
    is_trading, session_label = _resolve_trading_status(venue, target_date, is_holiday)

    if not is_trading:
        return _non_trading_result(session_label, calendar_name)

    result: dict[str, str | bool | None] = {
        "trading_session": session_label,
        "is_trading_day": is_trading,
        "holiday_calendar": calendar_name,
    }

    _compute_utc_hours(cfg, target_date, calendar_name, venue, result)

    return result


def _classify_bento_error(exc: db.common.error.BentoError) -> str:
    """Map a Databento SDK error to a UAC error code for classification."""
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "401" in msg or "auth" in msg or "unauthorized" in msg:
        return "AUTH_FAILURE"
    if "connection" in msg or "reset" in msg or "timeout" in msg:
        return "CONNECTION_RESET"
    if "422" in msg:
        return "VALIDATION_ERROR"
    if "404" in msg or "not found" in msg:
        return "NOT_FOUND"
    if "500" in msg or "internal" in msg:
        return "SERVER_ERROR"
    return "UNKNOWN"


class DatabentoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Databento reference data adapter using the official SDK.

    Fetches curated instruments from TRADFI_DATABENTO_INSTRUMENTS (UAC registry)
    using symbol-level queries (stype_in=parent for futures/options), plus
    S&P 500 / ETF equities from TRADFI_TICKER_UNIVERSE.

    FX spot pairs are static InstrumentRecords (not from Databento).
    """

    def __init__(
        self,
        project_id: str | None = None,
        datasets: list[str] | None = None,
        target_date: date | None = None,
        api_key: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._target_date: date = target_date or date.today()

    @property
    def venue(self) -> str:
        return "databento"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        api_key = self._optional_api_key()
        if api_key is None:
            raise ValueError(
                "api_key required — service must fetch databento-api-key from "
                "Secret Manager and pass it via api_key= constructor parameter."
            )

        results: list[InstrumentRecord] = []

        # 1. Fetch curated futures/options from TRADFI_DATABENTO_INSTRUMENTS
        #    Grouped by (dataset, stype_in) to batch API calls.
        groups: dict[tuple[str, str], list[str]] = {}
        for inst_def in TRADFI_DATABENTO_INSTRUMENTS:
            key = (inst_def.dataset, inst_def.stype_in)
            groups.setdefault(key, []).append(inst_def.symbol)

        for (dataset, stype_in), symbols in groups.items():
            batch = self._fetch_symbols(api_key, dataset, symbols, stype_in)
            results.extend(batch)

        # 2. Fetch S&P 500 equities + ETFs via DBEQ.BASIC / raw_symbol
        equity_symbols = self._get_equity_symbols()
        if equity_symbols:
            batch = self._fetch_symbols(api_key, "DBEQ.BASIC", equity_symbols, "raw_symbol")
            results.extend(batch)

        # 3. Static FX spot pairs (not from Databento)
        results.extend(self._create_fx_spot_records())

        # 4. Enrich with session metadata (trading hours, holidays, early closes)
        self._enrich_session_metadata(results)

        if instrument_type is not None:
            results = [r for r in results if r.instrument_type == instrument_type]

        logger.info(
            "Databento adapter: %d total instruments (%d futures/options, equities, FX)",
            len(results),
            len(results),
        )
        return results

    def _enrich_session_metadata(self, results: list[InstrumentRecord]) -> None:
        """Enrich records with session metadata (trading hours, holidays, early closes).

        Computed once per venue, then applied to all records for that venue.
        """
        session_cache: dict[str, dict[str, str | bool | None]] = {}
        for record in results:
            venue = record.venue
            if venue not in session_cache:
                session_cache[venue] = _get_session_metadata(venue, self._target_date)
            meta = session_cache[venue]
            if meta:
                record.is_trading_day = meta.get("is_trading_day")
                record.regular_open_utc = meta.get("regular_open_utc")
                record.regular_close_utc = meta.get("regular_close_utc")
                record.early_close_utc = meta.get("early_close_utc")

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
        instruments = await self.get_instruments(instrument_type="option")
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            und = inst.underlying or inst.base_asset or ""
            if underlying.upper() not in und.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.option_type and inst.option_type.upper() in ("C", "CALL"):
                calls.append(inst)
            elif inst.option_type and inst.option_type.upper() in ("P", "PUT"):
                puts.append(inst)
            if inst.strike is not None:
                strikes.add(inst.strike)
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
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        instruments = await self.get_instruments(instrument_type=instrument_type)
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
        raise NotImplementedError("Databento does not provide funding rates (equity/futures only)")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError(
            "Databento OHLCV requires timeseries.get_range with DBN binary format. "
            "Use the databento Python SDK directly for this operation."
        )

    # ------------------------------------------------------------------
    # Private: fetch helpers
    # ------------------------------------------------------------------

    def _get_equity_symbols(self) -> list[str]:
        """Build the equity symbol list from TRADFI_TICKER_UNIVERSE."""
        sp500 = TRADFI_TICKER_UNIVERSE.get("sp500_tickers", [])
        etfs = TRADFI_TICKER_UNIVERSE.get("etf_tickers", [])
        # Deduplicate, preserving order
        seen: set[str] = set()
        symbols: list[str] = []
        for s in [*sp500, *etfs]:
            if s not in seen:
                seen.add(s)
                symbols.append(s)
        return symbols

    def _fetch_symbols(
        self,
        api_key: str,
        dataset: str,
        symbols: list[str],
        stype_in: str,
    ) -> list[InstrumentRecord]:
        """Fetch specific symbols from a Databento dataset.

        Uses timeseries.get_range(schema=DEFINITION, symbols=..., stype_in=...)
        to fetch only the requested instruments instead of the entire dataset.
        """
        client = db.Historical(api_key)
        target = self._target_date
        # Databento has T+2 embargo — cap the query date to 3 days before today
        today = date.today()
        effective_date = min(target, today - timedelta(days=3))
        start = datetime(effective_date.year, effective_date.month, effective_date.day, tzinfo=UTC)
        end = start + timedelta(days=1)

        try:
            data = client.timeseries.get_range(
                dataset=dataset,
                schema="definition",
                symbols=symbols,
                stype_in=stype_in,
                start=start.isoformat(),
                end=end.isoformat(),
            )
        except db.common.error.BentoError as exc:
            error_code = _classify_bento_error(exc)
            classification = classify_venue_error("DATABENTO", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Databento SDK error dataset %s symbols=%d: %s (classified: %s, action: %s)",
                dataset,
                len(symbols),
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "DATABENTO",
                    "dataset": dataset,
                    "symbol_count": len(symbols),
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        try:
            df = data.to_df()
        except Exception as exc:
            logger.warning("Failed to parse Databento DBN data for %s: %s", dataset, exc)
            return []

        if df.empty:
            logger.info(
                "No instrument definitions found in %s for %d symbols on %s",
                dataset,
                len(symbols),
                target,
            )
            return []

        logger.info(
            "Fetched %d instrument definitions from %s (%d symbols requested)",
            len(df),
            dataset,
            len(symbols),
        )
        canonical_venue = _DATASET_TO_VENUE.get(dataset, dataset)
        results: list[InstrumentRecord] = []
        seen_symbols: set[str] = set()

        for _, row in df.iterrows():
            record = self._parse_row_to_record(row, dataset, canonical_venue)
            if record is None:
                continue
            # Deduplicate equities: DBEQ.BASIC returns multiple rows per symbol
            # (one per exchange listing). Keep first occurrence only.
            if stype_in == "raw_symbol" and record.raw_symbol in seen_symbols:
                continue
            seen_symbols.add(record.raw_symbol)
            results.append(record)
        return results

    def _create_fx_spot_records(self) -> list[InstrumentRecord]:
        """Create static InstrumentRecords for FX spot pairs (Yahoo Finance data)."""
        records: list[InstrumentRecord] = []
        for fx in FX_SPOT_PAIRS:
            symbol = f"{fx.base}-{fx.quote}"
            records.append(
                InstrumentRecord(
                    instrument_key=f"FX:SPOT:{symbol}",
                    venue="FX",
                    asset_class=AssetClass.FX,
                    instrument_type="spot",
                    raw_symbol=fx.yahoo_ticker,
                    base_asset=fx.base,
                    quote_asset=fx.quote,
                    tick_size=Decimal("0.0001"),
                    lot_size=Decimal("1"),
                    contract_size=Decimal("1"),
                    available_since=datetime(2020, 1, 1, tzinfo=UTC),
                )
            )
        return records

    # ------------------------------------------------------------------
    # Private: parsing
    # ------------------------------------------------------------------

    def _parse_tick_and_lot(self, row: object) -> tuple[Decimal, Decimal]:
        """Extract tick size and lot size from a DataFrame row."""
        tick_raw = getattr(row, "min_price_increment", None)
        try:
            tick_val = Decimal(str(tick_raw)) if tick_raw else Decimal("0.01")
            tick_size = tick_val if tick_val.is_finite() and tick_val > 0 else Decimal("0.01")
        except Exception:
            tick_size = Decimal("0.01")
        lot_raw = getattr(row, "min_lot_size_round_lot", None)
        try:
            lot_val = Decimal(str(lot_raw)) if lot_raw else Decimal("1")
            lot_size = lot_val if lot_val.is_finite() and lot_val > 0 else Decimal("1")
        except Exception:
            lot_size = Decimal("1")
        return tick_size, lot_size

    @staticmethod
    def _parse_expiry_from_row(row: object) -> datetime | None:
        """Parse expiry datetime from a DataFrame row."""
        expiry_raw = getattr(row, "expiration", None)
        if expiry_raw is None:
            return None
        with contextlib.suppress(ValueError, TypeError):
            return datetime.fromisoformat(str(expiry_raw).replace("Z", "+00:00")).astimezone(UTC)
        return None

    @staticmethod
    def _parse_strike_from_row(row: object) -> Decimal | None:
        """Parse strike price from a DataFrame row."""
        strike_raw = getattr(row, "strike_price", None)
        if strike_raw is None:
            return None
        try:
            val = Decimal(str(strike_raw))
            return val if val.is_finite() else None
        except Exception:
            return None

    @staticmethod
    def _parse_option_type_from_row(row: object, inst_class: str) -> str | None:
        """Parse option type from a DataFrame row, falling back to instrument_class."""
        option_type_raw = str(getattr(row, "option_type", "") or "").upper()
        if not option_type_raw and inst_class in ("C", "P"):
            option_type_raw = inst_class
        return option_type_raw or None

    def _is_filtered_out(self, dataset: str, inst_class: str, expiry: datetime | None) -> bool:
        """Check if a row should be filtered out (spreads, expired, too far out)."""
        # Filter spreads from futures datasets
        futures_datasets = {"GLBX.MDP3", "IFEU.IMPACT", "IFUS.IMPACT", "XCBF.PITCH"}
        if dataset in futures_datasets and inst_class in ("S", "T"):
            return True
        if expiry is not None and expiry.date() < self._target_date:
            return True
        max_expiry = self._target_date + timedelta(days=365)
        return expiry is not None and expiry.date() > max_expiry

    def _parse_row_to_record(
        self,
        row: object,
        dataset: str,
        canonical_venue: str,
    ) -> InstrumentRecord | None:
        """Parse a single DataFrame row into an InstrumentRecord."""
        raw_symbol = str(getattr(row, "raw_symbol", "") or getattr(row, "symbol", "") or "")
        if not raw_symbol:
            return None

        inst_class = str(getattr(row, "instrument_class", "E"))
        instrument_type = _CLASS_TO_TYPE.get(inst_class, "spot")
        currency = str(getattr(row, "currency", "USD") or "USD")

        expiry = self._parse_expiry_from_row(row)
        strike = self._parse_strike_from_row(row)
        option_type = self._parse_option_type_from_row(row, inst_class)
        underlying = str(getattr(row, "underlying", "") or "")
        tick_size, lot_size = self._parse_tick_and_lot(row)

        if self._is_filtered_out(dataset, inst_class, expiry):
            return None

        # Determine asset class from the UAC registry per-instrument, not per-dataset.
        # Build lookup: exchange_code → asset_class from the curated registry.
        asset_class = self._resolve_asset_class(dataset, raw_symbol, underlying)
        if dataset == "DBEQ.BASIC" and raw_symbol in KNOWN_ETFS:
            instrument_type = "etf"

        # For equity-venue instruments, route to correct canonical venue
        if dataset == "DBEQ.BASIC":
            nasdaq_tickers = set(TRADFI_TICKER_UNIVERSE.get("nasdaq_tickers", []))
            canonical_venue = "NASDAQ" if raw_symbol in nasdaq_tickers else "NYSE"

        return InstrumentRecord(
            instrument_key=f"{canonical_venue}:{instrument_type.upper()}:{raw_symbol}",
            venue=canonical_venue,
            asset_class=asset_class,
            raw_symbol=raw_symbol,
            instrument_type=instrument_type,
            base_asset=underlying or raw_symbol,
            quote_asset=currency,
            tick_size=tick_size,
            lot_size=lot_size,
            contract_size=Decimal("1"),
            expiry=expiry,
            strike=strike,
            option_type=option_type if option_type and option_type in ("C", "P") else None,
            underlying=underlying or None,
            available_since=None,
        )

    @staticmethod
    def _resolve_asset_class(dataset: str, raw_symbol: str, underlying: str) -> AssetClass:
        """Resolve asset_class per-instrument from the UAC registry.

        Checks the underlying (parent symbol from Databento, e.g. "ES", "CL", "6E")
        against the curated registry. Falls back to exchange code prefix extraction,
        then to dataset-level mapping.
        """
        # 1. Try underlying directly (best match for parent-stype queries)
        if underlying:
            ac = _EXCHANGE_CODE_ASSET_CLASS.get(underlying)
            if ac:
                return AssetClass(ac)

        # 2. Try known exchange code prefixes (longest match first)
        # Handles "ESM6" → "ES", "6EZ6" → "6E", "BTCM6" → "BTC"
        for length in (3, 2):
            if len(raw_symbol) >= length:
                prefix = raw_symbol[:length]
                ac = _EXCHANGE_CODE_ASSET_CLASS.get(prefix)
                if ac:
                    return AssetClass(ac)

        # 3. Fallback to dataset-level mapping
        return _DATASET_TO_ASSET_CLASS.get(dataset, AssetClass.EQUITY)
