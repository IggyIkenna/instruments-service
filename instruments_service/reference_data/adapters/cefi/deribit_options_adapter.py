"""Deribit options-chain reference data adapter — Deribit public REST (no creds).

Enumerates active option instruments (strikes x expiries) for an underlying via
``GET /api/v2/public/get_instruments?currency=<ccy>&kind=option&expired=false``
and attaches the venue-published mark IV via
``GET /api/v2/public/ticker?instrument_name=<name>`` (``mark_iv`` is a percent,
e.g. ``62.5`` → ``0.625`` fractional). Greeks are NOT pulled — they are computed
in-house by greeks-service. Live (real-time) chain source for the VOL_* family.

API doc: https://docs.deribit.com/#public-get_instruments (kind=option) +
https://docs.deribit.com/#public-ticker
Base URL: https://www.deribit.com/api/v2
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import aiohttp
from unified_api_contracts import UnsupportedCapabilityError, classify_venue_error
from unified_api_contracts._instrument_enums import OptionType
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import CanonicalExpiryCalendar, CanonicalOptionsChain, FundingRateRef, OHLCVRef

logger = logging.getLogger(__name__)

_BASE = "https://www.deribit.com/api/v2"
_DERIBIT_OPTION_UNDERLYINGS: frozenset[str] = frozenset({"BTC", "ETH", "SOL", "BNB", "XRP"})
_TICKER_CONCURRENCY = 16


def _classify_deribit_error(exc: Exception, status: int | None = None) -> str:
    """Map a Deribit HTTP/network error to a UAC error code."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "429"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    if (status is not None and status >= 500) or "500" in msg or "internal" in msg:
        return "500"
    return "UNKNOWN"


def _emit_fetch_failed(endpoint: str, currency: str, error_code: str, exc: Exception) -> None:
    """Classify via UAC + emit ADAPTER_FETCH_FAILED (shard-level isolation)."""
    classification = classify_venue_error("deribit", error_code)
    action = classification.action.value if classification else "fail"
    retry_safe = classification.retry_safe if classification else False
    logger.error(
        "Deribit options %s currency=%s failed: %s (code=%s action=%s retry_safe=%s)",
        endpoint,
        currency,
        exc,
        error_code,
        action,
        retry_safe,
    )
    log_event(
        "ADAPTER_FETCH_FAILED",
        details={
            "venue": "deribit",
            "adapter": "DeribitOptionsReferenceDataAdapter",
            "endpoint": endpoint,
            "currency": currency,
            "error": str(exc),
            "error_code": error_code,
            "action": action,
            "retry_safe": retry_safe,
        },
    )


def _parse_option(item: object) -> InstrumentRecord | None:
    """Parse one Deribit ``kind=option`` instrument dict into an InstrumentRecord."""
    if not isinstance(item, dict):
        return None
    name = str(item.get("instrument_name", ""))
    if not name:
        return None
    base = str(item.get("base_currency", name.split("-")[0]))
    try:
        strike = Decimal(str(item["strike"]))
    except (KeyError, InvalidOperation, ValueError):
        return None
    opt = OptionType.CALL if str(item.get("option_type", "")).lower() == "call" else OptionType.PUT
    try:
        expiry = datetime.fromtimestamp(int(item.get("expiration_timestamp", 0)) / 1000, tz=UTC)
    except (ValueError, OSError, OverflowError):
        return None
    return InstrumentRecord(
        instrument_key=f"DERIBIT:OPTION:{name}",
        venue="DERIBIT",
        raw_symbol=name,
        instrument_type=InstrumentType.OPTION,
        base_asset=base,
        quote_asset="USD",
        settle_asset=str(item.get("settlement_currency", base)),
        underlying=base,
        status=InstrumentStatus.ACTIVE,
        strike=strike,
        option_type=opt,
        expiry=expiry,
        contract_size=Decimal("1"),
        timezone="UTC",
    )


class DeribitOptionsReferenceDataAdapter(BaseReferenceDataAdapter):
    """Deribit options-chain adapter (live, public REST — no API key)."""

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "DERIBIT"

    async def get_instruments(self, instrument_type: str | None = None) -> list[InstrumentRecord]:
        """Fetch active OPTION instruments across all supported underlyings."""
        if instrument_type is not None and instrument_type != InstrumentType.OPTION:
            raise UnsupportedCapabilityError(
                venue="DERIBIT",
                capability=str(instrument_type),
                message="DeribitOptionsReferenceDataAdapter fetches OPTION instruments only.",
            )
        records: list[InstrumentRecord] = []
        failures: list[str] = []
        async with self._make_session() as session:
            for currency in sorted(_DERIBIT_OPTION_UNDERLYINGS):
                try:
                    records.extend(await self._fetch_options_for_currency(session, currency))
                except RuntimeError:
                    failures.append(currency)  # already classified + emitted
        if not records and failures:
            raise RuntimeError(f"Deribit options: all {len(failures)} currenc(ies) failed ({failures})")
        return records

    async def _fetch_options_for_currency(
        self, session: aiohttp.ClientSession, currency: str
    ) -> list[InstrumentRecord]:
        """Fetch + parse the option instrument list for a single currency."""
        url = f"{_BASE}/public/get_instruments"
        params = {"currency": currency, "kind": "option", "expired": "false"}
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                payload: object = await resp.json()
        except aiohttp.ClientError as exc:
            code = _classify_deribit_error(exc, getattr(exc, "status", None))
            _emit_fetch_failed("get_instruments", currency, code, exc)
            raise RuntimeError(f"Deribit options fetch failed currency={currency} code={code}") from exc
        result_raw: object = payload.get("result", []) if isinstance(payload, dict) else []
        items: list[object] = result_raw if isinstance(result_raw, list) else []
        parsed = [_parse_option(item) for item in items]
        return [r for r in parsed if r is not None]

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single option instrument by venue-native symbol."""
        for inst in await self.get_instruments(instrument_type=InstrumentType.OPTION):
            if inst.raw_symbol == symbol or inst.instrument_key == symbol:
                return inst
        return None

    async def get_options_chain(
        self, underlying: str, expiry: datetime | None = None
    ) -> CanonicalOptionsChain:
        """Build a CanonicalOptionsChain (strikes, calls, puts, mark IV) for one expiry.

        ``expiry=None`` → the nearest (soonest) expiry with active contracts.
        Mark IV is pulled per-leg from Deribit's public ticker and stored fractional.
        """
        ccy = underlying.upper()
        async with self._make_session() as session:
            all_opts = await self._fetch_options_for_currency(session, ccy)
            by_underlying = [o for o in all_opts if o.underlying == ccy and o.expiry is not None]
            if not by_underlying:
                return CanonicalOptionsChain(venue="DERIBIT", underlying=ccy, expiry=expiry or datetime.now(UTC))
            chosen = expiry or min(o.expiry for o in by_underlying if o.expiry is not None)
            legs = [o for o in by_underlying if o.expiry == chosen]
            calls = sorted([o for o in legs if o.option_type == OptionType.CALL], key=lambda o: o.strike or Decimal(0))
            puts = sorted([o for o in legs if o.option_type == OptionType.PUT], key=lambda o: o.strike or Decimal(0))
            strikes = sorted({o.strike for o in legs if o.strike is not None})
            mark_iv, underlying_mark = await self._fetch_mark_iv(session, ccy, legs)
        return CanonicalOptionsChain(
            venue="DERIBIT",
            underlying=ccy,
            expiry=chosen,
            strikes=strikes,
            calls=calls,
            puts=puts,
            mark_iv_by_instrument=mark_iv,
            underlying_mark=underlying_mark,
        )

    async def _fetch_mark_iv(
        self, session: aiohttp.ClientSession, currency: str, legs: list[InstrumentRecord]
    ) -> tuple[dict[str, Decimal], Decimal | None]:
        """Pull per-leg mark IV (fractional) + the underlying index price via public ticker."""
        sem = asyncio.Semaphore(_TICKER_CONCURRENCY)
        mark_iv: dict[str, Decimal] = {}
        underlying_mark: Decimal | None = None

        async def _one(leg: InstrumentRecord) -> None:
            nonlocal underlying_mark
            async with sem:
                try:
                    async with session.get(
                        f"{_BASE}/public/ticker", params={"instrument_name": leg.raw_symbol}
                    ) as resp:
                        resp.raise_for_status()
                        body: object = await resp.json()
                except aiohttp.ClientError as exc:
                    code = _classify_deribit_error(exc, getattr(exc, "status", None))
                    _emit_fetch_failed("ticker", currency, code, exc)
                    return  # shard-level isolation: a single leg's failure must not kill the chain
                res_raw: object = body.get("result", {}) if isinstance(body, dict) else {}
                if not isinstance(res_raw, dict):
                    return
                res: dict[str, object] = res_raw
                raw_iv: object = res.get("mark_iv")
                if raw_iv is not None:
                    with contextlib.suppress(InvalidOperation, ValueError):
                        mark_iv[leg.instrument_key] = Decimal(str(raw_iv)) / Decimal(100)
                raw_index: object = res.get("index_price")
                if underlying_mark is None and raw_index is not None:
                    with contextlib.suppress(InvalidOperation, ValueError):
                        underlying_mark = Decimal(str(raw_index))

        await asyncio.gather(*[_one(leg) for leg in legs])
        return mark_iv, underlying_mark

    async def get_expiry_calendar(
        self, underlying: str, instrument_type: str = "OPTION"
    ) -> CanonicalExpiryCalendar:
        """Return the distinct option expiries for an underlying."""
        async with self._make_session() as session:
            opts = await self._fetch_options_for_currency(session, underlying.upper())
        expiries = sorted({o.expiry for o in opts if o.expiry is not None})
        return CanonicalExpiryCalendar(
            venue="DERIBIT", instrument_type="OPTION", underlying=underlying.upper(), expiries=expiries
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Not supported — options do not carry funding rates."""
        raise UnsupportedCapabilityError(venue="DERIBIT", capability="funding_rate", message="Options have no funding.")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        """Not supported — use the Tardis adapter for option price history."""
        raise UnsupportedCapabilityError(venue="DERIBIT", capability="ohlcv", message="Use Tardis for option OHLCV.")
