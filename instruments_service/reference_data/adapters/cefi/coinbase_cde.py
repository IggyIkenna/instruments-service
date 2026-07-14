"""COINBASE-CDE reference data adapter — Coinbase Derivatives Exchange (CDE).

REST only, no auth needed for instrument discovery. Base URL:
https://api.coinbase.com (Advanced Trade public REST — same first-party
domain as COINBASE-SPOT, a DIFFERENT product from Coinbase International
Exchange/INTX which lives at api.international.coinbase.com and is served by
Tardis' ``coinbase-international`` endpoint under the SEPARATE COINBASE-FUTURES
venue).

Real, live-confirmed (2026-07-10, COINBASE-FUTURES/#3-vs-#8 resolution —
unified-trading-pm/plans/active/issues/instruments_remaining_work_audit_2026_07_10.md
Progress Log): ``GET /api/v3/brokerage/market/products?product_type=FUTURE``
returns 99 real, currently-trading contracts, ALL tagged
``future_product_details.venue == "cde"`` — dated futures (e.g.
``BIT-31JUL26-CDE``, expiry 2026-07-31) AND far-dated "nano perpetual" futures
(e.g. ``BIP-20DEC30-CDE``, expiry 2030-12-20, functionally a perpetual but
still classified FUTURE by Coinbase itself — no separate PERPETUAL product_type
exists on this endpoint). Every product carries a real, non-empty
``contract_root_unit`` (base asset — crypto AND non-crypto: gold/oil/copper/
"MAG7C" basket futures) + ``contract_expiry`` (ISO8601), all cash-settled in USD
(``quote_currency_id == "USD"``) — LINEAR margin. Zero Tardis coverage of this
venue under any name (confirmed against the full 62-exchange Tardis registry),
so this is a first-party-REST-only adapter — no batch/historical archive exists;
capture starts from live registration forward (see
market_data_categories.VENUE_DATA_TYPE_CAPABILITIES["COINBASE-CDE"]).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import cast

import aiohttp
from unified_api_contracts import UnsupportedCapabilityError, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Coinbase Advanced Trade public REST — first-party, no-auth for market data.
# Confirmed live 2026-07-10: product_type=FUTURE returns exactly the 99 CDE
# contracts, all future_product_details.venue == "cde".
_BASE = "https://api.coinbase.com"
_PRODUCTS_PATH = "/api/v3/brokerage/market/products"

# CDE is entirely USD cash-settled (quote_currency_id == "USD" on every one of
# the 99 live products confirmed 2026-07-10) — real margin_type is LINEAR, never
# coin-margined/INVERSE.
_MARGIN_TYPE = MarginType.LINEAR

# Zero Tardis/historical coverage for this venue (confirmed against the full
# 62-exchange Tardis registry) — capture starts from the date this venue was
# registered, not a fabricated pre-history. Mirrors
# market_data_categories.VENUE_DATA_TYPE_CAPABILITIES["COINBASE-CDE"]["trades"].
_CDE_REGISTRATION_DATE = datetime(2026, 7, 10, tzinfo=UTC)


def _classify_cde_error(exc: Exception, status: int | None = None) -> str:
    """Map a Coinbase Advanced Trade HTTP/network error to a UAC error code."""
    if status == 429:
        return "429"
    if status in (500, 502, 503, 504):
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "429"
    if "503" in msg or "unavailable" in msg:
        return "503"
    return "UNKNOWN"


def _parse_expiry(raw: object) -> datetime | None:
    """Parse a Coinbase ``contract_expiry`` ISO8601 string into a UTC datetime."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


class CoinbaseCdeReferenceDataAdapter(BaseReferenceDataAdapter):
    """Coinbase Derivatives Exchange (CDE) reference data: dated futures from
    Advanced Trade REST ``product_type=FUTURE``.

    ALL 99 real live products are InstrumentType.FUTURE — CDE has no
    SPOT_PAIR/PERPETUAL/OPTION product_type on this endpoint (confirmed live).
    """

    @property
    def venue(self) -> str:
        """Return the adapter-key venue identifier (lowercase, for logging/error classification)."""
        return "coinbase_cde"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active FUTURE instruments from Coinbase Advanced Trade REST."""
        # SPOT_PAIR/PERPETUAL/OPTION: not supported — CDE's Advanced Trade
        # listing has exactly one product_type (FUTURE); confirmed live 2026-07-10.
        if instrument_type in (InstrumentType.SPOT_PAIR, InstrumentType.PERPETUAL, InstrumentType.OPTION):
            raise UnsupportedCapabilityError(
                venue="COINBASE-CDE",
                capability=str(instrument_type),
                message=(f"COINBASE-CDE does not support {instrument_type} instruments. Only FUTURE is available."),
            )
        if instrument_type not in (None, InstrumentType.FUTURE):
            return []

        url = f"{_BASE}{_PRODUCTS_PATH}"
        params = {"product_type": "FUTURE", "limit": "250"}
        try:
            async with self._make_session() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = _classify_cde_error(exc)
            classification = classify_venue_error("coinbase_cde", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "COINBASE-CDE request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "coinbase_cde",
                    "endpoint": "products",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            # CF-11: re-raise so the venue threads STATE to _fetch_one's failed[]
            # (→ attempted_failed) instead of returning [] (clean empty → silent
            # universe shrink / A8 false-complete). Single-call adapter: this error
            # means the WHOLE COINBASE-CDE universe is unavailable.
            raise RuntimeError(
                f"COINBASE-CDE products fetch failed (error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc

        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        products_obj = raw_dict.get("products")
        products: list[object] = products_obj if isinstance(products_obj, list) else []
        results: list[InstrumentRecord] = []

        for prod_raw in products:
            if not isinstance(prod_raw, dict):
                continue
            prod = cast(dict[str, object], prod_raw)
            product_id = str(prod.get("product_id", ""))
            fpd_obj = prod.get("future_product_details")
            fpd: dict[str, object] = fpd_obj if isinstance(fpd_obj, dict) else {}
            # Only CDE-venue futures — the same products?product_type=FUTURE
            # endpoint is the sole real source, but filter defensively in case
            # Coinbase ever adds a non-CDE future_product_details.venue value.
            if fpd.get("venue") != "cde":
                continue
            base_asset = str(fpd.get("contract_root_unit", ""))
            quote_asset = str(prod.get("quote_currency_id", "USD")) or "USD"
            expiry = _parse_expiry(fpd.get("contract_expiry"))
            if not product_id or not base_asset or expiry is None:
                continue
            is_disabled = bool(prod.get("is_disabled", False))
            trading_disabled = bool(prod.get("trading_disabled", False))
            status = InstrumentStatus.INACTIVE if (is_disabled or trading_disabled) else InstrumentStatus.ACTIVE

            tick_str = str(prod.get("price_increment", "")) or str(prod.get("quote_increment", ""))
            lot_str = str(fpd.get("contract_size", "")) or str(prod.get("base_increment", ""))
            try:
                tick_size = Decimal(tick_str) if tick_str else Decimal("0.01")
            except (InvalidOperation, ValueError):
                tick_size = Decimal("0.01")
            try:
                contract_size = Decimal(lot_str) if lot_str else Decimal("1")
            except (InvalidOperation, ValueError):
                contract_size = Decimal("1")

            # Canonical instrument_id — raw exchange-native passthrough (the
            # product_id IS the wire symbol coinbase_cde_ws.py subscribes with —
            # matches TODAY's real production convention for CeFi FUTURE/OPTION
            # instrument_type across the fleet: VENUE:TYPE:RAW_ID, not yet the
            # @LIN/@INV-marked target format from instrument_id_format_
            # canonicalization_2026_07_08.md finding 1 (that format is
            # documented but NOT wired into any shipped adapter's live
            # instrument_key construction yet — see
            # reference_data/adapters/cefi/tardis/parsing.py:763-767). Using the
            # not-yet-adopted target format here would silently break the live
            # WS connector's subscribe path (it derives the wire product_id
            # directly from this id's 3rd segment) for no real benefit.
            cde_instrument_key = build_instrument_id(
                "COINBASE-CDE", InstrumentType.FUTURE, product_id, passthrough=True
            )
            results.append(
                InstrumentRecord(
                    instrument_key=cde_instrument_key,
                    # No CeFi raw-code-to-human-name translation gap (see other CeFi
                    # adapters' identical comment) — canonical_instrument_id mirrors
                    # instrument_key.
                    canonical_instrument_id=cde_instrument_key,
                    venue="COINBASE-CDE",
                    raw_symbol=product_id,
                    instrument_type=InstrumentType.FUTURE,
                    base_asset=base_asset,
                    # CDE's Advanced Trade endpoint returns ONLY FUTURE product_type
                    # (no SPOT_PAIR/PERPETUAL/OPTION — see class docstring), so every
                    # record here is a derivative and `underlying` is REQUIRED by
                    # instrument_validation.py's CeFi-derivatives check (`underlying
                    # is required for {asset_group} derivatives`). Pre-fix this field
                    # was never set (defaults to None), so ALL 99 real live CDE
                    # instruments were silently rejected at validation — zero rows
                    # ever reached the catalogue/manifest regardless of fetch success.
                    # base_asset (Coinbase's contract_root_unit, e.g. "BTC") IS the
                    # underlying for a dated/nano-perpetual future — same convention
                    # ccxt_adapter.py uses (`underlying = base if is_derivative else None`).
                    underlying=base_asset,
                    quote_asset=quote_asset,
                    settle_asset=quote_asset,
                    margin_type=_MARGIN_TYPE,
                    tick_size=tick_size,
                    min_size=contract_size,
                    contract_size=contract_size,
                    expiry=expiry,
                    available_from_datetime=_CDE_REGISTRATION_DATE,
                    available_to_datetime=expiry,
                    status=status,
                    timezone="UTC",
                )
            )

        logger.info("COINBASE-CDE: fetched %d FUTURE instruments", len(results))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by raw product_id."""
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
        raise NotImplementedError("COINBASE-CDE does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return the real expiry calendar for a CDE contract root (e.g. ``BIT``, ``ETP``)."""
        instruments = await self.get_instruments(InstrumentType.FUTURE)
        expiries = sorted(
            {inst.expiry for inst in instruments if inst.base_asset.upper() == underlying.upper() and inst.expiry}
        )
        return CanonicalExpiryCalendar(
            venue="COINBASE-CDE",
            instrument_type="FUTURE",
            underlying=underlying.upper(),
            expiries=expiries,
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch the latest funding rate for a far-dated "nano perpetual" CDE contract.

        Only the far-dated (2030-expiry) "nano perpetual" contracts carry a real
        ``perpetual_details.funding_rate``; dated (near-expiry) contracts have no
        funding mechanism — returns rate=0 for those (honest: no funding exists,
        not a fetch failure).
        """
        url = f"{_BASE}{_PRODUCTS_PATH}/{symbol}"
        now = datetime.now(UTC)
        try:
            async with self._make_session() as session, session.get(url) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_cde_error(exc)
            classification = classify_venue_error("coinbase_cde", error_code)
            action = classification.action.value if classification else "fail"
            logger.error(
                "COINBASE-CDE product request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={"venue": "coinbase_cde", "endpoint": "products/{id}", "symbol": symbol, "error": str(exc)},
            )
            return FundingRateRef(
                venue=self.venue,
                symbol=symbol,
                rate=Decimal("0"),
                next_funding_time=now,
                mark_price=None,
                updated_at=now,
            )

        entry: dict[str, object] = raw if isinstance(raw, dict) else {}
        fpd_obj = entry.get("future_product_details")
        fpd: dict[str, object] = fpd_obj if isinstance(fpd_obj, dict) else {}
        # funding_rate/funding_time live directly on future_product_details (real,
        # non-empty only for the far-dated "nano perpetual" contracts, confirmed
        # live 2026-07-10) — NOT under the nested perpetual_details sub-object
        # (that nested dict's own funding_rate/funding_time fields are ALWAYS
        # empty on every real product checked, a legacy/unused artifact).
        rate_raw = fpd.get("funding_rate") or "0"
        next_time_raw = fpd.get("funding_time")
        mark_raw = entry.get("mid_market_price") or entry.get("price") or "0"

        next_funding_time = now
        if isinstance(next_time_raw, str) and next_time_raw:
            parsed = _parse_expiry(next_time_raw)
            if parsed is not None:
                next_funding_time = parsed

        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)) if rate_raw else Decimal("0"),
            next_funding_time=next_funding_time,
            mark_price=Decimal(str(mark_raw)) if mark_raw and str(mark_raw) != "0" else None,
            updated_at=now,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Coinbase Advanced Trade public candles endpoint.

        GET /api/v3/brokerage/market/products/{product_id}/candles
        """
        granularity_map: dict[str, tuple[str, int]] = {
            "1m": ("ONE_MINUTE", 60),
            "5m": ("FIVE_MINUTE", 300),
            "15m": ("FIFTEEN_MINUTE", 900),
            "30m": ("THIRTY_MINUTE", 1800),
            "1h": ("ONE_HOUR", 3600),
            "2h": ("TWO_HOUR", 7200),
            "6h": ("SIX_HOUR", 21600),
            "1d": ("ONE_DAY", 86400),
        }
        granularity, bar_seconds = granularity_map.get(interval, ("ONE_DAY", 86400))
        now = datetime.now(UTC)
        end_ts = int(now.timestamp())
        start_ts = end_ts - limit * bar_seconds

        url = f"{_BASE}{_PRODUCTS_PATH}/{symbol}/candles"
        params = {"start": str(start_ts), "end": str(end_ts), "granularity": granularity}

        try:
            async with self._make_session() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_ohlcv: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_cde_error(exc)
            classification = classify_venue_error("coinbase_cde", error_code)
            action = classification.action.value if classification else "fail"
            logger.error(
                "COINBASE-CDE candles request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={"venue": "coinbase_cde", "endpoint": "candles", "symbol": symbol, "error": str(exc)},
            )
            return []

        entry: dict[str, object] = raw_ohlcv if isinstance(raw_ohlcv, dict) else {}
        candles_obj = entry.get("candles")
        candles: list[object] = candles_obj if isinstance(candles_obj, list) else []
        results: list[OHLCVRef] = []

        for candle_raw in candles:
            if not isinstance(candle_raw, dict):
                continue
            candle = cast(dict[str, object], candle_raw)
            start_raw = candle.get("start")
            if start_raw is None:
                continue
            ts = datetime.fromtimestamp(int(str(start_raw)), tz=UTC)
            try:
                results.append(
                    OHLCVRef(
                        venue=self.venue,
                        symbol=symbol,
                        timestamp=ts,
                        open=Decimal(str(candle.get("open", "0"))),
                        high=Decimal(str(candle.get("high", "0"))),
                        low=Decimal(str(candle.get("low", "0"))),
                        close=Decimal(str(candle.get("close", "0"))),
                        volume=Decimal(str(candle.get("volume", "0"))),
                        interval=interval,
                    )
                )
            except (InvalidOperation, ValueError):
                continue

        return results
