"""Polymarket crypto-perp reference data adapter — POLYMARKET-PERP.

Distinct from the prediction-market PolymarketReferenceDataAdapter in
``adapters/prediction/polymarket.py``. Polymarket launched CFTC-regulated
crypto perpetual futures (POLYMARKET-PERP beta) on 2026-04-21. These are
NOT prediction YES/NO markets — they are continuous perpetual contracts with
funding rates on a separate CLOB engine.

API reference:
  Perps base URL (beta): https://perps-api.polymarket.com/
  Contract list:  GET /markets              (public, no auth)
  Funding rate:   GET /markets/{id}/funding_rate
  Orderbook:      GET /orderbook/{id}
  WebSocket:      wss://perps-ws.polymarket.com

Public read — no API key required for contract enumeration.
First data: 2026-04-21 (beta launch).

TODO (BLOCKED-CREDENTIALS): the beta /markets endpoint response schema has not
been confirmed live. The field mapping below is based on the plan API-research
notes (prediction_venue_perps_and_live_clob_depth_2026_06_20.md Phase 0).
Once the polymarket-perp-api-key is provisioned, run an integration test
(@pytest.mark.requires_credentials) against the live endpoint to confirm field
names and add cursor/pagination shape.
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

_PERPS_BASE_URL = "https://perps-api.polymarket.com"
_PAGE_LIMIT = 200
_MAX_PAGES = 10  # cap at 2000 contracts per fetch

_STATUS_MAP: dict[int, str] = {429: "429", 401: "401", 403: "403", 400: "400"}
_MSG_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("429", "rate"), "429"),
    (("401", "unauthorized"), "401"),
    (("403", "forbidden"), "403"),
    (("400", "bad request"), "400"),
    (("500", "internal", "server"), "500"),
)


def _classify_polymarket_perp_error(exc: Exception, status: int | None = None) -> str:
    """Map a Polymarket HTTP/network error to a UAC error code."""
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


class PolymarketPerpReferenceDataAdapter(BaseReferenceDataAdapter):
    """Polymarket crypto-perp reference data adapter (POLYMARKET-PERP).

    Returns active Polymarket beta perpetual futures as InstrumentRecord
    instances with ``instrument_type=PERPETUAL``.

    Field mapping (beta — confirm against live endpoint):
      instrument_key  = market_id (e.g. "BTC-USD")
      venue           = "polymarket-perp"
      instrument_type = InstrumentType.PERPETUAL
      raw_symbol      = market_id
      base_asset      = underlying crypto symbol (e.g. "BTC")
      quote_asset     = "USD"
      status          = InstrumentStatus.ACTIVE when market.status in ("active","open"), else DELISTED

    Public read — no API key required for contract enumeration.

    TODO: confirm cursor/pagination field names once the beta API is live.
    TODO: add integration test @pytest.mark.requires_credentials.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "polymarket-perp"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Polymarket crypto-perp contracts as InstrumentRecord list.

        instrument_type filter: pass ``InstrumentType.PERPETUAL`` or None (all).
        Other values return an empty list (venue only has PERPETUAL).
        """
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        # TODO: confirm cursor/offset field name from live beta API response.
        # Plan API research notes (Phase 0) name the endpoint but not pagination shape.
        # Using offset-based pagination as a best-effort scaffold; switch to cursor-based
        # once confirmed.
        offset = 0
        fetch_failed = False

        async with self._make_session() as session:
            for _page in range(_MAX_PAGES):
                try:
                    batch, has_more = await self._fetch_markets_page(session, offset, now)
                except RuntimeError:
                    # Page fetch failed — ADAPTER_FETCH_FAILED already emitted in
                    # _fetch_markets_page. Per shard-isolation, keep pages already
                    # fetched; the all-failed case re-raises below.
                    fetch_failed = True
                    break
                results.extend(batch)
                if not has_more or len(batch) < _PAGE_LIMIT:
                    break
                offset += len(batch)

        if fetch_failed and not results:
            # All pages failed with zero records → raise so the caller records
            # this venue as attempted_failed (not a silent honest-empty).
            raise RuntimeError(
                "PolymarketPerp get_instruments: market fetch failed with no records "
                "(see ADAPTER_FETCH_FAILED) — recording attempted_failed, not empty"
            )
        return results

    async def _fetch_markets_page(
        self,
        session: aiohttp.ClientSession,
        offset: int,
        now: datetime,
    ) -> tuple[list[InstrumentRecord], bool]:
        """Fetch one page of Polymarket perp markets.

        TODO: the beta /markets pagination shape (cursor vs offset vs next_url)
        is not yet confirmed from a live API response. This scaffold uses
        limit+offset; update to the correct pagination mechanism once verified
        against the live beta endpoint (BLOCKED-CREDENTIALS: polymarket-perp-api-key
        needed for trading; enumeration itself is public but unverified in beta).

        Returns (records, has_more).
        """
        url = f"{_PERPS_BASE_URL}/markets"
        params: dict[str, str] = {
            "limit": str(_PAGE_LIMIT),
            "offset": str(offset),
        }
        headers = {"Accept": "application/json"}

        try:
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_polymarket_perp_error(exc)
            classification = classify_venue_error("polymarket", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Polymarket-perp markets request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
            raise RuntimeError(f"Polymarket-perp markets fetch failed: {exc}") from exc

        # TODO: confirm top-level field names from live beta API.
        # Best-effort: try "markets" then "data" then treat list responses directly.
        raw_obj = cast(object, raw_json)
        markets_raw: object = None
        if isinstance(raw_obj, dict):
            raw_dict = cast(dict[str, object], raw_obj)
            markets_raw = raw_dict.get("markets") or raw_dict.get("data") or raw_dict.get("results")
        elif isinstance(raw_obj, list):
            markets_raw = raw_obj

        if not isinstance(markets_raw, list):
            logger.warning(
                "Polymarket-perp: unexpected /markets response shape — returning empty "
                "(TODO: update field extraction once beta API schema is confirmed)"
            )
            return [], False

        records: list[InstrumentRecord] = []
        for raw_item in markets_raw:
            market = cast(dict[str, object], raw_item)
            record = self._parse_market(market, now)
            if record is not None:
                records.append(record)

        has_more = len(markets_raw) >= _PAGE_LIMIT
        return records, has_more

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single Polymarket perp contract by market_id."""
        url = f"{_PERPS_BASE_URL}/markets/{symbol}"
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
                error_code = _classify_polymarket_perp_error(exc)
                classification = classify_venue_error("polymarket", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Polymarket-perp get_instrument failed for %s: %s (classified: %s, action: %s)",
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
        # Beta response may wrap the market in a "market" key or return it directly.
        market_raw = raw_dict.get("market") or raw_dict
        if not isinstance(market_raw, dict):
            return None
        return self._parse_market(cast(dict[str, object], market_raw), now)

    def _parse_market(
        self,
        raw: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a Polymarket perp market dict to an InstrumentRecord (PERPETUAL type).

        TODO: field names are best-effort based on plan API-research notes. Confirm
        against live beta endpoint once credentials are available.

        Known fields (from plan Phase 0 research):
          market_id / id  — unique market identifier
          ticker          — e.g. "BTC-USD"
          base_asset      — underlying crypto (e.g. "BTC")
          quote_asset     — settlement currency (e.g. "USD")
          status          — "active" / "open" for live contracts
        """
        # Accept either "market_id", "id", or "ticker" as the primary key.
        market_id = raw.get("market_id") or raw.get("id") or raw.get("ticker")
        if not isinstance(market_id, str) or not market_id:
            return None

        # Derive base_asset: prefer explicit field, fall back to ticker parsing.
        base_raw = raw.get("base_asset") or raw.get("base") or raw.get("underlying")
        base_asset: str = (
            base_raw.upper()[:50] if isinstance(base_raw, str) and base_raw else _extract_base_asset(market_id)
        )

        quote_raw = raw.get("quote_asset") or raw.get("quote") or "USD"
        quote_asset = str(quote_raw).upper()[:20] if quote_raw else "USD"

        status_raw = raw.get("status")
        is_active_bool = str(status_raw).lower() in ("active", "open") if status_raw else True
        instrument_status = InstrumentStatus.ACTIVE if is_active_bool else InstrumentStatus.DELISTED

        return InstrumentRecord(
            instrument_key=market_id,
            venue=self.venue,
            raw_symbol=market_id,
            instrument_type=InstrumentType.PERPETUAL,
            base_asset=base_asset,
            quote_asset=quote_asset,
            tick_size=Decimal("0.01"),
            min_size=Decimal("1"),
            contract_size=Decimal("1"),
            settle_asset=quote_asset,
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
            "Polymarket-perp does not support options chains. "
            "Use get_instruments() to list available perpetual contracts."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "PERPETUAL",
    ) -> CanonicalExpiryCalendar:
        """Not supported — perpetual contracts have no expiry calendar."""
        raise NotImplementedError("Polymarket-perp does not have an expiry calendar — contracts are perpetual.")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch current funding rate for a Polymarket perp contract.

        TODO: wire GET /markets/{market_id}/funding_rate once MTDS funding
        data pipeline is set up. Instrument enumeration (this adapter) is
        public-read; funding data may require authentication.
        """
        raise NotImplementedError(
            "Polymarket-perp funding rate: implement via MTDS funding handler. "
            "API endpoint: GET /markets/{market_id}/funding_rate"
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """OHLCV not available via reference data API."""
        raise NotImplementedError("Polymarket-perp OHLCV is not available via the reference data API.")


def _extract_base_asset(market_id: str) -> str:
    """Extract base crypto asset from a Polymarket perp market_id or ticker.

    Examples:
      "BTC-USD"    → "BTC"
      "ETH-USD"    → "ETH"
      "BTCUSD"     → "BTC"
      "BTC"        → "BTC"

    Strategy: split on common separators, take the first token.
    """
    s = market_id.upper().strip()
    # Split on dash or slash separator
    for sep in ("-", "/", "_"):
        if sep in s:
            return s.split(sep)[0][:8]
    # Strip trailing quote suffix
    for suffix in ("USDT", "USDC", "USD", "PERP"):
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)][:8]
    return s[:8]
