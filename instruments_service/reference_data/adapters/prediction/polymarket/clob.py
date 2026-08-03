"""Polymarket CLOB transport — price history + full historical market scan.

Cohesion module of the ``adapters.prediction.polymarket`` package (split from
the former monolithic ``adapters/prediction/polymarket.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

``PolymarketClobMixin`` carries the CLOB-side methods of
``PolymarketReferenceDataAdapter`` (mixin composition keeps every public and
private method on the SAME class object, so ``patch.object(adapter, ...)``
targets are unchanged).  Module-level collaborators resolve through ``_pm`` —
the live package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.prediction.polymarket.<name>")``
targets behave exactly as they did before the split.
"""

# Package-internal access: the polymarket package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar, cast

import aiohttp

from ....schemas import OHLCVRef

if TYPE_CHECKING:
    from unified_api_contracts.internal import InstrumentRecord

    from instruments_service.reference_data.adapters.prediction import polymarket as _pm
    from instruments_service.reference_data.adapters.prediction.polymarket.adapter import (
        PolymarketReferenceDataAdapter,
    )
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.prediction.polymarket._pkg_ref import polymarket_namespace as _pm

__all__ = [
    "PolymarketClobMixin",
]


class PolymarketClobMixin:
    """CLOB-side methods of ``PolymarketReferenceDataAdapter``."""

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

    _CLOB_BASE = "https://clob.polymarket.com"
    _CLOB_PAGE_LIMIT = 1000
    _CLOB_MAX_PAGES = 10000  # safety cap against runaway loop; CLOB exits naturally via "LTE=" cursor sentinel
    _CLOB_RAW_CACHE_TTL: ClassVar[float] = 86400.0  # 24 h — CLOB history is immutable; matches Tardis TTL

    # Active-window creation-date field priority for a raw (pre-validation) CLOB
    # market dict (2026-07-14, prediction_lifecycle_prefetch_gate_and_resolution_
    # day_catalogue_2026_07_14.md, contingent P1). Primary: the gamma-shaped
    # creation fields, present when an item was already enriched or came from a
    # gamma-shaped raw dict. Fallback: per ``markets.py::
    # _enrich_clob_lifecycle_lower_bound``'s 43a note, the raw CLOB ``/markets``
    # shape usually carries NEITHER of the primary fields at all, only these two
    # best-effort listing-date proxies.
    _CLOB_CREATION_FIELD_CANDIDATES: ClassVar[tuple[str, ...]] = (
        "start_date",
        "startDate",
        "created_at",
        "createdAt",
    )
    _CLOB_CREATION_FALLBACK_FIELD_CANDIDATES: ClassVar[tuple[str, ...]] = (
        "accepting_order_timestamp",
        "game_start_time",
    )

    def _parse_clob_point(
        self: PolymarketReferenceDataAdapter, point_raw: object, symbol: str, interval: str
    ) -> OHLCVRef | None:
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

    async def _fetch_clob_history(
        self: PolymarketReferenceDataAdapter, symbol: str, clob_interval: str
    ) -> list[object]:
        """Fetch raw price history from Polymarket CLOB API. Returns [] on error."""
        url = "https://clob.polymarket.com/prices-history"
        params = {"market": symbol, "interval": clob_interval, "fidelity": "1"}
        async with self._make_session() as session:
            try:
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    raw: dict[str, object] = cast("dict[str, object]", await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _pm._classify_polymarket_error(exc)
                classification = _pm.classify_venue_error("POLYMARKET", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                _pm.logger.error(
                    "Polymarket CLOB prices-history failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                _pm.log_event(
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
        self: PolymarketReferenceDataAdapter,
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

    @staticmethod
    def _enrich_clob_outcomes(raw_item: dict[str, object]) -> None:
        """Enrich CLOB market dict for PolymarketGammaMarket compatibility.

        Fixes two CLOB→Gamma incompatibilities:
        1. CLOB has tokens[].outcome, not top-level outcomes list
        2. CLOB has tags as list of strings, Gamma has list of tag objects
        """
        # Extract outcomes from tokens
        if "outcomes" not in raw_item:
            tokens = raw_item.get("tokens")
            if isinstance(tokens, list):
                outcomes: list[str] = []
                for t in tokens:
                    if isinstance(t, dict) and "outcome" in t:
                        outcomes.append(str(t["outcome"]))
                if outcomes:
                    raw_item["outcomes"] = outcomes

        # Convert string tags to PolymarketGammaTag-compatible dicts
        tags = raw_item.get("tags")
        if isinstance(tags, list) and tags and isinstance(tags[0], str):
            raw_item["tags"] = [{"slug": str(t).lower().replace(" ", "-"), "label": str(t)} for t in tags]

    async def _fetch_all_raw_clob_markets(self: PolymarketReferenceDataAdapter) -> list[dict[str, object]]:
        """Scan the full CLOB market history and return all raw market dicts.

        No date filtering — caller filters the result per-date.  Cursor sharding
        via env vars is preserved for parallel-worker backfills.
        """
        import base64
        import os

        start_cursor_b64 = os.environ.get("POLYMARKET_START_CURSOR", "").strip()  # noqa: qg-os-env
        end_cursor_b64 = os.environ.get("POLYMARKET_END_CURSOR", "").strip()  # noqa: qg-os-env

        end_offset: int | None = None
        if end_cursor_b64:
            try:
                end_offset = int(base64.b64decode(end_cursor_b64).decode("ascii"))
            except (ValueError, UnicodeDecodeError):
                _pm.logger.warning("POLYMARKET_END_CURSOR=%r is not a valid base64 int — ignoring", end_cursor_b64)

        cursor = start_cursor_b64
        if cursor:
            try:
                start_offset = int(base64.b64decode(cursor).decode("ascii"))
                _pm.logger.info(
                    "CLOB shard: starting at cursor offset %d (end_offset=%s)",
                    start_offset,
                    end_offset if end_offset is not None else "unbounded",
                )
            except (ValueError, UnicodeDecodeError):
                _pm.logger.warning("POLYMARKET_START_CURSOR=%r is not a valid base64 int — ignoring", cursor)
                cursor = ""

        all_markets: list[dict[str, object]] = []
        page = 0
        async with self._make_session() as session:
            for page in range(self._CLOB_MAX_PAGES):
                params: dict[str, str] = {"limit": str(self._CLOB_PAGE_LIMIT)}
                if cursor:
                    params["next_cursor"] = cursor
                try:
                    async with session.get(f"{self._CLOB_BASE}/markets", params=params) as resp:
                        resp.raise_for_status()
                        data = cast("dict[str, object]", await resp.json())
                except aiohttp.ClientError as exc:
                    # A mid-scan page failure must NOT silently truncate the CLOB
                    # universe. Breaking here would return the PARTIAL ``all_markets``
                    # accumulated so far — which is cached for 24 h
                    # (``_get_raw_clob_markets_cached``) and read by every per-date
                    # filter as a COMPLETE (but smaller) universe → false-complete
                    # coverage (CF-11 / A8: a transient failure masquerading as a
                    # genuine smaller universe, with ZERO failure signal). Classify +
                    # emit ``ADAPTER_FETCH_FAILED`` (mirrors ``_fetch_clob_history`` /
                    # ``_fetch_page``) then RAISE so the per-venue handler records the
                    # cell ``attempted_failed`` rather than caching a truncated
                    # universe. This is a single-venue pagination loop (NOT a
                    # per-venue/per-shard loop), so raising does not violate
                    # shard-level failure isolation.
                    error_code = _pm._classify_polymarket_error(exc)
                    classification = _pm.classify_venue_error("POLYMARKET", error_code)
                    action = classification.action.value if classification else "fail"
                    retry_safe = classification.retry_safe if classification else False
                    _pm.logger.error(
                        "Polymarket CLOB markets scan failed at page %d (%d markets so far): %s "
                        "(classified: %s, action: %s) — raising to avoid a truncated universe",
                        page,
                        len(all_markets),
                        exc,
                        error_code,
                        action,
                    )
                    _pm.log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "POLYMARKET",
                            "endpoint": "clob/markets",
                            "page": page,
                            "markets_so_far": len(all_markets),
                            "error": str(exc),
                            "error_code": error_code,
                            "action": action,
                            "retry_safe": retry_safe,
                        },
                    )
                    # Wrap as RuntimeError (CF-11): a bare ``aiohttp.ClientError`` /
                    # ``ClientResponseError`` (4xx/429/5xx) is not caught by
                    # ``_fetch_one_venue`` and would crash the whole multi-venue
                    # ``asyncio.gather`` batch; RuntimeError IS caught → the cell records
                    # ``attempted_failed`` (mirrors cefi ``aster.py``).
                    raise RuntimeError(
                        f"Polymarket clob/markets fetch failed "
                        f"(error_code={error_code}, retry_safe={retry_safe}): {exc}"
                    ) from exc

                raw_markets = data.get("data")
                if not isinstance(raw_markets, list) or not raw_markets:
                    break

                all_markets.extend(cast("list[dict[str, object]]", raw_markets))

                cursor = str(data.get("next_cursor", ""))
                if not cursor or cursor == "LTE=":
                    break

                if end_offset is not None:
                    try:
                        next_offset = int(base64.b64decode(cursor).decode("ascii"))
                        if next_offset >= end_offset:
                            _pm.logger.info(
                                "CLOB shard: reached end_offset=%d at page %d (%d markets)",
                                end_offset,
                                page,
                                len(all_markets),
                            )
                            break
                    except (ValueError, UnicodeDecodeError):
                        pass

                if page > 0 and page % 100 == 0:
                    _pm.logger.info("CLOB scan: page %d, %d markets total", page, len(all_markets))

        _pm.logger.info("CLOB full scan: %d markets (%d pages)", len(all_markets), page + 1)
        return all_markets

    async def _get_raw_clob_markets_cached(self: PolymarketReferenceDataAdapter) -> list[dict[str, object]]:
        """Return the full CLOB market list, fetching once and caching for 24 h.

        A multi-date backfill runs the expensive full-scan exactly once — every
        subsequent date filters the in-memory list (same pattern as Tardis/CeFi).
        TTL of 24 h matches Tardis; CLOB history is append-only and resolved
        markets are immutable within any realistic single-run window.
        """
        if (
            self._clob_raw_markets is not None
            and (time.monotonic() - self._clob_raw_markets_fetched_at) < self._CLOB_RAW_CACHE_TTL
        ):
            _pm.logger.debug("CLOB raw cache hit (%d markets)", len(self._clob_raw_markets))
            return self._clob_raw_markets
        raw = await self._fetch_all_raw_clob_markets()
        self._clob_raw_markets = raw
        self._clob_raw_markets_fetched_at = time.monotonic()
        _pm.logger.info("CLOB raw cache populated: %d markets", len(raw))
        return raw

    def _clob_market_window_ts(
        self: PolymarketReferenceDataAdapter, raw_item: dict[str, object]
    ) -> tuple[int | None, int | None]:
        """Best-effort ``(pre_creation_ts, post_settlement_ts)`` epoch-second window
        bounds for a raw (pre-validation) CLOB market dict — the active-window
        companion to the resolution-day-only filter ``_fetch_clob_markets`` used
        before 2026-07-14
        (prediction_lifecycle_prefetch_gate_and_resolution_day_catalogue_2026_07_14.md,
        contingent P1).

        Mirrors market-tick-data-service's
        ``base_prediction_adapter.compute_lifecycle_window_ts`` bounds comparison
        bit-for-bit (a date-only settlement is extended to end-of-day, since a
        market resolving on day D trades THROUGH day D) so IS's per-day catalogue
        and MTDS's pre-fetch gate agree on "what counts as active that day".
        REIMPLEMENTED locally rather than imported: instruments-service and
        market-tick-data-service are separate T4 services with no
        service->service dependency
        (codex/04-architecture/tier-and-import-architecture.md) — this is a
        mirror of the comparison, not a shared call.

        Creation-date field priority is :data:`_CLOB_CREATION_FIELD_CANDIDATES`
        then :data:`_CLOB_CREATION_FALLBACK_FIELD_CANDIDATES` (see their
        docstrings). Returns ``pre_creation_ts=None`` when nothing parses —
        callers MUST fail OPEN on that (include, never exclude) per this task's
        explicit instruction: an unknown creation date must never masquerade as
        "not yet listed".

        ``post_settlement_ts`` requires a parseable ``end_date_iso`` (checked
        raw-key first — the native CLOB shape — then the gamma alias for
        robustness). Unlike the creation side this is NOT fail-open: the old
        resolution-day filter already required ``end_date_iso`` to exist and
        parse, and making the settlement bound fail-open too would let markets
        with an unknown resolution date appear in EVERY day's catalogue forever
        — unbounded, not "widened".
        """
        created_raw: object = None
        for key in self._CLOB_CREATION_FIELD_CANDIDATES:
            created_raw = raw_item.get(key)
            if created_raw:
                break
        if not created_raw:
            for key in self._CLOB_CREATION_FALLBACK_FIELD_CANDIDATES:
                created_raw = raw_item.get(key)
                if created_raw:
                    break
        created_at = self._parse_end_date(str(created_raw)) if isinstance(created_raw, str) else None

        end_raw = raw_item.get("end_date_iso") or raw_item.get("endDateIso")
        settlement_time = self._parse_end_date(str(end_raw)) if isinstance(end_raw, str) else None

        pre_creation_ts = int(created_at.timestamp()) if created_at is not None else None
        post_settlement_ts: int | None = None
        if settlement_time is not None:
            settle = settlement_time
            if settle.hour == 0 and settle.minute == 0 and settle.second == 0 and settle.microsecond == 0:
                settle = settle + timedelta(days=1)
            post_settlement_ts = int(settle.timestamp())
        return pre_creation_ts, post_settlement_ts

    async def _fetch_clob_markets(
        self: PolymarketReferenceDataAdapter,
        date: str,
        now: datetime,
    ) -> list[InstrumentRecord]:
        """Filter the cached CLOB market list to markets ACTIVE on a target date.

        Widened 2026-07-14
        (prediction_lifecycle_prefetch_gate_and_resolution_day_catalogue_2026_07_14.md,
        contingent P1) from a resolution-day-only filter
        (``end_date_iso.startswith(date)``) to an ACTIVE-WINDOW overlap: a market
        is included when its ``[created, resolved)`` lifecycle window overlaps the
        target day, not only when it resolves ON that day (see
        :meth:`_clob_market_window_ts`). A market whose creation date is unknown
        is included (fail-open); a market whose resolution date is unknown is
        excluded (fail-closed — see that method's docstring for why the two sides
        are asymmetric). Resolution-day inclusion is unchanged: a market ending on
        the target day still overlaps its own window by construction.

        Volume note: this widening on its own would explode per-day attempt
        counts (EVERY active-window market becomes a fetch candidate for EVERY
        day it's alive, not just its resolution day). It is safe to ship because
        the MTDS pre-fetch gate (market-tick-data-service@abe0904d,
        ``base_prediction_adapter.prefilter_ids_by_lifecycle_window``) already
        landed and re-applies the SAME bounds comparison before any network call
        — the wider catalogue only grows ``cid_to_shard``, the gate still bounds
        actual fetch attempts to markets truly in-window for that day.

        The full CLOB scan is performed once and cached (_get_raw_clob_markets_cached).
        Each per-date call filters the ~1.8M-market in-memory list (1,801,017
        measured 2026-07-14) by the active-window bounds check, so a multi-date
        backfill pays the download cost exactly once.

        Cursor sharding via env vars (optional, for parallel backfill):
            POLYMARKET_START_CURSOR — base64-encoded offset to start at
            POLYMARKET_END_CURSOR   — base64-encoded offset to stop at
        Both default unset → full scan from offset 0.
        """
        day_start_ts = int(datetime.fromisoformat(f"{date}T00:00:00+00:00").timestamp())
        day_end_ts = day_start_ts + 86400
        results: list[InstrumentRecord] = []
        raw_markets = await self._get_raw_clob_markets_cached()
        for raw_item in raw_markets:
            pre_creation_ts, post_settlement_ts = self._clob_market_window_ts(raw_item)
            if post_settlement_ts is None or day_start_ts >= post_settlement_ts:
                continue  # resolution date unknown, or day is entirely after settlement
            if pre_creation_ts is not None and day_end_ts <= pre_creation_ts:
                continue  # day is entirely before the market existed
            self._enrich_clob_outcomes(raw_item)
            _pm._enrich_raw_event_fields(raw_item)
            try:
                market = _pm.PolymarketGammaMarket.model_validate(raw_item)
            except (ValueError, TypeError):
                continue
            self._last_markets.append(market)
            record = self._parse_market(market, now)
            if record is not None:
                results.append(record)
        _pm.logger.info(
            "CLOB: %d instruments for date=%s (active-window; from %d cached markets)",
            len(results),
            date,
            len(raw_markets),
        )
        return results
