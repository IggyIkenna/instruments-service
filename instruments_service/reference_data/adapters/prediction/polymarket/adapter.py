"""Polymarket adapter class — Gamma live listing + adapter entrypoints.

Cohesion module of the ``adapters.prediction.polymarket`` package (split from
the former monolithic ``adapters/prediction/polymarket.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

``PolymarketReferenceDataAdapter`` composes the CLOB transport
(``PolymarketClobMixin``) and parsing/lifecycle (``PolymarketParsingMixin``)
mixins over ``BaseReferenceDataAdapter`` — every public and private method
stays on the SAME class object, so ``patch.object(adapter, ...)`` targets are
unchanged.  Module-level collaborators resolve through ``_pm`` — the live
package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.prediction.polymarket.<name>")``
targets behave exactly as they did before the split.
"""

# Package-internal access: the polymarket package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import aiohttp

from ....base_adapter import BaseReferenceDataAdapter
from ....schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
)
from .clob import PolymarketClobMixin
from .parsing import PolymarketParsingMixin

if TYPE_CHECKING:
    import pandas as pd
    from unified_api_contracts import PolymarketGammaMarket
    from unified_api_contracts.internal import InstrumentRecord

    from instruments_service.reference_data.adapters.prediction import polymarket as _pm
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.prediction.polymarket._pkg_ref import polymarket_namespace as _pm

__all__ = [
    "PolymarketReferenceDataAdapter",
]


class PolymarketReferenceDataAdapter(PolymarketClobMixin, PolymarketParsingMixin, BaseReferenceDataAdapter):
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
        # Captured during get_instruments() — available via get_market_metadata()
        self._last_markets: list[PolymarketGammaMarket] = []
        # Raw CLOB market cache — full historical list, filtered per-date in _fetch_clob_markets.
        # Paid once per process lifetime (like Tardis); saves the full 900K-market cursor scan
        # for every date beyond the first in a multi-date backfill run.
        self._clob_raw_markets: list[dict[str, object]] | None = None
        self._clob_raw_markets_fetched_at: float = 0.0

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "POLYMARKET"

    def get_market_metadata_df(self) -> pd.DataFrame:
        """Return captured market metadata from the last ``get_instruments()`` call.

        Returns a DataFrame with key metadata fields from every
        PolymarketGammaMarket seen during the most recent fetch.
        """
        import pandas as pd  # noqa: qg-inside-import

        if not self._last_markets:
            return pd.DataFrame()
        rows: list[dict[str, object]] = []
        for m in self._last_markets:
            rows.append(
                {
                    "condition_id": m.condition_id,
                    "question_id": m.question_id,
                    "question": m.question,
                    "description": m.description,
                    "market_slug": m.market_slug,
                    "outcomes": ",".join(m.outcomes) if m.outcomes else None,
                    "end_date_iso": m.end_date_iso,
                    "active": m.active,
                    "closed": m.closed,
                    "volume": str(m.volume) if m.volume else None,
                    "liquidity": str(m.liquidity) if m.liquidity else None,
                    "category": m.tags[0].slug if m.tags else None,
                    "series_slug": m.series_slug,
                    "event_title": m.event_title,
                }
            )
        return pd.DataFrame(rows)

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
        self._last_markets = []  # Reset for new fetch
        now = datetime.now(UTC)

        _today = now.strftime("%Y-%m-%d")
        if date and date < _today:
            # Historical (PAST dates only): use CLOB API directly — Gamma prunes old
            # resolved markets so end_date_min/max returns 0 for most dates.
            # CLOB has the full 863K+ market history with no pruning.
            results = await self._fetch_clob_markets(date, now)
        else:
            # Current date (today) OR live (date is None): Gamma ACTIVE markets — fast,
            # sorted by volume, and is the currently-tradeable set. The CLOB-historical path
            # for `date==today` returned markets that ENDED today (resolved), which would drop
            # "0 records after filtering" for the full active universe. Gamma gives the right
            # universe but does NOT reliably return clobTokenIds in its response. When a
            # specific date was given (date==today, backfill mode), supplement with a CLOB scan
            # to register per-outcome token IDs for markets settling today — this populates the
            # _CLOB_TOKEN_IDS_BY_CONDITION_ID side-table so _records_to_dataframe can write the
            # clob_token_ids column the Polymarket CLOB WS subscribes by.
            results = []
            self._last_raw_page_size = 0
            async with self._make_session() as session:
                for page in range(_pm._MAX_PAGES_ACTIVE):
                    offset = page * _pm._PAGE_LIMIT
                    batch = await self._fetch_page(session, offset, now, date=None)
                    results.extend(batch)
                    if self._last_raw_page_size < _pm._PAGE_LIMIT:
                        break

            # For date-specific runs (today's backfill), also run CLOB to register token IDs
            # for markets settling today — Gamma doesn't return clobTokenIds so we supplement.
            # Side-effect only: the return value is discarded (Gamma universe is authoritative).
            if date:
                try:
                    await self._fetch_clob_markets(date, now)
                    _pm.logger.info("Polymarket: CLOB token-id supplement completed for date=%s", date)
                except Exception as exc:
                    _pm.logger.warning(
                        "Polymarket: CLOB token-id supplement failed for date=%s: %s — "
                        "clob_token_ids may be absent in today's IS parquet",
                        date,
                        exc,
                    )

        _pm.logger.info("Polymarket: fetched %d instruments (date=%s)", len(results), date or "active")
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError(
            "Polymarket does not provide options chains. Use get_instruments() to list available prediction markets."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError(
            "Polymarket does not provide expiry calendars. Market resolution dates are in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Polymarket does not have perpetual funding rates.")

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        offset: int,
        now: datetime,
        date: str | None = None,
    ) -> list[InstrumentRecord]:
        # Live mode only — historical uses _fetch_clob_markets() directly
        params: dict[str, str] = {
            "closed": "false",
            "active": "true",
            "limit": str(_pm._PAGE_LIMIT),
            "offset": str(offset),
            "order": "volume24hr",
            "ascending": "false",
        }
        url = f"{_pm._GAMMA_BASE}/markets"
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_json: object = cast("object", await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _pm._classify_polymarket_error(exc)
            classification = _pm.classify_venue_error("POLYMARKET", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            _pm.logger.error(
                "Polymarket markets failed (offset=%d): %s (classified: %s, action: %s)",
                offset,
                exc,
                error_code,
                action,
            )
            _pm.log_event(
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
            # A mid-pagination page failure must NOT silently truncate the live
            # universe. The caller (`get_instruments` live-mode loop) breaks when a
            # page yields fewer than ``_PAGE_LIMIT`` records — so a transient ``[]``
            # here would masquerade as a COMPLETE (but smaller / empty) universe with
            # ZERO failure signal (CF-11 / A8). RAISE so the per-venue handler records
            # the cell ``attempted_failed`` instead. Single-venue pagination loop (NOT
            # a per-venue/per-shard loop) → raising respects shard-level failure
            # isolation; mirrors ``_fetch_all_raw_clob_markets``.
            # Wrap as RuntimeError (CF-11): the bare ``aiohttp.ClientError`` /
            # ``ClientResponseError`` (4xx/429/5xx from ``raise_for_status``) is NOT in
            # ``_fetch_one_venue``'s except-ladder, so a bare re-raise propagates UNCAUGHT
            # through the caller's ``asyncio.gather`` (no ``return_exceptions=True``) and
            # crashes the whole multi-venue batch. RuntimeError IS caught there → the cell
            # records ``attempted_failed`` (mirrors cefi ``aster.py``/``hyperliquid.py``).
            raise RuntimeError(
                f"Polymarket gamma/markets fetch failed (error_code={error_code}, retry_safe={retry_safe}): {exc}"
            ) from exc

        raw_list = cast("list[object]", raw_json)
        results: list[InstrumentRecord] = []
        markets: list[PolymarketGammaMarket] = []
        for raw_item in raw_list:
            _pm._enrich_raw_event_fields(raw_item)
            try:
                market = _pm.PolymarketGammaMarket.model_validate(raw_item)
            except (ValueError, TypeError) as exc:
                # Skip individual markets that fail validation — don't kill the whole page
                slug = raw_item.get("market_slug", "?") if isinstance(raw_item, dict) else "?"
                _pm.logger.debug("Polymarket: skipping market %s — validation error: %s", slug, exc)
                continue
            markets.append(market)
            record = self._parse_market(market, now)
            if record is not None:
                results.append(record)
        self._last_markets.extend(markets)
        # Store raw page size so caller can paginate correctly even when
        # many markets are filtered out (e.g. non-prediction-league sports).
        self._last_raw_page_size = len(raw_list)
        return results
