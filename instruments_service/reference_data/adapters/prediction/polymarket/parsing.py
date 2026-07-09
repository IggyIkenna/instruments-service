"""Polymarket market → InstrumentRecord / MarketLifecycle parsing.

Cohesion module of the ``adapters.prediction.polymarket`` package (split from
the former monolithic ``adapters/prediction/polymarket.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

``PolymarketParsingMixin`` carries the market-parsing / canonical-ID /
lifecycle methods of ``PolymarketReferenceDataAdapter`` (mixin composition
keeps every public and private method on the SAME class object, so
``patch.object(adapter, ...)`` targets are unchanged).  Module-level
collaborators (UAC registries, ``build_prediction_instrument_id``,
``PolymarketGammaMarket``, the keyword matchers) resolve through ``_pm`` — the
live package namespace — so ``unittest.mock.patch(
"instruments_service.reference_data.adapters.prediction.polymarket.<name>")``
targets behave exactly as they did before the split.
"""

# Package-internal access: the polymarket package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unified_api_contracts import PolymarketGammaMarket
    from unified_api_contracts.internal import InstrumentRecord
    from unified_api_contracts.predictions import CanonicalQuestionGroup, MarketLifecycle

    from instruments_service.reference_data.adapters.prediction import polymarket as _pm
    from instruments_service.reference_data.adapters.prediction.polymarket.adapter import (
        PolymarketReferenceDataAdapter,
    )
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.reference_data.adapters.prediction.polymarket._pkg_ref import polymarket_namespace as _pm

__all__ = [
    "PolymarketParsingMixin",
]


class PolymarketParsingMixin:
    """Market-parsing / canonical-ID / lifecycle methods of ``PolymarketReferenceDataAdapter``."""

    def _parse_market(
        self: PolymarketReferenceDataAdapter,
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
        mapped = _pm._MAPPER.map_market(
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
        _sub_category, base_asset, sports_canonical_instrument_id = result

        # Canonical question group — the SSOT classification pipeline (also drives
        # MarketLifecycle.canonical_group below). Computed ONCE here and threaded
        # into classify_lifecycle() (reuse, not reclassify) per
        # prediction_canonical_identity_migration_2026_07_08.md todo 2.
        group = (
            _pm.classify_polymarket_to_canonical_group(
                title=question,
                slug=slug,
                event_slug=market.event_slug or "",
                outcome=(market.outcomes or [""])[0] if market.outcomes else "",
                condition_id=condition_id,
            )
            or _pm.CanonicalQuestionGroup.OTHER
        )

        # Per CLAUDE.md "Prediction market lifecycle timing": every
        # prediction instrument MUST carry market_created_at and
        # settlement_time so MTDS CLOB capture + features-* compute can
        # gate ticks within the lifecycle window. We hang these on
        # InstrumentRecord.available_from_datetime / available_to_datetime
        # (the canonical SSOT slots) — the dedicated MARKET_LIFECYCLE
        # data_type carries the full lifecycle row including
        # canonical_question_group + current_status.
        lifecycle = self.classify_lifecycle(market, group=group)

        # Lifecycle BOUNDS (available_from / available_to) are the honest-absence
        # gate: MTDS / UTL must only emit a manifest cell (captured / empty /
        # failed) for dates WITHIN [available_from, available_to]; outside the
        # market's life is an honest BLANK, never empty_confirmed (the operator
        # 2026-06-23 drill-down: ~49.6k POLYMARKET empties were
        # EXPECTED_INSTRUMENT_NOT_LISTED for dates the market did not exist,
        # inflating honest coverage). classify_lifecycle() is STRICT — it returns
        # None unless BOTH a creation AND a resolution timestamp parse, so it
        # populated only ~16% of records (it is the MARKET_LIFECYCLE data_type's
        # full row). The InstrumentRecord bounds need only "when could this market
        # have data", so derive them DIRECTLY + best-effort from the gamma fields
        # (independent of the strict lifecycle): available_from from the listing
        # date (startDate / createdAt), available_to from the resolution date
        # (closedTime / endDateIso). Prefer the strict lifecycle's values when
        # present (they carry the settlement-lag-adjusted settlement_time), else
        # fall back to the raw gamma bound so out-of-life dates are bounded even
        # for markets where one timestamp is absent.
        available_from = self._parse_end_date(market.start_date) or self._parse_end_date(market.created_at)
        available_to = self._parse_end_date(market.closed_time) or self._parse_end_date(market.end_date_iso)
        if lifecycle is not None:
            available_from = lifecycle.market_created_at
            available_to = lifecycle.settlement_time

        # Canonical instrument_key: VENUE:TYPE:SYMBOL (2026-07-09 fix —
        # canonical_id_builder_retrofit_checklist_2026_07_08.md todo 7). Before this,
        # instrument_key was the bare condition_id with zero VENUE:TYPE: structure —
        # the only asset group missing it. Deliberately NOT passthrough=True: that mode
        # upper-cases the symbol for every non-DeFi type (canonical_id_builder.py::
        # _build_passthrough), which would corrupt condition_id — a real, lowercase
        # 0x…64hex hash — into a non-matching id. Calling the builder without
        # passthrough for PREDICTION_MARKET dispatches to _build_sports_or_prediction(),
        # which wraps VENUE:TYPE:{symbol} with case preserved verbatim, exactly what's
        # needed here.
        instrument_key = _pm.build_canonical_instrument_id(
            _pm.AssetGroup.PREDICTION, self.venue, _pm.InstrumentType.PREDICTION_MARKET, condition_id
        )

        # InstrumentRecord (UAC) carries no clob_token_ids field, so register the
        # per-outcome decimal CLOB token-ids in the package side-table keyed by the
        # SAME final instrument_key (== the wrapped condition_id, not the bare id —
        # process_write.py::_records_to_dataframe joins the side-table by whatever
        # instrument_key resolves to, so registering under that same value keeps the
        # join correct regardless of the id's shape) to materialise the
        # clob_token_ids availability-parquet column the Polymarket CLOB WS
        # subscribes by (live + batch resolve the same per-outcome token-ids).
        _pm._register_clob_token_ids(instrument_key, market.clob_token_ids)

        # underlying (prediction_canonical_identity_migration_2026_07_08.md todo 1 /
        # docs/PREDICTION_INSTRUMENTS.md § "Canonical identity model" §3 item 2): the
        # SAME classify_polymarket_to_canonical_group() -> underlying_for_group()
        # pipeline that already runs above for MarketLifecycle.canonical_group,
        # applying cross_venue_mapping.py::_build_mapping()'s existing convention —
        # sports fixtures don't have a single scalar underlying (None, "not
        # applicable"). Non-sports groups mostly resolve to a NAMED underlying (BTC,
        # CPI, TRUMP, GEO_ISRAEL_IRAN, OSCARS, …) — PredictionUnderlying.OTHER.value
        # is the honest catch-all reserved for genuinely-unclassified markets (cqg
        # OTHER / MISC_NOVELTY), not a blanket "politics/geo" bucket.
        underlying_axis = _pm.underlying_for_group(group)
        is_sports = underlying_axis.value.startswith("SPORTS_")
        underlying = None if is_sports else underlying_axis.value

        return _pm.InstrumentRecord(
            instrument_key=instrument_key,
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
            available_from_datetime=available_from,
            available_to_datetime=available_to,
            underlying=underlying,
            canonical_instrument_id=sports_canonical_instrument_id,
        )

    def _build_instrument_id(
        self: PolymarketReferenceDataAdapter,
        market: PolymarketGammaMarket,
        category: str,
        question: str,
        slug: str,
        expiry: datetime | None,
    ) -> tuple[str, str, str | None] | None:
        """Build (instrument_type, base_asset, canonical_instrument_id) with canonical naming.

        Returns:
            instrument_type: e.g. "prediction::crypto", "prediction::sports::EPL"
            base_asset: human-readable canonical e.g. "BTC:UP_DOWN:2026-03-25"
                        or "EPL:ARSENAL-v-CHELSEA:2026-03-25:MONEYLINE"
            canonical_instrument_id: Sports-asset-group-aligned fixture_id
                        (``LEAGUE:HOME_v_AWAY:YYYYMMDD``, see ``_build_sports_id``)
                        for a resolvable sports fixture, else None (crypto/macro/
                        other — populated separately by the cross-venue mapping
                        rollup step, see ``build_instrument_catalogue.py``).
            None if the market should be skipped (e.g. league not in prediction registry).
        """
        date_str = expiry.strftime("%Y-%m-%d") if expiry else "unknown"

        # Sports markets: use team names + league + market type
        if market.sports_market_type and market.outcomes:
            return self._build_sports_id(market, slug, expiry, date_str)

        q_lower = question.lower()
        crypto_match = _pm._match_crypto_asset(q_lower)
        if crypto_match:
            canonical_id = _pm.build_crypto_prediction_id("polymarket", crypto_match, "1D", date_str)
            return "prediction::crypto", canonical_id, None

        macro_match = _pm._match_macro_index(q_lower)
        if macro_match:
            canonical_id = _pm.build_macro_prediction_id("polymarket", macro_match, "1D", date_str)
            return "prediction::macro", canonical_id, None

        # Sports classified by PredictionMarketMapper but without sportsMarketType
        # (e.g. F1, UFC, NBA props) — reclassify as "other" since they're not
        # structured sports markets we can normalize.
        label = "other" if category == "sports" else category
        return f"prediction::{label}", question[:50] if question else slug[:50], None

    def _build_sports_id(
        self: PolymarketReferenceDataAdapter,
        market: PolymarketGammaMarket,
        slug: str,
        expiry: datetime | None,
        date_str: str,
    ) -> tuple[str, str, str | None] | None:
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
        league_id = _pm.get_canonical_league_for_polymarket_series(series_slug) if series_slug else None
        if not league_id or league_id not in _pm.POLYMARKET_PREDICTION_LEAGUES:
            return None

        outcomes = market.outcomes or []
        pm_market_type = market.sports_market_type or "moneyline"
        canonical_market = _pm.POLYMARKET_MARKET_TO_CANONICAL.get(
            pm_market_type, _pm.slugify_canonical_name(pm_market_type)
        )

        # Extract team names — priority: event_title > outcomes > question
        home, away = self._extract_teams(market, outcomes)

        # Derive season from date
        date_digits = date_str.replace("-", "")[:8]
        year = int(date_digits[:4]) if len(date_digits) >= 4 else 2026
        month = int(date_digits[4:6]) if len(date_digits) >= 6 else 1
        season_start = year if month >= 8 else year - 1
        season = f"{season_start}-{(season_start + 1) % 100:02d}"

        # Determine selection from outcomes
        selection = _pm._selection_from_outcomes(outcomes, pm_market_type)

        instrument_id = _pm.build_prediction_instrument_id(
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

        # Sports <-> Sports-asset-group fixture_id alignment
        # (prediction_canonical_identity_migration_2026_07_08.md todo 5 /
        # docs/PREDICTION_INSTRUMENTS.md §3 item 4). The Sports asset group's own
        # catalogue (build_instrument_catalogue.py::build_sports_fixture_team_player_catalogue)
        # computes fixture_id = build_fixture_id(league_id, build_team_id(home),
        # build_team_id(away), date_str) directly off the raw provider team names —
        # no crosswalk, no network call. Reuse that EXACT (league_id, build_team_id,
        # build_fixture_id) pipeline over the same "Away vs Home" pair
        # parse_polymarket_sports_fixture() already extracts for the cross-venue
        # matcher (fixture.home/away are only case/whitespace-normalized —
        # build_team_id()'s _slug() is case/whitespace-insensitive, so this is
        # byte-identical to calling build_team_id() on the raw split). Gives a
        # Polymarket sports market's canonical_instrument_id BYTE-PARITY with the
        # Sports asset group's fixture_id for the SAME real game, not just a
        # conceptually-similar id. None (honest absence, never a guessed id) when
        # event_title doesn't parse to a clean two-participant pair or no
        # settlement date is resolvable — deliberately NOT wired through the
        # unused, network-dependent _cross_reference_fixture() (a per-market
        # API-Football call in the hot adapter-parsing path would be a real
        # capture-throughput regression); that method remains available as a
        # higher-fidelity follow-up for an async, rate-limited pipeline stage.
        sports_canonical_instrument_id: str | None = None
        fixture = _pm.parse_polymarket_sports_fixture(
            league=league_id,
            event_title=market.event_title or "",
            slug=slug,
            resolution_date=expiry.date() if expiry is not None else None,
        )
        if fixture is not None:
            sports_canonical_instrument_id = _pm.build_fixture_id(
                fixture.league,
                _pm.build_team_id(fixture.home),
                _pm.build_team_id(fixture.away),
                fixture.fixture_date.isoformat(),
            )

        return instrument_type, instrument_id, sports_canonical_instrument_id

    def _extract_teams(
        self: PolymarketReferenceDataAdapter,
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
        teams = _pm._parse_vs_string(event_title)
        if teams:
            return _pm._normalize_team_pair(teams[0], teams[1])

        # 2. Try outcomes — works for spreads (e.g. ["Austin FC", "Real Salt Lake"])
        #    but NOT for moneyline/totals/btts where outcomes are Yes/No/Over/Under
        non_generic = [o for o in outcomes[:2] if o.lower() not in _pm._GENERIC_OUTCOMES]
        if len(non_generic) == 2:
            return _pm._normalize_team_pair(non_generic[0], non_generic[1])

        # 3. Try question text — "Will Arsenal win?", "Arsenal vs. Chelsea: O/U 2.5"
        question = market.question or ""
        teams = _pm._parse_vs_string(question.split(":")[0])  # strip ": O/U 2.5" suffix
        if teams:
            return _pm._normalize_team_pair(teams[0], teams[1])

        # 4. Single-team question: "Will Arsenal win on 2026-03-22?"
        single = _pm._parse_single_team_question(question)
        if single:
            canonical = _pm.get_canonical_team_for_polymarket(single)
            team_id = canonical if canonical else _pm.slugify_canonical_name(single)[:30]
            return team_id, "UNKNOWN"

        return "UNKNOWN", "UNKNOWN"

    async def _cross_reference_fixture(
        self: PolymarketReferenceDataAdapter,
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

        from unified_api_contracts.sports import (
            get_league,
        )

        league_def = get_league(league_id)
        if not league_def or not league_def.api_football_id:
            return None

        from ...sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key=self._api_football_key)
        try:
            fixtures = await adapter.get_fixtures(
                date=date_str,
                league_ids=[league_def.api_football_id],
            )
        except Exception as exc:
            _pm.logger.warning("API-Football fixture lookup failed for %s: %s", cache_key, exc)
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
                _pm.logger.info("Cross-ref match: %s %s-v-%s → %s", league_id, home_team, away_team, fid)
                return fid

        return None

    def _parse_end_date(self: PolymarketReferenceDataAdapter, end_date_raw: str | None) -> datetime | None:
        """Parse ISO end date string to UTC datetime."""
        if not end_date_raw:
            return None
        try:
            return datetime.fromisoformat(end_date_raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None

    def classify_lifecycle(
        self: PolymarketReferenceDataAdapter,
        market: PolymarketGammaMarket,
        group: CanonicalQuestionGroup | None = None,
    ) -> MarketLifecycle | None:
        """Build a :class:`MarketLifecycle` for a Polymarket Gamma market.

        Returns ``None`` when ``condition_id`` is missing (we can't key the
        lifecycle row without it) or when no ``market_created_at`` source is
        available (Gamma's ``createdAt`` is the only reliable creation
        timestamp; ``startDate`` is a scheduling hint that often disagrees
        for resolved markets).

        ``group`` lets a caller that already classified the market (e.g.
        ``_parse_market()``, which also needs the group for
        ``InstrumentRecord.underlying``) pass it in so this method doesn't
        reclassify — per
        ``prediction_canonical_identity_migration_2026_07_08.md`` todo 1
        ("reuse the result, don't reclassify"). Defaults to ``None``, which
        preserves the original self-contained behaviour for other callers
        (``get_market_lifecycles()``, direct test invocations).

        Lifecycle field derivation (per
        :mod:`unified_api_contracts.canonical.domain.predictions.lifecycle`):

        * ``market_created_at``: ``created_at`` (Gamma raw); falls back to
          ``start_date`` if missing.
        * ``resolution_time``: ``closed_time`` if available (the actual
          UMA-resolution timestamp); else falls back to ``end_date_iso``
          (the *scheduled* close — best-effort proxy for unresolved
          markets).
        * ``settlement_time``: ``resolution_time + canonical_group.settlement_lag``
          per UAC :data:`CANONICAL_GROUP_METADATA` (Polymarket UMA
          undisputed = +2h, disputed = +24-72h depending on group). When
          the canonical group is :class:`CanonicalQuestionGroup.OTHER`
          the +24h default lag applies.
        * ``current_status``: derived from ``closed`` + ``accepting_orders``
          flags — ``settled`` (closed=True), ``resolved`` (closed=True
          but lag not elapsed), ``active`` (accepting_orders=True),
          else ``created``.
        """
        condition_id = market.condition_id
        if not condition_id:
            return None

        created_at = self._parse_end_date(market.created_at) or self._parse_end_date(market.start_date)
        if created_at is None:
            return None

        resolution_time = self._parse_end_date(market.closed_time) or self._parse_end_date(market.end_date_iso)
        if resolution_time is None:
            return None

        if group is None:
            group = (
                _pm.classify_polymarket_to_canonical_group(
                    title=market.question or "",
                    slug=market.market_slug or "",
                    event_slug=market.event_slug or "",
                    outcome=(market.outcomes or [""])[0] if market.outcomes else "",
                    condition_id=condition_id,
                )
                or _pm.CanonicalQuestionGroup.OTHER
            )

        settlement_lag = _pm.CANONICAL_GROUP_METADATA[group].settlement_lag
        settlement_time = resolution_time + settlement_lag

        if market.closed:
            now = datetime.now(UTC)
            current_status = "settled" if now >= settlement_time else "resolved"
        elif market.accepting_orders:
            current_status = "active"
        else:
            current_status = "created"

        return _pm.MarketLifecycle(
            market_id=condition_id,
            venue=self.venue,
            canonical_group=group,
            market_created_at=created_at,
            resolution_time=resolution_time,
            settlement_time=settlement_time,
            current_status=current_status,
        )

    def get_market_lifecycles(self: PolymarketReferenceDataAdapter) -> list[MarketLifecycle]:
        """Return :class:`MarketLifecycle` rows for the markets captured by
        the most-recent :meth:`get_instruments` call.

        Used by the orchestrator's prediction writer path to emit the
        ``MARKET_LIFECYCLE`` data_type parquet alongside per-instrument
        records (per the
        ``predictions_master.plan.md`` Phase 1 critical path).

        Markets that fail :meth:`classify_lifecycle` (missing condition_id
        or unparseable created_at / resolution_time) are silently dropped
        — they're already excluded from the InstrumentRecord output for
        the same reasons. Returns an empty list before any
        ``get_instruments()`` call.
        """
        out: list[MarketLifecycle] = []
        for market in self._last_markets:
            lifecycle = self.classify_lifecycle(market)
            if lifecycle is not None:
                out.append(lifecycle)
        return out
