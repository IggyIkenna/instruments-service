"""Coverage boost for PolymarketGammaAdapter uncovered branches.

Targets:
- get_market_metadata_df (341-365): non-empty _last_markets
- _fetch_clob_markets active-window filter (2026-07-14, contingent P1 of
  prediction_lifecycle_prefetch_gate_and_resolution_day_catalogue_2026_07_14.md):
  active-but-not-resolving inclusion, pre-created/post-resolution exclusion,
  resolution-day-unchanged, creation-date fail-open vs settlement-date
  fail-closed, and the ValidationError → continue branch.
- _build_sports_id (958-987): valid league → return tuple
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch


def _make_adapter() -> object:
    from instruments_service.reference_data.adapters.prediction.polymarket import (
        PolymarketReferenceDataAdapter,
    )

    return PolymarketReferenceDataAdapter(api_key="test-key", project_id="test-project")


# ===========================================================================
# get_market_metadata_df (lines 341-365)
# ===========================================================================


class TestGetMarketMetadataDf:
    """Non-empty _last_markets → DataFrame with expected columns."""

    def test_returns_dataframe_with_rows(self) -> None:
        import pandas as pd

        adapter = _make_adapter()

        mock_market = MagicMock()
        mock_market.condition_id = "cond_abc"
        mock_market.question_id = "q123"
        mock_market.question = "Will X happen?"
        mock_market.description = "A test market"
        mock_market.market_slug = "will-x-happen"
        mock_market.outcomes = ["Yes", "No"]
        mock_market.end_date_iso = "2026-06-15T00:00:00Z"
        mock_market.active = True
        mock_market.closed = False
        mock_market.volume = None
        mock_market.liquidity = None
        mock_market.tags = []
        mock_market.series_slug = "test-series"
        mock_market.event_title = "Test Event"

        adapter._last_markets = [mock_market]  # type: ignore[attr-defined]
        df = adapter.get_market_metadata_df()  # type: ignore[attr-defined]

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["condition_id"] == "cond_abc"
        assert df.iloc[0]["question"] == "Will X happen?"

    def test_returns_dataframe_with_tags(self) -> None:
        import pandas as pd

        adapter = _make_adapter()

        mock_tag = MagicMock()
        mock_tag.slug = "sports"

        mock_market = MagicMock()
        mock_market.condition_id = "cond_xyz"
        mock_market.question_id = "q456"
        mock_market.question = "Sports question?"
        mock_market.description = ""
        mock_market.market_slug = "sports-q"
        mock_market.outcomes = []
        mock_market.end_date_iso = "2026-06-15T00:00:00Z"
        mock_market.active = True
        mock_market.closed = False
        mock_market.volume = "1000"
        mock_market.liquidity = "500"
        mock_market.tags = [mock_tag]
        mock_market.series_slug = None
        mock_market.event_title = ""

        adapter._last_markets = [mock_market]  # type: ignore[attr-defined]
        df = adapter.get_market_metadata_df()  # type: ignore[attr-defined]

        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["category"] == "sports"


# ===========================================================================
# _fetch_clob_markets — active-window filter (2026-07-14, contingent P1)
# ===========================================================================


class TestFetchClobMarkets:
    """Active-window widening: a market's ``[created, resolved)`` window, not
    just its resolution day, determines per-day catalogue membership.

    Volume is bounded by the MTDS pre-fetch gate
    (market-tick-data-service@abe0904d,
    ``base_prediction_adapter.prefilter_ids_by_lifecycle_window``), shipped in
    the same issue and landed BEFORE this widening per the issue doc's
    ordering rationale — this widening only grows IS's ``cid_to_shard``
    candidate list; MTDS's pre-fetch gate still bounds actual fetch attempts
    to markets truly in-window for that day.
    """

    async def test_active_non_resolving_market_included(self) -> None:
        """A market created well before day D and resolving well after day D
        (active-but-not-resolving) now appears in day D's catalogue — the
        exact widening this fix targets (previously dropped entirely)."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xactive",
                "market_slug": "long-lived-market",
                "question": "Will X happen eventually?",
                "active": True,
                "closed": False,
                "created_at": "2026-06-01T00:00:00Z",  # created before day D
                "end_date_iso": "2026-12-31T23:59:00Z",  # resolves well after day D
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert len(results) == 1

    async def test_pre_created_market_excluded(self) -> None:
        """A market whose known creation date is AFTER day D is excluded —
        day D is entirely before the market existed (fail CLOSED once the
        creation date IS known, per the day_end_ts <= pre_creation_ts bound)."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xfuture",
                "market_slug": "not-yet-created",
                "question": "Future market?",
                "active": True,
                "closed": False,
                "created_at": "2026-06-20T00:00:00Z",  # created AFTER day D
                "end_date_iso": "2026-12-31T23:59:00Z",
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert results == []

    async def test_post_resolution_market_excluded(self) -> None:
        """A market that already resolved BEFORE day D is excluded — day D is
        entirely after settlement."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xresolved",
                "market_slug": "already-resolved",
                "question": "Already resolved?",
                "active": False,
                "closed": True,
                "created_at": "2026-01-01T00:00:00Z",
                "end_date_iso": "2026-03-01T00:00:00Z",  # resolved well before day D
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert results == []

    async def test_resolution_day_behavior_unchanged(self) -> None:
        """A market resolving exactly ON day D is still included — the
        original resolution-day filter's core case is preserved by the
        widened active-window overlap check (a market always overlaps its
        own resolution day)."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xresolving-today",
                "market_slug": "resolves-today",
                "question": "Resolves today?",
                "active": False,
                "closed": True,
                "created_at": "2026-05-01T00:00:00Z",
                "end_date_iso": "2026-06-15T18:00:00Z",  # resolves ON day D
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert len(results) == 1

    async def test_unknown_creation_date_fails_open_include(self) -> None:
        """No creation-date field at all (neither primary nor fallback
        candidates) → fail OPEN, include — per the task's explicit
        instruction that an unknown creation date must never masquerade as
        "not yet listed"."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xunknown-created",
                "market_slug": "unknown-creation",
                "question": "Unknown creation date?",
                "active": True,
                "closed": False,
                "end_date_iso": "2026-12-31T23:59:00Z",  # resolves well after day D; no creation field at all
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert len(results) == 1

    async def test_missing_end_date_excluded_fail_closed(self) -> None:
        """No ``end_date_iso`` at all → excluded — fail CLOSED on the
        settlement side (asymmetric with the creation side by design): an
        unbounded/unknown resolution date must not let a market appear in
        EVERY day's catalogue forever."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xno-end-date",
                "market_slug": "no-end-date",
                "question": "No end date?",
                "active": True,
                "closed": False,
                "created_at": "2026-01-01T00:00:00Z",
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert results == []

    async def test_fallback_creation_field_game_start_time_used(self) -> None:
        """When the primary creation fields are absent, ``game_start_time``
        (a CLOB-native best-effort proxy, per ``markets.py::
        _enrich_clob_lifecycle_lower_bound``'s 43a note) is used as the
        creation-date signal — a market whose ``game_start_time`` is AFTER
        day D is excluded, proving the fallback field is actually consulted."""
        adapter = _make_adapter()

        raw_markets = [
            {
                "condition_id": "0xgame-start",
                "market_slug": "game-not-started",
                "question": "Game not started?",
                "active": True,
                "closed": False,
                "game_start_time": "2026-06-20T00:00:00Z",  # AFTER day D
                "end_date_iso": "2026-12-31T23:59:00Z",
            },
        ]

        with patch.object(
            adapter,
            "_get_raw_clob_markets_cached",
            new_callable=AsyncMock,
            return_value=raw_markets,
        ):
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert results == []

    async def test_validation_error_continues(self) -> None:
        """PolymarketGammaMarket.model_validate raises → continue (the
        market passes the active-window filter — it resolves on day D — but
        fails pydantic validation, so it's dropped without crashing the
        scan)."""
        from datetime import UTC, datetime

        adapter = _make_adapter()

        raw_markets = [
            {"end_date_iso": "2026-06-15T00:00:00Z", "condition_id": "bad"},
        ]

        with (
            patch.object(
                adapter,
                "_get_raw_clob_markets_cached",
                new_callable=AsyncMock,
                return_value=raw_markets,
            ),
            patch.object(adapter, "_enrich_clob_outcomes"),
            patch("instruments_service.reference_data.adapters.prediction.polymarket._enrich_raw_event_fields"),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.PolymarketGammaMarket"
            ) as mock_cls,
        ):
            mock_cls.model_validate.side_effect = ValueError("bad market")
            results = await adapter._fetch_clob_markets("2026-06-15", datetime.now(UTC))  # type: ignore[attr-defined]

        assert results == []


# ===========================================================================
# _build_sports_id (lines 958-987)
# ===========================================================================


class TestBuildSportsId:
    """Valid league series_slug → return (instrument_type, instrument_id, canonical_instrument_id).

    Signature gained ``slug`` + ``expiry`` params (prediction_canonical_identity_
    migration_2026_07_08.md todo 5 — Sports-asset-group-aligned fixture_id via
    ``build_fixture_id()``), so every call below now passes them positionally.
    """

    def test_valid_league_returns_tuple(self) -> None:
        adapter = _make_adapter()

        mock_market = MagicMock()
        mock_market.series_slug = "epl-matches-2026"
        mock_market.sports_market_type = "moneyline"
        mock_market.line = None
        mock_market.outcomes = ["Arsenal", "Chelsea"]
        mock_market.event_title = "Arsenal vs Chelsea"
        mock_market.question = "Will Arsenal win?"
        mock_market.category = "sports"

        with (
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.get_canonical_league_for_polymarket_series",
                return_value="EPL",
            ),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.POLYMARKET_PREDICTION_LEAGUES",
                new={"EPL"},
            ),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.POLYMARKET_MARKET_TO_CANONICAL",
                new={"moneyline": "MATCH_ODDS"},
            ),
            patch.object(adapter, "_extract_teams", return_value=("arsenal", "chelsea")),  # type: ignore[attr-defined]
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.build_prediction_instrument_id",
                return_value="FOOTBALL:POLYMARKET:MATCH_ODDS:EPL:2025-26:ARSENAL-CHELSEA::HOME",
            ),
        ):
            result = adapter._build_sports_id(  # type: ignore[attr-defined]
                mock_market, "arsenal-vs-chelsea-2026-03-22", datetime(2026, 3, 22, tzinfo=UTC), "2026-03-22"
            )

        assert result is not None
        assert result[0] == "prediction::sports::EPL"
        assert "ARSENAL" in result[1] or result[1] is not None
        # canonical_instrument_id: real build_fixture_id() output off the real
        # "Arsenal vs Chelsea" event_title (not mocked — todo 5's actual mechanism).
        assert result[2] == "EPL:CHELSEA_v_ARSENAL:20260322"

    def test_no_league_returns_none(self) -> None:
        adapter = _make_adapter()

        mock_market = MagicMock()
        mock_market.series_slug = "esports-league"

        with patch(
            "instruments_service.reference_data.adapters.prediction.polymarket.get_canonical_league_for_polymarket_series",
            return_value=None,
        ):
            result = adapter._build_sports_id(mock_market, "esports-league", None, "2026-03-22")  # type: ignore[attr-defined]

        assert result is None

    def test_league_not_in_registry_returns_none(self) -> None:
        adapter = _make_adapter()

        mock_market = MagicMock()
        mock_market.series_slug = "unknown-league"

        with (
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.get_canonical_league_for_polymarket_series",
                return_value="UNKNOWN_LEAGUE",
            ),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.POLYMARKET_PREDICTION_LEAGUES",
                new={"EPL", "LALIGA"},
            ),
        ):
            result = adapter._build_sports_id(mock_market, "unknown-league", None, "2026-03-22")  # type: ignore[attr-defined]

        assert result is None
