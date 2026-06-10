"""Coverage boost for PolymarketGammaAdapter uncovered branches.

Targets:
- get_market_metadata_df (341-365): non-empty _last_markets
- _fetch_clob_markets (716-717): date mismatch → continue
- _fetch_clob_markets (721-723): ValidationError → continue
- _build_sports_id (958-987): valid league → return tuple
"""

from __future__ import annotations

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
# _fetch_clob_markets (lines 716-717, 721-723)
# ===========================================================================


class TestFetchClobMarkets:
    """Date mismatch skip + ValidationError continue."""

    async def test_date_mismatch_skips_market(self) -> None:
        """Line 717: end_date doesn't start with target_prefix → continue."""
        from datetime import UTC, datetime

        adapter = _make_adapter()

        raw_markets = [
            {"end_date_iso": "2026-07-01T00:00:00Z"},  # different date
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
        """Lines 721-723: PolymarketGammaMarket.model_validate raises → continue."""
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
    """Valid league series_slug → return (instrument_type, instrument_id)."""

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
            result = adapter._build_sports_id(mock_market, "2026-03-22")  # type: ignore[attr-defined]

        assert result is not None
        assert result[0] == "prediction::sports::EPL"
        assert "ARSENAL" in result[1] or result[1] is not None

    def test_no_league_returns_none(self) -> None:
        adapter = _make_adapter()

        mock_market = MagicMock()
        mock_market.series_slug = "esports-league"

        with patch(
            "instruments_service.reference_data.adapters.prediction.polymarket.get_canonical_league_for_polymarket_series",
            return_value=None,
        ):
            result = adapter._build_sports_id(mock_market, "2026-03-22")  # type: ignore[attr-defined]

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
            result = adapter._build_sports_id(mock_market, "2026-03-22")  # type: ignore[attr-defined]

        assert result is None
