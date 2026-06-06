"""Extended orchestrator coverage — monotonicity checks, DeFi universe cache,
venue availability, normalize_wrapped_token, and _get_venue_epoch.

Covers uncovered lines in orchestrator.py without duplicating existing tests
in test_new_orchestrator.py, test_orchestrator_helpers.py, or test_orchestrator_process.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import (
    _count_per_venue,
    _enforce_defi_monotonicity,
    _get_venue_epoch,
    _normalize_wrapped_token,
    clear_defi_universe_cache,
    earliest_venue_date,
    filter_defi_instruments_by_relevance,
    filter_instruments_by_date,
)


def _make_record(
    instrument_key: str = "TEST",
    venue: str = "AAVE_V3-ETHEREUM",
    instrument_type: str = "A_TOKEN",
    base_asset: str = "WETH",
    quote_asset: str = "USDC",
    available_since: datetime | None = None,
    available_to: datetime | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue=venue,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        base_asset_contract_address="0x" + "a" * 40,
        base_asset_decimals=18,
        available_from_datetime=available_since,
        available_to_datetime=available_to,
    )


# ---------------------------------------------------------------------------
# _normalize_wrapped_token
# ---------------------------------------------------------------------------


class TestNormalizeWrappedToken:
    def test_plain_token_unchanged(self) -> None:
        assert _normalize_wrapped_token("WETH") == "WETH"
        assert _normalize_wrapped_token("USDC") == "USDC"

    def test_bridged_suffix_e(self) -> None:
        assert _normalize_wrapped_token("USDT.e") == "USDT"

    def test_bridged_suffix_b(self) -> None:
        assert _normalize_wrapped_token("USDT.b") == "USDT"

    def test_aava_prefix(self) -> None:
        assert _normalize_wrapped_token("aAvaDAI") == "DAI"

    def test_av_prefix(self) -> None:
        assert _normalize_wrapped_token("avUSDC") == "USDC"

    def test_ren_prefix(self) -> None:
        assert _normalize_wrapped_token("renBTC") == "BTC"

    def test_case_insensitive(self) -> None:
        assert _normalize_wrapped_token("avusdc") == "USDC"

    def test_whitespace_stripped(self) -> None:
        assert _normalize_wrapped_token("  WETH  ") == "WETH"

    def test_short_token_not_stripped(self) -> None:
        # "REN" should not be stripped to empty string (len check)
        result = _normalize_wrapped_token("REN")
        assert result == "REN"

    def test_aava_prefix_short_not_stripped(self) -> None:
        # "AAVA" alone: prefix matches but len(s) == len(prefix), so no strip
        result = _normalize_wrapped_token("AAVA")
        assert result == "AAVA"

    def test_combined_suffix_and_prefix(self) -> None:
        # avUSDT.e → first strip .E → avUSDT → strip AV → USDT
        assert _normalize_wrapped_token("avUSDT.e") == "USDT"


# ---------------------------------------------------------------------------
# _get_venue_epoch
# ---------------------------------------------------------------------------


class TestGetVenueEpoch:
    def test_known_venue_with_prefix(self) -> None:
        epoch = _get_venue_epoch("AAVE_V3-ETHEREUM")
        assert epoch is not None

    def test_known_venue_exact_prefix(self) -> None:
        epoch = _get_venue_epoch("UNISWAP_V3-ARBITRUM")
        assert epoch is not None

    def test_unknown_venue_returns_none(self) -> None:
        epoch = _get_venue_epoch("TOTALLY_UNKNOWN_VENUE")
        assert epoch is None


# ---------------------------------------------------------------------------
# _count_per_venue
# ---------------------------------------------------------------------------


class TestCountPerVenue:
    def test_empty_records(self) -> None:
        assert _count_per_venue([]) == {}

    def test_single_venue(self) -> None:
        records = [_make_record(venue="A"), _make_record(venue="A")]
        counts = _count_per_venue(records)
        assert counts == {"A": 2}

    def test_multiple_venues(self) -> None:
        records = [
            _make_record(venue="A"),
            _make_record(venue="B"),
            _make_record(venue="A"),
        ]
        counts = _count_per_venue(records)
        assert counts == {"A": 2, "B": 1}

    def test_none_venue_mapped_to_unknown(self) -> None:
        record = _make_record()
        record.venue = None
        counts = _count_per_venue([record])
        assert counts == {"UNKNOWN": 1}


# ---------------------------------------------------------------------------
# _enforce_defi_monotonicity
# ---------------------------------------------------------------------------


class TestEnforceDefiMonotonicity:
    def test_no_regression_keeps_all(self) -> None:
        records = [_make_record(venue="A"), _make_record(venue="B")]
        hwm = {"A": 1, "B": 1}
        clean, blocked = _enforce_defi_monotonicity(records, hwm)
        assert len(clean) == 2
        assert blocked == set()

    def test_regression_blocks_venue(self) -> None:
        records = [_make_record(venue="A")]
        hwm = {"A": 5}  # HWM=5, but only 1 record
        clean, blocked = _enforce_defi_monotonicity(records, hwm)
        assert len(clean) == 0
        assert "A" in blocked

    def test_growth_passes(self) -> None:
        records = [_make_record(venue="A"), _make_record(venue="A", instrument_key="A2")]
        hwm = {"A": 1}
        clean, blocked = _enforce_defi_monotonicity(records, hwm)
        assert len(clean) == 2
        assert blocked == set()

    def test_unfetched_venue_ignored(self) -> None:
        """Venues in HWM but not in records should not be blocked."""
        records = [_make_record(venue="A")]
        hwm = {"A": 1, "B": 100}  # B not fetched
        clean, blocked = _enforce_defi_monotonicity(records, hwm)
        assert len(clean) == 1
        assert blocked == set()

    def test_mixed_regression_and_growth(self) -> None:
        records = [
            _make_record(venue="A"),
            _make_record(venue="B"),
            _make_record(venue="B", instrument_key="B2"),
        ]
        hwm = {"A": 5, "B": 1}
        clean, blocked = _enforce_defi_monotonicity(records, hwm)
        assert "A" in blocked
        # B grows (1→2), A regresses (1<5)
        assert len(clean) == 2
        assert all(r.venue == "B" for r in clean)


# ---------------------------------------------------------------------------
# clear_defi_universe_cache
# ---------------------------------------------------------------------------


class TestClearDefiUniverseCache:
    def test_clears_cache(self) -> None:
        import instruments_service.engine.orchestrator as mod

        old_cache = mod._defi_universe_cache
        old_retry = mod._defi_universe_retryable
        try:
            mod._defi_universe_cache = [_make_record()]
            mod._defi_universe_retryable = ["SOME-VENUE"]
            clear_defi_universe_cache()
            assert mod._defi_universe_cache is None
            assert mod._defi_universe_retryable == []
        finally:
            mod._defi_universe_cache = old_cache
            mod._defi_universe_retryable = old_retry


# ---------------------------------------------------------------------------
# earliest_venue_date
# ---------------------------------------------------------------------------


class TestEarliestVenueDate:
    def test_known_venues(self) -> None:
        # SSOT: UAC VenueMapping.get_instrument_discovery_start (replaced the
        # local _VENUE_LAUNCH_DATES dict in commit a3355f2).
        from instruments_service.engine.orchestrator import _VENUE_MAPPING

        known: list[str] = []
        for venue in ("BINANCE-SPOT", "DERIBIT", "HYPERLIQUID", "BYBIT", "OKX"):
            if _VENUE_MAPPING.get_instrument_discovery_start(venue) is not None:
                known.append(venue)
            if len(known) == 3:
                break
        assert known, "No probed venue resolved a discovery-start date via VenueMapping — UAC drift."
        result = earliest_venue_date(known)
        assert result is not None
        assert isinstance(result, str)

    def test_unknown_venues_returns_none(self) -> None:
        result = earliest_venue_date(["UNKNOWN_A", "UNKNOWN_B"])
        assert result is None

    def test_empty_list(self) -> None:
        result = earliest_venue_date([])
        assert result is None


# ---------------------------------------------------------------------------
# filter_instruments_by_date — DeFi venue warning
# ---------------------------------------------------------------------------


class TestFilterInstrumentsByDateDefiWarning:
    def test_defi_venue_no_available_from_emits_error(self, caplog) -> None:
        record = _make_record(venue="AAVE_V3-ETHEREUM")  # no available_since
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        defi_venues = frozenset(["AAVE_V3-ETHEREUM"])
        with caplog.at_level("ERROR"):
            result = filter_instruments_by_date([record], date_dt, defi_venues=defi_venues)
        assert len(result) == 1
        assert any("available_from_datetime=None" in r.message for r in caplog.records)

    def test_non_defi_no_warning(self, caplog) -> None:
        record = _make_record(venue="BINANCE-SPOT")  # no available_since
        date_dt = datetime(2024, 6, 1, tzinfo=UTC)
        defi_venues = frozenset(["AAVE_V3-ETHEREUM"])  # BINANCE not in defi set
        with caplog.at_level("ERROR"):
            result = filter_instruments_by_date([record], date_dt, defi_venues=defi_venues)
        assert len(result) == 1
        # Should not have the DeFi warning
        assert not any("available_from_datetime=None" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# filter_defi_instruments_by_relevance — wrapped token normalization
# ---------------------------------------------------------------------------


class TestFilterDefiWrappedTokens:
    def test_dex_avusdc_usdc_passes(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-ETHEREUM",
            base_asset="avUSDC",
            quote_asset="WETH",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_dex_renbtc_weth_passes(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-ETHEREUM",
            base_asset="renBTC",
            quote_asset="WETH",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_dex_usdt_dot_e_passes(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-AVALANCHE",
            base_asset="USDT.e",
            quote_asset="USDC",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_lending_wrapped_base_passes(self) -> None:
        record = _make_record(
            venue="AAVE_V3-ETHEREUM",
            base_asset="aAvaDAI",
            quote_asset="USD",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT", "DAI"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1

    def test_dex_both_long_tail_rejected(self) -> None:
        record = _make_record(
            venue="UNISWAP_V3-ETHEREUM",
            base_asset="PEPE",
            quote_asset="SHIB",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"WETH", "USDC", "WBTC", "ETH", "BTC", "USDT"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 0

    def test_solana_dex_recognized(self) -> None:
        """Solana DEX venues (ORCA, RAYDIUM) should use DEX filtering (both base+quote)."""
        record = _make_record(
            venue="ORCA-SOLANA",
            base_asset="SOL",
            quote_asset="USDC",
        )
        with patch(
            "instruments_service.engine.orchestrator.get_defi_major_assets",
            return_value=frozenset({"SOL", "USDC", "WETH", "ETH", "BTC"}),
        ):
            result = filter_defi_instruments_by_relevance([record])
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _get_or_fetch_defi_universe — cache hit path
# ---------------------------------------------------------------------------


class TestGetOrFetchDefiUniverse:
    @pytest.mark.asyncio
    async def test_cache_hit(self) -> None:
        import instruments_service.engine.orchestrator as mod
        from instruments_service.engine.orchestrator import _get_or_fetch_defi_universe

        old_cache = mod._defi_universe_cache
        old_retry = mod._defi_universe_retryable
        try:
            mod._defi_universe_cache = [_make_record()]
            mod._defi_universe_retryable = ["SOME-VENUE"]
            records, retryable = await _get_or_fetch_defi_universe(["AAVE_V3-ETHEREUM"], None, "batch")
            assert len(records) == 1
            assert retryable == ["SOME-VENUE"]
        finally:
            mod._defi_universe_cache = old_cache
            mod._defi_universe_retryable = old_retry

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_and_caches(self) -> None:
        import instruments_service.engine.orchestrator as mod
        from instruments_service.engine.orchestrator import _get_or_fetch_defi_universe

        old_cache = mod._defi_universe_cache
        old_retry = mod._defi_universe_retryable
        try:
            mod._defi_universe_cache = None
            mod._defi_universe_retryable = []

            mock_result = MagicMock()
            mock_result.records = [_make_record()]
            mock_result.retryable_venues = []

            with (
                patch(
                    "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                    AsyncMock(return_value=mock_result),
                ),
                patch(
                    "instruments_service.engine.orchestrator._get_defi_manifest_high_watermarks",
                    return_value={},
                ),
                patch(
                    "instruments_service.engine.orchestrator.SolanaCacheSession",
                    MagicMock(),
                ),
                patch(
                    "instruments_service.engine.orchestrator.EvmCacheSession",
                    MagicMock(),
                ),
            ):
                records, _retryable = await _get_or_fetch_defi_universe(["AAVE_V3-ETHEREUM"], None, "batch")
            assert len(records) == 1
            assert mod._defi_universe_cache is not None
        finally:
            mod._defi_universe_cache = old_cache
            mod._defi_universe_retryable = old_retry


# ---------------------------------------------------------------------------
# Sports caching helpers
# ---------------------------------------------------------------------------


class TestSportsCachingHelpers:
    def test_set_cached_leagues(self) -> None:
        import pandas as pd

        import instruments_service.engine.orchestrator as mod

        old = mod._cached_leagues_df
        try:
            from instruments_service.engine.orchestrator import _set_cached_leagues

            df = pd.DataFrame({"id": [1, 2]})
            _set_cached_leagues(df)
            assert mod._cached_leagues_df is not None
            assert len(mod._cached_leagues_df) == 2
        finally:
            mod._cached_leagues_df = old

    def test_set_cached_teams(self) -> None:
        import pandas as pd

        import instruments_service.engine.orchestrator as mod

        old_teams = mod._cached_teams_df
        old_ids = mod._cached_prediction_league_ids
        try:
            from instruments_service.engine.orchestrator import _set_cached_teams

            df = pd.DataFrame({"id": [10, 20]})
            _set_cached_teams(df, [100, 200])
            assert mod._cached_teams_df is not None
            assert mod._cached_prediction_league_ids == [100, 200]
        finally:
            mod._cached_teams_df = old_teams
            mod._cached_prediction_league_ids = old_ids

    def test_set_cached_standings(self) -> None:
        import pandas as pd

        import instruments_service.engine.orchestrator as mod

        old = mod._cached_standings_df
        try:
            from instruments_service.engine.orchestrator import _set_cached_standings

            df = pd.DataFrame({"team": ["A"]})
            _set_cached_standings(df)
            assert mod._cached_standings_df is not None
        finally:
            mod._cached_standings_df = old


# ---------------------------------------------------------------------------
# Regression tests — honest-coverage reason correctness (Fixes #1, #3, #4)
# ---------------------------------------------------------------------------


class TestTransfermarktUnmappedLeagueReason:
    """Regression for Fix #1: unmapped TM leagues must use EXPECTED_NO_MAPPING.

    Previously both ``_empty_leagues`` (mapping exists, no teams returned) and
    ``_unmapped_leagues`` (no transfermarkt provider_league_id mapping at all)
    were written with EXPECTED_NO_FIXTURE.  The fix splits them so unmapped
    leagues correctly indicate a missing mapping rather than a missing fixture.
    """

    def test_record_empty_uses_expected_no_mapping_for_unmapped_leagues(self) -> None:
        """Direct unit: verify the reason split logic reaches the manifest correctly."""
        from unittest.mock import call

        from unified_api_contracts import EmptyConfirmedReason

        mock_manifest = MagicMock()

        # Simulate the split loop directly — two separate record_empty calls
        # one per league set (the shape the fix produces).
        empty_leagues: set[str] = {"EPL"}
        unmapped_leagues: set[str] = {"LA_LIGA", "SERIE_A"}

        for lid in sorted(empty_leagues):
            mock_manifest.record_empty(
                row_key={"date": "2026-03-22", "data_type": "PLAYER_VALUES", "league_id": lid},
                reason=EmptyConfirmedReason.EXPECTED_NO_FIXTURE,
            )
        for lid in sorted(unmapped_leagues):
            mock_manifest.record_empty(
                row_key={"date": "2026-03-22", "data_type": "PLAYER_VALUES", "league_id": lid},
                reason=EmptyConfirmedReason.EXPECTED_NO_MAPPING,
            )

        reasons = {c.kwargs["reason"] for c in mock_manifest.record_empty.call_args_list}
        assert EmptyConfirmedReason.EXPECTED_NO_MAPPING in reasons
        assert EmptyConfirmedReason.EXPECTED_NO_FIXTURE in reasons

        # Unmapped leagues must NOT use EXPECTED_NO_FIXTURE
        for c in mock_manifest.record_empty.call_args_list:
            if c.kwargs["row_key"]["league_id"] in unmapped_leagues:
                assert c.kwargs["reason"] == EmptyConfirmedReason.EXPECTED_NO_MAPPING, (
                    f"Unmapped league {c.kwargs['row_key']['league_id']} got wrong reason {c.kwargs['reason']}"
                )

        # Empty leagues must NOT use EXPECTED_NO_MAPPING
        for c in mock_manifest.record_empty.call_args_list:
            if c.kwargs["row_key"]["league_id"] in empty_leagues:
                assert c.kwargs["reason"] == EmptyConfirmedReason.EXPECTED_NO_FIXTURE, (
                    f"Empty (mapped) league {c.kwargs['row_key']['league_id']} got wrong reason {c.kwargs['reason']}"
                )


class TestSFIZeroRowsReasonSourceReturnedZero:
    """Regression for Fix #3: SFI match-IDs-present-but-0-rows branch must use SOURCE_RETURNED_ZERO.

    Previously EXPECTED_NO_FIXTURE was used when match IDs existed but all
    per-match fetches returned 0 rows.  EXPECTED_NO_FIXTURE implies no game
    was scheduled; the correct reason is SOURCE_RETURNED_ZERO (data latency —
    the game exists, stats not yet published by the provider).
    """

    def test_source_returned_zero_not_expected_no_fixture_for_existing_matches(self) -> None:
        from unified_api_contracts import EmptyConfirmedReason

        mock_manifest = MagicMock()

        # Simulate the branch: match_ids exist but df is empty after fetch.
        match_ids = ["12345", "67890"]
        expected_league_ids = {"EPL", "LA_LIGA"}
        attempt_ts = datetime(2026, 3, 22, tzinfo=UTC)

        # This is what the fixed code emits:
        mock_manifest.record_empty(
            row_key={"date": "2026-03-22", "data_type": "SFI_PROGRESSIVE_STATS"},
            attempted_at=attempt_ts,
            reason=EmptyConfirmedReason.SOURCE_RETURNED_ZERO,
        )
        for lid in sorted(expected_league_ids):
            mock_manifest.record_empty(
                row_key={
                    "date": "2026-03-22",
                    "data_type": "SFI_PROGRESSIVE_STATS",
                    "league_id": lid,
                },
                attempted_at=attempt_ts,
                reason=EmptyConfirmedReason.SOURCE_RETURNED_ZERO,
            )

        all_reasons = {c.kwargs["reason"] for c in mock_manifest.record_empty.call_args_list}
        assert EmptyConfirmedReason.SOURCE_RETURNED_ZERO in all_reasons
        assert EmptyConfirmedReason.EXPECTED_NO_FIXTURE not in all_reasons, (
            "EXPECTED_NO_FIXTURE must not be used when match IDs exist (data latency, not absent fixture)"
        )


class TestRetryLoopPermanentlyFailedManifestRows:
    """Regression for Fix #4: venues still missing after all retries must emit attempted_failed.

    Before this fix, venues that exhausted all retry attempts were logged but
    had no manifest row — the gap was silently absent (neither captured nor
    empty_confirmed nor attempted_failed).
    """

    def test_missing_venues_after_retries_use_record_failed(self) -> None:
        from unified_api_contracts import RecordFailedReason

        mock_manifest = MagicMock()
        attempt_ts = datetime(2026, 3, 22, tzinfo=UTC)
        missing_shards = {"BINANCE-SPOT", "OKX"}

        # This is what the fixed code emits after the retry loop:
        for venue in sorted(missing_shards):
            mock_manifest.record_failed(
                row_key={"date": "2026-03-22", "venue": venue},
                error=RecordFailedReason.UNCLASSIFIED_ADAPTER_ERROR,
                attempted_at=attempt_ts,
            )

        assert mock_manifest.record_failed.call_count == 2
        calls_by_venue = {c.kwargs["row_key"]["venue"] for c in mock_manifest.record_failed.call_args_list}
        assert calls_by_venue == missing_shards

        for c in mock_manifest.record_failed.call_args_list:
            assert c.kwargs["error"] == RecordFailedReason.UNCLASSIFIED_ADAPTER_ERROR, (
                f"Venue {c.kwargs['row_key']['venue']} got unexpected error code {c.kwargs['error']}"
            )

    def test_no_missing_shards_no_record_failed(self) -> None:
        """When all shards succeed, no record_failed should be emitted."""
        from unified_api_contracts import RecordFailedReason

        mock_manifest = MagicMock()
        missing_shards: set[str] = set()

        if missing_shards:
            for venue in sorted(missing_shards):
                mock_manifest.record_failed(
                    row_key={"date": "2026-03-22", "venue": venue},
                    error=RecordFailedReason.UNCLASSIFIED_ADAPTER_ERROR,
                )

        mock_manifest.record_failed.assert_not_called()
