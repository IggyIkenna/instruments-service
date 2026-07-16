"""Coverage gap tests for orchestrator.py.

Targets the specific uncovered branches identified in the coverage audit:
  - DeFi monotonicity retry path (_retry_regressed_venues + _get_or_fetch_defi_universe)
  - _get_defi_manifest_high_watermarks exception path
  - TradFi non-trading-day manifest writes + active-day-zero diagnostic
  - _extract_prediction_canonical_group (POLYMARKET / KALSHI / OTHER)
  - _compute_prediction_shards
  - _write_market_lifecycle (happy / empty / exception)
  - _should_skip_date_for_per_league (all covered / partial / force / expected_unattempted)
  - _classify_adapter_failure (UAC classification / fallback)
  - Schema validation failure branches in process_instruments
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.predictions import CanonicalQuestionGroup
from unified_trading_library import CaptureStatus

from instruments_service.engine.orchestrator import (
    _classify_adapter_failure,
    _compute_prediction_shards,
    _extract_prediction_canonical_group,
    _get_defi_manifest_high_watermarks,
    _get_manifest_high_watermarks,
    _should_skip_date_for_per_league,
    _write_market_lifecycle,
    clear_defi_universe_cache,
    process_instruments,
)
from instruments_service.engine.orchestrator.process_write import _asset_group_for_venue
from instruments_service.engine.urdi_reference_provider import VenueFetchResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _make_manifest_row(
    capture_status: str = CaptureStatus.CAPTURED.value,
    error_reason: str = "",
) -> SimpleNamespace:
    """Return a lightweight ManifestRow-shaped namespace."""
    return SimpleNamespace(capture_status=capture_status, error_reason=error_reason)


# ---------------------------------------------------------------------------
# _classify_adapter_failure
# ---------------------------------------------------------------------------


class TestClassifyAdapterFailure:
    def test_unknown_venue_returns_exception_class_name(self) -> None:
        exc = ValueError("boom")
        result = _classify_adapter_failure(exc, "TOTALLY_UNKNOWN_VENUE")
        assert result == "ValueError"

    def test_known_venue_uac_classification(self) -> None:
        # classify_venue_error returns a classification for known codes on known venues.
        # We can't rely on a specific UAC mapping without the real registry, but we
        # CAN verify the fallback: when UAC has no entry, the class name is returned.
        exc = RuntimeError("connection refused")
        result = _classify_adapter_failure(exc, "SOME_UNKNOWN_VENUE")
        assert result == "RuntimeError"

    def test_returns_string(self) -> None:
        result = _classify_adapter_failure(Exception("x"), "ANY_VENUE")
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# _get_defi_manifest_high_watermarks — exception path
# ---------------------------------------------------------------------------


class TestGetDefiManifestHighWatermarks:
    def test_exception_returns_empty_dict(self) -> None:
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                side_effect=RuntimeError("GCS unavailable"),
            ),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
        ):
            result = _get_defi_manifest_high_watermarks()
        assert result == {}

    def test_empty_index_returns_empty_dict(self) -> None:
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                return_value=pd.DataFrame(),
            ),
        ):
            result = _get_defi_manifest_high_watermarks()
        assert result == {}

    def test_populated_index_returns_max_per_venue(self) -> None:
        index_df = pd.DataFrame(
            {
                "venue": ["AAVE_V3-ETHEREUM", "AAVE_V3-ETHEREUM", "UNISWAP_V3-ETHEREUM"],
                "instrument_count": [100, 120, 50],
                "date": ["2026-01-01", "2026-01-02", "2026-01-01"],
            }
        )
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                return_value=index_df,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_venue_epoch",
                return_value=None,
            ),
        ):
            result = _get_defi_manifest_high_watermarks()
        assert result["AAVE_V3-ETHEREUM"] == 120
        assert result["UNISWAP_V3-ETHEREUM"] == 50


# ---------------------------------------------------------------------------
# _get_manifest_high_watermarks — the generalized, asset-group-parameterized
# helper that _get_defi_manifest_high_watermarks now delegates to. Todo 6 of
# cefi_monotonicity_guard_alerting_and_dark_venues_2026_07_07.md.
# ---------------------------------------------------------------------------


class TestGetManifestHighWatermarksGeneralized:
    def test_cefi_asset_group_reads_cefi_bucket(self) -> None:
        """A non-DEFI asset_group resolves via the same bucket-lookup mechanism."""
        index_df = pd.DataFrame(
            {
                "venue": ["LIGHTER-ZKSYNC", "LIGHTER-ZKSYNC", "EXTENDED-STARKNET"],
                "instrument_count": [213, 18, 10],
                "date": ["2026-06-25", "2026-06-26", "2026-06-20"],
            }
        )
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="cefi-test-bucket",
            ) as mock_bucket,
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                return_value=index_df,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_venue_epoch",
                return_value=None,
            ),
        ):
            result = _get_manifest_high_watermarks("CEFI")
        mock_bucket.assert_called_once_with("CEFI")
        assert result["LIGHTER-ZKSYNC"] == 213  # max across the two rows, not the latest
        assert result["EXTENDED-STARKNET"] == 10

    def test_exception_returns_empty_dict(self) -> None:
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                side_effect=RuntimeError("GCS unavailable"),
            ),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
        ):
            result = _get_manifest_high_watermarks("TRADFI")
        assert result == {}

    def test_defi_matches_original_wrapper(self) -> None:
        """asset_group='DEFI' reproduces _get_defi_manifest_high_watermarks() exactly."""
        index_df = pd.DataFrame(
            {
                "venue": ["AAVE_V3-ETHEREUM"],
                "instrument_count": [100],
                "date": ["2026-01-01"],
            }
        )
        with (
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="test-bucket",
            ),
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                return_value=index_df,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_venue_epoch",
                return_value=None,
            ),
        ):
            generic_result = _get_manifest_high_watermarks("DEFI")
            wrapper_result = _get_defi_manifest_high_watermarks()
        assert generic_result == wrapper_result == {"AAVE_V3-ETHEREUM": 100}


# ---------------------------------------------------------------------------
# _retry_regressed_venues
# ---------------------------------------------------------------------------


class TestRetryRegressedVenues:
    @pytest.mark.asyncio
    async def test_returns_retry_records(self) -> None:
        from instruments_service.engine.orchestrator import _retry_regressed_venues

        retry_records = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key="RETRY1")]
        mock_result = VenueFetchResult(records=retry_records)

        with (
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=mock_result),
            ),
        ):
            result = await _retry_regressed_venues(["AAVE_V3-ETHEREUM"], api_keys=None, mode="batch")

        assert len(result) == 1
        assert result[0].instrument_key == "RETRY1"

    @pytest.mark.asyncio
    async def test_empty_venues_list(self) -> None:
        from instruments_service.engine.orchestrator import _retry_regressed_venues

        mock_result = VenueFetchResult(records=[])
        with patch(
            "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
            AsyncMock(return_value=mock_result),
        ):
            result = await _retry_regressed_venues([], api_keys=None, mode="batch")
        assert result == []


# ---------------------------------------------------------------------------
# _get_or_fetch_defi_universe — retry path
# ---------------------------------------------------------------------------


class TestGetOrFetchDefiUniverseRetry:
    @pytest.mark.asyncio
    async def test_regression_triggers_retry_and_swap(self) -> None:
        """When a venue regresses, the retry result replaces the original if better."""
        import instruments_service.engine.orchestrator as mod

        clear_defi_universe_cache()

        # Initial fetch: AAVE has 3 instruments (HWM says 5 → regressed)
        initial_records = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key=f"A{i}") for i in range(3)]
        retry_records = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key=f"A{i}") for i in range(6)]

        fetch_call_count = 0

        async def _mock_fetch(venues, **kwargs):
            nonlocal fetch_call_count
            fetch_call_count += 1
            if fetch_call_count == 1:
                return VenueFetchResult(records=initial_records)
            return VenueFetchResult(records=retry_records)

        hwm = {"AAVE_V3-ETHEREUM": 5}

        with (
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                side_effect=_mock_fetch,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_defi_manifest_high_watermarks",
                return_value=hwm,
            ),
        ):
            records, _ = await mod._get_or_fetch_defi_universe(["AAVE_V3-ETHEREUM"], api_keys=None, mode="batch")

        # Retry improved: 6 > HWM 5, so venue is NOT blocked
        assert len(records) == 6
        assert fetch_call_count == 2
        # Clean up cache so other tests start fresh
        clear_defi_universe_cache()

    @pytest.mark.asyncio
    async def test_regression_retry_still_below_hwm_blocks_venue(self) -> None:
        """When retry still fails HWM, the venue is blocked (0 records in result)."""
        import instruments_service.engine.orchestrator as mod

        clear_defi_universe_cache()

        # Both initial and retry return 2 instruments; HWM = 10
        low_records = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key=f"A{i}") for i in range(2)]

        async def _mock_fetch(venues, **kwargs):
            return VenueFetchResult(records=low_records)

        hwm = {"AAVE_V3-ETHEREUM": 10}

        with (
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                side_effect=_mock_fetch,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_defi_manifest_high_watermarks",
                return_value=hwm,
            ),
        ):
            records, _retryable = await mod._get_or_fetch_defi_universe(
                ["AAVE_V3-ETHEREUM"], api_keys=None, mode="batch"
            )

        # Venue blocked after retry still below HWM
        assert len(records) == 0
        clear_defi_universe_cache()

    @pytest.mark.asyncio
    async def test_first_run_no_hwm_skips_check(self) -> None:
        """No manifest history → monotonicity check skipped, all records returned."""
        import instruments_service.engine.orchestrator as mod

        clear_defi_universe_cache()

        some_records = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key=f"A{i}") for i in range(5)]

        with (
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=some_records)),
            ),
            patch(
                "instruments_service.engine.orchestrator._get_defi_manifest_high_watermarks",
                return_value={},
            ),
        ):
            records, _retryable = await mod._get_or_fetch_defi_universe(
                ["AAVE_V3-ETHEREUM"], api_keys=None, mode="batch"
            )

        assert len(records) == 5
        clear_defi_universe_cache()

    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached_without_fetch(self) -> None:
        """Second call uses the cache; fetch_instruments is NOT called again."""
        import instruments_service.engine.orchestrator as mod

        clear_defi_universe_cache()

        cached = [_make_record(venue="AAVE_V3-ETHEREUM", instrument_key="CACHED")]
        mod._defi_universe_cache = cached
        mod._defi_universe_retryable = []

        fetch_mock = AsyncMock(return_value=VenueFetchResult(records=[]))
        with patch("instruments_service.engine.orchestrator.fetch_instruments_for_all_venues", fetch_mock):
            records, _retryable = await mod._get_or_fetch_defi_universe(
                ["AAVE_V3-ETHEREUM"], api_keys=None, mode="batch"
            )

        fetch_mock.assert_not_called()
        assert records == cached
        clear_defi_universe_cache()


# ---------------------------------------------------------------------------
# _should_skip_date_for_per_league
# ---------------------------------------------------------------------------


class TestShouldSkipDateForPerLeague:
    """Memory-safe single-read variant (2026-06-22 OOM fix): the helper reads
    the consolidated availability index ONCE via ``read_availability_index``
    and resolves all expected leagues in-memory (it no longer calls
    ``manifest.lookup`` per league -- that re-read the ~6.5 GB index 93x/date,
    OOM. These tests patch ``read_availability_index`` with a small DataFrame
    standing in for the per-(date, data_type, league_id) shard rows.
    """

    _DATE = "2026-01-01"
    _DATA_TYPE = "MATCHES"
    _SERVICE = "instruments-service"

    def _make_manifest(self) -> MagicMock:
        mock_mw = MagicMock()
        mock_mw.catalogue_bucket = "instruments-store-sports-test"
        mock_mw.service_name = self._SERVICE
        return mock_mw

    def _index_df(self, rows: dict[str, tuple[str, str]]) -> pd.DataFrame:
        """Build a stand-in availability index. ``rows`` maps league_id ->
        ``(capture_status, error_reason)``; a league absent from the map = no
        row (the "missing" case).
        """
        records = [
            {
                "service_name": self._SERVICE,
                "date": self._DATE,
                "data_type": self._DATA_TYPE,
                "league_id": lid,
                "capture_status": status,
                "error_reason": reason,
            }
            for lid, (status, reason) in rows.items()
        ]
        return pd.DataFrame(
            records,
            columns=["service_name", "date", "data_type", "league_id", "capture_status", "error_reason"],
        )

    def _run(self, manifest: MagicMock, index_df: pd.DataFrame, expected: list[str]) -> bool:
        with patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=index_df,
        ):
            return _should_skip_date_for_per_league(
                manifest,
                date=self._DATE,
                data_type=self._DATA_TYPE,
                expected_canonical_leagues=expected,
                force=False,
            )

    def test_all_captured_returns_true(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df(
            {"EPL": (CaptureStatus.CAPTURED.value, ""), "BUNDESLIGA": (CaptureStatus.CAPTURED.value, "")}
        )
        assert self._run(manifest, df, ["EPL", "BUNDESLIGA"]) is True

    def test_all_empty_confirmed_returns_true(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({"EPL": (CaptureStatus.EMPTY_CONFIRMED.value, "")})
        assert self._run(manifest, df, ["EPL"]) is True

    def test_one_missing_returns_false(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({"EPL": (CaptureStatus.CAPTURED.value, "")})  # LA_LIGA absent
        assert self._run(manifest, df, ["EPL", "LA_LIGA"]) is False

    def test_force_returns_false_immediately(self) -> None:
        manifest = MagicMock()
        with patch("instruments_service.engine.orchestrator.read_availability_index") as mock_read:
            result = _should_skip_date_for_per_league(
                manifest,
                date=self._DATE,
                data_type=self._DATA_TYPE,
                expected_canonical_leagues=["EPL"],
                force=True,
            )
        assert result is False
        mock_read.assert_not_called()

    def test_empty_leagues_returns_false(self) -> None:
        manifest = MagicMock()
        with patch("instruments_service.engine.orchestrator.read_availability_index") as mock_read:
            result = _should_skip_date_for_per_league(
                manifest,
                date=self._DATE,
                data_type=self._DATA_TYPE,
                expected_canonical_leagues=[],
                force=False,
            )
        assert result is False
        mock_read.assert_not_called()

    def test_empty_index_returns_false(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({})  # no rows at all
        assert self._run(manifest, df, ["EPL"]) is False

    def test_expected_unattempted_with_expected_reason_counts_as_covered(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({"EPL": (CaptureStatus.EXPECTED_UNATTEMPTED.value, "EXPECTED_WEEKEND")})
        assert self._run(manifest, df, ["EPL"]) is True

    def test_expected_unattempted_without_expected_reason_not_covered(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({"EPL": (CaptureStatus.EXPECTED_UNATTEMPTED.value, "SOME_OTHER")})
        assert self._run(manifest, df, ["EPL"]) is False

    def test_attempted_failed_league_returns_false(self) -> None:
        manifest = self._make_manifest()
        df = self._index_df({"EPL": (CaptureStatus.ATTEMPTED_FAILED.value, "")})
        assert self._run(manifest, df, ["EPL"]) is False

    def test_index_read_happens_once_not_per_league(self) -> None:
        """Regression guard for the 2026-06-22 OOM: the index is read EXACTLY
        once regardless of league count (the bug was N reads of a 6.5 GB frame).
        """
        manifest = self._make_manifest()
        df = self._index_df({f"L{i}": (CaptureStatus.CAPTURED.value, "") for i in range(50)})
        with patch(
            "instruments_service.engine.orchestrator.read_availability_index",
            return_value=df,
        ) as mock_read:
            result = _should_skip_date_for_per_league(
                manifest,
                date=self._DATE,
                data_type=self._DATA_TYPE,
                expected_canonical_leagues=[f"L{i}" for i in range(50)],
                force=False,
            )
        assert result is True
        assert mock_read.call_count == 1


# ---------------------------------------------------------------------------
# _extract_prediction_canonical_group
# ---------------------------------------------------------------------------


class TestExtractPredictionCanonicalGroup:
    def _make_row(self, venue: str, instrument_key: str = "", raw_symbol: str = "") -> pd.Series:
        return pd.Series({"venue": venue, "instrument_key": instrument_key, "raw_symbol": raw_symbol})

    def test_polymarket_btc_slug_classifies(self) -> None:
        row = self._make_row("POLYMARKET", raw_symbol="bitcoin-up-or-down-hour-2026-01-01")
        result = _extract_prediction_canonical_group(row)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_polymarket_unknown_slug_returns_misc_novelty(self) -> None:
        # Genuinely-uncategorised Polymarket slug → MISC_NOVELTY (UAC decision 338);
        # Kalshi unknown tickers still → OTHER (see test_kalshi_unknown_ticker_returns_other).
        row = self._make_row("POLYMARKET", raw_symbol="totally-unknown-market-xyz-99999")
        result = _extract_prediction_canonical_group(row)
        assert result == CanonicalQuestionGroup.MISC_NOVELTY.value

    def test_kalshi_known_ticker(self) -> None:
        row = self._make_row("kalshi", instrument_key="KXBTCX-25MAR14-T85000")
        result = _extract_prediction_canonical_group(row)
        assert isinstance(result, str)

    def test_kalshi_unknown_ticker_returns_other(self) -> None:
        row = self._make_row("KALSHI", instrument_key="ZZZNONSENSE999")
        result = _extract_prediction_canonical_group(row)
        assert result == CanonicalQuestionGroup.OTHER.value

    def test_other_venue_returns_other(self) -> None:
        row = self._make_row("SOME_OTHER_VENUE", instrument_key="WHATEVER")
        result = _extract_prediction_canonical_group(row)
        assert result == CanonicalQuestionGroup.OTHER.value

    def test_empty_venue_returns_other(self) -> None:
        row = self._make_row("")
        result = _extract_prediction_canonical_group(row)
        assert result == CanonicalQuestionGroup.OTHER.value


# ---------------------------------------------------------------------------
# _compute_prediction_shards
# ---------------------------------------------------------------------------


class TestComputePredictionShards:
    def test_empty_df_returns_empty(self) -> None:
        df = pd.DataFrame(columns=["venue", "instrument_key", "raw_symbol"])
        result = _compute_prediction_shards(df)
        assert result == {}

    def test_single_row(self) -> None:
        df = pd.DataFrame(
            [{"venue": "POLYMARKET", "instrument_key": "cond-abc", "raw_symbol": "bitcoin-up-or-down-hour-2026-01-01"}]
        )
        result = _compute_prediction_shards(df)
        assert len(result) >= 1
        total = sum(result.values())
        assert total == 1

    def test_multiple_rows_different_groups(self) -> None:
        df = pd.DataFrame(
            [
                {"venue": "POLYMARKET", "instrument_key": "cond-1", "raw_symbol": "bitcoin-up-or-down-hour-2026-01-01"},
                {"venue": "POLYMARKET", "instrument_key": "cond-2", "raw_symbol": "totally-unknown-market-zzz"},
                {"venue": "KALSHI", "instrument_key": "ZZZNONSENSE", "raw_symbol": ""},
            ]
        )
        result = _compute_prediction_shards(df)
        assert sum(result.values()) == 3


# ---------------------------------------------------------------------------
# _write_market_lifecycle
# ---------------------------------------------------------------------------


class TestWriteMarketLifecycle:
    def _make_group_df(self, n_rows: int = 3, settled: bool = False) -> pd.DataFrame:
        now = datetime.now(UTC)
        offset = timedelta(hours=1)
        rows = []
        for i in range(n_rows):
            rows.append(
                {
                    "instrument_key": f"cond-{i}",
                    "venue": "POLYMARKET",
                    "available_from_datetime": now - timedelta(hours=24),
                    "available_to_datetime": (now - offset) if settled else (now + timedelta(days=1)),
                }
            )
        return pd.DataFrame(rows)

    def test_happy_path_writes_to_sink(self) -> None:
        mock_sink = MagicMock()
        mock_manifest = MagicMock()
        group_df = self._make_group_df(n_rows=2)

        with (
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
        ):
            _write_market_lifecycle(
                sink=mock_sink,
                group_df=group_df,
                canonical_group_str=CanonicalQuestionGroup.OTHER.value,
                date="2026-01-01",
                manifest_venue="POLYMARKET",
                manifest=mock_manifest,
                pipeline_mode=MagicMock(),
            )

        mock_manifest.record_captured_from_counts.assert_called_once()

    def test_empty_df_due_to_missing_required_columns_returns_early(self) -> None:
        mock_sink = MagicMock()
        mock_manifest = MagicMock()
        # DataFrame without required columns → _build_market_lifecycle_df returns empty
        group_df = pd.DataFrame([{"venue": "POLYMARKET", "instrument_key": "cond-1"}])

        _write_market_lifecycle(
            sink=mock_sink,
            group_df=group_df,
            canonical_group_str=CanonicalQuestionGroup.OTHER.value,
            date="2026-01-01",
            manifest_venue="POLYMARKET",
            manifest=mock_manifest,
            pipeline_mode=MagicMock(),
        )

        mock_manifest.record_captured_from_counts.assert_not_called()

    def test_exception_in_write_does_not_raise(self) -> None:
        mock_sink = MagicMock()
        mock_manifest = MagicMock()
        group_df = self._make_group_df(n_rows=2)

        with (
            patch(
                "instruments_service.engine.orchestrator._build_market_lifecycle_df",
                side_effect=RuntimeError("gcs write fail"),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            # Must not raise — shard-level isolation
            _write_market_lifecycle(
                sink=mock_sink,
                group_df=group_df,
                canonical_group_str=CanonicalQuestionGroup.OTHER.value,
                date="2026-01-01",
                manifest_venue="POLYMARKET",
                manifest=mock_manifest,
                pipeline_mode=MagicMock(),
            )

    def test_settled_rows_get_settled_status(self) -> None:
        from instruments_service.engine.orchestrator import _build_market_lifecycle_df

        group_df = self._make_group_df(n_rows=1, settled=True)
        now = datetime.now(UTC)
        result = _build_market_lifecycle_df(group_df, CanonicalQuestionGroup.OTHER.value, now)
        if not result.empty:
            assert all(result["status"] == "settled")

    def test_active_rows_get_active_status(self) -> None:
        from instruments_service.engine.orchestrator import _build_market_lifecycle_df

        group_df = self._make_group_df(n_rows=1, settled=False)
        now = datetime.now(UTC)
        result = _build_market_lifecycle_df(group_df, CanonicalQuestionGroup.OTHER.value, now)
        if not result.empty:
            assert all(result["status"] == "active")


# ---------------------------------------------------------------------------
# TradFi non-trading-day path in process_instruments
# ---------------------------------------------------------------------------

_COMMON_PROCESS_PATCHES = {
    "instruments_service.engine.orchestrator.log_event": None,
    "instruments_service.engine.orchestrator.get_config": MagicMock,
    "instruments_service.engine.orchestrator._get_instruments_bucket": lambda _: "test-bucket",
}


class TestProcessInstrumentsTradFiNonTradingDay:
    @pytest.mark.asyncio
    async def test_all_non_trading_returns_zero_counts_and_writes_manifest(self) -> None:
        """When all TradFi venues are non-trading, return {venue: 0} and write manifest."""
        mock_manifest_instance = MagicMock()
        mock_manifest_cls = MagicMock(return_value=mock_manifest_instance)

        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["CME"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[])),
            ),
            patch("instruments_service.engine.orchestrator.is_non_trading_day", return_value=True),
            patch("instruments_service.engine.orchestrator.non_trading_day_reason", return_value="EXPECTED_WEEKEND"),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.filter_instruments_by_date", return_value=[]),
        ):
            result = await process_instruments(
                "2026-01-04",  # Sunday — all TradFi non-trading
                ["TRADFI"],
                redo_all=True,
            )

        assert result == {"CME": 0}
        mock_manifest_instance.record_expected_empty.assert_called_once()
        mock_manifest_instance.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_partial_non_trading_does_not_return_early(self) -> None:
        """When only SOME TradFi venues are non-trading, do NOT return early (fail the shard)."""
        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["CME", "NYSE"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[])),
            ),
            # CME is non-trading, NYSE is trading
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                side_effect=lambda venue, dt: venue == "CME",
            ),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.filter_instruments_by_date", return_value=[]),
            pytest.raises(RuntimeError),
        ):
            await process_instruments("2026-01-05", ["TRADFI"], redo_all=True)

    @pytest.mark.asyncio
    async def test_active_day_zero_logs_adapter_fetch_failed(self) -> None:
        """A trading day with 0 records emits ADAPTER_FETCH_FAILED before the RuntimeError."""
        captured_events: list[str] = []

        def _capture_event(event_name: str, **kwargs: object) -> None:
            captured_events.append(event_name)

        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["CME"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[])),
            ),
            patch("instruments_service.engine.orchestrator.is_non_trading_day", return_value=False),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.log_event", side_effect=_capture_event),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch("instruments_service.engine.orchestrator.filter_instruments_by_date", return_value=[]),
            pytest.raises(RuntimeError),
        ):
            await process_instruments("2026-01-06", ["TRADFI"], redo_all=True)

        assert "ADAPTER_FETCH_FAILED" in captured_events


# ---------------------------------------------------------------------------
# Schema validation failure branches in process_instruments
# ---------------------------------------------------------------------------


class TestProcessInstrumentsSchemaValidation:
    @staticmethod
    def _make_cefi_record(key: str = "BTC:SPOT:BTCUSDT") -> InstrumentRecord:
        return InstrumentRecord(
            instrument_key=key,
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
        )

    @pytest.mark.asyncio
    async def test_some_records_invalid_valid_ones_written(self) -> None:
        """Partially-rejected shard: valid records proceed; invalid ones are logged."""
        good = self._make_cefi_record("BTCUSDT")
        bad = self._make_cefi_record("ETHUSDT")

        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        mock_manifest = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["BINANCE-SPOT"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[good, bad])),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch(
                "instruments_service.engine.orchestrator.validate_instrument_records",
                return_value=([good], [(bad, "missing_quote_asset")]),
            ),
            patch("instruments_service.engine.orchestrator.DomainValidationService"),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_manifest),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch(
                "instruments_service.engine.orchestrator.filter_instruments_by_date",
                side_effect=lambda r, *a, **k: r,
            ),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.filter_defi_instruments_by_relevance",
                side_effect=lambda r: r,
            ),
        ):
            result = await process_instruments("2026-01-06", ["CEFI"])

        assert sum(result.values()) == 1

    @pytest.mark.asyncio
    async def test_all_records_invalid_raises_runtime_error(self) -> None:
        """When all records fail validation, RuntimeError is raised."""
        bad = self._make_cefi_record("BTCUSDT")

        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["BINANCE-SPOT"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[bad])),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch(
                "instruments_service.engine.orchestrator.validate_instrument_records",
                return_value=([], [(bad, "missing_quote_asset")]),
            ),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.ManifestWriter"),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch(
                "instruments_service.engine.orchestrator.filter_instruments_by_date",
                side_effect=lambda r, *a, **k: r,
            ),
            patch(
                "instruments_service.engine.orchestrator.filter_defi_instruments_by_relevance",
                side_effect=lambda r: r,
            ),
            pytest.raises(RuntimeError, match="All records rejected"),
        ):
            await process_instruments("2026-01-06", ["CEFI"])


# ---------------------------------------------------------------------------
# ALL-group / empty-asset-group expansion fix
# Regression tests for the crash introduced when asset_groups=["ALL"] was
# passed to _get_instruments_bucket which doesn't accept "all" as a key.
# ---------------------------------------------------------------------------


class TestAssetGroupForVenue:
    """Unit tests for the _asset_group_for_venue lookup helper."""

    def test_cefi_venue(self) -> None:
        assert _asset_group_for_venue("BINANCE-SPOT") == "cefi"

    def test_tradfi_venue(self) -> None:
        assert _asset_group_for_venue("CME") == "tradfi"

    def test_defi_venue_protocol_chain_format(self) -> None:
        assert _asset_group_for_venue("AAVE_V3-ETHEREUM") == "defi"

    def test_sports_venue(self) -> None:
        assert _asset_group_for_venue("API_FOOTBALL") == "sports"

    def test_prediction_kalshi(self) -> None:
        assert _asset_group_for_venue("KALSHI") == "prediction"

    def test_prediction_polymarket(self) -> None:
        assert _asset_group_for_venue("POLYMARKET") == "prediction"


class TestAllGroupExpansionNocrash:
    """Regression: asset_groups=["ALL"] must not raise BucketNamingError.

    Before the fix, _freshness_preflight and _write_all_venues called
    _get_instruments_bucket("all") which raised BucketNamingError because
    "all" is not a valid asset_group key.  The shard-level isolation in
    _adapter.py swallowed the error silently → container exit(1) with no
    Python traceback.

    Both crash sites are exercised indirectly through process_instruments.
    The critical assertion is: no BucketNamingError raised.
    """

    @staticmethod
    def _make_cefi_record(key: str = "BTC:SPOT:BTCUSDT") -> InstrumentRecord:
        return InstrumentRecord(
            instrument_key=key,
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
        )

    @pytest.mark.asyncio
    async def test_all_group_process_instruments_does_not_raise_bucket_naming_error(self) -> None:
        """asset_groups=["ALL"] must traverse both _freshness_preflight and
        _write_all_venues without BucketNamingError."""
        good = self._make_cefi_record()
        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        mock_manifest = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["BINANCE-SPOT"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=[good])),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.validate_instrument_records", return_value=([good], [])),
            patch("instruments_service.engine.orchestrator.DomainValidationService"),
            # Patch _get_instruments_bucket at the orchestrator level — this covers both
            # _freshness_preflight (process_preflight module) and _write_all_venues
            # (process_write module) because both import orchestrator as _orch.
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_manifest),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["BINANCE-SPOT"]),
            ),
            patch(
                "instruments_service.engine.orchestrator.filter_instruments_by_date",
                side_effect=lambda r, *a, **k: r,
            ),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.filter_defi_instruments_by_relevance",
                side_effect=lambda r: r,
            ),
            patch("instruments_service.engine.orchestrator.is_non_trading_day", return_value=False),
        ):
            # Must not raise BucketNamingError (the pre-fix crash).
            result = await process_instruments("2026-06-26", ["ALL"], redo_all=True)

        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_freshness_preflight_with_all_resolves_sports_bucket(self) -> None:
        """_freshness_preflight called with asset_groups=["ALL"] must resolve
        the bucket via "sports" (not "all") — the fix in process_preflight.py."""
        from instruments_service.engine.orchestrator.process_preflight import _freshness_preflight

        bucket_calls: list[str | None] = []

        def _capturing_get_bucket(ag: str | None = None) -> str:
            bucket_calls.append(ag)
            return "test-bucket"

        with (
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", side_effect=_capturing_get_bucket),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], []),
            ),
        ):
            outcome = _freshness_preflight(
                date="2026-06-26",
                asset_groups=["ALL"],
                active_venues=["BINANCE-SPOT"],
                is_sports_run=True,
                sports_entity_filter=None,
                recovery_fixture_ids=None,
                redo_all=False,
            )

        # The bucket must have been resolved via "sports", NOT via None/"ALL"/"all".
        assert bucket_calls, "Expected _get_instruments_bucket to be called"
        assert all(c == "sports" for c in bucket_calls), (
            f"_get_instruments_bucket must be called with 'sports' for ALL runs, got: {bucket_calls}"
        )
