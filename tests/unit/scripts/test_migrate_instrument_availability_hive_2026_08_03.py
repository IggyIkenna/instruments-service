"""Unit tests — migrate_instrument_availability_hive_2026_08_03.py.

Tests cover:
  1. hive_target_for: flat -> full-hive path mapping, per tree/asset_group/venue.
  2. Recognizes already-hive paths and unrecognized shapes as non-candidates.
  3. scan: bounded prefix listing classification (mocked client.list_blobs).
  4. _process_one_object: copy-if-missing / already-present-verified / content-mismatch / failed
     (mocked gcs_copy_object / gcs_describe_object).

No GCS or network access — pure helpers tested directly; GCS mocked.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_script() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "migrate_instrument_availability_hive_2026_08_03.py"
    module_name = "_migrate_instrument_availability_hive_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_script()

pytestmark = pytest.mark.unit

_CEFI_FLAT = "instrument_availability/by_date/day=2026-07-26/venue=HYPERLIQUID/instruments.parquet"
_CEFI_HIVE = (
    "instrument_availability/by_date/day=2026-07-26/pipeline_mode=batch_instruments_service/"
    "asset_group=cefi/venue=HYPERLIQUID/instruments.parquet"
)
_TRADFI_FUTURES_FLAT = "instrument_availability/by_date/day=2026-01-05/venue=CME/futures_contracts.parquet"
_PRED_POLY_FLAT = (
    "instrument_availability/by_date/day=2026-06-01/venue=POLYMARKET/"
    "canonical_question_group=BTC_UP_DOWN_HOURLY/instruments.parquet"
)
_PRED_KALSHI_FLAT = (
    "instrument_availability/by_date/day=2026-06-01/venue=KALSHI/canonical_question_group=OTHER/instruments.parquet"
)
_PRED_LIFECYCLE_POLY_FLAT = (
    "market_lifecycle/by_canonical_group/day=2026-06-01/venue=POLYMARKET/group=BTC_UP_DOWN_HOURLY/lifecycle.parquet"
)
_PRED_LIFECYCLE_KALSHI_FLAT = (
    "market_lifecycle/by_canonical_group/day=2026-06-01/venue=KALSHI/group=OTHER/lifecycle.parquet"
)
_ALREADY_HIVE = _CEFI_HIVE
# Within the scanned instrument_availability/by_date/ prefix (so a real GCS listing WOULD return it)
# but missing the venue= segment entirely -- an unrecognized shape, not a flat-candidate or hive object.
_UNRECOGNIZED = "instrument_availability/by_date/day=2026-07-26/some_other_shape.parquet"
# Sports league-sharded FLAT shape (todo 2 of instrument_availability_hive_migration_unrecognized_
# shapes_and_content_mismatch_2026_08_03.md) -- league= BEFORE venue=, confirmed live 2026-08-02.
_SPORTS_LEAGUE_FLAT = (
    "instrument_availability/by_date/day=2026-08-02/league=ARGENTINA_PRIMERA_NACIONAL/"
    "venue=API_FOOTBALL/instruments.parquet"
)
_SPORTS_LEAGUE_HIVE = (
    "instrument_availability/by_date/day=2026-08-02/pipeline_mode=batch_api_football/"
    "asset_group=sports/venue=API_FOOTBALL/league=ARGENTINA_PRIMERA_NACIONAL/instruments.parquet"
)
# Prediction canonical_question_group-BEFORE-day FLAT shape (todo 3 of
# instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md) --
# group= BEFORE day=, confirmed live (last write 2026-07-22T00:37:29Z, not written since).
_PRED_GROUP_FIRST_FLAT = (
    "instrument_availability/by_date/canonical_question_group=BTC_UP_DOWN_HOURLY/"
    "day=2026-07-13/venue=POLYMARKET/instruments.parquet"
)
_PRED_GROUP_FIRST_HIVE = (
    "instrument_availability/by_date/day=2026-07-13/pipeline_mode=batch_polymarket_clob/"
    "asset_group=prediction/venue=POLYMARKET/canonical_question_group=BTC_UP_DOWN_HOURLY/instruments.parquet"
)
_PRED_GROUP_FIRST_FLAT_KALSHI = (
    "instrument_availability/by_date/canonical_question_group=OTHER/day=2026-07-13/venue=KALSHI/instruments.parquet"
)
# Prediction market_lifecycle legacy FLAT shape (same todo, adjacent finding) -- day=/group=/venue=
# (venue third, not immediately after day=, so it never matched _FLAT_RE). Confirmed live, same
# cutover timing as the shape above.
_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT = (
    "market_lifecycle/by_canonical_group/day=2026-07-22/group=BTC_UP_DOWN_DAILY/"
    "venue=POLYMARKET/market_lifecycle.parquet"
)
_PRED_LIFECYCLE_DAY_GROUP_VENUE_HIVE = (
    "market_lifecycle/by_canonical_group/day=2026-07-22/pipeline_mode=batch_polymarket_gamma_api/"
    "asset_group=prediction/venue=POLYMARKET/group=BTC_UP_DOWN_DAILY/market_lifecycle.parquet"
)
_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT_KALSHI = (
    "market_lifecycle/by_canonical_group/day=2026-07-22/group=OTHER/venue=KALSHI/market_lifecycle.parquet"
)


class TestHiveTargetFor:
    def test_cefi_flat_maps_to_batch_instruments_service(self) -> None:
        assert _mod.hive_target_for(_CEFI_FLAT, "cefi") == _CEFI_HIVE

    def test_tradfi_futures_contracts_shares_tree(self) -> None:
        result = _mod.hive_target_for(_TRADFI_FUTURES_FLAT, "tradfi")
        assert result is not None
        assert "pipeline_mode=batch_instruments_service/asset_group=tradfi/venue=CME" in result
        assert result.endswith("futures_contracts.parquet")

    def test_prediction_polymarket_instrument_availability_uses_clob(self) -> None:
        result = _mod.hive_target_for(_PRED_POLY_FLAT, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_polymarket_clob/asset_group=prediction/venue=POLYMARKET" in result
        assert result.endswith("canonical_question_group=BTC_UP_DOWN_HOURLY/instruments.parquet")

    def test_prediction_kalshi_instrument_availability_uses_kalshi(self) -> None:
        result = _mod.hive_target_for(_PRED_KALSHI_FLAT, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_kalshi/asset_group=prediction/venue=KALSHI" in result

    def test_market_lifecycle_polymarket_uses_gamma_api(self) -> None:
        result = _mod.hive_target_for(_PRED_LIFECYCLE_POLY_FLAT, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_polymarket_gamma_api/asset_group=prediction/venue=POLYMARKET" in result

    def test_market_lifecycle_kalshi_uses_batch_instruments_service(self) -> None:
        result = _mod.hive_target_for(_PRED_LIFECYCLE_KALSHI_FLAT, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_instruments_service/asset_group=prediction/venue=KALSHI" in result

    def test_already_hive_path_returns_none(self) -> None:
        assert _mod.hive_target_for(_ALREADY_HIVE, "cefi") is None

    def test_unrecognized_tree_returns_none(self) -> None:
        assert _mod.hive_target_for(_UNRECOGNIZED, "cefi") is None

    def test_day_before_pipeline_mode_before_asset_group_before_venue_order(self) -> None:
        result = _mod.hive_target_for(_CEFI_FLAT, "cefi")
        assert result is not None
        idx_day = result.index("day=")
        idx_pm = result.index("pipeline_mode=")
        idx_ag = result.index("asset_group=")
        idx_venue = result.index("venue=")
        assert idx_day < idx_pm < idx_ag < idx_venue

    def test_sports_league_flat_maps_to_hive_with_trailing_league(self) -> None:
        assert _mod.hive_target_for(_SPORTS_LEAGUE_FLAT, "sports") == _SPORTS_LEAGUE_HIVE

    def test_sports_league_flat_uses_batch_api_football_pipeline_mode(self) -> None:
        result = _mod.hive_target_for(_SPORTS_LEAGUE_FLAT, "sports")
        assert result is not None
        assert "pipeline_mode=batch_api_football/asset_group=sports/venue=API_FOOTBALL" in result

    def test_sports_league_key_is_trailing_after_venue(self) -> None:
        result = _mod.hive_target_for(_SPORTS_LEAGUE_FLAT, "sports")
        assert result is not None
        idx_venue = result.index("venue=")
        idx_league = result.index("league=")
        assert idx_venue < idx_league
        assert result.endswith("league=ARGENTINA_PRIMERA_NACIONAL/instruments.parquet")

    def test_sports_league_flat_not_recognized_for_other_asset_groups(self) -> None:
        # The league-before-venue shape is sports-specific; other asset_groups never emit it,
        # so hive_target_for must not (mis)recognize it under a different asset_group.
        assert _mod.hive_target_for(_SPORTS_LEAGUE_FLAT, "cefi") is None

    def test_prediction_group_first_flat_maps_to_hive_with_trailing_group(self) -> None:
        assert _mod.hive_target_for(_PRED_GROUP_FIRST_FLAT, "prediction") == _PRED_GROUP_FIRST_HIVE

    def test_prediction_group_first_flat_kalshi_uses_kalshi_pipeline_mode(self) -> None:
        result = _mod.hive_target_for(_PRED_GROUP_FIRST_FLAT_KALSHI, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_kalshi/asset_group=prediction/venue=KALSHI" in result
        assert result.endswith("canonical_question_group=OTHER/instruments.parquet")

    def test_prediction_group_first_flat_not_recognized_for_other_asset_groups(self) -> None:
        # canonical_question_group= is a prediction-only concept -- guard against misrecognition.
        assert _mod.hive_target_for(_PRED_GROUP_FIRST_FLAT, "cefi") is None

    def test_prediction_lifecycle_day_group_venue_flat_maps_to_hive_with_trailing_group(self) -> None:
        result = _mod.hive_target_for(_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT, "prediction")
        assert result == _PRED_LIFECYCLE_DAY_GROUP_VENUE_HIVE

    def test_prediction_lifecycle_day_group_venue_flat_kalshi_uses_batch_instruments_service(self) -> None:
        result = _mod.hive_target_for(_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT_KALSHI, "prediction")
        assert result is not None
        assert "pipeline_mode=batch_instruments_service/asset_group=prediction/venue=KALSHI" in result
        assert result.endswith("group=OTHER/market_lifecycle.parquet")

    def test_prediction_lifecycle_day_group_venue_flat_not_recognized_for_other_asset_groups(self) -> None:
        assert _mod.hive_target_for(_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT, "cefi") is None


class TestScan:
    def _mock_client(self, names: list[str]) -> MagicMock:
        # MagicMock(name=...) sets the mock's own repr name, not a `.name` attribute — set it explicitly.
        blobs = []
        for n in names:
            b = MagicMock()
            b.name = n
            blobs.append(b)
        client = MagicMock()
        client.list_blobs.side_effect = lambda bucket, prefix: [b for b in blobs if b.name.startswith(prefix)]
        return client

    def test_classifies_candidates_already_hive_and_unrecognized(self) -> None:
        client = self._mock_client([_CEFI_FLAT, _ALREADY_HIVE, _UNRECOGNIZED])
        result = _mod.scan(client, "instruments-store-cefi-prd", "cefi")
        assert result.candidate_count == 1
        assert result.candidates[0] == (_CEFI_FLAT, _CEFI_HIVE)
        assert result.already_hive == 1
        assert result.unrecognized == 1

    def test_prediction_scans_both_trees(self) -> None:
        client = self._mock_client([_PRED_POLY_FLAT, _PRED_LIFECYCLE_POLY_FLAT])
        result = _mod.scan(client, "instruments-store-prediction-prd", "prediction")
        assert result.candidate_count == 2

    def test_non_prediction_does_not_scan_market_lifecycle(self) -> None:
        client = self._mock_client([_CEFI_FLAT, _PRED_LIFECYCLE_POLY_FLAT])
        result = _mod.scan(client, "instruments-store-cefi-prd", "cefi")
        # market_lifecycle prefix is never queried for non-prediction asset groups.
        assert result.candidate_count == 1

    def test_sports_scan_recognizes_league_sharded_candidates(self) -> None:
        client = self._mock_client([_SPORTS_LEAGUE_FLAT, _ALREADY_HIVE, _UNRECOGNIZED])
        result = _mod.scan(client, "instruments-store-sports-prd", "sports")
        assert result.candidate_count == 1
        assert result.candidates[0] == (_SPORTS_LEAGUE_FLAT, _SPORTS_LEAGUE_HIVE)

    def test_prediction_scan_recognizes_group_first_and_lifecycle_day_group_venue_candidates(self) -> None:
        client = self._mock_client(
            [
                _PRED_GROUP_FIRST_FLAT,
                _PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT,
                _ALREADY_HIVE,
                _UNRECOGNIZED,
            ]
        )
        result = _mod.scan(client, "instruments-store-prediction-prd", "prediction")
        assert result.candidate_count == 2
        assert (_PRED_GROUP_FIRST_FLAT, _PRED_GROUP_FIRST_HIVE) in result.candidates
        assert (
            _PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT,
            _PRED_LIFECYCLE_DAY_GROUP_VENUE_HIVE,
        ) in result.candidates


class TestProcessOneObject:
    def _meta(self, crc32c: str, size: int) -> MagicMock:
        m = MagicMock()
        m.crc32c = crc32c
        m.size = size
        return m

    def test_copies_when_target_absent(self) -> None:
        src_meta = self._meta("AAAA==", 100)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[None, src_meta, src_meta]) as mock_describe,
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "copied"
        assert detail is None
        mock_copy.assert_called_once_with(f"gs://bkt/{_CEFI_FLAT}", f"gs://bkt/{_CEFI_HIVE}")
        assert mock_describe.call_count == 3

    def test_already_present_verified_when_metadata_matches(self) -> None:
        meta = self._meta("BBBB==", 200)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[meta, meta]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            _src, outcome, _detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "already_present"
        mock_copy.assert_not_called()

    def test_content_mismatch_when_target_present_but_different(self) -> None:
        dst_meta = self._meta("CCCC==", 300)
        src_meta = self._meta("DDDD==", 301)
        with patch.object(_mod, "gcs_describe_object", side_effect=[dst_meta, src_meta]):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "content_mismatch"
        assert detail is not None

    def test_source_vanished_when_source_missing(self) -> None:
        with patch.object(_mod, "gcs_describe_object", side_effect=[None, None]):
            _src, outcome, _detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "source_vanished"

    def test_failed_on_unexpected_exception(self) -> None:
        with patch.object(_mod, "gcs_describe_object", side_effect=RuntimeError("boom")):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "failed"
        assert "boom" in (detail or "")

    def test_post_copy_verify_failure_flagged_as_content_mismatch(self) -> None:
        src_meta = self._meta("EEEE==", 400)
        mismatched_post = self._meta("FFFF==", 401)
        with (
            patch.object(_mod, "gcs_describe_object", side_effect=[None, src_meta, mismatched_post]),
            patch.object(_mod, "gcs_copy_object"),
        ):
            _src, outcome, detail = _mod._process_one_object("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert outcome == "content_mismatch"
        assert "post-copy" in (detail or "")


class TestBucketFor:
    def test_prediction_uses_dedicated_kind(self) -> None:
        with patch.object(_mod, "resolve_bucket_name", return_value="instruments-store-prediction-prd") as mock_rbn:
            result = _mod._bucket_for("prediction")
        mock_rbn.assert_called_once_with(cloud="gcp", kind="instruments-store-prediction")
        assert result == "instruments-store-prediction-prd"

    def test_non_prediction_passes_asset_group(self) -> None:
        with patch.object(_mod, "resolve_bucket_name", return_value="instruments-store-cefi-prd") as mock_rbn:
            result = _mod._bucket_for("cefi")
        mock_rbn.assert_called_once_with(cloud="gcp", kind="instruments-store", asset_group="cefi")


# ---------------------------------------------------------------------------
# todo 6 -- content_mismatch resolver (ruled "superset wins" policy, todo 4)
# ---------------------------------------------------------------------------


class TestIdentityColumnFor:
    def test_futures_contracts_uses_contract_symbol(self) -> None:
        assert _mod._identity_column_for(_TRADFI_FUTURES_FLAT) == "contract_symbol"

    def test_instruments_parquet_uses_raw_symbol(self) -> None:
        assert _mod._identity_column_for(_CEFI_FLAT) == "raw_symbol"

    def test_market_lifecycle_parquet_uses_market_id(self) -> None:
        # market_lifecycle.parquet has no raw_symbol column (schema: market_id,
        # canonical_question_group, ...) — must not fall back to raw_symbol default.
        assert _mod._identity_column_for(_PRED_LIFECYCLE_DAY_GROUP_VENUE_FLAT_KALSHI) == "market_id"


class TestIdentitySet:
    def test_extracts_non_null_values(self) -> None:
        import pandas as pd

        buf = pd.DataFrame({"raw_symbol": ["BTC-USD", "ETH-USD", None]}).to_parquet(index=False)
        assert _mod._identity_set(buf, "raw_symbol") == frozenset({"BTC-USD", "ETH-USD"})


class TestResolveOneMismatch:
    @staticmethod
    def _parquet(symbols: list[str]) -> bytes:
        import pandas as pd

        return pd.DataFrame({"raw_symbol": symbols}).to_parquet(index=False)

    def test_flat_strict_superset_wins_and_copies(self) -> None:
        flat = self._parquet(["A", "B", "C"])
        hive = self._parquet(["A", "B"])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (hive, 1)]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "flat_wins"
        mock_copy.assert_called_once_with(f"gs://bkt/{_CEFI_FLAT}", f"gs://bkt/{_CEFI_HIVE}")

    def test_hive_strict_superset_wins_no_write(self) -> None:
        flat = self._parquet(["A"])
        hive = self._parquet(["A", "B", "C"])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (hive, 1)]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "hive_wins"
        mock_copy.assert_not_called()

    def test_tied_membership_defaults_to_flat_bytes(self) -> None:
        flat = self._parquet(["A", "B"])
        hive = self._parquet(["A", "B"])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (hive, 1)]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "tie_flat_bytes"
        mock_copy.assert_called_once()

    def test_disjoint_sets_flagged_not_auto_resolved(self) -> None:
        flat = self._parquet(["A", "B"])
        hive = self._parquet(["C", "D"])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (hive, 1)]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "disjoint_needs_review"
        assert res.detail is not None
        mock_copy.assert_not_called()

    def test_vanished_object_flagged_failed(self) -> None:
        flat = self._parquet(["A"])
        with patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (None, 0)]):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "failed"
        assert "vanished" in (res.detail or "")

    def test_unexpected_exception_flagged_failed(self) -> None:
        with patch.object(_mod, "gcs_read_object_with_generation", side_effect=RuntimeError("boom")):
            res = _mod._resolve_one_mismatch("bkt", _CEFI_FLAT, _CEFI_HIVE)
        assert res.outcome == "failed"
        assert "boom" in (res.detail or "")

    def test_futures_contracts_pair_compares_on_contract_symbol(self) -> None:
        import pandas as pd

        flat = pd.DataFrame({"contract_symbol": ["ESH26", "ESM26"]}).to_parquet(index=False)
        hive = pd.DataFrame({"contract_symbol": ["ESH26"]}).to_parquet(index=False)
        with (
            patch.object(_mod, "gcs_read_object_with_generation", side_effect=[(flat, 1), (hive, 1)]),
            patch.object(_mod, "gcs_copy_object") as mock_copy,
        ):
            res = _mod._resolve_one_mismatch("bkt", _TRADFI_FUTURES_FLAT, _CEFI_HIVE)
        assert res.outcome == "flat_wins"
        mock_copy.assert_called_once()


class TestResolveContentMismatches:
    def test_aggregates_outcomes_and_counts_by_type(self) -> None:
        apply_result = _mod.ApplyResult(asset_group="cefi", content_mismatch=[_CEFI_FLAT])
        resolution = _mod.MismatchResolution(_CEFI_FLAT, _CEFI_HIVE, "flat_wins")
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "apply_asset_group", return_value=apply_result),
            patch.object(_mod, "hive_target_for", return_value=_CEFI_HIVE),
            patch.object(_mod, "_resolve_one_mismatch", return_value=resolution),
        ):
            result = _mod.resolve_content_mismatches(asset_group="cefi", workers=2)
        assert result.flat_wins == 1
        assert result.hive_wins == 0
        assert result.ok

    def test_disjoint_and_failed_collected_for_review(self) -> None:
        apply_result = _mod.ApplyResult(asset_group="cefi", content_mismatch=[_CEFI_FLAT, _CEFI_FLAT])
        outcomes = [
            _mod.MismatchResolution(_CEFI_FLAT, _CEFI_HIVE, "disjoint_needs_review", "flat=1 hive=1 ids"),
            _mod.MismatchResolution(_CEFI_FLAT, _CEFI_HIVE, "failed", "boom"),
        ]
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "apply_asset_group", return_value=apply_result),
            patch.object(_mod, "hive_target_for", return_value=_CEFI_HIVE),
            patch.object(_mod, "_resolve_one_mismatch", side_effect=outcomes),
        ):
            result = _mod.resolve_content_mismatches(asset_group="cefi", workers=2)
        assert len(result.disjoint) == 1
        assert len(result.failed) == 1
        assert not result.ok

    def test_unmapped_src_skipped(self) -> None:
        apply_result = _mod.ApplyResult(asset_group="cefi", content_mismatch=[_CEFI_FLAT])
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "apply_asset_group", return_value=apply_result),
            patch.object(_mod, "hive_target_for", return_value=None),
            patch.object(_mod, "_resolve_one_mismatch") as mock_resolve,
        ):
            result = _mod.resolve_content_mismatches(asset_group="cefi", workers=2)
        mock_resolve.assert_not_called()
        assert result.flat_wins == 0
        assert result.ok


# ---------------------------------------------------------------------------
# todo 8 — market-bucket reclassify (DEFAULT-RULED 2026-08-06, option (a))
# ---------------------------------------------------------------------------

_PRED_MARKET_FLAT = "instrument_availability/by_date/day=2025-03-15/market=BTC/venue=POLYMARKET/instruments.parquet"
_PRED_MARKET_FLAT_KALSHI = "instrument_availability/by_date/day=2025-03-15/market=ETH/venue=KALSHI/instruments.parquet"


class TestPredictionMarketFlatRe:
    def test_matches_market_flat_shape(self) -> None:
        m = _mod._PREDICTION_MARKET_FLAT_RE.match(_PRED_MARKET_FLAT)
        assert m is not None
        day, market, venue, tail = m.groups()
        assert day == "2025-03-15"
        assert market == "BTC"
        assert venue == "POLYMARKET"
        assert tail == "instruments.parquet"

    def test_does_not_match_already_hive(self) -> None:
        assert _mod._PREDICTION_MARKET_FLAT_RE.match(_ALREADY_HIVE) is None

    def test_does_not_match_flat_re_shape(self) -> None:
        assert _mod._PREDICTION_MARKET_FLAT_RE.match(_CEFI_FLAT) is None

    def test_does_not_match_group_first_shape(self) -> None:
        assert _mod._PREDICTION_MARKET_FLAT_RE.match(_PRED_GROUP_FIRST_FLAT) is None


class TestScanMarketShape:
    def _mock_client(self, names: list[str]) -> MagicMock:
        blobs = []
        for n in names:
            b = MagicMock()
            b.name = n
            blobs.append(b)
        client = MagicMock()
        client.list_blobs.side_effect = lambda bucket, prefix: [b for b in blobs if b.name.startswith(prefix)]
        return client

    def test_market_flat_goes_to_market_reclassify_not_candidates(self) -> None:
        client = self._mock_client([_PRED_MARKET_FLAT, _PRED_POLY_FLAT])
        result = _mod.scan(client, "instruments-store-prediction-prd", "prediction")
        assert result.market_reclassify_count == 1
        assert result.market_reclassify == [_PRED_MARKET_FLAT]
        # _PRED_POLY_FLAT is the old flat shape (recognized by _FLAT_RE), becomes a regular candidate
        assert result.candidate_count == 1

    def test_market_flat_not_counted_as_unrecognized(self) -> None:
        client = self._mock_client([_PRED_MARKET_FLAT])
        result = _mod.scan(client, "instruments-store-prediction-prd", "prediction")
        assert result.unrecognized == 0

    def test_market_flat_not_recognized_for_non_prediction_asset_groups(self) -> None:
        # market= is a prediction-only legacy shape; must not be misclassified for other asset_groups.
        client = self._mock_client([_PRED_MARKET_FLAT])
        result = _mod.scan(client, "instruments-store-cefi-prd", "cefi")
        assert result.market_reclassify_count == 0
        assert result.unrecognized == 1


class TestClassifyMarketRowGroup:
    @staticmethod
    def _row(**kwargs: object) -> MagicMock:
        row = MagicMock()
        row.get = lambda k, default=None: kwargs.get(k, default)
        return row

    def test_polymarket_delegates_to_uac_classifier(self) -> None:
        row = self._row(raw_symbol="bitcoin-up-or-down-2025-03-15", instrument_key="0xabc")
        with patch.object(
            _mod, "classify_polymarket_to_canonical_group", return_value=MagicMock(value="BTC_UP_DOWN_DAILY")
        ) as mock_cls:
            result = _mod._classify_market_row_group(row, "POLYMARKET")
        mock_cls.assert_called_once_with(
            title="", slug="bitcoin-up-or-down-2025-03-15", event_slug="", outcome="", condition_id="0xabc"
        )
        assert result == "BTC_UP_DOWN_DAILY"

    def test_kalshi_strips_composite_key_before_classifying(self) -> None:
        row = self._row(instrument_key="KALSHI:PREDICTION_MARKET:KXBTCD-25JUL30-B12345")
        with patch.object(
            _mod, "classify_kalshi_to_canonical_group", return_value=MagicMock(value="BTC_UP_DOWN_DAILY")
        ) as mock_cls:
            result = _mod._classify_market_row_group(row, "KALSHI")
        mock_cls.assert_called_once_with(ticker="KXBTCD-25JUL30-B12345")
        assert result == "BTC_UP_DOWN_DAILY"

    def test_polymarket_none_return_defaults_to_other(self) -> None:
        row = self._row(raw_symbol="unknown-slug", instrument_key="")
        with patch.object(_mod, "classify_polymarket_to_canonical_group", return_value=None):
            result = _mod._classify_market_row_group(row, "POLYMARKET")
        assert result == _mod.CanonicalQuestionGroup.OTHER.value

    def test_unknown_venue_defaults_to_other(self) -> None:
        row = self._row(raw_symbol="something")
        result = _mod._classify_market_row_group(row, "UNKNOWN_VENUE")
        assert result == _mod.CanonicalQuestionGroup.OTHER.value


class TestReclassifyOneMarketObject:
    @staticmethod
    def _parquet(rows: list[dict[str, object]]) -> bytes:
        import pandas as pd

        return pd.DataFrame(rows).to_parquet(index=False)

    def test_writes_one_hive_target_per_group(self) -> None:
        data = self._parquet(
            [
                {"raw_symbol": "btc-up-2025-03-15", "instrument_key": "0xabc"},
                {"raw_symbol": "spx-up-2025-03-15", "instrument_key": "0xdef"},
            ]
        )
        with (
            patch.object(_mod, "gcs_read_object_with_generation", return_value=(data, 1)),
            patch.object(
                _mod,
                "classify_polymarket_to_canonical_group",
                side_effect=[
                    MagicMock(value="BTC_UP_DOWN_DAILY"),
                    MagicMock(value="SPX_UP_DOWN_DAILY"),
                ],
            ),
            patch.object(_mod, "gcs_conditional_put", return_value=42) as mock_put,
        ):
            written, already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert written == 2
        assert already_present == 0
        assert failed == []
        assert mock_put.call_count == 2
        # Verify both target paths contain the expected canonical_question_group
        call_args = [c.args[0] for c in mock_put.call_args_list]
        assert any("canonical_question_group=BTC_UP_DOWN_DAILY" in p for p in call_args)
        assert any("canonical_question_group=SPX_UP_DOWN_DAILY" in p for p in call_args)

    def test_already_present_when_conditional_put_returns_none(self) -> None:
        data = self._parquet([{"raw_symbol": "btc-up", "instrument_key": "0xabc"}])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", return_value=(data, 1)),
            patch.object(
                _mod, "classify_polymarket_to_canonical_group", return_value=MagicMock(value="BTC_UP_DOWN_DAILY")
            ),
            patch.object(_mod, "gcs_conditional_put", return_value=None),
        ):
            written, already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert written == 0
        assert already_present == 1
        assert failed == []

    def test_download_failure_reported_as_failed(self) -> None:
        with patch.object(_mod, "gcs_read_object_with_generation", side_effect=RuntimeError("network")):
            written, already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert written == 0
        assert already_present == 0
        assert len(failed) == 1
        assert "download failed" in failed[0][2]

    def test_vanished_source_reported_as_failed(self) -> None:
        with patch.object(_mod, "gcs_read_object_with_generation", return_value=(None, 0)):
            _written, _already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert len(failed) == 1
        assert "vanished" in failed[0][2]

    def test_empty_dataframe_returns_zero_counts(self) -> None:
        import pandas as pd

        data = pd.DataFrame({"raw_symbol": [], "instrument_key": []}).to_parquet(index=False)
        with patch.object(_mod, "gcs_read_object_with_generation", return_value=(data, 1)):
            written, already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert written == 0
        assert already_present == 0
        assert failed == []

    def test_target_path_structure(self) -> None:
        data = self._parquet([{"raw_symbol": "btc-up", "instrument_key": "0xabc"}])
        captured_uris: list[str] = []
        with (
            patch.object(_mod, "gcs_read_object_with_generation", return_value=(data, 1)),
            patch.object(
                _mod, "classify_polymarket_to_canonical_group", return_value=MagicMock(value="BTC_UP_DOWN_DAILY")
            ),
            patch.object(
                _mod, "gcs_conditional_put", side_effect=lambda uri, *a, **kw: captured_uris.append(uri) or 42
            ),
        ):
            _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert len(captured_uris) == 1
        uri = captured_uris[0]
        assert "gs://bkt/" in uri
        assert "day=2025-03-15" in uri
        assert "pipeline_mode=batch_polymarket_clob" in uri
        assert "asset_group=prediction" in uri
        assert "venue=POLYMARKET" in uri
        assert "canonical_question_group=BTC_UP_DOWN_DAILY" in uri
        assert uri.endswith("instruments.parquet")

    def test_write_failure_collected(self) -> None:
        data = self._parquet([{"raw_symbol": "btc-up", "instrument_key": "0xabc"}])
        with (
            patch.object(_mod, "gcs_read_object_with_generation", return_value=(data, 1)),
            patch.object(
                _mod, "classify_polymarket_to_canonical_group", return_value=MagicMock(value="BTC_UP_DOWN_DAILY")
            ),
            patch.object(_mod, "gcs_conditional_put", side_effect=RuntimeError("quota")),
        ):
            written, _already_present, failed = _mod._reclassify_one_market_object("bkt", _PRED_MARKET_FLAT)
        assert written == 0
        assert len(failed) == 1
        assert "write failed" in failed[0][2]


class TestReclassifyMarketShapeObjects:
    def test_aggregates_written_and_already_present(self) -> None:
        scan_result = _mod.ScanResult(market_reclassify=[_PRED_MARKET_FLAT, _PRED_MARKET_FLAT_KALSHI])
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "get_storage_client"),
            patch.object(_mod, "scan", return_value=scan_result),
            patch.object(
                _mod,
                "_reclassify_one_market_object",
                side_effect=[
                    (2, 0, []),
                    (0, 1, []),
                ],
            ),
        ):
            result = _mod.reclassify_market_shape_objects(asset_group="prediction", workers=2)
        assert result.objects_processed == 2
        assert result.groups_written == 2
        assert result.groups_already_present == 1
        assert result.ok

    def test_failed_groups_collected(self) -> None:
        scan_result = _mod.ScanResult(market_reclassify=[_PRED_MARKET_FLAT])
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "get_storage_client"),
            patch.object(_mod, "scan", return_value=scan_result),
            patch.object(
                _mod,
                "_reclassify_one_market_object",
                return_value=(0, 0, [(_PRED_MARKET_FLAT, "BTC_UP_DOWN_DAILY", "boom")]),
            ),
        ):
            result = _mod.reclassify_market_shape_objects(asset_group="prediction", workers=1)
        assert len(result.groups_failed) == 1
        assert not result.ok

    def test_empty_market_reclassify_list_returns_early(self) -> None:
        scan_result = _mod.ScanResult()
        with (
            patch.object(_mod, "_bucket_for", return_value="bkt"),
            patch.object(_mod, "get_storage_client"),
            patch.object(_mod, "scan", return_value=scan_result),
            patch.object(_mod, "_reclassify_one_market_object") as mock_reclassify,
        ):
            result = _mod.reclassify_market_shape_objects(asset_group="prediction", workers=1)
        mock_reclassify.assert_not_called()
        assert result.objects_processed == 0
        assert result.ok
