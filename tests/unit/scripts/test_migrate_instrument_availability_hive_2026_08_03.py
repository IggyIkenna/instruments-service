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
        assert result == "instruments-store-cefi-prd"
