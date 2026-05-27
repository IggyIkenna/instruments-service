"""Unit tests for engine/data_utils.py — covers all branches for coverage."""

from __future__ import annotations

import pandas as pd
import pytest

from instruments_service.engine.data_utils import (
    af_id_from_canonical,
    canonical_league_id,
    coerce_adapter_output,
    compute_prediction_shards,
    count_per_venue,
    extract_prediction_canonical_group,
    normalize_wrapped_token,
)


class TestCanonicalLeagueId:
    def test_string_stripped(self) -> None:
        assert canonical_league_id("  39  ") == "39"

    def test_int_converted(self) -> None:
        assert canonical_league_id(39) == "39"

    def test_float_converted_to_int_str(self) -> None:
        assert canonical_league_id(39.0) == "39"

    def test_object_with_league_id_attr(self) -> None:
        class Obj:
            league_id = "PL"

        assert canonical_league_id(Obj()) == "PL"

    def test_fallback_str(self) -> None:
        assert canonical_league_id(None) == "None"


class TestCoerceAdapterOutput:
    def test_dict_passthrough(self) -> None:
        d = {"venue": "BINANCE"}
        assert coerce_adapter_output(d) is d

    def test_object_with_dict(self) -> None:
        class Obj:
            def __init__(self) -> None:
                self.venue = "BINANCE"

        result = coerce_adapter_output(Obj())
        assert "venue" in result

    def test_namedtuple_style(self) -> None:
        from collections import namedtuple

        NT = namedtuple("NT", ["venue"])
        obj = NT(venue="BINANCE")
        result = coerce_adapter_output(obj)
        assert result["venue"] == "BINANCE"

    def test_raw_fallback(self) -> None:
        result = coerce_adapter_output(42)
        assert result == {"raw": 42}


class TestAfIdFromCanonical:
    def test_dict_with_digit_af_fixture_id(self) -> None:
        assert af_id_from_canonical({"af_fixture_id": "123"}) == 123

    def test_dict_with_non_digit_af_fixture_id(self) -> None:
        assert af_id_from_canonical({"af_fixture_id": "abc"}) is None

    def test_dict_without_af_fixture_id(self) -> None:
        assert af_id_from_canonical({"other": "val"}) is None

    def test_dict_with_none_af_fixture_id(self) -> None:
        assert af_id_from_canonical({"af_fixture_id": None}) is None

    def test_object_with_af_fixture_id_attr(self) -> None:
        class Obj:
            af_fixture_id = "456"

        assert af_id_from_canonical(Obj()) == 456

    def test_object_with_none_af_fixture_id_attr(self) -> None:
        class Obj:
            af_fixture_id = None

        assert af_id_from_canonical(Obj()) is None

    def test_object_without_af_fixture_id(self) -> None:
        assert af_id_from_canonical(object()) is None

    def test_non_digit_object_attr(self) -> None:
        class Obj:
            af_fixture_id = "xyz"

        assert af_id_from_canonical(Obj()) is None


class TestNormalizeWrappedToken:
    def test_weth_normalized(self) -> None:
        assert normalize_wrapped_token("WETH") == "ETH"

    def test_wbtc_normalized(self) -> None:
        assert normalize_wrapped_token("WBTC") == "BTC"

    def test_wsol_normalized(self) -> None:
        assert normalize_wrapped_token("WSOL") == "SOL"

    def test_wavax_normalized(self) -> None:
        assert normalize_wrapped_token("WAVAX") == "AVAX"

    def test_wmatic_normalized(self) -> None:
        assert normalize_wrapped_token("WMATIC") == "MATIC"

    def test_wbnb_normalized(self) -> None:
        assert normalize_wrapped_token("WBNB") == "BNB"

    def test_unknown_w_prefix_unchanged(self) -> None:
        assert normalize_wrapped_token("WDOGE") == "WDOGE"

    def test_no_w_prefix_unchanged(self) -> None:
        assert normalize_wrapped_token("ETH") == "ETH"

    def test_w_alone_unchanged(self) -> None:
        assert normalize_wrapped_token("W") == "W"


class TestExtractPredictionCanonicalGroup:
    def test_category_used_when_present(self) -> None:
        row = pd.Series({"category": "  Crypto  ", "group": "other"})
        assert extract_prediction_canonical_group(row) == "crypto"

    def test_group_used_when_no_category(self) -> None:
        row = pd.Series({"group": "Politics"})
        assert extract_prediction_canonical_group(row) == "politics"

    def test_other_when_no_relevant_fields(self) -> None:
        row = pd.Series({"venue": "POLYMARKET"})
        assert extract_prediction_canonical_group(row) == "other"


class TestComputePredictionShards:
    def test_empty_df_returns_empty(self) -> None:
        assert compute_prediction_shards(pd.DataFrame()) == {}

    def test_no_group_column_returns_empty(self) -> None:
        df = pd.DataFrame({"venue": ["POLYMARKET"]})
        assert compute_prediction_shards(df) == {}

    def test_group_counts_returned(self) -> None:
        df = pd.DataFrame({"group": ["crypto", "crypto", "politics"]})
        result = compute_prediction_shards(df)
        assert result["crypto"] == 2
        assert result["politics"] == 1


class TestCountPerVenue:
    def test_dict_records(self) -> None:
        records = [{"venue": "BINANCE"}, {"venue": "BINANCE"}, {"venue": "BYBIT"}]
        result = count_per_venue(records)
        assert result["BINANCE"] == 2
        assert result["BYBIT"] == 1

    def test_object_records(self) -> None:
        class Rec:
            def __init__(self, venue: str) -> None:
                self.venue = venue

        records = [Rec("BINANCE"), Rec("OKX")]
        result = count_per_venue(records)
        assert result["BINANCE"] == 1
        assert result["OKX"] == 1

    def test_unknown_fallback(self) -> None:
        records = [42]
        result = count_per_venue(records)
        assert result["unknown"] == 1

    def test_empty_iterable(self) -> None:
        assert count_per_venue([]) == {}
