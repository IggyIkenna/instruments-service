"""Unit tests for instruments_service.engine.data_utils and validation_utils.

These utility modules were extracted from orchestrator.py. They are not
imported elsewhere; these tests provide coverage for the extracted logic.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# data_utils
# ---------------------------------------------------------------------------


class TestDataUtils:
    def test_canonical_league_id_string(self) -> None:
        from instruments_service.engine.data_utils import canonical_league_id

        assert canonical_league_id("EPL") == "EPL"
        assert canonical_league_id("  EPL  ") == "EPL"

    def test_canonical_league_id_int(self) -> None:
        from instruments_service.engine.data_utils import canonical_league_id

        assert canonical_league_id(39) == "39"
        assert canonical_league_id(39.0) == "39"

    def test_canonical_league_id_fallback(self) -> None:
        from instruments_service.engine.data_utils import canonical_league_id

        assert canonical_league_id(None) == "None"

    def test_coerce_adapter_output_dict(self) -> None:
        from instruments_service.engine.data_utils import coerce_adapter_output

        d = {"venue": "BINANCE", "date": "2024-01-01"}
        assert coerce_adapter_output(d) is d

    def test_coerce_adapter_output_with_dict_attr(self) -> None:
        from instruments_service.engine.data_utils import coerce_adapter_output

        class _Obj:
            def __init__(self) -> None:
                self.venue = "test"

        obj = _Obj()
        result = coerce_adapter_output(obj)
        assert result["venue"] == "test"

    def test_coerce_adapter_output_fallback(self) -> None:
        from instruments_service.engine.data_utils import coerce_adapter_output

        result = coerce_adapter_output(42)
        assert result == {"raw": 42}

    def test_af_id_from_canonical_dict(self) -> None:
        from instruments_service.engine.data_utils import af_id_from_canonical

        assert af_id_from_canonical({"af_fixture_id": "123"}) == 123
        assert af_id_from_canonical({"af_fixture_id": "abc"}) is None
        assert af_id_from_canonical({}) is None

    def test_af_id_from_canonical_none(self) -> None:
        from instruments_service.engine.data_utils import af_id_from_canonical

        assert af_id_from_canonical(None) is None

    def test_normalize_wrapped_token(self) -> None:
        from instruments_service.engine.data_utils import normalize_wrapped_token

        assert normalize_wrapped_token("WETH") == "ETH"
        assert normalize_wrapped_token("WBTC") == "BTC"
        assert normalize_wrapped_token("WFOO") == "WFOO"  # not a known base token
        assert normalize_wrapped_token("ETH") == "ETH"

    def test_count_per_venue(self) -> None:
        from instruments_service.engine.data_utils import count_per_venue

        records = [
            {"venue": "BINANCE"},
            {"venue": "BINANCE"},
            {"venue": "KRAKEN"},
        ]
        counts = count_per_venue(records)
        assert counts["BINANCE"] == 2
        assert counts["KRAKEN"] == 1

    def test_count_per_venue_object(self) -> None:
        from instruments_service.engine.data_utils import count_per_venue

        class _R:
            venue = "BYBIT"

        counts = count_per_venue([_R()])
        assert counts["BYBIT"] == 1

    def test_count_per_venue_unknown(self) -> None:
        from instruments_service.engine.data_utils import count_per_venue

        counts = count_per_venue([42])
        assert counts["unknown"] == 1

    def test_compute_prediction_shards_empty(self) -> None:
        import pandas as pd

        from instruments_service.engine.data_utils import compute_prediction_shards

        assert compute_prediction_shards(pd.DataFrame()) == {}

    def test_compute_prediction_shards(self) -> None:
        import pandas as pd

        from instruments_service.engine.data_utils import compute_prediction_shards

        df = pd.DataFrame({"group": ["sports", "sports", "politics"]})
        result = compute_prediction_shards(df)
        assert result["sports"] == 2
        assert result["politics"] == 1


# ---------------------------------------------------------------------------
# validation_utils
# ---------------------------------------------------------------------------


class TestValidationUtils:
    def test_get_venue_epoch_known(self) -> None:
        from instruments_service.engine.validation_utils import get_venue_epoch

        assert get_venue_epoch("binance") == "2017-07-14"
        assert get_venue_epoch("BINANCE") == "2017-07-14"

    def test_get_venue_epoch_unknown(self) -> None:
        from instruments_service.engine.validation_utils import get_venue_epoch

        assert get_venue_epoch("unknown_venue") is None

    def test_should_skip_shard_before_epoch(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_shard

        assert should_skip_shard("2015-01-01", "binance") is True

    def test_should_skip_shard_after_epoch(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_shard

        assert should_skip_shard("2020-01-01", "binance") is False

    def test_should_skip_shard_unknown_venue(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_shard

        assert should_skip_shard("2010-01-01", "unknown_venue") is False

    def test_should_skip_shard_weekend(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_shard

        # 2024-01-06 is a Saturday
        assert should_skip_shard("2024-01-06", "binance", skip_weekends=True, skip_before_epoch=False) is True
        # 2024-01-07 is a Sunday
        assert should_skip_shard("2024-01-07", "binance", skip_weekends=True, skip_before_epoch=False) is True
        # 2024-01-08 is a Monday
        assert should_skip_shard("2024-01-08", "binance", skip_weekends=True, skip_before_epoch=False) is False

    def test_should_skip_shard_invalid_date_weekend(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_shard

        # Bad date should not raise — just skip the weekend check
        assert should_skip_shard("not-a-date", "binance", skip_weekends=True, skip_before_epoch=False) is False

    def test_should_skip_date_for_per_league_normal(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_date_for_per_league

        assert should_skip_date_for_per_league("2021-04-01", "EPL") is False

    def test_should_skip_date_for_per_league_too_old(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_date_for_per_league

        assert should_skip_date_for_per_league("2010-01-01", "EPL") is True

    def test_should_skip_date_for_per_league_invalid(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_date_for_per_league

        assert should_skip_date_for_per_league("not-a-date", "EPL") is True

    def test_should_skip_date_for_per_league_season(self) -> None:
        from instruments_service.engine.validation_utils import should_skip_date_for_per_league

        # Within season range
        assert should_skip_date_for_per_league("2022-05-01", "EPL", season_year=2022) is False
        # Way outside season range
        assert should_skip_date_for_per_league("2018-05-01", "EPL", season_year=2022) is True

    def test_classify_adapter_failure_network(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(TimeoutError("connection timeout"), "test") == "TRANSIENT_NETWORK"

    def test_classify_adapter_failure_rate_limit(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("rate limit exceeded"), "test") == "RATE_LIMITED"

    def test_classify_adapter_failure_auth(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("unauthorized"), "test") == "AUTH_FAILURE"

    def test_classify_adapter_failure_not_found(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("not found"), "test") == "DATA_NOT_FOUND"

    def test_classify_adapter_failure_server_error(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("503 server error"), "test") == "SERVER_ERROR"

    def test_classify_adapter_failure_client_error(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("400 bad request"), "test") == "CLIENT_ERROR"

    def test_classify_adapter_failure_unknown(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        assert classify_adapter_failure(Exception("something weird"), "test") == "UNKNOWN_ERROR"

    def test_classify_adapter_failure_exception_class_name(self) -> None:
        from instruments_service.engine.validation_utils import classify_adapter_failure

        class ConnectionError(Exception):
            pass

        assert classify_adapter_failure(ConnectionError("err"), "test") == "TRANSIENT_NETWORK"
