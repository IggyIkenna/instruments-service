"""Gate G9 — regression tests for failures surfaced during canonicalisation.

instruments-service-owned surfaces:

* ``_get_instruments_bucket`` routes writes to a ``-test`` bucket when
  ``IS_TEST_RUN=true`` and to the prod bucket otherwise — nothing in
  between.
* FootyStats odds and predictions paths include a ``fetched_at_hour``
  partition key so repeated polls accumulate snapshots instead of
  overwriting the same file.
* Databento trading-calendar logic: a Saturday in a normal week is a
  non-trading day for equity venues — the instruments pipeline must
  not crash when it is handed a weekend date, it must simply report
  "no trading today".
"""

from __future__ import annotations

import datetime as _dt
import inspect
from typing import cast
from unittest.mock import patch

import pytest

from instruments_service.config.service_config import (
    InstrumentsServiceConfig,
)
from instruments_service.reference_data.adapters.tradfi.databento import (
    is_non_trading_day,
)

# ---------------------------------------------------------------------------
# #10 IS_TEST_RUN routes to the -test bucket
# ---------------------------------------------------------------------------


class TestInstrumentsBucketRoutingRegressionG9:
    """``_get_instruments_bucket`` must produce a ``-test`` suffix when
    ``is_test_run=True`` on the service config, and the bare prod name when
    ``False``. No middle ground, no silent prod writes from tests.
    """

    def test_test_run_returns_test_bucket_name(self) -> None:
        from instruments_service.engine import orchestrator as orch_mod

        fake_cfg = InstrumentsServiceConfig(
            is_test_run=True,
        )
        # gcp_project_id is inherited from UnifiedCloudConfig — populate via
        # env to keep the config construction cheap.
        with (
            patch.dict(
                "os.environ",
                {"GCP_PROJECT_ID": "my-project", "IS_TEST_RUN": "true"},
                clear=False,
            ),
            patch.object(orch_mod, "get_config", return_value=fake_cfg),
        ):
            fake_cfg.gcp_project_id = "my-project"
            fake_cfg.is_test_run = True
            out = orch_mod._get_instruments_bucket("cefi")
        assert out.endswith("-test"), f"IS_TEST_RUN=true must return a -test-suffixed bucket, got {out!r}"

    def test_non_test_run_returns_prod_bucket_name(self) -> None:
        from instruments_service.engine import orchestrator as orch_mod

        fake_cfg = InstrumentsServiceConfig(
            is_test_run=False,
        )
        with (
            patch.dict(
                "os.environ",
                {"GCP_PROJECT_ID": "my-project", "IS_TEST_RUN": "false"},
                clear=False,
            ),
            patch.object(orch_mod, "get_config", return_value=fake_cfg),
        ):
            fake_cfg.gcp_project_id = "my-project"
            fake_cfg.is_test_run = False
            out = orch_mod._get_instruments_bucket("cefi")
        assert not out.endswith("-test"), f"IS_TEST_RUN=false must NOT return a -test bucket, got {out!r}"


# ---------------------------------------------------------------------------
# #18 FootyStats paths include fetched_at_hour partition
# ---------------------------------------------------------------------------


class TestFootystatsFetchedAtHourPartitionRegressionG9:
    """FootyStats odds and predictions writers MUST include a
    ``fetched_at_hour`` partition key — repeated polls otherwise overwrite
    the same file. We assert the source for each writer carries the
    ``fetched_at_hour`` key in the partition dict and computes it via
    ``strftime("%Y-%m-%dT%H")``.
    """

    @pytest.mark.parametrize("fn_name", ["_fetch_footystats_odds", "_fetch_footystats_predictions"])
    def test_source_constructs_fetched_at_hour_partition(self, fn_name: str) -> None:
        from instruments_service.engine import orchestrator as orch_mod

        fn = getattr(orch_mod, fn_name)
        src = inspect.getsource(fn)
        assert "fetched_at_hour" in src, f"{fn_name} source does not reference fetched_at_hour at all"
        assert '"%Y-%m-%dT%H"' in src, (
            f"{fn_name} must compute fetched_at_hour via strftime('%Y-%m-%dT%H') "
            "so repeated polls within the same hour share a partition but "
            "polls across different hours do not overwrite each other"
        )
        # The partition dict passed to sink.write must literally include
        # "fetched_at_hour" as a key.
        assert '"fetched_at_hour"' in src or "'fetched_at_hour'" in src
        # Two distinct timestamps one hour apart must produce two distinct
        # strftime outputs — the canonical invariant the partition relies on.
        t1 = _dt.datetime(2026, 4, 17, 12, 0, tzinfo=_dt.UTC)
        t2 = _dt.datetime(2026, 4, 17, 13, 0, tzinfo=_dt.UTC)
        assert t1.strftime("%Y-%m-%dT%H") != t2.strftime("%Y-%m-%dT%H")


# ---------------------------------------------------------------------------
# #19 Weekend / holiday TradFi — graceful 0-record, no crash
# ---------------------------------------------------------------------------


class TestTradfiNonTradingDayRegressionG9:
    """A weekend date handed to the Databento TradFi adapter must surface
    cleanly as "non-trading day" rather than raise. Before the fix, the
    weekend path crashed attempting to compute session metadata.
    """

    def test_saturday_is_non_trading_for_equity_venues(self) -> None:
        saturday = _dt.date(2024, 6, 15)
        assert saturday.weekday() == 5  # Sat
        assert is_non_trading_day("NYSE", saturday) is True
        assert is_non_trading_day("NASDAQ", saturday) is True
        assert is_non_trading_day("CBOE", saturday) is True

    def test_saturday_is_non_trading_for_futures_venues(self) -> None:
        saturday = _dt.date(2024, 6, 15)
        # CME opens Sunday evening — Saturday is a non-trading day for CME
        # futures venues too.
        assert is_non_trading_day("CME", saturday) is True
        assert is_non_trading_day("ICE", saturday) is True

    def test_sunday_is_trading_for_cme_not_for_equities(self) -> None:
        sunday = _dt.date(2024, 6, 16)
        assert sunday.weekday() == 6
        # Equity exchanges closed all weekend
        assert is_non_trading_day("NASDAQ", sunday) is True
        assert is_non_trading_day("NYSE", sunday) is True
        # CME opens Sunday evening
        assert is_non_trading_day("CME", sunday) is False

    def test_non_trading_check_does_not_raise_on_weekend(self) -> None:
        saturday = _dt.date(2024, 6, 15)
        # Must not raise for any of the registered venues — no crash.
        for venue in ("NASDAQ", "NYSE", "CME", "ICE", "CBOE"):
            _ = is_non_trading_day(venue, saturday)  # smoke

    def test_weekday_is_trading_day(self) -> None:
        # 2024-06-13 is a Thursday — a regular trading day for all venues.
        thu = _dt.date(2024, 6, 13)
        assert thu.weekday() == 3
        for venue in ("NASDAQ", "NYSE", "CME", "ICE", "CBOE"):
            assert is_non_trading_day(venue, thu) is False, f"{venue} must treat {thu} (Thu) as a trading day"


# Keep `cast` imported for future type-narrowing needs.
_ = cast
