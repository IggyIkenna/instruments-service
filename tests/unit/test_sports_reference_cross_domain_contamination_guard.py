"""Regression test for the day=2026-04-14 wrong-schema fixtures_schedule incident.

sports_fixtures_schedule_wrong_schema_day_2026_04_14.md: 85 `entity=fixtures_schedule`
shards silently carried an instrument-catalogue/registry shape (`instrument_key`,
`venue`, `tick_size`, `base_asset`, `quote_asset`, ...) instead of sports fixtures
data (`af_fixture_id`, `af_league_id`, `season`, `round`, ...), undetected until a
downstream column-projection read failed with a schema-mismatch error. The
original write-path bug that produced the wrong DataFrame for that one day
predates the `engine/orchestrator.py` cohesion-module split and could not be
traced to an exact historical call site; this guard is the structural fix that
makes ANY future recurrence of this exact class of mix-up fail loud instead of
silently landing in GCS, regardless of what upstream bug produces the wrong
DataFrame.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import _gated_sink_write
from instruments_service.engine.orchestrator.sink import _assert_not_cross_domain_contamination


@dataclass
class _FakeSink:
    writes: list[dict[str, object]] = field(default_factory=list)

    def write(
        self,
        *,
        data: pd.DataFrame,
        partition: dict[str, str],
        format: str,
        filename: str,
    ) -> None:
        self.writes.append({"data": data.copy(), "partition": dict(partition), "format": format, "filename": filename})


def _instrument_catalogue_shaped_df() -> pd.DataFrame:
    """Mirrors the exact wrong shape observed in the day=2026-04-14 incident."""
    return pd.DataFrame(
        {
            "instrument_key": ["BINANCE:BTCUSDT"],
            "venue": ["BINANCE"],
            "instrument_type": ["PERP"],
            "raw_symbol": ["BTCUSDT"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "tick_size": [0.10],
            "min_size": [0.001],
            "contract_size": [1],
            "available_at": [pd.Timestamp("2026-04-14", tz="UTC")],
        }
    )


def _real_fixtures_schedule_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "af_fixture_id": [1001],
            "af_league_id": [39],
            "season": [2026],
            "round": ["Regular Season - 1"],
            "available_at": [pd.Timestamp("2026-04-14", tz="UTC")],
        }
    )


class TestCrossDomainContaminationGuard:
    def test_instrument_catalogue_shape_raises_directly(self) -> None:
        with pytest.raises(ValueError, match="instrument-catalogue column"):
            _assert_not_cross_domain_contamination(_instrument_catalogue_shaped_df(), "fixtures_schedule")

    def test_real_fixtures_schedule_shape_passes(self) -> None:
        _assert_not_cross_domain_contamination(_real_fixtures_schedule_df(), "fixtures_schedule")

    def test_scoped_to_sports_entities_only_non_sports_write_untouched(self) -> None:
        """The guard MUST NOT fire for a non-sports entity, even with these exact
        columns present — they are legitimate there. `_gated_sink_write` is a
        SHARED choke point across every instruments-service domain, including
        the real CeFi/TradFi/DeFi instrument-catalogue writers, whose rows
        naturally carry `instrument_key`/`tick_size`/etc. Regression: the first
        cut of this guard was unscoped and broke `test_orchestrator_process.py`
        + `test_orchestrator_futures_contracts.py` + `test_new_orchestrator.py`
        (real instrument-catalogue writes rejected as "contamination")."""
        _assert_not_cross_domain_contamination(_instrument_catalogue_shaped_df(), "instruments")
        _assert_not_cross_domain_contamination(_instrument_catalogue_shaped_df(), "futures_contracts")

    def test_gated_sink_write_refuses_wrong_schema_replay(self) -> None:
        """Replay the exact day=2026-04-14 incident shape through the real write
        choke point every sports_reference entity funnels through — must raise
        before anything reaches the sink, not just when called directly.
        """
        sink = _FakeSink()
        with pytest.raises(ValueError, match="instrument-catalogue column"):
            _gated_sink_write(
                sink,
                data=_instrument_catalogue_shaped_df(),
                partition={"day": "2026-04-14", "entity": "fixtures_schedule", "league": "EPL"},
                filename="fixtures_schedule.parquet",
                venue="api_football",
                entity="fixtures_schedule",
            )
        assert sink.writes == [], "the wrong-schema DataFrame must never reach the sink"

    def test_gated_sink_write_allows_real_fixtures_schedule_data(self) -> None:
        sink = _FakeSink()
        _gated_sink_write(
            sink,
            data=_real_fixtures_schedule_df(),
            partition={"day": "2026-04-14", "entity": "fixtures_schedule", "league": "EPL"},
            filename="fixtures_schedule.parquet",
            venue="api_football",
            entity="fixtures_schedule",
        )
        assert len(sink.writes) == 1
