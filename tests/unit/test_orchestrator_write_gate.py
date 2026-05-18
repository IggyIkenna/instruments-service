"""Regression gates for ``orchestrator._gated_sink_write`` + ``_WRITE_GATE``.

Plan 6 (``instruments_service_write_gate_validation_2026_04_22``) wraps the
most-critical sports-backfill ``sink.write(...)`` call sites with
``InstrumentsWriteGate.validate_and_write``. These tests assert the wrapper's
shape + that it catches the 2026-04-22 §5 data-crime regression (wall-clock-2026
row-timestamps landing on a historical ``day=2023-03-16`` partition).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
import pytest
from unified_trading_library import InstrumentsWriteGate, LookaheadBiasError, TimestampAlignmentError

from instruments_service.engine.orchestrator import (
    _WRITE_GATE,
    _gated_sink_write,
)


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
        self.writes.append(
            {
                "data": data.copy(),
                "partition": dict(partition),
                "format": format,
                "filename": filename,
            }
        )


class TestModuleLevelGate:
    def test_gate_starts_in_warn_mode(self) -> None:
        """Plan 6 rollout: warn-mode is the default until strict is flipped."""
        assert isinstance(_WRITE_GATE, InstrumentsWriteGate)
        assert _WRITE_GATE.mode == "warn"


class TestGatedSinkWrite:
    def test_compliant_rows_pass_through(self) -> None:
        sink = _FakeSink()
        _ts = pd.Timestamp("2023-03-16", tz="UTC")
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "valuation_date": ["2023-02-01", "2023-03-10"],
                "available_at": [_ts, _ts],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition={"day": "2023-03-16", "entity": "player_values"},
            filename="player_values.parquet",
            venue="transfermarkt",
            entity="player_values",
        )
        assert len(sink.writes) == 1
        assert sink.writes[0]["filename"] == "player_values.parquet"
        assert sink.writes[0]["partition"] == {
            "day": "2023-03-16",
            "entity": "player_values",
        }

    def test_tm_incident_shape_warn_mode_writes_but_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replay 2026-04-22 Transfermarkt VM incident: wall-clock valuation_date
        on historical day=2023-03-16. Warn mode emits + proceeds.

        A strict-mode flip would raise TimestampAlignmentError from the per-shard
        try/except in ``_fetch_transfermarkt_data`` — see plan Phase 3.
        """
        events: list[tuple[str, str, dict[str, object]]] = []

        def fake_log(
            event_name: str,
            severity: str = "INFO",
            details: dict[str, object] | None = None,
            **_: object,
        ) -> None:
            events.append((event_name, severity, dict(details or {})))

        monkeypatch.setattr(
            "unified_trading_library.events.log_event",
            fake_log,
            raising=False,
        )

        sink = _FakeSink()
        _ts = pd.Timestamp("2023-03-16", tz="UTC")
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "valuation_date": ["2026-04-22", "2026-04-22"],  # wall-clock-today
                "available_at": [_ts, _ts],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition={"day": "2023-03-16", "entity": "player_values"},
            filename="player_values.parquet",
            venue="transfermarkt",
            entity="player_values",
        )
        # warn mode → write still happens
        assert len(sink.writes) == 1
        # event emitted
        alignment_events = [e for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION"]
        assert len(alignment_events) == 1
        _, severity, details = alignment_events[0]
        assert severity == "WARNING"
        assert details["venue"] == "transfermarkt"
        assert details["entity"] == "player_values"

    def test_sfi_kickoff_compliant(self) -> None:
        """SFI progressive_stats stamps data_available_at = kickoff + timer_seconds.
        For a historical backfill on day=D, kickoff_utc derives from D at 15:00 UTC
        (see orchestrator.py ~L4894). All data_available_at values should satisfy
        value.date() <= D after timer_seconds addition (timer never negative)."""
        sink = _FakeSink()
        kickoff = pd.Timestamp("2023-03-16", tz="UTC") + pd.Timedelta(hours=15)
        write_ts = pd.Timestamp("2023-03-16", tz="UTC")
        # A few progressive ticks at 0s / 45s / 90s into the match.
        df = pd.DataFrame(
            {
                "match_id": ["m1", "m1", "m1"],
                "timer_seconds": [0, 45, 90],
                "data_available_at": [kickoff + pd.Timedelta(seconds=s) for s in [0, 45, 90]],
                "available_at": [write_ts, write_ts, write_ts],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition={"day": "2023-03-16", "entity": "progressive_stats"},
            filename="progressive_stats.parquet",
            venue="soccer_football_info",
            entity="progressive_stats",
        )
        assert len(sink.writes) == 1

    def test_mapping_partition_without_day_unchecked(self) -> None:
        """Team-mapping / fixture-mapping writes have no ``day=`` partition — the
        gate must pass through without scanning."""
        sink = _FakeSink()
        df = pd.DataFrame(
            {
                "valuation_date": ["2099-01-01"],
                "available_at": [pd.Timestamp("2023-03-16", tz="UTC")],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition={},
            filename="mapping.parquet",
            venue="transfermarkt",
            entity="team_mapping",
        )
        assert len(sink.writes) == 1


class TestVenueParityCases:
    """Phase 3a — each wired venue's warn-mode incident shape passes through
    the same ``_gated_sink_write`` helper and emits on wall-clock-on-historical.

    The gate is venue-agnostic; these cases exist so any future adapter-side
    wall-clock bug (like 2026-04-22 Transfermarkt) is caught at the FIRST
    sink.write call regardless of provider. Failure to emit here = the gate
    wasn't threaded into the caller's sink.write."""

    _BATCH_DATE = "2023-03-16"  # historical backfill partition
    _WALL_CLOCK_VIOLATION = "2026-04-22"  # today, per the 2026-04-22 session

    def _run_incident(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        partition: dict[str, str],
        filename: str,
        venue: str,
        entity: str,
        column: str = "valuation_date",
    ) -> tuple[_FakeSink, list[tuple[str, str, dict[str, object]]]]:
        events: list[tuple[str, str, dict[str, object]]] = []

        def fake_log(
            event_name: str,
            severity: str = "INFO",
            details: dict[str, object] | None = None,
            **_: object,
        ) -> None:
            events.append((event_name, severity, dict(details or {})))

        monkeypatch.setattr(
            "unified_trading_library.events.log_event",
            fake_log,
            raising=False,
        )
        _write_ts = pd.Timestamp("2023-03-16", tz="UTC")
        sink = _FakeSink()
        df = pd.DataFrame(
            {
                "row_id": ["r1", "r2"],
                column: [self._WALL_CLOCK_VIOLATION, self._WALL_CLOCK_VIOLATION],
                "available_at": [_write_ts, _write_ts],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition=partition,
            filename=filename,
            venue=venue,
            entity=entity,
        )
        return sink, events

    def test_api_football_fixtures_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "fixtures"},
            filename="fixtures.parquet",
            venue="api_football",
            entity="fixtures",
            column="kickoff_utc",
        )
        assert len(sink.writes) == 1  # warn mode writes
        alignment = [e for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION"]
        assert len(alignment) == 1
        _, severity, details = alignment[0]
        assert severity == "WARNING"
        assert details["venue"] == "api_football"
        assert details["entity"] == "fixtures"

    def test_api_football_injuries_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "injuries", "league": "epl"},
            filename="injuries.parquet",
            venue="api_football",
            entity="injuries",
            column="data_available_at",
        )
        assert len(sink.writes) == 1
        assert any(e[0] == "DATA_ALIGNMENT_VIOLATION" for e in events)

    def test_footystats_matches_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "footystats_matches"},
            filename="footystats_matches.parquet",
            venue="footystats",
            entity="footystats_matches",
            column="kickoff_utc",
        )
        assert len(sink.writes) == 1
        details = next(e[2] for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION")
        assert details["venue"] == "footystats"

    def test_footystats_odds_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={
                "day": self._BATCH_DATE,
                "entity": "footystats_odds",
                "fetched_at_hour": "12",
            },
            filename="footystats_odds.parquet",
            venue="footystats",
            entity="footystats_odds",
            column="data_available_at",
        )
        assert len(sink.writes) == 1
        assert any(e[0] == "DATA_ALIGNMENT_VIOLATION" for e in events)

    def test_understat_xg_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "understat_xg"},
            filename="understat_xg.parquet",
            venue="understat",
            entity="understat_xg",
            column="event_time",
        )
        assert len(sink.writes) == 1
        details = next(e[2] for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION")
        assert details["venue"] == "understat"

    def test_transfermarkt_leagues_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "transfermarkt_leagues"},
            filename="transfermarkt_leagues.parquet",
            venue="transfermarkt",
            entity="transfermarkt_leagues",
        )
        assert len(sink.writes) == 1
        assert any(e[0] == "DATA_ALIGNMENT_VIOLATION" for e in events)

    def test_weather_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "entity": "weather"},
            filename="weather.parquet",
            venue="open_meteo",
            entity="weather",
            column="data_available_at",
        )
        assert len(sink.writes) == 1
        details = next(e[2] for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION")
        assert details["venue"] == "open_meteo"

    def test_instrument_universe_universe_warn_mode_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """L1723/L2340 instrument-universe writes (DeFi/CeFi/TradFi/Prediction).
        Production DataFrames don't carry DEFAULT_AS_OF_COLUMNS so the gate
        no-ops; this test injects one to prove the gate fires if a future
        schema addition lands that column."""
        sink, events = self._run_incident(
            monkeypatch,
            partition={"day": self._BATCH_DATE, "venue": "HYPERLIQUID"},
            filename="instruments.parquet",
            venue="HYPERLIQUID",
            entity="instruments",
            column="computed_at",
        )
        assert len(sink.writes) == 1
        details = next(e[2] for e in events if e[0] == "DATA_ALIGNMENT_VIOLATION")
        assert details["venue"] == "HYPERLIQUID"
        assert details["entity"] == "instruments"


class TestAvailableAtPresent:
    """Phase 2.D — ``_gated_sink_write`` must reject DataFrames missing
    ``available_at`` or containing null values so ingesters that forget to
    stamp the column fail loud instead of silently writing lookahead-biased data."""

    def test_missing_available_at_raises(self) -> None:
        sink = _FakeSink()
        df = pd.DataFrame({"player_id": ["p1", "p2"]})
        with pytest.raises(LookaheadBiasError):
            _gated_sink_write(
                sink,
                data=df,
                partition={"day": "2023-03-16", "entity": "player_values"},
                filename="player_values.parquet",
                venue="transfermarkt",
                entity="player_values",
            )
        assert sink.writes == []

    def test_null_available_at_raises(self) -> None:
        sink = _FakeSink()
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "available_at": [pd.Timestamp("2023-03-16", tz="UTC"), pd.NaT],
            }
        )
        with pytest.raises(LookaheadBiasError):
            _gated_sink_write(
                sink,
                data=df,
                partition={"day": "2023-03-16", "entity": "player_values"},
                filename="player_values.parquet",
                venue="transfermarkt",
                entity="player_values",
            )
        assert sink.writes == []

    def test_present_available_at_passes(self) -> None:
        sink = _FakeSink()
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "available_at": [
                    pd.Timestamp("2023-03-16", tz="UTC"),
                    pd.Timestamp("2023-03-16", tz="UTC"),
                ],
            }
        )
        _gated_sink_write(
            sink,
            data=df,
            partition={"day": "2023-03-16", "entity": "player_values"},
            filename="player_values.parquet",
            venue="transfermarkt",
            entity="player_values",
        )
        assert len(sink.writes) == 1

    def test_empty_dataframe_passes(self) -> None:
        sink = _FakeSink()
        df = pd.DataFrame({"player_id": pd.Series([], dtype=str)})
        _gated_sink_write(
            sink,
            data=df,
            partition={"day": "2023-03-16", "entity": "player_values"},
            filename="player_values.parquet",
            venue="transfermarkt",
            entity="player_values",
        )
        assert len(sink.writes) == 1


class TestStrictModeEndToEnd:
    """Verify that flipping the gate to strict mode raises TimestampAlignmentError
    on incident-shape data — so Phase 3 of Plan 6 can flip the default safely."""

    def test_strict_mode_raises_and_skips_write(self, monkeypatch: pytest.MonkeyPatch) -> None:
        events: list[tuple[str, str]] = []

        def fake_log(
            event_name: str,
            severity: str = "INFO",
            details: dict[str, object] | None = None,
            **_: object,
        ) -> None:
            events.append((event_name, severity))

        monkeypatch.setattr(
            "unified_trading_library.events.log_event",
            fake_log,
            raising=False,
        )

        # Build a strict-mode gate + use it directly (don't mutate the module-level one).
        strict_gate = InstrumentsWriteGate(mode="strict")
        sink = _FakeSink()
        df = pd.DataFrame(
            {
                "valuation_date": [
                    datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
                ],
            }
        )
        with pytest.raises(TimestampAlignmentError):
            strict_gate.validate_and_write(
                sink=sink,
                data=df,
                partition={"day": "2023-03-16", "entity": "player_values"},
                format="parquet",
                filename="player_values.parquet",
                venue="transfermarkt",
                entity="player_values",
            )
        assert sink.writes == []  # strict mode → write skipped
