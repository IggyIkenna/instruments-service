"""Regression gates for ``orchestrator._gated_sink_write`` + ``_WRITE_GATE``.

Plan 6 (``instruments_service_write_gate_validation_2026_04_22``) wraps the
most-critical sports-backfill ``sink.write(...)`` call sites with
``InstrumentsWriteGate.validate_and_write``. These tests assert the wrapper's
shape + that it catches the 2026-04-22 §5 data-crime regression (wall-clock-2026
row-timestamps landing on a historical ``day=2023-03-16`` partition).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import (
    TimestampAlignmentError,
    _WRITE_GATE,
    _gated_sink_write,
)
from unified_trading_library.instruments_write_gate import InstrumentsWriteGate


@dataclass
class _FakeSink:
    writes: list[dict[str, object]] = field(default_factory=list)

    def write(
        self,
        *,
        data: pd.DataFrame,
        partition: dict[str, str],
        format: str,  # noqa: A002
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
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "valuation_date": ["2023-02-01", "2023-03-10"],
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

    def test_tm_incident_shape_warn_mode_writes_but_emits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
        df = pd.DataFrame(
            {
                "player_id": ["p1", "p2"],
                "valuation_date": ["2026-04-22", "2026-04-22"],  # wall-clock-today
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
        # A few progressive ticks at 0s / 45s / 90s into the match.
        df = pd.DataFrame(
            {
                "match_id": ["m1", "m1", "m1"],
                "timer_seconds": [0, 45, 90],
                "data_available_at": [
                    kickoff + pd.Timedelta(seconds=s) for s in [0, 45, 90]
                ],
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
        df = pd.DataFrame({"valuation_date": ["2099-01-01"]})
        _gated_sink_write(
            sink,
            data=df,
            partition={},
            filename="mapping.parquet",
            venue="transfermarkt",
            entity="team_mapping",
        )
        assert len(sink.writes) == 1


class TestStrictModeEndToEnd:
    """Verify that flipping the gate to strict mode raises TimestampAlignmentError
    on incident-shape data — so Phase 3 of Plan 6 can flip the default safely."""

    def test_strict_mode_raises_and_skips_write(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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
                    datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
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
