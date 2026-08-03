"""Regression for `cefi_coinbase_cde_urdi_zero_records_2026_07_28.md` [CODE] P2.

``_zero_records_non_sports`` used to have no honest-absence path for a real
CeFi/DeFi adapter whose fetched instrument universe is 100% future-dated
relative to the requested backfill day — it fell straight through to the
terminal ``RuntimeError`` (ground-truth incident: COINBASE-CDE fetched 118
real FUTURE instruments, all filtered out by ``filter_instruments_by_date``
because every one carried the SAME ``available_from_datetime`` after the
requested 2026-03-15, then crashed instead of writing an honest marker).

This covers the new ``pre_launch_venues`` short-circuit: a venue only
qualifies when EVERY remaining active venue (after the NO_ADAPTER_YET
short-circuit) is confirmed pre-launch — a mixed batch where some venue is
zero for a genuinely different reason must still raise.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from instruments_service.engine.orchestrator.process_fetch import (
    _pre_launch_venues_from_raw_fetch,
)
from instruments_service.engine.orchestrator.process_zero_records import (
    _zero_records_non_sports,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


class _FakeRecord:
    def __init__(self, venue: str, available_from_datetime: object) -> None:
        self.venue = venue
        self.available_from_datetime = available_from_datetime


class _CapManifest:
    def __init__(self, *_: object, **__: object) -> None:
        self.calls: list[dict[str, object]] = []

    def record_expected_empty(
        self,
        *,
        row_key: Mapping[str, object],
        reason: str,
        **_kw: object,
    ) -> None:
        self.calls.append({"row_key": dict(row_key), "reason": reason})

    def write(self) -> None:
        pass


class TestPreLaunchVenuesFromRawFetch:
    """Unit coverage for the raw-fetch classifier feeding the crash-hardening."""

    def test_venue_with_all_records_future_dated_is_pre_launch(self) -> None:
        date_dt = datetime(2026, 3, 15, tzinfo=UTC)
        future = datetime(2026, 7, 10, tzinfo=UTC)
        records = [_FakeRecord("COINBASE-CDE", future) for _ in range(3)]

        result = _pre_launch_venues_from_raw_fetch(records, date_dt)

        assert result == frozenset({"COINBASE-CDE"})

    def test_venue_with_one_past_dated_record_is_not_pre_launch(self) -> None:
        date_dt = datetime(2026, 3, 15, tzinfo=UTC)
        future = datetime(2026, 7, 10, tzinfo=UTC)
        past = datetime(2020, 1, 1, tzinfo=UTC)
        records = [_FakeRecord("COINBASE-CDE", future), _FakeRecord("COINBASE-CDE", past)]

        result = _pre_launch_venues_from_raw_fetch(records, date_dt)

        assert result == frozenset()

    def test_venue_with_none_available_from_datetime_is_not_pre_launch(self) -> None:
        """None means "always available" to the date filter — never pre-launch evidence."""
        date_dt = datetime(2026, 3, 15, tzinfo=UTC)
        records = [_FakeRecord("BYBIT", None)]

        result = _pre_launch_venues_from_raw_fetch(records, date_dt)

        assert result == frozenset()

    def test_no_records_yields_no_pre_launch_venues(self) -> None:
        assert _pre_launch_venues_from_raw_fetch([], datetime(2026, 3, 15, tzinfo=UTC)) == frozenset()


class TestZeroRecordsPreLaunchVenueDoesNotCrash:
    def test_sole_pre_launch_venue_returns_zero_counts_cleanly(self) -> None:
        """COINBASE-CDE alone, confirmed pre-launch, must return {"COINBASE-CDE": 0}
        AND stamp an honest EXPECTED_PRE_VENUE_LAUNCH manifest row instead of
        raising RuntimeError."""
        manifest = _CapManifest()
        with (
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=lambda *_a, **_k: manifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="cefi-bucket",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = _zero_records_non_sports(
                date="2026-03-15",
                asset_groups=["CEFI"],
                active_venues=["COINBASE-CDE"],
                mode="batch",
                pre_launch_venues=frozenset({"COINBASE-CDE"}),
            )

        assert result == {"COINBASE-CDE": 0}
        assert len(manifest.calls) == 1
        assert manifest.calls[0]["row_key"] == {"date": "2026-03-15", "venue": "COINBASE-CDE"}
        assert manifest.calls[0]["reason"] == "EXPECTED_PRE_VENUE_LAUNCH"

    def test_mixed_batch_with_one_non_pre_launch_venue_still_raises(self) -> None:
        """A mixed batch where NOT every remaining active venue is confirmed
        pre-launch must still fail loudly — the short-circuit never partially
        swallows a genuine failure."""
        with patch("instruments_service.engine.orchestrator.log_event"), pytest.raises(RuntimeError):
            _zero_records_non_sports(
                date="2026-03-15",
                asset_groups=["CEFI"],
                active_venues=["COINBASE-CDE", "BYBIT"],
                mode="batch",
                pre_launch_venues=frozenset({"COINBASE-CDE"}),
            )

    def test_no_pre_launch_venues_still_raises(self) -> None:
        """Default (``pre_launch_venues=None``) behaves exactly as before — a
        genuine zero-record CeFi shard on an active day still raises."""
        with patch("instruments_service.engine.orchestrator.log_event"), pytest.raises(RuntimeError):
            _zero_records_non_sports(
                date="2026-06-24",
                asset_groups=["CEFI"],
                active_venues=["BYBIT", "DERIBIT"],
                mode="batch",
            )
