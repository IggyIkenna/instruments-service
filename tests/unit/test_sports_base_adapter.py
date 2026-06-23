"""Tests for BaseSportsReferenceAdapter and sports adapter factory."""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.sports.factory import (
    create_sports_reference_adapter,
)


class TestSportsFactory:
    def test_create_api_football(self) -> None:
        adapter = create_sports_reference_adapter("api_football", api_key="test-key")
        assert adapter is not None
        assert adapter.venue == "api_football"

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            create_sports_reference_adapter("nonexistent_provider")


class TestSportsAdapterVenueProperty:
    """All sports adapters return a valid venue string."""

    def test_api_football_venue(self) -> None:
        adapter = create_sports_reference_adapter("api_football", api_key="test")
        assert adapter.venue == "api_football"


class TestUtcWindowAlignedLimiter:
    """UTC-boundary-aligned fixed-window counter (Part 1, operator 2026-06-23).

    The limiter's per-UTC-minute count must equal the provider's
    X-RateLimit-Remaining (same window, same phase): under a cap of N, the first N
    reservations in a minute return 0 sleep, the (N+1)th sleeps to the next :00.
    """

    def _fresh_adapter_cls(self):
        # Each test gets a private subclass so the class-level window counters don't
        # leak across tests (the adapter is a per-VM singleton; the state is cls-level).
        from instruments_service.reference_data.adapters.sports.adapters.base import (
            BaseSportsReferenceAdapter,
        )

        class _Probe(BaseSportsReferenceAdapter):
            @property
            def venue(self) -> str:
                return "probe"

            async def get_fixtures(self, date, league_ids=None):  # type: ignore[override]
                return []

            async def get_leagues(self):  # type: ignore[override]
                return []

            async def get_teams(self, league_id, season=None):  # type: ignore[override]
                return []

            async def get_odds(self, sport, regions="uk", markets="h2h"):  # type: ignore[override]
                return []

        return _Probe

    def test_uncapped_window_never_sleeps(self) -> None:
        cls = self._fresh_adapter_cls()
        # No cap set (0 = uncapped) → window check is skipped, always 0 sleep.
        for _ in range(50):
            assert cls._reserve_utc_window_slot() == 0.0

    def test_per_minute_cap_sleeps_to_next_boundary_when_full(self) -> None:
        cls = self._fresh_adapter_cls()
        cls._window_max_per_min = 5
        # First 5 in the window: no sleep (matches X-RateLimit-Remaining counting down).
        for _ in range(5):
            assert cls._reserve_utc_window_slot() == 0.0
        # 6th exceeds the cap → sleep to the next UTC :00 boundary (a positive value).
        sleep = cls._reserve_utc_window_slot()
        assert sleep > 0.0
        assert sleep <= 60.0  # never more than one minute to the next boundary

    def test_set_rate_budget_rpm_sets_minute_window_cap(self) -> None:
        cls = self._fresh_adapter_cls()
        adapter = cls(api_key=None)
        adapter.set_rate_budget_rpm(120)
        assert cls._window_max_per_min == 120
        assert abs(cls._min_request_interval - (60.0 / 120)) < 1e-9

    def test_set_window_quota_carries_daily_share(self) -> None:
        cls = self._fresh_adapter_cls()
        adapter = cls(api_key=None)
        adapter.set_window_quota(per_minute=90, per_day=45_000)
        assert cls._window_max_per_min == 90
        assert cls._window_max_per_day == 45_000
