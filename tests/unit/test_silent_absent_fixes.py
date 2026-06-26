"""Unit tests for the 4 silent-absent manifest cell fixes.

Covers (operator directive 2026-06-26 — instruments-service producer):
  1. Pre-genesis DeFi venue → empty_confirmed(EXPECTED_PRE_GENESIS_CHAIN).
  2. Pre-venue-launch CeFi venue → empty_confirmed(EXPECTED_PRE_VENUE_LAUNCH).
  3. Between chain-genesis and protocol-launch → empty_confirmed(EXPECTED_PRE_VENUE_LAUNCH).
  4. No-activity PREDICTION venue (empty_ok) → empty_confirmed(SOURCE_RETURNED_ZERO).
  5. TradFi non-trading-day in missing_shards → empty_confirmed(EXPECTED_WEEKEND/HOLIDAY).
  6. Already-captured row is NOT overwritten by pre-launch stamp.
  7. Already attempted_failed row is NOT overwritten.

Plan: instruments_foundation_completeness_2026_06_24.md.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from unified_api_contracts import EmptyConfirmedReason, FetchEvidence
from unified_trading_library import CaptureStatus

from instruments_service.engine.orchestrator.process_write import (
    _pre_launch_empty_reason,
    _seed_expected_unattempted_for_target_universe,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


# ---------------------------------------------------------------------------
# Fake manifest helpers (same pattern as test_orchestrator_eu_seeding.py)
# ---------------------------------------------------------------------------


def _key_tuple(row_key: Mapping[str, object]) -> tuple[str, str, str]:
    return (
        str(row_key.get("date", "")),
        str(row_key.get("venue", "")),
        str(row_key.get("chain", "")),
    )


class _FakeManifest:
    """Lightweight ManifestWriter stand-in for Fix 1 / seeding tests."""

    def __init__(self, prior: dict[tuple[str, str, str], tuple[str, str]] | None = None) -> None:
        self._prior = prior or {}
        self.eu_calls: list[tuple[str, str, str]] = []
        # (key, reason) for record_expected_empty calls (pre-launch stamps)
        self.expected_empty_calls: list[tuple[tuple[str, str, str], str]] = []

    def lookup(self, row_key: Mapping[str, object]) -> SimpleNamespace | None:
        hit = self._prior.get(_key_tuple(row_key))
        if hit is None:
            return None
        return SimpleNamespace(capture_status=hit[0], error_reason=hit[1])

    def record_expected_unattempted(self, *, row_key: Mapping[str, object], **_: object) -> None:
        self.eu_calls.append(_key_tuple(row_key))

    def record_expected_empty(self, *, row_key: Mapping[str, object], reason: str, **_: object) -> None:
        self.expected_empty_calls.append((_key_tuple(row_key), reason))

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Fix 1 tests — _pre_launch_empty_reason helper
# ---------------------------------------------------------------------------


class TestPreLaunchEmptyReason:
    """Unit tests for the _pre_launch_empty_reason helper function."""

    def test_defi_pre_genesis_returns_expected_pre_genesis_chain(self) -> None:
        """Date before chain genesis → EXPECTED_PRE_GENESIS_CHAIN."""
        # ETHEREUM genesis is 2015-07-30; using a very early date.
        reason = _pre_launch_empty_reason("AAVE_V3-ETHEREUM", "ETHEREUM", "2015-01-01")
        assert reason == EmptyConfirmedReason.EXPECTED_PRE_GENESIS_CHAIN.value

    def test_defi_after_genesis_but_pre_protocol_returns_expected_pre_venue_launch(self) -> None:
        """Date between chain genesis and protocol launch → EXPECTED_PRE_VENUE_LAUNCH."""
        # ETHEREUM genesis ~2015-07; AAVE_V3 deployed March 2022
        # A 2016 date: chain is live, protocol not yet deployed.
        reason = _pre_launch_empty_reason("AAVE_V3-ETHEREUM", "ETHEREUM", "2016-06-01")
        assert reason == EmptyConfirmedReason.EXPECTED_PRE_VENUE_LAUNCH.value

    def test_cefi_pre_launch_returns_expected_pre_venue_launch(self) -> None:
        """CeFi venue (no chain) always returns EXPECTED_PRE_VENUE_LAUNCH."""
        reason = _pre_launch_empty_reason("HYPERLIQUID", "", "2020-01-01")
        assert reason == EmptyConfirmedReason.EXPECTED_PRE_VENUE_LAUNCH.value

    def test_unknown_chain_falls_back_to_expected_pre_venue_launch(self) -> None:
        """Unknown chain (get_chain_genesis_date returns None) → EXPECTED_PRE_VENUE_LAUNCH."""
        reason = _pre_launch_empty_reason("FOO-UNKNOWNCHAIN", "UNKNOWNCHAIN", "2020-01-01")
        assert reason == EmptyConfirmedReason.EXPECTED_PRE_VENUE_LAUNCH.value


# ---------------------------------------------------------------------------
# Fix 1 tests — pre-launch stamping in the seeder
# ---------------------------------------------------------------------------


class TestPreLaunchStampingInSeeder:
    """Integration tests: seeder stamps pre-launch cells instead of staying absent."""

    def test_pre_venue_launch_cefi_stamps_expected_pre_venue_launch(self) -> None:
        """HYPERLIQUID (discovery start 2023-11-01) on a 2020 date → EXPECTED_PRE_VENUE_LAUNCH."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2020-01-01",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        pre_launch_venues = {v for (_, v, _c), _r in manifest.expected_empty_calls}
        assert "HYPERLIQUID" in pre_launch_venues
        reasons_for_hl = [r for (_, v, _c), r in manifest.expected_empty_calls if v == "HYPERLIQUID"]
        assert all(r == EmptyConfirmedReason.EXPECTED_PRE_VENUE_LAUNCH.value for r in reasons_for_hl)

    def test_pre_launch_venue_not_in_eu_calls(self) -> None:
        """Pre-launch venue must NOT appear in EU (expected_unattempted) calls."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2020-01-01",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        seeded_eu_venues = {v for (_d, v, _c) in manifest.eu_calls}
        assert "HYPERLIQUID" not in seeded_eu_venues

    def test_existing_captured_row_not_overwritten_by_pre_launch(self) -> None:
        """An existing captured row is NEVER overwritten, even when date is pre-launch."""
        manifest = _FakeManifest(
            prior={("2020-01-01", "HYPERLIQUID", ""): (CaptureStatus.CAPTURED.value, "")}
        )
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2020-01-01",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        # No pre_launch stamp for HYPERLIQUID since prior row exists.
        pre_launch_venues = {v for (_, v, _c), _r in manifest.expected_empty_calls}
        assert "HYPERLIQUID" not in pre_launch_venues

    def test_existing_attempted_failed_not_overwritten(self) -> None:
        """An existing attempted_failed row is NEVER overwritten by a pre-launch stamp."""
        manifest = _FakeManifest(
            prior={("2020-01-01", "BYBIT", ""): (CaptureStatus.ATTEMPTED_FAILED.value, "")}
        )
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2020-01-01",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        pre_launch_venues = {v for (_, v, _c), _r in manifest.expected_empty_calls}
        assert "BYBIT" not in pre_launch_venues

    def test_in_universe_venue_still_gets_eu_not_pre_launch(self) -> None:
        """A venue that IS in-universe on the date gets EU, not a pre-launch stamp."""
        manifest = _FakeManifest()
        # BYBIT has been live since 2018-12-01, so 2026-06-20 is in-universe.
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        seeded_eu_venues = {v for (_d, v, _c) in manifest.eu_calls}
        assert "BYBIT" in seeded_eu_venues
        pre_launch_venues = {v for (_, v, _c), _r in manifest.expected_empty_calls}
        assert "BYBIT" not in pre_launch_venues


# ---------------------------------------------------------------------------
# Fix 2 tests — SOURCE_RETURNED_ZERO for empty_ok_venues
# ---------------------------------------------------------------------------


class TestEmptyOkVenuesGetSourceReturnedZero:
    """Fix 2: venues in empty_ok_venues get SOURCE_RETURNED_ZERO stamp."""

    def test_empty_ok_venues_stamped_source_returned_zero(self) -> None:
        """empty_ok_venues → SOURCE_RETURNED_ZERO with valid FetchEvidence.

        We test the core of Fix 2 by calling _completeness_and_retry with
        a venue in non_error_venues that returned 0 records (not in counts).
        The fix stamps SOURCE_RETURNED_ZERO for it in a new ManifestWriter.
        """
        import asyncio

        from instruments_service.engine.orchestrator.process_completeness import (
            _completeness_and_retry,
        )

        _captured_records: list[dict[str, object]] = []

        class _CapturingManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_zero_rows(
                self,
                *,
                row_key: Mapping[str, object],
                reason: str,
                was_expected: bool,
                fetch_evidence: FetchEvidence | None = None,
                **_kw: object,
            ) -> None:
                # Fix 2 routes through record_zero_rows(was_expected=False).
                if not was_expected:
                    _captured_records.append(
                        {"row_key": dict(row_key), "reason": reason, "evidence": fetch_evidence}
                    )

            def record_empty(self, **_kw: object) -> None:
                pass

            def record_failed(self, **_: object) -> None:
                pass

            def record_expected_empty(self, **_: object) -> None:
                pass

            def close(self) -> None:
                pass

        with (
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapturingManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._check_emission_policy",
                return_value=MagicMock(
                    service_emission_state="PUBLISHED",
                    completeness_fraction=1.0,
                ),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                return_value=False,
            ),
        ):
            asyncio.run(
                _completeness_and_retry(
                    counts={},
                    date="2026-06-20",
                    date_dt=MagicMock(),
                    defi_venue_set=None,
                    asset_groups=["PREDICTION"],
                    api_keys=None,
                    mode="batch",
                    source=None,
                    bucket="prediction-bucket",
                    sink=MagicMock(),
                    sampler=MagicMock(),
                    active_venues=["POLYMARKET"],
                    non_error_venues={"POLYMARKET"},
                    validation_failed_venues=set(),
                    retryable_venues=[],
                    is_sports_run=False,
                    sports_entity_filter=None,
                    recovery_fixture_ids=None,
                )
            )

        # POLYMARKET ran OK but returned 0 → must have SOURCE_RETURNED_ZERO stamp.
        src_zero = [r for r in _captured_records if r.get("reason") == EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value]
        assert len(src_zero) >= 1, f"Expected SOURCE_RETURNED_ZERO record; got {_captured_records}"
        ev = src_zero[0]["evidence"]
        assert isinstance(ev, FetchEvidence), f"Expected FetchEvidence, got {type(ev)}"
        assert ev.proves_honest_absence(), f"FetchEvidence must prove honest absence: {ev}"


# ---------------------------------------------------------------------------
# Fix 3 tests — TradFi non-trading-day in missing_shards → empty_confirmed
# ---------------------------------------------------------------------------


class TestTradfiNonTradingDayInMissingShards:
    """Fix 3: TradFi missing shard on non-trading day → empty_confirmed, not attempted_failed."""

    def test_tradfi_non_trading_day_missing_shard_stamps_empty_confirmed(self) -> None:
        """A TradFi venue in missing_shards on a non-trading day → empty_confirmed."""
        from instruments_service.engine.orchestrator.process_completeness import (
            _finalize_completeness,
        )

        _expected_empty_calls: list[dict[str, object]] = []
        _failed_calls: list[dict[str, object]] = []

        class _CapturingManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_expected_empty(self, *, row_key: Mapping[str, object], reason: str, **_kw: object) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

            def record_failed(self, *, row_key: Mapping[str, object], **_kw: object) -> None:
                _failed_calls.append({"row_key": dict(row_key)})

            def record_empty(self, **_: object) -> None:
                pass

            def close(self) -> None:
                pass

        with (
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapturingManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._check_emission_policy",
                return_value=MagicMock(
                    service_emission_state="PUBLISHED",
                    completeness_fraction=1.0,
                ),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.is_non_trading_day", return_value=True),
            patch("instruments_service.engine.orchestrator.non_trading_day_reason", return_value="EXPECTED_WEEKEND"),
            patch("instruments_service.engine.orchestrator._TRADFI_VENUES", ["NYSE", "NASDAQ"]),
        ):
            # Use 2 expected venues and 1 written to stay above the 50% catastrophic threshold.
            # NYSE is missing; NASDAQ is already written (in counts).
            _finalize_completeness(
                counts={"NASDAQ": 10},
                date="2026-06-21",  # Saturday — non-trading for NYSE
                expected_venues={"NYSE", "NASDAQ"},
                written_venues={"NASDAQ"},
                missing_shards={"NYSE"},
                bucket="tradfi-bucket",
            )

        # NYSE missing on non-trading day → empty_confirmed with EXPECTED_WEEKEND reason.
        assert len(_expected_empty_calls) == 1, f"Expected 1 empty_confirmed, got {_expected_empty_calls}"
        assert _expected_empty_calls[0]["row_key"] == {"date": "2026-06-21", "venue": "NYSE"}
        assert _expected_empty_calls[0]["reason"] in (
            EmptyConfirmedReason.EXPECTED_WEEKEND.value,
            EmptyConfirmedReason.EXPECTED_HOLIDAY.value,
        )
        # No attempted_failed for NYSE (it's non-trading).
        assert _failed_calls == [], f"Unexpected attempted_failed calls: {_failed_calls}"

    def test_non_tradfi_missing_shard_still_stamps_attempted_failed(self) -> None:
        """A non-TradFi venue in missing_shards always gets attempted_failed (not weekend)."""
        from instruments_service.engine.orchestrator.process_completeness import (
            _finalize_completeness,
        )

        _failed_calls: list[dict[str, object]] = []
        _expected_empty_calls: list[dict[str, object]] = []

        class _CapturingManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_failed(self, *, row_key: Mapping[str, object], **_kw: object) -> None:
                _failed_calls.append({"row_key": dict(row_key)})

            def record_expected_empty(self, *, row_key: Mapping[str, object], reason: str, **_kw: object) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

            def record_empty(self, **_: object) -> None:
                pass

            def close(self) -> None:
                pass

        with (
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapturingManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._check_emission_policy",
                return_value=MagicMock(
                    service_emission_state="PUBLISHED",
                    completeness_fraction=1.0,
                ),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            # BYBIT is NOT in _TRADFI_VENUES (it's CeFi)
            patch("instruments_service.engine.orchestrator._TRADFI_VENUES", []),
        ):
            # Use 2 expected venues and 1 written to stay above the 50% catastrophic threshold.
            _finalize_completeness(
                counts={"DERIBIT": 5},
                date="2026-06-21",
                expected_venues={"BYBIT", "DERIBIT"},
                written_venues={"DERIBIT"},
                missing_shards={"BYBIT"},
                bucket="cefi-bucket",
            )

        assert len(_failed_calls) == 1, f"Expected 1 attempted_failed, got {_failed_calls}"
        assert _failed_calls[0]["row_key"]["venue"] == "BYBIT"
        assert _expected_empty_calls == [], f"No empty_confirmed expected for BYBIT: {_expected_empty_calls}"
