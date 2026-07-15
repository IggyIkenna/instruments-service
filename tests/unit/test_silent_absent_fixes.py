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

import contextlib
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
        manifest = _FakeManifest(prior={("2020-01-01", "HYPERLIQUID", ""): (CaptureStatus.CAPTURED.value, "")})
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
        manifest = _FakeManifest(prior={("2020-01-01", "BYBIT", ""): (CaptureStatus.ATTEMPTED_FAILED.value, "")})
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
                    _captured_records.append({"row_key": dict(row_key), "reason": reason, "evidence": fetch_evidence})

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
# Venue-vs-composite-key fold — real PREDICTION writes must NOT be misread as
# empty (regression for prediction_universe_capture_dead_since_07_01_2026_07_06.md,
# 2026-07-13 progress entry: KALSHI/POLYMARKET genuinely wrote real rows under
# the composite manifest key "{VENUE}/{GROUP}" (e.g. "KALSHI/OTHER") but the
# shard-completeness stage compared BARE venue names against `counts.keys()`,
# so `written_venues` never contained bare "KALSHI" and the venue was
# misclassified as empty_ok → dishonest SOURCE_RETURNED_ZERO.
# ---------------------------------------------------------------------------


class TestFoldWrittenVenues:
    """`_fold_written_venues` collapses composite counts keys onto bare venues."""

    def test_prediction_composite_keys_fold_to_bare_venue(self) -> None:
        from instruments_service.engine.orchestrator.process_completeness import (
            _fold_written_venues,
        )

        counts = {"KALSHI/OTHER": 1422, "KALSHI/BTC_UP_DOWN_DAILY": 12, "POLYMARKET/OTHER": 7577}
        folded = _fold_written_venues(counts, {"KALSHI", "POLYMARKET"})
        assert folded == {"KALSHI", "POLYMARKET"}

    def test_sports_fixtures_composite_keys_fold_to_api_football(self) -> None:
        """``"FIXTURES/{league_id}"`` folds to ``API_FOOTBALL`` when it's expected.

        Root-cause fix (api_football_fixtures_stuck_612_residual_2026_07_15,
        see plans/active/sports_data_sources_canonical_completion_2026_07_13.md):
        previously this composite key was left UNFOLDED (``"FIXTURES"`` is a
        data_type, never a venue name, so the generic prefix-in-expected_venues
        check never matched it) — a league-scoped FIXTURES run whose target
        league legitimately had zero fixtures that day (off-season, rest day,
        etc — the common case) always left ``written_venues`` without
        ``API_FOOTBALL``, so completeness misclassified the whole venue as
        ``SOURCE_RETURNED_ZERO`` and stamped a REDUNDANT blanket
        ``{date, venue}`` row that live-verified to collide with (and drop)
        the correct per-league honest-absence row in the same per-VM manifest
        shard flush — permanently orphaning any pre-existing stale
        ``attempted_failed`` row for that (date, league) FIXTURES cell.
        """
        from instruments_service.engine.orchestrator.process_completeness import (
            _fold_written_venues,
        )

        counts = {"FIXTURES/39": 10, "FIXTURES/61": 4}
        folded = _fold_written_venues(counts, {"API_FOOTBALL"})
        assert folded == {"API_FOOTBALL"}

    def test_sports_fixtures_composite_keys_untouched_when_api_football_not_expected(self) -> None:
        """When ``API_FOOTBALL`` isn't in ``expected_venues`` (out of scope for
        this run), a ``"FIXTURES/*"`` key must pass through unfolded — the
        API_FOOTBALL-specific fold only applies when that venue is actually
        expected this run.
        """
        from instruments_service.engine.orchestrator.process_completeness import (
            _fold_written_venues,
        )

        counts = {"FIXTURES/39": 10}
        folded = _fold_written_venues(counts, {"SOME_OTHER_VENUE"})
        assert folded == {"FIXTURES/39"}

    def test_bare_cefi_keys_are_noop(self) -> None:
        from instruments_service.engine.orchestrator.process_completeness import (
            _fold_written_venues,
        )

        counts = {"BINANCE-FUTURES": 678}
        folded = _fold_written_venues(counts, {"BINANCE-FUTURES"})
        assert folded == {"BINANCE-FUTURES"}


class TestPredictionCompositeWriteNotMisreadAsEmpty:
    """Regression: real KALSHI/POLYMARKET writes under composite manifest keys
    must NOT trigger the empty_ok / SOURCE_RETURNED_ZERO path."""

    def test_kalshi_real_composite_write_not_stamped_source_returned_zero(self) -> None:
        import asyncio

        from instruments_service.engine.orchestrator.process_completeness import (
            _completeness_and_retry,
        )

        _captured_records: list[dict[str, object]] = []
        _failed_records: list[dict[str, object]] = []

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
                if not was_expected:
                    _captured_records.append({"row_key": dict(row_key), "reason": reason, "evidence": fetch_evidence})

            def record_empty(self, **_kw: object) -> None:
                pass

            def record_failed(self, *, row_key: Mapping[str, object], **_kw: object) -> None:
                _failed_records.append(dict(row_key))

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
            patch(
                "instruments_service.engine.orchestrator.read_availability_index",
                side_effect=Exception("no index in this unit test"),
            ),
        ):
            asyncio.run(
                _completeness_and_retry(
                    # KALSHI wrote real rows under the composite manifest key
                    # ("{VENUE}/{GROUP}") that _write_prediction_venue actually uses.
                    counts={"KALSHI/OTHER": 1422, "KALSHI/KXNFLGAME": 8},
                    date="2026-07-09",
                    date_dt=MagicMock(),
                    defi_venue_set=None,
                    asset_groups=["PREDICTION"],
                    api_keys=None,
                    mode="batch",
                    source=None,
                    bucket="prediction-bucket",
                    sink=MagicMock(),
                    sampler=MagicMock(),
                    active_venues=["KALSHI"],
                    non_error_venues={"KALSHI"},
                    validation_failed_venues=set(),
                    retryable_venues=[],
                    is_sports_run=False,
                    sports_entity_filter=None,
                    recovery_fixture_ids=None,
                )
            )

        # KALSHI genuinely wrote 1,430 rows across 2 canonical_question_groups —
        # must NOT be classified empty_ok, so no dishonest SOURCE_RETURNED_ZERO.
        src_zero = [r for r in _captured_records if r.get("reason") == EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value]
        assert not src_zero, f"KALSHI wrote real data — must not get SOURCE_RETURNED_ZERO; got {src_zero}"
        # Nor should it be treated as a permanently-missing shard (attempted_failed).
        assert not any(r.get("venue") == "KALSHI" for r in _failed_records), (
            f"KALSHI wrote real data — must not be in missing_shards/attempted_failed; got {_failed_records}"
        )


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
            # BYBIT is NOT a TradFi venue (it's CeFi) — VENUE_TO_ASSET_GROUP confirms this
            # without any patching needed (BYBIT maps to "cefi" in UAC).
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


# ---------------------------------------------------------------------------
# Fix 4 tests — tradfi non-trading day write-stage stamping
# (process_write._write_all_venues pre-stamp + _zero_records_non_sports fix)
# ---------------------------------------------------------------------------


class TestTradfiNonTradingDayWriteStage:
    """Fix 4: tradfi weekend/holiday → empty_confirmed even when adapter
    returned look-back records (DBEQ 5-day look-back artefact).

    Root cause: the DBEQ.BASIC equity adapter uses a 5-day look-back window,
    so a Saturday query returns Friday's instrument definitions.  Without the
    fix these records were written as *captured* for the Saturday date.
    The fix pre-stamps empty_confirmed in _write_all_venues and suppresses
    the parquet write via the _non_trading_tradfi guard.

    Tests here focus on _write_tradfi_non_trading_day_entries (which now returns
    the stamped set) and the new return-value contract that the callers rely on
    to exclude already-stamped venues from the non_error_venues fallback path.
    """

    def test_tradfi_weekend_write_stage_stamps_empty_confirmed(self) -> None:
        """NASDAQ on Saturday: _write_tradfi_non_trading_day_entries returns
        the set of stamped non-trading venues so callers can exclude them."""
        from instruments_service.engine.orchestrator.process_write import _write_tradfi_non_trading_day_entries

        _expected_empty_calls: list[dict[str, object]] = []
        counts: dict[str, int] = {}

        class _CapManifest:
            def record_expected_empty(
                self,
                *,
                row_key: Mapping[str, object],
                reason: str,
                **_kw: object,
            ) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

        # Saturday 2026-06-21 — NASDAQ/NYSE are non-trading; FX is 24/7
        # VENUE_TO_ASSET_GROUP membership is used for tradfi detection (no patching needed —
        # NASDAQ/NYSE/FX are tradfi in UAC; is_non_trading_day is stubbed to simulate Saturday).
        saturday = "2026-06-21"
        with (
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                side_effect=lambda v, _d: v in {"NASDAQ", "NYSE"},
            ),
            patch(
                "instruments_service.engine.orchestrator.non_trading_day_reason",
                return_value="EXPECTED_WEEKEND",
            ),
        ):
            stamped = _write_tradfi_non_trading_day_entries(
                date=saturday,
                # non_error_venues includes NASDAQ/NYSE (ran OK, 0 records) AND FX
                non_error_venues={"NASDAQ", "NYSE", "FX"},
                counts=counts,
                manifest_for_venue=lambda _v: _CapManifest(),  # pyright: ignore[reportArgumentType]
            )

        # Function must return the set of non-trading venues that were stamped.
        assert "NASDAQ" in stamped, f"NASDAQ must be in returned stamped set; got {stamped}"
        assert "NYSE" in stamped, f"NYSE must be in returned stamped set; got {stamped}"
        # FX is 24/7 — must NOT be in the stamped set.
        assert "FX" not in stamped, f"FX is 24/7 and must not be stamped: {stamped}"
        # empty_confirmed calls must exist for NASDAQ and NYSE.
        stamped_keys = {c["row_key"]["venue"] for c in _expected_empty_calls}
        assert "NASDAQ" in stamped_keys, f"NASDAQ must have empty_confirmed call; got {stamped_keys}"
        assert "NYSE" in stamped_keys, f"NYSE must have empty_confirmed call; got {stamped_keys}"
        # counts must be set to 0 for the stamped venues.
        assert counts.get("NASDAQ") == 0
        assert counts.get("NYSE") == 0

    def test_tradfi_trading_day_records_written_unaffected(self) -> None:
        """On a trading day (Wed), _write_tradfi_non_trading_day_entries returns
        an empty set (nothing pre-stamped, nothing suppressed)."""
        from instruments_service.engine.orchestrator.process_write import _write_tradfi_non_trading_day_entries

        counts: dict[str, int] = {}

        class _NeverCalled:
            def record_expected_empty(self, **_: object) -> None:
                raise AssertionError("record_expected_empty must not be called on trading day")

        # Wednesday — NASDAQ/NYSE are in UAC tradfi (VENUE_TO_ASSET_GROUP); is_non_trading_day
        # is stubbed to return False so neither is stamped (trading day).
        wednesday = "2026-06-24"  # Wednesday — trading day
        with (
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                return_value=False,  # All venues are trading on Wednesday
            ),
        ):
            stamped = _write_tradfi_non_trading_day_entries(
                date=wednesday,
                non_error_venues={"NASDAQ"},
                counts=counts,
                manifest_for_venue=lambda _v: _NeverCalled(),  # pyright: ignore[reportArgumentType]
            )

        # On a trading day no venues are stamped and the function returns empty set.
        assert stamped == set(), f"No venues should be stamped on a trading day; got {stamped}"
        assert "NASDAQ" not in counts, f"NASDAQ must not be in counts on trading day: {counts}"


class TestZeroRecordsNonSportsFixedForFX:
    """Fix for _zero_records_non_sports: non-trading venues are stamped individually.

    Root cause: old code required ALL tradfi venues to be non-trading.  FX is
    declared 24/7 (is_non_trading_day returns False), so whenever FX is in
    active_venues the condition ``len(non_trading) == len(tradfi_active)`` was
    always False and the entire non-trading path was unreachable.
    """

    def test_zero_records_fx_present_stamps_non_trading_venues(self) -> None:
        """FX in active_venues (24/7) does not block CME/NASDAQ from being stamped."""
        from instruments_service.engine.orchestrator.process_zero_records import _zero_records_non_sports

        _expected_empty_calls: list[dict[str, object]] = []

        class _CapManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_expected_empty(
                self,
                *,
                row_key: Mapping[str, object],
                reason: str,
                **_kw: object,
            ) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

            def write(self) -> None:
                pass

        # FX is 24/7 and is in active_venues — old code would have blocked
        # the stamp entirely (len(non_trading)=2 != len(tradfi_active)=3).
        # New code stamps CME and NASDAQ individually; FX (24/7 trading but
        # returned 0) causes the subsequent active-day-zero branch to raise.
        # We suppress that RuntimeError here — the stamps happen before the raise.
        # CME/NASDAQ/FX are in UAC tradfi (VENUE_TO_ASSET_GROUP). is_non_trading_day is stubbed
        # to simulate a Saturday where CME+NASDAQ are non-trading but FX is 24/7.
        with (
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                side_effect=lambda v, _d: v in {"CME", "NASDAQ"},
            ),
            patch(
                "instruments_service.engine.orchestrator.non_trading_day_reason",
                return_value="EXPECTED_WEEKEND",
            ),
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="tradfi-bucket",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            contextlib.suppress(RuntimeError),
        ):
            _zero_records_non_sports(
                date="2026-06-21",
                asset_groups=["TRADFI"],
                active_venues=["CME", "NASDAQ", "FX"],
                mode="batch",
            )

        # CME and NASDAQ must have been stamped with EXPECTED_WEEKEND.
        stamped_venues = {c["row_key"]["venue"] for c in _expected_empty_calls}
        assert "CME" in stamped_venues, f"CME must be stamped; got {stamped_venues}"
        assert "NASDAQ" in stamped_venues, f"NASDAQ must be stamped; got {stamped_venues}"
        # FX must NOT be stamped as non-trading.
        assert "FX" not in stamped_venues, f"FX must not be stamped: {stamped_venues}"

    def test_zero_records_all_non_trading_returns_cleanly(self) -> None:
        """When ALL tradfi venues are non-trading (FX absent), return dict cleanly."""
        from instruments_service.engine.orchestrator.process_zero_records import _zero_records_non_sports

        _expected_empty_calls: list[dict[str, object]] = []

        class _CapManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_expected_empty(
                self,
                *,
                row_key: Mapping[str, object],
                reason: str,
                **_kw: object,
            ) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

            def write(self) -> None:
                pass

        # CME/NASDAQ are UAC tradfi (VENUE_TO_ASSET_GROUP). Both are stamped as non-trading.
        with (
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                return_value=True,
            ),
            patch(
                "instruments_service.engine.orchestrator.non_trading_day_reason",
                return_value="EXPECTED_WEEKEND",
            ),
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="tradfi-bucket",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = _zero_records_non_sports(
                date="2026-06-21",
                asset_groups=["TRADFI"],
                active_venues=["CME", "NASDAQ"],
                mode="batch",
            )

        assert result == {"CME": 0, "NASDAQ": 0}, f"Expected clean dict; got {result}"
        stamped = {c["row_key"]["venue"] for c in _expected_empty_calls}
        assert "CME" in stamped and "NASDAQ" in stamped

    def test_non_tradfi_unaffected(self) -> None:
        """CeFi/DeFi venues are never touched by the tradfi non-trading logic."""
        from instruments_service.engine.orchestrator.process_zero_records import _zero_records_non_sports

        # BYBIT/DERIBIT are CeFi (VENUE_TO_ASSET_GROUP == "cefi") — not tradfi.
        # No patching needed; the membership check uses UAC directly.
        with (
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            # Active venues are CeFi — should raise RuntimeError (shard failure)
            # because zero records from CeFi on an active trading day is an error.
            import pytest

            with pytest.raises(RuntimeError):
                _zero_records_non_sports(
                    date="2026-06-24",
                    asset_groups=["CEFI"],
                    active_venues=["BYBIT", "DERIBIT"],
                    mode="batch",
                )


class TestZeroRecordsNoAdapterYetVenueDoesNotCrash:
    """Fix for a real production crash: an explicitly-requested (``--venue``
    override) TradFi venue that is UAC-declared ``NO_ADAPTER_YET`` (i.e. no URDI
    adapter is built yet) must resolve as an honest 0-result absence, never a
    hard failure.

    Fixture note (2026-07-15): this class formerly used ``YAHOO_FINANCE`` — a
    legacy source-as-venue artifact that was REMOVED from every venue-shaped UAC
    registry that day (source-as-venue modeling error; Yahoo is a *source*, not
    a *venue*). ``FX`` is now the sole ``NO_ADAPTER_YET`` venue in the tradfi
    asset group (``VENUE_TO_ADAPTER_KEY["FX"] == NO_ADAPTER_YET``), so it is the
    canonical fixture for the adapterless-tradfi-venue short-circuit under test.

    What the short-circuit guards: ``_zero_records_non_sports`` filters every
    ``NO_ADAPTER_YET`` venue out of ``tradfi_active`` (``v not in
    _no_adapter_active``). Without the dedicated short-circuit, a lone
    ``NO_ADAPTER_YET`` venue that returned 0 records would therefore leave
    ``tradfi_active`` empty and fall straight through to the terminal
    ``raise RuntimeError`` (URDI-returned-zero shard failure) — turning an
    honest, already-declared adapterless absence into a hard error. The
    short-circuit instead returns ``{venue: 0}`` and stamps an honest
    ``EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE`` manifest row. (For a tradfi
    venue that is *also* absent from the session/calendar SSOT the alternative
    failure mode is ``UndeclaredTradfiVenueError`` from ``is_non_trading_day`` —
    the short-circuit returns before that call is ever reached either way.)
    """

    def test_sole_no_adapter_yet_venue_returns_zero_counts_cleanly(self) -> None:
        """FX alone in active_venues must return {"FX": 0} AND stamp an honest
        empty_confirmed manifest row (no silent absence, no RuntimeError)."""
        from instruments_service.engine.orchestrator.process_zero_records import _zero_records_non_sports

        _expected_empty_calls: list[dict[str, object]] = []

        class _CapManifest:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            def record_expected_empty(
                self,
                *,
                row_key: Mapping[str, object],
                reason: str,
                **_kw: object,
            ) -> None:
                _expected_empty_calls.append({"row_key": dict(row_key), "reason": reason})

            def write(self) -> None:
                pass

        # NO_ADAPTER_YET / VENUE_TO_ADAPTER_KEY come straight from the real UAC
        # registry (FX is the sole tradfi NO_ADAPTER_YET venue), and the
        # short-circuit returns before ever reaching the tradfi calendar path /
        # the terminal RuntimeError. ManifestWriter/_get_instruments_bucket ARE
        # patched here (the manifest-stamp behavior needs a real bucket/writer in
        # production, not in a unit test).
        with (
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=_CapManifest,
            ),
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="tradfi-bucket",
            ),
        ):
            result = _zero_records_non_sports(
                date="2026-07-09",
                asset_groups=["TRADFI"],
                active_venues=["FX"],
                mode="batch",
            )

        assert result == {"FX": 0}, f"Expected clean zero-count dict; got {result}"
        assert len(_expected_empty_calls) == 1
        assert _expected_empty_calls[0]["row_key"] == {"date": "2026-07-09", "venue": "FX"}
        assert _expected_empty_calls[0]["reason"] == "EXPECTED_SOURCE_DOES_NOT_OFFER_DATA_TYPE"

    def test_no_adapter_yet_venue_mixed_with_real_tradfi_venue_excluded_from_calendar_check(
        self,
    ) -> None:
        """FX (NO_ADAPTER_YET) mixed with a real tradfi venue is excluded from
        tradfi_active (never passed to is_non_trading_day) while the real venue
        still gets the normal calendar treatment.
        """
        from instruments_service.engine.orchestrator.process_zero_records import _zero_records_non_sports

        _checked_venues: list[str] = []

        with (
            patch(
                "instruments_service.engine.orchestrator.is_non_trading_day",
                side_effect=lambda v, _d: (_checked_venues.append(v), True)[1],
            ),
            patch(
                "instruments_service.engine.orchestrator.non_trading_day_reason",
                return_value="EXPECTED_WEEKEND",
            ),
            patch(
                "instruments_service.engine.orchestrator.ManifestWriter",
                side_effect=lambda *_a, **_k: SimpleNamespace(
                    record_expected_empty=lambda **_kw: None, write=lambda: None
                ),
            ),
            patch(
                "instruments_service.engine.orchestrator._get_instruments_bucket",
                return_value="tradfi-bucket",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = _zero_records_non_sports(
                date="2026-06-21",
                asset_groups=["TRADFI"],
                active_venues=["CME", "FX"],
                mode="batch",
            )

        # is_non_trading_day must never have been called with FX (NO_ADAPTER_YET) — only CME.
        assert "FX" not in _checked_venues, (
            f"FX (NO_ADAPTER_YET) must never reach is_non_trading_day; checked={_checked_venues}"
        )
        assert "CME" in _checked_venues
        assert result == {"CME": 0}, f"Expected only CME stamped as non-trading; got {result}"
