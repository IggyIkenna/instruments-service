"""Unit tests for the IS producer's expected_unattempted (EU) seeding.

The IS daily producer materialises ``expected_unattempted`` for every TARGET-universe
(venue x day) cell that was NOT captured this run and has no prior row, so a missing
day reads 0% (honest gap) instead of being silently absent. Out-of-universe
(pre-venue-launch) venues are NOT seeded — they stay honestly absent.

Covers (operator directive 2026-06-26 — writer materialises EU, never re-derived):
  * target venue, not captured, no prior row → EU seeded (gap reads 0%, not absent);
  * out-of-universe (pre-launch) venue → NOT seeded (stays honestly absent);
  * captured this run → NOT seeded (record_captured already wrote the cell);
  * already-captured prior row → NOT overwritten;
  * already attempted_failed prior row → NOT overwritten (honest 4-state intact);
  * DeFi PROTOCOL-CHAIN venue → seeded on the canonical (venue=PROTOCOL, chain) atom
    that matches the captured-row key;
  * sports/prediction venue names (different grain) → never seeded.

Plan: instruments_foundation_completeness_2026_06_24.md §"Daily instrument-definition
backfill ... HONEST reasons".
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

from unified_trading_library import CaptureStatus

from instruments_service.engine.orchestrator.process_write import (
    _seed_expected_unattempted_for_target_universe,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def _key_tuple(row_key: Mapping[str, object]) -> tuple[str, str, str]:
    """Normalise a row_key dict to a comparable (date, venue, chain) tuple."""
    return (
        str(row_key.get("date", "")),
        str(row_key.get("venue", "")),
        str(row_key.get("chain", "")),
    )


class _FakeManifest:
    """Lightweight ManifestWriter stand-in.

    ``prior`` maps (date, venue, chain) → (capture_status, error_reason); a missing
    key returns None from ``lookup`` (no prior row). Captures EU seed calls.
    """

    def __init__(self, prior: dict[tuple[str, str, str], tuple[str, str]] | None = None) -> None:
        self._prior = prior or {}
        self.eu_calls: list[tuple[str, str, str]] = []

    def lookup(self, row_key: Mapping[str, object]) -> SimpleNamespace | None:
        hit = self._prior.get(_key_tuple(row_key))
        if hit is None:
            return None
        return SimpleNamespace(capture_status=hit[0], error_reason=hit[1])

    def record_expected_unattempted(self, *, row_key: Mapping[str, object], **_: object) -> None:
        self.eu_calls.append(_key_tuple(row_key))


class TestEuSeeding:
    def test_target_venue_not_captured_no_prior_seeds_eu(self) -> None:
        """A configured cefi venue, not captured this run, no prior row → EU seeded."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        # BYBIT launched 2018-12-01, so it is in-universe on 2026-06-20 and uncaptured.
        assert ("2026-06-20", "BYBIT", "") in manifest.eu_calls

    def test_captured_this_run_not_seeded(self) -> None:
        """A venue captured this run is NOT seeded (record_captured wrote the cell)."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues={"BYBIT"},
        )
        assert ("2026-06-20", "BYBIT", "") not in manifest.eu_calls

    def test_already_captured_prior_row_not_overwritten(self) -> None:
        """An existing captured row in the manifest is NOT re-seeded as EU."""
        manifest = _FakeManifest(prior={("2026-06-20", "BYBIT", ""): (CaptureStatus.CAPTURED.value, "")})
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        assert ("2026-06-20", "BYBIT", "") not in manifest.eu_calls

    def test_attempted_failed_prior_row_not_overwritten(self) -> None:
        """An attempted_failed cell stays attempted_failed — EU never overwrites it.

        Honest 4-state: a fetch failure must remain visible as attempted_failed; EU
        seeding must not mask it with a 0% expected_unattempted cell.
        """
        manifest = _FakeManifest(prior={("2026-06-20", "BYBIT", ""): (CaptureStatus.ATTEMPTED_FAILED.value, "")})
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        # The seeder NEVER overwrites an existing row (it checks manifest.lookup is
        # None before seeding) — a stricter test than the capture-path
        # _should_skip_shard, which re-attempts failed shards on purpose. An
        # attempted_failed cell must stay visible, NOT be masked by a 0% EU cell.
        assert ("2026-06-20", "BYBIT", "") not in manifest.eu_calls

    def test_pre_launch_venue_not_seeded(self) -> None:
        """A venue whose discovery API is not live yet on the date is NOT seeded.

        Out-of-universe stays honestly absent (no fake 0%). HYPERLIQUID discovery
        starts 2023-06-14; a 2020 date is pre-launch → no EU seed.
        """
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2020-01-01",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        seeded_venues = {v for (_d, v, _c) in manifest.eu_calls}
        assert "HYPERLIQUID" not in seeded_venues

    def test_defi_venue_seeded_on_canonical_chain_atom(self) -> None:
        """A DeFi PROTOCOL-CHAIN venue is seeded on (venue=PROTOCOL, chain)."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["DEFI"],
            captured_venues=set(),
        )
        # AAVE_V3-ETHEREUM → canonical (venue=AAVE_V3, chain=ETHEREUM).
        assert ("2026-06-20", "AAVE_V3", "ETHEREUM") in manifest.eu_calls
        # The glued form must NOT appear (would be a key mismatch vs the captured row).
        assert ("2026-06-20", "AAVE_V3-ETHEREUM", "") not in manifest.eu_calls

    def test_sports_prediction_names_never_seeded(self) -> None:
        """ALL pulls in sports/prediction venue names — they are NOT venue-grain EU."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["ALL"],
            captured_venues=set(),
        )
        seeded_venues = {v for (_d, v, _c) in manifest.eu_calls}
        for non_venue_grain in (
            "API_FOOTBALL",
            "FOOTYSTATS",
            "UNDERSTAT",
            "TRANSFERMARKT",
            "SOCCER_FOOTBALL_INFO",
            "OPEN_METEO",
            "POLYMARKET",
            "KALSHI",
        ):
            assert non_venue_grain not in seeded_venues

    def test_synthetic_gap_day_reads_eu_not_absent(self) -> None:
        """Before/after: a synthetic cefi gap day (no captures, no prior rows) seeds EU
        for the whole in-universe venue set — so the day reads 0%, not absent."""
        manifest = _FakeManifest()
        # BEFORE: nothing captured, no prior rows → the day is ABSENT (0 EU calls yet).
        assert manifest.eu_calls == []
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["CEFI"],
            captured_venues=set(),
        )
        # AFTER: every in-universe cefi venue now carries an expected_unattempted cell
        # → day_coverage reads 0% (captured/expected = 0/N), not "absent".
        assert len(manifest.eu_calls) > 0
        seeded_venues = {v for (_d, v, _c) in manifest.eu_calls}
        # A spot of the cefi MVP universe is present (e.g. BYBIT, DERIBIT).
        assert "BYBIT" in seeded_venues
        assert "DERIBIT" in seeded_venues

    def test_no_venue_grain_asset_groups_is_noop(self) -> None:
        """SPORTS-only / PREDICTION-only runs do not venue-grain-seed (different atom)."""
        manifest = _FakeManifest()
        _seed_expected_unattempted_for_target_universe(
            manifest=manifest,  # pyright: ignore[reportArgumentType]
            date="2026-06-20",
            asset_groups=["SPORTS"],
            captured_venues=set(),
        )
        assert manifest.eu_calls == []
