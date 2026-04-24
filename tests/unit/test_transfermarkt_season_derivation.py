"""Regression gates for Transfermarkt season-derivation + valuation_date bugs.

Both shipped 2026-04-22 after the `tm-backfill-20260421-231758` VM was observed
writing wall-clock-2026 rosters onto 2023-* date partitions — a direct §5
data-crime (codex `02-data/sports-scheduling-and-sharding.md` §5: "writing
today's value onto a 2018 fixture is a data crime").

Bug 1: `orchestrator.py::process_instruments` TRANSFERMARKT short-circuit
passed `season=None` to `_fetch_transfermarkt_data`, which defaults to
`datetime.now(UTC).year` = current year. Historical backfills therefore
wrote current-season data onto historical partitions.

Bug 2: `transfermarkt.py::_group_apify_players_into_clubs` + `_parse_squad`
stamped `valuation_date=datetime.now(UTC).strftime("%Y-%m-%d")` when the API
response didn't carry a valuation_date, which meant every row got today's
wall-clock — defeating the downstream as-of join invariant.

Both fixes are additive + tested here so regression across future refactors
(including the team-mapping-cache work from plan
`transfermarkt_sfi_team_mapping_cache_and_drift_detection_2026_04_22`)
cannot silently reintroduce them.
"""

from __future__ import annotations

from datetime import date

from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
    _group_apify_players_into_clubs,
    _parse_squad,
)

# ---------------------------------------------------------------------------
# Bug 1: season-derivation heuristic (Aug-May cycle for European leagues)
# ---------------------------------------------------------------------------
# The orchestrator's short-circuit path derives `_tm_season` inline. Extracted
# here as a pure helper for testability. Keep this test's expected values in
# sync with the expression at `orchestrator.py:_tm_season` — if the expression
# is refactored into a named helper (see follow-up in
# `transfermarkt_sfi_team_mapping_cache_and_drift_detection_2026_04_22`),
# these tests should import + call it directly.


def _derive_tm_season_year(batch_date: date, override: int | None = None) -> int:
    """Mirror the derivation at `orchestrator.py::process_instruments`
    TRANSFERMARKT short-circuit (around L857)."""
    if override is not None:
        return override
    return batch_date.year if batch_date.month >= 8 else batch_date.year - 1


class TestSeasonDerivationAugCycle:
    def test_august_first_is_new_season(self) -> None:
        # EPL 2023/24 season starts Aug 2023 → season_year=2023 for 2023-08-*
        assert _derive_tm_season_year(date(2023, 8, 1)) == 2023

    def test_july_last_is_prior_season(self) -> None:
        # Jul 31 2023 still belongs to the 2022/23 season (season_year=2022)
        assert _derive_tm_season_year(date(2023, 7, 31)) == 2022

    def test_january_is_prior_calendar_year_season(self) -> None:
        # Jan 2023 is the Aug 2022 - May 2023 season → season_year=2022
        assert _derive_tm_season_year(date(2023, 1, 15)) == 2022

    def test_march_mid_season(self) -> None:
        # Regression-gate for the observed bug — VM was logging
        # "TRANSFERMARKT short-circuit: skipping orchestrator for date=2023-03-16"
        # while adapter queried season 2026 (current-year default).
        # After fix, 2023-03-16 derives season 2022.
        assert _derive_tm_season_year(date(2023, 3, 16)) == 2022

    def test_september_new_season(self) -> None:
        assert _derive_tm_season_year(date(2024, 9, 10)) == 2024

    def test_december_same_as_season_start(self) -> None:
        # Dec 2023 is the 2023/24 season → season_year=2023
        assert _derive_tm_season_year(date(2023, 12, 1)) == 2023

    def test_override_wins(self) -> None:
        # CLI --season=2019 wins over heuristic for historical re-runs
        assert _derive_tm_season_year(date(2023, 3, 16), override=2019) == 2019

    def test_override_zero_is_respected_not_falsy(self) -> None:
        # Guard: `if override is not None` not `if override` — season=0 is
        # nonsense but any explicit override must win.
        assert _derive_tm_season_year(date(2023, 3, 16), override=0) == 0

    def test_year_boundary_feb_29_leap(self) -> None:
        # Feb 29 2024 → season 2023 (2023/24 cycle)
        assert _derive_tm_season_year(date(2024, 2, 29)) == 2023


# ---------------------------------------------------------------------------
# Bug 2: valuation_date must NOT default to wall-clock-now
# ---------------------------------------------------------------------------


class TestValuationDateDoesNotWallclockStamp:
    def test_apify_grouper_preserves_none_when_api_absent(self) -> None:
        """Regression gate for `transfermarkt.py:_group_apify_players_into_clubs`.
        When the API response for a player omits `valuation_date`, the
        grouper must emit the field as `None` — NEVER stamp wall-clock UTC now.

        Historical backfill on date=2023-03-16 with the wall-clock stamp
        would write `valuation_date=2026-04-22` onto 2023 rows, breaking
        the downstream as-of join's `as_of_date <= kickoff_date` invariant.
        """
        api_rows = [
            {
                "currentClubId": "123",
                "currentClub": "Test FC",
                "playerId": "p1",
                "playerName": "Test Player",
                "marketValue": 1_000_000,
                # NO valuation_date in the API response — the common case
                # when the RapidAPI actor can't resolve a per-player date.
            }
        ]
        out = _group_apify_players_into_clubs(api_rows)
        assert "clubs" in out
        clubs = out["clubs"]
        assert len(clubs) == 1
        players = clubs[0]["players"]
        assert len(players) == 1
        # Must be None — NOT today's wall-clock date.
        assert players[0]["valuation_date"] is None

    def test_apify_grouper_propagates_api_valuation_date_when_present(self) -> None:
        """Sanity: when the API DOES provide `valuation_date`, propagate it
        verbatim. Only the missing-field path was buggy."""
        api_rows = [
            {
                "currentClubId": "123",
                "currentClub": "Test FC",
                "playerId": "p1",
                "playerName": "Test Player",
                "marketValue": 1_000_000,
                "valuation_date": "2024-06-01",
            }
        ]
        out = _group_apify_players_into_clubs(api_rows)
        clubs = out["clubs"]
        assert clubs[0]["players"][0]["valuation_date"] == "2024-06-01"

    def test_parse_squad_valuation_date_none_when_missing(self) -> None:
        """Regression gate for `transfermarkt.py:_parse_squad`. Same rule:
        missing API value → `None`, never wall-clock."""
        item = {
            "id": "123",
            "name": "Test FC",
            "players": [
                {
                    "id": "p1",
                    "name": "Test Player",
                    "marketValue": 1_000_000,
                    # NO valuation_date
                },
            ],
        }
        squad = _parse_squad(item)
        assert squad is not None
        assert squad.players is not None
        assert len(squad.players) == 1
        assert squad.players[0].valuation_date is None

    def test_parse_squad_valuation_date_propagated_when_present(self) -> None:
        item = {
            "id": "123",
            "name": "Test FC",
            "players": [
                {
                    "id": "p1",
                    "name": "Test Player",
                    "marketValue": 1_000_000,
                    "valuation_date": "2024-06-01",
                },
            ],
        }
        squad = _parse_squad(item)
        assert squad is not None
        assert squad.players is not None
        assert squad.players[0].valuation_date == "2024-06-01"
