"""Sports reference fixture-id filtering helpers (MVP-league + recovery allowlist).

Cohesion module of the ``engine.orchestrator`` package — carries the
enrichment-scope filters decomposed out of ``_fetch_sports_reference_data``
(function-size ratchet; pure behaviour-preserving extraction, same plan as
the earlier core/fixtures split):
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``.

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

from unified_api_contracts.sports import get_mvp_football_league_ids

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_filter_fixtures_for_enrichment",
]


def _filter_fixtures_for_enrichment(
    *,
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    recovery_fixture_ids: frozenset[int] | None,
    date: str,
) -> list[int] | None:
    """Apply the MVP-league filter and the optional recovery-mode allowlist filter
    to ``fixture_ids`` before the per-fixture entity loop.

    MVP-league filter — runs BEFORE the per-fixture entity loop so per-fixture
    ENRICHMENT (stats/events/lineups/player-stats) never follows the much wider
    FIXTURES curated-universe denominator (get_expected_leagues_for_source,
    383 leagues) out past MVP/prediction scope (96 leagues). Unconditional —
    applies to both the URDI-sourced fixture_ids_override path (spans all 383
    leagues since the 2026-07-24 widening) and the API-fallback path. A fixture
    with NO resolved league (mapping gap) is kept, not dropped — we can only
    prove a fixture is OUT of scope, never assume it, from a missing mapping.

    Recovery-mode fixture-id allowlist filter — runs AFTER the MVP filter so we
    only call api_football for the targeted set. Lifts the per-fixture work
    from O(all_fixtures_on_day x 5 entities) to O(allowlist_intersection_with_day
    x N_requested_entities). Used for targeted recovery (e.g. Phase 2's
    truth-set audit produced a 39k fixture-id list; we feed it here so we don't
    re-burn ~560k api_football calls re-fetching already-captured fixtures'
    per-fixture entities).

    Returns ``None`` when the recovery allowlist intersects the fixture set to
    zero — the caller must return early (skipping the per-fixture loop AND the
    cross-provider mapping writes) so we don't write phantom empty manifest
    rows for entities never attempted on this date.
    """
    if fixture_ids:
        _mvp_leagues = get_mvp_football_league_ids()
        _pre_mvp_filter = len(fixture_ids)
        fixture_ids = [
            fid for fid in fixture_ids if (_lg := af_fid_to_league.get(str(fid))) is None or _lg in _mvp_leagues
        ]
        _mvp_skipped = _pre_mvp_filter - len(fixture_ids)
        if _mvp_skipped:
            _orch.logger.info(
                "MVP-league filter applied for date=%s: %d → %d fixtures (%d skipped — non-MVP league, "
                "enrichment out of scope)",
                date,
                _pre_mvp_filter,
                len(fixture_ids),
                _mvp_skipped,
            )

    if recovery_fixture_ids is not None and fixture_ids:
        _pre_filter = len(fixture_ids)
        fixture_ids = [fid for fid in fixture_ids if fid in recovery_fixture_ids]
        _orch.logger.info(
            "Recovery fixture-id filter applied for date=%s: %d → %d fixtures (%d skipped — not in allowlist)",
            date,
            _pre_filter,
            len(fixture_ids),
            _pre_filter - len(fixture_ids),
        )
        if not fixture_ids:
            # Allowlist intersected to zero on this date — no per-fixture work
            # to do. Signal the caller to return early so we don't write
            # phantom empty manifest rows for entities we never attempted to
            # fetch on this date.
            _orch.logger.info(
                "Recovery fixture-id filter: no targeted fixtures on date=%s — skipping per-fixture loop",
                date,
            )
            return None

    return fixture_ids
