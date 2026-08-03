"""Sports reference fixture-id filtering helpers (recovery allowlist).

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

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_entity_league_scope",
    "_filter_fixtures_for_enrichment",
]


def _entity_league_scope(af_entity_dt: str) -> frozenset[str] | None:
    """Resolve ``SPORTS_ENTITY_LEAGUE_COVERAGE`` for a per-fixture entity's
    canonical manifest data_type. ``None`` = all leagues; a frozenset restricts
    to those leagues — e.g. FIXTURE_EVENTS/PLAYER_STATS stay MVP-only while
    FIXTURE_STATS/FIXTURE_LINEUPS cover all leagues (operator ruling
    2026-07-28). Used by ``_gather_per_fixture_rows``'s task-queueing loop and
    its prefetch-skip lookup so a policy-out-of-scope league is neither
    fetched nor read for (matches ``emit_empty_gaps_for_entity``'s per-entity
    gap denominator too)."""
    return _orch.get_entity_league_coverage(af_entity_dt) if af_entity_dt else None


def _filter_fixtures_for_enrichment(
    *,
    fixture_ids: list[int],
    af_fid_to_league: dict[str, str],
    recovery_fixture_ids: frozenset[int] | None,
    date: str,
) -> list[int] | None:
    """Apply the optional recovery-mode allowlist filter to ``fixture_ids`` before
    the per-fixture entity loop.

    MVP-vs-all-leagues scoping is no longer done HERE, unconditionally, for every
    per-fixture entity: ``SPORTS_ENTITY_LEAGUE_COVERAGE`` now varies by entity
    (FIXTURE_STATS/FIXTURE_LINEUPS cover all leagues; FIXTURE_EVENTS/PLAYER_STATS
    stay MVP-restricted — operator ruling 2026-07-28), so a single shared
    pre-filter here would incorrectly starve the all-leagues entities. The
    per-entity scope check now runs inside ``_gather_per_fixture_rows``'s
    task-queueing loop instead (see its ``get_entity_league_coverage`` check),
    where each entity's own coverage set is consulted independently.

    Recovery-mode fixture-id allowlist filter — lifts the per-fixture work from
    O(all_fixtures_on_day x 5 entities) to O(allowlist_intersection_with_day
    x N_requested_entities). Used for targeted recovery (e.g. Phase 2's
    truth-set audit produced a 39k fixture-id list; we feed it here so we don't
    re-burn ~560k api_football calls re-fetching already-captured fixtures'
    per-fixture entities).

    Returns ``None`` when the recovery allowlist intersects the fixture set to
    zero — the caller must return early (skipping the per-fixture loop AND the
    cross-provider mapping writes) so we don't write phantom empty manifest
    rows for entities never attempted on this date.
    """
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
