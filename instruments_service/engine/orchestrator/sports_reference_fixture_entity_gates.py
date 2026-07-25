"""Writer-side de-dup + schema-conformance gates for per-fixture entity writes.

Cohesion module of the ``engine.orchestrator`` package. Carries the
``player_stats`` writer-side de-dup gate and the ``fixture_events``
writer-side schema-conformance gate used by
``sports_reference_fixtures._prepare_fixture_entity_df`` /
``_write_fixture_entity_per_league`` (split out to keep that module under the
900-line file-size ratchet; plan:
``unified-trading-pm/plans/active/sports_satellite_ao_dispatch_batch2_2026_07_24.md``).

Context: issues/canonical_player_stats_fixture_events_quality_2026_07_16.md
found two pre-existing canonical data-correctness defects — player_stats
within-object exact-duplicate rows on ``(fixture_id, player_id)`` (~26% of
rows, remediated once already by ``dedup_canonical_player_stats_2026_07_25.py
--apply``), and fixture_events schema heterogeneity (~30% of a sampled
population carried a degenerate 5-col stub instead of the canonical 13-col
shape). These two gates stop both defects from re-accruing on every future
fetch.

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

from unified_api_contracts import lookup_contract

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_dedupe_player_stats_df",
    "_normalize_fixture_events_schema",
]

# Writer-side de-dup key for player_stats (mirrors the canonical remediation
# script's own methodology: dedup_canonical_player_stats_2026_07_25.py
# ``_KEY_COLS``) — kept as a single SSOT constant so the writer gate and any
# future remediation script never drift on the key definition.
_PLAYER_STATS_DEDUP_KEY_COLS = ("fixture_id", "player_id")


def _dedupe_player_stats_df(df: _orch.pd.DataFrame) -> _orch.pd.DataFrame:
    """Writer-side de-dup gate for player_stats — drop within-object exact
    duplicates on ``(fixture_id, player_id)`` before write.

    Fixes the re-accrual path for the canonical duplicate-row defect (740,725
    within-object duplicate rows / ~26%, resolved once already by
    ``dedup_canonical_player_stats_2026_07_25.py`` --apply; this gate stops it
    from coming back). Mirrors that script's own methodology exactly (same key
    columns, ``keep="first"``). No-op when either key column is absent — e.g.
    the nested ``[team, players, fixture_id, available_at]`` player_stats
    schema variant, which is out of this gate's scope (same doc, follow-up
    item). Source: issues/canonical_player_stats_fixture_events_quality_2026_07_16.md.
    """
    if not all(c in df.columns for c in _PLAYER_STATS_DEDUP_KEY_COLS):
        return df
    dup_mask = df.duplicated(subset=list(_PLAYER_STATS_DEDUP_KEY_COLS), keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count == 0:
        return df
    _orch.logger.info(
        "player_stats writer-side de-dup gate: dropping %d duplicate %s rows before write",
        dup_count,
        _PLAYER_STATS_DEDUP_KEY_COLS,
    )
    return df[~dup_mask].reset_index(drop=True)


def _normalize_fixture_events_schema(df: _orch.pd.DataFrame) -> _orch.pd.DataFrame:
    """Writer-side schema-conformance gate for fixture_events — reindex to the
    canonical UAC 13-col ``SPORTS_FIXTURE_EVENTS`` contract right before write.

    Fixes the re-accrual path for the canonical schema-heterogeneity defect
    (~30% of a sampled canonical population carried a degenerate 5-col stub or
    another non-canonical shape — 9-col named, 10-col ``af_``-prefixed). Any
    canonical column absent from the fetched/normalised row (a degenerate
    response) is added as null rather than fabricated; any column NOT in the
    canonical contract (a non-canonical shape like the 5-col stub's
    ``type``/``detail`` naming) is dropped rather than written through. Always
    normalizing (never skipping the write) avoids mis-classifying a genuine
    capture as an honest-absence empty (the CF-11 class of bug this same
    package already guards against for API-level failures — see
    ``sports_reference_fixtures._handle_empty_fixture_entity``).

    MUST run on the already per-league-split, post-recovery-merge frame (in
    ``sports_reference_fixtures._write_fixture_entity_per_league``, right
    before ``_gated_sink_write``) — NOT on the raw entity-level frame. The
    league-mapping join upstream can key off either ``fixture_id`` or
    ``af_fixture_id`` (``_fid_col``); reindexing to the canonical column set
    (``fixture_id`` only) BEFORE that join would silently drop the join key
    and break per-league capture. ``available_at`` is included in the reindex
    so it round-trips whether called before or after the PIT-approximation
    stamp. Source: issues/canonical_player_stats_fixture_events_quality_2026_07_16.md
    (Finding 2).
    """
    contract = lookup_contract(asset_group="sports", instrument_type="match", data_type="fixture_events")
    canonical_cols = [col.name for col in contract.columns]
    missing = [c for c in canonical_cols if c not in df.columns]
    extra = [c for c in df.columns if c not in canonical_cols]
    if missing or extra:
        _orch.logger.warning(
            "fixture_events writer-side schema gate: normalizing non-canonical shape to the "
            "UAC 13-col contract (missing_added=%s, non_canonical_dropped=%s)",
            missing,
            extra,
        )
    return df.reindex(columns=canonical_cols)
