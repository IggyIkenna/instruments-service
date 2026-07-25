"""Batched per-entity pre-fetch-skip read for the per-fixture enrichment loop.

Cohesion module of the ``engine.orchestrator`` package. Carries the
already-captured ``af_fixture_id`` lookup used by
``sports_reference_fixtures._read_captured_per_entity_league`` (split out to
keep that module under the 900-line file-size ratchet; plan:
``unified-trading-pm/plans/active/sports_satellite_ao_dispatch_batch2_2026_07_24.md``,
``sports_dependency_check_manifest_vs_gcs_path_2026_07_08.md``'s
``sports_fixtures.py:356`` todo).

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
    "_captured_fixture_ids_by_league",
    "_read_captured_league_fixture_ids_for_entity",
]


def _captured_fixture_ids_by_league(df: _orch.pd.DataFrame) -> dict[str, frozenset[int]]:
    """Group a ``_read_per_league_entity_df`` frame's rows into
    ``{league_id: frozenset(af_fixture_id)}``. Mirrors the retired
    ``_read_existing_per_league_fixture_ids``'s column-fallback (prefers
    ``af_fixture_id``, falls back to ``fixture_id``)."""
    if "league_id" not in df.columns:
        return {}
    fid_col = "af_fixture_id" if "af_fixture_id" in df.columns else "fixture_id"
    if fid_col not in df.columns:
        return {}
    out: dict[str, frozenset[int]] = {}
    for league_id, group in df.groupby("league_id"):
        fids = _orch.pd.to_numeric(group[fid_col], errors="coerce").dropna().astype(int)
        out[str(league_id)] = frozenset(int(x) for x in fids.tolist())
    return out


def _read_captured_league_fixture_ids_for_entity(bucket: str, date: str, entity_name: str) -> dict[str, frozenset[int]]:
    """Batched pre-fetch-skip read for ONE entity: every league's already-captured
    ``af_fixture_id`` set for ``date``, from a SINGLE ``list_blobs`` + per-blob
    download pass (``_read_per_league_entity_df``) instead of one
    ``blob.exists()`` probe per league.

    Returns ``{}`` on any read failure (transport error, no blobs found) — the
    caller treats that as "no captured fixtures known for this entity, fetch
    everything in scope," same fail-safe-empty contract the retired per-league
    helper had.
    """
    try:
        df = _orch._read_per_league_entity_df(bucket, date, entity_name, inject_league_id=True)
    except Exception as exc:
        _orch.logger.debug(
            "Pre-fetch skip batched read failed for entity=%s date=%s — proceeding without skip: %s",
            entity_name,
            date,
            exc,
        )
        return {}
    if df is None:
        return {}
    return _captured_fixture_ids_by_league(df)
