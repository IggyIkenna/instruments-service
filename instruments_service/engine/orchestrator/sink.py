"""Gated sink writes: write-gate enforcement and adapter output coercion.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

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

from unified_api_contracts import _SPORTS_ENTITY_TO_PIPELINE_MODE

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_assert_not_cross_domain_contamination",
    "_coerce_adapter_output",
    "_gated_sink_write",
]

#: Instrument-catalogue/registry columns that must NEVER appear in a
#: sports_reference write. Their presence means the caller passed the WRONG
#: DataFrame — an instrument-catalogue/registry snapshot instead of sports
#: data — exactly the day=2026-04-14 incident
#: (``sports_fixtures_schedule_wrong_schema_day_2026_04_14.md``): 85
#: ``entity=fixtures_schedule`` shards silently carried an instrument-catalogue
#: shape (``instrument_key``, ``tick_size``, ``base_asset``, ...) instead of
#: fixtures data, undetected until a downstream column-projection read failed
#: with a schema-mismatch error. A handful of columns that are structurally
#: exclusive to the catalogue/registry shape (never legitimate on ANY sports
#: entity) is deliberately used instead of an allowlist of every sports
#: entity's expected columns — narrower, and immune to false positives as
#: sports entities evolve.
_INSTRUMENT_CATALOGUE_SENTINEL_COLUMNS = frozenset(
    {
        "instrument_key",
        "tick_size",
        "min_size",
        "contract_size",
        "base_asset",
        "quote_asset",
    }
)


def _assert_not_cross_domain_contamination(data: _orch.pd.DataFrame, entity: str | None) -> None:
    """Fail loud if a genuine sports_reference entity write carries instrument-catalogue columns.

    Scoped to ``entity in _SPORTS_ENTITY_TO_PIPELINE_MODE`` (the UAC canonical
    registry of sports_reference entity names) — ``_gated_sink_write`` is a
    SHARED choke point used by every instruments-service domain, including the
    legitimate CeFi/TradFi/DeFi instrument-catalogue writers, whose rows
    naturally carry these exact columns. Without this scope the guard would
    reject every real instrument-catalogue write (caught by
    ``quality-gates.sh`` TESTS: ``test_orchestrator_process.py``,
    ``test_orchestrator_futures_contracts.py``, ``test_new_orchestrator.py``
    all failed on the first, unscoped version of this guard).

    Called at the top of :func:`_gated_sink_write` — every sports_reference
    entity write funnels through this one choke point, so this is the single
    place a cross-domain mix-up can be caught before it silently lands in GCS
    under the wrong schema.
    """
    if entity not in _SPORTS_ENTITY_TO_PIPELINE_MODE:
        return
    contaminated = _INSTRUMENT_CATALOGUE_SENTINEL_COLUMNS.intersection(data.columns)
    if contaminated:
        raise ValueError(
            f"refusing sports_reference write for entity={entity!r}: DataFrame carries "
            f"instrument-catalogue column(s) {sorted(contaminated)} — this looks like the wrong "
            "DataFrame was passed (an instrument-catalogue/registry snapshot, not sports data). "
            "See plans/active/issues/sports_fixtures_schedule_wrong_schema_day_2026_04_14.md."
        )


def _coerce_adapter_output(item: object) -> dict[str, object]:
    # UAC sports normalizers return dict[str, object]; some adapter return-type
    # annotations still claim list[CanonicalX] (Pydantic). Coerce defensively so
    # either shape works — prior assumption of Pydantic-only blew up INJURIES
    # backfill 2026-04-21 with AttributeError on every date.
    if isinstance(item, dict):
        return dict(item)
    dump = getattr(item, "model_dump", None)
    if callable(dump):
        return dump()
    return {}


def _gated_sink_write(
    sink: _orch.DataSink,
    *,
    data: _orch.pd.DataFrame,
    partition: dict[str, str],
    filename: str,
    venue: str | None = None,
    entity: str | None = None,
    format: str = "parquet",
) -> None:
    """Per-date sink write wrapped by ``InstrumentsWriteGate``.

    Callers should invoke this in place of ``sink.write(...)`` for any write
    whose partition carries ``day={D}`` so row-level timestamp misalignment
    fails loud instead of landing silently in GCS.

    In warn mode (current default) violations emit ``DATA_ALIGNMENT_VIOLATION``
    and the write still proceeds. In strict mode, ``TimestampAlignmentError``
    propagates and the caller's per-shard failure-isolation block records the
    shard as ``attempted_failed`` on the manifest.
    """
    _assert_not_cross_domain_contamination(data, entity)
    _orch.assert_available_at_present(data)
    _orch._WRITE_GATE.validate_and_write(
        sink=sink,
        data=data,
        partition=partition,
        format=format,
        filename=filename,
        venue=venue,
        entity=entity,
    )
