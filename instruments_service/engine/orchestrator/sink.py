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

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_coerce_adapter_output",
    "_gated_sink_write",
]


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
