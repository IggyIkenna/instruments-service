"""Adapter failure classification for manifest error reasons.

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
    "_classify_adapter_failure",
]


def _classify_adapter_failure(exc: Exception, venue: str) -> str:
    """Return a stable category string for ``record_failed`` from an exception.

    Honest-coverage manifest rows store a categorical failure code, not a
    raw exception message.  We try UAC ``classify_venue_error`` first using
    the exception's class name as the error code; if the venue/code pair is
    not known to UAC, we fall back to the exception class name itself so
    downstream dashboards can still group.

    Root-cause fix (api_football_injuries_error_misclassification_2026_07_13):
    some adapter exceptions carry a duck-typed ``error_key`` attribute — a
    real, UAC-matchable provenance code extracted from the raw API error
    envelope (e.g. ``ApiFootballResponseError.error_key`` = ``"plan"`` /
    ``"token"`` / ``"rateLimit"`` — see api_football.py's ``_raise_on_api_errors``)
    that classifies far better than the Python exception CLASS NAME (which
    never matches a venue's ``VENUE_ERRORS_*`` table — those are keyed by
    HTTP/domain codes, not class names). Prefer it when present + non-empty;
    fall back to the class name for every exception type that doesn't carry
    one (unaffected — this is a pure additive improvement, generic across
    every venue this function classifies for).
    """
    error_code = getattr(exc, "error_key", "") or type(exc).__name__
    classification = _orch.classify_venue_error(venue, error_code)
    if classification is not None:
        return classification.error_code
    return error_code
