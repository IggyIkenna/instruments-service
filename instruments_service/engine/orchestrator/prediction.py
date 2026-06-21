"""Prediction shard helpers: canonical question-group extraction and shard counting.

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
    "_compute_prediction_shards",
    "_extract_prediction_canonical_group",
]


def _extract_prediction_canonical_group(row: _orch.pd.Series) -> str:
    """Map a PREDICTION instrument row onto a canonical-question-group name.

    Per the
    ``predictions_master.plan.md`` Phase 1 critical-path
    todo: replace the legacy per-base_asset shard with the UAC
    canonical-question-group SSOT (``BTC_UP_DOWN_HOURLY``,
    ``BTC_UP_DOWN_DAILY``, ``SPX_UP_DOWN_DAILY``,
    ``ELECTION_PRESIDENT_2028``, etc.). Recurring market_ids cycle
    through canonical groups over time — HOURLY = ~24/day, DAILY = 1/day,
    macro-event = 1 over months. Bundling by canonical group is the
    options-chain-equivalent shape per CLAUDE.md "Per-asset-group
    shard-key matrix → Prediction".

    Polymarket: classifies via ``classify_polymarket_to_canonical_group``
    (override-first per ``POLYMARKET_CONDITION_ID_TO_GROUP``, fallback to
    rule-based slug-prefix path). Title + event_slug context isn't
    plumbed through the writer DataFrame today — the slug-prefix rule
    handles ~95% of real Polymarket slugs (``bitcoin-up-or-down-hour-*``,
    ``oscars-best-picture-2026``, etc.) on ``raw_symbol`` alone, and the
    ``OTHER`` catch-all bucket per UAC :class:`CanonicalQuestionGroup`
    captures the remainder so the data-status drilldown stays honest
    (per the predictions_master plan's C.12 "synthetic OTHER bucket"
    todo).

    Kalshi: classifies via ``classify_kalshi_to_canonical_group(ticker=...)``
    using a three-tier lookup — (1) exact override dict
    (:data:`unified_api_contracts.predictions.KALSHI_TICKER_TO_GROUP`),
    (2) rule-based prefix table
    (:data:`unified_api_contracts.predictions.KALSHI_TICKER_PREFIX_TO_GROUP`)
    mapping ``KXBTCD-*`` / ``KXINX-*`` / ``KXFED-*`` / ``KXCPI-*`` etc.
    to their canonical groups (the same
    :class:`~unified_api_contracts.predictions.CanonicalQuestionGroup`
    values as the equivalent Polymarket market, enabling cross-venue arb),
    (3) ``OTHER`` catch-all.  Unrecognised tickers route to ``OTHER``.

    Other venues route to ``OTHER`` defensively — should never trigger
    in practice because the writer only invokes this on rows with
    ``venue ∈ {POLYMARKET, kalshi}``.

    Returns the canonical-question-group string value (the
    ``CanonicalQuestionGroup`` enum's ``.value`` — used as the
    ``underlying`` slot on the manifest row + as the partition key on
    the GCS path).
    """
    venue_raw = str(row.get("venue", "")).strip()
    venue_upper = venue_raw.upper()
    if venue_upper == "POLYMARKET":
        condition_id = str(row.get("instrument_key", "") or "")
        slug = str(row.get("raw_symbol", "") or "")
        group = _orch.classify_polymarket_to_canonical_group(
            title="",
            slug=slug,
            event_slug="",
            outcome="",
            condition_id=condition_id,
        )
        return (group or _orch.CanonicalQuestionGroup.OTHER).value
    # Kalshi adapter ships venue as lowercase ``"kalshi"`` per
    # ``KalshiReferenceDataAdapter.venue``.
    if venue_upper == "KALSHI":
        ticker = str(row.get("instrument_key", "") or "")
        group = _orch.classify_kalshi_to_canonical_group(ticker=ticker)
        return (group or _orch.CanonicalQuestionGroup.OTHER).value
    return _orch.CanonicalQuestionGroup.OTHER.value


def _compute_prediction_shards(df: _orch.pd.DataFrame) -> dict[str, int]:
    """Group PREDICTION instruments by canonical-question-group, return
    ``{venue/group: count}``.

    Mirrors the per-row classifier output of
    :func:`_extract_prediction_canonical_group` — shard counts are keyed
    on the same string the manifest emits as ``underlying`` so the
    data-status drilldown sums up cleanly across the writer atomicity
    boundary + the manifest row key (per CLAUDE.md "Shard-granularity
    SSOT" requirement that the same atom appears at every layer).
    """
    shard_counts: dict[str, int] = {}
    for _, row in df.iterrows():
        group = _orch._extract_prediction_canonical_group(row)
        venue_raw = str(row.get("venue", "")).strip().upper()
        key = f"{venue_raw}/{group}"
        shard_counts[key] = shard_counts.get(key, 0) + 1
    return shard_counts
