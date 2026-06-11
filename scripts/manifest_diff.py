#!/usr/bin/env python3
"""manifest_diff.py — projected-vs-current ``_index`` diff (CF-20 / V2, the goalpost delta).

The manifest-vs-manifest diff — distinct from the orphan sweep's GCS-vs-manifest walk
(``migration_orphan_sweep.py``) and the beta writer's projection
(``beta_manifest_writer.py``). It loads TWO ``_index`` parquets:

* ``--projected`` — the ``beta_manifest_writer`` PROJECTED v9 ``_index`` (what a
  migrator/rebuild ``--dry-run`` says the post-migration manifest WOULD be);
* ``--current``  — the CURRENT/live consolidated ``_index``;

and diffs them by shard-key so the operator SEES the goalposts as a delta BEFORE
G4 ``--apply``:

1. **Cell diff** — added / removed / changed / unchanged cells, where a cell is
   ``(date, data_type, venue, chain, instrument_type)``;
2. **capture_status transition matrix** — per ``old→new`` counts for matched cells
   (e.g. how many ``expected_unattempted→captured`` the migration would realise);
3. **Per-(asset_group, data_type, venue) row deltas** — raw manifest-row counts per
   group on each side + the delta.

**Grain-aware wildcard matching** (the prediction ``A=0`` lesson from the orphan
sweep's ``build_covered_index``/``is_covered``): manifest rows can be keyed COARSER
than their counterpart — blank ``chain``/``instrument_type`` (and sometimes blank
``venue``) mean "any". A fine projected cell ``(POLYMARKET, POLYGON, "")`` matched by
a coarse current row ``(POLYMARKET, "", "")`` is a MATCH (a grain refinement), never a
false add+remove pair. Matching is bidirectional: blank fields wildcard on EITHER side.

**Multi-source collapse**: the v9 ``_index`` carries per-source rows; rows collapsing
to the same cell resolve by the multi-source union rule (≥1 ``captured`` → cell
``captured``; then ``empty_confirmed`` > ``attempted_failed`` > ``expected_unattempted``).

Key alignment reuses the CF-15 possible-manifest registry
(``unified_api_contracts.possible_manifest``): the pattern axes are exactly the
:class:`ShardKey` shard axes — a module-level guard loud-fails if UAC renames one.
``canonical_path_templates`` is not consulted because this tool diffs ``_index`` ROWS
(columns already carry the axes), never GCS object paths.

**READ-ONLY**: never writes GCS. ``--out`` writes a machine-readable JSON to a LOCAL
path only. Exit code 1 = the projected manifest REGRESSES the current one (removed
cells, or any ``captured→*`` downgrade) — the G3.5 pre-apply gate signal; exit 0
otherwise (pure additions/upgrades are the expected migration shape).

Acceptance role: V5/CF-20 per-AG projected-preview verdict
(``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V2 item
"Manifest-diff tool (projected-vs-current)").

Usage::

    cd instruments-service
    .venv/bin/python scripts/manifest_diff.py --asset-group prediction \\
        --projected gs://<dev>/_index/audit/projected_prediction.parquet \\
        --current gs://<bucket>/_index/availability_index.parquet \\
        --out /tmp/manifest_diff_prediction.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from unified_api_contracts import ShardKey

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Key alignment with the CF-15 possible-manifest registry (loud-fail guard):
# the cell identity is (date x data_type) x the ShardKey pattern axes below.
# Date is crossed in separately (ShardKey is deliberately lifecycle/date-free);
# instrument_id stays out (the _index cell grain — per-instrument leaves belong
# to the enumerator, not the consolidated availability cell).
# ---------------------------------------------------------------------------
_PATTERN_AXES: tuple[str, str, str] = ("venue", "chain", "instrument_type")
for _axis in (*_PATTERN_AXES, "data_type", "asset_group"):
    if _axis not in ShardKey._fields:
        raise RuntimeError(
            f"manifest_diff key axis {_axis!r} is no longer a ShardKey field "
            f"({ShardKey._fields}) — realign with unified_api_contracts.possible_manifest"
        )

# A (VENUE, CHAIN, instrument_type) pattern; blank fields are wildcards ("any").
CellPattern = tuple[str, str, str]
# (date, data_type) bucket → pattern → resolved capture_status. Mirrors the orphan
# sweep's CoveredIndex, with the captured-only filter replaced by a status payload.
CellIndex = dict[tuple[str, str], dict[CellPattern, str]]

# Multi-source collapse priority (≥1 captured → cell captured; the 4-state union).
# Unknown/blank statuses rank LAST so an honest legacy blank never masks a real state.
_STATUS_PRIORITY: tuple[str, ...] = ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")

VALID_ASSET_GROUPS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")

# Cap on per-class cell samples carried into the JSON/report (counts are always full).
SAMPLE_CAP: int = 100


def _norm_v(value: object) -> str:
    return str(value or "").upper()


def _norm_it(value: object) -> str:
    return str(value or "").lower()


def _norm_date(value: object) -> str:
    """``2024-06-01 00:00:00`` / ``Timestamp(...)`` / ``2024-06-01`` → ``2024-06-01``."""
    s = str(value or "")
    return s[:10]


def _status_rank(status: str) -> int:
    try:
        return _STATUS_PRIORITY.index(status)
    except ValueError:
        return len(_STATUS_PRIORITY)


# ---------------------------------------------------------------------------
# Loading (local path or gs:// URI; read-only)
# ---------------------------------------------------------------------------


def load_index(path_or_uri: str) -> pd.DataFrame:
    """Load an ``_index`` parquet from a local path or a ``gs://`` URI (read-only).

    Uses the same UTL storage-client read the sibling scaffolds use (never a
    subprocess ``gsutil``)."""
    if path_or_uri.startswith("gs://"):
        import io

        from unified_trading_library import get_storage_client

        bucket, _sep, blob_path = path_or_uri[len("gs://") :].partition("/")
        raw = get_storage_client().download_bytes(bucket, blob_path)  # type: ignore[attr-defined]
        return pd.read_parquet(io.BytesIO(raw))
    return pd.read_parquet(path_or_uri)


def rows_for_asset_group(df: pd.DataFrame, asset_group: str) -> list[dict[str, object]]:
    """The manifest rows scoped to ``asset_group``.

    A row belongs when its ``asset_group`` column equals the AG **or is blank**
    (legacy pre-v9 rows in a single-AG bucket carry no asset_group — dropping them
    would under-count the current side of every diff). A missing column keeps all
    rows (a pure-legacy v8 ``_index``)."""
    rows: list[dict[str, object]] = df.to_dict("records")  # type: ignore[assignment]
    if "asset_group" not in df.columns:
        return rows
    ag = asset_group.lower()
    return [r for r in rows if _norm_it(r.get("asset_group", "")) in ("", ag)]


# ---------------------------------------------------------------------------
# Cell index + grain-aware lookup (pure; unit-tested without GCS)
# ---------------------------------------------------------------------------


def build_cell_index(rows: list[dict[str, object]]) -> CellIndex:
    """Build the (date, data_type) → pattern → capture_status index from manifest rows.

    Blank pattern fields are preserved as wildcards (the manifest's coarser grain).
    Rows collapsing to the same cell (per-source v9 rows) resolve by
    ``_STATUS_PRIORITY`` — the multi-source union (≥1 captured → captured)."""
    index: CellIndex = defaultdict(dict)
    for r in rows:
        day = _norm_date(r.get("date", ""))
        dt = _norm_it(r.get("data_type", ""))
        if not day or not dt:
            continue
        pattern: CellPattern = (
            _norm_v(r.get("venue", "")),
            _norm_v(r.get("chain", "")),
            _norm_it(r.get("instrument_type", "")),
        )
        status = _norm_it(r.get("capture_status", ""))
        bucket = index[(day, dt)]
        existing = bucket.get(pattern)
        if existing is None or _status_rank(status) < _status_rank(existing):
            bucket[pattern] = status
    return index


def lookup_status(index: CellIndex, day: str, data_type: str, pattern: CellPattern) -> str | None:
    """Grain-aware status lookup: the resolved capture_status of the cell COVERING
    ``pattern`` on ``(day, data_type)``, or ``None`` when no row covers it.

    Fast path = the orphan sweep's fixed 8-way blank-combination lookup (covers an
    exact match + a COARSER index row whose blank fields wildcard the query's). When
    the QUERY itself carries blank fields (a coarse row probed against a finer index)
    the fast path can miss a finer covering row → fall back to a bucket scan with the
    generalized per-axis match (q == p, or either side blank). Buckets are per
    (date, data_type) — small — so the scan stays cheap and only runs for coarse
    queries."""
    bucket = index.get((day, data_type))
    if not bucket:
        return None
    v, c, it = pattern
    for mv in (v, ""):
        for mc in (c, ""):
            for mit in (it, ""):
                status = bucket.get((mv, mc, mit))
                if status is not None:
                    return status
    if "" in pattern:
        # No single finest match exists for a coarse query — resolve the covering
        # rows' statuses by the same priority union as the multi-source collapse
        # (≥1 captured → captured), keeping the lookup deterministic.
        covering = [
            status
            for (pv, pc, pit), status in bucket.items()
            if (v in ("", pv) or pv == "") and (c in ("", pc) or pc == "") and (it in ("", pit) or pit == "")
        ]
        if covering:
            return min(covering, key=_status_rank)
    return None


# ---------------------------------------------------------------------------
# The diff
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CellRef:
    """One diffed cell, JSON-serialisable."""

    date: str
    data_type: str
    venue: str
    chain: str
    instrument_type: str

    def as_dict(self) -> dict[str, str]:
        return {
            "date": self.date,
            "data_type": self.data_type,
            "venue": self.venue,
            "chain": self.chain,
            "instrument_type": self.instrument_type,
        }


@dataclass
class ManifestDiff:
    """The projected-vs-current cell delta."""

    added: list[CellRef] = field(default_factory=list)
    removed: list[CellRef] = field(default_factory=list)
    changed: list[tuple[CellRef, str, str]] = field(default_factory=list)  # (cell, old, new)
    unchanged: int = 0
    transitions: Counter[str] = field(default_factory=Counter)  # "old->new" → count

    @property
    def captured_regressions(self) -> int:
        """Matched cells the projection DOWNGRADES from ``captured`` — never expected
        from a migration; a non-zero count blocks G4 alongside removed cells."""
        return sum(n for key, n in self.transitions.items() if key.startswith("captured->"))

    @property
    def is_regression(self) -> bool:
        return bool(self.removed) or self.captured_regressions > 0


def diff_cell_indexes(projected: CellIndex, current: CellIndex) -> ManifestDiff:
    """Diff two cell indexes (grain-aware both ways).

    * projected cell with NO covering current cell → **added**;
    * current cell with NO covering projected cell → **removed** (a migration must
      never drop coverage — this is the gate's red flag);
    * covered both ways with a different resolved status → **changed** (+ the
      ``old->new`` transition matrix entry);
    * same status → unchanged."""
    diff = ManifestDiff()
    for (day, dt), patterns in sorted(projected.items()):
        for pattern, new_status in sorted(patterns.items()):
            old_status = lookup_status(current, day, dt, pattern)
            cell = CellRef(day, dt, *pattern)
            if old_status is None:
                diff.added.append(cell)
            elif old_status != new_status:
                diff.changed.append((cell, old_status, new_status))
                diff.transitions[f"{old_status}->{new_status}"] += 1
            else:
                diff.unchanged += 1
    for (day, dt), patterns in sorted(current.items()):
        for pattern in sorted(patterns):
            if lookup_status(projected, day, dt, pattern) is None:
                diff.removed.append(CellRef(day, dt, *pattern))
    return diff


def group_row_deltas(
    current_rows: list[dict[str, object]],
    projected_rows: list[dict[str, object]],
    asset_group: str,
) -> list[dict[str, object]]:
    """Raw manifest-ROW counts per (asset_group, data_type, venue) on each side + the
    delta (pre-collapse: per-source rows count individually — this is the row-volume
    view, complementing the cell view). Sorted by |delta| descending."""

    def _counts(rows: list[dict[str, object]]) -> Counter[tuple[str, str, str]]:
        counts: Counter[tuple[str, str, str]] = Counter()
        for r in rows:
            ag = _norm_it(r.get("asset_group", "")) or asset_group.lower()
            counts[(ag, _norm_it(r.get("data_type", "")), _norm_v(r.get("venue", "")))] += 1
        return counts

    cur, proj = _counts(current_rows), _counts(projected_rows)
    deltas: list[dict[str, object]] = []
    for key in sorted(set(cur) | set(proj)):
        ag, dt, venue = key
        n_cur, n_proj = cur.get(key, 0), proj.get(key, 0)
        deltas.append(
            {
                "asset_group": ag,
                "data_type": dt,
                "venue": venue,
                "current_rows": n_cur,
                "projected_rows": n_proj,
                "delta": n_proj - n_cur,
            }
        )
    deltas.sort(key=lambda d: (-abs(int(str(d["delta"]))), str(d["asset_group"]), str(d["data_type"]), str(d["venue"])))
    return deltas


# ---------------------------------------------------------------------------
# Reporting (human summary + --out JSON; never writes GCS)
# ---------------------------------------------------------------------------


def build_report(
    asset_group: str,
    diff: ManifestDiff,
    deltas: list[dict[str, object]],
    *,
    projected_uri: str,
    current_uri: str,
    projected_rows: int,
    current_rows: int,
) -> dict[str, object]:
    """The machine-readable report ``--out`` writes (counts full; samples capped)."""
    return {
        "asset_group": asset_group,
        "projected": {"uri": projected_uri, "rows": projected_rows},
        "current": {"uri": current_uri, "rows": current_rows},
        "cells": {
            "added": len(diff.added),
            "removed": len(diff.removed),
            "changed": len(diff.changed),
            "unchanged": diff.unchanged,
        },
        "status_transitions": dict(sorted(diff.transitions.items())),
        "regressions": {
            "removed_cells": len(diff.removed),
            "captured_regressions": diff.captured_regressions,
            "is_regression": diff.is_regression,
        },
        "group_row_deltas": deltas,
        "sample_cap": SAMPLE_CAP,
        "samples": {
            "added": [c.as_dict() for c in diff.added[:SAMPLE_CAP]],
            "removed": [c.as_dict() for c in diff.removed[:SAMPLE_CAP]],
            "changed": [{**c.as_dict(), "old": old, "new": new} for c, old, new in diff.changed[:SAMPLE_CAP]],
        },
    }


def _print_report(asset_group: str, diff: ManifestDiff, deltas: list[dict[str, object]]) -> int:
    """Print the human summary; return the exit code (1 = regression — removed cells
    or a captured→* downgrade; the G3.5 pre-apply gate signal)."""
    logger.info("=== manifest diff (projected vs current): %s ===", asset_group)
    logger.info(
        "  cells: added=%d removed=%d changed=%d unchanged=%d",
        len(diff.added),
        len(diff.removed),
        len(diff.changed),
        diff.unchanged,
    )
    logger.info("--- capture_status transition matrix (old->new) ---")
    for key, n in sorted(diff.transitions.items()):
        logger.info("  %-50s %d", key, n)
    logger.info("--- per-(asset_group, data_type, venue) row deltas (top 20 by |delta|) ---")
    for d in deltas[:20]:
        logger.info(
            "  %-45s current=%d projected=%d delta=%+d",
            f"{d['asset_group']}/{d['data_type']}/{d['venue']}",
            int(str(d["current_rows"])),
            int(str(d["projected_rows"])),
            int(str(d["delta"])),
        )
    if diff.removed:
        logger.warning("REMOVED cells (projection drops current coverage — blocks G4); first %d:", SAMPLE_CAP)
        for cell in diff.removed[:SAMPLE_CAP]:
            logger.warning("  - %s", cell.as_dict())
    logger.info(
        "=== GATE: removed_cells=%d (target 0), captured_regressions=%d (target 0) → %s ===",
        len(diff.removed),
        diff.captured_regressions,
        "RED (regression)" if diff.is_regression else "GREEN",
    )
    return 1 if diff.is_regression else 0


def run_diff(asset_group: str, projected_uri: str, current_uri: str, out: str = "") -> int:
    """Load both indexes, diff, report; optionally write the JSON to a LOCAL ``out``."""
    projected_df = load_index(projected_uri)
    current_df = load_index(current_uri)
    projected_rows = rows_for_asset_group(projected_df, asset_group)
    current_rows = rows_for_asset_group(current_df, asset_group)
    logger.info(
        "loaded projected=%d rows (%s) current=%d rows (%s) for asset_group=%s",
        len(projected_rows),
        projected_uri,
        len(current_rows),
        current_uri,
        asset_group,
    )
    diff = diff_cell_indexes(build_cell_index(projected_rows), build_cell_index(current_rows))
    deltas = group_row_deltas(current_rows, projected_rows, asset_group)
    code = _print_report(asset_group, diff, deltas)
    if out:
        report = build_report(
            asset_group,
            diff,
            deltas,
            projected_uri=projected_uri,
            current_uri=current_uri,
            projected_rows=len(projected_rows),
            current_rows=len(current_rows),
        )
        Path(out).write_text(json.dumps(report, indent=2, sort_keys=True))
        logger.info("wrote machine-readable diff to %s", out)
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Projected-vs-current _index diff (CF-20 goalpost delta). Read-only — never writes GCS."
    )
    parser.add_argument("--asset-group", required=True, choices=list(VALID_ASSET_GROUPS))
    parser.add_argument(
        "--projected", required=True, help="projected v9 _index (beta_manifest_writer output) — local path or gs:// URI"
    )
    parser.add_argument("--current", required=True, help="current/live consolidated _index — local path or gs:// URI")
    parser.add_argument("--out", type=str, default="", help="LOCAL json path for the machine-readable diff")
    args = parser.parse_args(argv)
    return run_diff(args.asset_group, args.projected, args.current, out=args.out)


if __name__ == "__main__":
    sys.exit(main())
