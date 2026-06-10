#!/usr/bin/env python3
"""migration_schema_completeness.py — schema-attribute completeness audit (CF-18 / V3).

Closes the operator's deepest "no v10" fear: **a v10 forced because we silently dropped
an ATTRIBUTE on the migration.** The cell-completeness checks (catalogue ⊇ manifest)
prove the right *cells* moved; they do NOT prove every *column the raw data physically
carries* survives into the v9 canonical contract. A dropped attribute is invisible to a
row-count or a 4-state check — the bytes and the cells are all present, but one column
is gone. This audit is the missing column-completeness gate.

Per (asset_group, data_type, venue):
  1. sample N recent source/legacy parquets; union their **actual footer columns**;
  2. diff vs the v9 UAC canonical contract (``schema_spec.find_schema`` — the
     per-data_type ``SchemaSpec``);
  3. **any source column not represented in the canonical target = RED** → carry it
     (extend the canonical schema BEFORE apply) or operator-ack the drop. Zero silent
     truncation.

The PURE diff (``diff_schema``) is unit-tested without GCS; the footer sampling rides
the URIs the orphan sweep already enumerated (single-walk discipline — pass the sweep's
class-A/B/E object list rather than re-walking).

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V3/CF-18.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field

from unified_api_contracts.registry.schema_spec import find_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Hive-partition keys + writer bookkeeping columns that live in the PATH or are
# manifest/emission metadata — never part of the data_type's canonical column contract,
# so their absence from ``SchemaSpec`` is not a truncation. Excluded from the RED diff.
_PARTITION_AND_META_COLUMNS: frozenset[str] = frozenset(
    {
        "asset_group",
        "category",
        "venue",
        "chain",
        "instrument_type",
        "instrument_id",
        "data_type",
        "day",
        "date",
        "pipeline_mode",
        "source",
        "transport",
        "available_at",
        "schema_version",
        "league_id",
        "canonical_question_group",
        "feature_group",
        "feature_group_version",
        "timeframe",
    }
)


@dataclass(frozen=True)
class SchemaDiff:
    """The column-level completeness verdict for one (asset_group, data_type[, venue])."""

    asset_group: str
    data_type: str
    venue: str
    carried: frozenset[str]
    dropped: frozenset[str]  # source columns NOT in the canonical contract → RED
    extra_canonical: frozenset[str]  # contract columns absent from the sampled source
    canonical_known: bool  # False = no SchemaSpec for this (ag, data_type)

    @property
    def is_red(self) -> bool:
        """RED iff a source column would be silently truncated (and we have a contract
        to truncate against). Missing-contract is a SEPARATE finding (``canonical_known``
        False) — reported, but not a truncation."""
        return self.canonical_known and bool(self.dropped)


def canonical_columns_for(asset_group: str, data_type: str) -> frozenset[str] | None:
    """The v9 canonical column set for (asset_group, data_type), or ``None`` when no
    ``SchemaSpec`` is registered (a coverage gap — reported separately)."""
    spec = find_schema(asset_group, data_type)
    if spec is None:
        return None
    return frozenset(c.name for c in spec.columns)


def diff_schema(
    asset_group: str,
    data_type: str,
    venue: str,
    source_columns: set[str],
) -> SchemaDiff:
    """Pure column diff: sampled source footer columns vs the v9 canonical contract.

    Partition-key + writer-metadata columns are excluded from BOTH sides (they are path
    keys / manifest bookkeeping, not part of the data_type's column contract). A source
    column outside the canonical contract is a RED drop; a contract column missing from
    the sample is informational (a not-yet-populated optional)."""
    src = frozenset(c for c in source_columns if c not in _PARTITION_AND_META_COLUMNS)
    canonical = canonical_columns_for(asset_group, data_type)
    if canonical is None:
        return SchemaDiff(
            asset_group=asset_group,
            data_type=data_type,
            venue=venue,
            carried=frozenset(),
            dropped=frozenset(),
            extra_canonical=frozenset(),
            canonical_known=False,
        )
    canon = frozenset(c for c in canonical if c not in _PARTITION_AND_META_COLUMNS)
    return SchemaDiff(
        asset_group=asset_group,
        data_type=data_type,
        venue=venue,
        carried=src & canon,
        dropped=src - canon,
        extra_canonical=canon - src,
        canonical_known=True,
    )


@dataclass
class SchemaCompletenessReport:
    """Aggregated per-(ag, data_type, venue) verdicts."""

    diffs: list[SchemaDiff] = field(default_factory=list)

    @property
    def red(self) -> list[SchemaDiff]:
        return [d for d in self.diffs if d.is_red]

    @property
    def missing_contract(self) -> list[SchemaDiff]:
        return [d for d in self.diffs if not d.canonical_known]

    def add(self, diff: SchemaDiff) -> None:
        self.diffs.append(diff)

    def is_green(self) -> bool:
        return not self.red


def read_footer_columns(uri: str) -> set[str]:
    """Read ONLY the parquet footer schema of a GCS object (no row data).

    Uses pyarrow's metadata read so a multi-GB parquet costs one footer fetch. The GCS
    object is opened via the cloud-agnostic storage client."""
    import io

    import pyarrow.parquet as pq
    from unified_trading_library import get_storage_client

    assert uri.startswith("gs://")
    bucket, _sep, blob_path = uri[len("gs://") :].partition("/")
    raw = get_storage_client().download_bytes(bucket, blob_path)
    schema = pq.read_schema(io.BytesIO(raw))
    return set(schema.names)


@dataclass(frozen=True)
class SampleTarget:
    """A (ag, data_type, venue) cell + a representative GCS uri to footer-sample."""

    asset_group: str
    data_type: str
    venue: str
    uri: str


def sample_targets_from_objects(
    objects: list[tuple[str, str, str, str]],
    *,
    per_cell: int = 3,
) -> list[SampleTarget]:
    """Pick up to ``per_cell`` representative URIs per (ag, data_type, venue) from the
    orphan-sweep object list ``(asset_group, data_type, venue, uri)`` (single-walk reuse —
    do NOT re-list GCS). Deterministic: takes the lexicographically-last URIs (most
    recent day sorts last under the ``day=YYYY-MM-DD`` hive key)."""
    by_cell: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for ag, dt, venue, uri in objects:
        if dt:
            by_cell[(ag, dt, venue)].append(uri)
    targets: list[SampleTarget] = []
    for (ag, dt, venue), uris in by_cell.items():
        for uri in sorted(uris)[-per_cell:]:
            targets.append(SampleTarget(asset_group=ag, data_type=dt, venue=venue, uri=uri))
    return targets


def run_completeness(targets: list[SampleTarget]) -> SchemaCompletenessReport:
    """Footer-sample each target, union columns per cell, diff vs the canonical contract."""
    cols_by_cell: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for t in targets:
        try:
            cols_by_cell[(t.asset_group, t.data_type, t.venue)] |= read_footer_columns(t.uri)
        except Exception as exc:
            logger.warning("footer read failed for %s: %s", t.uri, exc)
    report = SchemaCompletenessReport()
    for (ag, dt, venue), cols in sorted(cols_by_cell.items()):
        report.add(diff_schema(ag, dt, venue, cols))
    return report


def _print_report(report: SchemaCompletenessReport) -> int:
    logger.info("=== schema-attribute completeness (CF-18) ===")
    for d in report.red:
        logger.warning(
            "RED %s/%s/%s — DROPPED columns (silent truncation): %s",
            d.asset_group,
            d.data_type,
            d.venue,
            sorted(d.dropped),
        )
    for d in report.missing_contract:
        logger.info("no SchemaSpec for %s/%s (coverage gap — add a SchemaSpec)", d.asset_group, d.data_type)
    n_red = len(report.red)
    logger.info("=== ACCEPTANCE: %d RED cells (target 0); %d cells audited ===", n_red, len(report.diffs))
    return 0 if report.is_green() else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Schema-attribute completeness audit (CF-18).")
    parser.add_argument("--asset-group", required=True, choices=["cefi", "defi", "tradfi", "sports", "prediction"])
    parser.add_argument(
        "--objects-parquet",
        type=str,
        default="",
        help="gs:// orphan_sweep parquet (single-walk reuse) providing (ag,data_type,venue,uri) rows",
    )
    parser.add_argument("--per-cell", type=int, default=3)
    args = parser.parse_args(argv)

    if not args.objects_parquet:
        logger.error("--objects-parquet is required (reuse the orphan-sweep output; do NOT re-walk GCS)")
        return 2
    objects = _load_objects(args.objects_parquet, args.asset_group)
    targets = sample_targets_from_objects(objects, per_cell=args.per_cell)
    report = run_completeness(targets)
    return _print_report(report)


def _load_objects(objects_parquet: str, asset_group: str) -> list[tuple[str, str, str, str]]:
    import io

    import pandas as pd
    from unified_trading_library import get_storage_client

    assert objects_parquet.startswith("gs://")
    bucket, _sep, blob_path = objects_parquet[len("gs://") :].partition("/")
    raw = get_storage_client().download_bytes(bucket, blob_path)
    df = pd.read_parquet(io.BytesIO(raw))
    return [
        (asset_group, str(r["data_type"]), str(r["venue"]), str(r["uri"]))
        for r in df.to_dict("records")
        if r.get("data_type")
    ]


if __name__ == "__main__":
    sys.exit(main())
