#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off manifest correction for the KRAKEN-FUTURES row_count over-count
#   found in defi_orphan_sweep_test_artifact_prod_leak_2026_07_24.md todo 4.
# Delete-when: after the --apply run's post-write spot-check confirms all 4 cells
#   read `captured` with the canonical-only row_count, and the doc's todo 4 is flipped.
"""Correct/record the true canonical-only row_count for 4 cefi KRAKEN-FUTURES cells.

BACKGROUND (``defi_orphan_sweep_test_artifact_prod_leak_2026_07_24.md`` todo 4).
``backfill_orphan_class_e.py``'s ``record_cells()`` sums ``row_count`` across every
class-E result sharing a cell key with NO filter excluding stale
``_remediation_backups/kraken_futures_collision_2026_07_08/`` copies (server-side
backups made before the 2026-07-08 in-place column fix). cefi's
``backfill-orphan-e-cefi-20260722-213220`` apply run (landed 2026-07-22 23:12 UTC,
before ``split_unknown_prefix_rows``/any backup-aware filter existed) therefore
summed each canonical KRAKEN-FUTURES object's row count together with its stale
backup twin's row count for 4 cells: (2024-02-01, book_snapshot_5), (2024-02-01,
trades), (2025-01-10, book_snapshot_5), (2025-01-10, trades).

LIVE-MANIFEST VERIFICATION (2026-08-02, checked interactively via
``read_availability_index`` before writing this script — not re-checked at
runtime, since the correction itself is idempotent regardless of that prior
state). The live ``_index/availability_index.parquet`` was found to carry **zero**
rows from
``service_name="instruments-service"`` for ``venue=KRAKEN-FUTURES`` on either
affected date, at any ``capture_status`` — i.e. the inflated ``captured`` row the
issue doc describes is NOT the row present today (its per-VM shard, which would
have lived at ``_index/per_vm/orphan-backfill-cefi*.parquet``, no longer exists —
already merged-and-dropped or never durably flushed). The only physical rows at
this shard key are 72 unrelated ``attempted_failed``/count=0
``service_name="market-tick-data-service"`` rows from a pre-existing, separate
per-underlying bookkeeping gap (predates this fix; different ``service_name`` so
the manifest consolidator's dedup — keyed on
``(date, venue, data_type, service_name, ...)`` — never partitions them against an
instruments-service row anyway). Net effect: whether the prior inflated row still
exists or not, the correct end state is identical — a single ``captured`` row per
cell, from ``service_name="instruments-service"``, carrying the TRUE canonical-only
row_count. This script produces exactly that row (mirrors
``backfill_orphan_class_e.py::record_cells()``'s per-cell ``record_captured`` shape,
minus the backup-inclusion bug), rather than attempting to in-place-patch a row that
may not currently exist.

TRUE ROW COUNT — re-derived FRESH from GCS on every run (never trusts the stale
``_index/audit/orphan_backfill_cefi.parquet`` report), by listing the CANONICAL
prefix only (``.../venue=KRAKEN-FUTURES/instrument_type=future/data_type=<dt>/`` —
structurally distinct from and excludes ``_remediation_backups/...`` objects, which
live under a completely different top-level prefix) and footer-summing every object
found there. Bounded, scoped listing (4 known prefixes) — not a corpus walk.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python -u \\
        scripts/correct_cefi_kraken_futures_rowcount_2026_08_02.py --dry-run
    GCP_PROJECT_ID=central-element-323112 MANIFEST_PER_VM_SHARDS=true \\
        VM_NAME=cefi-kraken-futures-rowcount-fix .venv/bin/python -u \\
        scripts/correct_cefi_kraken_futures_rowcount_2026_08_02.py --apply
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ASSET_GROUP = "cefi"
VENUE = "KRAKEN-FUTURES"
INSTRUMENT_TYPE = "future"
_BACKUPS_TOP_LEVEL_PREFIX = "_remediation_backups/"

# The 4 affected (day, data_type) cells, per defi_orphan_sweep_test_artifact_prod_leak_2026_07_24.md todo 4.
AFFECTED_CELLS: tuple[tuple[str, str], ...] = (
    ("2024-02-01", "book_snapshot_5"),
    ("2024-02-01", "trades"),
    ("2025-01-10", "book_snapshot_5"),
    ("2025-01-10", "trades"),
)


def _load_backfill_module() -> ModuleType:
    """Load the sibling backfill script (its footer-read helpers are the SSOT for
    cheap ranged-GET row counting — this script must agree with them, not
    re-implement a second reader)."""
    script_path = Path(__file__).resolve().parent / "backfill_orphan_class_e.py"
    spec = importlib.util.spec_from_file_location("_backfill_orphan_class_e_for_correction", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_backfill_orphan_class_e_for_correction"] = module
    spec.loader.exec_module(module)
    return module


_backfill = _load_backfill_module()


def _canonical_prefix(day: str, data_type: str) -> str:
    return (
        f"raw_tick_data/by_date/day={day}/pipeline_mode=batch_tardis/asset_group={ASSET_GROUP}/"
        f"venue={VENUE}/instrument_type={INSTRUMENT_TYPE}/data_type={data_type}/"
    )


@dataclass(frozen=True)
class CellCorrection:
    day: str
    data_type: str
    true_row_count: int
    object_count: int
    schema_df: object  # footer-exact zero-row frame from one canonical object


def compute_true_row_count(client: object, bucket: str, day: str, data_type: str) -> CellCorrection:
    """List the CANONICAL prefix only (excludes ``_remediation_backups/`` by
    construction — that prefix lives at the bucket root, not under this one) and
    footer-sum every object found. Raises if 0 objects are found (refuses to record
    a captured cell with no evidence)."""
    prefix = _canonical_prefix(day, data_type)
    blobs = list(client.list_blobs(bucket, prefix=prefix))  # type: ignore[attr-defined]
    assert all(_BACKUPS_TOP_LEVEL_PREFIX not in b.name for b in blobs), (
        f"canonical prefix listing unexpectedly matched a backup path: {prefix}"
    )
    if not blobs:
        raise RuntimeError(f"0 canonical objects found at {prefix} — refusing to record an unevidenced cell")
    total = 0
    schema_df: object | None = None
    for b in blobs:
        n_rows, _cols, schema = _backfill._read_parquet_footer(bucket, b.name, b.size)
        total += n_rows
        if schema_df is None:
            schema_df = _backfill._empty_frame_from_schema(schema)
    return CellCorrection(
        day=day, data_type=data_type, true_row_count=total, object_count=len(blobs), schema_df=schema_df
    )


def apply_correction(bucket: str, corrections: list[CellCorrection]) -> int:
    """One ``record_captured`` per corrected cell — mirrors
    ``backfill_orphan_class_e.py::record_cells()``'s cefi RECORD_ONLY shape exactly,
    with the TRUE (backup-excluded) row_count."""
    from unified_api_contracts import PipelineMode, source_string_for
    from unified_trading_library import ManifestWriter

    pm = PipelineMode.BATCH_TARDIS
    source = source_string_for(pm) or ""
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        per_vm_shards=True,
        batch_size=10,
        strict_validation=False,
    )
    recorded = 0
    for c in corrections:
        rep_df = _backfill.canonicalise_frame(
            c.schema_df,
            asset_group=ASSET_GROUP,
            data_type=c.data_type,
            source=source,
            available_at=datetime.now(UTC),
        )
        writer.record_captured(
            row_key={"date": c.day, "venue": VENUE},
            df=rep_df,  # type: ignore[arg-type]
            asset_group=ASSET_GROUP,
            instrument_type=INSTRUMENT_TYPE,
            data_type=c.data_type,
            venue=VENUE,
            underlying="",
            row_count=c.true_row_count,
            attempted_at=datetime.now(UTC),
            pipeline_mode=pm,
            source=source,
        )
        recorded += 1
        logger.info(
            "recorded corrected cell (%s, %s): true_row_count=%d from %d canonical objects",
            c.day,
            c.data_type,
            c.true_row_count,
            c.object_count,
        )
    writer.close()
    return recorded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    from unified_trading_library import (
        GcsEventSink,
        UnifiedCloudConfig,
        get_storage_client,
        resolve_bucket_name,
        setup_events,
    )

    # M-COORD-6: the ManifestWriter validation path emits events — init first
    # (mirrors backfill_orphan_class_e.py::main()).
    project_id = UnifiedCloudConfig().gcp_project_id
    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(project_id=project_id, bucket=f"{project_id}-events", service_name="instruments-service"),
    )
    bucket = resolve_bucket_name(cloud="gcp", kind="tick-data", asset_group=ASSET_GROUP)
    client = get_storage_client()

    corrections: list[CellCorrection] = []
    for day, data_type in AFFECTED_CELLS:
        c = compute_true_row_count(client, bucket, day, data_type)
        corrections.append(c)
        logger.info(
            "cell (%s, %s): true canonical-only row_count=%d (%d objects)",
            day,
            data_type,
            c.true_row_count,
            c.object_count,
        )

    if args.dry_run:
        logger.info("DRY-RUN complete — %d cells computed, no manifest write performed.", len(corrections))
        return 0

    recorded = apply_correction(bucket, corrections)
    logger.info(
        "APPLY COMPLETE: %d/%d cells recorded with corrected row_count. "
        "Next: wait for the manifest consolidator, then spot-check the cells read "
        "capture_status=captured with the true row_count.",
        recorded,
        len(corrections),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
