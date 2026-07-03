# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after deletion verified in live cefi _index + honest-coverage re-measure
"""Delete ASTER book_snapshot_5/liquidations rows the enumerator over-seeded.

Provenance: plans/active/issues/honest_coverage_uac_writer_matrix_reconciliation_2026_06_29.md
(class-2 ASTER carve-out contradiction, operator-diagnosed 2026-06-29: **UAC is
CORRECT; the enumerator over-seeds**). ASTER is a perp DEX with no
orderbook-snapshot or liquidation feed (absent from
``VENUE_DATA_TYPE_CAPABILITIES["ASTER"]``), yet the manifest carries
``(ASTER, PERPETUAL, book_snapshot_5|liquidations)`` rows in three states — all
artifacts of the same broken expectation, none representing data reality:

  - ``expected_unattempted`` (blank reason)             → the raw over-seed
  - ``empty_confirmed[EXPECTED_NO_SOURCE_DATA]``        → the same seed on
    pre-launch dates / later honestly re-typed — but a venue-cannot-produce
    cell belongs OUTSIDE the universe (carve-out), not inside it as an absence
  - ``attempted_failed[VENUE_FETCH_FAILED]`` (26 rows)  → attempts against an
    endpoint that does not exist, driven by the same seed

The enumerator-side fix (``_row_data_types`` venue-capability carve-out) stops
new seeding; this one-off removes the existing rows so Layer-1 stops reporting
the stray and Layer-2 stops counting unreachable cells in ASTER's denominator.

Deletes from BOTH cefi manifest buckets (prd + legacy flat), canonical
``_index/availability_index.parquet`` AND every ``_index/per_vm/*.parquet``
(the consolidator merges per-VM shards into canonical — deleting canonical only
would resurrect on the next consolidation; same reasoning as
``delete_phantom_rows_from_shards.py``).

Backup-then-write per blob. Idempotent.

Usage:
  python scripts/delete_aster_overseeded_capability_rows.py --dry-run
  python scripts/delete_aster_overseeded_capability_rows.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from unified_trading_library import StorageClient, get_storage_client

logger = logging.getLogger(__name__)

_PROJECT_ID_DEFAULT = "central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_TARGET_VENUE = "ASTER"
_TARGET_DATA_TYPES = ("book_snapshot_5", "liquidations")


def _buckets(project_id: str) -> tuple[str, ...]:
    # Both cefi manifest candidates (bucket-SSOT migration in flight — the
    # measure harness merges prd + legacy; the over-seed lives in legacy).
    return (
        f"market-data-tick-cefi-prd-{project_id}",
        f"market-data-tick-cefi-{project_id}",
    )


def _backup_path(blob_name: str, run_ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.bak.parquet"
    return f"{blob_name}.{run_ts}.bak"


def _target_mask(table: pa.Table) -> pa.BooleanArray | pa.ChunkedArray:
    """Vectorized mask of rows to DELETE (ASTER × book5/liquidations, ANY status)."""
    venue_upper = pc.utf8_upper(pc.cast(table["venue"], pa.string()))
    dt_lower = pc.utf8_lower(pc.cast(table["data_type"], pa.string()))
    mask = pc.and_(
        pc.equal(venue_upper, pa.scalar(_TARGET_VENUE)),
        pc.is_in(dt_lower, value_set=pa.array(_TARGET_DATA_TYPES)),
    )
    # Treat nulls (missing venue/data_type) as not-target.
    return pc.fill_null(mask, pa.scalar(False))


def _process_blob(
    storage: StorageClient,
    bucket: str,
    blob_name: str,
    run_ts: str,
    *,
    apply: bool,
) -> tuple[int, int]:
    """Process one shard blob. Returns (deleted_count, total_rows_before)."""
    try:
        raw = storage.download_bytes(bucket, blob_name)
    except Exception as exc:  # pragma: no cover — best-effort per blob
        logger.warning("Failed to read gs://%s/%s: %s", bucket, blob_name, exc)
        return 0, 0

    table = pq.read_table(io.BytesIO(raw))
    total_before = table.num_rows
    if "venue" not in table.column_names or "data_type" not in table.column_names:
        return 0, total_before

    mask = _target_mask(table)
    deleted = int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)
    if deleted == 0:
        return 0, total_before

    logger.info(
        "  gs://%s/%s: %s %d / %d rows",
        bucket,
        blob_name,
        "DELETING" if apply else "would delete",
        deleted,
        total_before,
    )
    if not apply:
        return deleted, total_before

    filtered = table.filter(pc.invert(mask))
    backup = _backup_path(blob_name, run_ts)
    storage.upload_bytes(bucket, backup, raw)

    out = io.BytesIO()
    pq.write_table(filtered, out)
    out.seek(0)
    storage.upload_bytes(bucket, blob_name, out.read())
    logger.info("    backup → gs://%s/%s; rewrote %d rows", bucket, backup, filtered.num_rows)
    return deleted, total_before


def main() -> int:
    parser = argparse.ArgumentParser(description="delete ASTER over-seeded book5/liquidations manifest rows")
    parser.add_argument("--project-id", default=_PROJECT_ID_DEFAULT)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    storage = get_storage_client(project_id=args.project_id)

    grand_deleted = 0
    for bucket in _buckets(args.project_id):
        # Canonical index
        logger.info("=== gs://%s/%s ===", bucket, _INDEX_PATH)
        deleted, _ = _process_blob(storage, bucket, _INDEX_PATH, run_ts, apply=args.apply)
        grand_deleted += deleted

        # Per-VM shards (skip our own backups)
        names = [
            meta.name
            for meta in storage.list_blobs(bucket, prefix=_PER_VM_PREFIX)
            if meta.name.endswith(".parquet") and ".bak." not in meta.name
        ]
        logger.info("=== gs://%s/%s (%d shards) ===", bucket, _PER_VM_PREFIX, len(names))
        for name in names:
            deleted, _ = _process_blob(storage, bucket, name, run_ts, apply=args.apply)
            grand_deleted += deleted

    logger.info(
        "%s: %d ASTER book5/liquidations rows across both buckets",
        "DELETED" if args.apply else "WOULD DELETE",
        grand_deleted,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
