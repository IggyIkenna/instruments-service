"""Delete phantom rows from canonical + per-VM shards so orchestrator sees them as missing.

Targeted alternative to passing ``--force`` to the orchestrator: instead of
re-fetching every row in the date range, we delete only the rows we know
are phantoms or corrective-flipped, leaving the orchestrator to enumerate
them as MISSING and fetch them under the writer fix.

Phantom signature on a (date, league_id, data_type) row_key:

  - ``capture_status='attempted_failed' AND error_reason='phantom_re_attempt_after_writer_fix_f36651c'``
    (rows from the corrective flip in commit ``2821111``)
  - OR ``capture_status='captured' AND instrument_count==0`` for the api_football
    per-fixture data types (FIXTURES + 5 downstreams) — the original bug class
    where ``manifest.add(row_count=0)`` was emitted before the writer fix
    ``f36651c``. These exist in OLDER per-VM shards (e.g. af-backfill-20260429-*)
    that pre-date the corrective flip.

Why both signatures: the corrective flip wrote to canonical only. Older per-VM
shards still hold the original phantom-captured rows; if we delete only the
corrective marker, the per-VM merge falls back to those older rows and the
orchestrator's freshness check still treats them as "present" → skip.

What we DO NOT delete: rows where ``capture_status='captured' AND instrument_count > 0``
(real captures from a recent backfill VM under the writer fix). Those are honest
data and must survive.

Affected data types (from the FIXTURES phantom + per-fixture downstream audit):
  FIXTURES, INJURIES, FIXTURE_STATS, FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS

Backup-then-write per shard. Idempotent.

Usage:
  python scripts/delete_phantom_rows_from_shards.py --dry-run
  python scripts/delete_phantom_rows_from_shards.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_PER_VM_PREFIX = "_index/per_vm/"
_REFLIP_MARKER = "phantom_re_attempt_after_writer_fix_f36651c"

# Data types in the FIXTURES phantom + per-fixture downstream bug class.
_TARGET_DATA_TYPES: tuple[str, ...] = (
    "FIXTURES",
    "INJURIES",
    "FIXTURE_STATS",
    "FIXTURE_EVENTS",
    "FIXTURE_LINEUPS",
    "PLAYER_STATS",
)


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _backup_path(blob_name: str, run_ts: str) -> str:
    # Insert .{run_ts}.bak before the .parquet extension.
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{run_ts}.bak.parquet"
    return f"{blob_name}.{run_ts}.bak"


def _is_phantom(row: pd.Series) -> bool:
    """Return True if this row matches the phantom signature."""
    capture_status = str(row.get("capture_status", ""))
    error_reason = str(row.get("error_reason", ""))
    instrument_count = pd.to_numeric(row.get("instrument_count"), errors="coerce")
    if pd.isna(instrument_count):
        instrument_count = -1
    instrument_count = int(instrument_count)
    data_type = str(row.get("data_type", ""))

    if data_type not in _TARGET_DATA_TYPES:
        return False

    # Signature 1: corrective-flip marker
    if capture_status == "attempted_failed" and error_reason == _REFLIP_MARKER:
        return True

    # Signature 2: original phantom (older per-VM shards)
    return bool(capture_status == "captured" and instrument_count == 0)


def _filter_shard(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Return (df_without_phantoms, deleted_count)."""
    if df.empty:
        return df, 0
    phantom_mask = df.apply(_is_phantom, axis=1)
    deleted = int(phantom_mask.sum())
    if deleted == 0:
        return df, 0
    return df.loc[~phantom_mask].copy(), deleted


def _process_blob(
    storage: object,  # pyright: ignore[reportAny]
    bucket: str,
    blob_name: str,
    run_ts: str,
    *,
    apply: bool,
) -> tuple[int, int]:
    """Process one shard blob. Returns (deleted_count, total_rows_before)."""
    try:
        raw = storage.download_bytes(bucket, blob_name)  # type: ignore[attr-defined]  # pyright: ignore[reportAny]
    except Exception as exc:  # pragma: no cover — best-effort
        logger.warning("Failed to read %s: %s", blob_name, exc)
        return 0, 0

    df = pd.read_parquet(io.BytesIO(raw))
    total_before = len(df)
    filtered, deleted = _filter_shard(df)

    if deleted == 0:
        return 0, total_before

    logger.info(
        "  %s: would delete %d / %d rows",
        blob_name,
        deleted,
        total_before,
    )

    if not apply:
        return deleted, total_before

    # Backup-then-write
    backup = _backup_path(blob_name, run_ts)
    storage.upload_bytes(bucket, backup, raw)  # type: ignore[attr-defined]  # pyright: ignore[reportAny]

    out = io.BytesIO()
    filtered.to_parquet(out, index=False, engine="pyarrow")
    out.seek(0)
    storage.upload_bytes(bucket, blob_name, out.read())  # type: ignore[attr-defined]  # pyright: ignore[reportAny]
    logger.info(
        "    backup → gs://%s/%s; rewrote %d rows",
        bucket,
        backup,
        len(filtered),
    )
    return deleted, total_before


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default="central-element-323112")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = _bucket_for_project(args.project_id)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")

    storage = get_storage_client(project_id=args.project_id)

    # 1. Process canonical
    logger.info("=== Canonical: gs://%s/%s ===", bucket, _INDEX_PATH)
    canonical_deleted, canonical_total = _process_blob(
        storage,
        bucket,
        _INDEX_PATH,
        run_ts,
        apply=args.apply,
    )

    # 2. Process every per-VM shard
    logger.info("=== Per-VM shards: gs://%s/%s* ===", bucket, _PER_VM_PREFIX)
    # List blobs under per_vm/
    native = getattr(storage, "_client", None)  # pyright: ignore[reportAny]
    if native is None:
        logger.error("Could not access native GCS client for listing per-VM shards. Aborting.")
        return 2
    blobs = list(native.bucket(bucket).list_blobs(prefix=_PER_VM_PREFIX))  # type: ignore[union-attr]  # pyright: ignore[reportAny]
    parquet_blobs = [b.name for b in blobs if b.name.endswith(".parquet") and ".bak" not in b.name]
    logger.info("Per-VM shards to scan: %d", len(parquet_blobs))

    per_vm_total_deleted = 0
    per_vm_shards_touched = 0
    for blob_name in parquet_blobs:
        deleted, _ = _process_blob(storage, bucket, blob_name, run_ts, apply=args.apply)
        if deleted > 0:
            per_vm_total_deleted += deleted
            per_vm_shards_touched += 1

    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  Canonical: %d phantom rows deleted (of %d total)", canonical_deleted, canonical_total)
    logger.info("  Per-VM shards: %d rows deleted across %d shards", per_vm_total_deleted, per_vm_shards_touched)
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("DRY RUN — no writes performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
