#!/usr/bin/env python3
"""Migrate sports parquets: rename ``data_available_at`` → ``available_at``.

Sports adapters historically stamped a prefixed column name ``data_available_at``;
every other service (UTL ``assert_available_at_present``, every features-* and
strategy reader) uses the canonical ``available_at``. The prefixed name blocks
writegate plan Phase 2.C strict-mode flip (``LookaheadBiasError`` raises on
every sports ``record_captured`` call).

This script is the GCS-side half of the rename. The source-code rename across
UAC / UTL / instruments-service / features-sports ships separately, AFTER this
migration completes (per workspace "manifest migration not fallback" rule).

Reference plan:
    unified-trading-pm/plans/active/sports_data_available_at_rename_2026_05_07.md

Per-parquet behaviour (idempotent — rerun on already-migrated files is a no-op):

* **Case A** — column ``data_available_at`` present AND ``available_at`` absent
  → rename column atomically + re-upload parquet (CAS via if_generation_match).
* **Case B** — both columns present (mid-migration restart, or partial write)
  → drop the legacy ``data_available_at`` column; keep canonical ``available_at``.
* **Case C** — only ``available_at`` present → already migrated; skip.
* **Case D** — neither column present → log + skip (parquet is older schema;
  outside this migration's scope; sports manifest will surface the gap).

Per-VM shard isolation: ``--vm-name <tag>`` REQUIRED if running multi-worker
across VMs (per CLAUDE.md "Per-VM shard isolation for concurrent backfills").
Single-VM single-process runs do not need it.

Operator usage (run on same-region GCE VM in ``asia-northeast1-c``):

    cd instruments-service
    .venv/bin/python scripts/migrate_sports_available_at_column.py \\
        --bucket gs://instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 32 \\
        --dry-run

    # Inspect the dryrun output, then for real:
    .venv/bin/python scripts/migrate_sports_available_at_column.py \\
        --bucket gs://instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 32

Pre-flight (operator responsibility — this script does NOT do it):
    1. Pause sports forward-poll VMs (af-fwd-*, fs-fwd-*, tm-fwd-*, sfi-fwd-*,
       us-fwd-*, openmeteo-fwd-*).
    2. Pause sports backfill VMs (af-backfill-*, fs-backfill-*, etc.).
    3. Run this migration.
    4. Wait for source-code atomic 4-repo rename to ship.
    5. Resume forward-poll + backfill VMs.

Cross-region listing is 18x slower — always run on a same-region VM.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from requests.adapters import HTTPAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LEGACY_COL = "data_available_at"
CANONICAL_COL = "available_at"


@dataclass(frozen=True)
class MigrationCase:
    """Outcome of a single parquet migration attempt."""

    blob_name: str
    case: str  # "A_renamed" / "B_dedup" / "C_skip_already_canonical" / "D_skip_neither" / "E_cas_failed" / "F_read_failed"
    rows: int
    schema_before: tuple[str, ...]
    schema_after: tuple[str, ...]
    error: str | None


def _make_storage_client(project_id: str, workers: int) -> storage.Client:
    """Storage client with HTTP pool tuned for ``2*workers`` (per CLAUDE.md
    "Manifest phantom audit" rule — default pool of 10 silently truncates
    list_blobs() under high concurrency).
    """
    client = storage.Client(project=project_id)
    pool_size = max(64, workers * 2)
    try:
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=3,
        )
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
    except (AttributeError, TypeError):
        pass
    return client


def _list_parquets(bucket: storage.Bucket, prefix: str) -> list[str]:
    """Enumerate all .parquet blobs under ``prefix``."""
    logger.info("Listing parquets under gs://%s/%s …", bucket.name, prefix)
    t0 = time.time()
    names: list[str] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".parquet"):
            names.append(blob.name)
    elapsed = time.time() - t0
    logger.info(
        "Found %d .parquet blobs (%.1f sec, %.1f/sec)",
        len(names),
        elapsed,
        len(names) / max(0.01, elapsed),
    )
    return names


def _classify_schema(columns: tuple[str, ...]) -> str:
    """Return the case label for a parquet given its column tuple."""
    has_legacy = LEGACY_COL in columns
    has_canonical = CANONICAL_COL in columns
    if has_legacy and not has_canonical:
        return "A_renamed"
    if has_legacy and has_canonical:
        return "B_dedup"
    if not has_legacy and has_canonical:
        return "C_skip_already_canonical"
    return "D_skip_neither"


def _apply_migration(table: pa.Table) -> tuple[pa.Table, str]:
    """Apply the migration to an in-memory pyarrow table. Pure function — no I/O.

    Returns ``(new_table, case)``. For C/D cases, ``new_table is table`` (no-op).
    """
    columns_before = tuple(table.column_names)
    case = _classify_schema(columns_before)
    if case == "A_renamed":
        new_names = [CANONICAL_COL if name == LEGACY_COL else name for name in columns_before]
        return table.rename_columns(new_names), case
    if case == "B_dedup":
        return table.drop(columns=[LEGACY_COL]), case
    return table, case


def _migrate_one(
    bucket: storage.Bucket,
    blob_name: str,
    *,
    dry_run: bool,
) -> MigrationCase:
    """Migrate one parquet. Idempotent. CAS-safe."""
    blob = bucket.blob(blob_name)

    # Read schema cheaply via pyarrow (no row read needed for case classification).
    try:
        raw = blob.download_as_bytes()
        blob.reload()  # refresh generation for CAS
        generation = blob.generation
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",
            rows=0,
            schema_before=(),
            schema_after=(),
            error=f"download: {exc}",
        )

    try:
        table = pq.read_table(io.BytesIO(raw))
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",
            rows=0,
            schema_before=(),
            schema_after=(),
            error=f"read_table: {exc}",
        )

    columns_before = tuple(table.column_names)
    rows = table.num_rows
    new_table, case = _apply_migration(table)

    if case in ("C_skip_already_canonical", "D_skip_neither"):
        return MigrationCase(
            blob_name=blob_name,
            case=case,
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_before,
            error=None,
        )

    columns_after = tuple(new_table.column_names)

    if dry_run:
        return MigrationCase(
            blob_name=blob_name,
            case=case,
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_after,
            error="dry-run (no upload)",
        )

    # Re-serialise + upload with CAS.
    out = io.BytesIO()
    pq.write_table(
        new_table,
        out,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    try:
        blob.upload_from_string(
            out.getvalue(),
            content_type="application/octet-stream",
            if_generation_match=generation,
        )
    except PreconditionFailed:
        return MigrationCase(
            blob_name=blob_name,
            case="E_cas_failed",
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_after,
            error="if_generation_match (concurrent writer) — rerun script",
        )
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",  # write failure aliases to F (failed)
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_after,
            error=f"upload: {exc}",
        )

    return MigrationCase(
        blob_name=blob_name,
        case=case,
        rows=rows,
        schema_before=columns_before,
        schema_after=columns_after,
        error=None,
    )


def _run_migration(
    bucket: storage.Bucket,
    blob_names: list[str],
    *,
    dry_run: bool,
    workers: int,
) -> list[MigrationCase]:
    """Run the migration in a thread pool. Returns one MigrationCase per blob."""
    results: list[MigrationCase] = []
    completed = 0
    total = len(blob_names)
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_migrate_one, bucket, n, dry_run=dry_run) for n in blob_names]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            completed += 1
            if completed % 200 == 0 or completed == total:
                rate = completed / max(0.01, time.time() - t0)
                logger.info(
                    "  %d/%d (%.1f/sec) — last: %s [%s]",
                    completed,
                    total,
                    rate,
                    res.blob_name.rsplit("/", 1)[-1],
                    res.case,
                )
    return results


def _summarise(results: list[MigrationCase]) -> dict[str, int]:
    """Aggregate results by case for the operator-facing report."""
    summary: dict[str, int] = {}
    for r in results:
        summary[r.case] = summary.get(r.case, 0) + 1
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket URI, e.g. gs://instruments-store-sports-{project_id}",
    )
    parser.add_argument(
        "--prefix",
        default="sports_reference/by_date/",
        help="GCS prefix to enumerate (default: sports_reference/by_date/).",
    )
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers.")
    parser.add_argument("--dry-run", action="store_true", help="Plan only, no uploads.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N blobs (0 = all). For spot-check / smoke run.",
    )
    parser.add_argument(
        "--vm-name",
        default="",
        help="Per-VM tag for multi-worker runs (per CLAUDE.md shard-isolation rule). "
        "Single-VM single-process runs may omit this.",
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID for the storage client.",
    )
    args = parser.parse_args(argv)

    bucket_uri: str = args.bucket
    if not bucket_uri.startswith("gs://"):
        logger.error("--bucket must start with gs:// (got %r)", bucket_uri)
        return 2
    bucket_name = bucket_uri[len("gs://") :].split("/", 1)[0]

    started = datetime.now(UTC)
    logger.info(
        "Migration start: bucket=%s prefix=%s workers=%d dry_run=%s vm_name=%r",
        bucket_name,
        args.prefix,
        args.workers,
        args.dry_run,
        args.vm_name,
    )

    client = _make_storage_client(args.project_id, args.workers)
    bucket = client.bucket(bucket_name)

    blob_names = _list_parquets(bucket, args.prefix)
    if args.limit > 0:
        logger.info("Limiting to first %d blobs (per --limit)", args.limit)
        blob_names = blob_names[: args.limit]

    if not blob_names:
        logger.info("No parquets found; nothing to do.")
        return 0

    results = _run_migration(
        bucket,
        blob_names,
        dry_run=args.dry_run,
        workers=args.workers,
    )

    summary = _summarise(results)
    logger.info("--- Summary ---")
    logger.info("  A_renamed (data_available_at → available_at): %d", summary.get("A_renamed", 0))
    logger.info("  B_dedup (both columns; legacy dropped):       %d", summary.get("B_dedup", 0))
    logger.info("  C_skip_already_canonical:                     %d", summary.get("C_skip_already_canonical", 0))
    logger.info("  D_skip_neither (older schema; outside scope): %d", summary.get("D_skip_neither", 0))
    logger.info("  E_cas_failed (concurrent writer; rerun):      %d", summary.get("E_cas_failed", 0))
    logger.info("  F_read_failed:                                %d", summary.get("F_read_failed", 0))
    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info(
        "Migration end: %d total, %.1f sec wall-clock, dry_run=%s",
        len(results),
        elapsed,
        args.dry_run,
    )

    # Print the first few F_read_failed errors so the operator can triage.
    failures = [r for r in results if r.case in ("E_cas_failed", "F_read_failed")]
    if failures:
        logger.warning("--- Failures (first 20) ---")
        for r in failures[:20]:
            logger.warning("  %s [%s]: %s", r.blob_name, r.case, r.error)

    # Exit non-zero if any non-recoverable failures (F_read_failed).
    has_hard_failures = any(r.case == "F_read_failed" for r in results)
    return 1 if has_hard_failures else 0


if __name__ == "__main__":
    sys.exit(main())
