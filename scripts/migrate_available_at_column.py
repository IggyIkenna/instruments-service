#!/usr/bin/env python3
"""Migrate GCS parquets: add or repair the ``available_at`` column.

Generalized version of ``migrate_sports_available_at_column.py``.  Handles all
asset_groups by looking up the stamping semantic from UAC
``AVAILABILITY_AT_SEMANTICS[(asset_group, data_type)]``.

Per-parquet behaviour (idempotent — rerun on already-migrated files is a no-op):

* **Case A** — column ``data_available_at`` present AND ``available_at`` absent
  → rename column atomically + re-upload (CAS via if_generation_match).
* **Case B** — both columns present (mid-migration restart, or partial write)
  → drop the legacy ``data_available_at`` column; keep canonical ``available_at``.
* **Case C** — only ``available_at`` present → already migrated; skip.
* **Case D_tick** — neither present; semantic is ``tick_timestamp`` and a
  ``timestamp`` column exists → derive ``available_at`` from ``timestamp``.
* **Case D_blob** — neither present; ``tick_timestamp`` semantic but no
  ``timestamp`` column, OR non-tick semantic → use GCS blob update time as
  the best available approximation.
* **Case D_empty** — 0-row parquet with neither column → add an empty
  ``available_at`` column of type ``timestamp[us, tz=UTC]`` (honest-empty
  safeguard; UTL ``assert_available_at_present`` passes on empty DataFrames).
* **Case E** — CAS conflict (concurrent writer) → log + rerun.
* **Case F** — read/write failure → log + continue.

Per-VM shard isolation: set ``VM_NAME`` env var AND ``MANIFEST_PER_VM_SHARDS=true``
when running multi-worker across VMs (per CLAUDE.md "Per-VM shard isolation").

Operator usage (run on same-region GCE VM in ``asia-northeast1-c``):

    cd instruments-service
    .venv/bin/python scripts/migrate_available_at_column.py \\
        --asset-group defi \\
        --data-type lending_indices \\
        --bucket gs://market-tick-data-defi-${PROJECT_ID} \\
        --prefix defi/ \\
        --project-id ${PROJECT_ID} \\
        --dry-run

    # For real:
    VM_NAME=migrate-available-at-defi-$(date +%s) MANIFEST_PER_VM_SHARDS=true \\
    .venv/bin/python scripts/migrate_available_at_column.py \\
        --asset-group defi \\
        --data-type lending_indices \\
        --bucket gs://market-tick-data-defi-${PROJECT_ID} \\
        --prefix defi/ \\
        --project-id ${PROJECT_ID} \\
        --apply

Reference plan:
    unified-trading-pm/plans/active/available_at_lookahead_bias_completion_2026_05_08.md
    Phase 2: Historical parquet backfill.

Cross-region listing is 18x slower — always run on a same-region VM.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
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
from unified_api_contracts.canonical.crosscutting import AVAILABILITY_AT_SEMANTICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LEGACY_COL = "data_available_at"
CANONICAL_COL = "available_at"
TIMESTAMP_COL = "timestamp"

_TICK_SEMANTIC = "tick_timestamp"

# ---------------------------------------------------------------------------
# Storage client
# ---------------------------------------------------------------------------


def _make_storage_client(project_id: str, workers: int) -> storage.Client:
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


# ---------------------------------------------------------------------------
# Core migration logic (pure functions, no I/O)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MigrationCase:
    """Outcome of a single parquet migration attempt."""

    blob_name: str
    case: str  # see module docstring
    rows: int
    schema_before: tuple[str, ...]
    schema_after: tuple[str, ...]
    error: str | None


def _classify(columns: tuple[str, ...], semantic: str | None) -> str:
    has_legacy = LEGACY_COL in columns
    has_canonical = CANONICAL_COL in columns
    has_timestamp = TIMESTAMP_COL in columns

    if has_legacy and not has_canonical:
        return "A_renamed"
    if has_legacy and has_canonical:
        return "B_dedup"
    if not has_legacy and has_canonical:
        return "C_skip"

    # Neither column present — choose D variant.
    if semantic == _TICK_SEMANTIC and has_timestamp:
        return "D_tick"
    return "D_blob"


def _apply_migration(
    table: pa.Table,
    *,
    case: str,
    blob_updated: datetime,
) -> pa.Table:
    """Apply the migration to an in-memory pyarrow table. Pure function."""
    if case == "A_renamed":
        new_names = [CANONICAL_COL if name == LEGACY_COL else name for name in table.column_names]
        return table.rename_columns(new_names)

    if case == "B_dedup":
        return table.drop(columns=[LEGACY_COL])

    if case in ("C_skip", "E_cas_failed"):
        return table

    if case == "D_tick":
        ts_col = table.column(TIMESTAMP_COL)
        # Cast to timestamp[us, tz=UTC] — handles both int (unix-s) and
        # datetime columns.  For integer dtype, interpret as Unix seconds.
        if pa.types.is_integer(ts_col.type):
            ts_us = ts_col.cast(pa.int64()).cast(pa.timestamp("s", tz="UTC")).cast(pa.timestamp("us", tz="UTC"))
        else:
            ts_us = ts_col.cast(pa.timestamp("us", tz="UTC"))
        return table.append_column(CANONICAL_COL, ts_us)

    if case == "D_blob":
        # Honest-empty (0-row) or non-tick: use blob update time for every row.
        n = table.num_rows
        blob_ts_us = int(blob_updated.timestamp() * 1_000_000)
        arr = pa.array([blob_ts_us] * n, type=pa.int64()).cast(pa.timestamp("us", tz="UTC"))
        return table.append_column(CANONICAL_COL, arr)

    return table


# ---------------------------------------------------------------------------
# Per-blob worker
# ---------------------------------------------------------------------------


def _migrate_one(
    bucket: storage.Bucket,
    blob_name: str,
    *,
    semantic: str | None,
    apply: bool,
) -> MigrationCase:
    blob = bucket.blob(blob_name)

    try:
        raw = blob.download_as_bytes()
        blob.reload()
        generation = blob.generation
        blob_updated: datetime = blob.updated or datetime.now(UTC)
        if blob_updated.tzinfo is None:
            blob_updated = blob_updated.replace(tzinfo=UTC)
    except Exception as exc:
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
    except Exception as exc:
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
    case = "D_blob" if rows == 0 and CANONICAL_COL not in columns_before else _classify(columns_before, semantic)
    # 0-row parquets with available_at already present: skip
    if rows == 0 and CANONICAL_COL in columns_before:
        case = "C_skip"

    if case == "C_skip":
        return MigrationCase(
            blob_name=blob_name,
            case=case,
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_before,
            error=None,
        )

    new_table = _apply_migration(table, case=case, blob_updated=blob_updated)
    columns_after = tuple(new_table.column_names)

    if not apply:
        return MigrationCase(
            blob_name=blob_name,
            case=case,
            rows=rows,
            schema_before=columns_before,
            schema_after=columns_after,
            error="dry-run (no upload)",
        )

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
            error="if_generation_match conflict — rerun",
        )
    except Exception as exc:
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",
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


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _list_parquets(bucket: storage.Bucket, prefix: str) -> list[str]:
    logger.info("Listing parquets under gs://%s/%s …", bucket.name, prefix)
    t0 = time.time()
    names: list[str] = [b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]
    elapsed = time.time() - t0
    logger.info(
        "Found %d .parquet blobs (%.1f sec, %.1f/sec)",
        len(names),
        elapsed,
        len(names) / max(0.01, elapsed),
    )
    return names


def _run(
    bucket: storage.Bucket,
    blob_names: list[str],
    *,
    semantic: str | None,
    apply: bool,
    workers: int,
) -> list[MigrationCase]:
    results: list[MigrationCase] = []
    total = len(blob_names)
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_migrate_one, bucket, n, semantic=semantic, apply=apply) for n in blob_names]
        for completed, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            results.append(res)
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-group", required=True, help="cefi|defi|tradfi|sports|prediction")
    parser.add_argument("--data-type", required=True, help="Data type for semantic lookup (e.g. lending_indices)")
    parser.add_argument("--bucket", required=True, help="GCS bucket URI, e.g. gs://<name>")
    parser.add_argument("--prefix", required=True, help="GCS prefix to enumerate")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually upload (default: scan-only). Requires VM_NAME env + MANIFEST_PER_VM_SHARDS=true.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Process at most N blobs (0=all)")
    args = parser.parse_args(argv)

    if args.apply:
        vm_name = os.environ.get("VM_NAME", "")
        shards_env = os.environ.get("MANIFEST_PER_VM_SHARDS", "")
        if not vm_name:
            logger.error("--apply requires VM_NAME env var (per CLAUDE.md shard-isolation rule)")
            return 2
        if shards_env.lower() != "true":
            logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true env var")
            return 2

    # Extract typed values from Namespace to satisfy basedpyright reportAny.
    bucket_uri: str = str(args.bucket)
    prefix: str = str(args.prefix)
    project_id: str = str(args.project_id)
    workers: int = int(args.workers)
    limit: int = int(args.limit)
    apply: bool = bool(args.apply)
    asset_group: str = str(args.asset_group).lower()
    data_type: str = str(args.data_type)

    if not bucket_uri.startswith("gs://"):
        logger.error("--bucket must start with gs:// (got %r)", bucket_uri)
        return 2

    bucket_name = bucket_uri[len("gs://") :].split("/", 1)[0]

    semantic = AVAILABILITY_AT_SEMANTICS.get((asset_group, data_type))
    if semantic is None:
        logger.warning(
            "No AVAILABILITY_AT_SEMANTICS entry for (%r, %r) — will use D_blob fallback",
            asset_group,
            data_type,
        )

    started = datetime.now(UTC)
    logger.info(
        "Migration start: asset_group=%s data_type=%s semantic=%s bucket=%s prefix=%s workers=%d apply=%s",
        asset_group,
        data_type,
        semantic,
        bucket_name,
        prefix,
        workers,
        apply,
    )

    client = _make_storage_client(project_id, workers)
    bucket = client.bucket(bucket_name)

    blob_names = _list_parquets(bucket, prefix)
    if limit > 0:
        logger.info("Limiting to first %d blobs (per --limit)", limit)
        blob_names = blob_names[:limit]

    if not blob_names:
        logger.info("No parquets found; nothing to do.")
        return 0

    results = _run(bucket, blob_names, semantic=semantic, apply=apply, workers=workers)

    summary: dict[str, int] = {}
    for r in results:
        summary[r.case] = summary.get(r.case, 0) + 1

    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info("--- Summary ---")
    logger.info("  A_renamed (data_available_at → available_at):  %d", summary.get("A_renamed", 0))
    logger.info("  B_dedup   (both columns; legacy dropped):       %d", summary.get("B_dedup", 0))
    logger.info("  C_skip    (already canonical):                  %d", summary.get("C_skip", 0))
    logger.info("  D_tick    (derived from timestamp column):      %d", summary.get("D_tick", 0))
    logger.info("  D_blob    (derived from blob update time):      %d", summary.get("D_blob", 0))
    logger.info("  E_cas_failed (concurrent writer; rerun):        %d", summary.get("E_cas_failed", 0))
    logger.info("  F_read_failed:                                  %d", summary.get("F_read_failed", 0))
    logger.info("Migration end: %d total, %.1f sec wall-clock, apply=%s", len(results), elapsed, apply)

    failures = [r for r in results if r.case in ("E_cas_failed", "F_read_failed")]
    if failures:
        logger.warning("--- Failures (first 20) ---")
        for r in failures[:20]:
            logger.warning("  %s [%s]: %s", r.blob_name, r.case, r.error)

    return 1 if any(r.case == "F_read_failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
