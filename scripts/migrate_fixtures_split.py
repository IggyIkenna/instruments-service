#!/usr/bin/env python3
"""Migrate sports parquets: split ``entity=fixtures`` → ``entity=fixtures_schedule`` + ``entity=fixtures_outcomes``.

The FIXTURES schema-split (lookahead-bias fix) moves post-match scores off the
scheduling parquet.  This one-shot migration reads every
``entity=fixtures/*.parquet`` blob and writes two new blobs per source:

* ``entity=fixtures_schedule/`` — schedule columns + Q5 phase timestamps.
  ``available_at`` keeps its existing value (announced_at semantics).
* ``entity=fixtures_outcomes/`` — score/result columns + Q6 score-distinction.
  ``available_at`` is overwritten with ``match_end_time`` (correct lookahead-
  bias semantics: outcomes are only available after the match ends).

Source blobs are NOT deleted — the writer entity-split + writegate strict-mode
flip ships same-day as this migration so new writes go to the new entity paths
immediately after (per the writegate coordination protocol in the plan).

Per-parquet behaviour (idempotent — rerun on already-split files is a no-op):

* **Case A_split**   — neither target exists → write both (CAS
  ``if_generation_match=0`` asserts the target is new).
* **Case B_partial** — one target missing → write only the missing one.
* **Case D_skip**    — both targets already exist → skip.
* **Case F_read_failed** — read, split, or upload error (non-recoverable).

Per-VM shard isolation: ``--vm-name <tag>`` if running multi-worker across VMs
(per CLAUDE.md "Per-VM shard isolation for concurrent backfills" rule).

Operator usage (run on same-region GCE VM in ``asia-northeast1-c``):

    cd instruments-service
    .venv/bin/python scripts/migrate_fixtures_split.py \\
        --bucket gs://instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 32 \\
        --dry-run

    # Inspect output, then apply:
    .venv/bin/python scripts/migrate_fixtures_split.py \\
        --bucket gs://instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 32

Pre-flight (operator responsibility — this script does NOT do it):
    1. Pause sports forward-poll VMs (af-fwd-*, fs-fwd-*, etc.).
    2. Pause sports backfill VMs.
    3. Run this migration (dry-run first, then apply).
    4. Ship the writer entity-split + writegate strict-mode flip same-day.
    5. Resume VMs.

Cross-region listing is 18x slower — always run on a same-region VM.

Reference plan:
    unified-trading-pm/plans/active/sports_fixtures_schema_split_completion_2026_06_20.md
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

# Path segment identifying source blobs. Trailing slash prevents matching
# already-split entity=fixtures_schedule/ and entity=fixtures_outcomes/ paths.
_ENTITY_SEGMENT = "/entity=fixtures/"
_ENTITY_SCHED = "/entity=fixtures_schedule/"
_ENTITY_OUTCOMES = "/entity=fixtures_outcomes/"

# Columns that migrate to entity=fixtures_schedule (scheduling metadata + Q5 phase timestamps).
# Columns absent from the source table are silently skipped — older legacy rows
# lack Q5 columns written after uac@c4058c68.
_SCHEDULE_KEEP: frozenset[str] = frozenset({
    "af_fixture_id",
    "date",
    "timestamp",
    "periods_first",
    "periods_second",
    "venue_id",
    "venue_name",
    "venue_city",
    "referee_name",
    "status_long",
    "status_short",
    "status_elapsed_time",
    "af_league_id",
    "league_id",
    "season",
    "round",
    "af_home_id",
    "af_away_id",
    "af_home_name",
    "af_away_name",
    "day",
    "available_at",
    "announced_at",
    # Q5 phase timestamps (may be absent in legacy rows)
    "halftime_start_time",
    "halftime_end_time",
    "extra_time_first_half_start_time",
    "extra_time_first_half_end_time",
    "extra_time_second_half_start_time",
    "extra_time_second_half_end_time",
    "penalty_shootout_start_time",
    "penalty_shootout_end_time",
    "whistle_full_time_at",
})

# Columns that migrate to entity=fixtures_outcomes (match results + Q6 score-distinction).
# available_at is overwritten with match_end_time in _split_source() so that
# outcomes are only readable after the match ends (lookahead-bias fix).
_OUTCOMES_KEEP: frozenset[str] = frozenset({
    "af_fixture_id",
    "league_id",
    "day",
    "af_winner_id",
    "home_score",
    "away_score",
    "home_score_halftime",
    "away_score_halftime",
    "home_score_fulltime",
    "away_score_fulltime",
    "home_score_extratime",
    "away_score_extratime",
    "home_score_penalty",
    "away_score_penalty",
    "available_at",   # overwritten with match_end_time below
    "match_end_time",
    "report_time",
    # Q6 score-distinction (may be absent in legacy rows)
    "home_score_regulation",
    "away_score_regulation",
    "home_score_after_extra_time",
    "away_score_after_extra_time",
    "home_score_after_penalty_shootout",
    "away_score_after_penalty_shootout",
    "home_penalty_shootout_score",
    "away_penalty_shootout_score",
    "went_to_extra_time",
    "went_to_penalties",
    "match_result",
})


@dataclass(frozen=True)
class MigrationCase:
    """Outcome of a single source-blob migration attempt."""

    blob_name: str
    case: str  # "A_split" / "B_partial" / "D_skip" / "F_read_failed"
    rows: int
    sched_name: str
    out_name: str
    error: str | None


def _make_storage_client(project_id: str, workers: int) -> storage.Client:
    """Storage client with HTTP pool sized for concurrency (per CLAUDE.md rule)."""
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


def _list_source_parquets(bucket: storage.Bucket, prefix: str) -> list[str]:
    """List entity=fixtures/ parquets under prefix (already-split paths excluded)."""
    logger.info("Listing parquets under gs://%s/%s …", bucket.name, prefix)
    t0 = time.time()
    names: list[str] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".parquet") and _ENTITY_SEGMENT in blob.name:
            names.append(blob.name)
    elapsed = time.time() - t0
    logger.info(
        "Found %d entity=fixtures .parquet blobs (%.1f sec, %.1f/sec)",
        len(names),
        elapsed,
        len(names) / max(0.01, elapsed),
    )
    return names


def _target_names(blob_name: str) -> tuple[str, str]:
    """Compute (schedule_blob_name, outcomes_blob_name) from a source blob name."""
    sched = blob_name.replace(_ENTITY_SEGMENT, _ENTITY_SCHED, 1)
    out = blob_name.replace(_ENTITY_SEGMENT, _ENTITY_OUTCOMES, 1)
    return sched, out


def _split_source(table: pa.Table) -> tuple[pa.Table, pa.Table]:
    """Split source entity=fixtures table → (schedule_table, outcomes_table).

    outcomes.available_at is overwritten with match_end_time so that outcomes
    rows are only readable after the match ends (lookahead-bias fix per plan).
    Columns not present in the source are silently omitted (idempotent for
    both legacy rows without Q5/Q6 and rows that already carry them).
    """
    sched_cols = [c for c in table.column_names if c in _SCHEDULE_KEEP]
    out_cols = [c for c in table.column_names if c in _OUTCOMES_KEEP]

    sched = table.select(sched_cols)
    out = table.select(out_cols)

    # Overwrite outcomes available_at with match_end_time.  For completed
    # fixtures match_end_time is populated; for upcoming it is None — both are
    # correct (None means "not available yet").
    if "match_end_time" in out.column_names:
        met_col = out.column("match_end_time")
        if "available_at" in out.column_names:
            idx = out.schema.get_field_index("available_at")
            out = out.set_column(idx, pa.field("available_at", met_col.type), met_col)
        else:
            out = out.append_column(pa.field("available_at", met_col.type), met_col)

    return sched, out


def _serialise(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    return buf.getvalue()


def _upload_if_new(bucket: storage.Bucket, blob_name: str, data: bytes) -> bool:
    """Upload blob only if it does not already exist (if_generation_match=0).

    Returns True if written, False if already exists (PreconditionFailed).
    Any other exception is re-raised for the caller to handle.
    """
    blob = bucket.blob(blob_name)
    try:
        blob.upload_from_string(
            data,
            content_type="application/octet-stream",
            if_generation_match=0,
        )
        return True
    except PreconditionFailed:
        return False  # target already exists — treat as success (idempotent)


def _migrate_one(
    bucket: storage.Bucket,
    blob_name: str,
    *,
    dry_run: bool,
) -> MigrationCase:
    """Migrate one entity=fixtures blob into schedule + outcomes. Idempotent."""
    sched_name, out_name = _target_names(blob_name)

    # Fast-path: avoid source download when both targets already exist.
    sched_exists = bucket.blob(sched_name).exists()
    out_exists = bucket.blob(out_name).exists()
    if sched_exists and out_exists:
        return MigrationCase(
            blob_name=blob_name,
            case="D_skip",
            rows=0,
            sched_name=sched_name,
            out_name=out_name,
            error=None,
        )

    # Download + parse source.
    try:
        raw = bucket.blob(blob_name).download_as_bytes()
        table = pq.read_table(io.BytesIO(raw))
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",
            rows=0,
            sched_name=sched_name,
            out_name=out_name,
            error=f"read: {exc}",
        )

    rows = table.num_rows

    try:
        sched_table, out_table = _split_source(table)
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationCase(
            blob_name=blob_name,
            case="F_read_failed",
            rows=rows,
            sched_name=sched_name,
            out_name=out_name,
            error=f"split: {exc}",
        )

    if dry_run:
        case = "B_partial" if (sched_exists or out_exists) else "A_split"
        return MigrationCase(
            blob_name=blob_name,
            case=case,
            rows=rows,
            sched_name=sched_name,
            out_name=out_name,
            error="dry-run (no upload)",
        )

    sched_data = _serialise(sched_table)
    out_data = _serialise(out_table)
    sched_written = False
    out_written = False

    if not sched_exists:
        try:
            sched_written = _upload_if_new(bucket, sched_name, sched_data)
        except Exception as exc:  # broad-except-ok: per-blob isolation
            return MigrationCase(
                blob_name=blob_name,
                case="F_read_failed",
                rows=rows,
                sched_name=sched_name,
                out_name=out_name,
                error=f"upload schedule: {exc}",
            )

    if not out_exists:
        try:
            out_written = _upload_if_new(bucket, out_name, out_data)
        except Exception as exc:  # broad-except-ok: per-blob isolation
            return MigrationCase(
                blob_name=blob_name,
                case="F_read_failed",
                rows=rows,
                sched_name=sched_name,
                out_name=out_name,
                error=f"upload outcomes: {exc}",
            )

    if sched_written and out_written:
        case = "A_split"
    elif sched_written or out_written:
        case = "B_partial"
    else:
        # Both targets already existed (discovered via pre-check or CAS response).
        case = "D_skip"

    return MigrationCase(
        blob_name=blob_name,
        case=case,
        rows=rows,
        sched_name=sched_name,
        out_name=out_name,
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
        help=(
            "Per-VM tag for multi-worker runs (per CLAUDE.md shard-isolation rule). "
            "Single-VM single-process runs may omit this."
        ),
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

    blob_names = _list_source_parquets(bucket, args.prefix)
    if args.limit > 0:
        logger.info("Limiting to first %d blobs (per --limit)", args.limit)
        blob_names = blob_names[: args.limit]

    if not blob_names:
        logger.info("No entity=fixtures parquets found; nothing to do.")
        return 0

    results = _run_migration(bucket, blob_names, dry_run=args.dry_run, workers=args.workers)

    summary = _summarise(results)
    logger.info("--- Summary ---")
    logger.info("  A_split    (both targets written):           %d", summary.get("A_split", 0))
    logger.info("  B_partial  (one target was missing, added):  %d", summary.get("B_partial", 0))
    logger.info("  D_skip     (both targets already exist):     %d", summary.get("D_skip", 0))
    logger.info("  F_read_failed:                               %d", summary.get("F_read_failed", 0))
    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info(
        "Migration end: %d total, %.1f sec wall-clock, dry_run=%s",
        len(results),
        elapsed,
        args.dry_run,
    )

    failures = [r for r in results if r.case == "F_read_failed"]
    if failures:
        logger.warning("--- Failures (first 20) ---")
        for r in failures[:20]:
            logger.warning("  %s [%s]: %s", r.blob_name, r.case, r.error)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
